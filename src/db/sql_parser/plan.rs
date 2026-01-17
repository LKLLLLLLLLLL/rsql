/// Definitions for logical query plans.
/// This module defines the LogicalPlan enum and related structures for representing query execution plans.

// sqlparser crate
use sqlparser::ast::{
    Expr,
    Statement,
    Query,
    SetExpr,
    Select,
    GroupByExpr,
    SelectItem,
    TableWithJoins,
    TableFactor,
    ObjectType,
    AlterTableOperation as AstAlterTableOperation,
    ColumnDef,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

// Internal modules
use crate::db::sql_parser::utils::is_aggregate_expr;
use crate::db::common::{RsqlResult, RsqlError};

/// Represents the type of join operation.
#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner, // INNER JOIN
    Left,  // LEFT OUTER JOIN
    Right, // RIGHT OUTER JOIN
    Full,  // FULL OUTER JOIN
    Cross, // CROSS JOIN
}

/// Represents the type of apply operation for subqueries.
#[derive(Debug, Clone, Copy)]
pub enum ApplyType {
    Scalar,  // Scalar subquery
    In,      // IN subquery
    NotIn,   // NOT IN subquery
}

/// Represents operations for ALTER TABLE.
pub type AlterTableOperation = AstAlterTableOperation;

/// Represents a logical query plan.
/// Each variant corresponds to a relational algebra operation or DDL/DML operation.
#[derive(Debug)]
pub enum PlanNode {
    /// Scans a table for all rows.
    TableScan {
        table: String,
    },
    /// Represents a subquery.
    Subquery {
        subquery: Box<PlanNode>,
        alias: Option<String>,
    },
    /// Applies a subquery to each row from the input.
    Apply {
        input: Box<PlanNode>,
        subquery: Box<PlanNode>,
        apply_type: ApplyType,
    },
    /// Filters rows based on a predicate.
    Filter {
        predicate: Expr,
        input: Box<PlanNode>,
    },
    /// Groups rows and applies aggregate functions.
    Aggregate {
        group_by: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        input: Box<PlanNode>,
    },
    /// Projects specific columns from the input.
    Projection {
        exprs: Vec<Expr>,
        input: Box<PlanNode>,
    },
    /// Joins two plans based on a condition.
    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        on: Option<Expr>,
    },
    /// Creates a new table.
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
    },
    /// Alters an existing table.
    AlterTable {
        table_name: String,
        operation: AlterTableOperation,
    },
    /// Drops a table.
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    /// Creates an index on a table.
    /// Represents a CREATE INDEX statement.
    CreateIndex {
        index_name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
    },
    /// Inserts data into a table.
    /// If `input` is Some, it represents an INSERT ... SELECT subquery plan.
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
        input: Option<Box<PlanNode>>, // for INSERT ... SELECT subquery
    },
    /// Deletes rows produced by the input plan.
    Delete {
        input: Box<PlanNode>,
    },
    /// Updates rows produced by the input plan.
    Update {
        input: Box<PlanNode>,
        assignments: Vec<(String, Expr)>,
    },
}

#[derive(Debug)]
pub enum PlanItem {
    Statement(PlanNode),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug)]
pub struct Plan {
    pub items: Vec<PlanItem>,
}

impl Plan {
    /// Builds a logical plan from a SQL string.
    /// Flattens all statements into Plan.items, including transaction boundaries.
    pub fn build_plan(sql: &str) -> RsqlResult<Plan> {
        use sqlparser::ast::Statement::*;
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| RsqlError::ParserError(format!("{e}")))?;

        if ast.is_empty() {
            return Err(RsqlError::ParserError("Empty SQL".to_string()));
        }

        let mut items = Vec::new();

        for stmt in ast.iter() {
            match stmt {
                StartTransaction { .. } => items.push(PlanItem::Begin),
                Commit { .. } => items.push(PlanItem::Commit),
                Rollback { .. } => items.push(PlanItem::Rollback),
                _ => {
                    let node = Self::from_ast(stmt)?;
                    items.push(PlanItem::Statement(node));
                }
            }
        }

        Ok(Plan { items })
    }

    /// AST -> LogicalPlan
    fn from_ast(stmt: &Statement) -> RsqlResult<PlanNode> {
        match stmt {
            Statement::Query(_) => Self::from_select_ast(stmt),
            Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
            | Statement::CreateTable { .. }
            | Statement::Drop { .. }
            | Statement::AlterTable { .. }
            | Statement::CreateIndex { .. } => Self::from_ddl_ast(stmt),
            // Transaction statements are handled in build_plan, so treat as error here.
            Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. } => {
                Err(RsqlError::ParserError("Transaction statements are not valid as standalone logical plan nodes".to_string()))
            }
            _ => Err(RsqlError::ParserError("Unsupported statement type".to_string())),
        }
    }

    // ==================== SELECT ====================
    fn from_select_ast(stmt: &Statement) -> RsqlResult<PlanNode> {
        match stmt {
            Statement::Query(query) => Self::build_query(query),
            _ => Err(RsqlError::ParserError("Only SELECT supported".to_string())),
        }
    }

    fn build_query(query: &Query) -> RsqlResult<PlanNode> {
        match &*query.body {
            SetExpr::Select(select) => Self::build_select_plan(select),
            _ => Err(RsqlError::ParserError("Only simple SELECT is supported".to_string())),
        }
    }

    fn build_select_plan(select: &Select) -> RsqlResult<PlanNode> {
        let mut plan = Self::build_from(&select.from)?;

        if let Some(selection) = &select.selection {
            let (clean_predicate, sub_info) = Self::extract_subqueries_from_expr(selection)?;
            plan = PlanNode::Filter {
                predicate: clean_predicate,
                input: Box::new(plan),
            };
            if let Some((sub_plan, apply_type)) = sub_info {
                plan = PlanNode::Apply {
                    input: Box::new(plan),
                    subquery: Box::new(sub_plan),
                    apply_type,
                };
            }
        }

        if Self::has_grouping_or_aggregate(select) {
            let group_by = match &select.group_by {
                GroupByExpr::Expressions(exprs, _) => exprs.clone(),
                _ => vec![],
            };
            let aggr_exprs = Self::extract_aggr_exprs(&select.projection);
            plan = PlanNode::Aggregate {
                group_by,
                aggr_exprs,
                input: Box::new(plan),
            };
        }

        if let Some(having) = &select.having {
            let (clean_having, sub_info) = Self::extract_subqueries_from_expr(having)?;
            plan = PlanNode::Filter {
                predicate: clean_having,
                input: Box::new(plan),
            };
            if let Some((sub_plan, apply_type)) = sub_info {
                plan = PlanNode::Apply {
                    input: Box::new(plan),
                    subquery: Box::new(sub_plan),
                    apply_type,
                };
            }
        }

        // === Projection handling ===
        // Distinguish three cases:
        // 1) SELECT *        -> identity projection (no Projection node)
        // 2) SELECT a, b     -> Projection [a, b]
        // 3) SELECT <empty>  -> illegal (π[]), reject here

        let has_wildcard = select.projection.iter().any(|item| matches!(item, SelectItem::Wildcard(_)));
        let proj_exprs = Self::extract_projection(&select.projection);

        // Case 3: empty projection without wildcard is illegal
        if !has_wildcard && proj_exprs.is_empty() {
            return Err(RsqlError::ParserError(
                "SELECT list cannot be empty".to_string(),
            ));
        }

        // Case 2: explicit projection
        if !proj_exprs.is_empty() {
            let (clean_exprs, sub_info) =
                Self::extract_subqueries_from_exprs(&proj_exprs)?;
            plan = PlanNode::Projection {
                exprs: clean_exprs,
                input: Box::new(plan),
            };
            if let Some((sub_plan, apply_type)) = sub_info {
                plan = PlanNode::Apply {
                    input: Box::new(plan),
                    subquery: Box::new(sub_plan),
                    apply_type,
                };
            }
        }
        // Case 1 (SELECT *): do nothing, identity projection

        Ok(plan)
    }

    // ==================== FROM / TableFactor ====================
    fn build_from(from: &[TableWithJoins]) -> RsqlResult<PlanNode> {
        if from.is_empty() {
            return Err(RsqlError::ParserError("FROM clause is empty".to_string()));
        }

        // Start with the first table
        let mut plan = Self::build_table_factor(&from[0].relation)?;

        // Handle joins inside the first TableWithJoins
        for table_with_join in &from[0].joins {
            let right_plan = Self::build_table_factor(&table_with_join.relation)?;
            let join_type = match table_with_join.join_operator {
                sqlparser::ast::JoinOperator::Inner(_) => JoinType::Inner,
                sqlparser::ast::JoinOperator::LeftOuter(_) => JoinType::Left,
                sqlparser::ast::JoinOperator::RightOuter(_) => JoinType::Right,
                sqlparser::ast::JoinOperator::FullOuter(_) => JoinType::Full,
                sqlparser::ast::JoinOperator::CrossJoin(_) => JoinType::Cross,
                sqlparser::ast::JoinOperator::Join(_) => JoinType::Inner,
                _ => return Err(RsqlError::ParserError("Unsupported join type".to_string())),
            };
            let on_expr = match &table_with_join.join_operator {
                sqlparser::ast::JoinOperator::Inner(constraint)
                | sqlparser::ast::JoinOperator::LeftOuter(constraint)
                | sqlparser::ast::JoinOperator::RightOuter(constraint)
                | sqlparser::ast::JoinOperator::FullOuter(constraint)
                | sqlparser::ast::JoinOperator::Join(constraint)
                | sqlparser::ast::JoinOperator::CrossJoin(constraint) => {
                    match constraint {
                        sqlparser::ast::JoinConstraint::On(expr) => Some(expr.clone()),
                        _ => None,
                    }
                }
                _ => None,
            };
            plan = PlanNode::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type,
                on: on_expr,
            };
        }

        // Handle additional FROM items as implicit CROSS JOINs
        for table in from.iter().skip(1) {
            let right_plan = Self::build_table_factor(&table.relation)?;
            plan = PlanNode::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type: JoinType::Cross,
                on: None,
            };
        }

        Ok(plan)
    }

    fn build_table_factor(table_factor: &TableFactor) -> RsqlResult<PlanNode> {
        match table_factor {
            TableFactor::Table { name, .. } => Ok(PlanNode::TableScan { table: name.to_string() }),
            TableFactor::Derived { subquery, alias, .. } => {
                let sub_plan = Self::build_query(subquery)?;
                let alias_name = alias.as_ref().map(|a| a.name.to_string());
                Ok(PlanNode::Subquery { subquery: Box::new(sub_plan), alias: alias_name })
            }
            _ => Err(RsqlError::ParserError("Unsupported table factor".to_string())),
        }
    }

    fn extract_projection(items: &[SelectItem]) -> Vec<Expr> {
        items.iter().filter_map(|item| match item {
            SelectItem::UnnamedExpr(expr) => Some(expr.clone()),
            SelectItem::ExprWithAlias { expr, .. } => Some(expr.clone()),
            SelectItem::Wildcard(_) => None,
            _ => None,
        }).collect()
    }

    fn extract_aggr_exprs(items: &[SelectItem]) -> Vec<Expr> {
        items.iter().filter_map(|item| match item {
            SelectItem::UnnamedExpr(expr)
            | SelectItem::ExprWithAlias { expr, .. } => if is_aggregate_expr(expr) { Some(expr.clone()) } else { None },
            _ => None,
        }).collect()
    }

    fn has_grouping_or_aggregate(select: &Select) -> bool {
        matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty())
            || Self::extract_aggr_exprs(&select.projection).len() > 0
    }

    fn extract_subqueries_from_expr(expr: &Expr) -> RsqlResult<(Expr, Option<(PlanNode, ApplyType)>)> {
        match expr {
            Expr::Subquery(query) => {
                let plan = Self::build_query(query)?;
                Ok((expr.clone(), Some((plan, ApplyType::Scalar))))
            }
            Expr::InSubquery { subquery, negated, .. } => {
                let plan = Self::build_query(subquery)?;
                Ok((expr.clone(), Some((plan, if *negated { ApplyType::NotIn } else { ApplyType::In }))))
            }
            Expr::Exists { .. } => {
                Err(RsqlError::ParserError("EXISTS subquery is not supported".to_string()))
            }
            Expr::BinaryOp { left, op, right } => {
                let (left_clean, left_sub) = Self::extract_subqueries_from_expr(left)?;
                let (right_clean, right_sub) = Self::extract_subqueries_from_expr(right)?;
                Ok((
                    Expr::BinaryOp {
                        left: Box::new(left_clean),
                        op: op.clone(),
                        right: Box::new(right_clean),
                    },
                    left_sub.or(right_sub),
                ))
            }
            Expr::Function(func) if is_aggregate_expr(expr) => {
                Ok((expr.clone(), None))
            }
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Value(_)
            | Expr::Nested(_) => Ok((expr.clone(), None)),
            _ => Err(RsqlError::ParserError(format!("Unsupported expression: {}", expr))),
        }
    }

    fn extract_subqueries_from_exprs(exprs: &[Expr]) -> RsqlResult<(Vec<Expr>, Option<(PlanNode, ApplyType)>)> {
        let mut clean_exprs = Vec::new();
        let mut sub = None;

        for expr in exprs {
            let (clean, s) = Self::extract_subqueries_from_expr(expr)?;
            clean_exprs.push(clean);
            if sub.is_none() {
                sub = s;
            }
        }

        Ok((clean_exprs, sub))
    }

    // ==================== DDL / INSERT / UPDATE / DELETE ====================
    fn from_ddl_ast(stmt: &Statement) -> RsqlResult<PlanNode> {
        match stmt {
            Statement::CreateTable(create) => {
                // ========== Validation logic ==========
                use sqlparser::ast::{ColumnOption, DataType};
                for col in &create.columns {
                    let mut has_primary = false;
                    let mut has_unique = false;
                    let mut has_not_null = false;
                    let mut has_null = false;
                    for opt in &col.options {
                        match &opt.option {
                            ColumnOption::PrimaryKey(_) => {
                                has_primary = true;
                            }
                            ColumnOption::Unique(_) => {
                                has_unique = true;
                            }
                            ColumnOption::NotNull => {
                                has_not_null = true;
                            }
                            ColumnOption::Null => {
                                has_null = true;
                            }
                            _ => {}
                        }
                    }
                    // Only when both PRIMARY KEY and NULL are specified, it's an error.
                    if has_primary && has_null {
                        return Err(RsqlError::ParserError(format!(
                            "Column `{}` cannot be both PRIMARY KEY and NULL",
                            col.name
                        )));
                    }
                    // 2. VARCHAR-like columns cannot be PRIMARY KEY or UNIQUE.
                    match &col.data_type {
                        DataType::Varchar(_)
                        | DataType::Char(_)
                        | DataType::Character(_)
                        | DataType::CharacterVarying(_) => {
                            if has_primary || has_unique {
                                return Err(RsqlError::ParserError(format!(
                                    "VARCHAR column `{}` cannot be PRIMARY KEY or UNIQUE",
                                    col.name
                                )));
                            }
                        }
                        _ => {}
                    }
                }
                Ok(PlanNode::CreateTable {
                    table_name: create.name.to_string(),
                    columns: create.columns.clone(),
                })
            },
            Statement::AlterTable(alter) => {
                if alter.operations.len() != 1 {
                    return Err(RsqlError::ParserError("Multiple ALTER TABLE operations not supported".to_string()));
                }
                let op = &alter.operations[0];
                use sqlparser::ast::{AlterTableOperation, RenameTableNameKind};
                match op {
                    AlterTableOperation::RenameTable { table_name: new_name } => {
                        // For RENAME TABLE, keep RenameTableNameKind::To(obj_name) as is
                        Ok(PlanNode::AlterTable {
                            table_name: alter.name.to_string(),
                            operation: AlterTableOperation::RenameTable {
                                table_name: new_name.clone(),
                            },
                        })
                    }
                    AlterTableOperation::AddColumn { .. } => {
                        Err(RsqlError::ParserError("ALTER TABLE ADD COLUMN is not supported".to_string()))
                    }
                    AlterTableOperation::DropColumn { .. } => {
                        Err(RsqlError::ParserError("ALTER TABLE DROP COLUMN is not supported".to_string()))
                    }
                    AlterTableOperation::RenameColumn { .. } => {
                        Err(RsqlError::ParserError("ALTER TABLE RENAME COLUMN is not supported".to_string()))
                    }
                    AlterTableOperation::AlterColumn { op: sqlparser::ast::AlterColumnOperation::SetDataType { .. }, .. } => {
                        Err(RsqlError::ParserError("ALTER TABLE ALTER COLUMN TYPE is not supported".to_string()))
                    }
                    _ => Err(RsqlError::ParserError("ALTER TABLE operation is not supported".to_string())),
                }
            }
            Statement::Drop { object_type, names, if_exists, .. } => {
                if *object_type == ObjectType::Table && names.len() == 1 {
                    Ok(PlanNode::DropTable { table_name: names[0].to_string(), if_exists: *if_exists })
                } else { Err(RsqlError::ParserError("Only DROP TABLE supported".to_string())) }
            }
            Statement::CreateIndex(create_index) => {
                // Build a logical plan node for CREATE INDEX with safe unwrap for Option<ObjectName>
                let index_name = match &create_index.name {
                    Some(name) => name.to_string(),
                    None => return Err(RsqlError::ParserError("CREATE INDEX must have a name".to_string())),
                };
                Ok(PlanNode::CreateIndex {
                    index_name,
                    table_name: create_index.table_name.to_string(),
                    columns: create_index.columns.iter().map(|c| c.to_string()).collect(),
                    unique: create_index.unique,
                })
            }
            Statement::Insert(insert) => {
                if let Some(source) = &insert.source {
                    match &*source.body {
                        SetExpr::Values(values) => {
                            let rows: Vec<Vec<Expr>> = values.rows.iter()
                                .map(|row: &Vec<Expr>| row.iter().map(|expr: &Expr| expr.clone()).collect::<Vec<Expr>>())
                                .collect::<Vec<Vec<Expr>>>();
                            Ok(PlanNode::Insert {
                                table_name: insert.table.to_string(),
                                columns: if insert.columns.is_empty() { None } else { Some(insert.columns.iter().map(|c| c.to_string()).collect()) },
                                values: rows,
                                input: None,
                            })
                        },
                        SetExpr::Select(select) => {
                            let sub_plan = Self::build_select_plan(select)?;
                            Ok(PlanNode::Insert {
                                table_name: insert.table.to_string(),
                                columns: if insert.columns.is_empty() { None } else { Some(insert.columns.iter().map(|c| c.to_string()).collect()) },
                                values: vec![],
                                input: Some(Box::new(sub_plan)),
                            })
                        }
                        _ => return Err(RsqlError::ParserError("Unsupported INSERT source".to_string())),
                    }
                } else {
                    Ok(PlanNode::Insert {
                        table_name: insert.table.to_string(),
                        columns: if insert.columns.is_empty() { None } else { Some(insert.columns.iter().map(|c| c.to_string()).collect()) },
                        values: vec![],
                        input: None,
                    })
                }
            }
            Statement::Delete(delete) => {
                let table_name = match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables)
                    | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                        if tables.is_empty() {
                            return Err(RsqlError::ParserError("DELETE with no table".to_string()));
                        }
                        match &tables[0].relation {
                            TableFactor::Table { name, .. } => name.to_string(),
                            _ => {
                                return Err(RsqlError::ParserError(
                                    "Unsupported table factor in DELETE".to_string(),
                                ))
                            }
                        }
                    }
                };

                // Base scan
                let mut plan = PlanNode::TableScan { table: table_name };

                // WHERE clause → Filter (+ Apply if needed)
                if let Some(selection) = &delete.selection {
                    let (clean_pred, sub_info) =
                        Self::extract_subqueries_from_expr(selection)?;
                    plan = PlanNode::Filter {
                        predicate: clean_pred,
                        input: Box::new(plan),
                    };
                    if let Some((sub_plan, apply_type)) = sub_info {
                        plan = PlanNode::Apply {
                            input: Box::new(plan),
                            subquery: Box::new(sub_plan),
                            apply_type,
                        };
                    }
                }

                Ok(PlanNode::Delete {
                    input: Box::new(plan),
                })
            }
            Statement::Update(update) => {
                let table_name = update.table.to_string();

                // Base scan
                let mut plan = PlanNode::TableScan { table: table_name };

                // WHERE clause → Filter (+ Apply if needed)
                if let Some(selection) = &update.selection {
                    let (clean_pred, sub_info) =
                        Self::extract_subqueries_from_expr(selection)?;
                    plan = PlanNode::Filter {
                        predicate: clean_pred,
                        input: Box::new(plan),
                    };
                    if let Some((sub_plan, apply_type)) = sub_info {
                        plan = PlanNode::Apply {
                            input: Box::new(plan),
                            subquery: Box::new(sub_plan),
                            apply_type,
                        };
                    }
                }

                let assignments = update
                    .assignments
                    .iter()
                    .map(|a| (format!("{}", a.target), a.value.clone()))
                    .collect();

                Ok(PlanNode::Update {
                    input: Box::new(plan),
                    assignments,
                })
            }
            _ => Err(RsqlError::ParserError("DDL not implemented yet".to_string())),
        }
    }

    /// print LogicalPlan in a pretty tree format
    pub fn pretty_print(plan: &PlanNode) {
        fn fmt_exprs(exprs: &[Expr]) -> String {
            exprs.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ")
        }

        fn fmt_alter_op(op: &AlterTableOperation, table_name: &str) -> String {
            match op {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    format!(
                        "ADD COLUMN {} {}",
                        column_def.name,
                        column_def.data_type
                    )
                }
                AlterTableOperation::DropColumn { column_names, if_exists, .. } => {
                    let cols = column_names
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    if *if_exists {
                        format!("DROP COLUMN IF EXISTS {}", cols)
                    } else {
                        format!("DROP COLUMN {}", cols)
                    }
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    format!(
                        "RENAME COLUMN {} TO {}",
                        old_column_name,
                        new_column_name
                    )
                }
                AlterTableOperation::RenameTable { table_name: new_name } => {
                    // Only print the new table name (do not include "TO" or old name here)
                    format!("{}", new_name)
                }
                AlterTableOperation::AlterColumn { column_name, op } => {
                    match op {
                        sqlparser::ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                            format!(
                                "ALTER COLUMN {} TYPE {}",
                                column_name,
                                data_type
                            )
                        }
                        sqlparser::ast::AlterColumnOperation::SetNotNull => {
                            format!("ALTER COLUMN {} SET NOT NULL", column_name)
                        }
                        sqlparser::ast::AlterColumnOperation::DropNotNull => {
                            format!("ALTER COLUMN {} DROP NOT NULL", column_name)
                        }
                        _ => format!(
                            "ALTER COLUMN {} {:?}",
                            column_name,
                            op
                        ),
                    }
                }
                _ => format!("{:?}", op),
            }
        }

        fn inner(plan: &PlanNode, prefix: &str, is_last: bool) {
            let branch = if is_last { "└── " } else { "├── " };
            println!("{}{}{}", prefix, branch, label(plan));

            let new_prefix = if is_last { format!("{}    ", prefix) } else { format!("{}│   ", prefix) };
            for (i, child) in children(plan).iter().enumerate() {
                inner(child, &new_prefix, i + 1 == children(plan).len());
            }
        }

        fn label(plan: &PlanNode) -> String {
            match plan {
                PlanNode::TableScan { table } => format!("TableScan [{}]", table),
                PlanNode::Subquery { alias, .. } => format!("Subquery{}", alias.as_ref().map(|a| format!(" AS {}", a)).unwrap_or_default()),
                PlanNode::Apply { apply_type, .. } => format!("Apply [{:?}]", apply_type),
                PlanNode::Filter { predicate, .. } => format!("Filter [{}]", predicate),
                PlanNode::Aggregate { group_by, aggr_exprs, .. } => {
                    format!("Aggregate [group_by: {}, aggr: {}]", fmt_exprs(group_by), fmt_exprs(aggr_exprs))
                }
                PlanNode::Projection { exprs, .. } => format!("Projection [{}]", fmt_exprs(exprs)),
                PlanNode::Join { join_type, on, .. } => format!("Join [{:?}, on: {}]", join_type, on.as_ref().map_or("None".to_string(), |e| format!("{}", e))),
                PlanNode::CreateTable { table_name, columns } => {
                    // Enhanced: show column constraints (PRIMARY KEY, UNIQUE, NOT NULL, NULL) for each column
                    let mut col_labels = Vec::new();
                    for col in columns {
                        let mut constraints = Vec::new();
                        for opt in &col.options {
                            use sqlparser::ast::ColumnOption;
                            match &opt.option {
                                ColumnOption::PrimaryKey { .. } => constraints.push("PRIMARY KEY"),
                                ColumnOption::Unique { .. } => constraints.push("UNIQUE"),
                                ColumnOption::NotNull => constraints.push("NOT NULL"),
                                ColumnOption::Null => constraints.push("NULL"),
                                _ => {}
                            }
                        }
                        let col_str = if constraints.is_empty() {
                            format!("{}", col.name)
                        } else {
                            format!("{} [{}]", col.name, constraints.join(", "))
                        };
                        col_labels.push(col_str);
                    }
                    format!("CreateTable [{}] cols={}", table_name, col_labels.join(", "))
                }
                PlanNode::AlterTable { table_name, operation } => {
                    use sqlparser::ast::AlterTableOperation;
                    match operation {
                        AlterTableOperation::RenameTable { table_name: new_name } => {
                            // Only print both old and new table names, no "TO"
                            format!("AlterTable [{}] RENAME TABLE {} {}", table_name, table_name, new_name)
                        }
                        _ => format!("AlterTable [{}] {}", table_name, fmt_alter_op(operation, table_name)),
                    }
                }
                PlanNode::DropTable { table_name, if_exists } => {
                    if *if_exists { format!("DropTable [{}] IF EXISTS", table_name) } else { format!("DropTable [{}]", table_name) }
                }
                PlanNode::CreateIndex { index_name, table_name, columns, unique } => {
                    // Pretty print for CREATE INDEX logical plan node
                    let uniq_str = if *unique { "UNIQUE " } else { "" };
                    format!("CreateIndex [{}{}] on [{}] cols=[{}]", uniq_str, index_name, table_name, columns.join(", "))
                }
                PlanNode::Insert { table_name, columns, values, input } => {
                    if let Some(_) = input {
                        format!("Insert [{}] cols={:?} [Subquery]", table_name, columns)
                    } else {
                        let rows_str = values.iter().map(|row: &Vec<Expr>| {
                            row.iter().map(|e: &Expr| match e {
                                Expr::Subquery(_) => "[Subquery]".to_string(),
                                _ => format!("{}", e)
                            }).collect::<Vec<_>>().join(", ")
                        }).collect::<Vec<_>>().join(" | ");
                        format!("Insert [{}] cols={:?} rows={}", table_name, columns, rows_str)
                    }
                }
                PlanNode::Delete { .. } => "Delete".to_string(),
                PlanNode::Update { assignments, .. } => {
                    format!("Update [assigns={}]", assignments.len())
                }
            }
        }

        fn children(plan: &PlanNode) -> Vec<&PlanNode> {
            match plan {
                PlanNode::TableScan { .. } => vec![],
                PlanNode::Subquery { subquery, .. } => vec![subquery],
                PlanNode::Apply { input, subquery, .. } => vec![input, subquery],
                PlanNode::Filter { input, .. } => vec![input],
                PlanNode::Aggregate { input, .. } => vec![input],
                PlanNode::Projection { input, .. } => vec![input],
                PlanNode::Join { left, right, .. } => vec![left, right],
                PlanNode::Delete { input } => vec![input],
                PlanNode::Update { input, .. } => vec![input],
                PlanNode::Insert { input: Some(sub_plan), .. } => vec![sub_plan],
                _ => vec![],
            }
        }
        inner(plan, "", true);
    }
    
    /// print LogicalPlan in a pretty tree format, with expression field paths
    pub fn pretty_print_pro(plan: &PlanNode) {
        fn fmt_exprs(exprs: &[Expr]) -> String {
            exprs.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ")
        }

        fn fmt_alter_op(op: &AlterTableOperation, old_table_name: &str) -> String {
            use sqlparser::ast::RenameTableNameKind;
            match op {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    format!(
                        "ADD COLUMN {} {}",
                        column_def.name,
                        column_def.data_type
                    )
                }
                AlterTableOperation::DropColumn { column_names, if_exists, .. } => {
                    let cols = column_names
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    if *if_exists {
                        format!("DROP COLUMN IF EXISTS {}", cols)
                    } else {
                        format!("DROP COLUMN {}", cols)
                    }
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    format!(
                        "RENAME COLUMN {} TO {}",
                        old_column_name,
                        new_column_name
                    )
                }
                AlterTableOperation::RenameTable { table_name: new_name_kind } => {
                    // Print as: RENAME TABLE <old> TO <new>
                    match new_name_kind {
                        RenameTableNameKind::To(obj_name) => {
                            format!("RENAME TABLE {} TO {}", old_table_name, obj_name)
                        }
                        _ => {
                            // fallback
                            format!("RENAME TABLE {} TO {:?}", old_table_name, new_name_kind)
                        }
                    }
                }
                AlterTableOperation::AlterColumn { column_name, op } => {
                    match op {
                        sqlparser::ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                            format!(
                                "ALTER COLUMN {} TYPE {}",
                                column_name,
                                data_type
                            )
                        }
                        sqlparser::ast::AlterColumnOperation::SetNotNull => {
                            format!("ALTER COLUMN {} SET NOT NULL", column_name)
                        }
                        sqlparser::ast::AlterColumnOperation::DropNotNull => {
                            format!("ALTER COLUMN {} DROP NOT NULL", column_name)
                        }
                        _ => format!(
                            "ALTER COLUMN {} {:?}",
                            column_name,
                            op
                        ),
                    }
                }
                _ => format!("{:?}", op),
            }
        }

        // Print the PlanNode recursively, and for each PlanNode, print its expressions with paths.
        fn inner(plan: &PlanNode, prefix: &str, is_last: bool) {
            let branch = if is_last { "└── " } else { "├── " };
            println!("{}{}{}", prefix, branch, label(plan));
            // Print expressions with paths for this PlanNode.
            let expr_prefix = if is_last { format!("{}    ", prefix) } else { format!("{}│   ", prefix) };
            print_plan_expr_paths(plan, &expr_prefix, "");
            let new_prefix = expr_prefix.clone();
            for (i, child) in children(plan).iter().enumerate() {
                inner(child, &new_prefix, i + 1 == children(plan).len());
            }
        }

        // Print all expression fields of a PlanNode, with their field paths.
        fn print_plan_expr_paths(plan: &PlanNode, prefix: &str, plan_path: &str) {
            use sqlparser::ast::{AlterTableOperation, RenameTableNameKind};
            match plan {
                PlanNode::Filter { predicate, .. } => {
                    let path = format!("(PlanNode::Filter.predicate)");
                    print_expr_with_path(predicate, prefix, &path);
                }
                PlanNode::Aggregate { group_by, aggr_exprs, .. } => {
                    for (i, expr) in group_by.iter().enumerate() {
                        let path = format!("(PlanNode::Aggregate.group_by[{}])", i);
                        print_expr_with_path(expr, prefix, &path);
                    }
                    for (i, expr) in aggr_exprs.iter().enumerate() {
                        let path = format!("(PlanNode::Aggregate.aggr_exprs[{}])", i);
                        print_expr_with_path(expr, prefix, &path);
                    }
                }
                PlanNode::Projection { exprs, .. } => {
                    for (i, expr) in exprs.iter().enumerate() {
                        let path = format!("(PlanNode::Projection.exprs[{}])", i);
                        print_expr_with_path(expr, prefix, &path);
                    }
                }
                PlanNode::Join { on: Some(expr), .. } => {
                    let path = format!("(PlanNode::Join.on)");
                    print_expr_with_path(expr, prefix, &path);
                }
                PlanNode::Insert { values, .. } => {
                    for (row_idx, row) in values.iter().enumerate() {
                        for (col_idx, expr) in row.iter().enumerate() {
                            let path = format!("(PlanNode::Insert.values[{}][{}])", row_idx, col_idx);
                            print_expr_with_path(expr, prefix, &path);
                        }
                    }
                }
                PlanNode::Update { assignments, .. } => {
                    for (i, (_col, expr)) in assignments.iter().enumerate() {
                        let path = format!("(PlanNode::Update.assignments[{}].1)", i);
                        print_expr_with_path(expr, prefix, &path);
                    }
                }
                PlanNode::CreateTable { columns, .. } => {
                    // For each column, print its name and constraints
                    for (i, col) in columns.iter().enumerate() {
                        let name_path = format!("(PlanNode::CreateTable.columns[{}].name)", i);
                        println!("{}{} -> {}", prefix, name_path, col.name);
                        for (j, opt) in col.options.iter().enumerate() {
                            use sqlparser::ast::ColumnOption;
                            let opt_path = format!("(PlanNode::CreateTable.columns[{}].options[{}].option)", i, j);
                            let constraint = match &opt.option {
                                ColumnOption::PrimaryKey { .. } => "PRIMARY KEY",
                                ColumnOption::Unique { .. } => "UNIQUE",
                                ColumnOption::NotNull => "NOT NULL",
                                ColumnOption::Null => "NULL",
                                _ => continue,
                            };
                            println!("{}{} -> {}", prefix, opt_path, constraint);
                        }
                    }
                }
                PlanNode::CreateIndex { index_name, table_name, columns, unique } => {
                    let path_index = "(PlanNode::CreateIndex.index_name)";
                    println!("{}{} -> {}", prefix, path_index, index_name);
                    let path_table = "(PlanNode::CreateIndex.table_name)";
                    println!("{}{} -> {}", prefix, path_table, table_name);
                    let path_unique = "(PlanNode::CreateIndex.unique)";
                    println!("{}{} -> {}", prefix, path_unique, unique);
                    for (i, col) in columns.iter().enumerate() {
                        let path_col = format!("(PlanNode::CreateIndex.columns[{}])", i);
                        println!("{}{} -> {}", prefix, path_col, col);
                    }
                }
                PlanNode::AlterTable { table_name, operation } => {
                    // Print table_name and operation as detailed fields
                    let path_table = "(PlanNode::AlterTable.table_name)";
                    println!("{}{} -> {}", prefix, path_table, table_name);
                    match operation {
                        AlterTableOperation::RenameTable { table_name: new_name_kind } => {
                            // Print as two fields: old_table_name and new_table_name (only table names)
                            let old_path = "(PlanNode::AlterTable.operation.old_table_name)";
                            let new_path = "(PlanNode::AlterTable.operation.new_table_name)";
                            println!("{}{} -> {}", prefix, old_path, table_name);
                            match new_name_kind {
                                RenameTableNameKind::To(obj_name) => {
                                    println!("{}{} -> {}", prefix, new_path, obj_name);
                                }
                                _ => {
                                    println!("{}{} -> {:?}", prefix, new_path, new_name_kind);
                                }
                            }
                        }
                        _ => {
                            let path_op = "(PlanNode::AlterTable.operation)";
                            // Format operation as a string, e.g., "ADD COLUMN ..."
                            let op_str = fmt_alter_op(operation, table_name);
                            println!("{}{} -> {}", prefix, path_op, op_str);
                        }
                    }
                }
                _ => {}
            }
        }

        // Print an Expr, recursively, with its path (e.g., "(PlanNode::Filter.predicate)").
        fn print_expr_with_path(expr: &Expr, prefix: &str, path: &str) {
            // Print the path and the current expr node kind
            #[allow(unused_variables)]
            let _expr_label = expr_kind_label(expr);
            println!("{}{} -> {}", prefix, path, _expr_label);
            // For nested expressions, print their fields recursively with extended path
            match expr {
                Expr::BinaryOp { left, op, right } => {
                    let left_path = format!("{}.left", path);
                    print_expr_with_path(left, prefix, &left_path);
                    let op_path = format!("{}.op", path);
                    println!("{}{} -> {:?}", prefix, op_path, op);
                    let right_path = format!("{}.right", path);
                    print_expr_with_path(right, prefix, &right_path);
                }
                Expr::UnaryOp { op, expr: inner } => {
                    let op_path = format!("{}.op", path);
                    println!("{}{} -> {:?}", prefix, op_path, op);
                    let expr_path = format!("{}.expr", path);
                    print_expr_with_path(inner, prefix, &expr_path);
                }
                Expr::Nested(inner) => {
                    let inner_path = format!("{}.expr", path);
                    print_expr_with_path(inner, prefix, &inner_path);
                }
                Expr::Identifier(ident) => {
                    let val_path = format!("{}.ident", path);
                    println!("{}{} -> {:?}", prefix, val_path, ident);
                }
                Expr::Value(val) => {
                    let val_path = format!("{}.value", path);
                    println!("{}{} -> {:?}", prefix, val_path, val.value);
                }
                Expr::Function(func) => {
                    let name_path = format!("{}.name", path);
                    println!("{}{} -> {:?}", prefix, name_path, func.name);
                    // 不深入遍历函数参数，保持与原始代码一致，避免 FunctionArguments 兼容问题
                }
                Expr::Cast { expr: inner, data_type, .. } => {
                    let expr_path = format!("{}.expr", path);
                    print_expr_with_path(inner, prefix, &expr_path);
                    let dt_path = format!("{}.data_type", path);
                    println!("{}{} -> {:?}", prefix, dt_path, data_type);
                }
                Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
                    // Do not recurse into subqueries for now
                }
                Expr::CompoundIdentifier(idents) => {
                    let id_path = format!("{}.idents", path);
                    println!("{}{} -> {:?}", prefix, id_path, idents);
                }
                Expr::Between { expr: inner, low, high, .. } => {
                    let expr_path = format!("{}.expr", path);
                    print_expr_with_path(inner, prefix, &expr_path);
                    let low_path = format!("{}.low", path);
                    print_expr_with_path(low, prefix, &low_path);
                    let high_path = format!("{}.high", path);
                    print_expr_with_path(high, prefix, &high_path);
                }
                Expr::Case {
                    operand,
                    conditions,
                    else_result,
                    ..
                } => {
                    if let Some(opnd) = operand {
                        let opnd_path = format!("{}.operand", path);
                        print_expr_with_path(opnd, prefix, &opnd_path);
                    }
                    for (i, cw) in conditions.iter().enumerate() {
                        let cond_path = format!("{}.conditions[{}].condition", path, i);
                        print_expr_with_path(&cw.condition, prefix, &cond_path);
                        let res_path = format!("{}.conditions[{}].result", path, i);
                        print_expr_with_path(&cw.result, prefix, &res_path);
                    }
                    if let Some(else_res) = else_result {
                        let else_path = format!("{}.else_result", path);
                        print_expr_with_path(else_res, prefix, &else_path);
                    }
                }
                Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                    let inner_path = format!("{}.expr", path);
                    print_expr_with_path(inner, prefix, &inner_path);
                }
                _ => {}
            }
        }

        // Return a short label for an Expr node
        fn expr_kind_label(expr: &Expr) -> String {
            match expr {
                Expr::BinaryOp { .. } => format!("Expr::BinaryOp"),
                Expr::UnaryOp { .. } => format!("Expr::UnaryOp"),
                Expr::Value(val) => format!("Expr::Value({:?})", val.value),
                Expr::Identifier(ident) => format!("Expr::Identifier({})", ident),
                Expr::CompoundIdentifier(idents) => format!("Expr::CompoundIdentifier({:?})", idents),
                Expr::Nested(_) => format!("Expr::Nested"),
                Expr::Function(func) => format!("Expr::Function({})", func.name),
                Expr::Cast { .. } => format!("Expr::Cast"),
                Expr::Subquery(_) => format!("Expr::Subquery"),
                Expr::InSubquery { negated, .. } => format!("Expr::InSubquery(negated={})", negated),
                Expr::Exists { .. } => format!("Expr::Exists"),
                Expr::Between { .. } => format!("Expr::Between"),
                Expr::Case { .. } => format!("Expr::Case"),
                Expr::IsNull(_) => format!("Expr::IsNull"),
                Expr::IsNotNull(_) => format!("Expr::IsNotNull"),
                _ => format!("{:?}", expr),
            }
        }

        fn label(plan: &PlanNode) -> String {
            match plan {
                PlanNode::TableScan { table } => format!("TableScan [{}]", table),
                PlanNode::Subquery { alias, .. } => format!("Subquery{}", alias.as_ref().map(|a| format!(" AS {}", a)).unwrap_or_default()),
                PlanNode::Apply { apply_type, .. } => format!("Apply [{:?}]", apply_type),
                PlanNode::Filter { predicate, .. } => format!("Filter [{}]", predicate),
                PlanNode::Aggregate { group_by, aggr_exprs, .. } => {
                    format!("Aggregate [group_by: {}, aggr: {}]", fmt_exprs(group_by), fmt_exprs(aggr_exprs))
                }
                PlanNode::Projection { exprs, .. } => format!("Projection [{}]", fmt_exprs(exprs)),
                PlanNode::Join { join_type, on, .. } => format!("Join [{:?}, on: {}]", join_type, on.as_ref().map_or("None".to_string(), |e| format!("{}", e))),
                PlanNode::CreateTable { table_name, columns } => {
                    // Enhanced: show column constraints (UNIQUE, NOT NULL, NULL) for each column
                    let mut col_labels = Vec::new();
                    for col in columns {
                        let mut constraints = Vec::new();
                        for opt in &col.options {
                            use sqlparser::ast::ColumnOption;
                            match &opt.option {
                                ColumnOption::Unique { .. } => constraints.push("UNIQUE"),
                                ColumnOption::NotNull => constraints.push("NOT NULL"),
                                ColumnOption::Null => constraints.push("NULL"),
                                _ => {}
                            }
                        }
                        let col_str = if constraints.is_empty() {
                            format!("{}", col.name)
                        } else {
                            format!("{} [{}]", col.name, constraints.join(", "))
                        };
                        col_labels.push(col_str);
                    }
                    format!("CreateTable [{}] cols={}", table_name, col_labels.join(", "))
                }
                PlanNode::AlterTable { table_name, operation } => {
                    // For pretty_print_pro, print RENAME TABLE with old and new table names as separate fields
                    format!("AlterTable [{}] {}", table_name, fmt_alter_op(operation, table_name))
                }
                PlanNode::DropTable { table_name, if_exists } => {
                    if *if_exists { format!("DropTable [{}] IF EXISTS", table_name) } else { format!("DropTable [{}]", table_name) }
                }
                PlanNode::CreateIndex { index_name, table_name, columns, unique } => {
                    // Pretty print for CREATE INDEX logical plan node (pro version)
                    let uniq_str = if *unique { "UNIQUE " } else { "" };
                    format!("CreateIndex [{}{}] on [{}] cols=[{}]", uniq_str, index_name, table_name, columns.join(", "))
                }
                PlanNode::Insert { table_name, columns, values, input } => {
                    if let Some(_) = input {
                        format!("Insert [{}] cols={:?} [Subquery]", table_name, columns)
                    } else {
                        let rows_str = values.iter().map(|row: &Vec<Expr>| {
                            row.iter().map(|e: &Expr| match e {
                                Expr::Subquery(_) => "[Subquery]".to_string(),
                                Expr::Value(v) => format!("{:?}", v.value),
                                _ => format!("{}", e)
                            }).collect::<Vec<_>>().join(", ")
                        }).collect::<Vec<_>>().join(" | ");
                        format!("Insert [{}] cols={:?} rows={}", table_name, columns, rows_str)
                    }
                }
                PlanNode::Delete { .. } => "Delete".to_string(),
                PlanNode::Update { assignments, .. } => {
                    format!("Update [assigns={}]", assignments.len())
                }
            }
        }

        fn children(plan: &PlanNode) -> Vec<&PlanNode> {
            match plan {
                PlanNode::TableScan { .. } => vec![],
                PlanNode::Subquery { subquery, .. } => vec![subquery],
                PlanNode::Apply { input, subquery, .. } => vec![input, subquery],
                PlanNode::Filter { input, .. } => vec![input],
                PlanNode::Aggregate { input, .. } => vec![input],
                PlanNode::Projection { input, .. } => vec![input],
                PlanNode::Join { left, right, .. } => vec![left, right],
                PlanNode::Delete { input } => vec![input],
                PlanNode::Update { input, .. } => vec![input],
                PlanNode::Insert { input: Some(sub_plan), .. } => vec![sub_plan],
                _ => vec![],
            }
        }
        inner(plan, "", true);
    }
}