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
use crate::db::errors::{RsqlResult, RsqlError};

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
    Exists,  // EXISTS subquery
    In,      // IN subquery
    NotIn,   // NOT IN subquery
}

/// Represents operations for ALTER TABLE.
pub type AlterTableOperation = AstAlterTableOperation;

/// Represents a logical query plan.
/// Each variant corresponds to a relational algebra operation or DDL/DML operation.
#[derive(Debug)]
pub enum LogicalPlan {
    /// Scans a table for all rows.
    TableScan {
        table: String,
    },
    /// Represents a subquery.
    Subquery {
        subquery: Box<LogicalPlan>,
        alias: Option<String>,
    },
    /// Applies a subquery to each row from the input.
    Apply {
        input: Box<LogicalPlan>,
        subquery: Box<LogicalPlan>,
        apply_type: ApplyType,
    },
    /// Filters rows based on a predicate.
    Filter {
        predicate: Expr,
        input: Box<LogicalPlan>,
    },
    /// Groups rows and applies aggregate functions.
    Aggregate {
        group_by: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        input: Box<LogicalPlan>,
    },
    /// Projects specific columns from the input.
    Projection {
        exprs: Vec<Expr>,
        input: Box<LogicalPlan>,
    },
    /// Joins two plans based on a condition.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
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
    /// Inserts data into a table.
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
    },
    /// Deletes data from a table.
    Delete {
        table_name: String,
        predicate: Option<Expr>,
    },
    /// Updates data in a table.
    Update {
        table_name: String,
        assignments: Vec<(String, Expr)>,
        predicate: Option<Expr>,
    },
}

/// SQL -> LogicalPlan(IR)
pub struct LogicalPlanner;

impl LogicalPlanner {
    /// Builds a logical plan from a SQL string.
    pub fn build_logical_plan(sql: &str) -> RsqlResult<LogicalPlan> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| RsqlError::ParserError(format!("{e}")))?;
        if ast.is_empty() {
            return Err(RsqlError::ParserError("Empty SQL".to_string()));
        }
        Self::from_ast(&ast[0])
    }

    /// AST -> LogicalPlan
    fn from_ast(stmt: &Statement) -> RsqlResult<LogicalPlan> {
        match stmt {
            Statement::Query(_) => Self::from_select_ast(stmt),
            Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
            | Statement::CreateTable { .. }
            | Statement::Drop { .. }
            | Statement::AlterTable { .. } => Self::from_ddl_ast(stmt),
            _ => Err(RsqlError::ParserError("Unsupported statement type".to_string())),
        }
    }

    // ==================== SELECT ====================
    fn from_select_ast(stmt: &Statement) -> RsqlResult<LogicalPlan> {
        match stmt {
            Statement::Query(query) => Self::build_query(query),
            _ => Err(RsqlError::ParserError("Only SELECT supported".to_string())),
        }
    }

    fn build_query(query: &Query) -> RsqlResult<LogicalPlan> {
        match &*query.body {
            SetExpr::Select(select) => Self::build_select_plan(select),
            _ => Err(RsqlError::ParserError("Only simple SELECT is supported".to_string())),
        }
    }

    fn build_select_plan(select: &Select) -> RsqlResult<LogicalPlan> {
        let mut plan = Self::build_from(&select.from)?;

        if let Some(selection) = &select.selection {
            let (clean_predicate, sub_info) = Self::extract_subqueries_from_expr(selection);
            plan = LogicalPlan::Filter {
                predicate: clean_predicate,
                input: Box::new(plan),
            };
            if let Some((sub_plan, apply_type)) = sub_info {
                plan = LogicalPlan::Apply {
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
            plan = LogicalPlan::Aggregate {
                group_by,
                aggr_exprs,
                input: Box::new(plan),
            };
        }

        if let Some(having) = &select.having {
            let (clean_having, sub_info) = Self::extract_subqueries_from_expr(having);
            plan = LogicalPlan::Filter {
                predicate: clean_having,
                input: Box::new(plan),
            };
            if let Some((sub_plan, apply_type)) = sub_info {
                plan = LogicalPlan::Apply {
                    input: Box::new(plan),
                    subquery: Box::new(sub_plan),
                    apply_type,
                };
            }
        }

        let (clean_exprs, sub_info) = Self::extract_subqueries_from_exprs(&Self::extract_projection(&select.projection));
        plan = LogicalPlan::Projection {
            exprs: clean_exprs,
            input: Box::new(plan),
        };
        if let Some((sub_plan, apply_type)) = sub_info {
            plan = LogicalPlan::Apply {
                input: Box::new(plan),
                subquery: Box::new(sub_plan),
                apply_type,
            };
        }

        Ok(plan)
    }

    // ==================== FROM / TableFactor ====================
    fn build_from(from: &[TableWithJoins]) -> RsqlResult<LogicalPlan> {
        if from.is_empty() { return Err(RsqlError::ParserError("FROM clause is empty".to_string())); }
        let mut plan = Self::build_table_factor(&from[0].relation)?;
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
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type,
                on: on_expr,
            };
        }
        Ok(plan)
    }

    fn build_table_factor(table_factor: &TableFactor) -> RsqlResult<LogicalPlan> {
        match table_factor {
            TableFactor::Table { name, .. } => Ok(LogicalPlan::TableScan { table: name.to_string() }),
            TableFactor::Derived { subquery, alias, .. } => {
                let sub_plan = Self::build_query(subquery)?;
                let alias_name = alias.as_ref().map(|a| a.name.to_string());
                Ok(LogicalPlan::Subquery { subquery: Box::new(sub_plan), alias: alias_name })
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

    fn extract_subqueries_from_expr(expr: &Expr) -> (Expr, Option<(LogicalPlan, ApplyType)>) {
        match expr {
            Expr::Subquery(query) => {
                match Self::build_query(query) {
                    Ok(plan) => (expr.clone(), Some((plan, ApplyType::Scalar))),
                    Err(_) => (expr.clone(), None),
                }
            }
            Expr::InSubquery { expr: _, subquery, negated } => {
                match Self::build_query(subquery) {
                    Ok(plan) => (expr.clone(), Some((plan, if *negated { ApplyType::NotIn } else { ApplyType::In }))),
                    Err(_) => (expr.clone(), None),
                }
            }
            Expr::Exists { subquery, negated: _ } => {
                match Self::build_query(subquery) {
                    Ok(plan) => (expr.clone(), Some((plan, ApplyType::Exists))),
                    Err(_) => (expr.clone(), None),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let (left_clean, left_sub) = Self::extract_subqueries_from_expr(left);
                let (right_clean, right_sub) = Self::extract_subqueries_from_expr(right);
                let sub = left_sub.or(right_sub);
                (Expr::BinaryOp { left: Box::new(left_clean), op: op.clone(), right: Box::new(right_clean) }, sub)
            }
            _ => (expr.clone(), None),
        }
    }

    fn extract_subqueries_from_exprs(exprs: &[Expr]) -> (Vec<Expr>, Option<(LogicalPlan, ApplyType)>) {
        let mut clean_exprs = vec![];
        let mut sub = None;
        for expr in exprs {
            let (clean, s) = Self::extract_subqueries_from_expr(expr);
            clean_exprs.push(clean);
            if sub.is_none() { sub = s; }
        }
        (clean_exprs, sub)
    }

    // ==================== DDL / INSERT / UPDATE / DELETE ====================
    fn from_ddl_ast(stmt: &Statement) -> RsqlResult<LogicalPlan> {
        match stmt {
            Statement::CreateTable(create) => Ok(LogicalPlan::CreateTable {
                table_name: create.name.to_string(),
                columns: create.columns.clone(),
            }),
            Statement::AlterTable(alter) => {
                if alter.operations.len() == 1 {
                    Ok(LogicalPlan::AlterTable { table_name: alter.name.to_string(), operation: alter.operations[0].clone() })
                } else { Err(RsqlError::ParserError("Multiple ALTER TABLE operations not supported".to_string())) }
            }
            Statement::Drop { object_type, names, if_exists, .. } => {
                if *object_type == ObjectType::Table && names.len() == 1 {
                    Ok(LogicalPlan::DropTable { table_name: names[0].to_string(), if_exists: *if_exists })
                } else { Err(RsqlError::ParserError("Only DROP TABLE supported".to_string())) }
            }
            Statement::Insert(insert) => {
                let values = if let Some(source) = &insert.source {
                    match &*source.body { SetExpr::Values(values) => values.rows.clone(), _ => return Err(RsqlError::ParserError("Only VALUES supported in INSERT".to_string())) }
                } else { vec![] };
                let cols_opt = if insert.columns.is_empty() { None } else { Some(insert.columns.iter().map(|c| c.to_string()).collect()) };
                Ok(LogicalPlan::Insert { table_name: insert.table.to_string(), columns: cols_opt, values })
            }
            Statement::Update(update) => {
                let assignments = update.assignments.iter().map(|a| (format!("{}", a.target), a.value.clone())).collect();
                Ok(LogicalPlan::Update { table_name: update.table.to_string(), assignments, predicate: update.selection.clone() })
            }
            Statement::Delete(delete) => {
                let table_name = match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables)
                    | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                        if tables.is_empty() { return Err(RsqlError::ParserError("DELETE with no table".to_string())); }
                        match &tables[0].relation { TableFactor::Table { name, .. } => name.to_string(), _ => return Err(RsqlError::ParserError("Unsupported table factor in DELETE".to_string())) }
                    }
                };
                Ok(LogicalPlan::Delete { table_name, predicate: delete.selection.clone() })
            }
            _ => Err(RsqlError::ParserError("DDL not implemented yet".to_string())),
        }
    }

    /// print LogicalPlan in a pretty tree format
    pub fn pretty_print(plan: &LogicalPlan) {
        fn fmt_exprs(exprs: &[Expr]) -> String {
            exprs.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ")
        }

        fn fmt_alter_op(op: &AlterTableOperation) -> String {
            match op {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    format!("ADD COLUMN {} {}", column_def.name, column_def.data_type)
                }
                _ => format!("{:?}", op),
            }
        }

        fn inner(plan: &LogicalPlan, prefix: &str, is_last: bool) {
            let branch = if is_last { "└── " } else { "├── " };
            println!("{}{}{}", prefix, branch, label(plan));

            let new_prefix = if is_last { format!("{}    ", prefix) } else { format!("{}│   ", prefix) };
            for (i, child) in children(plan).iter().enumerate() {
                inner(child, &new_prefix, i + 1 == children(plan).len());
            }
        }

        fn label(plan: &LogicalPlan) -> String {
            match plan {
                LogicalPlan::TableScan { table } => format!("TableScan [{}]", table),
                LogicalPlan::Subquery { alias, .. } => format!("Subquery{}", alias.as_ref().map(|a| format!(" AS {}", a)).unwrap_or_default()),
                LogicalPlan::Apply { apply_type, .. } => format!("Apply [{:?}]", apply_type),
                LogicalPlan::Filter { predicate, .. } => format!("Filter [{}]", predicate),
                LogicalPlan::Aggregate { group_by, aggr_exprs, .. } => {
                    format!("Aggregate [group_by: {}, aggr: {}]", fmt_exprs(group_by), fmt_exprs(aggr_exprs))
                }
                LogicalPlan::Projection { exprs, .. } => format!("Projection [{}]", fmt_exprs(exprs)),
                LogicalPlan::Join { join_type, on, .. } => format!("Join [{:?}, on: {}]", join_type, on.as_ref().map_or("None".to_string(), |e| format!("{}", e))),
                LogicalPlan::CreateTable { table_name, columns } => format!("CreateTable [{}] cols={}", table_name, columns.iter().map(|c| c.name.to_string()).collect::<Vec<_>>().join(", ")),
                LogicalPlan::AlterTable { table_name, operation } => format!("AlterTable [{}] {}", table_name, fmt_alter_op(operation)),
                LogicalPlan::DropTable { table_name, if_exists } => {
                    if *if_exists { format!("DropTable [{}] IF EXISTS", table_name) } else { format!("DropTable [{}]", table_name) }
                }
                LogicalPlan::Insert { table_name, columns, values } => format!("Insert [{}] cols={:?} rows={}", table_name, columns, values.len()),
                LogicalPlan::Delete { table_name, predicate } => format!("Delete [{}] where={}", table_name, predicate.as_ref().map_or("None".to_string(), |p| format!("{}", p))),
                LogicalPlan::Update { table_name, assignments, predicate } => format!("Update [{}] assigns={} where={}", table_name, assignments.len(), predicate.as_ref().map_or("None".to_string(), |p| format!("{}", p))),
            }
        }

        fn children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
            match plan {
                LogicalPlan::TableScan { .. } => vec![],
                LogicalPlan::Subquery { subquery, .. } => vec![subquery],
                LogicalPlan::Apply { input, subquery, .. } => vec![input, subquery],
                LogicalPlan::Filter { input, .. } => vec![input],
                LogicalPlan::Aggregate { input, .. } => vec![input],
                LogicalPlan::Projection { input, .. } => vec![input],
                LogicalPlan::Join { left, right, .. } => vec![left, right],
                _ => vec![],
            }
        }

        inner(plan, "", true);
    }
}