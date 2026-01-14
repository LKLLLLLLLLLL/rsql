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

// Represents the state of a transaction.
#[derive(Debug)]
pub enum TnxState {
    Commit,
    Rollback,
}

pub struct Tnx {
    pub stmts: Vec<PlanNode>,
    pub commit_stat: TnxState,
}

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
pub struct Plan {
    pub tnxs: Vec<Tnx>,
}

impl Plan {
    /// Builds a logical plan from a SQL string.
    /// Handles multi-statement SQL and transaction boundaries.
    pub fn build_plan(sql: &str) -> RsqlResult<Plan> {
        use sqlparser::ast::Statement::*;
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| RsqlError::ParserError(format!("{e}")))?;
        if ast.is_empty() {
            return Err(RsqlError::ParserError("Empty SQL".to_string()));
        }

        let mut tnxs: Vec<Tnx> = Vec::new();
        let mut curr_stmts: Vec<PlanNode> = Vec::new();
        let mut in_explicit_tnx = false;
        let mut curr_commit_stat = TnxState::Commit;
        let mut has_explicit_tnx = false;

        for stmt in ast.iter() {
            match stmt {
                StartTransaction { .. } => {
                    // Start a new transaction
                    if in_explicit_tnx {
                        return Err(RsqlError::ParserError(
                            "Nested BEGIN detected, a transaction is already open".to_string()
                        ));
                    }
                    in_explicit_tnx = true;
                    has_explicit_tnx = true;
                    curr_stmts.clear();
                    curr_commit_stat = TnxState::Commit;
                }
                Commit { .. } => {
                    if in_explicit_tnx {
                        tnxs.push(Tnx {
                            stmts: std::mem::take(&mut curr_stmts),
                            commit_stat: TnxState::Commit,
                        });
                        in_explicit_tnx = false;
                        curr_commit_stat = TnxState::Commit;
                    } else {
                        // COMMIT without BEGIN: treat as single transaction
                        tnxs.push(Tnx {
                            stmts: std::mem::take(&mut curr_stmts),
                            commit_stat: TnxState::Commit,
                        });
                        curr_commit_stat = TnxState::Commit;
                    }
                }
                Rollback { .. } => {
                    if in_explicit_tnx {
                        tnxs.push(Tnx {
                            stmts: std::mem::take(&mut curr_stmts),
                            commit_stat: TnxState::Rollback,
                        });
                        in_explicit_tnx = false;
                        curr_commit_stat = TnxState::Commit;
                    } else {
                        // ROLLBACK without BEGIN: treat as single transaction
                        tnxs.push(Tnx {
                            stmts: std::mem::take(&mut curr_stmts),
                            commit_stat: TnxState::Rollback,
                        });
                        curr_commit_stat = TnxState::Commit;
                    }
                }
                _ => {
                    // Normal SQL statement
                    let plan_node = Self::from_ast(stmt)?;
                    curr_stmts.push(plan_node);
                }
            }
        }

        // If any statements remain, wrap them as a transaction.
        if in_explicit_tnx {
            return Err(RsqlError::ParserError(
                "Explicit transaction not closed, missing COMMIT or ROLLBACK".to_string()
            ));
        }

        if !curr_stmts.is_empty() {
            tnxs.push(Tnx {
                stmts: std::mem::take(&mut curr_stmts),
                commit_stat: curr_commit_stat,
            });
        }

        // If there were no explicit transactions, but statements exist and no tnxs added, wrap all as one tnx.
        if tnxs.is_empty() && !curr_stmts.is_empty() {
            tnxs.push(Tnx {
                stmts: std::mem::take(&mut curr_stmts),
                commit_stat: TnxState::Commit,
            });
        }

        // If there were no explicit transactions (no BEGIN/COMMIT/ROLLBACK), but statements exist, wrap all as a default transaction.
        if !has_explicit_tnx && !tnxs.is_empty() {
            // Already handled by above, do nothing.
        }

        Ok(Plan { tnxs })
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
            | Statement::AlterTable { .. } => Self::from_ddl_ast(stmt),
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

        let (clean_exprs, sub_info) = Self::extract_subqueries_from_exprs(&Self::extract_projection(&select.projection))?;
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

        Ok(plan)
    }

    // ==================== FROM / TableFactor ====================
    fn build_from(from: &[TableWithJoins]) -> RsqlResult<PlanNode> {
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
            plan = PlanNode::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type,
                on: on_expr,
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
                Ok((
                    expr.clone(),
                    Some((
                        plan,
                        if *negated { ApplyType::NotIn } else { ApplyType::In },
                    )),
                ))
            }

            Expr::Exists { .. } => {
                Err(RsqlError::ParserError(
                    "EXISTS subquery is not supported".to_string(),
                ))
            }

            Expr::BinaryOp { left, op, right } => {
                let (left_clean, left_sub) =
                    Self::extract_subqueries_from_expr(left)?;
                let (right_clean, right_sub) =
                    Self::extract_subqueries_from_expr(right)?;

                Ok((
                    Expr::BinaryOp {
                        left: Box::new(left_clean),
                        op: op.clone(),
                        right: Box::new(right_clean),
                    },
                    left_sub.or(right_sub),
                ))
            }

            // Handle other expression types recursively as needed
            _ => Err(RsqlError::ParserError(
                format!("Unsupported expression: {}", expr),
            )),
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
            Statement::CreateTable(create) => Ok(PlanNode::CreateTable {
                table_name: create.name.to_string(),
                columns: create.columns.clone(),
            }),
            Statement::AlterTable(alter) => {
                if alter.operations.len() == 1 {
                    Ok(PlanNode::AlterTable { table_name: alter.name.to_string(), operation: alter.operations[0].clone() })
                } else { Err(RsqlError::ParserError("Multiple ALTER TABLE operations not supported".to_string())) }
            }
            Statement::Drop { object_type, names, if_exists, .. } => {
                if *object_type == ObjectType::Table && names.len() == 1 {
                    Ok(PlanNode::DropTable { table_name: names[0].to_string(), if_exists: *if_exists })
                } else { Err(RsqlError::ParserError("Only DROP TABLE supported".to_string())) }
            }
            Statement::Insert(insert) => {
                let values = if let Some(source) = &insert.source {
                    match &*source.body { SetExpr::Values(values) => values.rows.clone(), _ => return Err(RsqlError::ParserError("Only VALUES supported in INSERT".to_string())) }
                } else { vec![] };
                let cols_opt = if insert.columns.is_empty() { None } else { Some(insert.columns.iter().map(|c| c.to_string()).collect()) };
                Ok(PlanNode::Insert { table_name: insert.table.to_string(), columns: cols_opt, values })
            }
            Statement::Update(update) => {
                let assignments = update.assignments.iter().map(|a| (format!("{}", a.target), a.value.clone())).collect();
                Ok(PlanNode::Update { table_name: update.table.to_string(), assignments, predicate: update.selection.clone() })
            }
            Statement::Delete(delete) => {
                let table_name = match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables)
                    | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                        if tables.is_empty() { return Err(RsqlError::ParserError("DELETE with no table".to_string())); }
                        match &tables[0].relation { TableFactor::Table { name, .. } => name.to_string(), _ => return Err(RsqlError::ParserError("Unsupported table factor in DELETE".to_string())) }
                    }
                };
                Ok(PlanNode::Delete { table_name, predicate: delete.selection.clone() })
            }
            _ => Err(RsqlError::ParserError("DDL not implemented yet".to_string())),
        }
    }

    /// print LogicalPlan in a pretty tree format
    pub fn pretty_print(plan: &PlanNode) {
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
                PlanNode::CreateTable { table_name, columns } => format!("CreateTable [{}] cols={}", table_name, columns.iter().map(|c| c.name.to_string()).collect::<Vec<_>>().join(", ")),
                PlanNode::AlterTable { table_name, operation } => format!("AlterTable [{}] {}", table_name, fmt_alter_op(operation)),
                PlanNode::DropTable { table_name, if_exists } => {
                    if *if_exists { format!("DropTable [{}] IF EXISTS", table_name) } else { format!("DropTable [{}]", table_name) }
                }
                PlanNode::Insert { table_name, columns, values } => format!("Insert [{}] cols={:?} rows={}", table_name, columns, values.len()),
                PlanNode::Delete { table_name, predicate } => format!("Delete [{}] where={}", table_name, predicate.as_ref().map_or("None".to_string(), |p| format!("{}", p))),
                PlanNode::Update { table_name, assignments, predicate } => format!("Update [{}] assigns={} where={}", table_name, assignments.len(), predicate.as_ref().map_or("None".to_string(), |p| format!("{}", p))),
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
                _ => vec![],
            }
        }

        inner(plan, "", true);
    }
}