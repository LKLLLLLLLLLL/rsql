/// Module for building logical plans from SQL AST.
/// This module contains functions to convert parsed SQL statements into logical query plans.

use sqlparser::ast::{
    Statement, Query, SetExpr, Select,
    GroupByExpr, SelectItem, Expr, TableWithJoins, TableFactor, Value
};
use crate::db::sql_parser::expr_utils::is_aggregate_expr;
use crate::db::sql_parser::logical_plan::{LogicalPlan, JoinType, ApplyType};

/// Builds a logical plan from a SQL statement.
/// Currently supports only SELECT queries.
pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::Query(query) => build_query(query),
        _ => Err("Only SELECT supported".to_string()),
    }
}

/// Builds a logical plan from a query.
/// Handles the query body, currently only SELECT statements.
fn build_query(query: &Query) -> Result<LogicalPlan, String> {
    match &*query.body {
        SetExpr::Select(select) => build_select(select),
        _ => Err("only simple SELECT is supported".to_string()),
    }
}

/// Builds a logical plan from a SELECT statement.
/// Applies the logical operators in the correct order: FROM, WHERE, GROUP BY/AGGREGATE, HAVING, PROJECTION.
fn build_select(select: &Select) -> Result<LogicalPlan, String> {
    // 1. FROM clause
    let mut plan = build_from(&select.from)?;

    // 2. WHERE clause
    if let Some(selection) = &select.selection {
        let (clean_predicate, sub_info) = extract_subqueries_from_expr(selection);
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

    // 3. GROUP BY / AGGREGATE
    if has_grouping_or_aggregate(select) {
        let group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs) => exprs.clone(),
            _ => vec![],
        };
        let aggr_exprs = extract_aggr_exprs(&select.projection);

        plan = LogicalPlan::Aggregate {
            group_by,
            aggr_exprs,
            input: Box::new(plan),
        };
    }

    // HAVING clause
    if let Some(having) = &select.having {
        let (clean_having, sub_info) = extract_subqueries_from_expr(having);
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

    // 4. PROJECTION
    let (clean_exprs, sub_info) = extract_subqueries_from_exprs(&extract_projection(&select.projection));
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
/// Builds a logical plan for the FROM clause.
/// Currently supports only single-table queries.
fn build_from(from: &[TableWithJoins]) -> Result<LogicalPlan, String> {
    if from.is_empty() {
        return Err("FROM clause is empty".to_string());
    }

    // First table
    let mut plan = build_table_factor(&from[0].relation)?;

    // Afterwards JOIN
    for table_with_join in &from[0].joins {
        let right_plan = build_table_factor(&table_with_join.relation)?;

        let join_type = match table_with_join.join_operator {
            sqlparser::ast::JoinOperator::Inner(_) => JoinType::Inner,
            sqlparser::ast::JoinOperator::LeftOuter(_) => JoinType::Left,
            sqlparser::ast::JoinOperator::RightOuter(_) => JoinType::Right,
            sqlparser::ast::JoinOperator::FullOuter(_) => JoinType::Full,
            sqlparser::ast::JoinOperator::CrossJoin => JoinType::Cross,
            _ => return Err("Unsupported join type".to_string()),
        };

        let on_expr = match &table_with_join.join_operator {
            sqlparser::ast::JoinOperator::Inner(constraint)
            | sqlparser::ast::JoinOperator::LeftOuter(constraint)
            | sqlparser::ast::JoinOperator::RightOuter(constraint)
            | sqlparser::ast::JoinOperator::FullOuter(constraint) => {
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

/// Extracts projection expressions from SELECT items.
/// Filters out wildcards and returns the expressions to be projected.
fn extract_projection(items: &[SelectItem]) -> Vec<Expr> {
    items.iter().filter_map(|item| {
        match item {
            SelectItem::UnnamedExpr(expr) => Some(expr.clone()),
            SelectItem::ExprWithAlias { expr, .. } => Some(expr.clone()),
            SelectItem::Wildcard(_) => None, // SELECT *
            _ => None,
        }
    }).collect()
}

/// Extracts aggregate expressions from SELECT items.
/// Identifies and collects expressions that are aggregate functions.
fn extract_aggr_exprs(items: &[SelectItem]) -> Vec<Expr> {
    items.iter().filter_map(|item| {
        match item {
            SelectItem::UnnamedExpr(expr)
            | SelectItem::ExprWithAlias { expr, .. } => {
                if is_aggregate_expr(expr) {
                    Some(expr.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }).collect()
}

/// Checks if the SELECT statement has GROUP BY or aggregate functions.
/// This determines whether an Aggregate node should be added to the logical plan.
fn has_grouping_or_aggregate(select: &Select) -> bool {
    matches!(&select.group_by, GroupByExpr::Expressions(exprs) if !exprs.is_empty())
        || extract_aggr_exprs(&select.projection).len() > 0
}

/// Builds a logical plan for a table factor (table or subquery).
fn build_table_factor(table_factor: &TableFactor) -> Result<LogicalPlan, String> {
    match table_factor {
        TableFactor::Table { name, .. } => {
            Ok(LogicalPlan::TableScan { table: name.to_string() })
        }
        TableFactor::Derived { subquery, alias, .. } => {
            let sub_plan = build_query(subquery)?;
            let alias_name = alias.as_ref().map(|a| a.name.to_string());
            Ok(LogicalPlan::Subquery {
                subquery: Box::new(sub_plan),
                alias: alias_name,
            })
        }
        _ => Err("Unsupported table factor".to_string()),
    }
}

/// Extracts subqueries from an expression and returns the cleaned expression and optional subquery info.
fn extract_subqueries_from_expr(expr: &Expr) -> (Expr, Option<(LogicalPlan, ApplyType)>) {
    match expr {
        Expr::Subquery(query) => {
            // For scalar subquery, build the plan
            match build_query(query) {
                Ok(plan) => (expr.clone(), Some((plan, ApplyType::Scalar))), // Keep original expr, extract subquery
                Err(_) => (expr.clone(), None),
            }
        }
        Expr::InSubquery { expr: in_expr, subquery, negated } => {
            // For IN/NOT IN subquery, build the plan
            match build_query(subquery) {
                Ok(plan) => {
                    let kind = if *negated { ApplyType::NotIn } else { ApplyType::In };
                    (expr.clone(), Some((plan, kind))) // Keep original expr
                }
                Err(_) => (expr.clone(), None),
            }
        }
        Expr::Exists { subquery, negated } => {
            // For EXISTS/NOT EXISTS subquery, build the plan
            match build_query(subquery) {
                Ok(plan) => (expr.clone(), Some((plan, ApplyType::Exists))), // Keep original expr
                Err(_) => (expr.clone(), None),
            }
        }
        // For other expressions, recursively extract from subexpressions
        Expr::BinaryOp { left, op, right } => {
            let (left_clean, left_sub) = extract_subqueries_from_expr(left);
            let (right_clean, right_sub) = extract_subqueries_from_expr(right);
            // For simplicity, take the first subquery found
            let sub = left_sub.or(right_sub);
            (Expr::BinaryOp { left: Box::new(left_clean), op: op.clone(), right: Box::new(right_clean) }, sub)
        }
        // Add more cases as needed, but for simplicity, handle common ones
        _ => (expr.clone(), None),
    }
}

/// Extracts subqueries from a list of expressions.
fn extract_subqueries_from_exprs(exprs: &[Expr]) -> (Vec<Expr>, Option<(LogicalPlan, ApplyType)>) {
    let mut clean_exprs = vec![];
    let mut sub = None;
    for expr in exprs {
        let (clean, s) = extract_subqueries_from_expr(expr);
        clean_exprs.push(clean);
        if sub.is_none() {
            sub = s;
        }
        // For simplicity, take the first subquery
    }
    (clean_exprs, sub)
}