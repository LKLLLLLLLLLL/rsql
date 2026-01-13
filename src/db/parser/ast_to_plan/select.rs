/// Module for building logical plans from SQL AST.
/// This module contains functions to convert parsed SQL statements into logical query plans.

use sqlparser::ast::{
    Statement, Query, SetExpr, Select,
    GroupByExpr, SelectItem, Expr, TableWithJoins
};
use crate::parser::expr_utils::is_aggregate_expr;
use crate::parser::logical_plan::{LogicalPlan, JoinType};

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
        plan = LogicalPlan::Filter {
            predicate: selection.clone(),
            input: Box::new(plan),
        };
    }

    // 3. GROUP BY / AGGREGATE
    if has_grouping_or_aggregate(select) {
        let group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => exprs.clone(),
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
        plan = LogicalPlan::Filter {
            predicate: having.clone(),
            input: Box::new(plan),
        };
    }

    // 4. PROJECTION
    Ok(LogicalPlan::Projection {
        exprs: extract_projection(&select.projection),
        input: Box::new(plan),
    })
}

/// TODO: Extend to support JOINs by converting TableWithJoins into LogicalPlan::Join
/// Builds a logical plan for the FROM clause.
/// Currently supports only single-table queries.
fn build_from(from: &[TableWithJoins]) -> Result<LogicalPlan, String> {
    if from.is_empty() {
        return Err("FROM clause is empty".to_string());
    }

    // First table
    let mut plan = LogicalPlan::TableScan {
        table: from[0].relation.to_string(),
    };

    // Afterwards JOIN
    for table_with_join in &from[0].joins {
        let right_plan = LogicalPlan::TableScan {
            table: table_with_join.relation.to_string(),
        };

        let join_type = match table_with_join.join_operator {
            sqlparser::ast::JoinOperator::Inner(_) => JoinType::Inner,
            sqlparser::ast::JoinOperator::LeftOuter(_) => JoinType::Left,
            sqlparser::ast::JoinOperator::RightOuter(_) => JoinType::Right,
            sqlparser::ast::JoinOperator::FullOuter(_) => JoinType::Full,
            sqlparser::ast::JoinOperator::CrossJoin(_) => JoinType::Cross,
            sqlparser::ast::JoinOperator::Join(_) => JoinType::Inner, // implicit JOIN is INNER
            _ => return Err("Unsupported join type".to_string()),
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
    matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty())
        || extract_aggr_exprs(&select.projection).len() > 0
}