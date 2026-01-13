/// Utility functions for working with SQL expressions.
/// This module provides helper functions to analyze and manipulate SQL expressions.

use sqlparser::ast::Expr;

pub fn is_aggregate_expr(expr: &Expr) -> bool {
    matches!(expr,
        Expr::Function(func)
        if matches!(func.name.to_string().to_uppercase().as_str(),
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
        )
    )
}