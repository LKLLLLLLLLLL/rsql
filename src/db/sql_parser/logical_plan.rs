/// Definitions for logical query plans.
/// This module defines the LogicalPlan enum and related structures for representing query execution plans.

use sqlparser::ast::{Expr, ColumnDef};
use sqlparser::ast::{AlterTableOperation as AstAlterTableOperation};

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