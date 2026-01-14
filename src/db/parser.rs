use sqlparser::{dialect::GenericDialect, parser::Parser};
use crate::db::sql_parser::ast_to_plan::build_logical_plan;
use crate::db::sql_parser::logical_plan::LogicalPlan;

/// Parses a SQL string into an Intermediate Representation (LogicalPlan).
///
/// This function takes a SQL query string, parses it using the SQL parser,
/// and converts it into a LogicalPlan which represents the query's logical structure.
///
/// # Arguments
/// * `sql` - A string slice containing the SQL query to parse.
///
/// # Returns
/// * `Result<LogicalPlan, String>` - The logical plan on success, or an error message on failure.
///

pub fn parse_sql_to_ir(sql: &str) -> Result<LogicalPlan, String> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

    if ast.len() != 1 {
        return Err("Only single statement supported".to_string());
    }

    let stmt = &ast[0];
    build_logical_plan(stmt)
}
