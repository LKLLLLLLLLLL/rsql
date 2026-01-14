pub mod select;
pub mod ddl;

use sqlparser::ast::Statement;
use crate::db::sql_parser::logical_plan::LogicalPlan;

pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::Query(_) => select::build_logical_plan(stmt),
        Statement::Insert { .. }
        | Statement::Update { .. }
        | Statement::Delete { .. }
        | Statement::CreateTable { .. }
        | Statement::Drop { .. }
        | Statement::AlterTable { .. } => ddl::build_logical_plan(stmt),
        _ => Err("Unsupported statement type".to_string()),
    }
}