pub mod select;
pub mod insert;
pub mod update;
pub mod delete;
pub mod ddl;

use sqlparser::ast::Statement;
use crate::parser::logical_plan::LogicalPlan;

pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::Query(_) => select::build_logical_plan(stmt),
        Statement::Insert { .. } => insert::build_logical_plan(stmt),
        Statement::Update { .. } => update::build_logical_plan(stmt),
        Statement::Delete { .. } => delete::build_logical_plan(stmt),
        Statement::CreateTable { .. }
        | Statement::Drop { .. }
        | Statement::AlterTable { .. } => ddl::build_logical_plan(stmt),
        _ => Err("Unsupported statement type".to_string()),
    }
}