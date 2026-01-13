use sqlparser::ast::{Statement, Delete};
use crate::db::sql_parser::logical_plan::LogicalPlan;

pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::Delete(delete) => {
            if delete.tables.is_empty() {
                return Err("DELETE with no table".to_string());
            }
            let table_name = delete.tables[0].to_string();
            Ok(LogicalPlan::Delete {
                table_name,
                predicate: delete.selection.clone(),
            })
        }
        _ => Err("DELETE not implemented yet".to_string()),
    }
}