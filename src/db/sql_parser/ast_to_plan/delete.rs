use sqlparser::ast::Statement;
use crate::db::sql_parser::logical_plan::LogicalPlan;

pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::Delete { tables, selection, .. } => {
            if tables.is_empty() {
                return Err("DELETE with no table".to_string());
            }
            let table_name = tables[0].to_string();
            Ok(LogicalPlan::Delete {
                table_name,
                predicate: selection.clone(),
            })
        }
        _ => Err("DELETE not implemented yet".to_string()),
    }
}