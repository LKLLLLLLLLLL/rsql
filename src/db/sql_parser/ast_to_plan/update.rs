use sqlparser::ast::{Statement, Update};
use crate::db::sql_parser::logical_plan::LogicalPlan;

pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::Update(update) => {
            let assignments = update.assignments.iter().map(|assignment| {
                (format!("{}", assignment.target), assignment.value.clone())
            }).collect();
            Ok(LogicalPlan::Update {
                table_name: update.table.to_string(),
                assignments,
                predicate: update.selection.clone(),
            })
        }
        _ => Err("UPDATE not implemented yet".to_string()),
    }
}