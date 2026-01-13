use sqlparser::ast::Statement;
use crate::db::sql_parser::logical_plan::LogicalPlan;

pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::Update { table, assignments, selection, .. } => {
            let assignments = assignments.iter().map(|assignment| {
                (assignment.id[0].to_string(), assignment.value.clone())
            }).collect();
            Ok(LogicalPlan::Update {
                table_name: table.to_string(),
                assignments,
                predicate: selection.clone(),
            })
        }
        _ => Err("UPDATE not implemented yet".to_string()),
    }
}