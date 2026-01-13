use sqlparser::ast::{Statement, Insert, SetExpr};
use crate::db::sql_parser::logical_plan::LogicalPlan;

pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::Insert(insert) => {
            let values = if let Some(source) = &insert.source {
                match &*source.body {
                    SetExpr::Values(values) => values.rows.clone(),
                    _ => return Err("Only VALUES supported in INSERT".to_string()),
                }
            } else {
                vec![]
            };

            let cols_opt = if insert.columns.is_empty() {
                None
            } else {
                Some(insert.columns.iter().map(|c| c.to_string()).collect())
            };

            Ok(LogicalPlan::Insert {
                table_name: insert.table.to_string(),
                columns: cols_opt,
                values,
            })
        }
        _ => Err("INSERT not implemented yet".to_string()),
    }
}