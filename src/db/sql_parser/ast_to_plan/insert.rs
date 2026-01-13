use sqlparser::ast::{Statement, SetExpr};
use crate::db::sql_parser::logical_plan::LogicalPlan;

pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::Insert { table, columns, source, .. } => {
            let values = if let Some(source) = source {
                match &*source.body {
                    SetExpr::Values(values) => values.rows.clone(),
                    _ => return Err("Only VALUES supported in INSERT".to_string()),
                }
            } else {
                vec![]
            };

            let cols_opt = if columns.is_empty() {
                None
            } else {
                Some(columns.iter().map(|c| c.to_string()).collect())
            };

            Ok(LogicalPlan::Insert {
                table_name: table.to_string(),
                columns: cols_opt,
                values,
            })
        }
        _ => Err("INSERT not implemented yet".to_string()),
    }
}