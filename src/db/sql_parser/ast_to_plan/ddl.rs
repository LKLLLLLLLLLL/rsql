use sqlparser::ast::{Statement, ObjectType, Insert, Delete, Update, SetExpr};
use crate::db::sql_parser::logical_plan::{LogicalPlan};

use sqlparser::ast::AlterTableOperation as AstAlterOp;

pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::CreateTable(create) => {
            Ok(LogicalPlan::CreateTable {
                table_name: create.name.to_string(),
                columns: create.columns.clone(),
            })
        }
        Statement::AlterTable(alter) => {
            if alter.operations.len() == 1 {
                let op = alter.operations[0].clone();
                // store the raw AST AlterTableOperation in the logical plan
                let operation: AstAlterOp = op;
                Ok(LogicalPlan::AlterTable {
                    table_name: alter.name.to_string(),
                    operation,
                })
            } else {
                Err("Multiple ALTER TABLE operations not supported".to_string())
            }
        }
        Statement::Drop { object_type, names, if_exists, .. } => {
            if *object_type == ObjectType::Table && names.len() == 1 {
                Ok(LogicalPlan::DropTable {
                    table_name: names[0].to_string(),
                    if_exists: *if_exists,
                })
            } else {
                Err("Only DROP TABLE supported".to_string())
            }
        }
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
        Statement::Delete(delete) => {
            let table_name = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables) | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                    if tables.is_empty() {
                        return Err("DELETE with no table".to_string());
                    }
                    match &tables[0].relation {
                        sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
                        _ => return Err("Unsupported table factor in DELETE".to_string()),
                    }
                }
            };
            Ok(LogicalPlan::Delete {
                table_name,
                predicate: delete.selection.clone(),
            })
        }
        _ => Err("DDL not implemented yet".to_string()),
    }
}