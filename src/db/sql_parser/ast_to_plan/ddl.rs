use sqlparser::ast::{Statement, ObjectType};
use crate::db::sql_parser::logical_plan::{LogicalPlan};

use sqlparser::ast::AlterTableOperation as AstAlterOp;

pub fn build_logical_plan(stmt: &Statement) -> Result<LogicalPlan, String> {
    match stmt {
        Statement::CreateTable { name, columns, .. } => {
            Ok(LogicalPlan::CreateTable {
                table_name: name.to_string(),
                columns: columns.clone(),
            })
        }
        Statement::AlterTable { name, operations, .. } => {
            if operations.len() == 1 {
                let op = operations[0].clone();
                // store the raw AST AlterTableOperation in the logical plan
                let operation: AstAlterOp = op;
                Ok(LogicalPlan::AlterTable {
                    table_name: name.to_string(),
                    operation,
                })
            } else {
                Err("Multiple ALTER TABLE operations not supported".to_string())
            }
        }
        Statement::Drop { object_type, names, .. } => {
            if *object_type == ObjectType::Table && names.len() == 1 {
                Ok(LogicalPlan::DropTable {
                    table_name: names[0].to_string(),
                })
            } else {
                Err("Only DROP TABLE supported".to_string())
            }
        }
        _ => Err("DDL not implemented yet".to_string()),
    }
}