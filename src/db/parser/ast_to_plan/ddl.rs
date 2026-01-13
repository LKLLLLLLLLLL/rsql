use sqlparser::ast::{Statement, ObjectType};
use crate::parser::logical_plan::{LogicalPlan};

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