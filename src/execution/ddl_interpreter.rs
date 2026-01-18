use sqlparser::ast::{ColumnDef, DataType, ObjectName};

use crate::catalog::SysCatalog;
use crate::common::{RsqlResult, RsqlError};
use crate::sql::plan::{PlanNode};
use crate::storage::Table;
use super::result::{ExecutionResult::{self, Ddl}};
use tracing::info;

fn create_table(tnx_id: u64, table_name: &str, columns: &Vec<ColumnDef>) -> RsqlResult<ExecutionResult> {
    todo!()
}

/// table and index relevant sql statements
pub fn execute_ddl_plan_node(node: &PlanNode, tnx_id: u64) -> RsqlResult<ExecutionResult> {
    match node {
        PlanNode::CreateTable { table_name, columns, if_not_exists} => {
            create_table(tnx_id, table_name, columns)
        },
        PlanNode::AlterTable {table_name, operation} => {
            todo!()
        },
        PlanNode::DropTable { table_name, if_exists} => {
            // check if table exists
            let table_id = SysCatalog::global().get_table_id(tnx_id, table_name)?;
            if table_id.is_none() {
                if *if_exists {
                    info!("Table {} does not exist, skipping drop table.", table_name);
                    return Ok(Ddl(format!("Table {} does not exist, skipping drop table.", table_name)));
                } else {
                    return Err(RsqlError::ExecutionError(format!("Table {} does not exist.", table_name)));
                }
            }
            // drop table itself first
            let table_id = table_id.unwrap();
            let table_schema = SysCatalog::global().get_table_schema(tnx_id, table_id)?;
            let table = Table::from(table_id, table_schema, false)?;
            table.drop(tnx_id)?;
            // unregister table from sys catalog
            SysCatalog::global().unregister_table(tnx_id, table_id)?;
            Ok(Ddl(format!("Table {} dropped successfully.", table_name)))
        },
        PlanNode::CreateIndex {
            index_name,
            table_name,
            column,
            unique,
            if_not_exists: bool,
        } => {
            // get table id
            let table_id = SysCatalog::global().get_table_id(tnx_id, table_name)?;
            if table_id.is_none() {
                return Err(RsqlError::ExecutionError(format!("Table {} does not exist.", table_name)));
            }
            let table_id = table_id.unwrap();
            // get table object
            let table_schema = SysCatalog::global().get_table_schema(tnx_id, table_id)?;
            let mut table = Table::from(table_id, table_schema, false)?;
            // check if unique constraint is violated
            if *unique {
                // find column index first
                let mut col_index = None;
                for (i, col) in table.get_schema().get_columns().iter().enumerate() {
                    if &col.name == column {
                        col_index = Some(i);
                        break;
                    }
                }
                if col_index.is_none() {
                    return Err(RsqlError::InvalidInput(format!("Column {} does not exist in table {}.", column, table_name)));
                }
                let col_index = col_index.unwrap();
                let mut value_set = std::collections::HashSet::new();
                for row in table.get_all_rows()? {
                    let row = row?;
                    let value = &row[col_index];
                    if value_set.contains(value) {
                        return Err(RsqlError::ExecutionError(format!(
                            "Unique constraint violated when creating unique index {} on table {}.",
                            index_name, table_name
                        )));
                    }
                    value_set.insert(value.clone());
                }
            }
            // create index on table
            table.creat_index(column, tnx_id);
            // register index in sys catalog
            SysCatalog::global().register_index(
                tnx_id,
                table_id,
                column,
                index_name,
                *unique,
            )?;
            Ok(Ddl(format!("Index {} created successfully on table {}.", index_name, table_name)))
        },
        _ => {
            panic!("Unsupported DDL operation")
        }
    }
}