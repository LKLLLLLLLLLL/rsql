use crate::catalog::{SysCatalog, sys_catalog};
use crate::common::{RsqlResult, RsqlError};
use crate::sql::plan::{PlanNode};
use crate::sql::plan::DdlOperation;
use crate::storage::Table;
use super::result::{ExecutionResult::{self, Ddl}};
use tracing::info;

/// table and index relevant sql statements
pub fn execute_ddl_plan_node(node: &PlanNode, tnx_id: u64) -> RsqlResult<ExecutionResult> {
    let PlanNode::DDL { op } = node else {
        return Err(RsqlError::InvalidInput("Not a DDL plan node".to_string()));
    };
    match op {
        DdlOperation::CreateTable { table_name, schema, if_not_exists} => {
            // check if table exists
            let table_id = SysCatalog::global().get_table_id(tnx_id, table_name)?;
            if table_id.is_some() {
                if *if_not_exists {
                    info!("Table {} already exists, skipping create table.", table_name);
                    return Ok(Ddl(format!("Table {} already exists, skipping create table.", table_name)));
                } else {
                    return Err(RsqlError::ExecutionError(format!("Table {} already exists.", table_name)));
                }
            }
            // register table in sys catalog
            let table_id = SysCatalog::global().register_table(tnx_id, table_name, &schema)?;
            // create table object
            let _ = Table::create(table_id, schema.clone(), tnx_id, false)?;
            Ok(Ddl(format!("Table {} created successfully.", table_name)))
        },
        DdlOperation::RenameTable {old_name, new_name} => {
            // check if old table exists
            let table_id = SysCatalog::global().get_table_id(tnx_id, old_name)?;
            if table_id.is_none() {
                return Err(RsqlError::ExecutionError(format!("Table {} does not exist.", old_name)));
            }
            let table_id = table_id.unwrap();
            // check if table is system table
            if sys_catalog::is_sys_table(table_id) {
                return Err(RsqlError::ExecutionError(format!("System table {} cannot be renamed.", old_name)));
            }
            // check if new table name already exists
            let new_table_id = SysCatalog::global().get_table_id(tnx_id, new_name)?;
            if new_table_id.is_some() {
                return Err(RsqlError::ExecutionError(format!("Table {} already exists.", new_name)));
            }
            // rename table in sys catalog
            SysCatalog::global().rename_table(tnx_id, table_id, new_name)?;
            Ok(Ddl(format!("Table {} renamed to {} successfully.", old_name, new_name)))
        },
        DdlOperation::DropTable { table_name, if_exists} => {
            // check if table exists
            let table_id = SysCatalog::global().get_table_id(tnx_id, table_name)?;
            if table_id.is_none() {
                if *if_exists {
                    return Ok(Ddl(format!("Table {} does not exist, skipping drop table.", table_name)));
                } else {
                    return Err(RsqlError::ExecutionError(format!("Table {} does not exist.", table_name)));
                }
            }
            let table_id = table_id.unwrap();
            // check if table is system table
            if sys_catalog::is_sys_table(table_id) {
                return Err(RsqlError::ExecutionError(format!("System table {} cannot be dropped.", table_name)));
            }
            // drop table itself first
            let table_schema = SysCatalog::global().get_table_schema(tnx_id, table_id)?;
            let table = Table::from(table_id, table_schema, false)?;
            table.drop(tnx_id)?;
            // unregister table from sys catalog
            SysCatalog::global().unregister_table(tnx_id, table_id)?;
            Ok(Ddl(format!("Table {} dropped successfully.", table_name)))
        },
        DdlOperation::CreateIndex {
            index_name,
            table_name,
            column,
            unique,
            if_not_exists,
        } => {
            // check if index exists
            let index_id = SysCatalog::global().get_index_id(tnx_id, index_name)?;
            if index_id.is_some() {
                if *if_not_exists {
                    return Ok(Ddl(format!("Index {} already exists, skipping create index.", index_name)));
                } else {
                    return Err(RsqlError::ExecutionError(format!("Index {} already exists.", index_name)));
                }
            }
            let table_id = SysCatalog::global().get_table_id(tnx_id, table_name)?;
            if table_id.is_none() {
                return Err(RsqlError::ExecutionError(format!("Table {} does not exist.", table_name)));
            }
            let table_id = table_id.unwrap();
            // check if table is system table
            if sys_catalog::is_sys_table(table_id) {
                return Err(RsqlError::ExecutionError(format!("System table {} cannot be indexed.", table_name)));
            }
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
    }
}