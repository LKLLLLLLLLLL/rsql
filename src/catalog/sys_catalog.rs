use std::sync::{OnceLock, Mutex};
use std::time;

use bcrypt::{hash, DEFAULT_COST};
use tracing::info;

use crate::storage::table;
use crate::storage::Table;
use crate::common::DataItem;
use crate::common::data_item::VarCharHead;
use crate::catalog::table_schema::TableColumn;
use crate::transaction::TnxManager;
use crate::common::{RsqlError, RsqlResult};
use crate::config::{
    MAX_COL_NAME_SIZE, 
    MAX_USERNAME_SIZE, 
    MAX_TABLE_NAME_SIZE, 
    DEFAULT_PASSWORD,
    DEFAULT_USERNAME,
};
use crate::common::PrivilegeConn;

use super::table_schema::TableSchema;

pub const SYS_TABLE_ID: u64 = 0;
pub const SYS_COLUMN_ID: u64 = 1;
pub const SYS_INDEX_ID: u64 = 2; // only for user created indexes
pub const SYS_SEQUENCE_ID: u64 = 3; // for autoincrement
pub const SYS_USER_ID: u64 = 4;

pub fn is_sys_table(table_id: u64) -> bool {
    table_id <= SYS_USER_ID
}

fn sys_table_schema() -> TableSchema {
    let columns = vec![
        TableColumn {
            name: "table_id".to_string(),
            data_type: super::table_schema::ColType::Integer,
            pk: true,
            nullable: false,
            unique: true,
            index: true,
        },
        TableColumn {
            name: "table_name".to_string(),
            data_type: super::table_schema::ColType::Chars(MAX_TABLE_NAME_SIZE),
            pk: false,
            nullable: false,
            unique: true,
            index: true,
        },
        TableColumn {
            name: "created_at".to_string(),
            data_type: super::table_schema::ColType::Integer,
            pk: false,
            nullable: false,
            unique: false,
            index: false,
        },
    ];
    TableSchema::new(columns).unwrap()
}

fn sys_column_schema() -> TableSchema {
    let columns = vec![
        TableColumn { // foreign key to sys_table.table_id
            name: "table_id".to_string(),
            data_type: super::table_schema::ColType::Integer,
            pk: false,
            nullable: false,
            unique: false,
            index: true,
        },
        TableColumn {
            name: "column_name".to_string(),
            data_type: super::table_schema::ColType::Chars(MAX_COL_NAME_SIZE),
            pk: true,
            nullable: false,
            unique: true,
            index: true,
        },
        TableColumn {
            name: "data_type".to_string(),
            data_type: super::table_schema::ColType::Integer,
            pk: false,
            nullable: false,
            unique: false,
            index: false,
        },
        TableColumn { // for varchar max size or char size
            name: "extra".to_string(),
            data_type: super::table_schema::ColType::Integer,
            pk: false,
            nullable: true,
            unique: false,
            index: false,
        },
        TableColumn {
            name: "is_primary".to_string(),
            data_type: super::table_schema::ColType::Bool,
            pk: false,
            nullable: false,
            unique: false,
            index: false,
        },
        TableColumn {
            name: "is_nullable".to_string(),
            data_type: super::table_schema::ColType::Bool,
            pk: false,
            nullable: false,
            unique: false,
            index: false,
        },
        TableColumn {
            name: "is_indexed".to_string(),
            data_type: super::table_schema::ColType::Bool,
            pk: false,
            nullable: false,
            unique: false,
            index: false,
        },
        TableColumn {
            name: "is_unique".to_string(),
            data_type: super::table_schema::ColType::Bool,
            pk: false,
            nullable: false,
            unique: false,
            index: false,
        },
    ];
    TableSchema::new(columns).unwrap()
}

fn sys_index_schema() -> TableSchema {
    let columns = vec![
        TableColumn {
            name: "index_name".to_string(),
            data_type: super::table_schema::ColType::Chars(MAX_COL_NAME_SIZE),
            pk: true,
            nullable: false,
            unique: true,
            index: true,
        },
        TableColumn { // foreign key to sys_table.table_id
            name: "table_id".to_string(),
            data_type: super::table_schema::ColType::Integer,
            pk: false,
            nullable: false,
            unique: false,
            index: true,
        },
        TableColumn {
            name: "column_name".to_string(),
            data_type: super::table_schema::ColType::Chars(MAX_COL_NAME_SIZE),
            pk: false,
            nullable: false,
            unique: false,
            index: true,
        },
    ];
    TableSchema::new(columns).unwrap()
}

fn sys_sequence_schema() -> TableSchema {
    let columns = vec![
        TableColumn {
            name: "sequence_name".to_string(),
            data_type: super::table_schema::ColType::Chars(MAX_COL_NAME_SIZE),
            pk: true,
            nullable: false,
            unique: true,
            index: true,
        },
        TableColumn {
            name: "next_val".to_string(),
            data_type: super::table_schema::ColType::Integer,
            pk: false,
            nullable: false,
            unique: false,
            index: false,
        },
    ];
    TableSchema::new(columns).unwrap()
}

fn sys_user_schema() -> TableSchema {
    let columns = vec![
        TableColumn {
            name: "username".to_string(),
            data_type: super::table_schema::ColType::Chars(MAX_USERNAME_SIZE),
            pk: true,
            nullable: false,
            unique: true,
            index: true,
        },
        TableColumn {
            name: "password_hash".to_string(),
            data_type: super::table_schema::ColType::Chars(128),
            pk: false,
            nullable: false,
            unique: false,
            index: false,
        },
        TableColumn { // TODO: implement is_admin
            name: "is_admin".to_string(),
            data_type: super::table_schema::ColType::Bool,
            pk: false,
            nullable: false,
            unique: false,
            index: false,
        },
    ];
    TableSchema::new(columns).unwrap()
}

static SYS_TABLE_INSTANCE: OnceLock<SysCatalog> = OnceLock::new();

/// System Catalog
/// Some special tables to store metadata about database objects
/// Singleton struct
pub struct SysCatalog {
    // The consistence is guaranteed by TnxManager locks.
    // This Mutex is only used to protect concurrent access.
    table: Mutex<Table>,
    column: Mutex<Table>,
    index: Mutex<Table>,
    sequence: Mutex<Table>,
    user: Mutex<Table>,
}

impl SysCatalog {
    pub fn global() -> &'static SysCatalog {
        SYS_TABLE_INSTANCE.get_or_init(|| SysCatalog::new())
    }
    /// Initialize system catalog
    pub fn init() -> RsqlResult<()> {
        // check if the first time init
        let table_path = table::get_table_path(SYS_TABLE_ID, true);
        if table_path.exists() {
            return Ok(());
        };
        info!("First time starting database, initializing system catalog...");
        let tnx_id = TnxManager::global()
            .begin_transaction(PrivilegeConn::INIT); // connection id 0 for privileged operations
        let table_ids = vec![
            SYS_TABLE_ID,
            SYS_COLUMN_ID,
            SYS_INDEX_ID,
            SYS_SEQUENCE_ID,
            SYS_USER_ID,
        ];
        TnxManager::global().acquire_read_locks(
            tnx_id, 
            &table_ids
        ).unwrap(); // should not fail
        // sys_table
        let table_schema = sys_table_schema();
        let mut table = Table::create(SYS_TABLE_ID, table_schema, tnx_id, true)?;
        // insert sys tables
        for table_id in &table_ids {
            let table_name = match *table_id {
                SYS_TABLE_ID => "sys_table",
                SYS_COLUMN_ID => "sys_column",
                SYS_INDEX_ID => "sys_index",
                SYS_SEQUENCE_ID => "sys_sequence",
                SYS_USER_ID => "sys_user",
                _ => unreachable!(),
            };
            let created_at = time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            table.insert_row(
                vec![
                    DataItem::Integer(*table_id as i64),
                    DataItem::Chars { 
                        len: MAX_TABLE_NAME_SIZE as u64, 
                        value: table_name.to_string(), 
                    },
                    DataItem::Integer(created_at as i64),
                ],
                tnx_id,
            )?;
        }
        // sys_column
        let column_schema = sys_column_schema();
        let _ = Table::create(SYS_COLUMN_ID, column_schema, tnx_id, true)?;
        // sys_index
        let index_schema = sys_index_schema();
        let _ = Table::create(SYS_INDEX_ID, index_schema, tnx_id, true)?;
        // sys_sequence
        let sequence_schema = sys_sequence_schema();
        let mut sequence = Table::create(SYS_SEQUENCE_ID, sequence_schema, tnx_id, true)?;
        // insert default sequences
        let init_table_id = table_ids.iter().max().unwrap() + 1;
        sequence.insert_row( // table_id
            vec![
                DataItem::Chars { 
                    len: MAX_COL_NAME_SIZE as u64, 
                    value: "table_id".to_string(), 
                },
                DataItem::Integer(init_table_id as i64),
            ],
            tnx_id
        )?;
        // sys_user
        let user_schema = sys_user_schema();
        let mut user = Table::create(SYS_USER_ID, user_schema, tnx_id, true)?;
        // insert default admin user
        user.insert_row( // default user
            vec![
                DataItem::Chars { 
                    len: MAX_USERNAME_SIZE as u64, 
                    value: DEFAULT_USERNAME.to_string(), 
                },
                DataItem::Chars { 
                    len: 128, 
                    value: hash(DEFAULT_PASSWORD, DEFAULT_COST).unwrap(),
                },
                DataItem::Bool(true),
            ],
            tnx_id,
        )?;
        TnxManager::global().end_transaction(0);
        info!("System catalog initialized successfully!");
        Ok(())
    }
    /// Construct syscatalog
    /// This method can only called when the database has been initialized
    fn new() -> Self {
        // sys_table
        let table_schema = sys_table_schema();
        let table = Table::from(SYS_TABLE_ID, table_schema, true).unwrap();
        // sys_column
        let column_schema = sys_column_schema();
        let column = Table::from(SYS_COLUMN_ID, column_schema, true).unwrap();
        // sys_index
        let index_schema = sys_index_schema();
        let index = Table::from(SYS_INDEX_ID, index_schema, true).unwrap();
        // sys_sequence
        let sequence_schema = sys_sequence_schema();
        let sequence = Table::from(SYS_SEQUENCE_ID, sequence_schema, true).unwrap();
        // sys_user
        let user_schema = sys_user_schema();
        let user = Table::from(SYS_USER_ID, user_schema, true).unwrap();
        SysCatalog {
            table: Mutex::new(table),
            column: Mutex::new(column),
            index: Mutex::new(index),
            sequence: Mutex::new(sequence),
            user: Mutex::new(user),
        }
    }
    /// Query the table schema from system catalog
    /// Input a table id, return the TableSchema of the table
    pub fn get_table_schema(&self, tnx_id: u64, table_id: u64) -> RsqlResult<TableSchema> {
        let read_table = vec![SYS_COLUMN_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        // query sys_column to get columns
        let column_guard = self.column.lock().unwrap();
        let pk = DataItem::Integer(table_id as i64);
        let pk_opt = Some(pk.clone());
        let column_rows = column_guard
            .get_rows_by_range_indexed_col(
                "table_id",
                &pk_opt,
                &pk_opt,
            ).unwrap();
        let mut columns = vec![];
        for row in column_rows {
            let row = row.unwrap();
            let DataItem::Chars{ len: _, value: name} = &row[1] else {
                panic!("column_name column is not Chars");
            };
            let DataItem::Integer(data_type) = &row[2] else {
                panic!("data_type column is not Integer");
            };
            let DataItem::Integer(extra) = &row[3] else {
                panic!("extra column is not Integer");
            };
            let data_type = match *data_type as u8 {
                0 => super::table_schema::ColType::Integer,
                1 => super::table_schema::ColType::Float,
                2 => super::table_schema::ColType::Chars(*extra as usize),
                3 => super::table_schema::ColType::VarChar(*extra as usize),
                4 => super::table_schema::ColType::Bool,
                _ => panic!("Invalid column type in sys_column"),
            };
            let DataItem::Bool(pk) = &row[4] else {
                panic!("is_primary column is not Bool");
            };
            let DataItem::Bool(nullable) = &row[5] else {
                panic!("is_nullable column is not Bool");
            };
            let DataItem::Bool(index) = &row[6] else {
                panic!("is_indexed column is not Bool");
            };
            let DataItem::Bool(unique) = &row[7] else {
                panic!("is_unique column is not Bool");
            };
            columns.push(TableColumn {
                name: name.clone(),
                data_type,
                pk: *pk,
                nullable: *nullable,
                unique: *unique,
                index: *index,
            });
        };
        Ok(TableSchema::new(columns).unwrap())
    }
    pub fn get_table_name(&self, table_id: u64, tnx_id: u64) -> RsqlResult<Option<String>> {
        let read_table = vec![SYS_TABLE_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        // query sys_table to get table name
        let table_guard = self.table.lock().unwrap();
        let pk = DataItem::Integer(table_id as i64);
        let table_row = match table_guard.get_row_by_pk(&pk).unwrap() {
            Some(row) => row,
            None => return Ok(None),
        };
        let DataItem::VarChar { value: name, .. } = &table_row[1] else {
            panic!("table_name column is not VarChar");
        };
        Ok(Some(name.clone()))
    }
    pub fn get_table_id(&self, tnx_id: u64, table_name: &str) -> RsqlResult<Option<u64>> {
        let read_table = vec![SYS_TABLE_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        // query sys_table to get table id
        let table_guard = self.table.lock().unwrap();
        let index = DataItem::Chars {
            len: MAX_TABLE_NAME_SIZE as u64,
            value: table_name.to_string(),
        };
        let key = Some(index.clone());
        let table_row = table_guard
            .get_rows_by_range_indexed_col("table_name", &key, &key)
            .unwrap()
            .next()
            .transpose()?;
        let table_row = match table_row {
            Some(row) => row,
            None => return Ok(None),
        };
        let DataItem::Integer(table_id) = &table_row[0] else {
            panic!("table_id column is not Integer");
        };
        Ok(Some(*table_id as u64))
    }
    fn get_autoincrement(&self, tnx_id: u64, sequence_name: &str) -> RsqlResult<Option<u64>> {
        let read_table = vec![SYS_SEQUENCE_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        let mut sequence_guard = self.sequence.lock().unwrap();
        let index = DataItem::Chars { 
            len: MAX_COL_NAME_SIZE as u64, 
            value: sequence_name.to_string(), 
        };
        let key = Some(index.clone());
        let sequence_row_opt = sequence_guard
            .get_rows_by_range_indexed_col("sequence_name", &key, &key)?
            .next();
        if let None = sequence_row_opt {
            return Ok(None);
        };
        // get next_val
        let sequence_row = sequence_row_opt.unwrap()?;
        let DataItem::Integer(next_val) = &sequence_row[1] else {
            panic!("next_val column is not Integer");
        };
        let next_val = *next_val as u64;
        // update next_val
        let new_next_val = next_val + 1;
        sequence_guard.update_row(
            &index,
            vec![
                sequence_row[0].clone(),
                DataItem::Integer(new_next_val as i64),
            ],
            tnx_id,
        )?;
        Ok(Some(next_val))
    }
    /// Register a new table into system catalog
    /// You should create table first, then call this function to register
    /// Return the table id
    pub fn register_table(
        &self,
        tnx_id: u64,
        table_name: &str,
        schema: &TableSchema
    ) -> RsqlResult<u64> {
        let write_table = vec![SYS_TABLE_ID, SYS_INDEX_ID, SYS_COLUMN_ID];
        TnxManager::global().acquire_write_locks(tnx_id, &write_table)?;
        let mut table_guard = self.table.lock().unwrap();
        let mut column_guard = self.column.lock().unwrap();
        // get table id
        let table_id = self.get_autoincrement(tnx_id, "table_id");
        let table_id = match table_id? {
            Some(id) => id,
            None => panic!("Failed to get autoincrement for table_id"),
        };
        // insert into sys_table
        let created_at = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        table_guard.insert_row(
            vec![
                DataItem::Integer(table_id as i64),
                DataItem::Chars {
                    len: MAX_TABLE_NAME_SIZE as u64,
                    value: table_name.to_string(),
                },
                DataItem::Integer(created_at as i64),
            ],
            tnx_id,
        )?;
        // insert into sys_column
        for col in schema.get_columns() {
            let data_type = match &col.data_type {
                super::table_schema::ColType::Integer => 0,
                super::table_schema::ColType::Float => 1,
                super::table_schema::ColType::Chars(_) => 2,
                super::table_schema::ColType::VarChar(_) => 3,
                super::table_schema::ColType::Bool => 4,
            };
            let extra = match &col.data_type {
                super::table_schema::ColType::Chars(size) => *size as i64,
                super::table_schema::ColType::VarChar(size) => *size as i64,
                _ => 0,
            };
            column_guard.insert_row(
                vec![
                    DataItem::Integer(table_id as i64),
                    DataItem::Chars { 
                        len: MAX_COL_NAME_SIZE as u64, 
                        value: col.name.clone(), 
                    },
                    DataItem::Integer(data_type),
                    DataItem::Integer(extra),
                    DataItem::Bool(col.pk),
                    DataItem::Bool(col.nullable),
                    DataItem::Bool(col.index),
                    DataItem::Bool(col.unique),
                ],
                tnx_id,
            )?;
        }
        // register indexes for indexed columns
        for col in schema.get_columns() {
            if col.index {
                let mut index_guard = self.index.lock().unwrap();
                let index_name = format!("IDX_{}_{}", table_name, col.name);
                index_guard.insert_row(
                    vec![
                        DataItem::Chars { 
                            len: MAX_COL_NAME_SIZE as u64, 
                            value: index_name, 
                        },
                        DataItem::Integer(table_id as i64),
                        DataItem::Chars { 
                            len: MAX_COL_NAME_SIZE as u64, 
                            value: col.name.clone(), 
                        },
                    ],
                    tnx_id,
                )?;
            }
        }
        Ok(table_id)
    }
    pub fn unregister_table(
        &self,
        tnx_id: u64,
        table_id: u64,
    ) -> RsqlResult<()> {
        let write_table = vec![SYS_TABLE_ID, SYS_COLUMN_ID];
        TnxManager::global().acquire_write_locks(tnx_id, &write_table)?;
        let mut table_guard = self.table.lock().unwrap();
        let mut column_guard = self.column.lock().unwrap();
        // delete from sys_table
        let pk = DataItem::Integer(table_id as i64);
        table_guard.delete_row(&pk, tnx_id)?;
        // delete from sys_column
        let key = Some(pk.clone());
        let column_rows: Vec<_> = column_guard
            .get_rows_by_range_indexed_col("table_id", &key, &key)?
            .collect();
        for row_opt in column_rows {
            let row = row_opt?;
            let index = row[1].clone(); // column_name is the second column
            column_guard.delete_row(&index, tnx_id)?;
        }
        // delete from sys_index
        let mut index_guard = self.index.lock().unwrap();
        let table_id_item = DataItem::Integer(table_id as i64);
        let key_start = Some(table_id_item.clone());
        let key_end = Some(table_id_item.clone());
        let index_rows: Vec<_> = index_guard
            .get_rows_by_range_indexed_col("table_id", &key_start, &key_end)?
            .collect();
        for row_opt in index_rows {
            let row = row_opt?;
            let index = row[0].clone(); // index_name is the first column
            index_guard.delete_row(&index, tnx_id)?;
        }
        Ok(())
    }
    pub fn rename_table(
        &self,
        tnx_id: u64,
        table_id: u64,
        new_table_name: &str,
    ) -> RsqlResult<()> {
        let write_table = vec![SYS_TABLE_ID];
        TnxManager::global().acquire_write_locks(tnx_id, &write_table)?;
        let mut table_guard = self.table.lock().unwrap();
        let pk = DataItem::Integer(table_id as i64);
        let table_row = table_guard.get_row_by_pk(&pk)?.ok_or(
            RsqlError::Unknown(format!("Table id {} not found", table_id))
        )?;
        table_guard.update_row(
            &pk,
            vec![
                table_row[0].clone(),
                DataItem::VarChar {
                    head: VarCharHead {
                        max_len: MAX_TABLE_NAME_SIZE as u64,
                        len: new_table_name.len() as u64,
                        page_ptr: None,
                    },
                    value: new_table_name.to_string(),
                },
                table_row[2].clone(),
            ],
            tnx_id,
        )?;
        Ok(())
    }
    pub fn get_all_table_ids(&self, tnx_id: u64) -> RsqlResult<Vec<u64>> {
        let read_table = vec![SYS_TABLE_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        let table_guard = self.table.lock().unwrap();
        let mut table_ids = vec![];
        let table_rows = table_guard.get_all_rows()?;
        for row in table_rows {
            let row = row?;
            let DataItem::Integer(table_id) = &row[0] else {
                panic!("table_id column is not Integer");
            };
            table_ids.push(*table_id as u64);
        }
        Ok(table_ids)
    }
    
    pub fn register_index(
        &self,
        tnx_id: u64,
        table_id: u64,
        column_name: &str,
        index_name: &str,
        unique: bool,
    ) -> RsqlResult<()> {
        let write_table = vec![SYS_COLUMN_ID];
        TnxManager::global().acquire_write_locks(tnx_id, &write_table)?;
        // register to column table
        let mut column_guard = self.column.lock().unwrap();
        let pk = DataItem::Integer(table_id as i64);
        let key = Some(pk.clone());
        let column_rows = column_guard
            .get_rows_by_range_indexed_col("table_id", &key, &key)?
            .collect::<RsqlResult<Vec<_>>>()?;
        let mut found = false;
        for row in column_rows {
            let row = row;
            let DataItem::Chars{ len: _, value: name} = &row[1] else {
                panic!("column_name column is not Chars");
            };
            if name == column_name {
                // update is_indexed to true
                column_guard.update_row(
                    &row[1],
                    vec![
                        row[0].clone(),
                        row[1].clone(),
                        row[2].clone(),
                        row[3].clone(),
                        row[4].clone(),
                        row[5].clone(),
                        DataItem::Bool(true), // set is_indexed to true
                        DataItem::Bool(unique), // set is_unique
                    ],
                    tnx_id,
                )?;
                found = true;
                break;
            }
        };
        if !found {
            return Err(RsqlError::Unknown(format!(
                "Column {} not found in table id {}",
                column_name, table_id
            )))
        };
        // register to index table
        let mut index_guard = self.index.lock().unwrap();
        index_guard.insert_row(
            vec![
                DataItem::Chars { 
                    len: MAX_COL_NAME_SIZE as u64, 
                    value: index_name.to_string(), 
                },
                DataItem::Integer(table_id as i64),
                DataItem::Chars { 
                    len: MAX_COL_NAME_SIZE as u64, 
                    value: column_name.to_string(), 
                },
            ],
            tnx_id,
        )?;
        Ok(())
    }
    pub fn get_index_name(&self, tnx_id: u64, table_id: u64, column_name: &str) -> RsqlResult<Option<String>> {
        let read_table = vec![SYS_INDEX_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        let index_guard = self.index.lock().unwrap();
        let mut index_name_opt = None;
        let table_id_item = DataItem::Integer(table_id as i64);
        let key_start = Some(table_id_item.clone());
        let key_end = Some(table_id_item.clone());
        let index_rows = index_guard
            .get_rows_by_range_indexed_col("table_id", &key_start, &key_end)?
            .collect::<RsqlResult<Vec<_>>>()?;
        for row in index_rows {
            let row = row;
            let DataItem::Chars{ len: _, value: col_name} = &row[2] else {
                panic!("column_name column is not Chars");
            };
            if col_name == column_name {
                let DataItem::Chars{ len: _, value: index_name} = &row[0] else {
                    panic!("index_name column is not Chars");
                };
                index_name_opt = Some(index_name.clone());
                break;
            }
        };
        Ok(index_name_opt)
    }
    pub fn get_index_id(&self, tnx_id: u64, index_name: &str) -> RsqlResult<Option<u64>> {
        let read_table = vec![SYS_INDEX_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        let index_guard = self.index.lock().unwrap();
        let index_item = DataItem::Chars { 
            len: MAX_COL_NAME_SIZE as u64, 
            value: index_name.to_string(), 
        };
        let key = Some(index_item.clone());
        let index_row_opt = index_guard
            .get_rows_by_range_indexed_col("index_name", &key, &key)?
            .next();
        if let None = index_row_opt {
            return Ok(None);
        };
        let index_row = index_row_opt.unwrap()?;
        let DataItem::Integer(table_id) = &index_row[1] else {
            panic!("table_id column is not Integer");
        };
        Ok(Some(*table_id as u64))
    }

    pub fn validate_user(
        &self,
        tnx_id: u64,
        username: &str,
        password: &str,
    ) -> RsqlResult<bool> {
        let read_table = vec![SYS_USER_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        let user_guard = self.user.lock().unwrap();
        let index = DataItem::Chars { 
            len: MAX_USERNAME_SIZE as u64, 
            value: username.to_string(), 
        };
        let key = Some(index.clone());
        let user_row_opt = user_guard
            .get_rows_by_range_indexed_col("username", &key, &key)?
            .next();
        if let None = user_row_opt {
            return Ok(false);
        };
        let user_row = user_row_opt.unwrap()?;
        let DataItem::Chars{ len: _, value: password_hash} = &user_row[1] else {
            panic!("password_hash column is not Chars");
        };
        match bcrypt::verify(password, password_hash) {
            Ok(valid) => Ok(valid),
            Err(_) => Ok(false),
        }
    }
    pub fn register_user(
        &self,
        tnx_id: u64,
        username: &str,
        password: &str,
    ) -> RsqlResult<()> {
        let write_table = vec![SYS_USER_ID];
        TnxManager::global().acquire_write_locks(tnx_id, &write_table)?;
        let mut user_guard = self.user.lock().unwrap();
        let password_hash = hash(password, DEFAULT_COST).unwrap();
        user_guard.insert_row(
            vec![
                DataItem::Chars { 
                    len: MAX_USERNAME_SIZE as u64, 
                    value: username.to_string(), 
                },
                DataItem::Chars { 
                    len: 128, 
                    value: password_hash,
                },
                DataItem::Bool(false), // is_admin false
            ],
            tnx_id,
        )?;
        Ok(())
    }
    pub fn unregister_user(
        &self,
        tnx_id: u64,
        username: &str,
    ) -> RsqlResult<()> {
        let write_table = vec![SYS_USER_ID];
        TnxManager::global().acquire_write_locks(tnx_id, &write_table)?;
        let mut user_guard = self.user.lock().unwrap();
        let index = DataItem::Chars { 
            len: MAX_USERNAME_SIZE as u64, 
            value: username.to_string(), 
        };
        user_guard.delete_row(&index, tnx_id)?;
        Ok(())
    }
    pub fn get_all_users(&self, tnx_id: u64) -> RsqlResult<Vec<String>> {
        let read_table = vec![SYS_USER_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        let user_guard = self.user.lock().unwrap();
        let mut usernames = vec![];
        let user_rows = user_guard.get_all_rows()?;
        for row in user_rows {
            let row = row?;
            let DataItem::Chars{ len: _, value: username} = &row[0] else {
                panic!("username column is not Chars");
            };
            usernames.push(username.clone());
        }
        Ok(usernames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::table_schema::ColType;
    use crate::transaction::TnxManager;
    use serial_test::serial;

    fn setup_test_catalog() -> &'static SysCatalog {
        TnxManager::init(1);
        SysCatalog::init().unwrap();
        SysCatalog::global()
    }

    #[test]
    #[serial]
    fn test_auto_increment() {
        let catalog = setup_test_catalog();
        let tnx_id = TnxManager::global().begin_transaction(0);

        let first_id = catalog.get_autoincrement(tnx_id, "table_id").unwrap().unwrap();
        let second_id = catalog.get_autoincrement(tnx_id, "table_id").unwrap().unwrap();
        let third_id = catalog.get_autoincrement(tnx_id, "table_id").unwrap().unwrap();

        assert_eq!(first_id + 1, second_id);
        assert_eq!(second_id + 1, third_id);
        TnxManager::global().end_transaction(0);
    }

    #[test]
    #[serial]
    fn test_reg_table() {
        let catalog = setup_test_catalog();
        let tnx_id = TnxManager::global().begin_transaction(1);
        let columns = vec![
            TableColumn {
                name: "id".to_string(),
                data_type: ColType::Integer,
                pk: true,
                nullable: false,
                unique: true,
                index: true,
            },
            TableColumn {
                name: "name".to_string(),
                data_type: ColType::Chars(MAX_COL_NAME_SIZE),
                pk: false,
                nullable: false,
                unique: false,
                index: false,
            },
        ];
        let schema = TableSchema::new(columns).unwrap();
        let table_name = "test_table";

        let table_id = catalog.register_table(tnx_id, table_name, &schema).unwrap();
        assert!(table_id > SYS_USER_ID);

        let retrieved_schema = catalog.get_table_schema(tnx_id, table_id).unwrap();
        assert_eq!(retrieved_schema.get_columns().len(), 2);
        assert_eq!(retrieved_schema.get_columns()[0].name, "id");
        assert_eq!(retrieved_schema.get_columns()[1].name, "name");
        TnxManager::global().end_transaction(1);
    }

    #[test]
    #[serial]
    fn test_user_management() {
        let catalog = setup_test_catalog();
        let tnx_id = TnxManager::global().begin_transaction(2);

        let username = "testuser";
        let password = "password123";

        // 1. Register user
        catalog.register_user(tnx_id, username, password).expect("Failed to register user");

        // 2. Validate user
        assert!(catalog.validate_user(tnx_id, username, password).expect("Failed to validate"));
        assert!(!catalog.validate_user(tnx_id, username, "wrongpassword").expect("Validation should fail"));
        assert!(!catalog.validate_user(tnx_id, "nonexistent", password).expect("Validation should fail"));

        // 3. Unregister user
        catalog.unregister_user(tnx_id, username).expect("Failed to unregister user");
        assert!(!catalog.validate_user(tnx_id, username, password).expect("User should be gone"));
        TnxManager::global().end_transaction(2);
    }

    #[test]
    #[serial]
    fn test_system_tables_init() {
        let catalog = setup_test_catalog();
        let tnx_id = TnxManager::global().begin_transaction(3);
        // Verify admin user
        assert!(catalog.validate_user(tnx_id, DEFAULT_USERNAME, DEFAULT_PASSWORD).expect("Admin validation failed"));
        TnxManager::global().end_transaction(3);
    }
}
