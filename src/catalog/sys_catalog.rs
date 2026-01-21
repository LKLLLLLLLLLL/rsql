use std::sync::{OnceLock};
use std::time;
use std::collections::HashMap;

use bcrypt::{hash, DEFAULT_COST};
use tracing::info;

use crate::storage::{WAL, table};
use crate::storage::Table;
use crate::common::DataItem;
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
        TableColumn {
            name: "column_id".to_string(),
            data_type: super::table_schema::ColType::Integer,
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
        TableColumn {
            name: "privileges".to_string(),
            data_type: super::table_schema::ColType::VarChar(4096),
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
pub struct SysCatalog {} // Sys tables are all processed as common tables


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
            .begin_transaction(PrivilegeConn::INIT);
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
        let mut sequence_num = 0;
        let column_schema = sys_column_schema();
        let mut column = Table::create(SYS_COLUMN_ID, column_schema, tnx_id, true)?;
        // init sys table columns
        for table_id in &table_ids {
            let schema = match *table_id {
                SYS_TABLE_ID => sys_table_schema(),
                SYS_COLUMN_ID => sys_column_schema(),
                SYS_INDEX_ID => sys_index_schema(),
                SYS_SEQUENCE_ID => sys_sequence_schema(),
                SYS_USER_ID => sys_user_schema(),
                _ => unreachable!(),
            };
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
                let col_id = sequence_num;
                sequence_num += 1;
                column.insert_row(
                    vec![
                        DataItem::Integer(col_id as i64),
                        DataItem::Integer(*table_id as i64),
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
        }
        // sys_index
        let index_schema = sys_index_schema();
        let _ = Table::create(SYS_INDEX_ID, index_schema, tnx_id, true)?;
        // no need to insert default indexes

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
        sequence.insert_row(
            vec![
                DataItem::Chars { 
                    len: MAX_COL_NAME_SIZE as u64, 
                    value: "column_id".to_string(), 
                },
                DataItem::Integer(sequence_num as i64),
            ], 
            tnx_id,
        )?;
        // sys_user
        let user_schema = sys_user_schema();
        let mut user = Table::create(SYS_USER_ID, user_schema, tnx_id, true)?;
        // insert default admin user
        let root_privileges = "{\"global\":\"W\"}".to_string();
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
                DataItem::VarChar {
                    head: crate::common::VarCharHead {
                        max_len: 4096,
                        len: root_privileges.len() as u64, // len of "{\"global\":\"W\"}"
                        page_ptr: None,
                    },
                    value: root_privileges,
                },
            ],
            tnx_id,
        )?;
        WAL::global().commit_tnx(tnx_id)?;
        TnxManager::global().end_transaction(0);
        info!("System catalog initialized successfully!");
        Ok(())
    }
    /// Construct syscatalog
    /// This method can only called when the database has been initialized
    fn new() -> Self {
        SysCatalog {}
    }

    // pub fn get_storage(&self, table_id: u64) -> Arc<Mutex<StorageManager>> {
    //     match table_id {
    //         SYS_TABLE_ID => self.table.lock().unwrap().get_storage().get_storage(),
    //         SYS_COLUMN_ID => self.column.lock().unwrap().get_storage().get_storage(),
    //         SYS_INDEX_ID => self.index.lock().unwrap().get_storage().get_storage(),
    //         SYS_SEQUENCE_ID => self.sequence.lock().unwrap().get_storage().get_storage(),
    //         SYS_USER_ID => self.user.lock().unwrap().get_storage().get_storage(),
    //         _ => panic!("Invalid system table id: {}", table_id),
    //     }
    // }
    
    /// Query the table schema from system catalog
    /// Input a table id, return the TableSchema of the table
    pub fn get_table_schema(&self, tnx_id: u64, table_id: u64) -> RsqlResult<TableSchema> {
        let read_table = vec![SYS_COLUMN_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        // query sys_column to get columns
        let column = Table::from(SYS_COLUMN_ID, sys_column_schema(), true)?;
        let pk = DataItem::Integer(table_id as i64);
        let pk_opt = Some(pk.clone());
        let column_rows = column
            .get_rows_by_range_indexed_col(
                "table_id",
                &pk_opt,
                &pk_opt,
            ).unwrap();
        let mut columns = vec![];
        for row in column_rows {
            let row = row.unwrap();
            let DataItem::Chars{ len: _, value: name} = &row[2] else {
                panic!("column_name column is not Chars");
            };
            let DataItem::Integer(data_type) = &row[3] else {
                panic!("data_type column is not Integer");
            };
            let DataItem::Integer(extra) = &row[4] else {
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
            let DataItem::Bool(pk) = &row[5] else {
                panic!("is_primary column is not Bool");
            };
            let DataItem::Bool(nullable) = &row[6] else {
                panic!("is_nullable column is not Bool");
            };
            let DataItem::Bool(index) = &row[7] else {
                panic!("is_indexed column is not Bool");
            };
            let DataItem::Bool(unique) = &row[8] else {
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
        let table = Table::from(SYS_TABLE_ID, sys_table_schema(), true)?;
        let pk = DataItem::Integer(table_id as i64);
        let table_row = match table.get_row_by_pk(&pk).unwrap() {
            Some(row) => row,
            None => return Ok(None),
        };
        let DataItem::Chars { value: name, .. } = &table_row[1] else {
            panic!("table_name column is not Chars");
        };
        Ok(Some(name.clone()))
    }
    pub fn get_table_id(&self, tnx_id: u64, table_name: &str) -> RsqlResult<Option<u64>> {
        let read_table = vec![SYS_TABLE_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        // query sys_table to get table id
        let table = Table::from(SYS_TABLE_ID, sys_table_schema(), true)?;
        let index = DataItem::Chars {
            len: MAX_TABLE_NAME_SIZE as u64,
            value: table_name.to_string(),
        };
        let key = Some(index.clone());
        let table_row = table
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
        let mut sequence = Table::from(SYS_SEQUENCE_ID, sys_sequence_schema(), true)?;
        let index = DataItem::Chars { 
            len: MAX_COL_NAME_SIZE as u64, 
            value: sequence_name.to_string(), 
        };
        let key = Some(index.clone());
        let sequence_row_opt = sequence
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
        sequence.update_row(
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
        let mut table = Table::from(SYS_TABLE_ID, sys_table_schema(), true)?;
        let mut column = Table::from(SYS_COLUMN_ID, sys_column_schema(), true)?;
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
        table.insert_row(
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
            column.insert_row(
                vec![
                    DataItem::Integer(self.get_autoincrement(tnx_id, "column_id")?.unwrap() as i64),
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
                let mut index = Table::from(SYS_INDEX_ID, sys_index_schema(), true)?;
                let index_name = format!("IDX_{}_{}", table_name, col.name);
                index.insert_row(
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
        let mut table = Table::from(SYS_TABLE_ID, sys_table_schema(), true)?;
        let mut column = Table::from(SYS_COLUMN_ID, sys_column_schema(), true)?;
        // delete from sys_table
        let pk = DataItem::Integer(table_id as i64);
        table.delete_row(&pk, tnx_id)?;
        // delete from sys_column
        let key = Some(pk.clone());
        let column_rows: Vec<_> = column
            .get_rows_by_range_indexed_col("table_id", &key, &key)?
            .collect();
        for row_opt in column_rows {
            let row = row_opt?;
            let index = row[0].clone(); // column_name is the second column
            column.delete_row(&index, tnx_id)?;
        }
        // delete from sys_index
        let mut index = Table::from(SYS_INDEX_ID, sys_index_schema(), true)?;
        let table_id_item = DataItem::Integer(table_id as i64);
        let key_start = Some(table_id_item.clone());
        let key_end = Some(table_id_item.clone());
        let index_rows: Vec<_> = index
            .get_rows_by_range_indexed_col("table_id", &key_start, &key_end)?
            .collect();
        for row_opt in index_rows {
            let row = row_opt?;
            let key = row[0].clone(); // index_name is the first column
            index.delete_row(&key, tnx_id)?;
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
        let mut table = Table::from(SYS_TABLE_ID, sys_table_schema(), true)?;
        let pk = DataItem::Integer(table_id as i64);
        let table_row = table.get_row_by_pk(&pk)?.ok_or(
            RsqlError::Unknown(format!("Table id {} not found", table_id))
        )?;
        table.update_row(
            &pk,
            vec![
                table_row[0].clone(),
                DataItem::Chars {
                    len: MAX_TABLE_NAME_SIZE as u64,
                    value: new_table_name.to_string(),
                },
                table_row[2].clone(),
            ],
            tnx_id,
        )?;
        Ok(())
    }
    pub fn rename_column(&self, tnx_id: u64, table_id: u64, old_col_name: &str, new_col_name: &str) -> RsqlResult<()> {
        // 1. lock and open sys_column table
        TnxManager::global().acquire_write_locks(tnx_id, &[SYS_COLUMN_ID])?;
        let mut sys_column = Table::from(SYS_COLUMN_ID, sys_column_schema(), true)?;
        
        // 2. find the corresponding column record
        let tid_data = DataItem::Integer(table_id as i64);
        let key = Some(tid_data.clone());
        let iter = sys_column.get_rows_by_range_indexed_col("table_id", &key, &key)?.collect::<RsqlResult<Vec<_>>>()?;
        
        for row_res in iter.into_iter() {
            let row = row_res;
            if let DataItem::Chars { value: name, .. } = &row[2] {
                if name == old_col_name {
                    // found the target column
                    let mut new_row = row.clone();

                    // 3. check if the column has beed indexed
                    let is_indexed = match &row[7] {
                        DataItem::Bool(b) => *b,
                        _ => panic!("is_indexed column is not Bool"),
                    };
                    if is_indexed {
                        return Err(RsqlError::InvalidInput(format!("Cannot rename indexed column: {}", old_col_name)));
                    }
                    
                    // 4. update column name
                    if new_col_name.len() > MAX_COL_NAME_SIZE {
                        return Err(RsqlError::InvalidInput(format!("New column name too long: {}", new_col_name)));
                    }
                    new_row[2] = DataItem::Chars { 
                        len: MAX_COL_NAME_SIZE as u64, 
                        value: new_col_name.to_string() 
                    };
                    
                    // 5. execute update (sys_column primary key is column_id, at index 0)
                    let pk = &row[0];
                    sys_column.update_row(pk, new_row, tnx_id)?;
                    return Ok(());
                }
            }
        }
        
        Err(RsqlError::InvalidInput(format!("Column not found: {}", old_col_name)))
    }
    pub fn get_all_table_ids(&self, tnx_id: u64) -> RsqlResult<Vec<u64>> {
        let read_table = vec![SYS_TABLE_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        let table = Table::from(SYS_TABLE_ID, sys_table_schema(), true)?;
        let mut table_ids = vec![];
        let table_rows = table.get_all_rows()?;
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
        let mut column = Table::from(SYS_COLUMN_ID, sys_column_schema(), true)?;
        let pk = DataItem::Integer(table_id as i64);
        let key = Some(pk.clone());
        let column_rows = column
            .get_rows_by_range_indexed_col("table_id", &key, &key)?
            .collect::<RsqlResult<Vec<_>>>()?;
        let mut found = false;
        for row in column_rows {
            let row = row;
            let DataItem::Chars{ len: _, value: name} = &row[2] else {
                panic!("column_name column is not Chars");
            };
            if name == column_name {
                // update is_indexed to true
                column.update_row(
                    &row[0],
                    vec![
                        row[0].clone(),
                        row[1].clone(),
                        row[2].clone(),
                        row[3].clone(),
                        row[4].clone(),
                        row[5].clone(),
                        row[6].clone(),
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
        let mut index = Table::from(SYS_INDEX_ID, sys_index_schema(), true)?;
        index.insert_row(
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
        let index = Table::from(SYS_INDEX_ID, sys_index_schema(), true)?;
        let mut index_name_opt = None;
        let table_id_item = DataItem::Integer(table_id as i64);
        let key_start = Some(table_id_item.clone());
        let key_end = Some(table_id_item.clone());
        let index_rows = index
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
        let index = Table::from(SYS_INDEX_ID, sys_index_schema(), true)?;
        let index_item = DataItem::Chars { 
            len: MAX_COL_NAME_SIZE as u64, 
            value: index_name.to_string(), 
        };
        let key = Some(index_item.clone());
        let index_row_opt = index
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
        let user = Table::from(SYS_USER_ID, sys_user_schema(), true)?;
        let index = DataItem::Chars { 
            len: MAX_USERNAME_SIZE as u64, 
            value: username.to_string(), 
        };
        let key = Some(index.clone());
        let user_row_opt = user
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
    pub fn unregister_user(
        &self,
        tnx_id: u64,
        username: &str,
    ) -> RsqlResult<()> {
        let write_table = vec![SYS_USER_ID];
        TnxManager::global().acquire_write_locks(tnx_id, &write_table)?;
        let mut user = Table::from(SYS_USER_ID, sys_user_schema(), true)?;
        let index = DataItem::Chars { 
            len: MAX_USERNAME_SIZE as u64, 
            value: username.to_string(), 
        };
        user.delete_row(&index, tnx_id)?;
        Ok(())
    }
    pub fn check_user_privilege(
        &self,
        tnx_id: u64,
        username: &str,
        table_name: Option<&str>,
        privilege: &str, // "R" or "W"
    ) -> RsqlResult<bool> {
        let read_table = vec![SYS_USER_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        let user = Table::from(SYS_USER_ID, sys_user_schema(), true)?;
        let index = DataItem::Chars { 
            len: MAX_USERNAME_SIZE as u64, 
            value: username.to_string(), 
        };
        let user_row_opt = user.get_row_by_pk(&index)?;
        if let Some(user_row) = user_row_opt {
            let DataItem::VarChar { value: privileges_json, .. } = &user_row[2] else {
                panic!("privileges column is not VarChar");
            };
            
            let mut privs: HashMap<String, String> = HashMap::new();
            let trimmed = privileges_json.trim_matches(|c| c == '{' || c == '}');
            if !trimmed.is_empty() {
                for part in trimmed.split(',') {
                    let kv: Vec<&str> = part.split(':').collect();
                    if kv.len() == 2 {
                        let k = kv[0].trim().trim_matches('"').to_string();
                        let v = kv[1].trim().trim_matches('"').to_string();
                        privs.insert(k, v);
                    }
                }
            }

            // 1. Check Global Write (W implies R)
            if let Some(global_priv) = privs.get("global") {
                if global_priv == "W" {
                    return Ok(true);
                }
                if privilege == "R" && global_priv == "R" {
                    return Ok(true);
                }
            }

            // 2. Check Table-specific Permissions
            if let Some(table) = table_name {
                if let Some(table_priv) = privs.get(table) {
                    if table_priv == "W" {
                        return Ok(true);
                    }
                    if privilege == "R" && table_priv == "R" {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        } else {
            Ok(false) // User not found, no privilege
        }
    }
    pub fn check_user_write_permission(
        &self,
        tnx_id: u64,
        username: &str,
    ) -> RsqlResult<bool> {
        self.check_user_privilege(tnx_id, username, None, "W")
    }

    pub fn set_user_table_privilege(
        &self,
        tnx_id: u64,
        username: &str,
        table_name: &str,
        privilege: Option<&str>, // Some("R"), Some("W"), or None to revoke
    ) -> RsqlResult<()> {
        let write_table = vec![SYS_USER_ID];
        TnxManager::global().acquire_write_locks(tnx_id, &write_table)?;
        let mut user = Table::from(SYS_USER_ID, sys_user_schema(), true)?;
        let index = DataItem::Chars { 
            len: MAX_USERNAME_SIZE as u64, 
            value: username.to_string(), 
        };
        let user_row_opt = user.get_row_by_pk(&index)?;
        if let Some(user_row) = user_row_opt {
            let DataItem::VarChar { value: privileges_json, .. } = &user_row[2] else {
                panic!("privileges column is not VarChar");
            };
            
            // Re-construct the privileges JSON-like string
            // Very basic implementation: 
            // 1. Remove existing entry for table_name
            // 2. Add new entry if privilege is Some
            
            let mut privs: HashMap<String, String> = HashMap::new();
            // rudimentary parsing of {"k":"v", "k2":"v2"}
            let trimmed = privileges_json.trim_matches(|c| c == '{' || c == '}');
            if !trimmed.is_empty() {
                for part in trimmed.split(',') {
                    let kv: Vec<&str> = part.split(':').collect();
                    if kv.len() == 2 {
                        let k = kv[0].trim().trim_matches('"').to_string();
                        let v = kv[1].trim().trim_matches('"').to_string();
                        privs.insert(k, v);
                    }
                }
            }

            if let Some(p) = privilege {
                privs.insert(table_name.to_string(), p.to_string());
            } else {
                privs.remove(table_name);
            }

            let mut new_json = "{".to_string();
            let mut first = true;
            for (k, v) in privs {
                if !first { new_json.push_str(", "); }
                new_json.push_str(&format!("\"{}\":\"{}\"", k, v));
                first = false;
            }
            new_json.push('}');

            let new_len = new_json.len() as u64;
            user.update_row(
                &index,
                vec![
                    user_row[0].clone(),
                    user_row[1].clone(),
                    DataItem::VarChar {
                        head: crate::common::VarCharHead {
                            max_len: 4096,
                            len: new_len,
                            page_ptr: None,
                        },
                        value: new_json,
                    },
                ],
                tnx_id,
            )?;
            Ok(())
        } else {
            Err(RsqlError::Unknown(format!("User {} not found", username)))
        }
    }
    pub fn set_user_permission(
        &self,
        tnx_id: u64,
        username: &str,
        privilege: Option<&str>, // Some("W"), Some("R"), or None
    ) -> RsqlResult<()> {
        self.set_user_table_privilege(tnx_id, username, "global", privilege)
    }
    pub fn get_all_users(&self, tnx_id: u64) -> RsqlResult<Vec<String>> {
        let read_table = vec![SYS_USER_ID];
        TnxManager::global().acquire_read_locks(tnx_id, &read_table)?;
        let user = Table::from(SYS_USER_ID, sys_user_schema(), true)?;
        let mut usernames = vec![];
        let user_rows = user.get_all_rows()?;
        for row in user_rows {
            let row = row?;
            let DataItem::Chars{ value: username, .. } = &row[0] else {
                panic!("username column is not Chars");
            };
            usernames.push(username.clone());
        }
        Ok(usernames)
    }
    pub fn register_user(
        &self,
        tnx_id: u64,
        username: &str,
        password: &str,
    ) -> RsqlResult<()> {
        let write_table = vec![SYS_USER_ID];
        TnxManager::global().acquire_write_locks(tnx_id, &write_table)?;
        let mut user = Table::from(SYS_USER_ID, sys_user_schema(), true)?;
        user.insert_row(
            vec![
                DataItem::Chars { 
                    len: MAX_USERNAME_SIZE as u64, 
                    value: username.to_string(), 
                },
                DataItem::Chars { 
                    len: 128, 
                    value: hash(password, DEFAULT_COST).unwrap(),
                },
                DataItem::VarChar {
                    head: crate::common::VarCharHead {
                        max_len: 4096,
                        len: 2, 
                        page_ptr: None,
                    },
                    value: "{}".to_string(),
                },
            ],
            tnx_id,
        )?;
        Ok(())
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
