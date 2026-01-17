// use std::sync::{OnceLock, Mutex};
// use std::fs;
// use std::path;

// use crate::db::data_item::DataItem;
// use crate::db::storage_engine::table;
// use crate::db::table_schema::TableColumn;
// use crate::db::tnx_manager;
// use crate::db::common::{RsqlError, RsqlResult};
// use crate::config::{MAX_COL_NAME_SIZE, MAX_USERNAME_SIZE, MAX_TABLE_NAME_SIZE, DB_DIR};

// use super::table_schema::TableSchema;
// use super::storage_engine::Table;

// pub const SYS_TABLE_ID: u64 = 0;
// pub const SYS_COLUMN_ID: u64 = 1;
// pub const SYS_SEQUENCE_ID: u64 = 2; // for autoincrement
// pub const SYS_USER_ID: u64 = 3;

// fn sys_table_schema() -> TableSchema {
//     TableSchema {
//         columns: vec![
//             TableColumn {
//                 name: "table_id".to_string(),
//                 data_type: super::table_schema::ColType::Integer,
//                 pk: true,
//                 nullable: false,
//                 unique: true,
//                 index: true,
//             },
//             TableColumn {
//                 name: "table_name".to_string(),
//                 data_type: super::table_schema::ColType::VarChar(MAX_TABLE_NAME_SIZE),
//                 pk: false,
//                 nullable: false,
//                 unique: true,
//                 index: true,
//             },
//             TableColumn {
//                 name: "created_at".to_string(),
//                 data_type: super::table_schema::ColType::Integer,
//                 pk: false,
//                 nullable: false,
//                 unique: false,
//                 index: false,
//             },
//         ]
//     }
// }

// fn sys_column_schema() -> TableSchema {
//     TableSchema {
//         columns: vec![
//             TableColumn { // foreign key to sys_table.table_id
//                 name: "table_id".to_string(),
//                 data_type: super::table_schema::ColType::Integer,
//                 pk: false,
//                 nullable: false,
//                 unique: false,
//                 index: true,
//             },
//             TableColumn {
//                 name: "column_name".to_string(),
//                 data_type: super::table_schema::ColType::Chars(MAX_COL_NAME_SIZE),
//                 pk: true,
//                 nullable: false,
//                 unique: true,
//                 index: false,
//             },
//             TableColumn {
//                 name: "data_type".to_string(),
//                 data_type: super::table_schema::ColType::Integer,
//                 pk: false,
//                 nullable: false,
//                 unique: false,
//                 index: false,
//             },
//             TableColumn { // for varchar max size or char size
//                 name: "extra".to_string(),
//                 data_type: super::table_schema::ColType::Integer,
//                 pk: false,
//                 nullable: true,
//                 unique: false,
//                 index: false,
//             },
//             TableColumn {
//                 name: "is_primary".to_string(),
//                 data_type: super::table_schema::ColType::Bool,
//                 pk: false,
//                 nullable: false,
//                 unique: false,
//                 index: false,
//             },
//             TableColumn {
//                 name: "is_nullable".to_string(),
//                 data_type: super::table_schema::ColType::Bool,
//                 pk: false,
//                 nullable: false,
//                 unique: false,
//                 index: false,
//             },
//             TableColumn {
//                 name: "is_indexed".to_string(),
//                 data_type: super::table_schema::ColType::Bool,
//                 pk: false,
//                 nullable: false,
//                 unique: false,
//                 index: false,
//             },
//             TableColumn {
//                 name: "is_unique".to_string(),
//                 data_type: super::table_schema::ColType::Bool,
//                 pk: false,
//                 nullable: false,
//                 unique: false,
//                 index: false,
//             },
//         ]
//     }
// }

// fn sys_sequence_schema() -> TableSchema {
//     TableSchema {
//         columns: vec![
//             TableColumn {
//                 name: "sequence_name".to_string(),
//                 data_type: super::table_schema::ColType::Chars(MAX_COL_NAME_SIZE),
//                 pk: true,
//                 nullable: false,
//                 unique: true,
//                 index: false,
//             },
//             TableColumn {
//                 name: "next_val".to_string(),
//                 data_type: super::table_schema::ColType::Integer,
//                 pk: false,
//                 nullable: false,
//                 unique: false,
//                 index: false,
//             },
//         ]
//     }
// }

// fn sys_user_schema() -> TableSchema {
//     TableSchema {
//         columns: vec![
//             TableColumn {
//                 name: "username".to_string(),
//                 data_type: super::table_schema::ColType::Chars(MAX_USERNAME_SIZE),
//                 pk: true,
//                 nullable: false,
//                 unique: true,
//                 index: false,
//             },
//             TableColumn {
//                 name: "password_hash".to_string(),
//                 data_type: super::table_schema::ColType::Chars(128),
//                 pk: false,
//                 nullable: false,
//                 unique: false,
//                 index: false,
//             },
//             TableColumn {
//                 name: "is_admin".to_string(),
//                 data_type: super::table_schema::ColType::Bool,
//                 pk: false,
//                 nullable: false,
//                 unique: false,
//                 index: false,
//             },
//         ]
//     }
// }

// static SYS_TABLE_INSTANCE: OnceLock<SysCatalog> = OnceLock::new();

// /// System Catalog
// /// Some special tables to store metadata about database objects
// /// Singleton struct
// pub struct SysCatalog {
//     table: Mutex<Table>,
//     column: Mutex<Table>,
//     sequence: Mutex<Table>,
//     user: Mutex<Table>,
// }

// impl SysCatalog {
//     pub fn global() -> &'static SysCatalog {
//         SYS_TABLE_INSTANCE.get_or_init(|| SysCatalog::new())
//     }
//     fn new() -> Self {
//         let tnx_id = tnx_manager::TnxManager::get_tnx_id();
//         if !path::Path::new(DB_DIR).exists() {
//             fs::create_dir_all(DB_DIR).unwrap();
//         }
//         // sys_table
//         let table_schema = sys_table_schema();
//         let table;
//         if path::Path::new(&format!("{}/sys_table.dbs", DB_DIR)).exists() == false {
//             table = Table::create(SYS_TABLE_ID, table_schema, tnx_id).unwrap();
//         } else {
//             table = Table::from(SYS_TABLE_ID, table_schema).unwrap();
//         };
//         // sys_column
//         let column_schema = sys_column_schema();
//         let column;
//         if path::Path::new(&format!("{}/sys_column.dbs", DB_DIR)).exists() == false {
//             column = Table::create(SYS_COLUMN_ID, column_schema, tnx_id).unwrap();
//         } else {
//             column = Table::from(SYS_COLUMN_ID, column_schema).unwrap();
//         };
//         // sys_sequence
//         let sequence_schema = sys_sequence_schema();
//         let sequence;
//         if path::Path::new(&format!("{}/sys_sequence.dbs", DB_DIR)).exists() == false {
//             sequence = Table::create(SYS_SEQUENCE_ID, sequence_schema, tnx_id).unwrap();
//         } else {
//             sequence = Table::from(SYS_SEQUENCE_ID, sequence_schema).unwrap();
//         };
//         // sys_user
//         let user_schema = sys_user_schema();
//         let user;
//         if path::Path::new(&format!("{}/sys_user.dbs", DB_DIR)).exists() == false {
//             user = Table::create(SYS_USER_ID, user_schema, tnx_id).unwrap();
//         } else {
//             user = Table::from(SYS_USER_ID, user_schema).unwrap();
//         };
//         SysCatalog {
//             table: Mutex::new(table),
//             column: Mutex::new(column),
//             sequence: Mutex::new(sequence),
//             user: Mutex::new(user),
//         }
//     }
//     /// Query the table schema from system catalog
//     /// Input a table id, return the TableSchema of the table
//     pub fn get_table_schema(&self, table_id: u64) -> Option<TableSchema> {
//         // query sys_column to get columns
//         let column_guard = self.column.lock().unwrap();
//         let pk = DataItem::Integer(table_id as i64);
//         let column_rows = column_guard
//             .get_rows_by_range_indexed_col(
//                 "table_id", 
//                 &pk,
//                 &pk,
//             ).unwrap();
//         let mut columns = vec![];
//         for row in column_rows {
//             let row = row.unwrap();
//             let DataItem::Chars{ len: _, value: name} = &row[1] else {
//                 panic!("column_name column is not Chars");
//             };
//             let DataItem::Integer(data_type) = &row[2] else {
//                 panic!("data_type column is not Integer");
//             };
//             let DataItem::Integer(extra) = &row[3] else {
//                 panic!("extra column is not Integer");
//             };
//             let data_type = match *data_type as u8 {
//                 0 => super::table_schema::ColType::Integer,
//                 1 => super::table_schema::ColType::Float,
//                 2 => super::table_schema::ColType::Chars(*extra as usize),
//                 3 => super::table_schema::ColType::VarChar(*extra as usize),
//                 4 => super::table_schema::ColType::Bool,
//                 _ => panic!("Invalid column type in sys_column"),
//             };
//             let DataItem::Bool(pk) = &row[4] else {
//                 panic!("is_primary column is not Bool");
//             };
//             let DataItem::Bool(nullable) = &row[5] else {
//                 panic!("is_nullable column is not Bool");
//             };
//             let DataItem::Bool(index) = &row[6] else {
//                 panic!("is_indexed column is not Bool");
//             };
//             let DataItem::Bool(unique) = &row[7] else {
//                 panic!("is_unique column is not Bool");
//             };
//             columns.push(TableColumn {
//                 name: name.clone(),
//                 data_type,
//                 pk: *pk,
//                 nullable: *nullable,
//                 unique: *unique,
//                 index: *index,
//             });
//         };
//         Some(TableSchema { columns })
//     }
//     pub fn get_table_name(&self, table_id: u64) -> Option<String> {
//         // query sys_table to get table name
//         let table_guard = self.table.lock().unwrap();
//         let pk = DataItem::Integer(table_id as i64);
//         let table_row = table_guard.get_row_by_pk(&pk).unwrap()?;
//         let DataItem::VarChar { value: name, .. } = &table_row[1] else {
//             panic!("table_name column is not VarChar");
//         };
//         Some(name.clone())
//     }
//     pub fn get_table_id(&self, table_name: &str) -> Option<u64> {
//         // query sys_table to get table id
//         let table_guard = self.table.lock().unwrap();
//         let index = DataItem::VarChar {
//             head: super::data_item::VarCharHead {
//                 max_len: MAX_TABLE_NAME_SIZE as u64,
//                 len: table_name.len() as u64,
//                 page_ptr: None,
//             },
//             value: table_name.to_string(),
//         };
//         let table_row = table_guard.get_row_by_indexed_col("table_name", &index).unwrap()?;
//         let DataItem::Integer(table_id) = &table_row[0] else {
//             panic!("table_id column is not Integer");
//         };
//         Some(*table_id as u64)
//     }
//     pub fn register_table(
//         &self, 
//         tnx_id: u64,
//         table_name: &str, 
//         schema: &TableSchema
//     ) -> RsqlResult<()> {
//         let mut table_guard = self.table.lock().unwrap();
//         let mut column_guard = self.column.lock().unwrap();
//         // insert into sys_table
//         let table_id = table_guard.;
//     }
// }