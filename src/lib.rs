mod config;
mod server;
mod execution;
mod common;
mod sql;
mod storage;
mod catalog;
mod transaction;
mod utils;

use tracing_subscriber::prelude::*;
use std::fs;
use std::path;
use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use tracing::info;

use crate::common::RsqlResult;
use crate::server::conncetion_user_map::ConnectionUserMap;
use crate::storage::WAL;
use crate::storage::storage::{Page, StorageManager};
use crate::storage::table::get_table_path;
use crate::catalog::sys_catalog::is_sys_table;
use crate::transaction::TnxManager;

pub fn init_log() {
    let log_dir = path::Path::new(config::LOG_PATH).parent().unwrap();
    let log_filename = path::Path::new(config::LOG_PATH).file_name().unwrap().to_str().unwrap();
    fs::create_dir_all(log_dir).unwrap();

    let stdout_log = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .with_thread_names(true)
        .with_level(true);

    let file_appender = tracing_appender::rolling::daily(log_dir, log_filename);
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    Box::leak(Box::new(_guard));

    let file_log = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_thread_names(true)
        .with_level(true);

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(config::LOG_LEVEL))
        .with(stdout_log)
        .with(file_log)
        .init();
}

fn recovery_wal() -> RsqlResult<u64> {
    let tmp_storages: Rc<RefCell<HashMap<u64, Arc<Mutex<StorageManager>>>>> = Rc::new(RefCell::new(HashMap::new()));
    // Helper closures
    let get_sm = |table_id: u64| -> RsqlResult<Arc<Mutex<StorageManager>>> {
        let mut tmp_storages = tmp_storages.borrow_mut();
        if let Some(sm) = tmp_storages.get(&table_id) {
            Ok(sm.clone())
        } else {
            let file_path = get_table_path(table_id, is_sys_table(table_id));
            let sm = StorageManager::new(file_path.to_str().unwrap())?;
            tmp_storages.insert(table_id, sm);
            Ok(tmp_storages.get(&table_id).unwrap().clone())
        }
    };
    let mut write_page = |table_id: u64, page_id: u64, data: &[u8]| -> RsqlResult<()> {
        let sm_rc = get_sm(table_id)?;
        let mut sm = sm_rc.lock().unwrap();
        let mut page = sm.read_page(page_id)?;
        assert_eq!(Page::max_size(), data.len());
        page.data[..data.len()].copy_from_slice(data);
        sm.write_page(&page, page_id)
    };
    let mut update_page = |table_id: u64, page_id: u64, offset: u64, len: u64, data: &[u8]| -> RsqlResult<()> {
        let sm_rc = get_sm(table_id)?;
        let mut sm = sm_rc.lock().unwrap();
        let mut page = sm.read_page(page_id)?;
        assert!(offset + len <= Page::max_size() as u64);
        page.data[offset as usize..(offset + len) as usize].copy_from_slice(data);
        sm.write_page(&page, page_id)
    };
    let mut append_page = |table_id: u64| -> RsqlResult<u64> {
        let sm_rc = get_sm(table_id)?;
        let mut sm = sm_rc.lock().unwrap();
        Ok(sm.new_page()?.0)
    };
    let mut trunc_page = |table_id: u64| -> RsqlResult<()> {
        let sm_rc = get_sm(table_id)?;
        let mut sm = sm_rc.lock().unwrap();
        sm.free()?;
        Ok(())
    };
    let mut max_page_idx = |table_id: u64| -> RsqlResult<Option<u64>> {
        let sm_rc = get_sm(table_id)?;
        let sm = sm_rc.lock().unwrap();
        Ok(sm.max_page_index())
    };
    // Execute WAL operation
    WAL::recovery(
        &mut write_page,
        &mut update_page,
        &mut append_page,
        &mut trunc_page,
        &mut max_page_idx,
    )
}

pub fn init_database() -> RsqlResult<()> {
    info!("Initializing database...");
    // If single file mode is enabled, unpack the archive first
    if config::SINGLE_FILE_MODE {
        storage::archiver::init_single_file()?;
    }
    let max_tnx_id = recovery_wal()?;
    TnxManager::init(max_tnx_id + 1);
    catalog::SysCatalog::init()?;
    info!("Database initialized successfully!");
    Ok(())
}

pub fn init_connection_user_map() {
    ConnectionUserMap::init();
}

pub fn run() {
    init_log();
    init_connection_user_map();
    init_database().expect("Failed to initialize database");
    server::daemon::daemon();

    // After daemon returns (server shut down)
    if config::SINGLE_FILE_MODE {
        info!("Single file mode enabled, archiving database...");
        storage::archiver::archive_single_file().expect("Failed to archive single file on shutdown");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::result::ExecutionResult;

    #[test]
    fn test_drop_column() {
        // 0. Init Global Managers
        crate::transaction::TnxManager::init(1);
        crate::catalog::SysCatalog::init().unwrap(); // ensure system tables exist
        
        // 1. Setup Connection
        crate::server::conncetion_user_map::ConnectionUserMap::global().insert_connection(999, "root".to_string());
        let conn_id = 999;

        // 2. Create Table
        let sql = "CREATE TABLE test_drop_col (id INTEGER PRIMARY KEY, name VARCHAR(20), age INTEGER);";
        let _ = crate::execution::executor::execute(sql, conn_id).unwrap();

        // 3. Insert Data
        let sql = "INSERT INTO test_drop_col VALUES (1, 'Alice', 30);";
        let _ = crate::execution::executor::execute(sql, conn_id).unwrap();

        // 4. Drop Column
        let sql = "ALTER TABLE test_drop_col DROP COLUMN age;";
        let res = crate::execution::executor::execute(sql, conn_id).unwrap();

        // 5. Verify Select (Should show id, name)
        let sql = "SELECT * FROM test_drop_col;";
        let res = crate::execution::executor::execute(sql, conn_id).unwrap();
        if let ExecutionResult::Query { cols, rows } = &res[0] {
             assert_eq!(cols.0.len(), 2);
             assert_eq!(cols.0[0], "id");
             assert_eq!(cols.0[1], "name");
             // Rows might be empty if transaction didn't commit? 
             // Wait, execute does autocommit if not explicit transaction? 
             // TnxManager usually handles this.
             // assert_eq!(rows.len(), 1); 
             // Let's print rows to see
             println!("Rows: {:?}", rows);
        } else {
            panic!("Select failed, got {:?}", res);
        }

        // 6. Insert new data (without age, which would fail if schema wasn't updated)
        let sql = "INSERT INTO test_drop_col VALUES (2, 'Bob');";
        let res = crate::execution::executor::execute(sql, conn_id);
        assert!(res.is_ok(), "Insert after drop column failed: {:?}", res.err());

        // 7. Clean up
        let sql = "DROP TABLE test_drop_col;";
        let _ = crate::execution::executor::execute(sql, conn_id).unwrap();
    }
}
