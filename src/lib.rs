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

use tracing::info;

use crate::common::RsqlResult;
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
    let tmp_storages: Rc<RefCell<HashMap<u64, Rc<RefCell<StorageManager>>>>> = Rc::new(RefCell::new(HashMap::new()));
    // Helper closures
    let get_sm = |table_id: u64| -> RsqlResult<Rc<RefCell<StorageManager>>> {
        let mut tmp_storages = tmp_storages.borrow_mut();
        if let Some(sm) = tmp_storages.get(&table_id) {
            Ok(sm.clone())
        } else {
            let file_path = get_table_path(table_id, is_sys_table(table_id));
            let sm = StorageManager::new(file_path.to_str().unwrap())?;
            tmp_storages.insert(table_id, Rc::new(RefCell::new(sm)));
            Ok(tmp_storages.get(&table_id).unwrap().clone())
        }
    };
    let mut write_page = |table_id: u64, page_id: u64, data: &[u8]| -> RsqlResult<()> {
        let sm_rc = get_sm(table_id)?;
        let mut sm = sm_rc.borrow_mut();
        let mut page = sm.read_page(page_id)?;
        assert_eq!(Page::max_size(), data.len());
        page.data[..data.len()].copy_from_slice(data);
        sm.write_page(&page, page_id)
    };
    let mut update_page = |table_id: u64, page_id: u64, offset: u64, len: u64, data: &[u8]| -> RsqlResult<()> {
        let sm_rc = get_sm(table_id)?;
        let mut sm = sm_rc.borrow_mut();
        let mut page = sm.read_page(page_id)?;
        assert!(offset + len <= Page::max_size() as u64);
        page.data[offset as usize..(offset + len) as usize].copy_from_slice(data);
        sm.write_page(&page, page_id)
    };
    let mut append_page = |table_id: u64| -> RsqlResult<u64> {
        let sm_rc = get_sm(table_id)?;
        let mut sm = sm_rc.borrow_mut();
        Ok(sm.new_page()?.0)
    };
    let mut trunc_page = |table_id: u64| -> RsqlResult<()> {
        let sm_rc = get_sm(table_id)?;
        let mut sm = sm_rc.borrow_mut();
        sm.free()?;
        Ok(())
    };
    let mut max_page_idx = |table_id: u64| -> RsqlResult<u64> {
        let sm_rc = get_sm(table_id)?;
        let sm = sm_rc.borrow_mut();
        let max_idx = match sm.max_page_index() {
            Some(idx) => idx,
            None => 0,
        };
        Ok(max_idx)
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
    let max_tnx_id = recovery_wal()?;
    TnxManager::init(max_tnx_id + 1);
    catalog::SysCatalog::init()?;
    info!("Database initialized successfully!");
    Ok(())
}

pub fn run() {
    init_log();
    init_database();
    server::daemon::daemon();
}
