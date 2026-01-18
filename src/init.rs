use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

use crate::catalog;
use crate::common::RsqlResult;
use crate::config::DB_DIR;
use crate::storage::WAL;
use crate::storage::storage::{self, StorageManager};
use crate::catalog::sys_catalog::is_sys_table;
use crate::transaction::TnxManager;


fn get_file_path(table_id: u64) -> String {
    if is_sys_table(table_id) {
        format!("{DB_DIR}/sys/{table_id}.dbs")
    } else {
        format!("{DB_DIR}/tables/{table_id}.dbt")
    }
}

fn recovery_wal() -> RsqlResult<u64> {
    let tmp_storages: Rc<RefCell<HashMap<u64, Rc<RefCell<StorageManager>>>>> = Rc::new(RefCell::new(HashMap::new()));
    // Helper closures
    let get_sm = |table_id: u64| -> RsqlResult<Rc<RefCell<StorageManager>>> {
        let mut tmp_storages = tmp_storages.borrow_mut();
        if let Some(sm) = tmp_storages.get(&table_id) {
            Ok(sm.clone())
        } else {
            let file_path = get_file_path(table_id);
            let sm = StorageManager::new(&file_path)?;
            tmp_storages.insert(table_id, Rc::new(RefCell::new(sm)));
            Ok(tmp_storages.get(&table_id).unwrap().clone())
        }
    };
    let mut write_page = |table_id: u64, page_id: u64, data: &[u8]| -> RsqlResult<()> {
        let sm_rc = get_sm(table_id)?;
        let mut sm = sm_rc.borrow_mut();
        let mut page = sm.read_page(page_id)?;
        assert_eq!(storage::Page::max_size(), data.len());
        page.data[..data.len()].copy_from_slice(data);
        sm.write_page(&page, page_id)
    };
    let mut update_page = |table_id: u64, page_id: u64, offset: u64, len: u64, data: &[u8]| -> RsqlResult<()> {
        let sm_rc = get_sm(table_id)?;
        let mut sm = sm_rc.borrow_mut();
        let mut page = sm.read_page(page_id)?;
        assert!(offset + len <= storage::Page::max_size() as u64);
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
    let max_tnx_id = recovery_wal()?;
    TnxManager::init(max_tnx_id + 1);
    catalog::SysCatalog::init()?;
    Ok(())
}