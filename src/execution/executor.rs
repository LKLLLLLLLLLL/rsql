use std::collections::{HashSet, HashMap};
use std::sync::{Mutex, LazyLock};
use std::rc::Rc;
use std::cell::RefCell;
use std::thread;

use crate::common::{RsqlResult, RsqlError};
use crate::sql::{Plan, plan::{PlanItem}};
use crate::storage::WAL;
use crate::storage::storage::{Page, StorageManager};
use crate::catalog::sys_catalog::is_sys_table;
use crate::storage::table::get_table_path;
use super::{dml_interpreter::execute_dml_plan_node, ddl_interpreter::execute_ddl_plan_node, dcl_interpreter::execute_dcl_plan_node};
use tracing::info;
use crate::transaction::TnxManager;

static ACTIVE_CONN: LazyLock<Mutex<HashSet<u64>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

struct ExecGuard {
    connection_id: u64,
}

impl ExecGuard {
    fn new(connection_id: u64) -> Self {
        {
            let mut active = ACTIVE_CONN.lock().unwrap();
            // check if exists
            if active.contains(&connection_id) {
                panic!("Connection {} is already active", connection_id);
            }
            active.insert(connection_id);
        }
        ExecGuard { connection_id }
    }
}

impl Drop for ExecGuard {
    fn drop(&mut self) {
        let mut active = ACTIVE_CONN.lock().unwrap();
        active.remove(&self.connection_id);
    }
}

fn commit_transaction(tnx_id: Option<u64>, connection_id: u64) -> RsqlResult<()> {
    if tnx_id.is_none() {
        return Err(RsqlError::InvalidInput("No active transaction to commit".to_string()));
    }
    WAL::global().commit_tnx(tnx_id.unwrap())?;
    TnxManager::global().end_transaction(connection_id);
    Ok(())
}

fn rollback_transaction(tnx_id: Option<u64>, connection_id: u64) -> RsqlResult<()> {
    if tnx_id.is_none() {
        return Err(RsqlError::InvalidInput("No active transaction to rollback".to_string()));
    }
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
    WAL::global().rollback_tnx(
        tnx_id.unwrap(),
        &mut write_page,
        &mut update_page,
        &mut append_page,
        &mut trunc_page,
        &mut max_page_idx,
    )?;
    TnxManager::global().end_transaction(connection_id);
    Ok(())
}

pub fn execute(sql: &str, connection_id: u64) -> RsqlResult<()> {
    // todo!()
    let _guard = ExecGuard::new(connection_id);
    info!("Executing SQL: {}, in thread {:?}", sql, thread::current().id());
    
    let plan = Plan::build_plan(sql)?;
    let mut tnx_id = None;
    for item in plan.items.iter() {
        match item {
            PlanItem::Begin => {
                tnx_id = Some(TnxManager::global().begin_transaction(connection_id));
            },
            PlanItem::Commit => {
                commit_transaction(tnx_id, connection_id)?;
            },
            PlanItem::Rollback => {
                rollback_transaction(tnx_id, connection_id)?;
            },
            PlanItem::DCL(plan_node) => {
                execute_dcl_plan_node(plan_node, connection_id)?;
            },
            PlanItem::DDL(plan_node) => {
                execute_ddl_plan_node(plan_node, connection_id)?;
            },
            PlanItem::DML(plan_node) => {
                execute_dml_plan_node(plan_node, connection_id)?;
            },
        }
    }
    Ok(())
}