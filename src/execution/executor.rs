use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex, LazyLock};
use std::rc::Rc;
use std::cell::RefCell;
use std::{thread, vec};

use crate::catalog::SysCatalog;
use crate::common::{RsqlResult, RsqlError};
use crate::execution::result::ExecutionResult;
use crate::sql::{Plan, plan::{PlanItem}};
use crate::storage::WAL;
use crate::storage::storage::{Page, StorageManager};
use crate::catalog::sys_catalog::is_sys_table;
use crate::storage::table::get_table_path;
use super::{dml_interpreter::execute_dml_plan_node, ddl_interpreter::execute_ddl_plan_node, dcl_interpreter::execute_dcl_plan_node};
use tracing::{info, warn};
use crate::transaction::TnxManager;
use crate::config::LOCK_MAX_RETRY;

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

fn commit_transaction(connection_id: u64) -> RsqlResult<()> {
    let tnx_id = TnxManager::global().get_transaction_id(connection_id);
    // Check if there is an active transaction
    if tnx_id.is_none() {
        return Err(RsqlError::InvalidInput("No active transaction to commit".to_string()));
    }
    WAL::global().commit_tnx(tnx_id.unwrap())?;
    TnxManager::global().end_transaction(connection_id);
    Ok(())
}

fn rollback_transaction(connection_id: u64) -> RsqlResult<()> {
    let tnx_id = TnxManager::global().get_transaction_id(connection_id);
    if tnx_id.is_none() {
        return Err(RsqlError::InvalidInput("No active transaction to rollback".to_string()));
    }
    let tmp_storages: Rc<RefCell<HashMap<u64, Arc<Mutex<StorageManager>>>>> = Rc::new(RefCell::new(HashMap::new()));
    // Helper closures
    let get_sm = |table_id: u64| -> RsqlResult<Arc<Mutex<StorageManager>>> {
        let mut tmp_storages = tmp_storages.borrow_mut();
        if let Some(sm) = tmp_storages.get(&table_id) {
            Ok(sm.clone())
        } else if is_sys_table(table_id) {
            Ok(SysCatalog::global().get_storage(table_id))
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
    let mut max_page_idx = |table_id: u64| -> RsqlResult<u64> {
        let sm_rc = get_sm(table_id)?;
        let sm = sm_rc.lock().unwrap();
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

fn execute_inner(sql: &str, connection_id: u64) -> RsqlResult<Vec<ExecutionResult>> {
    let plan = Plan::build_plan(sql)?;
    let mut results = vec![];
    for item in plan.items.iter() {
        match item {
            PlanItem::Begin => {
                TnxManager::global().begin_transaction(connection_id);
                results.push(ExecutionResult::TnxBeginSuccess);
            },
            PlanItem::Commit => {
                commit_transaction(connection_id)?;
                results.push(ExecutionResult::CommitSuccess);
            },
            PlanItem::Rollback => {
                rollback_transaction(connection_id)?;
                results.push(ExecutionResult::RollbackSuccess);
            },
            PlanItem::DCL(plan_node) => {
                let tnx_id = TnxManager::global().get_transaction_id(connection_id);
                let mut auto_tnx = false;
                let tnx_id = match tnx_id {
                    Some(id) => id,
                    None => {
                        // auto begin transaction
                        auto_tnx = true;
                        TnxManager::global().begin_transaction(connection_id)
                    },
                };
                let res = execute_dcl_plan_node(plan_node, tnx_id)?;
                if auto_tnx {
                    commit_transaction(connection_id)?;
                };
                results.push(res);
            },
            PlanItem::DDL(plan_node) => {
                let tnx_id = TnxManager::global().get_transaction_id(connection_id);
                let mut auto_tnx = false;
                let tnx_id = match tnx_id {
                    Some(id) => id,
                    None => {
                        // auto begin transaction
                        auto_tnx = true;
                        TnxManager::global().begin_transaction(connection_id)
                    },
                };
                let res = execute_ddl_plan_node(plan_node, tnx_id)?;
                if auto_tnx {
                    commit_transaction(connection_id)?;
                };
                results.push(res);
            },
            PlanItem::DML(plan_node) => {
                let tnx_id = TnxManager::global().get_transaction_id(connection_id);
                let mut auto_tnx = false;
                let tnx_id = match tnx_id {
                    Some(id) => id,
                    None => {
                        // auto begin transaction
                        auto_tnx = true;
                        TnxManager::global().begin_transaction(connection_id)
                    },
                };
                let res = execute_dml_plan_node(plan_node, tnx_id, false)?;
                let res = res.to_exec_result()?;
                if auto_tnx {
                    commit_transaction(connection_id)?;
                };
                results.push(res);
            },
        }
    };
    Ok(results)
}

/// Execute a SQL statement
pub fn execute(sql: &str, connection_id: u64) -> RsqlResult<Vec<ExecutionResult>> {
    let _guard = ExecGuard::new(connection_id);
    info!("Executing SQL: {}, in thread {:?}", sql, thread::current().id());
    let mut retry_count = 0;
    while retry_count < LOCK_MAX_RETRY {
        let exec_res = execute_inner(sql, connection_id);
        match exec_res {
            Ok(res) => {
                info!("SQL {} in thread {:?} executed successfully", sql, thread::current().id());
                return Ok(res);
            },
            Err(RsqlError::LockError(e)) => {
                warn!("SQL {} execution in thread {:?} failed due to lock error: {}\n\
                    retry it!", sql, thread::current().id(), e);
                if TnxManager::global().get_transaction_id(connection_id).is_some() {
                    rollback_transaction(connection_id)?;
                }
                // continue to retry
            }
            Err(e) => {
                warn!("SQL {} execution in thread {:?} failed: {}", sql, thread::current().id(), e);
                if TnxManager::global().get_transaction_id(connection_id).is_some() {
                    rollback_transaction(connection_id)?;
                }
                return Err(e);
            },
        }
        retry_count += 1;
    };
    Err(RsqlError::LockError("Maximum retry limit reached".to_string()))
}

/// Check if checkpoint is needed
/// If needed, perform a checkpoint operation
pub fn checkpoint() -> RsqlResult<()> {
    let need_checkpoint = WAL::global().need_checkpoint();
    if !need_checkpoint {
        info!("No checkpoint needed at this time.");
        return Ok(());
    }
    // Perform checkpoint operation
    info!("Performing checkpoint operation.");
    // Execute WAL operation
    WAL::global().checkpoint(&|| {
        StorageManager::flush_all()
    })?;
    Ok(())
}

/// Validate user credentials
pub fn validate_user(username: &str, password: &str) -> RsqlResult<bool> {
    let tnx_id = TnxManager::global().begin_transaction(1);
    let is_valid = SysCatalog::global().validate_user(tnx_id, username, password)?;
    TnxManager::global().end_transaction(1);
    Ok(is_valid)
}

/// Callback function when a connection is disconnected
/// Will automatically rollback any active transaction for the connection
pub fn disconnect_callback(connection_id: u64) -> RsqlResult<()> {
    let tnx_id_opt = TnxManager::global().get_transaction_id(connection_id);
    if tnx_id_opt.is_some() {
        warn!("Connection {} disconnected with active transaction, rolling back...", connection_id);
        rollback_transaction(connection_id)?;
    };
    Ok(())
}
