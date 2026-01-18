use std::collections::HashMap;
use std::sync::{Mutex, Condvar, OnceLock};
use std::sync::atomic::{AtomicU64};

use crate::common::{RsqlResult, RsqlError};
use crate::config::LOCK_TIMEOUT_MS;

const TIME_OUT: std::time::Duration = std::time::Duration::from_millis(LOCK_TIMEOUT_MS);
static TNX_MANAGER: OnceLock<TnxManager> = OnceLock::new();

struct TableState {
    writers: u64,
    readers: u64,
}

impl TableState {
    pub fn new() -> Self {
        TableState {
            writers: 0,
            readers: 0,
        }
    }
    pub fn try_read(&mut self) -> bool {
        if self.writers > 0 {
            return false;
        }
        self.readers += 1;
        true
    }
    pub fn try_write(&mut self) -> bool {
        if self.writers > 0 || self.readers > 0 {
            return false;
        }
        self.writers += 1;
        true
    }
    pub fn try_upgrade(&mut self) -> bool {
        if self.readers != 1 || self.writers > 0 {
            return false;
        }
        self.readers -= 1;
        self.writers += 1;
        true
    }
    pub fn release_read(&mut self) {
        if self.readers > 0 {
            self.readers -= 1;
        }
    }
    pub fn release_write(&mut self) {
        if self.writers > 0 {
            self.writers -= 1;
        }
    }
}

/// Transaction Manager
/// Singleton struct
/// Use 2pl protocol for concurrency control
/// Use timeout and rollback for deadlock handling
pub struct TnxManager{
    tnx_counter: AtomicU64,
    tnx_map: Mutex<HashMap<u64, u64>>, // connection_id -> tnx_id
    table_locks: Mutex<HashMap<u64, TableState>>, // table_id -> lock state
    tnx_associated_tables: Mutex<HashMap<u64, (Vec<u64>, Vec<u64>)>>, // tnx_id -> (read_table_ids, write_table_ids)
    lock_condvar: Condvar,
}

impl TnxManager {
    pub fn global() -> &'static TnxManager {
        TNX_MANAGER.get().expect("TnxManager not initialized")
    }
    pub fn init(start_tnx_id: u64) {
        let manager = TnxManager {
            tnx_counter: AtomicU64::new(start_tnx_id),
            tnx_map: Mutex::new(HashMap::new()),
            table_locks: Mutex::new(HashMap::new()),
            tnx_associated_tables: Mutex::new(HashMap::new()),
            lock_condvar: Condvar::new(),
        };
        if cfg!(test) {
            match TNX_MANAGER.set(manager) {
                Ok(_) => {},
                Err(_) => {} // allow multiple init in tests
            }
        } else {
            TNX_MANAGER.set(manager).ok().expect("TnxManager already initialized");
        }
    }
    fn new_tnx_id(&self) -> u64 {
        self.tnx_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    /// Begin a new transaction for the given connection ID
    /// Returns the assigned transaction ID
    pub fn begin_transaction(
        &self, 
        connection_id: u64,
    ) -> u64 {
        // assign transaction id
        let tnx_id = self.new_tnx_id();
        {
            let mut tnx_map = self.tnx_map.lock().unwrap();
            tnx_map.insert(connection_id, tnx_id);
        }
        // insert record to associated tables
        {
            let mut tnx_associated_tables = self.tnx_associated_tables.lock().unwrap();
            tnx_associated_tables.insert(tnx_id, (Vec::new(), Vec::new()));
        }
        // return
        tnx_id
    }
    /// Acquire read locks on the given table IDs for the transaction
    /// Returns true if all locks are acquired
    /// If failed, the caller should rollback and retry
    pub fn acquire_read_locks(
        &self,
        tnx_id: u64,
        table_ids: &[u64],
    ) -> RsqlResult<()> {
        // try acquire all locks
        let mut stats = self.table_locks.lock().unwrap();
        let mut acquired = vec![];
        for &table_id in table_ids {
            // check if this transaction already has lock on this table
            if let Some((reads, writes)) = self.tnx_associated_tables.lock().unwrap().get(&tnx_id) {
                if reads.contains(&table_id) || writes.contains(&table_id) {
                    continue; // already has lock
                }
            }
            while !stats.entry(table_id).or_insert_with(TableState::new).try_read() {
                let (new_stats, wait_res) = self.lock_condvar.wait_timeout(stats, TIME_OUT).unwrap();
                stats = new_stats;
                if wait_res.timed_out() {
                    // timeout, release all acquired locks
                    for &t_id in &acquired {
                        if let Some(state) = stats.get_mut(&t_id) {
                            state.release_read();
                        }
                    }
                    self.lock_condvar.notify_all();
                    return Err(RsqlError::LockError(format!(
                        "Timeout acquiring read lock on table {}", table_id
                    )));
                }
            }
            acquired.push(table_id);
        }
        // record associated tables
        {
            let mut tnx_associated_tables = self.tnx_associated_tables.lock().unwrap();
            let entry = tnx_associated_tables.get_mut(&tnx_id).expect("Associated tables not found for transaction");
            for &table_id in table_ids {
                entry.0.push(table_id);
            }
        }
        Ok(())
    }
    pub fn acquire_write_locks(
        &self,
        tnx_id: u64,
        table_ids: &[u64],
    ) -> RsqlResult<()> {
        let mut stats = self.table_locks.lock().unwrap();
        for &table_id in table_ids {
            // check if this transaction already has lock on this table
            let (has_read, has_write) = {
                let assoc = self.tnx_associated_tables.lock().unwrap();
                let (reads, writes) = assoc.get(&tnx_id).ok_or_else(|| {
                    RsqlError::LockError("Transaction info missing".to_string())
                })?;
                (reads.contains(&table_id), writes.contains(&table_id))
            };
            // already has write lock
            if has_write {
                continue;
            }
            // need to upgrade from read lock to write lock
            if has_read {
                while !stats.entry(table_id).or_insert_with(TableState::new).try_upgrade() {
                    let (new_stats, wait_res) = self.lock_condvar.wait_timeout(stats, TIME_OUT).unwrap();
                    stats = new_stats;
                    if wait_res.timed_out() {
                        return Err(RsqlError::LockError(format!(
                            "Timeout upgrading to write lock on table {}", table_id
                        )));
                    }
                }
                // remove from associated read tables, add to write tables
                let mut assoc = self.tnx_associated_tables.lock().unwrap();
                if let Some((reads, writes)) = assoc.get_mut(&tnx_id) {
                    reads.retain(|&id| id != table_id);
                    writes.push(table_id);
                }
            } else {
                // need to acquire write lock directly
                while !stats.entry(table_id).or_insert_with(TableState::new).try_write() {
                    let (new_stats, wait_res) = self.lock_condvar.wait_timeout(stats, TIME_OUT).unwrap();
                    stats = new_stats;
                    
                    if wait_res.timed_out() {
                        return Err(RsqlError::LockError(format!(
                            "Timeout acquiring write lock on table {}", table_id
                        )));
                    }
                }
                
                let mut assoc = self.tnx_associated_tables.lock().unwrap();
                if let Some((_, writes)) = assoc.get_mut(&tnx_id) {
                    writes.push(table_id);
                }
            }
        }
        Ok(())
    }
    pub fn end_transaction(&self, connection_id: u64) {
        let tnx_id = {
            let mut tnx_map = self.tnx_map.lock().unwrap();
            tnx_map.remove(&connection_id).expect("Transaction ID not found for connection")
        };
        // find all associated tables
        // and remove entry in tnx_associated_tables
        let (reads, writes) = {
            let mut tnx_associated_tables = self.tnx_associated_tables.lock().unwrap();
            tnx_associated_tables.remove(&tnx_id).expect("Associated tables not found for transaction")
        };
        // release all locks
        let mut stats = self.table_locks.lock().unwrap();
        for table_id in reads {
            if let Some(state) = stats.get_mut(&table_id) {
                state.release_read();
            }
        }
        for table_id in writes {
            if let Some(state) = stats.get_mut(&table_id) {
                state.release_write();
            }
        }
        // notify waiting threads that locks might be available
        self.lock_condvar.notify_all();
    }
    pub fn get_transaction_id(&self, connection_id: u64) -> Option<u64> {
        let tnx_map = self.tnx_map.lock().unwrap();
        tnx_map.get(&connection_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::Arc;

    fn setup() {
        TnxManager::init(1);
    }

    #[test]
    fn test_basic_tx() {
        setup();
        let mgr = TnxManager::global();
        let tid = mgr.begin_transaction(1);
        mgr.acquire_read_locks(tid, &[10]).unwrap();
        mgr.acquire_write_locks(tid, &[11]).unwrap();
        assert_eq!(mgr.get_transaction_id(1), Some(tid));
        mgr.end_transaction(1);
        assert_eq!(mgr.get_transaction_id(1), None);
    }

    #[test]
    fn test_read_sharing() {
        setup();
        let mgr = TnxManager::global();
        // Two transactions reading the same table should succeed
        let tid1 = mgr.begin_transaction(2);
        mgr.acquire_read_locks(tid1, &[10]).unwrap();
        let tid2 = mgr.begin_transaction(3);
        mgr.acquire_read_locks(tid2, &[10]).unwrap();
        assert!(tid2 > 0);
        mgr.end_transaction(2);
        mgr.end_transaction(3);
    }

    #[test]
    fn test_write_exclusive() {
        setup();
        let mgr = Arc::new(TnxManager::global());
        let tid4 = mgr.begin_transaction(4);
        mgr.acquire_write_locks(tid4, &[10]).unwrap(); // conn 4 has write lock on 10

        let mgr_clone = mgr.clone();
        let handle = thread::spawn(move || {
            // conn 5 tries to read table 10, should block until conn 4 releases
            let tid5 = mgr_clone.begin_transaction(5);
            mgr_clone.acquire_read_locks(tid5, &[10]).unwrap();
            5
        });

        thread::sleep(std::time::Duration::from_millis(100));
        // at this point the thread should still be yielding
        mgr.end_transaction(4); // release lock

        let result_tid = handle.join().unwrap();
        assert_eq!(result_tid, 5);
        mgr.end_transaction(5);
    }

    #[test]
    fn test_multi_table_conflict() {
        setup();
        let mgr = TnxManager::global();
        let tid6 = mgr.begin_transaction(6);
        mgr.acquire_read_locks(tid6, &[10]).unwrap(); // Tx1 holds Read(10)

        let mgr_ref = mgr;
        let t = thread::spawn(move || {
            // Tx2 wants Write(10). Should wait.
            let tid7 = mgr_ref.begin_transaction(7);
            mgr_ref.acquire_write_locks(tid7, &[10]).unwrap();
            mgr_ref.end_transaction(7);
        });

        thread::sleep(std::time::Duration::from_millis(100));
        mgr.end_transaction(6);
        t.join().unwrap();
    }

    #[test]
    fn test_deadlock_detection() {
        setup();
        let mgr = Arc::new(TnxManager::global());

        let mgr_a = mgr.clone();
        let handle_a = thread::spawn(move || {
            let tid = mgr_a.begin_transaction(8);
            // acquire write on table 1000 first
            assert!(mgr_a.acquire_write_locks(tid, &[1000]).is_ok());
            // give other tx a chance to acquire conflicting lock
            thread::sleep(std::time::Duration::from_millis(100));
            // then try to acquire write on table 1001 -> may deadlock
            let res = mgr_a.acquire_write_locks(tid, &[1001]).is_ok();
            mgr_a.end_transaction(8);
            res
        });

        let mgr_b = mgr.clone();
        let handle_b = thread::spawn(move || {
            let tid = mgr_b.begin_transaction(9);
            // acquire write on table 1001 first
            assert!(mgr_b.acquire_write_locks(tid, &[1001]).is_ok());
            thread::sleep(std::time::Duration::from_millis(100));
            // then try to acquire write on table 1000 -> may deadlock
            let res = mgr_b.acquire_write_locks(tid, &[1000]).is_ok();
            mgr_b.end_transaction(9);
            res
        });

        let r_a = handle_a.join().unwrap();
        let r_b = handle_b.join().unwrap();
        // At least one should fail to acquire the second lock due to timeout/deadlock handling
        assert!(!(r_a && r_b), "Both transactions acquired conflicting locks; deadlock not detected");
    }

    #[test]
    fn test_complex_deadlocks() {
        setup();
        let mgr = Arc::new(TnxManager::global());

        // Use several tables and more threads to simulate a complex contention
        let tables: Vec<u64> = vec![2000, 2001, 2002, 2003, 2004];
        let thread_count = 8;
        let mut handles = Vec::new();

        for i in 0..thread_count {
            let mgr_c = mgr.clone();
            let tables = tables.clone();
            handles.push(thread::spawn(move || {
                let conn_id = 3000 + i as u64;
                let tid = mgr_c.begin_transaction(conn_id);
                // Each thread locks a pair of adjacent tables in a ring
                let t1 = tables[i % tables.len()];
                let t2 = tables[(i + 1) % tables.len()];
                // acquire first write lock
                let ok1 = mgr_c.acquire_write_locks(tid, &[t1]).is_ok();
                if !ok1 {
                    mgr_c.end_transaction(conn_id);
                    return false;
                }
                // small delay to increase chance of overlap
                thread::sleep(std::time::Duration::from_millis(50));
                // try to acquire second write lock
                let ok2 = mgr_c.acquire_write_locks(tid, &[t2]).is_ok();
                mgr_c.end_transaction(conn_id);
                ok1 && ok2
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(h.join().unwrap());
        }

        let successes = results.iter().filter(|&&r| r).count();
        // In a realistic contention scenario some threads should fail to get the second lock
        assert!(successes < thread_count, "All threads unexpectedly acquired both locks (no deadlock/timout observed)");
    }
}