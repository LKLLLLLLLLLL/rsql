use std::sync::OnceLock;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64};

use tracing::warn;

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
pub struct TnxManager{
    tnx_counter: AtomicU64,
    tnx_map: Mutex<HashMap<u64, u64>>, // connection_id -> tnx_id
    table_locks: Mutex<HashMap<u64, TableState>>, // table_id -> lock state
    tnx_associated_tables: Mutex<HashMap<u64, (Vec<u64>, Vec<u64>)>>, // tnx_id -> (read_table_ids, write_table_ids)
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
        };
        TNX_MANAGER.set(manager).ok().expect("TnxManager already initialized");
    }
    fn get_tnx_id(&self) -> u64 {
        self.tnx_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    pub fn begin_transaction(
        &self, 
        connection_id: u64,
        read_tables: &[u64],
        write_tables: &[u64],
    ) -> u64 {
        // check if any intersactions in read_tables and write_tables
        let all_tables = {
            let mut set = std::collections::HashSet::new();
            for &t in read_tables {
                set.insert(t);
            }
            for &t in write_tables {
                set.insert(t);
            }
            set
        };
        if all_tables.len() < read_tables.len() + write_tables.len() {
            panic!("Transaction requests conflicting locks on the same table");
        }
        let table_order =  {
            let mut v: Vec<u64> = all_tables.into_iter().collect();
            v.sort_unstable();
            v
        };
        // assign transaction id
        let tnx_id = self.get_tnx_id();
        {
            let mut tnx_map = self.tnx_map.lock().unwrap();
            tnx_map.insert(connection_id, tnx_id);
        }
        // try to acquire all locks
        let mut cursor = 0;
        while cursor < table_order.len() {
            let table_id = table_order[cursor];
            let mut stats = self.table_locks.lock().unwrap();
            let state = stats.entry(table_id).or_insert_with(TableState::new);
            let can_acquire = if read_tables.contains(&table_id) {
                state.try_read()
            } else {
                state.try_write()
            };
            drop(stats); // release lock before potential yield
            if can_acquire {
                cursor += 1;
            } else {
                // yield and retry
                std::thread::yield_now();
            }
        }
        // Record associated tables
        {
            let mut tnx_associated_tables = self.tnx_associated_tables.lock().unwrap();
            let associated_tables = (read_tables.to_vec(), write_tables.to_vec());
            tnx_associated_tables.insert(tnx_id, associated_tables);
        }
        // return
        tnx_id
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
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            TnxManager::init(1);
        });
    }

    #[test]
    fn test_basic_tx() {
        setup();
        let mgr = TnxManager::global();
        let tid = mgr.begin_transaction(1, &[10], &[11]);
        assert_eq!(mgr.get_transaction_id(1), Some(tid));
        mgr.end_transaction(1);
        assert_eq!(mgr.get_transaction_id(1), None);
    }

    #[test]
    fn test_read_sharing() {
        setup();
        let mgr = TnxManager::global();
        // Two transactions reading the same table should succeed
        mgr.begin_transaction(2, &[10], &[]);
        let tid2 = mgr.begin_transaction(3, &[10], &[]);
        assert!(tid2 > 0);
        mgr.end_transaction(2);
        mgr.end_transaction(3);
    }

    #[test]
    fn test_write_exclusive() {
        setup();
        let mgr = Arc::new(TnxManager::global());
        mgr.begin_transaction(4, &[], &[10]); // conn 4 has write lock on 10

        let mgr_clone = mgr.clone();
        let handle = thread::spawn(move || {
            // conn 5 tries to read table 10, should block until conn 4 releases
            mgr_clone.begin_transaction(5, &[10], &[]);
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
        mgr.begin_transaction(6, &[10], &[]); // Tx1 holds Read(10)
        
        let mgr_ref = mgr;
        let t = thread::spawn(move || {
            // Tx2 wants Write(10). Should wait.
            mgr_ref.begin_transaction(7, &[], &[10]);
            mgr_ref.end_transaction(7);
        });

        thread::sleep(std::time::Duration::from_millis(100));
        mgr.end_transaction(6);
        t.join().unwrap();
    }
}