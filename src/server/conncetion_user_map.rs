use std::sync::{Mutex, OnceLock};
use std::collections::HashMap;

static INSTANCE: OnceLock<ConnectionUserMap> = OnceLock::new();

pub struct ConnectionUserMap {
    map: Mutex<HashMap<u64, String>>,
}

impl ConnectionUserMap {
    fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub fn global() -> &'static Self {
        INSTANCE.get_or_init(|| Self::new())
    }

    pub fn init() {
        INSTANCE.get_or_init(|| Self::new());
    }

    pub fn get_username(&self, connection_id: u64) -> Option<String> {
        let map = self.map.lock().unwrap();
        map.get(&connection_id).cloned()
    }

    // pub fn get_all_connections(&self) -> Vec<u64> {
    //     let map = self.map.lock().unwrap();
    //     map.keys().cloned().collect()
    // }

    pub fn insert_connection(&self, connection_id: u64, username: String) {
        let mut map = self.map.lock().unwrap();
        map.insert(connection_id, username);
    }

    pub fn remove_connection(&self, connection_id: u64) {
        let mut map = self.map.lock().unwrap();
        map.remove(&connection_id);
    }
}
