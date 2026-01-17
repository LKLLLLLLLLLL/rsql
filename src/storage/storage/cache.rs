use linked_hash_map::LinkedHashMap;
use super::Page;
use std::sync::{RwLock, Arc};

pub struct LRUCache {
    pub map: LinkedHashMap<u64, Arc<RwLock<Page>>>,
    capacity: usize,
}

impl LRUCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            map: LinkedHashMap::new(),
            capacity,
        }
    }

    // get the latest page and move it to the back
    pub fn get(&mut self, key: &u64) -> Option<&mut Arc<RwLock<Page>>> {
        self.map.get_refresh(key)
    }

    // insert value, if the cache is full, remove the oldest one
    pub fn insert(&mut self, key: u64, value: Arc<RwLock<Page>>) -> Option<(u64, Arc<RwLock<Page>>)> {
        // if exists, remove it first
        self.map.remove(&key);
        
        // insert (become the latest automatically)
        self.map.insert(key, value);
        
        // if exceeds capacity, remove the oldest one
        let evicted;
        if self.map.len() > self.capacity {
            evicted = self.map.pop_front();
        } else {
            evicted = None;
        }
        evicted
    }

    /// remove a page from cache
    /// WARN: if the page is dirty, it will be lost!
    pub fn remove(&mut self, key: &u64) -> Option<Arc<RwLock<Page>>> {
        self.map.remove(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn max_key(&self) -> Option<u64> {
        self.map.keys().max().copied()
    }
}