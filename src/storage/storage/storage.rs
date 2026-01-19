use crate::config::{PAGE_SIZE_BYTES, MAX_PAGE_CACHE_BYTES};
use crate::common::{RsqlError, RsqlResult};
use super::cache::LRUCache;
use std::sync::{RwLock, Mutex, Arc, OnceLock, Weak};
use std::fs::{self, OpenOptions, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Page {
    pub data: Vec<u8>,
    need_flush: bool,
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: vec![0u8; PAGE_SIZE_BYTES],
            need_flush: true, 
        }
    }

    pub fn max_size() -> usize {
        PAGE_SIZE_BYTES
    }
}

static STORAGE_REGISTRY: OnceLock<RwLock<HashMap<String, Weak<Mutex<StorageManager>>>>> = OnceLock::new(); // global single instance registry

pub struct StorageManager {
    file: Mutex<File>, // file handle
    file_path: String,
    file_page_num: Mutex<u64>, // number of pages in file
    pages: Mutex<LRUCache>,  // cache of pages which has the latest data
}

// implement Drop trait so that StorageManager will be unregistered when it is dropped
impl Drop for StorageManager {
    fn drop(&mut self) {
        let _ = self.flush(); // don't forget to flush
        if let Some(registry) = STORAGE_REGISTRY.get() {
            let mut write_guard = registry.write().unwrap();
            write_guard.remove(&self.file_path);
        }
    }
}

impl StorageManager {
    fn get_registry() -> &'static RwLock<HashMap<String, Weak<Mutex<StorageManager>>>> {
        STORAGE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
    }

    fn create_file<P: AsRef<Path>>(path: P) -> RsqlResult<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        } // create dirs if not exist
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            Err(e) => Err(RsqlError::StorageError(format!("Failed to create file: {}", e))),
        }   // create file if not exist
    }

    pub fn max_page_index(&self) -> Option<u64> {
        let max_file_page_index = if self.file_page_num.lock().unwrap().clone() >= 1 {Some(self.file_page_num.lock().unwrap().clone() - 1)} else {None};
        let max_cached_page_index = self.pages.lock().unwrap().max_key();

        match (max_file_page_index, max_cached_page_index) {
            (None, None) => None,
            (Some(f), None) => Some(f),
            (None, Some(c)) => Some(c),
            (Some(f), Some(c)) => Some(f.max(c)),
        } // return the max page index of file and cache(None means empty file and cache)
    }

    fn is_page_index_valid(&self, page_index: u64) -> RsqlResult<()> {
        let max_page_index = self.max_page_index();
        match max_page_index {
            Some(max_index) if page_index <= max_index => Ok(()),
            _ => Err(RsqlError::StorageError(
                "page index out of bounds".to_string()
            )),
        }
    }

    fn write_back_evicted_page(&self, evicted: Option<(u64, Arc<RwLock<Page>>)>) -> RsqlResult<()> {
        if let Some((evicted_page_index, evicted_page)) = evicted {
            // write the evicted_page to the file
            let mut evicted_page = evicted_page.write().map_err(|_| RsqlError::StorageError(
                "Poisoned RwLock in page cache".to_string()
            ))?;
            if evicted_page.need_flush {
                let mut file_page_num = self.file_page_num.lock().unwrap();
                if evicted_page_index >= *file_page_num {
                    let required_file_size = (evicted_page_index + 1) * PAGE_SIZE_BYTES as u64;
                    self.file.lock().unwrap().set_len(required_file_size)?; // extend file (fills with zeros)
                    *file_page_num = evicted_page_index + 1;
                }
                let page_data = &evicted_page.data;
                let offset = evicted_page_index * PAGE_SIZE_BYTES as u64;
                self.file.lock().unwrap().seek(SeekFrom::Start(offset))?;
                self.file.lock().unwrap().write_all(page_data)?; // write page data to the file
                self.file.lock().unwrap().sync_data()?; // ensure data is written to the disk
                evicted_page.need_flush = false;
            }
        }
        Ok(())
    }

    pub fn new(file_path: &str) -> RsqlResult<Arc<Mutex<Self>>> {
        let registry = Self::get_registry();
        
        // 1. check if already exists
        {
            let read_guard = registry.read().unwrap();
            if let Some(weak_ref) = read_guard.get(file_path) {
                if weak_ref.strong_count() > 0 {
                    panic!("StorageManager for file {} already exists!", file_path);
                }
            }
        }

        // 2. create new StorageManager
        Self::create_file(file_path)?;
        let file = OpenOptions::new().read(true).write(true).open(file_path)?;
        let metadata = fs::metadata(file_path)?;
        let file_page_num = metadata.len() / PAGE_SIZE_BYTES as u64;

        let manager = Arc::new(Mutex::new(Self {
            file: Mutex::new(file),
            file_path: file_path.to_string(),
            file_page_num: Mutex::new(file_page_num),
            pages: Mutex::new(LRUCache::new(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES)),
        }));

        // 3. register the new StorageManager
        registry.write().unwrap().insert(
            file_path.to_string(),
            Arc::downgrade(&manager),
        );

        Ok(manager)
    }

    /// deallocate last page
    pub fn free(&mut self) -> RsqlResult<u64> {
        let page_idx = match self.max_page_index() {
            Some(num) => num,
            None => return Err(RsqlError::StorageError("No pages to free".to_string())),
        };
        // 1. delete from cache
        self.pages.lock().unwrap().remove(&page_idx);
        // 2. truncate file
        let new_file_size = (page_idx) * PAGE_SIZE_BYTES as u64;
        self.file.lock().unwrap().set_len(new_file_size)?;
        *self.file_page_num.lock().unwrap() = page_idx;
        Ok(page_idx)
    }

    pub fn read_page(&self, page_index: u64) -> RsqlResult<Page> {
        self.is_page_index_valid(page_index)?;

        if let Some(page_arc) = self.pages.lock().unwrap().get(&page_index) {
            let page = page_arc.read().unwrap().clone();
            Ok(page)
        } else {
            self.file.lock().unwrap().seek(SeekFrom::Start(page_index * PAGE_SIZE_BYTES as u64))?; // go to the start position of the page with page_index
            let mut buffer = vec![0u8; PAGE_SIZE_BYTES];
            self.file.lock().unwrap().read_exact(&mut buffer)?;
            let page = Page {
                data: buffer,
                need_flush: false,
            };
            let evicted = self.pages.lock().unwrap().insert(page_index, Arc::new(RwLock::new(page.clone()))); // insert the page into cache
            self.write_back_evicted_page(evicted)?;
            Ok(page)
        }
    }

    pub fn write_page(&mut self, page: &Page, page_index: u64) -> RsqlResult<()> {
        self.is_page_index_valid(page_index)?;
        let mut page = page.clone();
        page.need_flush = true;
        let page_arc = Arc::new(RwLock::new(page));
        let evicted = self.pages.lock().unwrap().insert(page_index, Arc::clone(&page_arc)); // write the page into cache
        self.write_back_evicted_page(evicted)?;
        Ok(())
    }

    pub fn new_page(&mut self) -> RsqlResult<(u64, Page)> {
        let new_page_index = match self.max_page_index() {
            Some(max_index) => max_index + 1,
            None => 0,
        };
        let new_page = Page::new();
        let evicted = self.pages.lock().unwrap().insert(new_page_index, Arc::new(RwLock::new(new_page.clone())));
        self.write_back_evicted_page(evicted)?;
        Ok((new_page_index, new_page))
    }

    pub fn flush(&mut self) -> RsqlResult<()> {
        if self.pages.lock().unwrap().is_empty() {
            return Ok(());
        };

        let max_page_index = match self.max_page_index() {
            Some(idx) => idx,
            None => return Ok(()),
        };

        let required_file_size = (max_page_index + 1) * PAGE_SIZE_BYTES as u64;
        let file_page_num = max_page_index + 1;

        let current_file_size = *self.file_page_num.lock().unwrap() * PAGE_SIZE_BYTES as u64;
        if current_file_size < required_file_size {
            self.file.lock().unwrap().set_len(required_file_size)?; // extend file (fills with zeros)
        }
        for (page_index, page_arc) in &self.pages.lock().unwrap().map {
            let mut page = page_arc.write().map_err(|_| RsqlError::StorageError(
                "Poisoned RwLock in page cache".to_string()
            ))?;
            if !page.need_flush {
                continue;
            }
            let page_data = &page.data;
            let offset = page_index * PAGE_SIZE_BYTES as u64;
            self.file.lock().unwrap().seek(SeekFrom::Start(offset))?;
            self.file.lock().unwrap().write_all(page_data)?; // write page data to the file
            page.need_flush = false;
        }
        self.file.lock().unwrap().flush()?;
        self.file.lock().unwrap().sync_data()?; // ensure data is written to the disk
        *self.file_page_num.lock().unwrap() = file_page_num; // update pages number in the file
        Ok(())
    }
    pub fn flush_all() -> RsqlResult<()> {
        let registry = Self::get_registry();
        let read_guard = registry.read().unwrap();
        for weak_ref in read_guard.values() {
            if let Some(strong_ref) = weak_ref.upgrade() {
                let mut sm = strong_ref.lock().unwrap();
                sm.flush()?;
            }
        }
        Ok(())
    }
}
