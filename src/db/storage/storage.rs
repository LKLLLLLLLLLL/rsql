use crate::config::{PAGE_SIZE_BYTES, MAX_PAGE_CACHE_BYTES};
use super::super::errors::{RsqlError, RsqlResult};
use super::cache::LRUCache;
use std::sync::{RwLock, Arc, OnceLock};
use std::fs::{self, OpenOptions, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::collections::HashSet;

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

pub struct StorageManager {
    file: File, // file handle
    file_path: String,
    file_page_num: u64, // number of pages in file
    pages: LRUCache,  // cache of pages which has the latest data
}

static STORAGE_MANAGERS: OnceLock<RwLock<HashSet<String>>> = OnceLock::new(); // global single instance registry

impl StorageManager {
    fn get_registry() -> &'static RwLock<HashSet<String>> {
        STORAGE_MANAGERS.get_or_init(|| RwLock::new(HashSet::new()))
    }

    fn register_file_path(file_path: &str) -> RsqlResult<()> {
        let registry = Self::get_registry();
        let mut paths = registry.write()
            .map_err(|_| RsqlError::StorageError("Poisoned RwLock in registry".to_string()))?;
        
        // use insert method, false means the file path already exists
        if !paths.insert(file_path.to_string()) {
            panic!("StorageManager for file {} already exists!", file_path);
        }
        
        Ok(())
    }

    fn unregister_file_path(file_path: &str) {
        if let Ok(mut registry) = Self::get_registry().write() {
            registry.remove(file_path);
        }
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
        let max_file_page_index = if self.file_page_num >= 1 {Some(self.file_page_num - 1)} else {None};
        let max_cached_page_index = self.pages.max_key();

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

    fn write_back_evicted_page(&mut self, evicted: Option<(u64, Arc<RwLock<Page>>)>) -> RsqlResult<()> {
        if let Some((evicted_page_index, evicted_page)) = evicted {
            // write the evicted_page to the file
            let evicted_page = evicted_page.read().map_err(|_| RsqlError::StorageError(
                "Poisoned RwLock in page cache".to_string()
            ))?;
            if evicted_page.need_flush {
                let mut file_page_num = self.file_page_num;
                if evicted_page_index >= self.file_page_num {
                    let required_file_size = (evicted_page_index + 1) * PAGE_SIZE_BYTES as u64;
                    self.file.set_len(required_file_size)?; // extend file (fills with zeros)
                    file_page_num = evicted_page_index + 1;
                }
                let page_data = &evicted_page.data;
                let offset = evicted_page_index * PAGE_SIZE_BYTES as u64;
                self.file.seek(SeekFrom::Start(offset))?;
                self.file.write_all(page_data)?; // write page data to the file
                self.file.sync_data()?; // ensure data is written to the disk
                self.file_page_num = file_page_num; // update pages number in the file
            }
        }
        Ok(())
    }

    pub fn new(file_path: &str) -> RsqlResult<Self> {
        Self::register_file_path(file_path)?; // register file path

        Self::create_file(file_path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)
            .map_err(|e| RsqlError::StorageError(format!("Failed to open file: {}", e)))?;
        let metadata= fs::metadata(file_path).map_err(|e| RsqlError::StorageError(format!("Failed to read file metadata: {}", e)))?;
        let file_size = metadata.len(); // file size in bytes
        if file_size % PAGE_SIZE_BYTES as u64 != 0 {
            Self::unregister_file_path(file_path); // unregister file path
            return Err(RsqlError::StorageError(
                "file size is not aligned to page size".to_string()
            ));
        }
        let file_page_num = file_size / PAGE_SIZE_BYTES as u64;
        Ok(Self {
            file,
            file_path: file_path.to_string(),
            file_page_num,
            pages: LRUCache::new(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES), // cache of pages which has the latest data
        })
    }

    /// deallocate last page
    pub fn free(&mut self) -> RsqlResult<()> {
        let page_idx = match self.max_page_index() {
            Some(num) => num + 1,
            None => return Err(RsqlError::StorageError("No pages to free".to_string())),
        };
        // 1. delete from cache
        if let Some(_) = self.pages.get(&page_idx) {
            self.pages.remove(& (page_idx - 1));
        }
        // 2. truncate file
        let new_file_size = (page_idx + 1) * PAGE_SIZE_BYTES as u64;
        self.file.set_len(new_file_size)?;
        self.file_page_num = page_idx;
        Ok(())
    }

    pub fn read_page(&mut self, page_index: u64) -> RsqlResult<Arc<RwLock<Page>>> {
        self.is_page_index_valid(page_index)?;

        if let Some(page_arc) = self.pages.get(&page_index) {
            Ok(Arc::clone(page_arc))
        } else {
            self.file.seek(SeekFrom::Start(page_index * PAGE_SIZE_BYTES as u64))?; // go to the start position of the page with page_index
            let mut buffer = vec![0u8; PAGE_SIZE_BYTES];
            self.file.read_exact(&mut buffer)?;
            let page = Arc::new(RwLock::new(
                Page {
                    data: buffer,
                    need_flush: false,
                }
            ));

            let evicted = self.pages.insert(page_index, Arc::clone(&page)); // insert the page into cache
            self.write_back_evicted_page(evicted)?;
            Ok(page)
        }
    }

    pub fn write_page(&mut self, page: &Page, page_index: u64) -> RsqlResult<()> {
        self.is_page_index_valid(page_index)?;
        let mut page = page.clone();
        page.need_flush = true;
        let page_arc = Arc::new(RwLock::new(page));
        let evicted = self.pages.insert(page_index, Arc::clone(&page_arc)); // write the page into cache
        self.write_back_evicted_page(evicted)?;
        Ok(())
    }

    pub fn new_page(&mut self) -> RsqlResult<(u64, Arc<RwLock<Page>>)> {
        let new_page_index = match self.max_page_index() {
            Some(max_index) => max_index + 1,
            None => 0,
        };
        let new_page = Arc::new(RwLock::new(Page::new()));
        let evicted = self.pages.insert(new_page_index, Arc::clone(&new_page));
        self.write_back_evicted_page(evicted)?;
        Ok((new_page_index, new_page))
    }

    pub fn flush(&mut self) -> RsqlResult<()> {
        if self.pages.is_empty() {
            return Ok(());
        };

        let max_page_index = match self.max_page_index() {
            Some(idx) => idx,
            None => return Ok(()),
        };

        let required_file_size = (max_page_index + 1) * PAGE_SIZE_BYTES as u64;
        let file_page_num = max_page_index + 1;
        
        let current_file_size = self.file_page_num * PAGE_SIZE_BYTES as u64;
        if current_file_size < required_file_size {
            self.file.set_len(required_file_size)?; // extend file (fills with zeros)
        }
        for (page_index, page_arc) in &self.pages.map {
            let page = page_arc.read().map_err(|_| RsqlError::StorageError(
                "Poisoned RwLock in page cache".to_string()
            ))?;
            if !page.need_flush {
                continue;
            }
            let page_data = &page.data;
            let offset = page_index * PAGE_SIZE_BYTES as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(page_data)?; // write page data to the file
        }
        self.file.flush()?;
        self.file.sync_data()?; // ensure data is written to the disk
        self.file_page_num = file_page_num; // update pages number in the file
        Ok(())
    }
}

// implement Drop trait so that StorageManager will be unregistered when it is dropped
impl Drop for StorageManager {
    fn drop(&mut self) {
        self.flush();
        Self::unregister_file_path(&self.file_path);
    }
}
