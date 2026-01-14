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

    pub fn write_page(&mut self, page: Page, page_index: u64) -> RsqlResult<()> {
        self.is_page_index_valid(page_index)?;
        let mut page = page;
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

// test
#[cfg(test)]
mod tests {
    use super::super::cache::LRUCache;
    use super::super::storage::{Page, StorageManager};
    use crate::config::{PAGE_SIZE_BYTES, MAX_PAGE_CACHE_BYTES};
    use std::sync::{Arc, RwLock};
    use std::fs;
    use tempfile::NamedTempFile;

    #[test]
    fn test_lru_cache_creation() {
        let cache = LRUCache::new(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES);
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_lru_cache_insert_within_capacity() {
        let mut cache = LRUCache::new(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES);

        for i in 0..MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES {
            let page = Arc::new(RwLock::new(Page {
                data: vec![0u8; PAGE_SIZE_BYTES],
                need_flush: false,
            }));
            let evicted = cache.insert(i as u64, page);
            assert!(evicted.is_none(), "Inserting page {} should not trigger eviction", i);
        }

        assert_eq!(cache.len(), MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES);
    }

    #[test]
    fn test_lru_cache_eviction() {
        let mut cache = LRUCache::new(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES);

        // First fill the cache
        for i in 0..MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES {
            let page = Arc::new(RwLock::new(Page {
                data: vec![0u8; PAGE_SIZE_BYTES],
                need_flush: false,
            }));
            cache.insert(i as u64, page);
        }

        // Insert page 4, should trigger eviction
        let page = Arc::new(RwLock::new(Page {
            data: vec![0u8; PAGE_SIZE_BYTES],
            need_flush: false,
        }));
        let evicted = cache.insert((MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES) as u64, page);
        
        assert!(evicted.is_some(), "Inserting page {} should trigger eviction", MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES);
        assert_eq!(evicted.unwrap().0, 0, "Should evict the earliest inserted page 0");
        assert_eq!(cache.len(), MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES, "Cache size should remain at capacity");
    }

    #[test]
    fn test_lru_cache_access_refresh() {
        let mut cache = LRUCache::new(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES);

        // Insert 3 pages
        for i in 0..MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES {
            let page = Arc::new(RwLock::new(Page {
                data: vec![0u8; PAGE_SIZE_BYTES],
                need_flush: false,
            }));
            cache.insert(i as u64, page);
        }

        // Access the first page, it should become the latest
        let _ = cache.get(&0);

        // Insert page 4, now should evict page 1 (because page 0 was accessed)
        let page = Arc::new(RwLock::new(Page {
            data: vec![0u8; PAGE_SIZE_BYTES],
            need_flush: false,
        }));
        let evicted = cache.insert((MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES) as u64, page);
        
        assert!(evicted.is_some());
        assert_eq!(evicted.unwrap().0, 1, "Should evict page 1 instead of page 0");
    }

    #[test]
    fn test_storage_manager_new() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path().to_str().unwrap();
        
        // Delete temp file, let StorageManager create it
        fs::remove_file(file_path).ok();
        
        let result = StorageManager::new(file_path);
        assert!(result.is_ok(), "StorageManager::new should succeed");
        
        let manager = result.unwrap();
        assert_eq!(manager.file_page_num, 0, "New file should have no pages");
    }

    #[test]
    fn test_storage_manager_new_page() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        let mut manager = StorageManager::new(file_path).unwrap();
        
        // Create a new page
        let result = manager.new_page();
        assert!(result.is_ok(), "Creating new page should succeed");
        
        let (page_index, page) = result.unwrap();
        assert_eq!(page_index, 0, "First new page index should be 0");
        
        // Check page content
        let page_read = page.read().unwrap();
        assert_eq!(page_read.data.len(), PAGE_SIZE_BYTES, "Page size should be correct");
        assert!(page_read.need_flush, "New page should be marked as need_flush");
        
        // Create second page
        let result2 = manager.new_page();
        assert!(result2.is_ok(), "Creating second new page should succeed");
        let (page_index2, _) = result2.unwrap();
        assert_eq!(page_index2, 1, "Second new page index should be 1");
    }

    #[test]
    fn test_storage_manager_read_write_page() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        let mut manager = StorageManager::new(file_path).unwrap();
        
        // Create a new page
        let (page_index, page_arc) = manager.new_page().unwrap();
        
        // Modify page data
        {
            let mut page = page_arc.write().unwrap();
            page.data[0] = 42;
            page.data[1] = 24;
        }
        
        // Write page to storage
        let page_to_write = {
            let page_ref = page_arc.read().unwrap();
            Page {
                data: page_ref.data.clone(),
                need_flush: page_ref.need_flush,
            }
        };
        
        let result = manager.write_page(page_to_write, page_index);
        assert!(result.is_ok(), "Writing page should succeed");
        
        // Read page from cache
        let cached_page_result = manager.read_page(page_index);
        assert!(cached_page_result.is_ok(), "Reading page from cache should succeed");
        
        let cached_page_arc = cached_page_result.unwrap();
        let cached_page_read = cached_page_arc.read().unwrap();
        assert_eq!(cached_page_read.data[0], 42, "Read data should be correct");
        assert_eq!(cached_page_read.data[1], 24, "Read data should be correct");
    }

    #[test]
    fn test_storage_manager_cache_eviction() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        let mut manager = StorageManager::new(file_path).unwrap();
        
        // Create enough pages to trigger cache eviction
        // Note: We need to create MAX_PAGE_CACHE_SIZE + 1 pages to trigger eviction
        let mut pages = Vec::new();
        
        for i in 0..MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES + 1 {
            let (page_index, page_arc) = manager.new_page().unwrap();
            assert_eq!(page_index, i as u64, "Page index should increment");
            
            // Modify page data to mark as dirty
            {
                let mut page = page_arc.write().unwrap();
                page.data[0] = i as u8;
            }
            
            pages.push((page_index, page_arc));
        }
        
        // Force flush to ensure all dirty pages are written to file
        let flush_result = manager.flush();
        assert!(flush_result.is_ok(), "Flush should succeed");
        
        // Verify file size
        let metadata = fs::metadata(file_path).unwrap();
        let expected_size = ((MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES) + 1) as u64 * PAGE_SIZE_BYTES as u64;
        assert_eq!(metadata.len(), expected_size, "File size should be correct");
    }

    #[test]
    fn test_storage_manager_flush() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        let mut manager = StorageManager::new(file_path).unwrap();
        
        // Create several pages and modify them
        for i in 0..5 {
            let (page_index, page_arc) = manager.new_page().unwrap();
            
            // Modify page data
            {
                let mut page = page_arc.write().unwrap();
                page.data[0..4].copy_from_slice(&(i as u32).to_le_bytes());
            }
        }
        
        // Flush to disk
        let flush_result = manager.flush();
        assert!(flush_result.is_ok(), "Flush should succeed");
        
        // Verify file size
        let metadata = fs::metadata(file_path).unwrap();
        let expected_size = 5 * PAGE_SIZE_BYTES as u64;
        assert_eq!(metadata.len(), expected_size, "File size should be correct");
        
        // Reopen file to verify content
        let file_content = fs::read(file_path).unwrap();
        
        // Check first word of each page
        for i in 0..5 {
            let offset = i * PAGE_SIZE_BYTES;
            let page_data = &file_content[offset..offset + 4];
            let value = u32::from_le_bytes([page_data[0], page_data[1], page_data[2], page_data[3]]);
            assert_eq!(value, i as u32, "Page {} data should be correct", i);
        }
    }

    #[test]
    fn test_storage_manager_drop_flush() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        // Create StorageManager in a separate scope
        {
            let mut manager = StorageManager::new(file_path).unwrap();
            
            // Create and modify a page but don't explicitly flush
            let (page_index, page_arc) = manager.new_page().unwrap();
            
            {
                let mut page = page_arc.write().unwrap();
                page.data[0] = 255;
            }
        } // manager is dropped here
        
        // Verify file is created and contains data
        let metadata = fs::metadata(file_path);
        assert!(metadata.is_ok(), "File should exist");
    }

    #[test]
    fn test_page_size_constants() {
        assert_eq!(PAGE_SIZE_BYTES, 4096, "Page size should be 4096 bytes");
        assert!(PAGE_SIZE_BYTES.is_power_of_two(), "Page size should be a power of two");
        assert!(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES > 0, "Cache capacity should be greater than 0");
        assert_eq!(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES, 25600, "Cache capacity should be 25600");
    }

    #[test]
    fn test_read_page_from_disk() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        let mut manager = StorageManager::new(file_path).unwrap();
        
        // Create a page and flush to disk
        let (page_index, page_arc) = manager.new_page().unwrap();
        
        {
            let mut page = page_arc.write().unwrap();
            page.data[0] = 100;
            page.data[1] = 200;
        }
        
        // Flush to disk
        manager.flush().unwrap();
        
        // Clear cache and simulate reading from disk again
        // Note: In a real StorageManager, we don't have a direct method to clear the cache
        // But we can simulate it by creating a new StorageManager instance
        drop(manager);
        
        // Reopen StorageManager
        let mut manager2 = StorageManager::new(file_path).unwrap();
        
        // Read page from disk
        let read_result = manager2.read_page(page_index);
        assert!(read_result.is_ok(), "Reading page from disk should succeed");
        
        let read_page_arc = read_result.unwrap();
        let read_page = read_page_arc.read().unwrap();
        
        assert_eq!(read_page.data[0], 100, "Data read from disk should be correct");
        assert_eq!(read_page.data[1], 200, "Data read from disk should be correct");
        assert!(!read_page.need_flush, "Page read from disk should not be marked as need_flush");
    }
}