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
            assert!(evicted.is_none(), "插入第 {} 个页面不应该触发逐出", i);
        }

        assert_eq!(cache.len(), MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES);
    }

    #[test]
    fn test_lru_cache_eviction() {
        let mut cache = LRUCache::new(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES);

        // 先填满缓存
        for i in 0..MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES {
            let page = Arc::new(RwLock::new(Page {
                data: vec![0u8; PAGE_SIZE_BYTES],
                need_flush: false,
            }));
            cache.insert(i as u64, page);
        }

        // 插入第4个页面，应该触发逐出
        let page = Arc::new(RwLock::new(Page {
            data: vec![0u8; PAGE_SIZE_BYTES],
            need_flush: false,
        }));
        let evicted = cache.insert((MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES) as u64, page);
        
        assert!(evicted.is_some(), "插入第 {} 个页面应该触发逐出", MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES);
        assert_eq!(evicted.unwrap().0, 0, "应该逐出最早插入的页面 0");
        assert_eq!(cache.len(), MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES, "缓存大小应该保持为容量值");
    }

    #[test]
    fn test_lru_cache_access_refresh() {
        let mut cache = LRUCache::new(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES);

        // 插入3个页面
        for i in 0..MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES {
            let page = Arc::new(RwLock::new(Page {
                data: vec![0u8; PAGE_SIZE_BYTES],
                need_flush: false,
            }));
            cache.insert(i as u64, page);
        }

        // 访问第一个页面，它应该变为最新的
        let _ = cache.get(&0);

        // 插入第4个页面，现在应该逐出页面1（因为页面0被访问过）
        let page = Arc::new(RwLock::new(Page {
            data: vec![0u8; PAGE_SIZE_BYTES],
            need_flush: false,
        }));
        let evicted = cache.insert((MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES) as u64, page);
        
        assert!(evicted.is_some());
        assert_eq!(evicted.unwrap().0, 1, "应该逐出页面1而不是页面0");
    }

    #[test]
    fn test_storage_manager_new() {
        let temp_file = NamedTempFile::new().expect("创建临时文件失败");
        let file_path = temp_file.path().to_str().unwrap();
        
        // 删除临时文件，让 StorageManager 自己创建
        fs::remove_file(file_path).ok();
        
        let result = StorageManager::new(file_path);
        assert!(result.is_ok(), "StorageManager::new 应该成功");
        
        let manager = result.unwrap();
        assert_eq!(manager.file_page_num, 0, "新文件应该没有页面");
    }

    #[test]
    fn test_storage_manager_new_page() {
        let temp_file = NamedTempFile::new().expect("创建临时文件失败");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        let mut manager = StorageManager::new(file_path).unwrap();
        
        // 创建一个新页面
        let result = manager.new_page();
        assert!(result.is_ok(), "创建新页面应该成功");
        
        let (page_index, page) = result.unwrap();
        assert_eq!(page_index, 0, "第一个新页面索引应该是 0");
        
        // 检查页面内容
        let page_read = page.read().unwrap();
        assert_eq!(page_read.data.len(), PAGE_SIZE_BYTES, "页面大小应该正确");
        assert!(page_read.need_flush, "新页面应该标记为需要刷新");
        
        // 创建第二个页面
        let result2 = manager.new_page();
        assert!(result2.is_ok(), "创建第二个新页面应该成功");
        let (page_index2, _) = result2.unwrap();
        assert_eq!(page_index2, 1, "第二个新页面索引应该是 1");
    }

    #[test]
    fn test_storage_manager_read_write_page() {
        let temp_file = NamedTempFile::new().expect("创建临时文件失败");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        let mut manager = StorageManager::new(file_path).unwrap();
        
        // 创建一个新页面
        let (page_index, page_arc) = manager.new_page().unwrap();
        
        // 修改页面数据
        {
            let mut page = page_arc.write().unwrap();
            page.data[0] = 42;
            page.data[1] = 24;
        }
        
        // 将页面写入存储
        let page_to_write = {
            let page_ref = page_arc.read().unwrap();
            Page {
                data: page_ref.data.clone(),
                need_flush: page_ref.need_flush,
            }
        };
        
        let result = manager.write_page(page_to_write, page_index);
        assert!(result.is_ok(), "写入页面应该成功");
        
        // 从缓存读取页面 - 修复生命周期问题
        let cached_page_result = manager.read_page(page_index);
        assert!(cached_page_result.is_ok(), "从缓存读取页面应该成功");
        
        let cached_page_arc = cached_page_result.unwrap();
        let cached_page_read = cached_page_arc.read().unwrap();
        assert_eq!(cached_page_read.data[0], 42, "读取的数据应该正确");
        assert_eq!(cached_page_read.data[1], 24, "读取的数据应该正确");
    }

    #[test]
    fn test_storage_manager_cache_eviction() {
        let temp_file = NamedTempFile::new().expect("创建临时文件失败");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        let mut manager = StorageManager::new(file_path).unwrap();
        
        // 创建足够多的页面以触发缓存逐出
        // 注意：我们需要创建 MAX_PAGE_CACHE_SIZE + 1 个页面来触发逐出
        let mut pages = Vec::new();
        
        for i in 0..MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES + 1 {
            let (page_index, page_arc) = manager.new_page().unwrap();
            assert_eq!(page_index, i as u64, "页面索引应该递增");
            
            // 修改页面数据以标记为脏页
            {
                let mut page = page_arc.write().unwrap();
                page.data[0] = i as u8;
            }
            
            pages.push((page_index, page_arc));
        }
        
        // 强制刷新，确保所有脏页都写入文件
        let flush_result = manager.flush();
        assert!(flush_result.is_ok(), "刷新应该成功");
        
        // 验证文件大小
        let metadata = fs::metadata(file_path).unwrap();
        let expected_size = ((MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES) + 1) as u64 * PAGE_SIZE_BYTES as u64;
        assert_eq!(metadata.len(), expected_size, "文件大小应该正确");
    }

    #[test]
    fn test_storage_manager_flush() {
        let temp_file = NamedTempFile::new().expect("创建临时文件失败");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        let mut manager = StorageManager::new(file_path).unwrap();
        
        // 创建几个页面并修改它们
        for i in 0..5 {
            let (page_index, page_arc) = manager.new_page().unwrap();
            
            // 修改页面数据
            {
                let mut page = page_arc.write().unwrap();
                page.data[0..4].copy_from_slice(&(i as u32).to_le_bytes());
            }
        }
        
        // 刷新到磁盘
        let flush_result = manager.flush();
        assert!(flush_result.is_ok(), "刷新应该成功");
        
        // 验证文件大小
        let metadata = fs::metadata(file_path).unwrap();
        let expected_size = 5 * PAGE_SIZE_BYTES as u64;
        assert_eq!(metadata.len(), expected_size, "文件大小应该正确");
        
        // 重新打开文件验证内容
        let file_content = fs::read(file_path).unwrap();
        
        // 检查每个页面的第一个字
        for i in 0..5 {
            let offset = i * PAGE_SIZE_BYTES;
            let page_data = &file_content[offset..offset + 4];
            let value = u32::from_le_bytes([page_data[0], page_data[1], page_data[2], page_data[3]]);
            assert_eq!(value, i as u32, "页面 {} 的数据应该正确", i);
        }
    }

    #[test]
    fn test_storage_manager_drop_flush() {
        let temp_file = NamedTempFile::new().expect("创建临时文件失败");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        // 在单独的作用域中创建 StorageManager
        {
            let mut manager = StorageManager::new(file_path).unwrap();
            
            // 创建并修改一个页面但不显式刷新
            let (page_index, page_arc) = manager.new_page().unwrap();
            
            {
                let mut page = page_arc.write().unwrap();
                page.data[0] = 255;
            }
        } // manager 在这里被 drop
        
        // 验证文件是否被创建且包含数据
        let metadata = fs::metadata(file_path);
        assert!(metadata.is_ok(), "文件应该存在");
    }

    #[test]
    fn test_page_size_constants() {
        assert_eq!(PAGE_SIZE_BYTES, 4096, "页面大小应该为 4096 字节");
        assert!(PAGE_SIZE_BYTES.is_power_of_two(), "页面大小应该是2的幂次方");
        assert!(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES > 0, "缓存容量应该大于0");
        assert_eq!(MAX_PAGE_CACHE_BYTES / PAGE_SIZE_BYTES, 100, "缓存容量应该为100");
    }

    #[test]
    fn test_read_page_from_disk() {
        let temp_file = NamedTempFile::new().expect("创建临时文件失败");
        let file_path = temp_file.path().to_str().unwrap();
        
        fs::remove_file(file_path).ok();
        
        let mut manager = StorageManager::new(file_path).unwrap();
        
        // 创建一个页面并刷新到磁盘
        let (page_index, page_arc) = manager.new_page().unwrap();
        
        {
            let mut page = page_arc.write().unwrap();
            page.data[0] = 100;
            page.data[1] = 200;
        }
        
        // 刷新到磁盘
        manager.flush().unwrap();
        
        // 清除缓存，模拟从磁盘重新读取
        // 注意：在真实的 StorageManager 中，我们没有直接的方法来清除缓存
        // 但我们可以通过创建新的 StorageManager 实例来模拟
        drop(manager);
        
        // 重新打开 StorageManager
        let mut manager2 = StorageManager::new(file_path).unwrap();
        
        // 从磁盘读取页面
        let read_result = manager2.read_page(page_index);
        assert!(read_result.is_ok(), "从磁盘读取页面应该成功");
        
        let read_page_arc = read_result.unwrap();
        let read_page = read_page_arc.read().unwrap();
        
        assert_eq!(read_page.data[0], 100, "从磁盘读取的数据应该正确");
        assert_eq!(read_page.data[1], 200, "从磁盘读取的数据应该正确");
        assert!(!read_page.need_flush, "从磁盘读取的页面不应该标记为需要刷新");
    }
}