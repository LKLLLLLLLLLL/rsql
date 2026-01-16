use std::sync::Arc;

use crate::db::errors::RsqlResult;

use super::storage::{StorageManager, Page};
use super::wal::WAL;
/// This struct implements a consistent storage engine for the database.
pub struct ConsistStorageEngine {
    table_id: u64,
    storage_manager: StorageManager,
    wal: Arc<WAL>,
}

impl ConsistStorageEngine {
    pub fn new(file_path: &str, table_id: u64) -> RsqlResult<Self> {
        let storage_manager = StorageManager::new(file_path)?;
        let wal = WAL::global();
        Ok(ConsistStorageEngine {
            table_id,
            storage_manager,
            wal,
        })
    }
    pub fn read(&self, page_id: u64) -> RsqlResult<Page> {
        self.storage_manager.read_page(page_id)
    }
    pub fn read_bytes(&self, page_id: u64, offset: usize, size: usize) -> RsqlResult<Vec<u8>> {
        let page = self.storage_manager.read_page(page_id)?;
        Ok(page.data[offset..offset + size].to_vec())
    }
    pub fn write(&mut self, tnx_id: u64, page_id: u64, page: &Page) -> RsqlResult<()> {
        // analyze the differences, to find out continuous byte ranges
        // this will significantly reduce the WAL size
        let old_page = self.storage_manager.read_page(page_id)?;
        let mut start = None;
        for (i, byte) in page.data.iter().enumerate() {
            if *byte != old_page.data[i] {
                start = Some(i);
                break; // find the start of the first difference
            };
        };
        let start = match start {
            Some(s) => s,
            None => return Ok(()), // no difference
        };

        let mut end = page.data.len();
        for (i, byte) in page.data.iter().rev().enumerate() {
            let idx = page.data.len() - 1 - i;
            if *byte != old_page.data[idx] {
                end = idx + 1;
                break; // find the end of the last difference
            }
        };
        assert!(end >= start);
        self.write_bytes(tnx_id, page_id, start, &page.data[start..end])
    }
    pub fn write_bytes(&mut self, tnx_id: u64, page_id: u64, offset: usize, data: &[u8]) -> RsqlResult<()> {
        // read old data for WAL
        let mut old_page = self.storage_manager.read_page(page_id)?;
        let old_data = &old_page.data[offset..offset + data.len()];
        // write to WAL first
        self.wal.update_page(tnx_id, self.table_id, page_id, offset as u64, old_data, data)?;
        self.wal.flush()?;
        // then write to storage
        old_page.data[offset..offset + data.len()].copy_from_slice(data);
        self.storage_manager.write_page(&old_page, page_id)?;
        Ok(())
    }
    pub fn new_page(&mut self, tnx_id: u64) -> RsqlResult<(u64, Page)> {
        let (page_id, page) = self.storage_manager.new_page()?;
        // log the new page creation in WAL
        self.wal.new_page(tnx_id, self.table_id, page_id, &page.data)?;
        self.wal.flush()?;
        Ok((page_id, page))
    }
    pub fn free_page(&mut self, tnx_id: u64, page_id: u64) -> RsqlResult<()> {
        let check_page_id = self.storage_manager
            .max_page_index()
            .unwrap(); // None means no page exists, so cannot free any page
        if page_id != check_page_id {
            panic!("can only free the last page, 
                    try to free page_id: {}, max_page_id: {}", page_id, check_page_id);
        };
        let freed_page = self.storage_manager.read_page(page_id)?;
        // log the page deletion in WAL
        self.wal.delete_page(tnx_id, self.table_id, page_id, &freed_page.data)?;
        self.wal.flush()?;
        self.storage_manager.free()?;
        Ok(())
    }
    pub fn max_page_index(&self) -> Option<u64> {
        self.storage_manager.max_page_index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DB_DIR;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_consist_storage_new_write_and_persistence() {
        let file_path = "./data/test_consist_storage.db";
        // cleanup
        let wal_path = Path::new(DB_DIR).join("wal.log");
        let _ = fs::remove_file(&wal_path);
        if Path::new(file_path).exists() {
            fs::remove_file(file_path).unwrap();
        }

        // create engine and new page, then write
        {
            let mut engine = ConsistStorageEngine::new(file_path, 777).unwrap();
            let tnx = 1u64;
            let (pid, mut page) = engine.new_page(tnx).unwrap();
            assert_eq!(pid, 0);
            page.data[0] = 99;
            engine.write(tnx, pid, &page).unwrap();
        }

        // reopen and read
        {
            let engine = ConsistStorageEngine::new(file_path, 777).unwrap();
            let p = engine.read(0).unwrap();
            assert_eq!(p.data[0], 99);
        }

        let _ = fs::remove_file(file_path);
    }
}
