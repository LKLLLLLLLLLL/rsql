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
            }
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
        let old_page = self.storage_manager.read_page(page_id)?;
        let old_data = &old_page.data[offset..offset + data.len()];
        // write to WAL first
        self.wal.update_page(tnx_id, self.table_id, page_id, offset as u64, old_data, data)?;
        self.wal.flush()?;
        // then write to storage
        let mut page = self.storage_manager.read_page(page_id)?;
        page.data[offset..offset + data.len()].copy_from_slice(data);
        self.storage_manager.write_page(&page, page_id)?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use crate::config::DB_DIR;
    use crate::db::storage_engine::wal::WAL;

    #[test]
    fn test_consist_storage_write_and_recovery() {
        let db_path = "test_consist.db";
        let wal_path = Path::new(DB_DIR).join("wal.log");
        
        // cleanup
        if Path::new(db_path).exists() { fs::remove_file(db_path).unwrap(); }
        if wal_path.exists() { fs::remove_file(&wal_path).unwrap(); }

        // Initialize WAL recovery state
        // (Since it's a OnceLock, we just hope it's not set or set it here)
        // recovery() will set it.
        
        {
            let mut engine = ConsistStorageEngine::new(db_path, 1).unwrap();
            
            // we must call recovery first to satisfy HAS_RECOVERED
            let mut noop_append = |_| Ok(0u64);
            let mut noop_max = |_| Ok(0u64);
            let mut noop_write = |_, _, _| Ok(());
            let mut noop_update = |_, _, _, _, _| Ok(());
            WAL::recovery(&mut noop_write, &mut noop_update, &mut noop_append, &mut |_| Ok(()), &mut noop_max).unwrap();

            // 1. Transaction 1: Create page and write data
            engine.new_page(1).unwrap(); // index 0
            let mut page = engine.read(0).unwrap();
            page.data[0..4].copy_from_slice(&[1, 2, 3, 4]);
            engine.write(1, 0, &page).unwrap();
            
            // commit t1
            WAL::global().commit_tnx(1).unwrap();

            // 2. Transaction 2: Update data but DON'T commit
            let mut page2 = engine.read(0).unwrap();
            page2.data[0..4].copy_from_slice(&[9, 9, 9, 9]);
            engine.write(2, 0, &page2).unwrap();
        }

        // Now simulate crash and recovery
        {
            // The uncommitted transaction 2 should be undone.
            // Committed transaction 1 should be redone (though it's already on disk here).
            
            let _engine = ConsistStorageEngine::new(db_path, 1).unwrap();
            
            let mut updated_pages = Vec::new();
            let mut update_fn = |table_id: u64, page_id: u64, offset: u64, len: u64, data: Vec<u8>| {
                updated_pages.push((table_id, page_id, offset, len, data));
                Ok(())
            };

            // In a real recovery, we would use the engine's methods as callbacks.
            // For testing, we just see if recovery tells us to undo t2.
            
            WAL::recovery(
                &mut |_, _, _| Ok(()),
                &mut update_fn,
                &mut |_| Ok(0),
                &mut |_| Ok(()),
                &mut |_| Ok(0),
            ).unwrap();

            // recovery should have triggered undo for t2: [9,9,9,9] -> [1,2,3,4]
            assert!(updated_pages.iter().any(|(_, _, off, _, data)| *off == 0 && data == &[1, 2, 3, 4]));
        }

        fs::remove_file(db_path).unwrap();
        fs::remove_file(wal_path).unwrap();
    }
}
