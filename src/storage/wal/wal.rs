use core::panic;
use std::collections::HashSet;
use std::fs;
use std::io::Seek;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, atomic::{AtomicU64, Ordering}};

use tracing::{warn, info, debug};

use crate::config::{DB_DIR, MAX_WAL_SIZE};
use crate::common::{RsqlError, RsqlResult};
use crate::utils;

use super::wal_entry::WALEntry;

/// Guard to ensure WAL recovery is done before any DB operation
static HAS_RECOVERED: OnceLock<()> = OnceLock::new();
fn check_recovered() {
    #[cfg(not(test))]
    HAS_RECOVERED.get().expect("WAL recovery must be done before any DB operation");
}

static WAL_INSTANCE: OnceLock<Arc<WAL>> = OnceLock::new();
const HEADER_MAGIC: u32 = 0x4c515352; // 'RSQL' in little endian hex

/// Write-Ahead Log (WAL) structure
/// A thread safe structure to handle concurrent writes to the log file.
/// Singleton pattern is used to ensure only one instance of WAL exists.
/// The Wal log file structure:
/// [HEADER_MAGIC (4 bytes)][WALEntry 1(not fixed size)][WALEntry 2]...
pub struct WAL {
    log_file: Arc<Mutex<fs::File>>,
    active_tnx_ids: Arc<Mutex<Vec<u64>>>,
    length: AtomicU64,
    log_path: PathBuf,
}

impl WAL {
    pub fn global() -> Arc<Self> {
        WAL_INSTANCE.get_or_init(|| {
            Arc::new(Self::new().expect("Failed to init WAL"))
        }).clone()
    }
    fn init_header(log_file: &mut fs::File) -> RsqlResult<()> {
        let header = HEADER_MAGIC.to_le_bytes();
        log_file.write_all(&header)?;
        Ok(())
    }
    fn check_header(log_file: &mut fs::File) -> RsqlResult<()> {
        let mut header = [0u8; 4];
        log_file.read_exact(&mut header)?;
        let magic = u32::from_le_bytes(header);
        if magic != HEADER_MAGIC {
            return Err(RsqlError::WalError("Invalid WAL header".to_string()));
        }
        Ok(())
    }
    fn new() -> RsqlResult<Self> {
        // initialize log file
        let log_path = if cfg!(test) {
            // for multi-threaded tests
            utils::test_dir("wal".to_string())
        } else {
            std::path::Path::new(DB_DIR).join("wal.log")
        };
        if !log_path.exists() {
            // not exists, create new file with header
            if let Some(parent) = log_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = fs::File::create(&log_path)?;
            Self::init_header(&mut file)?;
        }
        let mut log_file = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&log_path)?;
        // check if file head valid
        if let Err(err) = Self::check_header(&mut log_file) {
            // invalid header, re-initialize
            warn!("WAL header invalid: {}, re-initializing WAL file", err);
            drop(log_file);
            fs::remove_file(&log_path)?;
            let mut file = fs::File::create(&log_path)?;
            Self::init_header(&mut file)?;
            log_file = fs::OpenOptions::new()
                .read(true)
                .append(true)
                .open(&log_path)?;
            log_file.seek(std::io::SeekFrom::End(0))?; 
        }
        let length = log_file.metadata()?.len();
        Ok(WAL {
            active_tnx_ids: Arc::new(Mutex::new(Vec::new())),
            log_file: Arc::new(Mutex::new(log_file)),
            length: AtomicU64::new(length),
            log_path,
        })
    }
    /// Recovery the database to a consistent state using the WAL log.
    /// Helper function for testing with custom WAL instance.
    /// Args:
    /// - write_page(table_id, page_id, data): function to write a page to storage
    /// - update_page(table_id, page_id, offset, len, data): function to update a page in storage
    /// - append_page(table_id) -> new_page_id: function to append a new page to storage, returns new page_id
    /// - trunc_page(table_id): function to truncate the last page from storage
    /// - max_page_idx(table_id) -> max_page_id: function to get the current max page index in storage
    /// Returns:
    /// - RsqlResult<u64>: The maximum transaction ID found in the log
    pub fn recovery_with_instance(
        wal: Arc<WAL>,
        write_page: &mut impl FnMut(u64, u64, &[u8]) -> RsqlResult<()>,
        update_page: &mut impl FnMut(u64, u64, u64, u64, &[u8]) -> RsqlResult<()>,
        append_page: &mut impl FnMut(u64) -> RsqlResult<u64>,
        trunc_page: &mut impl FnMut(u64) -> RsqlResult<()>,
        max_page_idx: &mut impl FnMut(u64) -> RsqlResult<u64>,
    ) -> RsqlResult<u64> {
        info!("Starting WAL recovery");
        let buf = {
            let mut file = wal.log_file.lock().unwrap();
            file.seek(std::io::SeekFrom::Start(0))?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            file.seek(std::io::SeekFrom::End(0))?;
            buf
        };

        if buf.len() < 4 {
            panic!("WAL recovery: log file too short to contain header");
        }
        let entrys: Vec<_> = WALEntry::from_bytes(&buf[4..]).collect();

        if entrys.is_empty() {
            HAS_RECOVERED.get_or_init(|| ());
            info!("WAL recovery: no entries to process");
            return Ok(0);
        }
        // 2. find nearest checkpoint
        let mut checkpoint_index = None;
        for (i, entry) in entrys.iter().enumerate().rev() {
            if let WALEntry::Checkpoint { .. } = entry {
                checkpoint_index = Some(i);
                break;
            }
        }
        let checkpoint_index = match checkpoint_index {
            Some(idx) => idx,
            None => 0,
        };
        // 3. find transactions to redo or undo
        let mut recover_num = 0;
        let mut redo_tnx_ids = HashSet::new();
        let mut undo_tnx_ids = HashSet::new();
        if let WALEntry::Checkpoint { active_tnx_ids } = &entrys[checkpoint_index] {
            for id in active_tnx_ids {
                undo_tnx_ids.insert(*id);
            }
        }
        for entry in &entrys[checkpoint_index..] {
            match entry {
                WALEntry::OpenTnx { tnx_id } => {
                    undo_tnx_ids.insert(*tnx_id);
                },
                WALEntry::CommitTnx { tnx_id } => {
                    undo_tnx_ids.remove(tnx_id);
                    redo_tnx_ids.insert(*tnx_id);
                },
                WALEntry::RollbackTnx { tnx_id } => {
                    undo_tnx_ids.remove(tnx_id);
                },
                _ => {},
            }
        }
        // 4. redo operations
        for entry in &entrys[checkpoint_index..] {
            match entry {
                WALEntry::UpdatePage { tnx_id, table_id, page_id, offset, len, new_data, .. } => {
                    if redo_tnx_ids.contains(tnx_id) {
                        update_page(*table_id, *page_id, *offset, *len, &new_data)?;
                        recover_num += 1;
                    }
                },
                WALEntry::NewPage { tnx_id, table_id, data, page_id } => {
                    if redo_tnx_ids.contains(tnx_id) {
                        // if too short, append pages until enough
                        while max_page_idx(*table_id)? < *page_id {
                            append_page(*table_id)?;
                        }
                        // if too long, delete pages until enough
                        while max_page_idx(*table_id)? > *page_id {
                            trunc_page(*table_id)?;
                        }
                        write_page(*table_id, *page_id, &data)?;
                        recover_num += 1;
                    }
                },
                WALEntry::DeletePage { tnx_id, table_id, page_id, .. } => {
                    if redo_tnx_ids.contains(tnx_id) {
                        // if too short, append pages until enough
                        while max_page_idx(*table_id)? < *page_id - 1 {
                            append_page(*table_id)?;
                        }
                        // if too long, delete pages until enough
                        while max_page_idx(*table_id)? > *page_id - 1 {
                            trunc_page(*table_id)?;
                        }
                        recover_num += 1;
                    }
                },
                _ => {},
            }
        }
        // 5. undo operations
        for entry in entrys.iter().rev() {
            match entry {
                WALEntry::UpdatePage { tnx_id, table_id, page_id, offset, len, old_data, .. } => {
                    if undo_tnx_ids.contains(tnx_id) {
                        update_page(*table_id, *page_id, *offset, *len, &old_data)?;
                        recover_num += 1;
                    }
                },
                WALEntry::NewPage { tnx_id, table_id, page_id, .. } => {
                    if undo_tnx_ids.contains(tnx_id) {
                        // if too short, append pages until enough
                        while max_page_idx(*table_id)? < *page_id - 1 {
                            append_page(*table_id)?;
                        }
                        // if too long, delete pages until enough
                        while max_page_idx(*table_id)? > *page_id - 1 {
                            trunc_page(*table_id)?;
                        }
                        recover_num += 1;
                    }
                },
                WALEntry::DeletePage { tnx_id, table_id, page_id, old_data } => {
                    if undo_tnx_ids.contains(tnx_id) {
                        // if too short, append pages until enough
                        while max_page_idx(*table_id)? < *page_id {
                            append_page(*table_id)?;
                        }
                        // if too long, delete pages until enough
                        while max_page_idx(*table_id)? > *page_id {
                            trunc_page(*table_id)?;
                        }
                        write_page(*table_id, *page_id, &old_data)?;
                        recover_num += 1;
                    }
                },
                _ => {},
            }
        }
        // 6. find max tnx id
        let max_tnx_id = entrys.iter().filter_map(|entry| {
            match entry {
                WALEntry::OpenTnx { tnx_id }
                | WALEntry::CommitTnx { tnx_id }
                | WALEntry::RollbackTnx { tnx_id }
                | WALEntry::NewPage { tnx_id, .. }
                | WALEntry::UpdatePage { tnx_id, .. }
                | WALEntry::DeletePage { tnx_id, .. } => Some(*tnx_id),
                _ => None,
            }
        }).max().unwrap_or(0);
        info!("WAL recovery completed, {} operations applied", recover_num);
        HAS_RECOVERED.get_or_init(|| ());
        Ok(max_tnx_id)
    }

    /// Recovery the database to a consistent state using the WAL log.
    /// Returns:
    /// - RsqlResult<u64>: The maximum transaction ID found in the log
    pub fn recovery(
        write_page: &mut impl FnMut(u64, u64, &[u8]) -> RsqlResult<()>,
        update_page: &mut impl FnMut(u64, u64, u64, u64, &[u8]) -> RsqlResult<()>,
        append_page: &mut impl FnMut(u64) -> RsqlResult<u64>,
        trunc_page: &mut impl FnMut(u64) -> RsqlResult<()>,
        max_page_idx: &mut impl FnMut(u64) -> RsqlResult<u64>,
    ) -> RsqlResult<u64> {
        Self::recovery_with_instance(WAL::global(), write_page, update_page, append_page, trunc_page, max_page_idx)
    }

    pub fn checkpoint(
        &self,
        flush_page: &impl Fn() -> RsqlResult<()>, 
    ) -> RsqlResult<()> {
        check_recovered();
        info!("Starting WAL checkpoint");
        // 1. flush all dirty pages to storage
        flush_page()?;
        
        // Hold both locks for the entire duration to ensure atomicity and consistency
        let active_tnx_ids = self.active_tnx_ids.lock().unwrap();
        let mut log_file = self.log_file.lock().unwrap();

        let old_bytes = {
            let mut buf = Vec::new();
            log_file.seek(std::io::SeekFrom::Start(0))?;
            log_file.read_to_end(&mut buf)?;
            buf
        };

        // 2. construct simplified wal log
        let mut new_entrys = Vec::new();
        for entry in WALEntry::from_bytes(&old_bytes[4..]) {
            match entry {
                WALEntry::Checkpoint {..} => continue,
                WALEntry::CommitTnx { tnx_id} 
                | WALEntry::RollbackTnx { tnx_id } 
                | WALEntry::OpenTnx { tnx_id } => {
                    if active_tnx_ids.contains(&tnx_id) {
                        new_entrys.push(entry);
                    }
                },
                WALEntry::NewPage { tnx_id, ..} => {
                    if active_tnx_ids.contains(&tnx_id) {
                        new_entrys.push(entry);
                    }
                },
                WALEntry::UpdatePage { tnx_id, ..} => {
                    if active_tnx_ids.contains(&tnx_id) {
                        new_entrys.push(entry);
                    }
                },
                WALEntry::DeletePage { tnx_id, ..} => {
                    if active_tnx_ids.contains(&tnx_id) {
                        new_entrys.push(entry);
                    }
                },
            }
        };
        // 2.5 append checkpoint entry
        new_entrys.push(WALEntry::Checkpoint { active_tnx_ids: active_tnx_ids.clone() });

        // 3. write new wal log
        let tmp_path = self.log_path.with_extension("log.tmp");
        {
            let mut new_log_file = fs::File::create(&tmp_path)?;
            Self::init_header(&mut new_log_file)?;
            for entry in new_entrys {
                let entry_bytes = entry.to_bytes();
                new_log_file.write_all(&entry_bytes)?;
            }
            new_log_file.flush()?;
            new_log_file.sync_all()?;
        }
        // 4. rename new log file to current log file
        // THIS MUST BE ATOMIC OPERATION
        fs::rename(&tmp_path, &self.log_path)?;
        // 5. update self handle
        *log_file = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.log_path)?;
        log_file.seek(std::io::SeekFrom::End(0))?; 
        self.length.store(log_file.metadata()?.len(), Ordering::SeqCst); 
        
        info!("WAL checkpoint completed");
        Ok(())
    }

    fn append_entry(&self, entry: &WALEntry) -> RsqlResult<()> {
        check_recovered();
        let entry_bytes = entry.to_bytes();
        let mut log_file = self.log_file.lock().unwrap();
        debug!("Appending WAL entry: {:?}", &entry);
        // 1. write entry bytes
        log_file.write_all(&entry_bytes)?;
        // 2. update length
        self.length.fetch_add(entry_bytes.len() as u64, Ordering::SeqCst);
        Ok(())
    }

    pub fn flush(&self) -> RsqlResult<()> {
        check_recovered();
        let mut log_file = self.log_file.lock().unwrap();
        log_file.flush()?;
        log_file.sync_all()?;
        Ok(())
    }

    pub fn update_page(
        &self,
        tnx_id: u64,
        table_id: u64,
        page_id: u64,
        offset: u64,
        old_data: &[u8],
        new_data: &[u8],
    ) -> RsqlResult<()> {
        check_recovered();
        if old_data.len() != new_data.len() {
            panic!("WAL::update_page: old_data and new_data length mismatch");
        }
        let len = old_data.len() as u64;
        let entry = WALEntry::UpdatePage {
            tnx_id,
            table_id,
            page_id,
            offset,
            len,
            old_data: old_data.to_vec(),
            new_data: new_data.to_vec(),
        };
        self.append_entry(&entry)
    }
    pub fn new_page(
        &self,
        tnx_id: u64,
        table_id: u64,
        page_id: u64,
        data: &[u8],
    ) -> RsqlResult<()> {
        check_recovered();
        let entry = WALEntry::NewPage {
            tnx_id,
            table_id,
            page_id,
            data: data.to_vec(),
        };
        self.append_entry(&entry)
    }
    pub fn delete_page(
        &self,
        tnx_id: u64,
        table_id: u64,
        page_id: u64,
        old_data: &[u8],
    ) -> RsqlResult<()> {
        check_recovered();
        let entry = WALEntry::DeletePage {
            tnx_id,
            table_id,
            page_id,
            old_data: old_data.to_vec(),
        };
        self.append_entry(&entry)
    }

    pub fn open_tnx(&self, tnx_id: u64) -> RsqlResult<()> {
        check_recovered();
        let entry = WALEntry::OpenTnx {
            tnx_id
        };
        self.active_tnx_ids.lock().unwrap().push(tnx_id);
        self.append_entry(&entry)?;
        Ok(())
    }
    /// This method will force flush the log after committing
    pub fn commit_tnx(&self, tnx_id: u64) -> RsqlResult<()> {
        check_recovered();
        // nothing more to do, it's a happy path
        let entry = WALEntry::CommitTnx {
            tnx_id
        };
        self.active_tnx_ids.lock().unwrap().retain(|&id| id != tnx_id);
        self.append_entry(&entry)?;
        self.flush()?;
        Ok(())
    }
    /// This method will force flush the log after rolling back
    pub fn rollback_tnx(
        &self, 
        tnx_id: u64,
        write_page: &mut dyn FnMut(u64, u64, &[u8]) -> RsqlResult<()>,
        update_page: &mut dyn FnMut(u64, u64, u64, u64, &[u8]) -> RsqlResult<()>,
        append_page: &mut dyn FnMut(u64) -> RsqlResult<u64>,
        trunc_page: &mut dyn FnMut(u64) -> RsqlResult<()>,
        max_page_idx: &mut dyn FnMut(u64) -> RsqlResult<u64>,
    ) -> RsqlResult<()> {
        check_recovered();
        // 1. find all entries related to this transaction
        let undo_entries = {
            let mut file = self.log_file.lock().unwrap();
            let mut buf = Vec::new();
            file.seek(std::io::SeekFrom::Start(0))?;
            file.read_to_end(&mut buf)?;
            file.seek(std::io::SeekFrom::End(0))?; // Reset cursor
            
            WALEntry::from_bytes(&buf[4..])
                .filter(|e| match e {
                    WALEntry::UpdatePage { tnx_id: eid, .. }
                    | WALEntry::NewPage { tnx_id: eid, .. }
                    | WALEntry::DeletePage { tnx_id: eid, .. } => *eid == tnx_id,
                    _ => false,
                })
                .collect::<Vec<_>>()
        }; // Lock dropped before callbacks to avoid deadlock

        // 2. undo them all in reverse order
        for entry in undo_entries.iter().rev() {
            match entry {
                WALEntry::UpdatePage { table_id, page_id, offset, len, old_data, .. } => {
                    update_page(*table_id, *page_id, *offset, *len, &old_data)?;
                },
                WALEntry::NewPage { table_id, page_id, .. } => {
                    // if too short, append pages until enough
                    while max_page_idx(*table_id)? < *page_id - 1 {
                        append_page(*table_id)?;
                    }
                    // if too long, delete pages until enough
                    while max_page_idx(*table_id)? > *page_id - 1 {
                        trunc_page(*table_id)?;
                    }
                },
                WALEntry::DeletePage { table_id, page_id, old_data, .. } => {
                    // if too short, append pages until enough
                    while max_page_idx(*table_id)? < *page_id {
                        append_page(*table_id)?;
                    }
                    // if too long, delete pages until enough
                    while max_page_idx(*table_id)? > *page_id {
                        trunc_page(*table_id)?;
                    }
                    write_page(*table_id, *page_id, &old_data)?;
                },
                _ => {},
            }
        }
        
        // 3. write rollback entry
        self.active_tnx_ids.lock().unwrap().retain(|&id| id != tnx_id);
        let entry = WALEntry::RollbackTnx {
            tnx_id
        };
        self.append_entry(&entry)?;
        self.flush()?;
        Ok(())
    }
    /// Detect whether a checkpoint is needed based on the current WAL size
    pub fn need_checkpoint(&self) -> bool {
        check_recovered();
        let length = self.length.load(Ordering::SeqCst);
        length > MAX_WAL_SIZE
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DB_DIR;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_wal_recovery_redo_undo() {
        // cleanup
        let wal_path = Path::new(DB_DIR).join("wal.log");
        let _ = fs::remove_file(&wal_path);

        // create a fresh WAL instance
        let wal = Arc::new(WAL::new().expect("Failed to init WAL"));

        // start a committed transaction t1 that creates a new page
        wal.open_tnx(1).unwrap();
        let data1 = vec![1u8,2,3];
        wal.new_page(1, 42, 0, &data1).unwrap();
        wal.commit_tnx(1).unwrap();

        // start an uncommitted transaction t2 that updates the same page
        wal.open_tnx(2).unwrap();
        let old = vec![1u8,2,3];
        let new = vec![9u8,9,9];
        wal.update_page(2, 42, 0, 0, &old, &new).unwrap();
        // do not commit t2

        // Now perform recovery into collectors
        let mut wrote_pages = Vec::new();
        let mut updated_pages = Vec::new();
        let mut appended = Vec::new();
        let mut truncated = Vec::new();

        // closures for recovery
        let mut write_page = |table_id: u64, page_id: u64, data: &[u8]| -> RsqlResult<()> {
            wrote_pages.push((table_id, page_id, data.to_vec()));
            Ok(())
        };
        let mut update_page = |table_id: u64, page_id: u64, offset: u64, len: u64, data: &[u8]| -> RsqlResult<()> {
            updated_pages.push((table_id, page_id, offset, len, data.to_vec()));
            Ok(())
        };
        let mut append_page = |_: u64| -> RsqlResult<u64> { appended.push(()); Ok(0) };
        let mut trunc_page = |_: u64| -> RsqlResult<()> { truncated.push(()); Ok(()) };
        let mut max_page_idx = |_: u64| -> RsqlResult<u64> { Ok(0) };

        WAL::recovery_with_instance(wal, &mut write_page, &mut update_page, &mut append_page, &mut trunc_page, &mut max_page_idx).unwrap();

        // After recovery: new_page from committed t1 should be redone
        assert!(wrote_pages.iter().any(|(t, p, d)| *t == 42 && *p == 0 && *d == data1));

        // update from uncommitted t2 should be undone (i.e., undo phase will apply old data)
        assert!(updated_pages.iter().any(|(t, p, _off, _len, d)| *t == 42 && *p == 0 && *d == old));
    }

    #[test]
    fn test_wal_rollback_deadlock() {
        // mark recovered so test can call WAL methods
        HAS_RECOVERED.get_or_init(|| ());

        let wal = WAL::global();
        wal.open_tnx(100).unwrap();
        wal.update_page(100, 1, 1, 0, &[1], &[2]).unwrap();
        
        // This should NOT deadlock even if callback calls WAL
        wal.rollback_tnx(100, 
            &mut |_, _, _| Ok(()),
            &mut |_, _, _, _, _| {
                let wal2 = WAL::global();
                // different tnx id to avoid any other logic issues
                wal2.open_tnx(101).unwrap();
                wal2.update_page(101, 1, 1, 0, &[3], &[4]).unwrap();
                Ok(())
            },
            &mut |_| Ok(0),
            &mut |_| Ok(()),
            &mut |_| Ok(0)
        ).unwrap();
    }

    #[test]
    fn test_wal_checkpoint() {
        // mark recovered
        let _ = HAS_RECOVERED.get_or_init(|| ());

        let wal = Arc::new(WAL::new().expect("Failed to init WAL"));
        let wal_path = wal.log_path.clone();

        // 1. Committed transaction t1
        wal.open_tnx(1).unwrap();
        wal.update_page(1, 10, 0, 0, &[0u8], &[1u8]).unwrap();
        wal.commit_tnx(1).unwrap();

        // 2. Active transaction t2
        wal.open_tnx(2).unwrap();
        wal.update_page(2, 10, 0, 1, &[0u8], &[2u8]).unwrap();

        // 3. Checkpoint
        let flushed = Arc::new(Mutex::new(false));
        let flushed_clone = flushed.clone();
        wal.checkpoint(&|| {
            *flushed_clone.lock().unwrap() = true;
            Ok(())
        }).unwrap();

        assert!(*flushed.lock().unwrap());

        // 4. Verify log content - should only contain t2 and Checkpoint
        let bytes = fs::read(&wal_path).unwrap();
        let entries: Vec<_> = WALEntry::from_bytes(&bytes[4..]).collect();
        
        // Should have OpenTnx(2), UpdatePage(2, ...), and Checkpoint
        assert!(entries.iter().any(|e| match e {
            WALEntry::OpenTnx { tnx_id } => *tnx_id == 2,
            _ => false,
        }));
        assert!(entries.iter().any(|e| matches!(e, WALEntry::Checkpoint { .. })));
        // Should NOT have t1 entries
        assert!(!entries.iter().any(|e| match e {
            WALEntry::OpenTnx { tnx_id } => *tnx_id == 1,
            WALEntry::CommitTnx { tnx_id } => *tnx_id == 1,
            _ => false,
        }));

        // 5. Recovery test
        let mut updated = Vec::new();
        let mut update_fn = |_: u64, _: u64, _: u64, _: u64, data: &[u8]| {
            updated.push(data.to_vec());
            Ok(())
        };
        
        // Reset recovery state for testing
        // (This is tricky because HAS_RECOVERED is a OnceLock and can't be reset easily)
        // But recovery_with_instance doesn't check HAS_RECOVERED, it SETS it.

        WAL::recovery_with_instance(
            wal.clone(),
            &mut |_, _, _| Ok(()),
            &mut update_fn,
            &mut |_| Ok(0),
            &mut |_| Ok(()),
            &mut |_| Ok(0),
        ).unwrap();

        // Since t2 was active during checkpoint, it should be in undo_tnx_ids
        // and its update should be undone (reverting to old data [0u8])
        assert!(updated.contains(&vec![0u8]));
    }
}
