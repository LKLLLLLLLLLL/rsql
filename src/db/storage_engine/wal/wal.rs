use core::panic;
use std::collections::HashSet;
use std::fs;
use std::io::Seek;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex, OnceLock, atomic::{AtomicU64, Ordering}};

use tracing::{warn, info};

use crate::config::{DB_DIR, MAX_WAL_SIZE};
use crate::db::errors::{RsqlError, RsqlResult};

use super::wal_entry::WALEntry;

/// Guard to ensure WAL recovery is done before any DB operation
static HAS_RECOVERED: OnceLock<()> = OnceLock::new();
fn check_recovered() {
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
        let log_path = std::path::Path::new(DB_DIR).join("wal.log");
        if !log_path.exists() {
            // not exists, create new file with header
            fs::create_dir_all(DB_DIR)?;
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
        }
        let length = log_file.metadata()?.len();
        Ok(WAL {
            active_tnx_ids: Arc::new(Mutex::new(Vec::new())),
            log_file: Arc::new(Mutex::new(log_file)),
            length: AtomicU64::new(length),
        })
    }
    /// Recovery the database to a consistent state using the WAL log.w
    /// args:
    /// - write_page(table_id, page_id, data): write a new page to the database
    /// - update_page(table_id, page_id, offset, len, data): update an existing page in the database
    /// - append_page(table_id): append a new page to the database, return the new page id
    /// - trunc_page(table_id): truncate the last page in the table file
    /// - max_page_idx(table_id): get the current max page index in the table file
    pub fn recovery(
        write_page: &mut dyn FnMut(u64, u64, Vec<u8>) -> RsqlResult<()>,
        update_page: &mut dyn FnMut(u64, u64, u64, u64, Vec<u8>) -> RsqlResult<()>,
        append_page: &mut dyn FnMut(u64) -> RsqlResult<u64>,
        trunc_page: &mut dyn FnMut(u64) -> RsqlResult<()>,
        max_page_idx: &mut dyn FnMut(u64) -> RsqlResult<u64>,
    ) -> RsqlResult<()> {
        HAS_RECOVERED.get_or_init(|| ());
        info!("Starting WAL recovery");
        let wal = WAL::global();
        let mut file = wal.log_file.lock().unwrap();
        // 1. read all entries
        // seek to start
        file.seek(std::io::SeekFrom::Start(0))?;
        let mut entrys = Vec::new();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        if buf.len() < 4 {
            panic!("WAL recovery: log file too short to contain header");
        }
        for entry in WALEntry::from_bytes(&buf[4..]) {
            entrys.push(entry);
        }
        if entrys.is_empty() {
            info!("WAL recovery: no entries to process");
            return Ok(());
        }
        drop(file);
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
                        update_page(*table_id, *page_id, *offset, *len, new_data.clone())?;
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
                        write_page(*table_id, *page_id, data.clone())?;
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
        for entry in entrys.iter().rev().take(entrys.len() - checkpoint_index) {
            match entry {
                WALEntry::UpdatePage { tnx_id, table_id, page_id, offset, len, old_data, .. } => {
                    if undo_tnx_ids.contains(tnx_id) {
                        update_page(*table_id, *page_id, *offset, *len, old_data.clone())?;
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
                        write_page(*table_id, *page_id, old_data.clone())?;
                        recover_num += 1;
                    }
                },
                _ => {},
            }
        }
        info!("WAL recovery completed, {} operations applied", recover_num);
        Ok(())
    }

    pub fn checkpoint(
        &self,
        flush_page: &impl Fn() -> RsqlResult<()>, 
    ) -> RsqlResult<()> {
        check_recovered();
        info!("Starting WAL checkpoint");
        // 1. flush all dirty pages to storage
        flush_page()?;
        let active_tnx_ids = self.active_tnx_ids.lock().unwrap();
        let mut file = self.log_file.lock().unwrap();
        // 2. construct simplified wal log
        let old_bytes = fs::read(std::path::Path::new(DB_DIR).join("wal.log"))?;
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
        // 3. write new wal log
        {
            let mut new_log_file = fs::File::create(std::path::Path::new(DB_DIR).join("wal.log.tmp"))?;
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
        fs::rename(std::path::Path::new(DB_DIR).join("wal.log.tmp"), std::path::Path::new(DB_DIR).join("wal.log"))?;
        // 5. update self
        *file = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(std::path::Path::new(DB_DIR).join("wal.log"))?;
        self.length.store(file.metadata()?.len(), Ordering::SeqCst); // only header left
        info!("WAL checkpoint completed");
        Ok(())
    }

    fn append_entry(&self, entry: &WALEntry) -> RsqlResult<bool> {
        check_recovered();
        let entry_bytes = entry.to_bytes();
        let mut log_file = self.log_file.lock().unwrap();
        // 1. write entry bytes
        log_file.write_all(&entry_bytes)?;
        // 2. update length
        let new_length = self.length.fetch_add(entry_bytes.len() as u64, Ordering::SeqCst) + entry_bytes.len() as u64;
        Ok(new_length > MAX_WAL_SIZE)
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
    ) -> RsqlResult<bool> {
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
    ) -> RsqlResult<bool> {
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
    ) -> RsqlResult<bool> {
        check_recovered();
        let entry = WALEntry::DeletePage {
            tnx_id,
            table_id,
            page_id,
            old_data: old_data.to_vec(),
        };
        self.append_entry(&entry)
    }

    pub fn open_tnx(&self, tnx_id: u64) -> RsqlResult<bool> {
        check_recovered();
        let entry = WALEntry::OpenTnx {
            tnx_id
        };
        self.active_tnx_ids.lock().unwrap().push(tnx_id);
        self.append_entry(&entry)
    }
    /// This method will force flush the log after committing
    pub fn commit_tnx(&self, tnx_id: u64) -> RsqlResult<bool> {
        check_recovered();
        // nothing more to do, it's a happy path
        let entry = WALEntry::CommitTnx {
            tnx_id
        };
        self.active_tnx_ids.lock().unwrap().retain(|&id| id != tnx_id);
        let need_checkpoint = self.append_entry(&entry)?;
        self.flush()?;
        Ok(need_checkpoint)
    }
    /// This method will force flush the log after rolling back
    pub fn rollback_tnx(
        &self, 
        tnx_id: u64,
        write_page: &mut dyn FnMut(u64, u64, Vec<u8>) -> RsqlResult<()>,
        update_page: &mut dyn FnMut(u64, u64, u64, u64, Vec<u8>) -> RsqlResult<()>,
        append_page: &mut dyn FnMut(u64) -> RsqlResult<u64>,
        trunc_page: &mut dyn FnMut(u64) -> RsqlResult<()>,
        max_page_idx: &mut dyn FnMut(u64) -> RsqlResult<u64>,
    ) -> RsqlResult<bool> {
        check_recovered();
        // 1. undo everything related to this transaction
        let mut file = self.log_file.lock().unwrap();
        let mut undo_entries = Vec::new();
        // find all entries related to this transaction
        let mut buf = Vec::new();
        file.seek(std::io::SeekFrom::Start(0))?;
        file.read_to_end(&mut buf)?;
        for entry in WALEntry::from_bytes(&buf[4..]) {
            match &entry {
                WALEntry::UpdatePage { tnx_id: eid, .. }
                | WALEntry::NewPage { tnx_id: eid, .. }
                | WALEntry::DeletePage { tnx_id: eid, .. } => {
                    if *eid == tnx_id {
                        undo_entries.push(entry);
                    }
                },
                _ => {},
            }
        }
        // undo them all
        for entry in undo_entries.iter().rev() {
            match entry {
                WALEntry::UpdatePage { table_id, page_id, offset, len, old_data, .. } => {
                    update_page(*table_id, *page_id, *offset, *len, old_data.clone())?;
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
                    write_page(*table_id, *page_id, old_data.clone())?;
                },
                _ => {},
            }
        }
        drop(file);
        // 2. write rollback entry
        let entry = WALEntry::RollbackTnx {
            tnx_id
        };
        self.active_tnx_ids.lock().unwrap().retain(|&id| id != tnx_id);
        let need_checkpoint = self.append_entry(&entry)?;
        self.flush()?;
        Ok(need_checkpoint)
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
        // mark recovered so test can call WAL methods without panic
        HAS_RECOVERED.get_or_init(|| ());

        // backup existing wal.log if any
        let wal_path = Path::new(DB_DIR).join("wal.log");
        let backup_path = Path::new(DB_DIR).join("wal.log.bak");
        if wal_path.exists() {
            // remove any pre-existing bak
            let _ = fs::remove_file(&backup_path);
            fs::create_dir_all(DB_DIR).unwrap();
            fs::rename(&wal_path, &backup_path).unwrap();
        } else {
            fs::create_dir_all(DB_DIR).unwrap();
        }

        // ensure WAL is initialized (this will create wal.log with header)
        let wal = WAL::global();

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
        let mut write_page = |table_id: u64, page_id: u64, data: Vec<u8>| -> RsqlResult<()> {
            wrote_pages.push((table_id, page_id, data));
            Ok(())
        };
        let mut update_page = |table_id: u64, page_id: u64, offset: u64, len: u64, data: Vec<u8>| -> RsqlResult<()> {
            updated_pages.push((table_id, page_id, offset, len, data));
            Ok(())
        };
        let mut append_page = |_: u64| -> RsqlResult<u64> { appended.push(()); Ok(0) };
        let mut trunc_page = |_: u64| -> RsqlResult<()> { truncated.push(()); Ok(()) };
        let mut max_page_idx = |_: u64| -> RsqlResult<u64> { Ok(0) };

        // ensure the WAL file cursor is at start so read_to_end reads full content
        {
            use std::io::Seek;
            let mut file = wal.log_file.lock().unwrap();
            file.seek(std::io::SeekFrom::Start(0)).unwrap();
        }
        WAL::recovery(&mut write_page, &mut update_page, &mut append_page, &mut trunc_page, &mut max_page_idx).unwrap();

        // After recovery: new_page from committed t1 should be redone
        assert!(wrote_pages.iter().any(|(t, p, d)| *t == 42 && *p == 0 && *d == data1));

        // update from uncommitted t2 should be undone (i.e., undo phase will apply old data)
        // undo applies old_data via update_page; ensure updated_pages contains an update restoring old
        assert!(updated_pages.iter().any(|(t, p, _off, _len, d)| *t == 42 && *p == 0 && *d == old));

        // cleanup: try to restore backup if existed
        if backup_path.exists() {
            // overwrite current wal.log with backup content
            let bak_bytes = fs::read(&backup_path).unwrap_or_default();
            let _ = fs::write(&wal_path, &bak_bytes);
            let _ = fs::remove_file(&backup_path);
        }
    }
}
