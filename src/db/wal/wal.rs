use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex, OnceLock, atomic::{AtomicU64, Ordering}};

use tracing::{warn, info};

use crate::config::{DB_DIR, MAX_WAL_SIZE};
use super::super::errors::{RsqlError, RsqlResult};
use super::super::storage;

use super::wal_entry::WALEntry;


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
    /// - write_page(page_id, data): write a new page to the database
    /// - update_page(page_id, offset, len, data): update an existing page in the database
    /// - append_page(data): append a new page to the database, return the new page id
    pub fn recovery(
        write_page: &mut dyn FnMut(u64, Vec<u8>) -> RsqlResult<()>,
        update_page: &mut dyn FnMut(u64, u64, u64, Vec<u8>) -> RsqlResult<()>,
        append_page: &mut dyn FnMut(u64, Vec<u8>) -> RsqlResult<u64>,
    ) -> RsqlResult<()> {
        info!("Starting WAL recovery");
        let wal = WAL::global();
        let mut file = wal.log_file.lock().unwrap();
        // 1. read all entries
        let mut entrys = Vec::new();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        for entry in WALEntry::from_bytes(&buf[4..]) {
            entrys.push(entry);
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
                WALEntry::UpdatePage { tnx_id, page_id, offset, len, new_data, .. } => {
                    if redo_tnx_ids.contains(tnx_id) {
                        update_page(*page_id, *offset, *len, new_data.clone())?;
                        recover_num += 1;
                    }
                },
                WALEntry::NewPage { tnx_id, data, .. } => {
                    if redo_tnx_ids.contains(tnx_id) {
                        append_page(0, data.clone())?;
                        recover_num += 1;
                    }
                },
                WALEntry::DeletePage { tnx_id, page_id, .. } => {
                    if redo_tnx_ids.contains(tnx_id) {
                        write_page(*page_id, vec![0u8; storage::Page::max_size()])?;
                        recover_num += 1;
                    }
                },
                _ => {},
            }
        }
        // 5. undo operations
        for entry in entrys.iter().rev().take(entrys.len() - checkpoint_index) {
            match entry {
                WALEntry::UpdatePage { tnx_id, page_id, offset, len, old_data, .. } => {
                    if undo_tnx_ids.contains(tnx_id) {
                        update_page(*page_id, *offset, *len, old_data.clone())?;
                        recover_num += 1;
                    }
                },
                WALEntry::NewPage { tnx_id, page_id, .. } => {
                    if undo_tnx_ids.contains(tnx_id) {
                        write_page(*page_id, vec![0u8; storage::Page::max_size()])?;
                        recover_num += 1;
                    }
                },
                WALEntry::DeletePage { tnx_id, page_id, old_data } => {
                    if undo_tnx_ids.contains(tnx_id) {
                        write_page(*page_id, old_data.clone())?;
                        recover_num += 1;
                    }
                },
                _ => {},
            }
        }
        info!("WAL recovery completed, {} operations applied", recover_num);
        Ok(())
    }

    pub fn checkpoint(&self) -> RsqlResult<()> {
        info!("Starting WAL checkpoint");
        let active_tnx_ids = self.active_tnx_ids.lock().unwrap();
        let mut file = self.log_file.lock().unwrap();
        // 1. construct simplified wal log
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
        // 2. write new wal log
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
        // 3. rename new log file to current log file
        // THIS MUST BE ATOMIC OPERATION
        fs::rename(std::path::Path::new(DB_DIR).join("wal.log.tmp"), std::path::Path::new(DB_DIR).join("wal.log"))?;
        // 4. update self
        *file = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(std::path::Path::new(DB_DIR).join("wal.log"))?;
        self.length.store(file.metadata()?.len(), Ordering::SeqCst); // only header left
        info!("WAL checkpoint completed");
        Ok(())
    }

    fn append_entry(&self, entry: &WALEntry) -> RsqlResult<bool> {
        let entry_bytes = entry.to_bytes();
        let mut log_file = self.log_file.lock().unwrap();
        // 1. write entry bytes
        log_file.write_all(&entry_bytes)?;
        // 2. update length
        let new_length = self.length.fetch_add(entry_bytes.len() as u64, Ordering::SeqCst) + entry_bytes.len() as u64;
        Ok(new_length > MAX_WAL_SIZE)
    }

    fn flush(&self) -> RsqlResult<()> {
        let mut log_file = self.log_file.lock().unwrap();
        log_file.flush()?;
        log_file.sync_all()?;
        Ok(())
    }

    pub fn update_page(
        &self,
        tnx_id: u64,
        page_id: u64,
        offset: u64,
        len: u64,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
    ) -> RsqlResult<bool> {
        let entry = WALEntry::UpdatePage {
            tnx_id,
            page_id,
            offset,
            len,
            old_data,
            new_data,
        };
        self.append_entry(&entry)
    }
    pub fn new_page(
        &self,
        tnx_id: u64,
        page_id: u64,
        data: Vec<u8>,
    ) -> RsqlResult<bool> {
        let entry = WALEntry::NewPage {
            tnx_id,
            page_id,
            data,
        };
        self.append_entry(&entry)
    }
    pub fn delete_page(
        &self,
        tnx_id: u64,
        page_id: u64,
        old_data: Vec<u8>,
    ) -> RsqlResult<bool> {
        let entry = WALEntry::DeletePage {
            tnx_id,
            page_id,
            old_data,
        };
        self.append_entry(&entry)
    }

    pub fn open_tnx(&self, tnx_id: u64) -> RsqlResult<bool> {
        let entry = WALEntry::OpenTnx {
            tnx_id
        };
        self.active_tnx_ids.lock().unwrap().push(tnx_id);
        self.append_entry(&entry)
    }
    pub fn commit_tnx(&self, tnx_id: u64) -> RsqlResult<bool> {
        let entry = WALEntry::CommitTnx {
            tnx_id
        };
        self.active_tnx_ids.lock().unwrap().retain(|&id| id != tnx_id);
        let need_checkpoint = self.append_entry(&entry)?;
        self.flush()?;
        Ok(need_checkpoint)
    }
    pub fn rollback_tnx(&self, tnx_id: u64) -> RsqlResult<bool> {
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
    use std::fs;
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::OnceLock as StdOnceLock;

    use crate::config::DB_DIR;
    use crate::db::errors::RsqlResult;

    static TEST_LOCK: StdOnceLock<Mutex<()>> = StdOnceLock::new();

    fn setup_clean() -> std::path::PathBuf {
        // create an isolated temp dir and chdir into it, return previous cwd
        let prev = std::env::current_dir().unwrap();
        let mut tmp = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let uniq = format!("rsql_test_{}_{}", std::process::id(), nanos);
        tmp.push(uniq);
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        std::env::set_current_dir(&tmp).unwrap();
        // ensure DB_DIR exists inside this temp dir
        let _ = fs::remove_dir_all(DB_DIR);
        fs::create_dir_all(DB_DIR).unwrap();
        prev
    }

    fn teardown_clean(prev: std::path::PathBuf) {
        let _ = fs::remove_file(Path::new(DB_DIR).join("wal.log"));
        let _ = fs::remove_file(Path::new(DB_DIR).join("wal.log.tmp"));
        let _ = fs::remove_dir_all(DB_DIR);
        // restore cwd and attempt to remove the temp dir
        let cur = std::env::current_dir().unwrap();
        std::env::set_current_dir(prev).unwrap();
        let _ = fs::remove_dir_all(cur);
    }

    #[test]
    fn test_wal_new_append_flush() {
        // serialize filesystem tests to avoid parallel interference
        let _g = TEST_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|p| p.into_inner());
        let prev = setup_clean();
        let wal = WAL::new().expect("create wal");
        // append an OpenTnx entry
        let need_checkpoint = wal.append_entry(&WALEntry::OpenTnx { tnx_id: 42 }).unwrap();
        assert!(!need_checkpoint);
        wal.flush().unwrap();
        // ensure file cursor is at start so recovery can read entries
        {
            use std::io::{Seek, SeekFrom};
            let mut f = wal.log_file.lock().unwrap_or_else(|p| p.into_inner());
            f.seek(SeekFrom::Start(0)).unwrap();
        }
        // also ensure the global WAL (used by recovery) reads from start
        {
            use std::io::{Seek, SeekFrom};
            let gw = WAL::global();
            let mut gf = gw.log_file.lock().unwrap_or_else(|p| p.into_inner());
            gf.seek(SeekFrom::Start(0)).unwrap();
        }

        let bytes = fs::read(Path::new(DB_DIR).join("wal.log")).expect("read wal log");
        assert!(bytes.len() > 4, "wal log should contain header + entries");
        let mut iter = WALEntry::from_bytes(&bytes[4..]);
        let e = iter.next().expect("entry exists");
        match e {
            WALEntry::OpenTnx { tnx_id } => assert_eq!(tnx_id, 42),
            _ => ::core::panic!("unexpected entry type"),
        }
        teardown_clean(prev);
    }

    #[test]
    fn test_checkpoint_filters_entries() {
        // serialize filesystem tests to avoid parallel interference
        let _g = TEST_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|p| p.into_inner());
        let prev = setup_clean();
        let wal = WAL::new().expect("create wal");
        // simulate an active transaction
        wal.active_tnx_ids.lock().unwrap().push(100);

        // add some entries: open, commit for tnx 100 and an unrelated tnx 200
        wal.append_entry(&WALEntry::OpenTnx { tnx_id: 100 }).unwrap();
        wal.append_entry(&WALEntry::CommitTnx { tnx_id: 100 }).unwrap();
        wal.append_entry(&WALEntry::OpenTnx { tnx_id: 200 }).unwrap();
        wal.append_entry(&WALEntry::CommitTnx { tnx_id: 200 }).unwrap();
        wal.flush().unwrap();

        // checkpoint should keep entries related to active tnx (100) only
        wal.checkpoint().expect("checkpoint");

        let bytes = fs::read(Path::new(DB_DIR).join("wal.log")).expect("read wal log after checkpoint");
        let mut iter = WALEntry::from_bytes(&bytes[4..]);
        // Expect entries for tnx 100 (OpenTnx and CommitTnx) but not for tnx 200
        let first = iter.next().expect("first");
        match first {
            WALEntry::OpenTnx { tnx_id } => assert_eq!(tnx_id, 100),
            _ => ::core::panic!("unexpected first entry"),
        }
        let second = iter.next().expect("second");
        match second {
            WALEntry::CommitTnx { tnx_id } => assert_eq!(tnx_id, 100),
            _ => ::core::panic!("unexpected second entry"),
        }
        teardown_clean(prev);
    }

    #[test]
    fn test_recovery_redo_and_undo() {
        // serialize filesystem tests to avoid parallel interference
        let _g = TEST_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|p| p.into_inner());
        let prev = setup_clean();

        // create WAL and write entries: checkpoint, tnx1 (open->update->commit), tnx2 (open->update no commit)
        let wal = WAL::new().expect("create wal");
        wal.append_entry(&WALEntry::Checkpoint { active_tnx_ids: Vec::new() }).unwrap();

        // tnx1: committed -> should be REDO with new_data
        wal.append_entry(&WALEntry::OpenTnx { tnx_id: 1 }).unwrap();
        let old1 = vec![9u8,9,9];
        let new1 = vec![1u8,2,3];
        wal.append_entry(&WALEntry::UpdatePage { tnx_id: 1, page_id: 5, offset: 0, len: 3, old_data: old1.clone(), new_data: new1.clone() }).unwrap();
        wal.append_entry(&WALEntry::CommitTnx { tnx_id: 1 }).unwrap();

        // tnx2: not committed -> should be UNDO with old_data
        wal.append_entry(&WALEntry::OpenTnx { tnx_id: 2 }).unwrap();
        let old2 = vec![7u8,7,7];
        let new2 = vec![4u8,4,4];
        wal.append_entry(&WALEntry::UpdatePage { tnx_id: 2, page_id: 6, offset: 1, len: 3, old_data: old2.clone(), new_data: new2.clone() }).unwrap();

        wal.flush().unwrap();
        // ensure file cursor is at start so recovery can read entries
        {
            use std::io::{Seek, SeekFrom};
            let mut f = wal.log_file.lock().unwrap_or_else(|p| p.into_inner());
            f.seek(SeekFrom::Start(0)).unwrap();
        }
        // also ensure the global WAL (used by recovery) reads from start
        {
            use std::io::{Seek, SeekFrom};
            let gw = WAL::global();
            let mut gf = gw.log_file.lock().unwrap_or_else(|p| p.into_inner());
            gf.seek(SeekFrom::Start(0)).unwrap();
        }

        // prepare collectors for callbacks
        let mut wrote_pages: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut updated_pages: Vec<(u64,u64,u64,Vec<u8>)> = Vec::new();
        let mut appended: Vec<Vec<u8>> = Vec::new();

        let mut write_page = |page_id: u64, data: Vec<u8>| -> RsqlResult<()> {
            wrote_pages.push((page_id, data));
            Ok(())
        };
        let mut update_page = |page_id: u64, offset: u64, len: u64, data: Vec<u8>| -> RsqlResult<()> {
            updated_pages.push((page_id, offset, len, data));
            Ok(())
        };
        let mut append_page = |_len: u64, data: Vec<u8>| -> RsqlResult<u64> {
            appended.push(data);
            Ok(0)
        };

        // run recovery
        WAL::recovery(&mut write_page, &mut update_page, &mut append_page).expect("recovery");

        // verify redo applied new1 to page 5
        let found_redo = updated_pages.iter().any(|(pid, _off, _len, data)| *pid == 5 && *data == new1);
        assert!(found_redo, "redo new_data for tnx1 should be applied");

        // verify undo applied old2 to page 6
        let found_undo = updated_pages.iter().any(|(pid, _off, _len, data)| *pid == 6 && *data == old2);
        assert!(found_undo, "undo old_data for tnx2 should be applied");

        teardown_clean(prev);
    }
}
