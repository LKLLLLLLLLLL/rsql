use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex, OnceLock, atomic::{AtomicU64, Ordering}};

use tracing::{warn, info};

use crate::config::{DB_DIR, MAX_WAL_SIZE};
use super::errors::{RsqlError, RsqlResult};

#[cfg(test)]
mod tests;

mod wal_entry;
use wal_entry::WALEntry;

static WAL_INSTANCE: OnceLock<Arc<WAL>> = OnceLock::new();
const HEADER_MAGIC: u32 = 0x4c515352; // 'RSQL' in little endian hex

/// Write-Ahead Log (WAL) structure
/// A thread safe structure to handle concurrent writes to the log file.
/// Singleton pattern is used to ensure only one instance of WAL exists.
struct WAL {
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
    pub fn new() -> RsqlResult<Self> {
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
                        write_page(*page_id, vec![0u8; 4096])?;
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
                        write_page(*page_id, vec![0u8; 4096])?;
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
