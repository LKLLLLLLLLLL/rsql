use core::panic;
use std::iter;

/// entries recorded in WAL 
#[derive(Debug, Clone)]
pub enum WALEntry {
    UpdatePage {
        tnx_id: u64,
        table_id: u64,
        page_id: u64,
        offset: u64,
        len: u64,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
    },
    NewPage { // allocate a new page
        tnx_id: u64,
        table_id: u64,
        page_id: u64, // the page id should always be the biggest one
        data: Vec<u8>,
    },
    DeletePage {
        tnx_id: u64,
        table_id: u64,
        page_id: u64, // the page id should always be the biggest one
        old_data: Vec<u8>,
    },
    OpenTnx {
        tnx_id: u64,
    },
    CommitTnx {
        tnx_id: u64,
    },
    RollbackTnx {
        tnx_id: u64,
    },
    Checkpoint {
        active_tnx_ids: Vec<u64>,
    }
}

impl WALEntry {
    /// Serialize the WAL entry to bytes
    /// return (bytes, crc)
    /// Warn: the ctc must be right after the bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        // the bytes layout:
        // [total_size: u64][operation_type: u8][operation_data...][crc: u32]
        let mut buf = Vec::with_capacity(64);
        // 1. total_size placeholders
        buf.extend(&0u64.to_le_bytes());
        // 2. operation
        match &self {
            WALEntry::UpdatePage { tnx_id, table_id, page_id, offset, len, old_data, new_data } => {
                buf.push(0u8); // operation type
                buf.extend(&tnx_id.to_le_bytes());
                buf.extend(&table_id.to_le_bytes());
                buf.extend(&page_id.to_le_bytes());
                buf.extend(&offset.to_le_bytes());
                buf.extend(&len.to_le_bytes());
                buf.extend(&(old_data.len() as u64).to_le_bytes());
                buf.extend(old_data);
                buf.extend(&(new_data.len() as u64).to_le_bytes());
                buf.extend(new_data);
            },
            WALEntry::NewPage { tnx_id, table_id, page_id, data } => {
                buf.push(1u8); // operation type
                buf.extend(&tnx_id.to_le_bytes());
                buf.extend(&table_id.to_le_bytes());
                buf.extend(&page_id.to_le_bytes());
                buf.extend(&(data.len() as u64).to_le_bytes());
                buf.extend(data);
            },
            WALEntry::DeletePage { tnx_id, table_id, page_id, old_data } => {
                buf.push(2u8); // operation type
                buf.extend(&tnx_id.to_le_bytes());
                buf.extend(&table_id.to_le_bytes());
                buf.extend(&page_id.to_le_bytes());
                buf.extend(&(old_data.len() as u64).to_le_bytes());
                buf.extend(old_data);
            },
            WALEntry::OpenTnx { tnx_id } => {
                buf.push(3u8); // operation type
                buf.extend(&tnx_id.to_le_bytes());
            },
            WALEntry::CommitTnx { tnx_id } => {
                buf.push(4u8); // operation type
                buf.extend(&tnx_id.to_le_bytes());
            },
            WALEntry::RollbackTnx { tnx_id } => {
                buf.push(5u8); // operation type
                buf.extend(&tnx_id.to_le_bytes());
            },
            WALEntry::Checkpoint { active_tnx_ids } => {
                buf.push(6u8); // operation type
                buf.extend(&(active_tnx_ids.len() as u64).to_le_bytes());
                for tnx_id in active_tnx_ids {
                    buf.extend(&tnx_id.to_le_bytes());
                }
            },
        }
        // 4. append crc placeholder
        buf.extend(&0u32.to_le_bytes());
        // 5. calculate total size
        let total_size = buf.len() as u64;
        buf[0..8].copy_from_slice(&total_size.to_le_bytes());
        // 6. calculate crc
        let crc_begin = total_size as usize - 4;
        let crc = crc32fast::hash(&buf[..crc_begin]);
        buf[crc_begin..crc_begin+4].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// A iterator to deserialize all WAL entries from a byte slice
    pub fn from_bytes(mut buf: &[u8]) -> impl Iterator<Item = Self> {
        iter::from_fn(move || {
            if buf.len() < 8 {
                return None;
            }
            // read total_size
            let mut total_size_bytes = [0u8; 8];
            total_size_bytes.copy_from_slice(&buf[..8]);
            let total_size = u64::from_le_bytes(total_size_bytes) as usize;
            if buf.len() < total_size {
                return None;
            }
            let entry_buf = &buf[..total_size];
            // verify crc
            let crc_index = total_size - 4;
            let mut crc_bytes = [0u8; 4];
            crc_bytes.copy_from_slice(&entry_buf[crc_index..]);
            let expected_crc = u32::from_le_bytes(crc_bytes);
            let actual_crc = crc32fast::hash(&entry_buf[..crc_index]);
            if expected_crc != actual_crc {
                return None;
            }
            // parse entry
            let entry_type = entry_buf[8];
            let mut offset = 9;
            let entry = match entry_type {
                0 => { // UpdatePage
                    let mut tnx_id_bytes = [0u8; 8];
                    tnx_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let tnx_id = u64::from_le_bytes(tnx_id_bytes);
                    offset += 8;
                    let mut table_id_bytes = [0u8; 8];
                    table_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let table_id = u64::from_le_bytes(table_id_bytes);
                    offset += 8;
                    let mut page_id_bytes = [0u8; 8];
                    page_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let page_id = u64::from_le_bytes(page_id_bytes);
                    offset += 8;
                    let mut offset_bytes = [0u8; 8];
                    offset_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let page_offset = u64::from_le_bytes(offset_bytes);
                    offset += 8;
                    let mut len_bytes = [0u8; 8];
                    len_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let len = u64::from_le_bytes(len_bytes);
                    offset += 8;
                    let mut old_data_len_bytes = [0u8; 8];
                    old_data_len_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let old_data_len = u64::from_le_bytes(old_data_len_bytes) as usize;
                    offset += 8;
                    let old_data = entry_buf[offset..offset+old_data_len].to_vec();
                    offset += old_data_len;
                    let mut new_data_len_bytes = [0u8; 8];
                    new_data_len_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let new_data_len = u64::from_le_bytes(new_data_len_bytes) as usize;
                    offset += 8;
                    let new_data = entry_buf[offset..offset+new_data_len].to_vec();
                    WALEntry::UpdatePage { tnx_id, table_id, page_id, offset: page_offset, len, old_data, new_data  }
                },
                1 => { // NewPage
                    let mut tnx_id_bytes = [0u8; 8];
                    tnx_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let tnx_id = u64::from_le_bytes(tnx_id_bytes);
                    offset += 8;
                    let mut table_id_bytes = [0u8; 8];
                    table_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let table_id = u64::from_le_bytes(table_id_bytes);
                    offset += 8;
                    let mut page_id_bytes = [0u8; 8];
                    page_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let page_id = u64::from_le_bytes(page_id_bytes);
                    offset += 8;
                    let mut data_len_bytes = [0u8; 8];
                    data_len_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let data_len = u64::from_le_bytes(data_len_bytes) as usize;
                    offset += 8;
                    let data = entry_buf[offset..offset+data_len].to_vec();
                    WALEntry::NewPage { tnx_id, table_id, page_id, data }
                },
                2 => { // DeletePage
                    let mut tnx_id_bytes = [0u8; 8];
                    tnx_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let tnx_id = u64::from_le_bytes(tnx_id_bytes);
                    offset += 8;
                    let mut table_id_bytes = [0u8; 8];
                    table_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let table_id = u64::from_le_bytes(table_id_bytes);
                    offset += 8;
                    let mut page_id_bytes = [0u8; 8];
                    page_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let page_id = u64::from_le_bytes(page_id_bytes);
                    offset += 8;
                    let mut old_data_len_bytes = [0u8; 8];
                    old_data_len_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let old_data_len = u64::from_le_bytes(old_data_len_bytes) as usize;
                    offset += 8;
                    let old_data = entry_buf[offset..offset+old_data_len].to_vec();
                    WALEntry::DeletePage { tnx_id, table_id, page_id, old_data }
                },
                3 => { // OpenTnx
                    let mut tnx_id_bytes = [0u8; 8];
                    tnx_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let tnx_id = u64::from_le_bytes(tnx_id_bytes);
                    // offset += 8;
                    WALEntry::OpenTnx { tnx_id }
                },
                4 => { // CommitTnx
                    let mut tnx_id_bytes = [0u8; 8];
                    tnx_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let tnx_id = u64::from_le_bytes(tnx_id_bytes);
                    // offset += 8;
                    WALEntry::CommitTnx { tnx_id }
                },
                5 => { // RollbackTnx
                    let mut tnx_id_bytes = [0u8; 8];
                    tnx_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let tnx_id = u64::from_le_bytes(tnx_id_bytes);
                    // offset += 8;
                    WALEntry::RollbackTnx { tnx_id }
                },
                6 => { // Checkpoint
                    let mut active_tnx_count_bytes = [0u8; 8];
                    active_tnx_count_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                    let active_tnx_count = u64::from_le_bytes(active_tnx_count_bytes) as usize;
                    offset += 8;
                    let mut active_tnx_ids = Vec::with_capacity(active_tnx_count);
                    for _ in 0..active_tnx_count {
                        let mut tnx_id_bytes = [0u8; 8];
                        tnx_id_bytes.copy_from_slice(&entry_buf[offset..offset+8]);
                        let tnx_id = u64::from_le_bytes(tnx_id_bytes);
                        offset += 8;
                        active_tnx_ids.push(tnx_id);
                    }
                    WALEntry::Checkpoint { active_tnx_ids }
                },
                _ => {
                    panic!("A crc-passed WAL entry has invalid entry type: {}", entry_type);
                },
            };
            // move buffer forward
            buf = &buf[total_size..];
            Some(entry)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_walentry_roundtrip() {
        let e1 = WALEntry::OpenTnx { tnx_id: 1 };
        let e2 = WALEntry::CommitTnx { tnx_id: 1 };
        let mut buf = Vec::new();
        buf.extend(e1.to_bytes());
        buf.extend(e2.to_bytes());

        let mut iter = WALEntry::from_bytes(&buf);
        let a = iter.next().expect("first entry");
        match a {
            WALEntry::OpenTnx { tnx_id } => assert_eq!(tnx_id, 1),
            _ => ::core::panic!("unexpected entry type"),
        }
        let b = iter.next().expect("second entry");
        match b {
            WALEntry::CommitTnx { tnx_id } => assert_eq!(tnx_id, 1),
            _ => ::core::panic!("unexpected entry type"),
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_walentry_crc_mismatch() {
        let e = WALEntry::OpenTnx { tnx_id: 7 };
        let mut buf = e.to_bytes();
        // flip a byte inside crc
        let len = buf.len();
        buf[len - 1] ^= 0xFF;
        let mut iter = WALEntry::from_bytes(&buf);
        assert!(iter.next().is_none(), "iterator should stop on crc mismatch");
    }
}
