use super::*;
use std::fs;
use std::path::Path;
use std::sync::OnceLock as StdOnceLock;

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
    let mut update_page = |page_id: u64, offset: u64, len: u64, data: Vec<u8>| -> RsqlResult<(())> {
        updated_pages.push((page_id, offset, len, data));
        Ok((()))
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