use std::fs::{self, File};
use std::path::{Path};
use tracing::{debug};
use tar::{Builder, Archive};
use chrono::Local;

use crate::common::{RsqlResult, RsqlError};
use crate::config::DB_DIR;

/// Single file mode: Packs data/sys and data/tables into data/rsql.db and deletes originals
pub fn archive_single_file() -> RsqlResult<()> {
    let db_path = Path::new(DB_DIR);
    let archive_path = db_path.join("rsql.db");
    
    debug!("Archiving to single file: {}", archive_path.display());

    let file = File::create(&archive_path)
        .map_err(|e| RsqlError::StorageError(format!("Failed to create archive: {}", e)))?;
    let mut builder = Builder::new(file);

    let sys_path = db_path.join("sys");
    let tables_path = db_path.join("tables");

    if sys_path.exists() {
        builder.append_dir_all("sys", &sys_path)
            .map_err(|e| RsqlError::StorageError(format!("Failed to pack sys: {}", e)))?;
    }
    if tables_path.exists() {
        builder.append_dir_all("tables", &tables_path)
            .map_err(|e| RsqlError::StorageError(format!("Failed to pack tables: {}", e)))?;
    }

    builder.finish()
        .map_err(|e| RsqlError::StorageError(format!("Failed to finish archive: {}", e)))?;

    // Hack: delete original directories
    if sys_path.exists() {
        fs::remove_dir_all(&sys_path)
            .map_err(|e| RsqlError::StorageError(format!("Failed to remove sys dir: {}", e)))?;
    }
    if tables_path.exists() {
        fs::remove_dir_all(&tables_path)
            .map_err(|e| RsqlError::StorageError(format!("Failed to remove tables dir: {}", e)))?;
    }

    debug!("Single file archive completed.");
    Ok(())
}

/// Single file mode: Unpacks data/rsql.db into data/sys and data/tables
pub fn init_single_file() -> RsqlResult<()> {
    let db_path = Path::new(DB_DIR);
    let archive_path = db_path.join("rsql.db");

    if !archive_path.exists() {
        debug!("Archive {} not found, skipping init.", archive_path.display());
        return Ok(());
    }

    debug!("Unpacking single file: {}", archive_path.display());

    let file = File::open(&archive_path)
        .map_err(|e| RsqlError::StorageError(format!("Failed to open archive: {}", e)))?;
    let mut archive = Archive::new(file);

    archive.unpack(db_path)
        .map_err(|e| RsqlError::StorageError(format!("Failed to unpack: {}", e)))?;

    debug!("Single file unpack completed.");
    Ok(())
}

/// Backup: Packs data/sys, data/tables, and data/wal.log into data/backup/{TIMESTAMP}.bak
pub fn backup() -> RsqlResult<String> {
    let db_path = Path::new(DB_DIR);
    let backup_dir = db_path.join("backup");
    
    if !backup_dir.exists() {
        fs::create_dir_all(&backup_dir)
            .map_err(|e| RsqlError::StorageError(format!("Failed to create backup dir: {}", e)))?;
    }

    let timestamp = Local::now().format("%y_%m_%d_%H_%M_%S").to_string();
    let backup_file_name = format!("{}.bak", timestamp);
    let backup_file_path = backup_dir.join(&backup_file_name);
    
    debug!("Creating backup: {}", backup_file_path.display());

    let file = File::create(&backup_file_path)
        .map_err(|e| RsqlError::StorageError(format!("Failed to create backup file: {}", e)))?;
    let mut builder = Builder::new(file);

    let sys_path = db_path.join("sys");
    let tables_path = db_path.join("tables");
    let wal_path = db_path.join("wal.log");

    if sys_path.exists() {
        builder.append_dir_all("sys", &sys_path)
            .map_err(|e| RsqlError::StorageError(format!("Failed to pack sys for backup: {}", e)))?;
    }
    if tables_path.exists() {
        builder.append_dir_all("tables", &tables_path)
            .map_err(|e| RsqlError::StorageError(format!("Failed to pack tables for backup: {}", e)))?;
    }
    if wal_path.exists() {
        builder.append_path_with_name(&wal_path, "wal.log")
            .map_err(|e| RsqlError::StorageError(format!("Failed to pack wal.log for backup: {}", e)))?;
    }

    builder.finish()
        .map_err(|e| RsqlError::StorageError(format!("Failed to finish backup: {}", e)))?;

    debug!("Backup completed: {}", backup_file_path.display());
    Ok(backup_file_path.to_string_lossy().into_owned())
}

/// Restore: Unpacks a .bak file into data/
pub fn restore_backup(backup_path: &str) -> RsqlResult<()> {
    let path = Path::new(backup_path);
    if !path.exists() {
        return Err(RsqlError::StorageError(format!("Backup file not found: {}", backup_path)));
    }

    let db_path = Path::new(DB_DIR);
    debug!("Restoring from backup: {}", path.display());

    if !db_path.exists() {
        fs::create_dir_all(db_path)?;
    }

    let file = File::open(path)
        .map_err(|e| RsqlError::StorageError(format!("Failed to open backup: {}", e)))?;
    let mut archive = Archive::new(file);

    archive.unpack(db_path)
        .map_err(|e| RsqlError::StorageError(format!("Failed to unpack backup: {}", e)))?;

    debug!("Restore completed.");
    Ok(())
}

/// Get the path of the latest backup file in data/backup
pub fn get_latest_backup() -> Option<String> {
    let db_path = Path::new(DB_DIR);
    let backup_dir = db_path.join("backup");

    if !backup_dir.exists() {
        return None;
    }

    let entries = fs::read_dir(backup_dir).ok()?;
    let mut backups: Vec<_> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()? == "bak" {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    // Sort by file name (which contains timestamp) descending
    backups.sort();
    backups.pop().map(|p| p.to_string_lossy().into_owned())
}
