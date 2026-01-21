//! This file provide some configuration for rsql
//! Caution: if you changed anyone below, make sure you have deleted
//! old database files before starting rsql, or it may cause some unexpected errors.

pub const _NAME: &str = "rsql";
pub const _VERSION: &str = "0.1.0";
pub const PORT: u16 = 4456;
pub const LOG_LEVEL: &str = "debug";
pub const LOG_PATH: &str = "./logs/rsql.log";

pub const THREAD_MAXNUM: usize = 0;

pub const DB_DIR: &str = "./data";
pub const SINGLE_FILE_MODE: bool = false; // unstable feature

pub const MAX_WAL_SIZE: u64 = 10 * 1024 * 1024; // 10 MB
pub const MAX_VARCHAR_SIZE: usize = 65535; // 64 KB
pub const MAX_COL_NAME_SIZE: usize = 64; // 64 bytes
pub const MAX_USERNAME_SIZE: usize = 64; // 64 bytes
pub const MAX_TABLE_NAME_SIZE: usize = 64; // 64 bytes

pub const MAX_PAGE_CACHE_BYTES: usize = 100 * 1024 * 1024; // 100 MB
pub const PAGE_SIZE_BYTES: usize = 16 * 1024; // 16 KB

pub const DEFAULT_USERNAME: &str = "root";
pub const DEFAULT_PASSWORD: &str = "password";

pub const MAX_RESTART_TIMES: u32 = 5;
pub const RESTART_DELAY_SECS: u64 = 2; // 2 seconds

pub const BACKUP_INTERVAL_SECS: u64 = 60*60; // 1 hour
pub const CHECKPOINT_INTERVAL_SECS: u64 = 60; // 1 minute

pub const LOCK_TIMEOUT_MS: u64 = 5000; // 5 seconds
pub const LOCK_MAX_RETRY: u32 = 3; // Maximum number of retries for acquiring a lock