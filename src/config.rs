//! This file provide some configuration for rsql
//! Caution: if you changed anyone below, make sure you have deleted
//! old database files before starting rsql, or it may cause some unexpected errors.

pub const _NAME: &str = "rsql";
pub const _VERSION: &str = "0.1.0";

pub const PORT: u16 = 4455;
pub const WEB_PORT: u16 = 4456;

pub const LOG_LEVEL: &str = "debug";
pub const LOG_PATH: &str = "./logs/rsql.log";

pub const THREAD_MAXNUM: usize = 10;

pub const DB_DIR: &str = "./data";

pub const MAX_WAL_SIZE: u64 = 10 * 1024 * 1024; // 10 MB

pub const MAX_VARCHAR_SIZE: usize = 65535; // 64 KB
pub const MAX_COL_NAME_SIZE: usize = 64; // 64 bytes
pub const MAX_USERNAME_SIZE: usize = 64; // 64 bytes
pub const MAX_TABLE_NAME_SIZE: usize = 64; // 64 bytes

pub const MAX_PAGE_CACHE_BYTES: usize = 100 * 1024 * 1024; // 100 MB
pub const PAGE_SIZE_BYTES: usize = 4 * 1024; // 4 KB

pub const DEFAULT_USERNAME: &str = "root";
pub const DEFAULT_PASSWORD: &str = "password";