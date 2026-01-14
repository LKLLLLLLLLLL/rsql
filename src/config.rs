pub const _NAME: &str = "rsql";
pub const _VERSION: &str = "0.1.0";

pub const PORT: u16 = 4455;
pub const WEB_PORT: u16 = 4456;

pub const LOG_LEVEL: &str = "debug";
pub const LOG_PATH: &str = "./logs/rsql.log";

pub const THREAD_MAXNUM: u64 = 10;
pub const PAGE_SIZE_BYTES: usize = 4 * 1024; // 4 KB

pub const DB_DIR: &str = "./data";
pub const MAX_WAL_SIZE: u64 = 10 * 1024 * 1024; // 10 MB
