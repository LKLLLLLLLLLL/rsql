mod btree_index;
mod storage;
pub mod wal;  // 改为public，允许外部访问WAL
mod allocator;
mod consist_storage;
pub mod table;
pub use table::Table;
