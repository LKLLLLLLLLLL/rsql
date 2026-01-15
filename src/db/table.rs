use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Mutex;
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;

use super::storage;
use super::super::config;
use super::errors::{RsqlError, RsqlResult};
use super::data_item::DataItem;
use super::super::config::MAX_VARCHAR_SIZE;
use super::wal;
use super::btree_index;

pub enum ColType {
    Integer,
    Float,
    Chars(usize),
    VarChar,
    Bool
}

struct TableColumn {
    name: String, // fix 64 bytes
    data_type: ColType,
    pk: bool,
    nullable: bool,
    index: bool,
    unique: bool, // TODO: not implemented
}

struct TableSchema {
    columns: Vec<TableColumn>,
}

impl TableSchema {
    /// Bytes structure in disk:
    /// [schema_length: 8bytes][col1_name: 64bytes][col1_type: 1byte][col1_extra: 8bytes][col1_pk:1byte][col1_nullable:1byte][col1_unique:1byte][col1_index:1byte]...
    /// each column takes 76bytes
    fn from_bytes(bytes: &[u8]) -> RsqlResult<(Self, u64)> {
        let mut offset = 0;
        let schema_length_bytes = &bytes[offset..offset+8];
        let schema_length = u64::from_le_bytes(schema_length_bytes.try_into().unwrap());
        offset += 8;
        let mut columns = vec![];
        while offset + 76 <= schema_length as usize {
            let name_bytes = &bytes[offset..offset+64];
            let name = String::from_utf8(name_bytes.iter().cloned().take_while(|&b| b != 0).collect())
                .map_err(|_| RsqlError::StorageError("Invalid column name".to_string()))?;
            offset += 64;
            let col_type_byte = bytes[offset];
            offset += 1;
            let extra_bytes = &bytes[offset..offset+8];
            let extra = u64::from_le_bytes(extra_bytes.try_into().unwrap());
            offset += 8;
            let pk = bytes[offset] != 0;
            offset += 1;
            let nullable = bytes[offset] != 0;
            offset += 1;
            let unique = bytes[offset] != 0;
            offset += 1;
            let index = bytes[offset] != 0;
            offset += 1;
            let data_type = match col_type_byte {
                0 => ColType::Integer,
                1 => ColType::Float,
                2 => ColType::Chars(extra as usize),
                3 => ColType::VarChar,
                4 => ColType::Bool,
                _ => return Err(RsqlError::StorageError("Invalid column type".to_string())),
            };
            columns.push(TableColumn {
                name,
                data_type,
                pk,
                nullable,
                unique,
                index,
            });
        }
        Ok((TableSchema { columns }, schema_length))
    }
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 8];
        for col in &self.columns {
            if col.name.len() > 64 {
                panic!("Column name too long");
            }
            let mut name_bytes = [0u8; 64];
            name_bytes[..col.name.len()].copy_from_slice(col.name.as_bytes());
            buf.extend_from_slice(&name_bytes);
            match col.data_type {
                ColType::Integer => {
                    buf.push(0u8);
                    buf.extend_from_slice(&0u64.to_le_bytes());
                }
                ColType::Float => {
                    buf.push(1u8);
                    buf.extend_from_slice(&0u64.to_le_bytes());
                }
                ColType::Chars(size) => {
                    buf.push(2u8);
                    buf.extend_from_slice(&(size as u64).to_le_bytes());
                }
                ColType::VarChar => {
                    buf.push(3u8);
                    buf.extend_from_slice(&0u64.to_le_bytes());
                }
                ColType::Bool => {
                    buf.push(4u8);
                    buf.extend_from_slice(&0u64.to_le_bytes());
                }
            }
            buf.push(if col.pk { 1u8 } else { 0u8 });
            buf.push(if col.nullable { 1u8 } else { 0u8 });
            buf.push(if col.unique { 1u8 } else { 0u8 });
            buf.push(if col.index { 1u8 } else { 0u8 });
        }
        // write schema length at the beginning
        let schema_length = buf.len() as u64;
        buf[..8].copy_from_slice(&schema_length.to_le_bytes());
        buf
    }
    pub fn satisfy(&self, data: &Vec<DataItem>) -> RsqlResult<()> {
        // 1. check if data length matches
        if data.len() != self.columns.len() {
            return Err(RsqlError::InvalidInput(
                format!("Data length {} does not match schema length {}", data.len(), self.columns.len())));
        }
        // 2. check nullable
        for (i, col) in self.columns.iter().enumerate() {
            let data_item = &data[i];
            match data_item {
                DataItem::NullInt | DataItem::NullFloat | 
                DataItem::NullVarChar | DataItem::NullBool | 
                DataItem::NullChars { .. } => {
                    return Err(RsqlError::InvalidInput(
                    format!("Null value found for non-nullable column {}", col.name)));
                },
                _ => {},
            }
        }
        // 3. check data type
        for (i, col) in self.columns.iter().enumerate() {
            match col.data_type {
                ColType::Integer => match data[i] {
                    DataItem::Integer(_) => {},
                    _ => return Err(RsqlError::InvalidInput(
                        format!("Expected Integer for column {}, found different type", col.name))),
                },
                ColType::Float => match data[i] {
                    DataItem::Float(_) => {},
                    _ => return Err(RsqlError::InvalidInput(
                        format!("Expected Float for column {}, found different type", col.name))),
                },
                ColType::Chars(size) => match &data[i] {
                    DataItem::Chars{ len, value } => {
                        if *len as usize != size {
                            return Err(RsqlError::InvalidInput(
                                format!("Expected Chars({}) for column {}, found Chars({})", size, col.name, len)));
                        }
                        if value.len() > size {
                            return Err(RsqlError::InvalidInput(
                                format!("Value length {} exceeds size {} for column {}", value.len(), size, col.name)));
                        }
                    },
                    _ => return Err(RsqlError::InvalidInput(
                        format!("Expected Chars({}) for column {}, found different type", size, col.name))),
                },
                ColType::VarChar => match &data[i] {
                    DataItem::VarChar{ head: _, value } => {
                        if value.len() > MAX_VARCHAR_SIZE {
                            return Err(RsqlError::InvalidInput(
                                format!("Value length {} exceeds max varchar size {} for column {}", value.len(), MAX_VARCHAR_SIZE, col.name)));
                        }
                    },
                    _ => return Err(RsqlError::InvalidInput(
                        format!("Expected VarChar for column {}, found different type", col.name))),
                },
                ColType::Bool => match data[i] {
                    DataItem::Bool(_) => {},
                    _ => return Err(RsqlError::InvalidInput(
                        format!("Expected Bool for column {}, found different type", col.name))),
                },
            }
        }
        Ok(())
    }
    pub fn new(columns: Vec<TableColumn>) -> Self {
        // check if the varchar columns is indexed
        for col in &columns {
            if col.index {
                match col.data_type {
                    ColType::VarChar => {
                        panic!("VarChar column {} cannot be indexed", col.name);
                    },
                    _ => {},
                }
            }
        }
        Self { columns }
    }
    pub fn get_sizes(&self) -> Vec<usize> {
        let mut sizes = vec![];
        for col in &self.columns {
            sizes.push(DataItem::cal_size_from_coltype(&col.data_type));
        };
        sizes
    }
}

const HEADER_MAGIC: u32 = 0x4c515352; // 'RSQL' in little endian hex

// Guard to avoid instantiating Table on same table file multiple times
static TABLE_GUARD: OnceLock<Mutex<HashSet<u64>>> = OnceLock::new();
fn get_table_guard() -> &'static Mutex<HashSet<u64>> {
    TABLE_GUARD.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Data sturcture manage a table in database
/// CAUTION: the table schema, table name is stored in other places, not in this file
/// The metadata stored in the first page in the table file:
/// - magic number: 4 bytes
/// - version: 4 bytes
/// - indexes count: 8 bytes
/// - each index [column_name: 64bytes][root_page: 8bytes]
pub struct Table {
    id: u64,
    schema: TableSchema,
    indexes: HashMap<String, btree_index::BTreeIndex>, // column name -> index

    storage_manager: storage::StorageManager,
    wal: Arc<wal::WAL>,
}

impl Drop for Table {
    fn drop(&mut self) {
        let guard = get_table_guard();
        let mut guard = guard.lock().unwrap();
        guard.remove(&self.id);
    }
}

impl Table {
    pub fn from(id: u64, schema: TableSchema) -> RsqlResult<Self> {
        // check if table already opened
        let guard = get_table_guard();
        let mut guard = guard.lock().unwrap();
        if guard.contains(&id) {
            panic!("Table {} already opened in this process", id);
        }
        guard.insert(id);
        // open table file
        let path = config::DB_DIR.to_string() + "/" + &id.to_string() + ".dbt"; // .dbt for database table
        let storage_manager = storage::StorageManager::new(&path)?;
        let wal = wal::WAL::global();
        if let None = storage_manager.max_page_index() {
            return Err(RsqlError::StorageError(format!("Table {id} file is empty, maybe corrupted")));
        };
        let header_page = storage_manager
            .read_page(0)?;
        // read magic number and version
        let magic = u32::from_le_bytes(header_page.data[0..4].try_into().unwrap());
        if magic != HEADER_MAGIC {
            return Err(RsqlError::StorageError("Invalid table file, has wrong magic number".to_string()));
        }
        let version = u32::from_le_bytes(header_page.data[4..8].try_into().unwrap()); // unused for now
        if version != 1 {
            panic!("Unsupported table file version: {}", version);
        }
        // read indexes
        let mut offset = 4 + 4;
        let indexes_count_bytes = &header_page.data[offset..offset+8];
        let indexes_count = u64::from_le_bytes(indexes_count_bytes.try_into().unwrap());
        offset += 8;
        let mut indexes = HashMap::new();
        for _ in 0..indexes_count {
            let col_name_bytes = &header_page.data[offset..offset+64];
            let col_name = String::from_utf8(col_name_bytes.iter().cloned().take_while(|&b| b != 0).collect())
                .map_err(|_| RsqlError::StorageError("Invalid index column name".to_string()))?;
            offset += 64;
            let root_page_bytes = &header_page.data[offset..offset+8];
            let root_page = u64::from_le_bytes(root_page_bytes.try_into().unwrap());
            offset += 8;
            let btree_index = btree_index::BTreeIndex::from(root_page)?;
            indexes.insert(col_name, btree_index);
        };
        // check if indexes compatible with schema
        if indexes.len() != schema.columns.iter().filter(|col| col.index).count() {
            panic!("Incompatible index count between schema and table file {}", path);
        }
        Ok(Table {
            id,
            schema,
            storage_manager,
            wal,
            indexes,
        })
    }
    /// Create a new table with given schema
    pub fn create(id: u64, schema: TableSchema, tnx_id: u64) -> RsqlResult<Self> {
        // check if table already opened
        let guard = get_table_guard();
        let mut guard = guard.lock().unwrap();
        if guard.contains(&id) {
            panic!("Table {} already opened in this process", id);
        }
        guard.insert(id);
        // create table file
        let path = config::DB_DIR.to_string() + "/" + &id.to_string() + ".dbt"; // .dbt for database table
        let storage_manager = storage::StorageManager::new(&path)?;
        let wal = wal::WAL::global();
        // 1. collect indexes info
        let mut index_cols = HashSet::new();
        for col in &schema.columns {
            if col.index {
                index_cols.insert(col.name.clone());
            }
        }
        // 2. new indexes
        let mut indexes = HashMap::new();
        let storage_manager_cell = RefCell::new(storage_manager);
        for col_name in &index_cols {
            let btree_index = btree_index::BTreeIndex::new(
                || {
                    let (page_idx, page) = storage_manager_cell.borrow_mut().new_page()?;
                    wal.new_page(tnx_id, id, page_idx, vec![0u8; storage::Page::max_size()])?;
                    Ok((page, page_idx))
                },
                | page_idx, page_data | {
                    wal.update_page(
                        tnx_id, id, 
                        page_idx, 
                        0, 
                        page_data.data.len() as u64, 
                        vec![], 
                        page_data.data.clone()
                    )?;
                    storage_manager_cell.borrow_mut().write_page(page_data, page_idx)
                }
            )?;
            indexes.insert(col_name.clone(), btree_index);
        }
        // 3. collect header page bytes
        let mut page_data: Vec<u8> = vec![0u8; storage::Page::max_size()];
        page_data[0..4].copy_from_slice(&HEADER_MAGIC.to_le_bytes());
        page_data[4..8].copy_from_slice(&1u32.to_le_bytes()); // version 1
        let indexes_count = indexes.len() as u64;
        let mut offset = 4 + 4;
        page_data[offset..offset+8].copy_from_slice(&indexes_count.to_le_bytes());
        offset += 8;
        for (col_name, btree_index) in &indexes {
            let mut col_name_bytes = [0u8; 64];
            if col_name.len() > 64 {
                panic!("Column name too long");
            }
            col_name_bytes[..col_name.len()].copy_from_slice(col_name.as_bytes());
            page_data[offset..offset+64].copy_from_slice(&col_name_bytes);
            offset += 64;
            page_data[offset..offset+8].copy_from_slice(&btree_index.root_page_num().to_le_bytes());
            offset += 8;
        }
        // 4. write wal first
        wal.new_page(tnx_id, id, 0, page_data.clone())?;
        // 5. write head to disk
        let (page_num, mut header_page) = storage_manager_cell.borrow_mut()
            .new_page()?;
        if page_num != 0 {
            panic!("First page of table file should be page 0");
        }
        header_page.data = page_data;
        Ok(Table {
            id,
            schema,
            storage_manager: storage_manager_cell.into_inner(),
            wal,
            indexes,
        })
    }
    /// Drop the table
    /// This implements will only set the table file length to 0
    /// TODO: support deleting the table file
    pub fn drop(mut self, tnx_id: u64) -> RsqlResult<()> {
        let page_max_idx = self.storage_manager.max_page_index().unwrap_or(0);
        // 1. log drop table in wal
        for page_idx in 0..=page_max_idx {
            let page = self.storage_manager.read_page(page_idx)?;
            self.wal.delete_page(
                tnx_id, 
                self.id, 
                page_idx, 
                page.data.clone())?;
        }
        // 2. truncate the file
        for page_idx in (0..=page_max_idx).rev() {
            let freed_page = self.storage_manager.free()?;
            if freed_page != page_idx {
                panic!("Free page index mismatch when dropping table");
            }
        }
        Ok(())
    }
    pub fn get_row_by_pk(&self, pk: &DataItem) -> RsqlResult<Option<Vec<DataItem>>> {
        // find the primary key column
        let pk_col = self.schema.columns.iter().find(|col| col.pk);
        if pk_col.is_none() {
            return Err(RsqlError::InvalidInput("Table has no primary key".to_string()));
        }
        let pk_col = pk_col.unwrap();
        // find the index for primary key column
        let index = self.indexes.get(&pk_col.name);
        if index.is_none() {
            return Err(RsqlError::InvalidInput("Primary key column has no index".to_string()));
        }
        let index = index.unwrap();
        // search the index
        let pair_opt = index.find_entry(pk.clone(), &|page_idx| {
            self.storage_manager.read_page(page_idx)
        })?;
        let (match_page, match_offset) = match pair_opt {
            Some(pair) => pair,
            None => return Ok(None),
        };
        // read the row from data page
        let data_page = self.storage_manager.read_page(match_page)?;
        let sizes = self.schema.get_sizes();
        let mut offset = match_offset as usize;
        let mut row = vec![];
        for size in sizes {
            let data_item = DataItem::from_bytes(&data_page.data[offset..offset+size], None)?;
            row.push(data_item);
            offset += size;
        };
        Ok(Some(row))
    }
    pub fn get_row_by_indexed_col(&self, col_name: &str, value: &DataItem) -> RsqlResult<Option<Vec<DataItem>>> {
        // get index
        let index = self.indexes.get(col_name);
        if index.is_none() {
            panic!("Column {} is not indexed, cannot search", col_name);
        };
        let index = index.unwrap();
        // search the index
        let pair_opt = index.find_entry(value.clone(), &|page_idx| {
            self.storage_manager.read_page(page_idx)
        })?;
        let (match_page, match_offset) = match pair_opt {
            Some(pair) => pair,
            None => return Ok(None),
        };
        // read the row from data page
        let data_page = self.storage_manager.read_page(match_page)?;
        let sizes = self.schema.get_sizes();
        let mut offset = match_offset as usize;
        let mut row = vec![];
        for size in sizes {
            let data_item = DataItem::from_bytes(&data_page.data[offset..offset+size], None)?;
            row.push(data_item);
            offset += size;
        };
        Ok(Some(row))
    }
    pub fn get_rows_by_range_indexed_col(
        &self,
        col_name: &str,
        start: &DataItem,
        end: &DataItem,
    ) -> RsqlResult<impl Iterator<Item = RsqlResult<Vec<DataItem>>>> {
        // get index
        let index = self.indexes.get(col_name).ok_or(RsqlError::InvalidInput(
            format!("Column {} is not indexed, cannot search", col_name)
        ))?;
        // get iterator from index (注意 unwrap Result)
        let entry_iter = index.find_range_entry(start.clone(), end.clone(), |page_idx| {
            self.storage_manager.read_page(page_idx)
        })?;

        // map to row iterator
        let iter = entry_iter.map(move |pair_res| {
            let (match_page, match_offset) = pair_res?;
            // read the row from data page
            let data_page = self.storage_manager.read_page(match_page)?;
            let sizes = self.schema.get_sizes();
            let mut offset = match_offset as usize;
            let mut row = vec![];
            for size in sizes {
                let data_item = DataItem::from_bytes(&data_page.data[offset..offset+size], None)?;
                row.push(data_item);
                offset += size;
            };
            Ok(row)
        });
        Ok(iter)
    }
    pub fn get_all_rows(&self) -> RsqlResult<impl Iterator<Item = RsqlResult<Vec<DataItem>>>> {
        // find primary key column
        let pk_col = self.schema.columns.iter().find(|col| col.pk);
        if pk_col.is_none() {
            panic!("Table has no primary key column, cannot get all rows");
        }
        let pk_col = pk_col.unwrap();
        // find index for primary key column
        let index = self.indexes.get(&pk_col.name);
        if index.is_none() {
            panic!("Primary key column has no index, cannot get all rows");
        }
        let index = index.unwrap();
        // get all entries iterator
        let iter = index.traverse_all_entries(|page_idx| {
            self.storage_manager.read_page(page_idx)
        })?;
        let iter = iter.map(move |pair_res| {
            let (match_page, match_offset) = pair_res?;
            // read the row from data page
            let data_page = self.storage_manager.read_page(match_page)?;
            let sizes = self.schema.get_sizes();
            let mut offset = match_offset as usize;
            let mut row = vec![];
            for size in sizes {
                let data_item = DataItem::from_bytes(&data_page.data[offset..offset+size], None)?;
                row.push(data_item);
                offset += size;
            };
            Ok(row)
        });
        Ok(iter)
    }
    pub fn get_indexed_col(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }
    pub fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}
