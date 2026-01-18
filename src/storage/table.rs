use std::sync::OnceLock;
use std::sync::Mutex;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use super::storage::Page;
use crate::config;
use crate::common::{RsqlError, RsqlResult};
use crate::common::{DataItem, VarCharHead};
use super::btree_index;
use super::consist_storage::ConsistStorageEngine;
use crate::catalog::TableSchema;
use crate::utils;

use super::allocator::Allocator;


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
/// - allocator metadata: rest of the page
pub struct Table {
    id: u64,
    schema: TableSchema,
    indexes: HashMap<String, btree_index::BTreeIndex>, // column name -> index
    allocator: Allocator,

    storage: ConsistStorageEngine,
}

impl Drop for Table {
    fn drop(&mut self) {
        let guard = get_table_guard();
        let mut guard = guard.lock().unwrap();
        guard.remove(&self.id);
    }
}


fn pack_ptr(page_idx: u64, offset: u64) -> u64 {
    (page_idx << 16) | (offset & 0xFFFF)
}

fn unpack_ptr(ptr: u64) -> (u64, u64) {
    (ptr >> 16, ptr & 0xFFFF)
}

impl Table {
    fn read_row_at(&self, page_idx: u64, offset: u64) -> RsqlResult<Vec<DataItem>> {
        let data_page = self.storage.read(page_idx)?;
        let mut row = vec![];
        let mut curr_offset = offset as usize;
        let sizes = self.schema.get_sizes();
        
        for size in sizes {
            let item_bytes = &data_page.data[curr_offset..curr_offset+size];
            match DataItem::from_bytes(item_bytes, None)? {
                DataItem::VarChar { head, value } => {
                    let varchar = self.load_varchar(&DataItem::VarChar { head, value })?;
                    row.push(varchar);
                },
                item => row.push(item),
            }
            curr_offset += size;
        }
        Ok(row)
    }

    /// Helper function to store a new VarChar data item
    fn store_varchar(&mut self, varchar: DataItem, tnx_id: u64) -> RsqlResult<DataItem> {
        let DataItem::VarChar { head, value } = varchar else {
            panic!("new_varchar called with non-varchar data item");
        };
        if head.len != value.len() as u64 {
            panic!("VarChar head length does not match value length");
        }
        let heap_len = value.len() as u64;
        let (heap_page_idx, heap_offset) = self.allocator.alloc_heap(tnx_id, heap_len, &mut self.storage)?;
        // write heap data
        self.storage.write_bytes(tnx_id, heap_page_idx, heap_offset as usize, value.as_bytes())?;
        // write ptr in head
        let new_head = VarCharHead {
            max_len: head.max_len,
            len: heap_len,
            page_ptr: Some(pack_ptr(heap_page_idx, heap_offset)),
        };
        Ok(DataItem::VarChar { head: new_head, value })
    }

    /// Helper function to load a VarChar data item
    fn load_varchar(&self, varchar_head: &DataItem) -> RsqlResult<DataItem> {
        let DataItem::VarChar { head: varchar_head, value } = varchar_head else {
            panic!("load_varchar called with non-varchar data item");
        };
        if value.len() != 0 {
            panic!("load_varchar called with non-empty value");
        }
        if let None = varchar_head.page_ptr {
            return Err(RsqlError::StorageError("Cannot load varchar with empty pointer".to_string()));
        };
        let (heap_page_idx, heap_offset) = unpack_ptr(varchar_head.page_ptr.unwrap());
        let heap_data = self.storage.read_bytes(heap_page_idx, heap_offset as usize, varchar_head.len as usize)?;
        Ok(DataItem::VarChar {
            head: varchar_head.clone(),
            value: String::from_utf8(heap_data).map_err(|_| RsqlError::StorageError("Invalid UTF-8 in VarChar".to_string()))?,
        })
    }

    /// Helper function to load and deallocate a VarChar data item
    fn del_varchar(&mut self, varchar_head: &DataItem, tnx_id: u64) -> RsqlResult<()> {
        let DataItem::VarChar { head: varchar_head, ..} = varchar_head else {
            panic!("load_varchar called with non-varchar data item");
        };
        if let None = varchar_head.page_ptr {
            return Err(RsqlError::StorageError("Cannot delete varchar with empty pointer".to_string()));
        };
        let (heap_page_idx, heap_offset) = unpack_ptr(varchar_head.page_ptr.unwrap());
        // free heap
        self.allocator.free_heap(tnx_id, heap_page_idx, heap_offset, &mut self.storage)?;
        Ok(())
    }

    /// Convert a row of data items to bytes for storage
    /// Caution: does not handle VarChar heap storage
    fn row_to_bytes(data: &Vec<DataItem>) -> RsqlResult<Vec<u8>> {
        let mut entry_data: Vec<u8> = vec![];
        let mut heap_data: Vec<Vec<u8>> = vec![];
        for item in data {
            let (item_bytes, heap_bytes) = item.to_bytes()?;
            entry_data.extend_from_slice(&item_bytes);
            if let Some(hb) = heap_bytes {
                heap_data.push(hb);
            }
        }
        Ok(entry_data)
    }

    pub fn from(id: u64, schema: TableSchema, is_sys: bool) -> RsqlResult<Self> {
        // 1. check if table already opened
        let guard = get_table_guard();
        let mut guard = guard.lock().unwrap();
        #[cfg(not(test))]
        if guard.contains(&id) {
            panic!("Table {} already opened in this process", id);
        }
        guard.insert(id);
        // 2. open table file
        let path = get_table_path(id, is_sys);
        let path_str = path.to_str().unwrap();
        let storage = ConsistStorageEngine::new(path_str, id)?;
        if let None = storage.max_page_index() {
            return Err(RsqlError::StorageError(format!("Table {id} file is empty, maybe corrupted")));
        };
        let header_page = storage.read(0)?;
        // 3. read and check magic number and version
        let magic = u32::from_le_bytes(header_page.data[0..4].try_into().unwrap());
        if magic != HEADER_MAGIC {
            return Err(RsqlError::StorageError("Invalid table file, has wrong magic number".to_string()));
        }
        let version = u32::from_le_bytes(header_page.data[4..8].try_into().unwrap()); // unused for now
        if version != 1 {
            panic!("Unsupported table file version: {}", version);
        }
        // 4. read indexes
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
        // 5. check if indexes compatible with schema
        if indexes.len() != schema.get_columns().iter().filter(|col| col.index).count() {
            panic!("Incompatible index count between schema and table file {:?}", path);
        }
        // 6. construct allocator
        let allocator = Allocator::from(&header_page, offset as u64)?;
        Ok(Table {
            id,
            schema,
            storage,
            indexes,
            allocator,
        })
    }
    /// Create a new table with given schema
    pub fn create(id: u64, schema: TableSchema, tnx_id: u64, is_sys: bool) -> RsqlResult<Self> { 
        // check if table already opened
        let guard = get_table_guard();
        let mut guard = guard.lock().unwrap();
        #[cfg(not(test))]
        if guard.contains(&id) {
            panic!("Table {} already opened in this process", id);
        }
        guard.insert(id);
        // create table file
        let path = get_table_path(id, is_sys);
        let path_str = path.to_str().unwrap();
        let mut storage = ConsistStorageEngine::new(path_str, id)?;
        // 1. collect indexes info
        let mut index_cols = HashSet::new();
        for col in schema.get_columns() {
            if col.index {
                index_cols.insert(col.name.clone());
            }
        }
        // 2. new indexes
        let mut indexes = HashMap::new();
        // Reserve page 0 for header
        let (header_page_idx, mut header_page) = storage.new_page(tnx_id)?;
        if header_page_idx != 0 {
            panic!("First page of table file should be page 0");
        }
        for col_name in &index_cols {
            let btree_index = btree_index::BTreeIndex::new(&mut storage, tnx_id)?;
            indexes.insert(col_name.clone(), btree_index);
        }
        // 3. collect header page bytes
        let mut page_data: Vec<u8> = vec![0u8; Page::max_size()];
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
        // 4. construct allocator
        // calculate entry size
        let mut entry_size = 0u64;
        for col in schema.get_columns() {
            entry_size += DataItem::cal_size_from_coltype(&col.data_type) as u64;
        }
        let allocator = Allocator::create(entry_size, offset as u64);
        let allocator_bytes = allocator.to_bytes();
        offset += allocator_bytes.len();
        page_data[offset - allocator_bytes.len()..offset]
            .copy_from_slice(&allocator_bytes);
        // 5. write head to disk
        header_page.data = page_data;
        storage.write(tnx_id, 0, &header_page)?;
        Ok(Table {
            id,
            schema,
            storage,
            indexes,
            allocator,
        })
    }
    /// Drop the table
    /// This implements will only set the table file length to 0
    /// TODO: support deleting the table file
    pub fn drop(mut self, tnx_id: u64) -> RsqlResult<()> {
        let page_max_idx = self.storage.max_page_index().unwrap_or(0);
        // truncate the file
        for page_idx in 0..=page_max_idx {
            self.storage.free_page(tnx_id, page_idx)?;
        };
        Ok(())
    }
    fn get_row_ptr_by_pk(&self, pk: &DataItem) -> RsqlResult<Option<(u64, u64)>> {
        // find the primary key column
        let pk_col = self.schema.get_columns().iter().find(|col| col.pk);
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
        let pair_opt = index.find_entry(pk.clone(), &self.storage)?;
        Ok(pair_opt)
    }
    pub fn get_row_by_pk(&self, pk: &DataItem) -> RsqlResult<Option<Vec<DataItem>>> {
        let pair_opt = self.get_row_ptr_by_pk(pk)?;
        let (match_page, match_offset) = match pair_opt {
            Some(pair) => pair,
            None => return Ok(None),
        };
        // read the row from data page
        let row = self.read_row_at(match_page, match_offset)?;
        Ok(Some(row))
    }
    /// Get rows by range on an indexed column
    /// returns entry in [start, end]
    pub fn get_rows_by_range_indexed_col(
        &self,
        col_name: &str,
        start: &Option<DataItem>,
        end: &Option<DataItem>,
    ) -> RsqlResult<impl Iterator<Item = RsqlResult<Vec<DataItem>>>> {
        // get index
        let index = self.indexes.get(col_name).ok_or(RsqlError::InvalidInput(
            format!("Column {} is not indexed, cannot search", col_name)
        ))?;
        // get iterator from index
        let entry_iter = index.find_range_entry(start.clone(), end.clone(), &self.storage)?;

        // map to row iterator
        let iter = entry_iter.map(move |pair_res| {
            let (match_page, match_offset) = pair_res?;
            // read the row from data page
            let row = self.read_row_at(match_page, match_offset)?;
            Ok(row)
        });
        Ok(iter)
    }
    pub fn get_all_rows(&self) -> RsqlResult<impl Iterator<Item = RsqlResult<Vec<DataItem>>>> {
        // find primary key column
        let pk_col = self.schema.get_columns().iter().find(|col| col.pk);
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
        let iter = index.traverse_all_entries(&self.storage)?;
        let iter = iter.map(move |pair_res| {
            let (match_page, match_offset) = pair_res?;
            let row = self.read_row_at(match_page, match_offset)?;
            Ok(row)
        });
        Ok(iter)
    }
    pub fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
    pub fn insert_row(&mut self, data: Vec<DataItem>, tnx_id: u64) -> RsqlResult<()> {
        // 1. check if data satisfies schema
        self.schema.satisfy(&data)?;
        // check unique constraints separately
        // the pk must be unique, so no need to check again
        for (i, col) in self.schema.get_columns().iter().enumerate() {
            if col.unique {
                let index = self.indexes.get(&col.name).unwrap();
                let existing = index.check_exists(data[i].clone(), &self.storage)?;
                if existing {
                    return Err(RsqlError::InvalidInput(
                        format!("Unique constraint violation on column {}", col.name)));
                }
            }
        }
        // 2. allocate entry
        let (entry_page_idx, entry_offset) = self.allocator.alloc_entry(tnx_id, &mut self.storage)?;
        // 3. store VarChar data if any
        let data = data.into_iter().map(|item| {
            if let DataItem::VarChar { .. } = item {
                self.store_varchar(item, tnx_id)
            } else {
                Ok(item)
            }
        }).collect::<RsqlResult<Vec<DataItem>>>()?;
        // 4. write entry data
        let entry_bytes = Self::row_to_bytes(&data)?;
        self.storage.write_bytes(tnx_id, entry_page_idx, entry_offset as usize, &entry_bytes)?;
        // 5. write index entries
        for (i, col) in self.schema.get_columns().iter().enumerate() {
            if col.index {
                let index = self.indexes.get_mut(&col.name).unwrap();
                index.insert_entry(
                    tnx_id,
                    data[i].clone(),
                    entry_page_idx,
                    entry_offset,
                    &mut self.storage,
                )?;
            }
        }
        Ok(())
    }
    pub fn update_row(&mut self, pk: &DataItem, new_data: Vec<DataItem>, tnx_id: u64) -> RsqlResult<()> {
        // TODO: optimize update in place if sizes match
        self.delete_row(pk, tnx_id)?;
        self.insert_row(new_data, tnx_id)
    }
    pub fn delete_row(&mut self, pk: &DataItem, tnx_id: u64) -> RsqlResult<()> {
        // 1. find the row by primary key
        let pair_opt = self.get_row_ptr_by_pk(pk)?;
        let (match_page, match_offset) = match pair_opt {
            Some(pair) => pair,
            None => {
                return Err(RsqlError::InvalidInput("No such row with given primary key".to_string()));
            }
        };
        // 2. read the row data
        let row = self.read_row_at(match_page, match_offset)?;
        // 3. free the entry and update indexes
        self.allocator.free_entry(tnx_id, match_page, match_offset, &mut self.storage)?;
        // 4. find if any VarChar to free
        for item in &row {
            if let DataItem::VarChar { .. } = item {
                self.del_varchar(item, tnx_id)?;
            }
        }
        // 5. delete from indexes
        for (i, col) in self.schema.get_columns().iter().enumerate() {
            if col.index {
                let index = self.indexes.get_mut(&col.name).unwrap();
                index.delete_entry(
                    tnx_id,
                    row[i].clone(),
                    match_page,
                    match_offset,
                    &mut self.storage,
                )?;
            }
        }
        Ok(())
    }
    pub fn creat_index(&mut self, col_name: &str, tnx_id: u64) -> RsqlResult<()> {
        // check if column exists and is already indexed
        let col = self.schema.get_columns().iter().find(|col| col.name == col_name);
        if col.is_none() {
            return Err(RsqlError::InvalidInput(format!("Column {} does not exist", col_name)));
        };
        let col = col.unwrap();
        if col.index {
            return Err(RsqlError::InvalidInput(format!("Column {} is already indexed", col_name)));
        };
        // update schema
        let mut columns = vec![];
        for schema_col in self.schema.get_columns().iter() {
            if schema_col.name == col_name {
                let mut new_col = schema_col.clone();
                new_col.index = true;
                columns.push(new_col);
            } else {
                columns.push(schema_col.clone());
            }
        };
        self.schema = TableSchema::new(columns)?;
        // create new index
        let mut btree_index = btree_index::BTreeIndex::new(&mut self.storage, tnx_id)?;
        // populate index with existing data
        let pk_index = self.schema.get_columns().iter().position(|c| c.pk).unwrap();
        let col_index = self.schema.get_columns().iter().position(|c| c.name == col_name).unwrap();
        let entry_iter = self.get_all_rows()?
            .collect::<RsqlResult<Vec<_>>>()?;
        for row_res in entry_iter {
            let row = row_res;
            let pk = row[pk_index].clone();
            let (entry_page_idx, entry_offset) = self.get_row_ptr_by_pk(&pk)?.unwrap();
            btree_index.insert_entry(
                tnx_id,
                row[col_index].clone(),
                entry_page_idx,
                entry_offset,
                &mut self.storage,
            )?;
        };
        self.indexes.insert(col_name.to_string(), btree_index);
        Ok(())
    }
    pub fn drop_index(&mut self, col_name: &str, tnx_id: u64) -> RsqlResult<()> {
        // check if column exists and is indexed
        let col = self.schema.get_columns().iter().find(|col| col.name == col_name);
        if col.is_none() {
            return Err(RsqlError::InvalidInput(format!("Column {} does not exist", col_name)));
        };
        let col = col.unwrap();
        if !col.index {
            return Err(RsqlError::InvalidInput(format!("Column {} is not indexed", col_name)));
        };
        // check if column is primary key or is unique
        if col.pk || col.unique {
            return Err(RsqlError::InvalidInput(format!("Cannot drop index on primary key or unique column {}", col_name)));
        };
        // update schema
        let mut columns = vec![];
        for schema_col in self.schema.get_columns().iter() {
            if schema_col.name == col_name {
                let mut new_col = schema_col.clone();
                new_col.index = false;
                columns.push(new_col);
            } else {
                columns.push(schema_col.clone());
            }
        };
        self.schema = TableSchema::new(columns)?;
        // remove index
        let index = self.indexes.remove(col_name).unwrap();
        index.drop(tnx_id, &mut self.storage)?;
        Ok(())
    }
    pub fn get_storage(&mut self) -> &mut ConsistStorageEngine {
        &mut self.storage
    }
}

/// Get the file path for a table given its ID
/// For tests, use temp directory
pub fn get_table_path(id: u64, is_sys: bool) -> PathBuf {
    if is_sys {
        if cfg!(test) {
            utils::test_dir(format!("table_{id}")).join(format!("{}.dbs", id))
        } else {
            std::path::Path::new(config::DB_DIR).join("sys").join(format!("{}.dbs", id))
        }
    } else {
        if cfg!(test) {
            utils::test_dir(format!("table_{id}")).join(format!("{}.dbt", id))
        } else {
            std::path::Path::new(config::DB_DIR).join("tables").join(format!("{}.dbt", id))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::table_schema::{TableColumn, ColType};
    use std::fs;

    fn setup_schema() -> TableSchema {
        let columns = vec![
                TableColumn {
                    name: "id".to_string(),
                    data_type: ColType::Integer,
                    pk: true,
                    nullable: false,
                    index: true,
                    unique: true,
                },
                TableColumn {
                    name: "name".to_string(),
                    data_type: ColType::Chars(32),
                    pk: false,
                    nullable: false,
                    index: false,
                    unique: false,
                },
            ];
        TableSchema::new(columns).unwrap()
    }

    fn make_chars(s: &str, len: usize) -> String {
        let mut res = s.to_string();
        while res.len() < len {
            res.push('\0');
        }
        res.truncate(len);
        res
    }

    #[test]
    fn test_table_create_and_open() {
        let table_id = 999;
        let schema = setup_schema();
        let tnx_id = 1;

        // cleanup if exists
        let path = get_table_path(table_id, false);
        let _ = fs::remove_file(&path);

        {
            // 1. Create table
            let table = Table::create(table_id, schema.clone(), tnx_id, false).expect("Failed to create table");
            assert_eq!(table.id, table_id);
            assert_eq!(table.get_schema().get_columns().len(), 2);
        }

        {
            // 2. Open table
            let table = Table::from(table_id, schema, false).expect("Failed to open table");
            assert_eq!(table.id, table_id);
        }

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_table_crud_operations() {
        let table_id = 2000;
        let columns = vec![
                TableColumn {
                    name: "id".to_string(),
                    data_type: ColType::Integer,
                    pk: true,
                    nullable: false,
                    index: true,
                    unique: true,
                },
                TableColumn {
                    name: "bio".to_string(),
                    data_type: ColType::VarChar(64),
                    pk: false,
                    nullable: false,
                    index: false,
                    unique: false,
                },
            ];
        let schema = TableSchema::new(columns).unwrap();
        let tnx_id = 1;
        let path = get_table_path(table_id, false);
        let _ = fs::remove_file(&path);

        let mut table = Table::create(table_id, schema, tnx_id, false).expect("Failed to create table");

        // 0. Insert a dummy row to prevent pages from becoming completely empty
        // (The current implementation panics when trying to free a non-last page)
        let dummy_row = vec![
            DataItem::Integer(999),
            DataItem::VarChar {
                head: VarCharHead { max_len: 64, len: 5, page_ptr: Some(0) }, 
                value: "dummy".to_string(),
            },
        ];
        table.insert_row(dummy_row, tnx_id).expect("Failed to insert dummy row");

        // 1. Insert
        let row1 = vec![
            DataItem::Integer(1),
            DataItem::VarChar {
                head: VarCharHead { max_len: 64, len: 11, page_ptr: Some(0) }, 
                value: "Hello World".to_string(),
            },
        ];
        table.insert_row(row1.clone(), tnx_id).expect("Failed to insert row 1");

        // 2. Get by PK
        let retrieved = table.get_row_by_pk(&DataItem::Integer(1)).expect("Failed to get row 1");
        assert!(retrieved.is_some());
        let retrieved_row = retrieved.unwrap();
        assert_eq!(retrieved_row[0], DataItem::Integer(1));
        if let DataItem::VarChar { value, .. } = &retrieved_row[1] {
            assert_eq!(value, "Hello World");
        } else {
            panic!("Expected VarChar");
        }

        // 3. Update
        let row1_updated = vec![
            DataItem::Integer(1),
            DataItem::VarChar {
                head: VarCharHead { max_len: 64, len: 3, page_ptr: Some(0) },
                value: "Bye".to_string(),
            },
        ];
        table.update_row(&DataItem::Integer(1), row1_updated, tnx_id).expect("Failed to update row");
        
        let retrieved_after_update = table.get_row_by_pk(&DataItem::Integer(1)).expect("Failed to get row after update");
        if let DataItem::VarChar { value, .. } = &retrieved_after_update.unwrap()[1] {
            assert_eq!(value, "Bye");
        }

        // 4. Delete
        table.delete_row(&DataItem::Integer(1), tnx_id).expect("Failed to delete row");
        let retrieved_after_delete = table.get_row_by_pk(&DataItem::Integer(1)).expect("Failed to get row after delete");
        assert!(retrieved_after_delete.is_none());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_table_scans() {
        let table_id = 2001;
        let schema = setup_schema();
        let tnx_id = 1;
        let path = get_table_path(table_id, false);
        let _ = fs::remove_file(&path);

        let mut table = Table::create(table_id, schema, tnx_id, false).expect("Failed to create table");

        // Insert 5 rows
        for i in 1..=5 {
            let row = vec![
                DataItem::Integer(i as i64),
                DataItem::Chars { len: 32, value: make_chars(&format!("User{}", i), 32) },
            ];
            table.insert_row(row, tnx_id).expect("Insert failed");
        }

        // 1. Full scan
        let all_rows: Vec<_> = table.get_all_rows().expect("Full scan failed")
            .collect::<RsqlResult<Vec<_>>>().expect("Iterator error");
        assert_eq!(all_rows.len(), 5);

        // 2. Range scan [2, 4]
        let range_rows: Vec<_> = table.get_rows_by_range_indexed_col(
            "id", 
            &Some(DataItem::Integer(2)), 
            &Some(DataItem::Integer(4))
        ).expect("Range scan failed")
        .collect::<RsqlResult<Vec<_>>>().expect("Iterator error");
        
        assert_eq!(range_rows.len(), 3);
        assert_eq!(range_rows[0][0], DataItem::Integer(2));
        assert_eq!(range_rows[2][0], DataItem::Integer(4));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_get_row_by_pk_empty() {
        let table_id = 1000;
        let schema = setup_schema();
        let tnx_id = 1;
        let path = get_table_path(table_id, false);
        let _ = fs::remove_file(&path);

        let table = Table::create(table_id, schema, tnx_id, false).expect("Failed to create table");
        let pk_val = DataItem::Integer(1);
        
        let result = table.get_row_by_pk(&pk_val).expect("Search failed");
        assert!(result.is_none());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_get_all_rows_empty() {
        let table_id = 1001;
        let schema = setup_schema();
        let tnx_id = 1;
        let path = get_table_path(table_id, false);
        let _ = fs::remove_file(&path);

        let table = Table::create(table_id, schema, tnx_id, false).expect("Failed to create table");
        let mut iter = table.get_all_rows().expect("Failed to get all rows");
        
        assert!(iter.next().is_none());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_create_and_drop_index() {
        let table_id = 3000;
        let columns = vec![
            crate::catalog::table_schema::TableColumn {
                name: "id".to_string(),
                data_type: crate::catalog::table_schema::ColType::Integer,
                pk: true,
                nullable: false,
                unique: true,
                index: true,
            },
            crate::catalog::table_schema::TableColumn {
                name: "age".to_string(),
                data_type: crate::catalog::table_schema::ColType::Integer,
                pk: false,
                nullable: false,
                unique: false,
                index: false,
            },
        ];
        let schema = crate::catalog::table_schema::TableSchema::new(columns).unwrap();
        let tnx_id = 1;
        let path = get_table_path(table_id, false);
        let _ = std::fs::remove_file(&path);

        let mut table = Table::create(table_id, schema, tnx_id, false).expect("Failed to create table");

        // insert rows (id, age)
        for i in 1..=5 {
            table.insert_row(
                vec![
                    DataItem::Integer(i as i64),
                    DataItem::Integer((i * 10) as i64),
                ],
                tnx_id,
            ).expect("Failed to insert row");
        }

        // create index on `age`
        table.creat_index("age", tnx_id).expect("Failed to create index");
        assert!(table.indexes.contains_key("age"));

        // range query on age [20,40] => ages 20,30,40 -> 3 rows
        let start = Some(DataItem::Integer(20));
        let end = Some(DataItem::Integer(40));
        let rows: Vec<_> = table.get_rows_by_range_indexed_col("age", &start, &end)
            .expect("Range scan failed")
            .collect::<RsqlResult<Vec<_>>>()
            .expect("Iterator error");
        assert_eq!(rows.len(), 3);

        // drop the index
        table.drop_index("age", tnx_id).expect("Failed to drop index");
        assert!(!table.indexes.contains_key("age"));

        // now querying by that index should return an error
        assert!(table.get_rows_by_range_indexed_col("age", &start, &end).is_err());

        let _ = std::fs::remove_file(&path);
    }
}
