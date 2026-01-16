use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Mutex;
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;

use super::storage;
use crate::config;
use crate::db::errors::{RsqlError, RsqlResult};
use crate::db::data_item::{DataItem, VarCharHead};
use super::wal;
use super::btree_index;
use crate::db::table_schema::{ColType, TableSchema};

mod allocator;
use allocator::Allocator;


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


fn pack_ptr(page_idx: u64, offset: u64) -> u64 {
    (page_idx << 16) | (offset & 0xFFFF)
}

fn unpack_ptr(ptr: u64) -> (u64, u64) {
    (ptr >> 16, ptr & 0xFFFF)
}

impl Table {
    fn read_row_at(&self, page_idx: u64, offset: u64) -> RsqlResult<Vec<DataItem>> {
        let data_page = self.storage_manager.read_page(page_idx)?;
        let mut row = vec![];
        let mut curr_offset = offset as usize;
        let sizes = self.schema.get_sizes();
        
        for size in sizes {
            let item_bytes = &data_page.data[curr_offset..curr_offset+size];
            match DataItem::from_bytes(item_bytes, None)? {
                DataItem::VarChar { head, .. } => {
                     // fetch body
                     let (heap_page, heap_offset) = unpack_ptr(head.page_ptr);
                     let heap_page_data = self.storage_manager.read_page(heap_page)?;
                     let body_bytes = &heap_page_data.data[heap_offset as usize..(heap_offset+head.len) as usize];
                     row.push(DataItem::from_bytes(item_bytes, Some(body_bytes))?);
                },
                item => row.push(item),
            }
            curr_offset += size;
        }
        Ok(row)
    }

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
        // constuct allocator
        let allocator = Allocator::from(&header_page, offset as u64)?;
        Ok(Table {
            id,
            schema,
            storage_manager,
            wal,
            indexes,
            allocator,
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

        // Reserve page 0 for header
        let (header_page_idx, mut header_page) = storage_manager_cell.borrow_mut().new_page()?;
        if header_page_idx != 0 {
            panic!("First page of table file should be page 0");
        }
        wal.new_page(tnx_id, id, header_page_idx, &vec![0u8; storage::Page::max_size()])?;

        for col_name in &index_cols {
            let btree_index = btree_index::BTreeIndex::new(
                || {
                    let (page_idx, page) = storage_manager_cell.borrow_mut().new_page()?;
                    wal.new_page(tnx_id, id, page_idx, &vec![0u8; storage::Page::max_size()])?;
                    Ok((page, page_idx))
                },
                | page_idx, page_data | {
                    // This is the initial write for a newly created index page.
                    // The new page is already logged in WAL via the `new_page` callback,
                    // so here we just persist the page to storage without another WAL entry.
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
        // 3. construct allocator
        // calculate entry size
        let mut entry_size = 0u64;
        for col in &schema.columns {
            entry_size += DataItem::cal_size_from_coltype(&col.data_type) as u64;
        }
        let allocator = Allocator::create(entry_size, offset as u64);
        let allocator_bytes = allocator.to_bytes();
        offset += allocator_bytes.len();
        page_data[offset - allocator_bytes.len()..offset]
            .copy_from_slice(&allocator_bytes);
        // 4. write wal first
        
        let zero_page = vec![0u8; storage::Page::max_size()];
        wal.update_page(tnx_id, id, 0, 0, &zero_page, &page_data)?;
        wal.flush()?;
        // 5. write head to disk
        header_page.data = page_data;
        storage_manager_cell.borrow_mut().write_page(&header_page, 0)?;
        Ok(Table {
            id,
            schema,
            storage_manager: storage_manager_cell.into_inner(),
            wal,
            indexes,
            allocator,
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
                &page.data)?;
        }
        self.wal.flush()?;
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
        let row = self.read_row_at(match_page, match_offset)?;
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
        let row = self.read_row_at(match_page, match_offset)?;
        Ok(Some(row))
    }
    /// Get rows by range on an indexed column
    /// returns entry in [start, end]
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
    pub fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
    pub fn insert_row(&mut self, data: Vec<DataItem>, tnx_id: u64) -> RsqlResult<()> {
        let storage_manager = RefCell::new(&mut self.storage_manager);
        // 1. check if data satisfies schema
        self.schema.satisfy(&data)?;
        // 2. write data to a new data page
        // allocate entry
        let (entry_page_idx, entry_offset) = self.allocator.alloc_entry(
            |page_idx, page_offset, data| {
                let mut storage_manager = storage_manager.borrow_mut();
                // get old data
                let mut page = storage_manager.read_page(page_idx)?;
                let old_data = page.data[page_offset as usize..(page_offset as usize + data.len())].to_vec();
                // write wal first
                self.wal.update_page(
                    tnx_id,
                    self.id,
                    page_idx,
                    page_offset,
                    &old_data,
                    &data,
                )?;
                // write to storage
                page.data[page_offset as usize..(page_offset as usize + data.len())]
                    .copy_from_slice(&data);
                storage_manager.write_page(&page, page_idx)
            },
            |page_idx| {
                let page = storage_manager.borrow_mut().read_page(page_idx)?;
                Ok(page)
            },
            || {
                let (page_idx, page) = storage_manager.borrow_mut().new_page()?;
                self.wal.new_page(tnx_id, self.id, page_idx, &vec![0u8; storage::Page::max_size()])?;
                Ok((page, page_idx))
            }
        )?;
        // write into entry
        let mut entry_data: Vec<u8> = vec![];
        let mut heap_data: HashMap<(u64, u64), Vec<u8>> = HashMap::new(); // (page_idx, offset) -> data
        for item in &data {
            if let DataItem::VarChar { head, value } = item {
                // alloc heap
                let heap_len = value.len() as u64;
                let (heap_page_idx, heap_offset) = self.allocator.alloc_heap(
                    heap_len,
                    || {
                        let (page_idx, page) = storage_manager.borrow_mut().new_page()?;
                        self.wal.new_page(tnx_id, self.id, page_idx, &vec![0u8; storage::Page::max_size()])?;
                        Ok((page, page_idx))
                    },
                    |page_idx, page_offset, data| {
                        // get old data
                        let mut page = storage_manager.borrow_mut().read_page(page_idx)?;
                        let old_data = page.data[page_offset as usize..(page_offset as usize + data.len())].to_vec();
                        // write wal first
                        self.wal.update_page(
                            tnx_id,
                            self.id,
                            page_idx,
                            page_offset,
                            &old_data,
                            &data,
                        )?;
                        // write to storage
                        page.data[page_offset as usize..(page_offset as usize + data.len())]
                            .copy_from_slice(&data);
                        storage_manager.borrow_mut().write_page(&page, page_idx)
                    },
                    |page_idx| {
                        let page = storage_manager.borrow_mut().read_page(page_idx)?;
                        Ok(page)
                    }
                )?;
                // construct HEAD with packed ptr
                let new_head = VarCharHead {
                    max_len: head.max_len,
                    len: heap_len,
                    page_ptr: pack_ptr(heap_page_idx, heap_offset),
                };
                let new_item = DataItem::VarChar { head: new_head, value: value.clone() };
                let (item_bytes, _) = new_item.to_bytes()?; 
                entry_data.extend_from_slice(&item_bytes);
                heap_data.insert((heap_page_idx, heap_offset), value.as_bytes().to_vec());
            } else {
                let (item_bytes, _) = item.to_bytes()?;
                entry_data.extend_from_slice(&item_bytes);
            }
        }
        // write wal first

        self.wal.update_page(
            tnx_id,
            self.id,
            entry_page_idx,
            entry_offset,
            &storage_manager.borrow_mut().read_page(entry_page_idx)?
                .data[entry_offset as usize..entry_offset as usize + entry_data.len()]
                .to_vec(),
            &entry_data,
        )?;
        for heap_entry in &heap_data {
            let (page_idx, offset) = heap_entry.0;
            let data = heap_entry.1;
            self.wal.update_page(
                tnx_id,
                self.id,
                *page_idx,
                *offset,
                &storage_manager.borrow_mut().read_page(*page_idx)?
                    .data[*offset as usize..*offset as usize + data.len()]
                    .to_vec(),
                data,
            )?;
        };
        self.wal.flush()?;
        // write entry data to storage
        let mut entry_page = storage_manager.borrow_mut().read_page(entry_page_idx)?;
        let entry_length = entry_data.len();
        entry_page.data[entry_offset as usize..entry_offset as usize + entry_length]
            .copy_from_slice(&entry_data);
        storage_manager.borrow_mut().write_page(&entry_page, entry_page_idx)?;
        // write heap data to storage
        for heap_entry in &heap_data {
            let (page_idx, offset) = heap_entry.0;
            let data = heap_entry.1;
            let mut heap_page = storage_manager.borrow_mut().read_page(*page_idx)?;
            heap_page.data[*offset as usize..*offset as usize + data.len()]
                .copy_from_slice(data);
            storage_manager.borrow_mut().write_page(&heap_page, *page_idx)?;
        };
        // write index entries
        for (i, col) in self.schema.columns.iter().enumerate() {
            if col.index {
                let index = self.indexes.get_mut(&col.name).unwrap();
                index.insert_entry(
                    data[i].clone(),
                    entry_page_idx,
                    entry_offset,
                    & |page_idx| {
                        storage_manager.borrow_mut().read_page(page_idx)
                    },
                    & |page_idx, page_data: &storage::Page| {
                        // get old data
                        let old_page = storage_manager.borrow_mut().read_page(page_idx)?;
                        self.wal.update_page(
                            tnx_id,
                            self.id,
                            page_idx,
                            0,
                            &old_page.data,
                            &page_data.data,
                        )?;
                        storage_manager.borrow_mut().write_page(&page_data, page_idx)
                    },
                    & || {
                        let (page_idx, page) = storage_manager.borrow_mut().new_page()?;
                        self.wal.new_page(tnx_id, self.id, page_idx, &vec![0u8; storage::Page::max_size()])?;
                        Ok((page, page_idx))
                    }
                )?;
            }
        };
        Ok(())
    }
    pub fn update_row(&mut self, pk: &DataItem, new_data: Vec<DataItem>, tnx_id: u64) -> RsqlResult<()> {
        self.delete_row(pk, tnx_id)?;
        self.insert_row(new_data, tnx_id)
    }
    pub fn delete_row(&mut self, pk: &DataItem, tnx_id: u64) -> RsqlResult<()> {
        let storage_manager = RefCell::new(&mut self.storage_manager);
        // 1. find the primary key
        let pk_col = self.schema.columns.iter().find(|col| col.pk);
        if pk_col.is_none() {
            return Err(RsqlError::InvalidInput("Table has no primary key".to_string()));
        };
        let pk_index = self.indexes.get(&pk_col.unwrap().name);
        if pk_index.is_none() {
            return Err(RsqlError::InvalidInput("Primary key column has no index".to_string()));
        };
        let pk_index = pk_index.unwrap();
        // 2. find the row page, offset by pk
        let pair_opt = pk_index.find_entry(pk.clone(), &|page_idx| {
            storage_manager.borrow().read_page(page_idx)
        })?;
        let (match_page, match_offset) = match pair_opt {
            Some(pair) => pair,
            None => return Err(RsqlError::InvalidInput("No such primary key found".to_string())),
        };
        // 3. read the old data
        let has_heap = self.schema.columns.iter().any(|col| {
            matches!(col.data_type, ColType::VarChar(_))
        });
        let data_page = storage_manager.borrow().read_page(match_page)?;
        let sizes = self.schema.get_sizes();
        let old_entry_data = data_page.data[match_offset as usize..match_offset as usize + sizes.iter().sum::<usize>()].to_vec();

        // 4. [FIX] Remove from all indexes FIRST (to ensure index consistency)
        // Parse old data to get values for index deletion
        let mut old_values = Vec::new();
        let mut offset = 0;
        for size in &sizes {
            let item_bytes = &old_entry_data[offset..offset+size];
            let item = DataItem::from_bytes(item_bytes, None)?;
            old_values.push(item);
            offset += size;
        }

        for (i, col) in self.schema.columns.iter().enumerate() {
            if col.index {
                let index = self.indexes.get_mut(&col.name).unwrap();
                index.delete_entry(
                    old_values[i].clone(),
                    match_offset as u64,
                     &|page_idx| {
                        storage_manager.borrow_mut().read_page(page_idx)
                    },
                    &|page_idx, page| {
                         // get old data (for WAL)
                         // Note: BTreeIndex update might involve reading old page for WAL inside it? 
                         // Assuming we need to handle WAL here for the index page update:
                        let old_page = storage_manager.borrow_mut().read_page(page_idx)?;
                        self.wal.update_page(
                            tnx_id,
                            self.id,
                            page_idx,
                            0,
                            &old_page.data,
                            &page.data,
                        )?;
                        storage_manager.borrow_mut().write_page(page, page_idx)
                    }
                )?;
            }
        }

        // 5. Free entry
        self.allocator.free_entry(
            match_page, 
            match_offset, 
            |page_idx, page_offset, data| {
                let mut storage_manager = storage_manager.borrow_mut();
                // get old data
                let mut page = storage_manager.read_page(page_idx)?;
                let old_data = page.data[page_offset as usize..(page_offset as usize + data.len())].to_vec();
                // write wal first
                self.wal.update_page(
                    tnx_id,
                    self.id,
                    page_idx,
                    page_offset,
                    &old_data,
                    &data,
                )?;
                // write to storage
                page.data[page_offset as usize..(page_offset as usize + data.len())]
                    .copy_from_slice(&data);
                storage_manager.write_page(&page, page_idx)
            }, 
            |page_idx| {
                let page = storage_manager.borrow_mut().read_page(page_idx)?;
                Ok(page)
            }, 
            |page_idx| {
                // free
                // get old data
                let page = storage_manager.borrow_mut().read_page(page_idx)?;
                self.wal.delete_page(
                    tnx_id,
                    self.id,
                    page_idx,
                    &page.data,
                )?;
                let free_page = storage_manager.borrow_mut().free()?;
                assert_eq!(page_idx, free_page);
                Ok(())
            }
        )?;
        if !has_heap {
            return Ok(()); 
        };
        // 6. free heap data if any
        // find heap ptrs from old entry data
        for col in &self.schema.columns {
            let size = DataItem::cal_size_from_coltype(&col.data_type);
            let item_bytes = &old_entry_data[0..size];
            let data_item = DataItem::from_bytes(item_bytes, None)?;
            if let DataItem::VarChar { head, .. } = data_item {
                let (heap_page, heap_offset) = unpack_ptr(head.page_ptr);
                // free heap
                self.allocator.free_heap(
                    heap_page,
                    heap_offset,
                    |page_idx, page_offset, data| {
                        let mut storage_manager = storage_manager.borrow_mut();
                        // get old data
                        let mut page = storage_manager.read_page(page_idx)?;
                        let old_data = page.data[page_offset as usize..(page_offset as usize + data.len())].to_vec();
                        // write wal first
                        self.wal.update_page(
                            tnx_id,
                            self.id,
                            page_idx,
                            page_offset,
                            &old_data,
                            &data,
                        )?;
                        // write to storage
                        page.data[page_offset as usize..(page_offset as usize + data.len())]
                            .copy_from_slice(&data);
                        storage_manager.write_page(&page, page_idx)
                    },
                    |page_idx| {
                        let page = storage_manager.borrow_mut().read_page(page_idx)?;
                        Ok(page)
                    },
                )?;
            }
        };
        Ok(())
    }
}
