use super::super::storage;
use super::heap_utils::{HeapChunk, HeapPage};
use super::entry_utils::{Bitmap, EntryPage};
use crate::common::RsqlResult;
use crate::storage::consist_storage::ConsistStorageEngine;

/// Help stuct for page management and allocation
/// This allocator implement a disk page allocater.
/// It supports fixed size entry allocation and variable size heap allocation.
pub struct Allocator {
    begin_with: u64, // metadata begin with byte offset in page 0
    // entry pages: for fixed size entries
    entry_size: u64,
    entries_per_page: u64,
    first_free_entry_page: u64, // first free entry is stored in bitmap in this page header
    // heap pages: for variable size items, such as varchar
    // the free space is managed by linked list of free chunks
    first_free_heap_page: u64, // ptr to first heap chunk has free space
}

/// Serilization and deserialization util functions
/// Byte layout:
/// [entry_size: 8bytes][entries_per_page: 8bytes][first_free_entry_page: 8bytes][first_free_heap_page: 8bytes]
impl Allocator {
    fn _to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend_from_slice(&self.entry_size.to_le_bytes());
        buf.extend_from_slice(&self.entries_per_page.to_le_bytes());
        buf.extend_from_slice(&self.first_free_entry_page.to_le_bytes());
        buf.extend_from_slice(&self.first_free_heap_page.to_le_bytes());
        buf
    }
    fn from_bytes(bytes: &[u8], begin_with: u64) -> RsqlResult<Self> {
        let mut offset = begin_with as usize;
        let entry_size_bytes = &bytes[offset..offset+8];
        let entry_size = u64::from_le_bytes(entry_size_bytes.try_into().unwrap());
        offset += 8;
        let entries_per_page_bytes = &bytes[offset..offset+8];
        let entries_per_page = u64::from_le_bytes(entries_per_page_bytes.try_into().unwrap());
        offset += 8;
        let first_free_entry_page_bytes = &bytes[offset..offset+8];
        let first_free_entry_page = u64::from_le_bytes(first_free_entry_page_bytes.try_into().unwrap());
        offset += 8;
        let first_free_heap_page_bytes = &bytes[offset..offset+8];
        let first_free_heap_page = u64::from_le_bytes(first_free_heap_page_bytes.try_into().unwrap());
        Ok(Allocator {
            begin_with,
            entry_size,
            entries_per_page,
            first_free_entry_page,
            first_free_heap_page,
        })
    }
    fn set_first_free_entry_page(
        &mut self, 
        free_entry_page: u64,
        storage: &mut ConsistStorageEngine,
        tnx_id: u64,
    ) -> RsqlResult<()> {
        self.first_free_entry_page = free_entry_page;
        // write to page 0
        let offset = self.begin_with + 8 + 8;
        let bytes = free_entry_page.to_le_bytes();
        storage.write_bytes(tnx_id, 0, offset as usize, &bytes)
    }
    fn set_first_free_heap_page(
        &mut self, 
        free_heap_page: u64,
        storage: &mut ConsistStorageEngine,
        tnx_id: u64,
    ) -> RsqlResult<()> {
        self.first_free_heap_page = free_heap_page;
        // write to page 0
        let offset = self.begin_with + 8 + 8 + 8;
        let bytes = free_heap_page.to_le_bytes();
        storage.write_bytes(tnx_id, 0, offset as usize, &bytes)
    }
}

impl Allocator {
    pub fn create(entry_size: u64, begin_with: u64) -> Self {
        // 8 + 8 + bitmap:((entry_num * 1 + 7) / 8) + entry_num * entry_size <= page_size
        let mut entry_num = 0;
        let mut left = 16 + (entry_num + 7) / 8 + entry_num * entry_size;
        while left < storage::Page::max_size() as u64 {
            entry_num += 1;
            left = 16 + (entry_num + 7) / 8 + entry_num * entry_size;
        }
        let entries_per_page = entry_num - 1;
        Allocator {
            begin_with,
            entry_size,
            entries_per_page,
            first_free_entry_page: 0, // 0 means no free entry page (page 0 is never used for entry pages)
            first_free_heap_page: 0, // 0 means no free heap chunk
        }
    }
    pub fn reset_begin_with(&mut self, begin_with: u64) {
        self.begin_with = begin_with;
    }
    /// Serialize allocator metadata to bytes
    /// [entry_size: 8bytes][entries_per_page: 8bytes][first_free_entry_page: 8bytes][first_free_heap_page: 8bytes]
    pub fn to_bytes(&self) -> Vec<u8> {
        self._to_bytes()
    }
    pub fn from(page: &storage::Page, begin_with: u64) -> RsqlResult<Self> {
        let bytes = &page.data;
        Self::from_bytes(bytes, begin_with)
    }
    /// find the tail of entry page linked list
    /// 0 indicates no entry pages
    fn entry_page_list_tail(
        &self,
        storage: &ConsistStorageEngine,
    ) -> RsqlResult<u64> {
        let mut current_page = self.first_free_entry_page;
        let mut prev_page = 0;
        while current_page != 0 {
            let page = storage.read(current_page)?;
            let next_free_page = EntryPage::next_free_page(&page);
            if next_free_page == 0 {
                return Ok(current_page);
            }
            prev_page = current_page;
            current_page = next_free_page;
        }
        Ok(prev_page)
    }
    /// allocate and initialize a new entry page
    /// The new entry page will always in the tail of the linked list
    fn new_entry_page(
        &mut self,
        tnx_id: u64,
        storage: &mut ConsistStorageEngine,
    ) -> RsqlResult<u64> {
        let (page_idx, _) = storage.new_page(tnx_id)?;
        // find previous tail
        let tail = match self.entry_page_list_tail(storage)?{
            0 => None,
            n => Some(n),
        };
        // initialize page header
        let page = EntryPage::new_page(self.entries_per_page, None, tail);
        // update previous next pointer
        let previous_page = self.entry_page_list_tail(storage)?;
        if previous_page == 0 {
            // first entry page
            self.set_first_free_entry_page(page_idx, storage, tnx_id)?;
        } else {
            let mut prev_page_data = storage.read(previous_page)?;
            EntryPage::set_next_free_page(&mut prev_page_data, page_idx);
            storage.write(tnx_id, previous_page, &prev_page_data)?;
        }
        // write new page to disk
        storage.write(tnx_id, page_idx, &page)?;
        Ok(page_idx)
    }
    /// Free an entry page
    fn del_entry_page(
        &mut self,
        tnx_id: u64,
        page_idx: u64,
        storage: &mut ConsistStorageEngine,
    ) -> RsqlResult<()> {
        // read page
        let page = storage.read(page_idx)?;
        let next_free_page = EntryPage::next_free_page(&page);
        let prev_free_page = EntryPage::prev_free_page(&page);
        // update previous next
        if prev_free_page == 0 {
            self.first_free_entry_page = next_free_page;
        } else {
            let mut prev_page_data = storage.read(prev_free_page)?;
            EntryPage::set_next_free_page(&mut prev_page_data, next_free_page);
            storage.write(tnx_id, prev_free_page, &prev_page_data)?;
        }
        // update next prev
        if next_free_page != 0 {
            let mut next_page_data = storage.read(next_free_page)?;
            EntryPage::set_prev_free_page(&mut next_page_data, prev_free_page);
            storage.write(tnx_id, next_free_page, &next_page_data)?;
        }
        // free the page
        let max_page = match storage.max_page_index() {
            None => -1 as i64,
            Some(n) => n as i64,
        };
        if page_idx as i64 == max_page {
            storage.free_page(tnx_id, page_idx)?;
        }
        Ok(())
    }
    /// Allocate an empty entry
    /// Return: (page_idx, page_offset)
    pub fn alloc_entry(
        &mut self,
        tnx_id: u64,
        storage: &mut ConsistStorageEngine,
    ) -> RsqlResult<(u64, u64)> {
        // 1. find a page with free entry
        // Traverse the linked list of entry pages to find one with an empty slot
        // because the linked list only contains pages with free slots, we can stop at the first one
        let mut free_page = self.first_free_entry_page;
        if free_page == 0 {
            free_page = self.new_entry_page(tnx_id, storage)?;
        }
        // double check the page has free entry
        let mut page = storage.read(free_page)?;
        let mut bitmap = EntryPage::bitmap(&page, self.entries_per_page).to_vec();
        assert!(!Bitmap::is_full(&bitmap, self.entries_per_page as usize), 
                "Entry page {} should have free entries", free_page
        );
        // 2. find free entry in the page
        let entries_per_page = self.entries_per_page as usize;
        let entry_index = Bitmap::find_empty_bit(&bitmap, entries_per_page).unwrap();

        // 3. mark entry as used
        Bitmap::set_bit_true(&mut bitmap, entry_index as usize);
        EntryPage::set_bitmap(&mut page, entries_per_page as u64, &bitmap);

        // 4. if page is full now, remove it from free list
        if Bitmap::is_full(&bitmap, entries_per_page) {
            let next_free_page = EntryPage::next_free_page(&page);
            let prev_free_page = EntryPage::prev_free_page(&page);
            // update previous next
            if prev_free_page == 0 {
                self.set_first_free_entry_page(next_free_page, storage, tnx_id)?;
            } else {
                let mut prev_page_data = storage.read(prev_free_page)?;
                EntryPage::set_next_free_page(&mut prev_page_data, next_free_page);
                storage.write(tnx_id, prev_free_page, &prev_page_data)?;
            }
            // update next prev
            if next_free_page != 0 {
                let mut next_page_data = storage.read(next_free_page)?;
                EntryPage::set_prev_free_page(&mut next_page_data, prev_free_page);
                storage.write(tnx_id, next_free_page, &next_page_data)?;
            }
            // update this page pointer to 0
            EntryPage::set_next_free_page(&mut page, 0);
            EntryPage::set_prev_free_page(&mut page, 0);
        }
        storage.write(tnx_id, free_page, &page)?;

        // 5. calculate entry offset
        let entry_offset = EntryPage::entries_offset(entry_index as u64, self.entry_size, self.entries_per_page);
        Ok((free_page, entry_offset))
    }
    /// Free an entry
    pub fn free_entry(
        &mut self,
        tnx_id: u64,
        page_idx: u64,
        entry_offset: u64,
        storage: &mut ConsistStorageEngine,
    ) -> RsqlResult<()> {
        // 1. read page
        let mut page = storage.read(page_idx)?;
        let entry_index = EntryPage::entries_index(entry_offset, self.entry_size, self.entries_per_page);
        // 2. mark entry as free
        let mut bitmap = EntryPage::bitmap(&page, self.entries_per_page).to_vec();
        Bitmap::set_bit_false(&mut bitmap, entry_index as usize);
        EntryPage::set_bitmap(&mut page, self.entries_per_page, &bitmap);
        storage.write(tnx_id, page_idx, &page)?;
        // 3. check if the page is now completely free
        let all_free = Bitmap::is_all_empty(&bitmap, self.entries_per_page as usize);
        if all_free {
            self.del_entry_page(tnx_id, page_idx, storage)?;
        }
        Ok(())
    }
    fn heap_page_list_tail(
        &self,
        storage: &ConsistStorageEngine,
    ) -> RsqlResult<u64> {
        let mut current_page = self.first_free_heap_page;
        let mut prev_page = 0;
        while current_page != 0 {
            let page = storage.read(current_page)?;
            let next_free_chunk = HeapPage::next_free_page(&page);
            if next_free_chunk == 0 {
                return Ok(current_page);
            }
            prev_page = current_page;
            current_page = next_free_chunk;
        }
        Ok(prev_page)
    }
    /// Allocate and initialize a new heap page
    fn new_heap_page (
        &mut self,
        tnx_id: u64,
        storage: &mut ConsistStorageEngine,
    ) -> RsqlResult<u64> {
        let (page_idx, _) = storage.new_page(tnx_id)?;
        // find previous tail
        let prev_page = self.heap_page_list_tail(storage)?;
        let prev_page_opt = if prev_page == 0 {
            None
        } else {
            Some(prev_page)
        };
        // initialize page
        let page = HeapPage::new_free_page(prev_page_opt, None);
        // update previous page's next pointer
        if prev_page == 0 {
            // first heap page
            self.set_first_free_heap_page(page_idx, storage, tnx_id)?;
        } else {
            let mut prev_page_data = storage.read(prev_page)?;
            HeapPage::set_next_free_page(&mut prev_page_data, page_idx);
            storage.write(tnx_id, prev_page, &prev_page_data)?;
        }
        // write new page to disk
        storage.write(tnx_id, page_idx, &page)?;
        Ok(page_idx)
    }
    /// Deallocate a heap page NOT CHUNK
    fn del_heap_page(
        &mut self,
        tnx_id: u64,
        page_idx: u64,
        storage: &mut ConsistStorageEngine,
    ) -> RsqlResult<()> {
        let page = storage.read(page_idx)?;
        // 1. check if the page is empty
        if !HeapPage::check_page_empty(&page) {
            panic!("Trying to delete a non-empty heap page {}", page_idx);
        };
        // 2. update prev next pointer to this next pointer
        let prev_free_page = HeapPage::prev_free_page(&page);
        let next_free_page = HeapPage::next_free_page(&page);
        if prev_free_page == 0 {
            self.set_first_free_heap_page(next_free_page, storage, tnx_id)?;
        } else {
            let mut prev_page_data = storage.read(prev_free_page)?;
            HeapPage::set_next_free_page(&mut prev_page_data, next_free_page);
            storage.write(tnx_id, prev_free_page, &prev_page_data)?;
        }
        // 3. update next prev pointer to this prev pointer
        if next_free_page != 0 {
            let mut next_page_data = storage.read(next_free_page)?;
            HeapPage::set_prev_free_page(&mut next_page_data, prev_free_page);
            storage.write(tnx_id, next_free_page, &next_page_data)?;
        }
        // 4. free the page
        storage.free_page(tnx_id, page_idx)?;
        Ok(())
    }
    
    /// Allocate heap space of given size
    /// Return: (page_idx, offset)
    pub fn alloc_heap(
        &mut self, 
        tnx_id: u64,
        size:u64,
        storage: &mut ConsistStorageEngine,
    ) -> RsqlResult<(u64, u64)> {
        let mut current_page;
        if self.first_free_heap_page == 0 {
            current_page = self.new_heap_page(tnx_id, storage)?;
        } else {
            current_page = self.first_free_heap_page;
        };
        // traverse heap pages and chunks to find **first-fit**
        'found: 
        loop { // traverse pages
            let mut page = storage.read(current_page)?;
            let mut current_chunk = HeapPage::first_free_chunk_offset(&page);
            while current_chunk != 0 { // traverse chunks
                let chunk_size = HeapChunk::chunk_size(&page, current_chunk);
                if chunk_size >= size {
                    // found a suitable chunk
                    let ptr = HeapChunk::alloc_chunk(&mut page, current_chunk, size)?;
                    storage.write(tnx_id, current_page, &page)?;
                    break 'found Ok((current_page, ptr))
                }
                // move to next chunk
                current_chunk = HeapChunk::next_free_chunk(&page, current_chunk);
            }
            // move to next page
            let next_page = HeapPage::next_free_page(&page);
            if next_page == 0 {
                // no more pages, create a new one
                current_page = self.new_heap_page(tnx_id, storage)?;
            } else {
                current_page = next_page;
            }
        }
    }
    /// Free a heap chunk
    pub fn free_heap(
        &mut self,
        tnx_id: u64,
        page_idx: u64,
        offset: u64,
        storage: &mut ConsistStorageEngine,
    ) -> RsqlResult<()> {
        let mut page = storage.read(page_idx)?;
        // dealloc chunk
        HeapChunk::dealloc_chunk(&mut page, offset)?;
        storage.write(tnx_id, page_idx, &page)?;
        // check if the page is now empty
        let page = storage.read(page_idx)?;
        if HeapPage::check_page_empty(&page) {
            // delete the page
            self.del_heap_page(tnx_id, page_idx, storage)?;
        }
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::sync::Mutex;
    use tempfile::tempdir;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn setup_storage(db_path: &str) -> ConsistStorageEngine {
        if Path::new(db_path).exists() { fs::remove_file(db_path).unwrap(); }
        ConsistStorageEngine::new(db_path, 1).unwrap()
    }

    fn cleanup(db_path: &str) {
        if Path::new(db_path).exists() { fs::remove_file(db_path).unwrap(); }
    }

    #[test]
    fn test_bitmap() {
        let num_bits = 20;
        let mut bytes = Bitmap::empty_bitmap(num_bits);
        assert_eq!(bytes.len(), 3);
        assert!(!Bitmap::is_full(&bytes, num_bits));
        assert!(Bitmap::is_all_empty(&bytes, num_bits));

        for i in 0..num_bits {
            assert_eq!(Bitmap::find_empty_bit(&bytes, num_bits), Some(i));
            Bitmap::set_bit_true(&mut bytes, i);
        }
        assert!(Bitmap::is_full(&bytes, num_bits));
        assert!(!Bitmap::is_all_empty(&bytes, num_bits));
        assert_eq!(Bitmap::find_empty_bit(&bytes, num_bits), None);

        Bitmap::set_bit_false(&mut bytes, 5);
        assert_eq!(Bitmap::find_empty_bit(&bytes, num_bits), Some(5));
        assert!(!Bitmap::is_full(&bytes, num_bits));
    }

    #[test]
    fn test_entry_alloc_free() {
        let _guard = TEST_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_entry.db");
        let db_path_str = db_path.to_str().unwrap();
        let mut storage = setup_storage(db_path_str);
        let tnx_id = 1;

        // Create page 0 for allocator metadata
        storage.new_page(tnx_id).unwrap();

        let mut allocator = Allocator::create(100, 0); // entry_size=100, begin_with=0 in page 0
        
        // alloc entries
        let mut entries = Vec::new();
        for _ in 0..10 {
            let (page_idx, offset) = allocator.alloc_entry(tnx_id, &mut storage).unwrap();
            entries.push((page_idx, offset));
        }

        // check uniqueness
        let mut seen = std::collections::HashSet::new();
        for e in &entries {
            assert!(seen.insert(e.clone()));
        }

        // free entries
        for (page_idx, offset) in entries {
            allocator.free_entry(tnx_id, page_idx, offset, &mut storage).unwrap();
        }

        cleanup(db_path_str);
    }

    #[test]
    fn test_heap_alloc_free() {
        let _guard = TEST_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_heap.db");
        let db_path_str = db_path.to_str().unwrap();
        let mut storage = setup_storage(db_path_str);
        let tnx_id = 1;

        storage.new_page(tnx_id).unwrap();
        let mut allocator = Allocator::create(100, 0);

        // alloc heap space
        let (p1, o1) = allocator.alloc_heap(tnx_id, 500, &mut storage).unwrap();
        let (p2, o2) = allocator.alloc_heap(tnx_id, 1000, &mut storage).unwrap();
        
        // write some data
        let data1 = vec![1u8; 500];
        storage.write_bytes(tnx_id, p1, o1 as usize, &data1).unwrap();

        // free
        allocator.free_heap(tnx_id, p1, o1, &mut storage).unwrap();
        allocator.free_heap(tnx_id, p2, o2, &mut storage).unwrap();

        cleanup(db_path_str);
    }

    #[test]
    fn test_heap_merge() {
        let _guard = TEST_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_merge.db");
        let db_path_str = db_path.to_str().unwrap();
        let mut storage = setup_storage(db_path_str);
        let tnx_id = 1;

        storage.new_page(tnx_id).unwrap();
        let mut allocator = Allocator::create(100, 0);

        // alloc 3 contiguous chunks
        let (p1, o1) = allocator.alloc_heap(tnx_id, 100, &mut storage).unwrap();
        let (p2, o2) = allocator.alloc_heap(tnx_id, 100, &mut storage).unwrap();
        let (p3, o3) = allocator.alloc_heap(tnx_id, 100, &mut storage).unwrap();

        assert_eq!(p1, p2);
        assert_eq!(p2, p3);

        // free middle one
        allocator.free_heap(tnx_id, p2, o2, &mut storage).unwrap();

        // free first one (should merge with middle)
        allocator.free_heap(tnx_id, p1, o1, &mut storage).unwrap();

        // free last one (should merge with previous merged chunk)
        allocator.free_heap(tnx_id, p3, o3, &mut storage).unwrap();
        
        // After all freed, the page should be deleted
        // try to read page will failed
        match storage.read(p1) {
            Ok(p) => {
                assert!(false, "Page data after all frees: {:?}", p.data);
            },
            Err(_) => {},
        };

        cleanup(db_path_str);
    }
}
