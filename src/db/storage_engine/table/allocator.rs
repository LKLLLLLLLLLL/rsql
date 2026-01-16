use super::storage;
use crate::db::errors::RsqlResult;
use crate::db::utils;

const MAGIC_NUMBER: u32 = 0x4c515352; // 'RSQL' in little endian hex

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

impl Allocator {
    pub fn create(entry_size: u64, begin_with: u64) -> Self {
        let entries_per_page = storage::Page::max_size() as u64 / entry_size;
        Allocator {
            begin_with,
            entry_size,
            entries_per_page,
            first_free_entry_page: 0, // 0 means no free entry page (page 0 is never used for entry pages)
            first_free_heap_page: 0, // 0 means no free heap chunk
        }
    }
    /// Serialize allocator metadata to bytes
    /// [entry_size: 8bytes][entries_per_page: 8bytes][first_free_entry_page: 8bytes][first_free_heap_page: 8bytes]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend_from_slice(&self.entry_size.to_le_bytes());
        buf.extend_from_slice(&self.entries_per_page.to_le_bytes());
        buf.extend_from_slice(&self.first_free_entry_page.to_le_bytes());
        buf.extend_from_slice(&self.first_free_heap_page.to_le_bytes());
        buf
    }
    pub fn from(page: &storage::Page, begin_with: u64) -> RsqlResult<Self> {
        let bytes = &page.data;
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
    /// find the tail of entry page linked list
    /// 0 indicates no entry pages
    /// Args:
    /// - read(page_idx) -> page
    fn entry_page_list_tail(
        &self,
        read: &impl Fn(u64) -> RsqlResult<storage::Page>,
    ) -> RsqlResult<u64> {
        let mut current_page = self.first_free_entry_page;
        let mut prev_page = 0;
        while current_page != 0 {
            let page = read(current_page)?;
            let next_free_chunk_bytes = &page.data[8..16];
            let next_free_chunk = u64::from_le_bytes(next_free_chunk_bytes.try_into().unwrap());
            if next_free_chunk == 0 {
                return Ok(current_page);
            }
            prev_page = current_page;
            current_page = next_free_chunk;
        }
        Ok(prev_page)
    }
    /// allocate and initialize a new entry page
    /// entry page sturcture:
    /// [entries begin: 8bytes][next_free_chunk: 8bytes][bitmap: var size][entries...]
    /// Args:
    /// - allocate() -> (new_page, page_idx)
    /// - write(page_idx, offset, data)
    /// - read(page_idx) -> page
    fn new_entry_page(
        &mut self,
        allocate: &impl Fn() -> RsqlResult<(storage::Page, u64)>,
        write: &impl Fn(u64, u64, &[u8]) -> RsqlResult<()>,
        read: &impl Fn(u64) -> RsqlResult<storage::Page>,
    ) -> RsqlResult<u64> {
        let (_, page_idx) = allocate()?;
        // initialize page header
        let mut buf = vec![];
        buf.extend_from_slice(0u64.to_le_bytes().as_ref()); // entries begin
        buf.extend_from_slice(0u64.to_le_bytes().as_ref()); // next free chunk
        let bitmap_size = ((self.entries_per_page + 7) / 8) as usize;
        let bitmap_bytes = vec![0u8; bitmap_size];
        buf.extend_from_slice(&bitmap_bytes);
        let entries_begin = buf.len() as u64;
        buf[..8].copy_from_slice(&entries_begin.to_le_bytes()); // entries begin
        // update previous pointer
        let previous_page = self.entry_page_list_tail(&read)?;
        let update_page;
        let update_offset;
        if previous_page == 0 {
            self.first_free_entry_page = page_idx;
            update_page = 0;
            update_offset = self.begin_with + 16; 
        } else {
            update_page = previous_page;
            update_offset = 8; // next_free_chunk offset
        }
        // write new page to disk
        write(page_idx, 0, &buf)?;
        // update previous page's next pointer
        let mut prev_page_data = read(update_page)?;
        prev_page_data.data[update_offset as usize..(update_offset as usize + 8)]
            .copy_from_slice(&page_idx.to_le_bytes());
        write(update_page, update_offset, &prev_page_data.data[update_offset as usize..(update_offset as usize + 8)])?;
        Ok(page_idx)
    }
    /// Free an entry page
    /// Args:
    /// - write(page_idx, offset, data)
    /// - read(page_idx) -> page
    /// - free(page_idx)
    fn del_entry_page(
        &mut self,
        page_idx: u64,
        write: impl Fn(u64, u64, &[u8]) -> RsqlResult<()>,
        read: impl Fn(u64) -> RsqlResult<storage::Page>,
        free: impl Fn(u64) -> RsqlResult<()>,
    ) -> RsqlResult<()> {
        // read page
        let page = read(page_idx)?;
        let next_free_chunk_bytes = &page.data[8..16];
        let next_free_chunk = u64::from_le_bytes(next_free_chunk_bytes.try_into().unwrap());
        // find previous page
        let mut current_page = self.first_free_entry_page;
        let mut prev_page = 0;
        while current_page != 0 && current_page != page_idx {
            let current_page_data = read(current_page)?;
            let next_free_chunk_bytes = &current_page_data.data[8..16];
            let next_free_chunk_ptr = u64::from_le_bytes(next_free_chunk_bytes.try_into().unwrap());
            if next_free_chunk_ptr == page_idx {
                prev_page = current_page;
                break;
            }
            current_page = next_free_chunk_ptr;
        }
        // update previous page's next pointer
        if prev_page == 0 {
            self.first_free_entry_page = next_free_chunk;
        } else {
            let mut prev_page_data = read(prev_page)?;
            prev_page_data.data[8..16].copy_from_slice(&next_free_chunk.to_le_bytes());
            write(prev_page, 8, &prev_page_data.data[8..16])?;
        }
        // free the page
        free(page_idx)?;
        Ok(())
    }
    /// Allocate an empty entry
    /// Args:
    /// - write(page_idx, page)
    /// - read(page_idx) -> page
    /// - allocate() -> (new_page, page_idx)
    /// Return: (page_idx, page_offset)
    pub fn alloc_entry(
        &mut self,
        write: impl Fn(u64, u64, &[u8]) -> RsqlResult<()>,
        read: impl Fn(u64) -> RsqlResult<storage::Page>,
        allocate: impl Fn() -> RsqlResult<(storage::Page, u64)>,
    ) -> RsqlResult<(u64, u64)> {
        // 1. find a page with free entry
        // Traverse the linked list of entry pages to find one with an empty slot
        let mut free_page = 0;
        let mut current_search = self.first_free_entry_page;
        let bitmap_size = ((self.entries_per_page + 7) / 8) as usize;

        while current_search != 0 {
            let page = read(current_search)?;
            if utils::find_first_0_bit(&page.data[16..16+bitmap_size], self.entries_per_page as usize).is_some() {
                free_page = current_search;
                break;
            }
            let next_page_bytes = &page.data[8..16];
            current_search = u64::from_le_bytes(next_page_bytes.try_into().unwrap());
        }

        if free_page == 0 {
            free_page = self.new_entry_page(&allocate, &write, &read)?;
        }
        
        // 2. find free entry in the page
        let page = read(free_page)?;
        let mut bitmap_bytes = page.data[16..16+bitmap_size].to_vec();
        
        let entry_index = utils::find_first_0_bit(&bitmap_bytes, self.entries_per_page as usize).expect("Should have space now") as u64;


        // 3. mark entry as used
        let byte_idx = (entry_index / 8) as usize;
        let bit_idx = (entry_index % 8) as u8;
        bitmap_bytes[byte_idx] |= 1u8 << bit_idx;
        write(free_page, 16 + byte_idx as u64, &bitmap_bytes[byte_idx..byte_idx+1])?;
        
        // 4. calculate entry offset
        // We must re-read or use correct buffer for calculating begin, because 'page' is valid
        let entry_begin_bytes = &page.data[0..8];
        let entry_begin = u64::from_le_bytes(entry_begin_bytes.try_into().unwrap());
        let entry_offset = entry_index * self.entry_size + entry_begin;
        
        Ok((free_page, entry_offset))
    }
    /// Free an entry
    pub fn free_entry(
        &mut self,
        page_idx: u64,
        entry_offset: u64,
        write: impl Fn(u64, u64, &[u8]) -> RsqlResult<()>,
        read: impl Fn(u64) -> RsqlResult<storage::Page>,
        free: impl Fn(u64) -> RsqlResult<()>,
    ) -> RsqlResult<()> {
        // 1. read page
        let page = read(page_idx)?;
        let entry_begin_bytes = &page.data[0..8];
        let entry_begin = u64::from_le_bytes(entry_begin_bytes.try_into().unwrap());
        let relative_offset = entry_offset - entry_begin;
        let entry_index = relative_offset / self.entry_size;
        // 2. mark entry as free
        let bitmap_size = ((self.entries_per_page + 7) / 8) as usize;
        let mut bitmap_bytes = page.data[16..16+bitmap_size].to_vec();
        let byte_idx = (entry_index / 8) as usize;
        let bit_idx = (entry_index % 8) as u8;
        bitmap_bytes[byte_idx] &= !(1u8 << bit_idx);
        write(page_idx, 16 + byte_idx as u64, &bitmap_bytes[byte_idx..byte_idx+1])?;
        // 3. check if the page is now completely free
        let all_free = bitmap_bytes.iter().all(|&b| b == 0u8);
        if all_free {
            self.del_entry_page(page_idx, write, read, free)?;
        }
        Ok(())
    }
    fn heap_page_list_tail(
        &self,
        read: &impl Fn(u64) -> RsqlResult<storage::Page>,
    ) -> RsqlResult<u64> {
        let mut current_page = self.first_free_heap_page;
        let mut prev_page = 0;
        while current_page != 0 {
            let page = read(current_page)?;
            let next_free_chunk_bytes = &page.data[0..8];
            let next_free_chunk = u64::from_le_bytes(next_free_chunk_bytes.try_into().unwrap());
            if next_free_chunk == 0 {
                return Ok(current_page);
            }
            prev_page = current_page;
            current_page = next_free_chunk;
        }
        Ok(prev_page)
    }
    /// Allocate and initialize a new heap page
    /// The heap page structure:
    /// [next_free_chunk: 8bytes][prev_free_chunk: 8bytes][free_list_begin_in_this_page: 8bytes][data...]
    /// Each free chunk structure:
    /// [next_free_chunk: 8bytes][prev_free_chunk: 8bytes][chunk_size: 8bytes(exclude header)][data...]
    /// Each non-free chunk structure:
    /// [chunk_size: 8bytes(exclude header)][magic_number: 16bytes][data...]
    fn new_heap_page (
        &mut self,
        allocate: impl Fn() -> RsqlResult<(storage::Page, u64)>,
        write: impl Fn(u64, u64, &[u8]) -> RsqlResult<()>,
        read: impl Fn(u64) -> RsqlResult<storage::Page>,
    ) -> RsqlResult<u64> {
        let prev_page = self.heap_page_list_tail(&read)?;
        let (_, page_idx) = allocate()?;
        // initialize page header
        let mut buf = vec![];
        buf.extend_from_slice(0u64.to_le_bytes().as_ref()); // next free chunk
        buf.extend_from_slice(prev_page.to_le_bytes().as_ref()); // prev free chunk
        let first_free_chunk_offset: u64 = 8 + 8 + 8; // after page header
        buf.extend_from_slice(first_free_chunk_offset.to_le_bytes().as_ref()); // free list begin in this page
        write(page_idx, 0, &buf)?;
        // initialize first free chunk
        let max_size = storage::Page::max_size() as u64;
        
        // chunk_size represents payload size.
        // Chunk Header is 24 bytes (next:8, prev:8, size:8).
        // Total available space = PageSize - PageHeader(24)
        // Data Size = Total Available - ChunkHeader(24)
        let chunk_size = max_size - first_free_chunk_offset - 24; 
        
        let mut free_chunk_data = vec![];
        free_chunk_data.extend_from_slice(0u64.to_le_bytes().as_ref()); // next
        free_chunk_data.extend_from_slice(0u64.to_le_bytes().as_ref()); // prev
        free_chunk_data.extend_from_slice(chunk_size.to_le_bytes().as_ref()); // size
        write(page_idx, first_free_chunk_offset, &free_chunk_data)?;
        // update previous pointer
        if prev_page == 0 {
            self.first_free_heap_page = page_idx;
            // write to page 0
            let offset = self.begin_with + 8 + 8 + 8; // after entry page ptrs
            let bytes = page_idx.to_le_bytes();
            write(0, offset, &bytes)?;
        } else {
            let mut prev_page_data = read(prev_page)?;
            prev_page_data.data[0..8].copy_from_slice(&page_idx.to_le_bytes());
            write(prev_page, 0, &prev_page_data.data[0..8])?;
        }
        Ok(page_idx)
    }
    /// Deallocate a heap page NOT CHUNK
    fn del_heap_page(
        &mut self,
        page_idx: u64,
        write: impl Fn(u64, u64, &[u8]) -> RsqlResult<()>,
        read: impl Fn(u64) -> RsqlResult<storage::Page>,
        free: impl Fn(u64) -> RsqlResult<()>,
    ) -> RsqlResult<()> {
        let current_page = read(page_idx)?;
        // check if the page is empty
        let free_list_begin_bytes = &current_page.data[16..24];
        let free_list_begin = u64::from_le_bytes(free_list_begin_bytes.try_into().unwrap());
        if free_list_begin != 8 + 8 + 8 {
            panic!("Trying to delete a non-empty heap page {}", page_idx);
        }
        let first_chunk_size_bytes = &current_page.data[24..32];
        let first_chunk_size = u64::from_le_bytes(first_chunk_size_bytes.try_into().unwrap());
        let max_size = storage::Page::max_size() as u64;
        
        // CORRECTION: first_chunk_size is payload size. 
        // We verify that PayloadSize + ChunkHeader(24) + PageHeader(24) == PageSize
        if first_chunk_size + 24 + 24 != max_size {
            panic!("Trying to delete a non-empty heap page {}", page_idx);
        }
        // read next and prev pointers
        let next_free_page_bytes = &current_page.data[0..8];
        let next_free_page = u64::from_le_bytes(next_free_page_bytes.try_into().unwrap());
        let prev_free_page_bytes = &current_page.data[8..16];
        let prev_free_page = u64::from_le_bytes(prev_free_page_bytes.try_into().unwrap());
        // update previous page's next pointer
        if prev_free_page == 0 {
            self.first_free_heap_page = next_free_page;
            // write to page 0
            let offset = self.begin_with + 8 + 8 + 8; // after entry page ptrs
            let bytes = next_free_page.to_le_bytes();
            write(0, offset, &bytes)?;
        } else {
            let mut prev_page_data = read(prev_free_page)?;
            prev_page_data.data[0..8].copy_from_slice(&next_free_page.to_le_bytes());
            write(prev_free_page, 0, &prev_page_data.data[0..8])?;
        };
        // update next page's prev pointer
        if next_free_page != 0 {
            let mut next_page_data = read(next_free_page)?;
            next_page_data.data[8..16].copy_from_slice(&prev_free_page.to_le_bytes());
            write(next_free_page, 8, &next_page_data.data[8..16])?;
        }
        free(page_idx)?;
        Ok(())
    }
    /// Allocate heap space of given size
    /// Args:
    /// - allocate() -> (new_page, page_idx)
    /// - write(page_idx, offset, data)
    /// - read(page_idx) -> page
    /// Return: (page_idx, offset)
    pub fn alloc_heap(
        &mut self, 
        size:u64,
        allocate: impl Fn() -> RsqlResult<(storage::Page, u64)>,
        write: impl Fn(u64, u64, &[u8]) -> RsqlResult<()>,
        read: impl Fn(u64) -> RsqlResult<storage::Page>,
    ) -> RsqlResult<(u64, u64)> {
        let mut current_page;
        if self.first_free_heap_page == 0 {
            current_page = self.new_heap_page(&allocate, &write, &read)?;
        } else {
            current_page = self.first_free_heap_page;
        };
        // traverse heap pages and chunks to find **first-fit**
        let (page_idx, offset) = 'outer: loop {
            let page = read(current_page)?;
            let free_list_begin_bytes = &page.data[16..24];
            let chunk_header_size = 8 + 16; // chunk_size + magic_number + padding(actually 24 bytes total header)
            let free_chunk_ptr = u64::from_le_bytes(free_list_begin_bytes.try_into().unwrap());
            let mut cursor = free_chunk_ptr;
            while cursor != 0 {
                // Verify bounds or assume valid. Read from `page.data` directly.
                let cursor_idx = cursor as usize;
                
                // Chunk structure: [next: 8][prev: 8][size: 8]
                let chunk_size_bytes = &page.data[cursor_idx + 16 .. cursor_idx + 24];
                let chunk_size = u64::from_le_bytes(chunk_size_bytes.try_into().unwrap());
                
                if chunk_size >= size { // no need to plus header size, chunk_size only counts data size
                    // found a suitable chunk
                    // 1. remove chunk from free list
                    let next_free_chunk_bytes = &page.data[cursor_idx..cursor_idx+8];
                    let next_free_chunk = u64::from_le_bytes(next_free_chunk_bytes.try_into().unwrap());
                    let prev_free_chunk_bytes = &page.data[cursor_idx+8..cursor_idx+16];
                    let prev_free_chunk = u64::from_le_bytes(prev_free_chunk_bytes.try_into().unwrap());
                    
                    // update previous chunk's next pointer
                    if prev_free_chunk == 0 {
                        // update page header
                        let mut page_data = read(current_page)?;
                        page_data.data[16..24].copy_from_slice(&next_free_chunk.to_le_bytes());
                        write(current_page, 16, &page_data.data[16..24])?;
                    } else {
                        // Write to current_page at prev_free_chunk offset
                        let mut prev_chunk_page = read(current_page)?;
                        let prev_offset = prev_free_chunk as usize;
                        prev_chunk_page.data[prev_offset..prev_offset+8].copy_from_slice(&next_free_chunk.to_le_bytes());
                        write(current_page, prev_free_chunk, &prev_chunk_page.data[prev_offset..prev_offset+8])?;
                    }
                    // update next chunk's prev pointer
                    if next_free_chunk != 0 {
                        // Write to current_page at next_free_chunk offset
                        let mut next_chunk_page = read(current_page)?;
                        let next_offset = next_free_chunk as usize;
                        next_chunk_page.data[next_offset+8..next_offset+16].copy_from_slice(&prev_free_chunk.to_le_bytes());
                        write(current_page, next_free_chunk + 8, &next_chunk_page.data[next_offset+8..next_offset+16])?;
                    }
                    // 2. check if we need to split the chunk
                    if chunk_size > size + chunk_header_size {
                        // split the chunk
                        let new_free_chunk_ptr = cursor + 8 + 16 + size;
                        let new_free_chunk_size = chunk_size - size - 8 - 16;
                        if new_free_chunk_size > 0 { // if size too small, do not split
                            // split and insert new free chunk into free list
                            let mut new_free_chunk_data = vec![];
                            new_free_chunk_data.extend_from_slice(0u64.to_le_bytes().as_ref()); // next free chunk
                            new_free_chunk_data.extend_from_slice(0u64.to_le_bytes().as_ref()); // prev free chunk
                            new_free_chunk_data.extend_from_slice(new_free_chunk_size.to_le_bytes().as_ref()); // chunk size
                            write(current_page, new_free_chunk_ptr, &new_free_chunk_data)?;
                            // insert new free chunk into free list (at the beginning)
                            let mut page_data = read(current_page)?;
                            let old_free_list_begin_bytes = &page_data.data[16..24];
                            let old_free_list_begin = u64::from_le_bytes(old_free_list_begin_bytes.try_into().unwrap());
                            // update new free chunk's next and prev pointers
                            // new_free_chunk_ptr is in `current_page`.
                            // We construct the header data then write it.
                            let mut new_free_chunk_header = vec![];
                            new_free_chunk_header.extend_from_slice(&old_free_list_begin.to_le_bytes());
                            new_free_chunk_header.extend_from_slice(&0u64.to_le_bytes());
                            // Size is already written
                            write(current_page, new_free_chunk_ptr, &new_free_chunk_header)?;

                            // update old free list begin's prev pointer
                            if old_free_list_begin != 0 {
                                // Write to current_page at old_free_list_begin offset
                                let mut old_free_chunk_page = read(current_page)?;
                                let old_offset = old_free_list_begin as usize;
                                old_free_chunk_page.data[old_offset+8..old_offset+16].copy_from_slice(&new_free_chunk_ptr.to_le_bytes());
                                write(current_page, old_free_list_begin + 8, &old_free_chunk_page.data[old_offset+8..old_offset+16])?;
                            }
                            // update page header
                            page_data.data[16..24].copy_from_slice(&new_free_chunk_ptr.to_le_bytes());
                            write(current_page, 16, &page_data.data[16..24])?;
                        }
                    }
                    // 3. initialize allocated chunk
                    let mut allocated_chunk_data = vec![];
                    allocated_chunk_data.extend_from_slice(size.to_le_bytes().as_ref()); // chunk size
                    allocated_chunk_data.extend_from_slice(MAGIC_NUMBER.to_le_bytes().as_ref()); // magic number
                    allocated_chunk_data.extend_from_slice(&vec![0u8; 16 - 4]); // padding
                    write(current_page, cursor, &allocated_chunk_data)?;
                    // return allocated chunk ptr
                    break 'outer (current_page, cursor);
                }
                let next_free_chunk_bytes = &page.data[cursor_idx..cursor_idx+8];
                cursor = u64::from_le_bytes(next_free_chunk_bytes.try_into().unwrap());
            }
            // move to next heap page
            let next_page_bytes = &page.data[0..8];
            let next_page = u64::from_le_bytes(next_page_bytes.try_into().unwrap());
            if next_page == 0 {
                // allocate new heap page
                let new_page = self.new_heap_page(&allocate, &write, &read)?;
                current_page = new_page;
            } else {
                current_page = next_page;
            }
        };
        Ok((page_idx, offset))
    }
    /// Free a heap chunk
    /// Args:
    /// - write(page_idx, offset, data)
    /// - read(page_idx) -> page
    pub fn free_heap(
        &mut self,
        page_idx: u64,
        offset: u64,
        write: impl Fn(u64, u64, &[u8]) -> RsqlResult<()>,
        read: impl Fn(u64) -> RsqlResult<storage::Page>,
    ) -> RsqlResult<()> {
        // read chunk header
        let page = read(page_idx)?;
        let chunk_size_bytes = &page.data[offset as usize..(offset as usize + 8)];
        let chunk_size = u64::from_le_bytes(chunk_size_bytes.try_into().unwrap());
        // initialize free chunk
        let mut free_chunk_data = vec![];
        free_chunk_data.extend_from_slice(0u64.to_le_bytes().as_ref()); // next free chunk
        free_chunk_data.extend_from_slice(0u64.to_le_bytes().as_ref()); // prev free chunk
        free_chunk_data.extend_from_slice(chunk_size.to_le_bytes().as_ref()); // chunk size
        write(page_idx, offset, &free_chunk_data)?;
        // insert free chunk into free list (at the beginning)
        let mut page_data = read(page_idx)?;
        let old_free_list_begin_bytes = &page_data.data[16..24];
        let old_free_list_begin = u64::from_le_bytes(old_free_list_begin_bytes.try_into().unwrap());
        // update new free chunk's next and prev pointers
        let mut new_free_chunk_page = read(page_idx)?;
        let offset_idx = offset as usize;
        new_free_chunk_page.data[offset_idx..offset_idx+8].copy_from_slice(&old_free_list_begin.to_le_bytes());
        new_free_chunk_page.data[offset_idx+8..offset_idx+16].copy_from_slice(&0u64.to_le_bytes());
        write(page_idx, offset, &new_free_chunk_page.data[offset_idx..offset_idx+16])?;
        
        // update old free list begin's prev pointer
        if old_free_list_begin != 0 {
            // Update current page (page_idx) at `old_free_list_begin`
            let mut old_free_chunk_page = read(page_idx)?;
            let old_offset = old_free_list_begin as usize;
            old_free_chunk_page.data[old_offset+8..old_offset+16].copy_from_slice(&offset.to_le_bytes());
            write(page_idx, old_free_list_begin + 8, &old_free_chunk_page.data[old_offset+8..old_offset+16])?;
        }
        // update page header
        page_data.data[16..24].copy_from_slice(&offset.to_le_bytes());
        write(page_idx, 16, &page_data.data[16..24])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::{HashMap, HashSet};

    // Enhanced MockEnv to track allocations and frees
    struct MockEnv {
        pages: RefCell<HashMap<u64, storage::Page>>,
        next_page_id: RefCell<u64>,
        freed_pages: RefCell<Vec<u64>>, 
    }

    impl MockEnv {
        fn new() -> Self {
            MockEnv {
                pages: RefCell::new(HashMap::new()),
                next_page_id: RefCell::new(1), // start from 1, 0 is reserved
                freed_pages: RefCell::new(Vec::new()),
            }
        }
        
        fn init_page_0(&self) {
             self.pages.borrow_mut().insert(0, storage::Page::new());
        }

        fn read(&self, page_idx: u64) -> RsqlResult<storage::Page> {
            let pages = self.pages.borrow();
            // Simulate reading from disk; if page exists return it, else panic or return empty
            // In a real FS, reading beyond EOF or unallocated blocks might error or return 0s.
            if let Some(p) = pages.get(&page_idx) {
                Ok(p.clone())
            } else {
                // Return empty new page if not found (simulating sparse file)
                Ok(storage::Page::new())
            }
        }

        fn write(&self, page_idx: u64, offset: u64, data: &[u8]) -> RsqlResult<()> {
            let mut pages = self.pages.borrow_mut();
            let page = pages.entry(page_idx).or_insert(storage::Page::new());
             if offset as usize + data.len() > page.data.len() {
                panic!("Write out of bounds: page {} offset {} len {}", page_idx, offset, data.len());
            }
            page.data[offset as usize..offset as usize + data.len()].copy_from_slice(data);
            Ok(())
        }

        fn allocate(&self) -> RsqlResult<(storage::Page, u64)> {
            let mut id = self.next_page_id.borrow_mut();
            let page_idx = *id;
            *id += 1;
            let page = storage::Page::new();
            self.pages.borrow_mut().insert(page_idx, page.clone());
            Ok((page, page_idx))
        }
        
        fn free(&self, page_idx: u64) -> RsqlResult<()> {
            self.pages.borrow_mut().remove(&page_idx);
            self.freed_pages.borrow_mut().push(page_idx);
            Ok(())
        }

        // Helper to check if a page is currently allocated
        fn is_allocated(&self, page_idx: u64) -> bool {
            self.pages.borrow().contains_key(&page_idx)
        }
    }

    // --- Helpers to reduce boilerplate in tests ---
    fn get_closures<'a>(env: &'a MockEnv) -> (
        impl Fn() -> RsqlResult<(storage::Page, u64)> + 'a,
        impl Fn(u64, u64, &[u8]) -> RsqlResult<()> + 'a,
        impl Fn(u64) -> RsqlResult<storage::Page> + 'a,
        impl Fn(u64) -> RsqlResult<()> + 'a
    ) {
        (
            || env.allocate(),
            |p, o, d| env.write(p, o, d),
            |p| env.read(p),
            |p| env.free(p)
        )
    }

    #[test]
    fn test_allocator_entry_basic() {
        let env = MockEnv::new();
        env.init_page_0();
        let mut alloc = Allocator::create(100, 0);
        let (alloc_fn, write_fn, read_fn, _) = get_closures(&env);

        let (page1, offset1) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();
        assert!(page1 > 0);
        let (page2, offset2) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();
        
        // Assertions
        assert_eq!(page1, page2, "Small entries should fit in same page");
        assert_ne!(offset1, offset2, "Offsets should differ");
        // Verify offsets are aligned
        assert_eq!((offset2 - offset1), 100);
    }

    #[test]
    fn test_allocator_entry_free_and_reuse() {
        let env = MockEnv::new();
        env.init_page_0();
        let mut alloc = Allocator::create(100, 0);
        let (alloc_fn, write_fn, read_fn, free_fn) = get_closures(&env);

        let (p1, o1) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();
        let (p2, o2) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();

        // Free the first entry
        alloc.free_entry(p1, o1, &write_fn, &read_fn, &free_fn).unwrap();

        // Next allocation should reuse the freed slot (since it's the first 0 bit)
        let (p3, o3) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();
        
        assert_eq!(p1, p3);
        assert_eq!(o1, o3, "Should reuse the freed entry slot");

        // Allocating again should give a new slot
        let (p4, o4) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();
        assert_ne!(o4, o3);
        assert_ne!(o4, o2);
    }

    #[test]
    fn test_allocator_entry_page_cleanup() {
        let env = MockEnv::new();
        env.init_page_0();
        // Use large entries to fill a page quickly. 
        // Page size 4096. Entry size 2000. 
        // Approx 2 entries per page.
        let mut alloc = Allocator::create(2000, 0);
        let (alloc_fn, write_fn, read_fn, free_fn) = get_closures(&env);

        // Fill Page 1
        let (p1, o1) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();
        let (p2, o2) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();
        assert_eq!(p1, p2);

        // Trigger Page 2 allocation
        let (p3, o3) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();
        assert_ne!(p3, p1);

        // Free all entries in Page 2
        alloc.free_entry(p3, o3, &write_fn, &read_fn, &free_fn).unwrap();

        // Assert Page 2 is freed in environment
        assert!(env.freed_pages.borrow().contains(&p3), "Page should be freed when empty");
        assert!(!env.is_allocated(p3));

        // Allocating again should create a NEW page (ID will increment)
        // or reuse Page 2 id if mocked allocator reused ids (but ours flat increases).
        let (p4, _) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();
        assert!(p4 > p3, "Should allocate new page ID");
    }

    #[test]
    fn test_allocator_heap_basic_and_reuse() {
        let env = MockEnv::new();
        env.init_page_0();
        let mut alloc = Allocator::create(100, 0);
        let (alloc_fn, write_fn, read_fn, _) = get_closures(&env);

        let size = 120;
        let (p1, o1) = alloc.alloc_heap(size, &alloc_fn, &write_fn, &read_fn).unwrap();
        
        // Free it
        alloc.free_heap(p1, o1, &write_fn, &read_fn).unwrap();

        // Alloc same size again, should reuse exact spot
        let (p2, o2) = alloc.alloc_heap(size, &alloc_fn, &write_fn, &read_fn).unwrap();
        
        assert_eq!(p1, p2);
        assert_eq!(o1, o2, "Should reuse heap chunk");
    }

    #[test]
    fn test_allocator_heap_split() {
        let env = MockEnv::new();
        env.init_page_0();
        let mut alloc = Allocator::create(100, 0);
        let (alloc_fn, write_fn, read_fn, _) = get_closures(&env);

        // 1. Allocate big chunk (e.g. 1000 bytes)
        let (p1, o1) = alloc.alloc_heap(1000, &alloc_fn, &write_fn, &read_fn).unwrap();

        // 2. Free it
        alloc.free_heap(p1, o1, &write_fn, &read_fn).unwrap();

        // 3. Allocate small chunk (e.g. 100 bytes)
        // Should reuse the 1000 byte hole, splitting it.
        let (p2, o2) = alloc.alloc_heap(100, &alloc_fn, &write_fn, &read_fn).unwrap();

        assert_eq!(p1, p2);
        assert_eq!(o1, o2);

        // 4. Allocate another small chunk
        // Should be right after the previous one (plus header overhead)
        let (p3, o3) = alloc.alloc_heap(100, &alloc_fn, &write_fn, &read_fn).unwrap();
        
        assert_eq!(p1, p3);
        // Header is 24 bytes (next, prev, size) -> see new_heap_page logic
        // But alloc_heap logic: returns pointer to MAGIC_NUMBER/Data start? 
        // Looking at code: `break 'outer (current_page, cursor);` where cursor is the start of Chunk Header.
        // Wait, normally allocator returns data pointer.
        // Your code returns `cursor`, which is the start of the Chunk (Header included).
        // Let's verify offset logic:
        // Chunk1: [Header 24][Data 100][Padding? if aligned]
        // Chunk2: Starts at Offset1 + 24 + 100 approx.
        assert!(o3 > o2 + 100); 
    }

    #[test]
    fn test_allocator_heap_multi_page() {
        let env = MockEnv::new();
        env.init_page_0();
        let mut alloc = Allocator::create(64, 0);
        let (alloc_fn, write_fn, read_fn, _) = get_closures(&env);

        // Page size 4096. 
        // Alloc 3000 bytes -> Page 1
        let (p1, _) = alloc.alloc_heap(3000, &alloc_fn, &write_fn, &read_fn).unwrap();
        
        // Alloc 3000 bytes -> Wont fit in Page 1, should go to Page 2
        let (p2, _) = alloc.alloc_heap(3000, &alloc_fn, &write_fn, &read_fn).unwrap();

        assert_ne!(p1, p2);
        
        // Alloc 500 bytes -> Should fit in remaining space of Page 1 or Page 2?
        // Current logic: traverse pages. Page 1 has ~1000 free. Page 2 has ~1000 free.
        // If it starts search from `first_free_heap_page`, it should fill Page 1 first.
        let (p3, _) = alloc.alloc_heap(500, &alloc_fn, &write_fn, &read_fn).unwrap();
        
        // This depends on whether Page 1 was removed from free list when full? 
        // Your logic keeps pages linked. 
        // Page structure: [NextPage]...
        // So it should iterate Page 1 -> Page 2.
        // 3000 + 24 header = 3024. 4096 - 3024 = 1072 free.
        // 500 should fit in Page 1.
        assert_eq!(p3, p1, "Should fill holes in first pages");
    }

    #[test]
    fn test_persistence() {
        let env = MockEnv::new();
        env.init_page_0();
        
        // 1. Setup Allocator and make some changes
        let mut alloc = Allocator::create(128, 100); // begin_with 100
        let (alloc_fn, write_fn, read_fn, _) = get_closures(&env);
        
        let (p_entry, _) = alloc.alloc_entry(&write_fn, &read_fn, &alloc_fn).unwrap();
        let (_p_heap, _) = alloc.alloc_heap(50, &alloc_fn, &write_fn, &read_fn).unwrap();
        
        // 2. Serialize to Page 0
        let bytes = alloc.to_bytes();
        env.write(0, 100, &bytes).unwrap();
        
        // 3. New Allocator instance from Page 0
        let page0 = env.read(0).unwrap();
        let alloc_restored = Allocator::from(&page0, 100).unwrap();

        // 4. Verify state
        assert_eq!(alloc.entry_size, alloc_restored.entry_size);
        assert_eq!(alloc.entries_per_page, alloc_restored.entries_per_page);
        // Pointers should point to valid pages we allocated
        assert_eq!(alloc_restored.first_free_entry_page, p_entry); 
        // For heap, `first_free_heap_page` might be p_heap or something earlier
        assert!(alloc_restored.first_free_heap_page > 0);
    }
}