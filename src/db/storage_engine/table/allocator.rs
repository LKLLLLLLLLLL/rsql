use super::storage;
use crate::db::errors::RsqlResult;
use super::super::consist_storage::ConsistStorageEngine;

/// Helper function set for bitmap
struct Bitmap;
impl Bitmap {
    pub fn is_full(bytes: &[u8], num_bits: usize) -> bool {
        for i in 0..num_bits {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            if (bytes[byte_idx] & (1u8 << bit_idx)) == 0 {
                return false;
            }
        }
        true
    }
    pub fn is_all_empty(bytes: &[u8], num_bits: usize) -> bool {
        for i in 0..num_bits {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            if (bytes[byte_idx] & (1u8 << bit_idx)) != 0 {
                return false;
            }
        }
        true
    }
    pub fn empty_bitmap(num_bits: usize) -> Vec<u8> {
        let num_bytes = (num_bits + 7) / 8;
        vec![0u8; num_bytes]
    }
    pub fn find_empty_bit(bytes: &[u8], num_bits: usize) -> Option<usize> {
        for (byte_idx, byte) in bytes.iter().enumerate() {
            if *byte != 0xFF {
                let inv = !byte;
                let tz = inv.trailing_zeros() as usize;
                if byte_idx * 8 + tz >= num_bits {
                    return None;
                }
                return Some(byte_idx * 8 + tz);
            }
        }
        None
    }
    pub fn set_bit_true(bytes: &mut [u8], bit_idx: usize) {
        let byte_idx = bit_idx / 8;
        let bit_in_byte = bit_idx % 8;
        bytes[byte_idx] |= 1u8 << bit_in_byte;
    }   
    pub fn set_bit_false(bytes: &mut [u8], bit_idx: usize) {
        let byte_idx = bit_idx / 8;
        let bit_in_byte = bit_idx % 8;
        bytes[byte_idx] &= !(1u8 << bit_in_byte);
    }
}


/// Helper function set for entry page
/// entry page sturcture:
/// - header: [next_free_page: 8bytes][prev_free_page: 8bytes][bitmap: var size]
/// - body:   [entries...]
struct EntryPage;
impl EntryPage {
    pub fn next_free_page(page: &storage::Page) -> u64 {
        let bytes = &page.data;
        let next_bytes = &bytes[0..8];
        u64::from_le_bytes(next_bytes.try_into().unwrap())
    }
    pub fn set_next_free_page(page: &mut storage::Page, next: u64) {
        let bytes = &mut page.data;
        bytes[0..8].copy_from_slice(&next.to_le_bytes());
    }
    pub fn prev_free_page(page: &storage::Page) -> u64 {
        let bytes = &page.data;
        let prev_bytes = &bytes[8..16];
        u64::from_le_bytes(prev_bytes.try_into().unwrap())
    }
    pub fn set_prev_free_page(page: &mut storage::Page, prev: u64) {
        let bytes = &mut page.data;
        bytes[8..16].copy_from_slice(&prev.to_le_bytes());
    }
    pub fn bitmap_size(entries_per_page: u64) -> usize {
        ((entries_per_page + 7) / 8) as usize
    }
    pub fn bitmap(page: &storage::Page, entries_per_page: u64) -> &[u8] {
        let bytes = &page.data;
        let bitmap_size = Self::bitmap_size(entries_per_page);
        &bytes[16..16 + bitmap_size]
    }
    pub fn set_bitmap(page: &mut storage::Page, entries_per_page: u64, bitmap: &[u8]) {
        let bytes = &mut page.data;
        let bitmap_size = Self::bitmap_size(entries_per_page);
        bytes[16..16 + bitmap_size].copy_from_slice(&bitmap[0..bitmap_size]);
    }
    pub fn entries_begin(entries_per_page: u64) -> u64 {
        let bitmap_size = Self::bitmap_size(entries_per_page) as u64;
        16 + bitmap_size
    }
    pub fn entries_offset(entry_index: u64, entry_size: u64, entries_per_page: u64) -> u64 {
        Self::entries_begin(entries_per_page) + entry_index * entry_size
    }
    pub fn entries_index(entry_offset: u64, entry_size: u64, entries_per_page: u64) -> u64 {
        (entry_offset - Self::entries_begin(entries_per_page)) / entry_size
    }
    pub fn new_page(
        entries_per_page: u64,
        next_free_page: Option<u64>,
        prev_free_page: Option<u64>,
    ) -> storage::Page {
        let mut page = storage::Page::new();
        // initialize next_free_chunk
        let next = match next_free_page {
            Some(n) => n,
            None => 0,
        };
        Self::set_next_free_page(&mut page, next);
        // initialize prev_free_page
        let prev = match prev_free_page {
            Some(p) => p,
            None => 0,
        };
        Self::set_prev_free_page(&mut page, prev);
        // initialize bitmap
        let bitmap_bytes = Bitmap::empty_bitmap(entries_per_page as usize);
        Self::set_bitmap(&mut page, entries_per_page, &bitmap_bytes);
        page
    }
}

/// Helper function set for heap page
/// The heap free space is managed by:
/// [first_free_heap_page_ptr] -> [free_heap_page] -> [free_heap_page] -> ...
/// Each free heap page:
/// - [next_free_page: 8bytes][prev_free_page: 8bytes]
/// - [first_free_chunk_offset: 8bytes]: >= 24
/// - [data...]
struct HeapPage;
impl HeapPage {
    /// Create and initialize a new free heap page
    /// Will initialize both header and first free chunk(include entire page)
    pub fn new_free_page(
        prev_free_page: Option<u64>,
        next_free_page: Option<u64>,
    ) -> storage::Page {
        let mut page = storage::Page::new();
        // initialize next_free_page
        let next = match next_free_page {
            Some(n) => n,
            None => 0,
        };
        Self::set_next_free_page(&mut page, next);
        // initialize prev_free_page
        let prev = match prev_free_page {
            Some(p) => p,
            None => 0,
        };
        Self::set_prev_free_page(&mut page, prev);
        // initialize first free chunk offset
        let first_chunk_offset = 24u64; // right after the header
        Self::set_first_free_chunk_offset(&mut page, first_chunk_offset);
        // initialize the only free chunk in this page
        HeapChunk::set_next_free_chunk(&mut page, first_chunk_offset, 0);
        HeapChunk::set_prev_free_chunk(&mut page, first_chunk_offset, 0);
        let chunk_size = (storage::Page::max_size() as u64) - first_chunk_offset - HeapChunk::header_size();
        HeapChunk::set_chunk_size_to_free(&mut page, first_chunk_offset, chunk_size);
        page
    }
    pub fn next_free_page(page: &storage::Page) -> u64 {
        let bytes = &page.data;
        let next_bytes = &bytes[0..8];
        u64::from_le_bytes(next_bytes.try_into().unwrap())
    }
    pub fn set_next_free_page(page: &mut storage::Page, next: u64) {
        let bytes = &mut page.data;
        bytes[0..8].copy_from_slice(&next.to_le_bytes());
    }
    pub fn prev_free_page(page: &storage::Page) -> u64 {
        let bytes = &page.data;
        let prev_bytes = &bytes[8..16];
        u64::from_le_bytes(prev_bytes.try_into().unwrap())
    }
    pub fn set_prev_free_page(page: &mut storage::Page, prev: u64) {
        let bytes = &mut page.data;
        bytes[8..16].copy_from_slice(&prev.to_le_bytes());
    }
    pub fn first_free_chunk_offset(page: &storage::Page) -> u64 {
        let bytes = &page.data;
        let offset_bytes = &bytes[16..24];
        u64::from_le_bytes(offset_bytes.try_into().unwrap())
    }
    pub fn set_first_free_chunk_offset(page: &mut storage::Page, offset: u64) {
        let bytes = &mut page.data;
        bytes[16..24].copy_from_slice(&offset.to_le_bytes());
    }
    pub fn check_page_empty(page: &storage::Page) -> bool {
        // check if first free chunk offset == 0
        let first_chunk_offset = Self::first_free_chunk_offset(page);
        if first_chunk_offset != 24 {
            return false;
        };
        // check if if chunk size == page size - chunk_header - page_header
        let chunk_size = HeapChunk::chunk_size(page, first_chunk_offset);
        if chunk_size != (storage::Page::max_size() as u64) - first_chunk_offset - 24 {
            return false;
        };
        true
    }
}

/// Helper function set for heap chunk
/// - Each free chunk:
///     - [next_free_chunk_offset: 8bytes][prev_free_chunk_offset: 8bytes]
///     - [chunk_size(exclude header): 8bytes(exclude header)]
///     - [data...]
/// - Each used chunk(has same header size as free chunk):
///     - [padding: 12bytes]: must be all zero
///     - [chunk_size: 8bytes(exclude header)][magic_number:4bytes]
///     - [data...]
/// CAUTION: the free chunk list must sorted by offset in ascending order
struct HeapChunk;
impl HeapChunk {
    pub fn header_size() -> u64 {
        24u64
    }
    /// then the chunk is free
    pub fn is_free(page: &storage::Page, offset: u64) -> bool {
        !Self::is_used(page, offset)
    }
    pub fn is_used(page: &storage::Page, offset: u64) -> bool {
        // 1. check magic number
        let magic_number_bytes = &page.data[offset as usize + 20..offset as usize + 24];
        let magic_number = u32::from_le_bytes(magic_number_bytes.try_into().unwrap());
        if magic_number != MAGIC_NUMBER {
            return false;
        }
        // 2. check padding bytes
        let padding_bytes = &page.data[offset as usize..offset as usize + 12];
        for b in padding_bytes {
            if *b != 0u8 {
                panic!("Heap chunk corruption detected: magic number satisfied but used chunk padding bytes not zero, found {}", *b);
            }
        }
        true
    }
    pub fn next_free_chunk(page: &storage::Page, offset: u64) -> u64 {
        if !Self::is_free(page, offset) {
            panic!("Trying to get next free chunk of a used chunk at offset {}", offset);
        }
        let bytes = &page.data;
        let offset = offset as usize;
        let chunk_header = &bytes[offset..offset + 8];
        u64::from_le_bytes(chunk_header.try_into().unwrap())
    }

    pub fn set_next_free_chunk(page: &mut storage::Page, offset: u64, next: u64) {
        if !Self::is_free(page, offset) {
            panic!("Trying to set next free chunk of a used chunk at offset {}", offset);
        }
        let bytes = &mut page.data;
        let offset = offset as usize;
        bytes[offset..offset + 8].copy_from_slice(&next.to_le_bytes());
    }

    pub fn prev_free_chunk(page: &storage::Page, offset: u64) -> u64 {
        if !Self::is_free(page, offset) {
            panic!("Trying to get prev free chunk of a used chunk at offset {}", offset);
        }
        let bytes = &page.data;
        let offset = offset as usize;
        let chunk_header = &bytes[offset + 8..offset + 16];
        u64::from_le_bytes(chunk_header.try_into().unwrap())
    }

    pub fn set_prev_free_chunk(page: &mut storage::Page, offset: u64, prev: u64) {
        if !Self::is_free(page, offset) {
            panic!("Trying to set prev free chunk of a used chunk at offset {}", offset);
        }
        let bytes = &mut page.data;
        let offset = offset as usize;
        bytes[offset + 8..offset + 16].copy_from_slice(&prev.to_le_bytes());
    }

    pub fn chunk_size(page: &storage::Page, offset: u64) -> u64 {
        if Self::is_free(page, offset) {
            // free chunk: chunk_size is at offset + 16
            let bytes = &page.data;
            let offset = offset as usize;
            let chunk_size_bytes = &bytes[offset + 16..offset + 24];
            u64::from_le_bytes(chunk_size_bytes.try_into().unwrap())
        } else {
            // used chunk: chunk_size is at offset + 12
            let bytes = &page.data;
            let offset = offset as usize;
            let chunk_size_bytes = &bytes[offset + 12..offset + 20];
            u64::from_le_bytes(chunk_size_bytes.try_into().unwrap())
        }
    }
    pub fn set_chunk_size_to_free(page: &mut storage::Page, offset: u64, size: u64) {
        // free chunk: chunk_size is at offset + 16
        let bytes = &mut page.data;
        let offset = offset as usize;
        bytes[offset + 16..offset + 24].copy_from_slice(&size.to_le_bytes());
    }
    pub fn set_chunk_size_to_used(page: &mut storage::Page, offset: u64, size: u64) {
        // used chunk: chunk_size is at offset + 12
        let bytes = &mut page.data;
        let offset = offset as usize;
        bytes[offset + 12..offset + 20].copy_from_slice(&size.to_le_bytes());
    }
    // offset is the pointer to the header of the chunk
    pub fn ptr_to_offset(ptr: u64) -> u64 {
        ptr - Self::header_size()
    }
    // ptr is the pointer to the data area of the chunk
    pub fn offset_to_ptr(offset: u64) -> u64 {
        offset + Self::header_size()
    }

    /// Set the padding bytes to all zero
    /// And the magic number to avoid corruption
    fn set_used(page: &mut storage::Page, offset: u64) {
        let offset = offset as usize;
        let old_chunk_size_bytes = page.data[offset + 16..offset + 24].to_vec();
        // let old_chunk_size = u64::from_le_bytes(old_chunk_size_bytes.try_into().unwrap());
        let magic_bytes = MAGIC_NUMBER.to_le_bytes();
        let zero_bytes = [0u8; 12];
        page.data[offset..offset + 12].copy_from_slice(&zero_bytes);
        page.data[offset + 12..offset + 20].copy_from_slice(&old_chunk_size_bytes);
        page.data[offset + 20..offset + 24].copy_from_slice(&magic_bytes);
        assert!(Self::is_used(page, offset as u64));
    }

    /// Set the chunk as free chunk
    /// This will not update free list links
    fn set_free(
        page: &mut storage::Page,
        offset: u64,
        next_free: u64,
        prev_free: u64,
    ) {
        let offset = offset as usize;
        let old_chunk_size_bytes = page.data[offset + 12..offset + 20].to_vec();
        let next_free_bytes = next_free.to_le_bytes();
        let prev_free_bytes = prev_free.to_le_bytes();
        page.data[offset..(offset + 8)].copy_from_slice(&next_free_bytes);
        page.data[(offset + 8)..(offset + 16)].copy_from_slice(&prev_free_bytes);
        page.data[(offset + 16)..(offset + 24)].copy_from_slice(&old_chunk_size_bytes);
        assert!(Self::is_free(page, offset as u64));
    }
    /// If you found this chunk suitable,
    /// you can call this function to allocate it
    /// This function will split the chunk, 
    /// repaire the freelist links, 
    /// and initialize the used chunk header
    /// Return: ptr to the data part of the allocated chunk
    pub fn alloc_chunk(
        page: &mut storage::Page,
        offset: u64,
        size: u64,
    ) -> RsqlResult<u64> {
        let chunk_size = Self::chunk_size(page, offset);
        if chunk_size < size {
            panic!("Trying to allocate chunk of size {} from chunk of size {}", size, chunk_size);
        }
        let next_free = Self::next_free_chunk(page, offset);
        let prev_free = Self::prev_free_chunk(page, offset);
        // 1. try split the chunk and update freelist links
        let remaining_size = chunk_size - size - Self::header_size();
        if remaining_size >= Self::header_size() + 8 { // make sure the remaining chunk's size >= 8bytes
            // split the chunk
            let new_free_chunk_offset = offset + Self::header_size() + size;
            // update current chunk size
            Self::set_chunk_size_to_free(page, offset, size); // the chunk size will be convert by set_used later
            // initialize new free chunk
            Self::set_chunk_size_to_free(page, new_free_chunk_offset, remaining_size);
            Self::set_next_free_chunk(page, new_free_chunk_offset, next_free);
            Self::set_prev_free_chunk(page, new_free_chunk_offset, prev_free);
            // update prev next pointer
            if prev_free != 0 {
                HeapChunk::set_next_free_chunk(page, prev_free, new_free_chunk_offset);
            } else {
                // update first free chunk pointer in page header
                HeapPage::set_first_free_chunk_offset(page, new_free_chunk_offset);
            }
            // update next prev pointer
            if next_free != 0 {
                HeapChunk::set_prev_free_chunk(page, next_free, new_free_chunk_offset);
            }
        } else {
            // only update freelist links
            // update prev next pointer
            if prev_free != 0 {
                HeapChunk::set_next_free_chunk(page, prev_free, next_free);
            } else {
                // update first free chunk pointer in page header
                HeapPage::set_first_free_chunk_offset(page, next_free);
            }
            // update next prev pointer
            if next_free != 0 {
                HeapChunk::set_prev_free_chunk(page, next_free, prev_free);
            }
        };
        // 2. initialize used chunk header
        Self::set_used(page, offset);
        Ok(Self::offset_to_ptr(offset))
    }
    pub fn dealloc_chunk(
        page: &mut storage::Page,
        ptr: u64,
    ) -> RsqlResult<()> {
        let offset = Self::ptr_to_offset(ptr);
        if Self::is_free(page, offset) {
            panic!("Trying to dealloc a free chunk at offset {}", offset);
        };
        // set chunk as free
        Self::set_free(page, offset, 0, 0);
        // traverse the free chunk list to find the proper position
        let mut current_chunk_offset = HeapPage::first_free_chunk_offset(page);
        let mut prev_free_chunk_offset = 0u64;
        loop {
            if current_chunk_offset == 0 || current_chunk_offset > offset {
                break;
            }
            prev_free_chunk_offset = current_chunk_offset;
            current_chunk_offset = Self::next_free_chunk(page, current_chunk_offset);
        };
        // find if the adjacent chunks are free
        let mut merge_prev = false;
        if prev_free_chunk_offset != 0 {
            let prev_chunk_size = Self::chunk_size(page, prev_free_chunk_offset);
            if prev_free_chunk_offset + Self::header_size() + prev_chunk_size == offset {
                merge_prev = true;
            };
        };
        let mut merge_next = false;
        if current_chunk_offset != 0 {
            if offset + Self::header_size() + Self::chunk_size(page, offset) == current_chunk_offset {
                merge_next = true;
            };
        };
        // merge prev and next if possible
        if merge_prev && !merge_next {
            let next_free_chunk = Self::next_free_chunk(page, prev_free_chunk_offset);
            let prev_free_chunk = Self::prev_free_chunk(page, prev_free_chunk_offset);
            let new_chunk_offset = prev_free_chunk_offset;
            let new_chunk_size = 
                Self::chunk_size(page, prev_free_chunk_offset) +
                Self::header_size() +
                Self::chunk_size(page, offset);
            Self::set_chunk_size_to_free(page, new_chunk_offset, new_chunk_size);
            Self::set_next_free_chunk(page, new_chunk_offset, next_free_chunk);
            Self::set_prev_free_chunk(page, offset, prev_free_chunk);
            // update prev next pointer
            if prev_free_chunk != 0 {
                HeapChunk::set_next_free_chunk(page, prev_free_chunk, new_chunk_offset);
            } else {
                // update first free chunk pointer in page header
                HeapPage::set_first_free_chunk_offset(page, new_chunk_offset);
            }
            // update next prev pointer
            if next_free_chunk != 0 {
                HeapChunk::set_prev_free_chunk(page, next_free_chunk, new_chunk_offset);
            }
        };
        if !merge_prev && merge_next {
            let next_free_chunk = Self::next_free_chunk(page, current_chunk_offset);
            let prev_free_chunk = Self::prev_free_chunk(page, current_chunk_offset);
            let new_chunk_offset = offset;
            let new_chunk_size = 
                Self::chunk_size(page, offset) +
                Self::header_size() +
                Self::chunk_size(page, current_chunk_offset);
            Self::set_chunk_size_to_free(page, new_chunk_offset, new_chunk_size);
            Self::set_next_free_chunk(page, new_chunk_offset, next_free_chunk);
            Self::set_prev_free_chunk(page, new_chunk_offset, prev_free_chunk);
            // update prev next pointer
            if prev_free_chunk != 0 {
                HeapChunk::set_next_free_chunk(page, prev_free_chunk, new_chunk_offset);
            } else {
                // update first free chunk pointer in page header
                HeapPage::set_first_free_chunk_offset(page, new_chunk_offset);
            }
            // update next prev pointer
            if next_free_chunk != 0 {
                HeapChunk::set_prev_free_chunk(page, next_free_chunk, new_chunk_offset);
            }
        };
        if merge_prev && merge_next {
            let next_free_chunk = Self::next_free_chunk(page, current_chunk_offset);
            let prev_free_chunk = Self::prev_free_chunk(page, prev_free_chunk_offset);
            let new_chunk_offset = prev_free_chunk_offset;
            let new_chunk_size = 
                Self::chunk_size(page, prev_free_chunk_offset) +
                Self::header_size() +
                Self::chunk_size(page, offset) +
                Self::header_size() +
                Self::chunk_size(page, current_chunk_offset);
            Self::set_chunk_size_to_free(page, new_chunk_offset, new_chunk_size);
            Self::set_next_free_chunk(page, new_chunk_offset, next_free_chunk);
            Self::set_prev_free_chunk(page, new_chunk_offset, prev_free_chunk);
            // update prev next pointer
            if prev_free_chunk != 0 {
                HeapChunk::set_next_free_chunk(page, prev_free_chunk, new_chunk_offset);
            } else {
                // update first free chunk pointer in page header
                HeapPage::set_first_free_chunk_offset(page, new_chunk_offset);
            }
            // update next prev pointer
            if next_free_chunk != 0 {
                HeapChunk::set_prev_free_chunk(page, next_free_chunk, new_chunk_offset);
            }
        };
        // if no merge happened, just insert into the free list
        if !merge_prev && !merge_next {
            // insert between prev_free_chunk_offset and current_chunk_offset
            let new_chunk_offset = offset;
            Self::set_next_free_chunk(page, new_chunk_offset, current_chunk_offset);
            Self::set_prev_free_chunk(page, new_chunk_offset, prev_free_chunk_offset);
            // update prev next pointer
            if prev_free_chunk_offset != 0 {
                HeapChunk::set_next_free_chunk(page, prev_free_chunk_offset, new_chunk_offset);
            } else {
                // update first free chunk pointer in page header
                HeapPage::set_first_free_chunk_offset(page, new_chunk_offset);
            }
            // update next prev pointer
            if current_chunk_offset != 0 {
                HeapChunk::set_prev_free_chunk(page, current_chunk_offset, new_chunk_offset);
            }
        };
        Ok(())
    }
}


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
            self.set_first_free_entry_page(page_idx, storage, 0)?; // tnx_id 0 for system operations
        } else {
            let mut prev_page_data = storage.read(previous_page)?;
            EntryPage::set_next_free_page(&mut prev_page_data, page_idx);
            storage.write(0, previous_page, &prev_page_data)?; // tnx_id 0 for system operations
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
        storage.free_page(tnx_id, page_idx)?;
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
    use crate::db::storage_engine::wal::WAL;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn setup_storage(db_path: &str) -> ConsistStorageEngine {
        let wal_path = Path::new(crate::config::DB_DIR).join("wal.log");
        if Path::new(db_path).exists() { fs::remove_file(db_path).unwrap(); }
        
        // Ensure data directory exists
        if !Path::new(crate::config::DB_DIR).exists() {
            fs::create_dir_all(crate::config::DB_DIR).unwrap();
        }

        let mut noop_append = |_| Ok(0u64);
        let mut noop_max = |_| Ok(0u64);
        let mut noop_write = |_, _, _| Ok(());
        let mut noop_update = |_, _, _, _, _| Ok(());
        WAL::recovery(&mut noop_write, &mut noop_update, &mut noop_append, &mut |_| Ok(()), &mut noop_max).unwrap();

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
        let db_path = "./data/test_entry.db";
        let mut storage = setup_storage(db_path);
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

        cleanup(db_path);
    }

    #[test]
    fn test_heap_alloc_free() {
        let _guard = TEST_LOCK.lock().unwrap();
        let db_path = "./data/test_heap.db";
        let mut storage = setup_storage(db_path);
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

        cleanup(db_path);
    }

    #[test]
    fn test_heap_merge() {
        let _guard = TEST_LOCK.lock().unwrap();
        let db_path = "./data/test_merge.db";
        let mut storage = setup_storage(db_path);
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

        cleanup(db_path);
    }
}
