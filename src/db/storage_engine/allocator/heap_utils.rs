use super::super::storage;
use crate::db::errors::RsqlResult;

const MAGIC_NUMBER: u32 = 0x4c515352; // 'RSQL' in little endian hex

/// Helper function set for heap page
/// The heap free space is managed by:
/// [first_free_heap_page_ptr] -> [free_heap_page] -> [free_heap_page] -> ...
/// Each free heap page:
/// - [next_free_page: 8bytes][prev_free_page: 8bytes]
/// - [first_free_chunk_offset: 8bytes]: >= 24
/// - [data...]
pub struct HeapPage;
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
pub struct HeapChunk;
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