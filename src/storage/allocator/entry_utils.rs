use super::super::storage;

/// Helper function set for bitmap
pub struct Bitmap;
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
pub struct EntryPage;
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