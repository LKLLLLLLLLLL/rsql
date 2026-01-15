/// Find the index(in bit) of the first 0 bit in the given bitset slice.
/// If find index exceeds max_index, return None.
pub fn find_first_0_bit(bitset: &[u8], max_index: usize) -> Option<usize> {
    for (byte_idx, byte) in bitset.iter().enumerate() {
        if *byte != 0xFF {
            let inv = !byte;
            let tz = inv.trailing_zeros() as usize;
            if byte_idx * 8 + tz >= max_index {
                return None;
            }
            return Some(byte_idx * 8 + tz);
        }
    }
    None
}