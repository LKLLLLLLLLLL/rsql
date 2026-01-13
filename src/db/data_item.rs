use super::errors::{RsqlError, RsqlResult};

/// Data item representation in one block in table.
pub enum DataItem {
    Inteager(i64),
    Float(f64),
    Chars {len: i64, value: String}, // Fixed length, the len is in bytes
    VarChar {head: VarCharHead, value: String}, // Variable length
    Bool(bool),
    Null,
}

pub struct VarCharHead {
    length: u64,
    page_ptr: u64,
}

impl DataItem {
    /// Return the size in bytes of the data item.
    /// If the data item organized in pointer, return the size of its head.
    pub fn size(&self) -> usize {
        match self {
            DataItem::Inteager(_) => 1 + 8,
            DataItem::Float(_) => 1 + 8,
            DataItem::Chars {len, ..} => 1 + *len as usize,
            DataItem::VarChar {..}=> 1 + size_of::<VarCharHead>(),
            DataItem::Bool(_) => 1 + 1,
            DataItem::Null => 1 + 0,
        }
    }
    fn tag_to_byte(&self) -> u8 {
        match self {
            DataItem::Inteager(_) => 1,
            DataItem::Float(_) => 2,
            DataItem::Chars {..} => 3,
            DataItem::VarChar {..}=> 4,
            DataItem::Bool(_) => 5,
            DataItem::Null => 0,
        }
    }
    pub fn to_bytes(&self) -> (Vec<u8>, Option<Vec<u8>>) {
        // the bytes include [data type(1 byte), data/data_head]
        match self {
            DataItem::Inteager(v) => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.extend_from_slice(&v.to_le_bytes());
                (bytes, None)
            },
            DataItem::Float(v) => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.extend_from_slice(&v.to_le_bytes());
                (bytes, None)
            },
            DataItem::Chars {len: _, value} => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.extend_from_slice(value.as_bytes());
                (bytes, None)
            },
            DataItem::VarChar {head, value} => {
                let mut head_bytes = vec![self.tag_to_byte()];
                head_bytes.extend_from_slice(&head.length.to_le_bytes());
                head_bytes.extend_from_slice(&head.page_ptr.to_le_bytes());
                let body_bytes = value.as_bytes().to_vec();
                (head_bytes, Some(body_bytes))
            },
            DataItem::Bool(v) => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.push(if *v {1} else {0});
                (bytes, None)
            },
            DataItem::Null => {
                (vec![self.tag_to_byte()], None)
            },
        }
    }
    pub fn from_bytes(head_bytes: &[u8], body_bytes: Option<&[u8]>) -> RsqlResult<Self> {
        if head_bytes.len() == 0 {
            return Err(RsqlError::Unknown("Cannot parse DataItem from empty bytes".to_string()));
        }
        let tag_byte = head_bytes[0];
        match tag_byte {
            0 => Ok(DataItem::Null),
            1 => {
                if head_bytes.len() != 9 {
                    return Err(RsqlError::Unknown("Invalid bytes length for Inteager".to_string() + &head_bytes.len().to_string()));
                }
                let mut int_bytes = [0u8; 8];
                int_bytes.copy_from_slice(&head_bytes[1..9]);
                Ok(DataItem::Inteager(i64::from_le_bytes(int_bytes)))
            },
            2 => {
                if head_bytes.len() != 9 {
                    return Err(RsqlError::Unknown("Invalid bytes length for Float".to_string() + &head_bytes.len().to_string()));
                }
                let mut float_bytes = [0u8; 8];
                float_bytes.copy_from_slice(&head_bytes[1..9]);
                Ok(DataItem::Float(f64::from_le_bytes(float_bytes)))
            },
            3 => {
                let value = String::from_utf8(head_bytes[1..].to_vec()).map_err(|e| RsqlError::ParserError(e.to_string()))?;
                Ok(DataItem::Chars {len: (head_bytes.len() - 1) as i64, value})
            },
            4 => {
                if head_bytes.len() != 17 {
                    return Err(RsqlError::Unknown("Invalid bytes length for VarChar head".to_string() + &head_bytes.len().to_string()));
                }
                let mut len_bytes = [0u8; 8];
                len_bytes.copy_from_slice(&head_bytes[1..9]);
                let mut ptr_bytes = [0u8; 8];
                ptr_bytes.copy_from_slice(&head_bytes[9..17]);
                let length = u64::from_le_bytes(len_bytes);
                let page_ptr = u64::from_le_bytes(ptr_bytes);
                let value = match body_bytes {
                    Some(b) => String::from_utf8(b.to_vec()).map_err(|e| RsqlError::ParserError(e.to_string()))?,
                    None => return Err(RsqlError::Unknown("Missing body bytes for VarChar data".to_string())),
                };
                Ok(DataItem::VarChar {head: VarCharHead {length, page_ptr}, value})
            },
            5 => {
                if head_bytes.len() != 2 {
                    return Err(RsqlError::Unknown("Invalid bytes length for Bool".to_string() + &head_bytes.len().to_string()));
                }
                let value = match head_bytes[1] {
                    0 => false,
                    1 => true,
                    _ => return Err(RsqlError::Unknown("Invalid byte for Bool data".to_string())),
                };
                Ok(DataItem::Bool(value))
            },
            _ => Err(RsqlError::Unknown("Unknown data type tag".to_string())),
        }
    }
}
