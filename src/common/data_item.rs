use core::panic;
use std::mem::size_of;
use std::cmp::Ordering;

use crate::common::{RsqlError, RsqlResult};
use crate::catalog::table_schema;

/// Data item representation in one block in table.
#[derive(Debug, PartialEq,Clone)]
pub enum DataItem {
    Integer(i64),
    Float(f64),
    Chars {len: u64, value: String}, // Fixed length, the len is in bytes
    VarChar {head: VarCharHead, value: String}, // Variable length
    Bool(bool),
    // Nulls for fixed width support
    NullInt,
    NullFloat,
    NullChars {len: u64},
    NullVarChar,
    NullBool,
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct VarCharHead {
    pub max_len: u64,
    pub len: u64,
    pub page_ptr: Option<u64>,
}

impl DataItem {
    /// Return the size in bytes of the data item.
    /// If the data item organized in pointer, return the size of its head.
    pub fn size(&self) -> usize {
        match self {
            DataItem::Integer(_) | DataItem::NullInt => 1 + 8,
            DataItem::Float(_) | DataItem::NullFloat => 1 + 8,
            DataItem::Chars { len, .. } | DataItem::NullChars { len } => 1 + 8 + *len as usize,
            DataItem::VarChar { .. } | DataItem::NullVarChar => 1 + size_of::<VarCharHead>(),
            DataItem::Bool(_) | DataItem::NullBool => 1 + 1,
        }
    }
    pub fn cal_size_from_coltype(col_type: &table_schema::ColType) -> usize {
        match col_type {
            table_schema::ColType::Integer => 1 + 8,
            table_schema::ColType::Float => 1 + 8,
            table_schema::ColType::Chars(len) => 1 + 8 + *len as usize,
            table_schema::ColType::VarChar(_) => 1 + size_of::<VarCharHead>(),
            table_schema::ColType::Bool => 1 + 1,
        }
    }
    fn tag_to_byte(&self) -> u8 {
        match self {
            DataItem::Integer(_) => 1,
            DataItem::Float(_) => 2,
            DataItem::Chars {..} => 3,
            DataItem::VarChar {..}=> 4,
            DataItem::Bool(_) => 5,
            DataItem::NullInt => 6,
            DataItem::NullFloat => 7,
            DataItem::NullChars {..} => 8,
            DataItem::NullVarChar => 9,
            DataItem::NullBool => 10,
        }
    }
    pub fn to_bytes(&self) -> RsqlResult<(Vec<u8>, Option<Vec<u8>>)> {
        // the bytes include [data type(1 byte), data/data_head]
        match self {
            DataItem::Integer(v) => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.extend_from_slice(&v.to_le_bytes());
                Ok((bytes, None))
            },
            DataItem::Float(v) => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.extend_from_slice(&v.to_le_bytes());
                Ok((bytes, None))
            },
            DataItem::Chars {len, value} => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.extend_from_slice(&len.to_le_bytes());
                if value.len() > *len as usize {
                    return Err(RsqlError::Unknown("Length of Chars does not match the actual value length".to_string()));
                }
                // fill padding with zeros if necessary
                bytes.extend_from_slice(value.as_bytes());
                bytes.resize(1 + 8 + *len as usize, 0);
                Ok((bytes, None))
            },
            DataItem::VarChar {head, value} => {
                if value.len() > head.max_len as usize {
                    return Err(RsqlError::Unknown("Value length exceeds maximum length for VarChar".to_string()));
                };
                if value.len() as u64 != head.len {
                    return Err(RsqlError::Unknown("Length of VarChar does not match the actual value length".to_string()));
                };
                if let None = head.page_ptr {
                    return Err(RsqlError::Unknown("VarChar with None pointer cannot be serialized".to_string()));
                };
                if let Some(0) = head.page_ptr {
                    return Err(RsqlError::Unknown("VarChar head page pointer cannot be zero".to_string()));
                };
                let ptr = head.page_ptr.unwrap();
                let mut head_bytes = vec![self.tag_to_byte()];
                head_bytes.extend_from_slice(&head.max_len.to_le_bytes());
                head_bytes.extend_from_slice(&head.len.to_le_bytes());
                head_bytes.extend_from_slice(&ptr.to_le_bytes());
                let body_bytes = value.as_bytes().to_vec();
                Ok((head_bytes, Some(body_bytes)))
            },
            DataItem::Bool(v) => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.push(if *v {1} else {0});
                Ok((bytes, None))
            },
            // Nulls
            DataItem::NullInt => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.extend_from_slice(&[0u8; 8]);
                Ok((bytes, None))
            },
            DataItem::NullFloat => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.extend_from_slice(&[0u8; 8]);
                Ok((bytes, None))
            },
            DataItem::NullChars {len} => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend(vec![0u8; *len as usize]);
                Ok((bytes, None))
            },
            DataItem::NullVarChar => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.extend_from_slice(&[0u8; 24]); // VarCharHead is 24 bytes (8+8+8)
                Ok((bytes, None))
            },
            DataItem::NullBool => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.push(0);
                Ok((bytes, None))
            },
        }
    }
    pub fn from_bytes(head_bytes: &[u8], body_bytes: Option<&[u8]>) -> RsqlResult<Self> {
        if head_bytes.len() == 0 {
            return Err(RsqlError::Unknown("Cannot parse DataItem from empty bytes".to_string()));
        }
        let tag_byte = head_bytes[0];
        match tag_byte {
            1 => {
                if head_bytes.len() < 9 {
                    return Err(RsqlError::Unknown("Invalid bytes length for Inteager".to_string() + &head_bytes.len().to_string()));
                }
                let mut int_bytes = [0u8; 8];
                int_bytes.copy_from_slice(&head_bytes[1..9]);
                Ok(DataItem::Integer(i64::from_le_bytes(int_bytes)))
            },
            2 => {
                if head_bytes.len() < 9 {
                    return Err(RsqlError::Unknown("Invalid bytes length for Float".to_string() + &head_bytes.len().to_string()));
                }
                let mut float_bytes = [0u8; 8];
                float_bytes.copy_from_slice(&head_bytes[1..9]);
                Ok(DataItem::Float(f64::from_le_bytes(float_bytes)))
            },
            3 => {
                if head_bytes.len() < 1 + 8 {
                    return Err(RsqlError::Unknown("Invalid bytes length for Chars".to_string()));
                }
                let mut len_bytes = [0u8; 8];
                len_bytes.copy_from_slice(&head_bytes[1..9]);
                let len = u64::from_le_bytes(len_bytes);
                let expected_len = 1 + 8 + len as usize;
                if head_bytes.len() < expected_len {
                    return Err(RsqlError::Unknown("Invalid bytes length for Chars value".to_string()));
                }
                // truncate trailing zeros
                let value = String::from_utf8(head_bytes[9..expected_len].to_vec())
                    .map_err(|e| RsqlError::ParserError(e.to_string()))?
                    .trim_matches('\0')
                    .to_string();
                Ok(DataItem::Chars {len, value})
            },
            4 => {
                // head layout: [tag(1)] [max_len(8)] [len(8)] [page_ptr(8)] => total 25
                if head_bytes.len() < 25 {
                    return Err(RsqlError::Unknown("Invalid bytes length for VarChar head".to_string() + &head_bytes.len().to_string()));
                }
                let mut max_len_bytes = [0u8; 8];
                max_len_bytes.copy_from_slice(&head_bytes[1..9]);
                let mut len_bytes = [0u8; 8];
                len_bytes.copy_from_slice(&head_bytes[9..17]);
                let mut ptr_bytes = [0u8; 8];
                ptr_bytes.copy_from_slice(&head_bytes[17..25]);
                let max_len = u64::from_le_bytes(max_len_bytes);
                let len = u64::from_le_bytes(len_bytes);
                let page_ptr = u64::from_le_bytes(ptr_bytes);
                let value = match body_bytes {
                    Some(b) => {
                        if b.len() != len as usize {
                            return Err(RsqlError::Unknown("VarChar body length does not match head.len".to_string()));
                        }
                        String::from_utf8(b.to_vec()).map_err(|e| RsqlError::ParserError(e.to_string()))?
                    },
                    None => String::new(), // empty string if body not provided
                };
                Ok(DataItem::VarChar {head: VarCharHead {max_len, len, page_ptr: Some(page_ptr)}, value})
            },
            5 => {
                if head_bytes.len() < 2 {
                    return Err(RsqlError::Unknown("Invalid bytes length for Bool".to_string() + &head_bytes.len().to_string()));
                }
                let value = match head_bytes[1] {
                    0 => false,
                    1 => true,
                    _ => return Err(RsqlError::Unknown("Invalid byte for Bool data".to_string())),
                };
                Ok(DataItem::Bool(value))
            },
            // Nulls
            6 => { // NullInt
                if head_bytes.len() < 9 {
                     return Err(RsqlError::Unknown("Invalid bytes length for NullInt".to_string()));
                }
                Ok(DataItem::NullInt)
            },
            7 => { // NullFloat
                if head_bytes.len() < 9 {
                     return Err(RsqlError::Unknown("Invalid bytes length for NullFloat".to_string()));
                }
                Ok(DataItem::NullFloat)
            },
            8 => { // NullChars
                if head_bytes.len() < 9 {
                     return Err(RsqlError::Unknown("Invalid bytes length for NullChars header".to_string()));
                }
                let mut len_bytes = [0u8; 8];
                len_bytes.copy_from_slice(&head_bytes[1..9]);
                let len = u64::from_le_bytes(len_bytes);
                
                let expected_len = 1 + 8 + len as usize;
                if head_bytes.len() < expected_len {
                    return Err(RsqlError::Unknown("Invalid bytes length for NullChars padding".to_string()));
                }
                Ok(DataItem::NullChars { len })
            },
            9 => { // NullVarChar
                if head_bytes.len() < 25 {
                     return Err(RsqlError::Unknown("Invalid bytes length for NullVarChar".to_string()));
                }
                Ok(DataItem::NullVarChar)
            },
            10 => { // NullBool
                if head_bytes.len() < 2 {
                     return Err(RsqlError::Unknown("Invalid bytes length for NullBool".to_string()));
                }
                Ok(DataItem::NullBool)
            },
            _ => Err(RsqlError::Unknown("Unknown data type tag".to_string())),
        }
    }
}

impl PartialOrd for DataItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // check groups first.
        let same_group = match (self, other) {
            (DataItem::Integer(_) | DataItem::NullInt, DataItem::Integer(_) | DataItem::NullInt) => true,
            (DataItem::Float(_) | DataItem::NullFloat, DataItem::Float(_) | DataItem::NullFloat) => true,
            (DataItem::Chars{..} | DataItem::NullChars{..}, DataItem::Chars{..} | DataItem::NullChars{..}) => true,
            (DataItem::VarChar{..} | DataItem::NullVarChar, DataItem::VarChar{..} | DataItem::NullVarChar) => true,
            (DataItem::Bool(_) | DataItem::NullBool, DataItem::Bool(_) | DataItem::NullBool) => true,
            _ => panic!("Comparing different data item types: {:?} vs {:?}", self, other),
        };

        if !same_group {
             // If different groups, fallback to tag comparison (consistent but arbitrary order between types)
             return None;
        }

        // Same group comparison
        match (self, other) {
            // Int Group
            (DataItem::NullInt, DataItem::Integer(_)) => Some(Ordering::Less),
            (DataItem::Integer(_), DataItem::NullInt) => Some(Ordering::Greater),
            (DataItem::NullInt, DataItem::NullInt) => Some(Ordering::Equal),
            (DataItem::Integer(v1), DataItem::Integer(v2)) => Some(v1.cmp(v2)),

            // Float Group
            (DataItem::NullFloat, DataItem::Float(_)) => Some(Ordering::Less),
            (DataItem::Float(_), DataItem::NullFloat) => Some(Ordering::Greater),
            (DataItem::NullFloat, DataItem::NullFloat) => Some(Ordering::Equal),
            (DataItem::Float(v1), DataItem::Float(v2)) => v1.partial_cmp(v2),

            // Chars Group
            (DataItem::NullChars{..}, DataItem::Chars{..}) => Some(Ordering::Less),
            (DataItem::Chars{..}, DataItem::NullChars{..}) => Some(Ordering::Greater),
            (DataItem::NullChars{..}, DataItem::NullChars{..}) => Some(Ordering::Equal),
            (DataItem::Chars {value: v1, ..}, DataItem::Chars {value: v2, ..}) => Some(v1.cmp(v2)),

            // VarChar Group
            (DataItem::NullVarChar, DataItem::VarChar{..}) => Some(Ordering::Less),
            (DataItem::VarChar{..}, DataItem::NullVarChar) => Some(Ordering::Greater),
            (DataItem::NullVarChar, DataItem::NullVarChar) => Some(Ordering::Equal),
            (DataItem::VarChar {value: v1, ..}, DataItem::VarChar {value: v2, ..}) => Some(v1.cmp(v2)),

            // Bool Group
            (DataItem::NullBool, DataItem::Bool(_)) => Some(Ordering::Less),
            (DataItem::Bool(_), DataItem::NullBool) => Some(Ordering::Greater),
            (DataItem::NullBool, DataItem::NullBool) => Some(Ordering::Equal),
            (DataItem::Bool(b1), DataItem::Bool(b2)) => Some(b1.cmp(b2)),

            _ => panic!("DataItem compare should be covered by same_group check"),
        }
    }
}
