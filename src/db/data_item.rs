use std::mem::size_of;
use std::cmp::Ordering;
use std::mem;

use super::errors::{RsqlError, RsqlResult};

/// Data item representation in one block in table.
#[derive(Debug, PartialEq, Clone)]
pub enum DataItem {
    Inteager(i64),
    Float(f64),
    Chars {len: u64, value: String}, // Fixed length, the len is in bytes
    VarChar {head: VarCharHead, value: String}, // Variable length
    Bool(bool),
    Null,
}

#[derive(Debug, PartialEq, Clone)]
pub struct VarCharHead {
    max_len: u64,
    len: u64,
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
    pub fn to_bytes(&self) -> RsqlResult<(Vec<u8>, Option<Vec<u8>>)> {
        // the bytes include [data type(1 byte), data/data_head]
        match self {
            DataItem::Inteager(v) => {
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
                if *len as usize != value.len() {
                    return Err(RsqlError::Unknown("Length of Chars does not match the actual value length".to_string()));
                }
                bytes.extend_from_slice(value.as_bytes());
                Ok((bytes, None))
            },
            DataItem::VarChar {head, value} => {
                if value.len() > head.max_len as usize {
                    return Err(RsqlError::Unknown("Value length exceeds maximum length for VarChar".to_string()));
                }
                if value.len() as u64 != head.len {
                    return Err(RsqlError::Unknown("Length of VarChar does not match the actual value length".to_string()));
                }
                if head.page_ptr == 0 {
                    return Err(RsqlError::Unknown("VarChar head page pointer cannot be zero".to_string()));
                }
                let mut head_bytes = vec![self.tag_to_byte()];
                head_bytes.extend_from_slice(&head.max_len.to_le_bytes());
                head_bytes.extend_from_slice(&head.len.to_le_bytes());
                head_bytes.extend_from_slice(&head.page_ptr.to_le_bytes());
                let body_bytes = value.as_bytes().to_vec();
                Ok((head_bytes, Some(body_bytes)))
            },
            DataItem::Bool(v) => {
                let mut bytes = vec![self.tag_to_byte()];
                bytes.push(if *v {1} else {0});
                Ok((bytes, None))
            },
            DataItem::Null => {
                Ok((vec![self.tag_to_byte()], None))
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
                if head_bytes.len() < 1 + 8 {
                    return Err(RsqlError::Unknown("Invalid bytes length for Chars".to_string()));
                }
                let mut len_bytes = [0u8; 8];
                len_bytes.copy_from_slice(&head_bytes[1..9]);
                let len = u64::from_le_bytes(len_bytes);
                let expected_len = 1 + 8 + len as usize;
                if head_bytes.len() != expected_len {
                    return Err(RsqlError::Unknown("Invalid bytes length for Chars value".to_string()));
                }
                let value = String::from_utf8(head_bytes[9..].to_vec()).map_err(|e| RsqlError::ParserError(e.to_string()))?;
                Ok(DataItem::Chars {len, value})
            },
            4 => {
                // head layout: [tag(1)] [max_len(8)] [len(8)] [page_ptr(8)] => total 25
                if head_bytes.len() != 1 + 8 + 8 + 8 {
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
                    None => return Err(RsqlError::Unknown("Missing body bytes for VarChar data".to_string())),
                };
                Ok(DataItem::VarChar {head: VarCharHead {max_len, len, page_ptr}, value})
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

impl PartialOrd for DataItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if mem::discriminant(self) != mem::discriminant(other) {
            return None;
        }
        match self {
            DataItem::Inteager(v1) => {
                if let DataItem::Inteager(v2) = other {
                    Some(v1.cmp(v2))
                } else {
                    None
                }
            },
            DataItem::Float(v1) => {
                if let DataItem::Float(v2) = other {
                    v1.partial_cmp(v2)
                } else {
                    None
                }
            },
            DataItem::Chars {value: v1, ..} => {
                if let DataItem::Chars {value: v2, ..} = other {
                    Some(v1.cmp(v2))
                } else {
                    None
                }
            },
            DataItem::VarChar {value: v1, ..} => {
                if let DataItem::VarChar {value: v2, ..} = other {
                    Some(v1.cmp(v2))
                } else {
                    None
                }
            },
            DataItem::Bool(b1) => {
                if let DataItem::Bool(b2) = other {
                    Some(b1.cmp(b2))
                } else {
                    None
                }
            },
            DataItem::Null => Some(Ordering::Equal),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inteager_roundtrip() {
        let v = DataItem::Inteager(-123456789);
        let (head, body) = v.to_bytes().unwrap();
        assert!(body.is_none());
        let parsed = DataItem::from_bytes(&head, None).unwrap();
        assert_eq!(v, parsed);
    }

    #[test]
    fn test_float_roundtrip() {
        let v = DataItem::Float(3.14159);
        let (head, body) = v.to_bytes().unwrap();
        assert!(body.is_none());
        let parsed = DataItem::from_bytes(&head, None).unwrap();
        assert_eq!(v, parsed);
    }

    #[test]
    fn test_bool_and_null() {
        let b = DataItem::Bool(true);
        let (hb, bb) = b.to_bytes().unwrap();
        assert!(bb.is_none());
        assert_eq!(DataItem::from_bytes(&hb, None).unwrap(), b);

        let n = DataItem::Null;
        let (hn, nb) = n.to_bytes().unwrap();
        assert!(nb.is_none());
        assert_eq!(DataItem::from_bytes(&hn, None).unwrap(), n);
    }

    #[test]
    fn test_chars_roundtrip() {
        let s = "hello".to_string();
        let d = DataItem::Chars { len: s.len() as u64, value: s.clone() };
        let (head, body) = d.to_bytes().unwrap();
        assert!(body.is_none());
        let parsed = DataItem::from_bytes(&head, None).unwrap();
        assert_eq!(parsed, d);
    }

    #[test]
    fn test_chars_len_mismatch() {
        let d = DataItem::Chars { len: 2, value: "abc".to_string() };
        assert!(d.to_bytes().is_err());
    }

    #[test]
    fn test_varchar_roundtrip() {
        let head = VarCharHead { max_len: 100, len: 3, page_ptr: 1 };
        let val = "hey".to_string();
        let d = DataItem::VarChar { head: head.clone(), value: val.clone() };
        let (hbytes, body_opt) = d.to_bytes().unwrap();
        assert!(body_opt.is_some());
        let body = body_opt.unwrap();
        let parsed = DataItem::from_bytes(&hbytes, Some(&body)).unwrap();
        assert_eq!(parsed, d);
    }

    #[test]
    fn test_varchar_head_page_ptr_zero_error() {
        let head = VarCharHead { max_len: 10, len: 3, page_ptr: 0 };
        let val = "abc".to_string();
        let d = DataItem::VarChar { head, value: val };
        assert!(d.to_bytes().is_err());
    }

    #[test]
    fn test_varchar_body_len_mismatch() {
        let head = VarCharHead { max_len: 10, len: 5, page_ptr: 1 };
        let val = "abc".to_string();
        // build head bytes manually as to_bytes would
        let mut head_bytes = vec![4u8];
        head_bytes.extend_from_slice(&head.max_len.to_le_bytes());
        head_bytes.extend_from_slice(&head.len.to_le_bytes());
        head_bytes.extend_from_slice(&head.page_ptr.to_le_bytes());
        let body = val.as_bytes().to_vec();
        let res = DataItem::from_bytes(&head_bytes, Some(&body));
        assert!(res.is_err());
    }

    #[test]
    fn test_size_values() {
        let i = DataItem::Inteager(1);
        assert_eq!(i.size(), 1 + 8);
        let f = DataItem::Float(1.0);
        assert_eq!(f.size(), 1 + 8);
        let c = DataItem::Chars { len: 4, value: "test".to_string() };
        assert_eq!(c.size(), 1 + 4);
        let vh = VarCharHead { max_len: 100, len: 3, page_ptr: 1 };
        let v = DataItem::VarChar { head: vh.clone(), value: "hey".to_string() };
        assert_eq!(v.size(), 1 + size_of::<VarCharHead>());
    }
}
