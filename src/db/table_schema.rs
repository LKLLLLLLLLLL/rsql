use super::common::{RsqlError, RsqlResult};
use super::data_item::DataItem;
use super::super::config::{MAX_VARCHAR_SIZE, MAX_COL_NAME_SIZE};

#[derive(Clone)]
pub enum ColType {
    Integer,
    Float,
    Chars(usize), // (fixed size)
    VarChar(usize), // (max size)
    Bool
}

#[derive(Clone)]
pub struct TableColumn {
    pub name: String, // fix to MAX_COL_NAME_SIZE bytes
    pub data_type: ColType,
    pub pk: bool,
    pub nullable: bool,
    pub index: bool,
    pub unique: bool,
}

#[derive(Clone)]
pub struct TableSchema {
    columns: Vec<TableColumn>,
}

impl TableSchema {
    /// Bytes structure in disk:
    /// [schema_length: 8bytes][col1_name: MAX_COL_NAME_SIZE bytes][col1_type: 1byte][col1_extra: 8bytes][col1_pk:1byte][col1_nullable:1byte][col1_unique:1byte][col1_index:1byte]...
    /// each column takes 76bytes
    fn from_bytes(bytes: &[u8]) -> RsqlResult<(Self, u64)> {
        let mut offset = 0;
        let schema_length_bytes = &bytes[offset..offset+8];
        let schema_length = u64::from_le_bytes(schema_length_bytes.try_into().unwrap());
        offset += 8;
        let mut columns = vec![];
        while offset + 76 <= schema_length as usize {
            let name_bytes = &bytes[offset..offset+MAX_COL_NAME_SIZE];
            let name = String::from_utf8(name_bytes.iter().cloned().take_while(|&b| b != 0).collect())
                .map_err(|_| RsqlError::StorageError("Invalid column name".to_string()))?;
            offset += MAX_COL_NAME_SIZE;
            let col_type_byte = bytes[offset];
            offset += 1;
            let extra_bytes = &bytes[offset..offset+8];
            let extra = u64::from_le_bytes(extra_bytes.try_into().unwrap());
            offset += 8;
            let pk = bytes[offset] != 0;
            offset += 1;
            let nullable = bytes[offset] != 0;
            offset += 1;
            let unique = bytes[offset] != 0;
            offset += 1;
            let index = bytes[offset] != 0;
            offset += 1;
            let data_type = match col_type_byte {
                0 => ColType::Integer,
                1 => ColType::Float,
                2 => ColType::Chars(extra as usize),
                3 => ColType::VarChar(extra as usize),
                4 => ColType::Bool,
                _ => return Err(RsqlError::StorageError("Invalid column type".to_string())),
            };
            columns.push(TableColumn {
                name,
                data_type,
                pk,
                nullable,
                unique,
                index,
            });
        }
        Ok((TableSchema { columns }, schema_length))
    }
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 8];
        for col in &self.columns {
            if col.name.len() > MAX_COL_NAME_SIZE {
                panic!("Column name {} exceeds max length {}", col.name, MAX_COL_NAME_SIZE);
            }
            let mut name_bytes = [0u8; MAX_COL_NAME_SIZE];
            name_bytes[..col.name.len()].copy_from_slice(col.name.as_bytes());
            buf.extend_from_slice(&name_bytes);
            match col.data_type {
                ColType::Integer => {
                    buf.push(0u8);
                    buf.extend_from_slice(&0u64.to_le_bytes());
                }
                ColType::Float => {
                    buf.push(1u8);
                    buf.extend_from_slice(&0u64.to_le_bytes());
                }
                ColType::Chars(size) => {
                    buf.push(2u8);
                    buf.extend_from_slice(&(size as u64).to_le_bytes());
                }
                ColType::VarChar(size) => {
                    buf.push(3u8);
                    buf.extend_from_slice(&(size as u64).to_le_bytes());
                }
                ColType::Bool => {
                    buf.push(4u8);
                    buf.extend_from_slice(&0u64.to_le_bytes());
                }
            }
            buf.push(if col.pk { 1u8 } else { 0u8 });
            buf.push(if col.nullable { 1u8 } else { 0u8 });
            buf.push(if col.unique { 1u8 } else { 0u8 });
            buf.push(if col.index { 1u8 } else { 0u8 });
        }
        // write schema length at the beginning
        let schema_length = buf.len() as u64;
        buf[..8].copy_from_slice(&schema_length.to_le_bytes());
        buf
    }
    pub fn satisfy(&self, data: &Vec<DataItem>) -> RsqlResult<()> {
        // 1. check if data length matches
        if data.len() != self.columns.len() {
            return Err(RsqlError::InvalidInput(
                format!("Data length {} does not match schema length {}", data.len(), self.columns.len())));
        }
        // 2. check nullable
        for (i, col) in self.columns.iter().enumerate() {
            let data_item = &data[i];
            match data_item {
                DataItem::NullInt | DataItem::NullFloat | 
                DataItem::NullVarChar | DataItem::NullBool | 
                DataItem::NullChars { .. } => {
                    return Err(RsqlError::InvalidInput(
                    format!("Null value found for non-nullable column {}", col.name)));
                },
                _ => {},
            }
        }
        // 3. check data type
        for (i, col) in self.columns.iter().enumerate() {
            match col.data_type {
                ColType::Integer => match data[i] {
                    DataItem::Integer(_) => {},
                    _ => return Err(RsqlError::InvalidInput(
                        format!("Expected Integer for column {}, found different type", col.name))),
                },
                ColType::Float => match data[i] {
                    DataItem::Float(_) => {},
                    _ => return Err(RsqlError::InvalidInput(
                        format!("Expected Float for column {}, found different type", col.name))),
                },
                ColType::Chars(size) => match &data[i] {
                    DataItem::Chars{ len, value } => {
                        if *len as usize != size {
                            return Err(RsqlError::InvalidInput(
                                format!("Expected Chars({}) for column {}, found Chars({})", size, col.name, len)));
                        }
                        if value.len() > size {
                            return Err(RsqlError::InvalidInput(
                                format!("Value length {} exceeds size {} for column {}", value.len(), size, col.name)));
                        }
                    },
                    _ => return Err(RsqlError::InvalidInput(
                        format!("Expected Chars({}) for column {}, found different type", size, col.name))),
                },
                ColType::VarChar(size) => match &data[i] {
                    DataItem::VarChar{ head: _, value } => {
                        if value.len() > size {
                            return Err(RsqlError::InvalidInput(
                                format!("Value length {} exceeds max varchar size {} for column {}", value.len(), size, col.name)));
                        }
                    },
                    _ => return Err(RsqlError::InvalidInput(
                        format!("Expected VarChar for column {}, found different type", col.name))),
                },
                ColType::Bool => match data[i] {
                    DataItem::Bool(_) => {},
                    _ => return Err(RsqlError::InvalidInput(
                        format!("Expected Bool for column {}, found different type", col.name))),
                },
            }
        }
        Ok(())
    }
    pub fn new(columns: Vec<TableColumn>) -> RsqlResult<Self> {
        // check if the varchar columns is indexed
        for col in &columns {
            if col.index {
                match col.data_type {
                    ColType::VarChar(_) => {
                        return Err(RsqlError::InvalidInput(format!("VarChar column {} cannot be indexed", col.name)));
                    },
                    _ => {},
                }
            }
        }
        // check if varchar length exceeds max
        for col in &columns {
            match col.data_type {
                ColType::VarChar(size) => {
                    if size > MAX_VARCHAR_SIZE {
                        return Err(RsqlError::InvalidInput(format!("VarChar column {} size {} exceeds max {}", col.name, size, MAX_VARCHAR_SIZE)));
                    }
                },
                _ => {},
            }
        }
        // check if the unique columns are indexed
        for col in &columns {
            if col.unique && !col.index {
                return Err(RsqlError::InvalidInput(format!("Unique column {} must be indexed", col.name)));
            }
        }
        // check if primary key columns are indexed and not null
        for col in &columns {
            if col.pk {
                if !col.index {
                    return Err(RsqlError::InvalidInput(format!("Primary key column {} must be indexed", col.name)));
                }
                if col.nullable {
                    return Err(RsqlError::InvalidInput(format!("Primary key column {} cannot be nullable", col.name)));
                }
            }
        }
        // check if column name length exceeds max
        for col in &columns {
            if col.name.len() > MAX_COL_NAME_SIZE {
                return Err(RsqlError::InvalidInput(format!("Column name {} exceeds max length {}", col.name, MAX_COL_NAME_SIZE)));
            }
        }
        // check if there are duplicate column names
        let mut name_set = std::collections::HashSet::new();
        for col in &columns {
            if name_set.contains(&col.name) {
                return Err(RsqlError::InvalidInput(format!("Duplicate column name {}", col.name)));
            }
            name_set.insert(col.name.clone());
        }
        // check if there are multiple primary key columns
        let pk_count = columns.iter().filter(|col| col.pk).count();
        if pk_count > 1 {
            return Err(RsqlError::InvalidInput("Multiple primary key columns found".to_string()));
        }
        Ok(Self { columns })
    }
    pub fn get_sizes(&self) -> Vec<usize> {
        let mut sizes = vec![];
        for col in &self.columns {
            sizes.push(DataItem::cal_size_from_coltype(&col.data_type));
        };
        sizes
    }
    pub fn get_columns(&self) -> &Vec<TableColumn> {
        &self.columns
    }
}
