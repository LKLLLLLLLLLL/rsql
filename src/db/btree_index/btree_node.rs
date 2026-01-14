use super::super::data_item;
use super::super::storage;
use super::super::errors::RsqlResult;

#[derive(Clone)]
pub struct IndexItem {
    pub key: data_item::DataItem, // not allowed varchar type
    pub child_page_num: u64,
}

#[derive(Clone)]
pub struct LeafItem {
    pub key: data_item::DataItem, // not allowed varchar type
    pub child_page_num: u64,
    pub page_offset: u64,
}

#[derive(Clone)]
pub enum BTreeNode {
    Internal {
        items: Vec<IndexItem>,
        next_page_num: u64, // 0 indicates no next page
    },
    Leaf {
        items: Vec<LeafItem>,
        next_page_num: u64, // 0 indicates no next page
    },
}

impl BTreeNode {
    pub fn is_leaf(&self) -> bool {
        matches!(self, BTreeNode::Leaf { .. })
    }
    /// The tree node bytes structure in page:
    /// [node_length(8)][node_type(1)][items...][next_page_num(8)]
    pub fn to_page(
        &self, 
        page: &mut storage::Page
    ) -> RsqlResult<()> {
        let max_size = 4 * 1024; // For debug
        let buf = match self {
            BTreeNode::Internal { items , next_page_num } => {
                // Serialize internal node
                let mut buf: Vec<u8> = vec![];
                buf.extend(vec![0; 8]); // Placeholder for node length
                buf.push(0); // Internal node marker
                for item in items {
                    let (key_bytes, body_bytes) = item.key.to_bytes()?;
                    if let Some(_) = body_bytes {
                        panic!("BTreeNode::to_page: key in internal node should not have body. Maybe a varchar is wrongly used as key.");
                    }
                    buf.extend_from_slice(&key_bytes);
                    buf.extend_from_slice(&item.child_page_num.to_le_bytes());
                }
                buf.extend_from_slice(&next_page_num.to_le_bytes());
                if buf.len() > max_size {
                    panic!("BTreeNode::to_page: node size exceeds max size");
                }
                let node_length = buf.len() as u64;
                buf[0..8].copy_from_slice(&node_length.to_le_bytes());
                buf
            },
            BTreeNode::Leaf { items, next_page_num } => {
                // Serialize leaf node
                let mut buf: Vec<u8> = vec![];
                buf.extend(vec![0; 8]); // Placeholder for node length
                buf.push(1); // Leaf node marker
                for item in items {
                    let (key_bytes, body_bytes) = item.key.to_bytes()?;
                    if let Some(_) = body_bytes {
                        panic!("BTreeNode::to_page: key in leaf node should not have body. Maybe a varchar is wrongly used as key.");
                    }
                    buf.extend_from_slice(&key_bytes);
                    buf.extend_from_slice(&item.child_page_num.to_le_bytes());
                    buf.extend_from_slice(&item.page_offset.to_le_bytes());
                }
                buf.extend_from_slice(&next_page_num.to_le_bytes());
                if buf.len() > max_size {
                    panic!("BTreeNode::to_page: node size exceeds max size");
                }
                let node_length = buf.len() as u64;
                buf[0..8].copy_from_slice(&node_length.to_le_bytes());
                buf
            }
        };
        page.data[..buf.len()].copy_from_slice(&buf);
        Ok(())
    }
    pub fn from_page(page: &storage::Page) -> RsqlResult<Self> {
        let data = &page.data;
        let node_length = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
        let node_type = data[8];
        let next_page_num = u64::from_le_bytes(data[node_length-8..node_length].try_into().unwrap());
        let mut offset = 9;
        if node_type == 0 {
            // Internal node
            let mut items: Vec<IndexItem> = vec![];
            while offset < node_length - 8 {
                let data_item = data_item::DataItem::from_bytes(&data[offset..], None)?;
                offset += data_item.size();
                let child_page_num = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
                offset += 8;
                items.push(IndexItem { key: data_item, child_page_num});
            }
            Ok(BTreeNode::Internal { items, next_page_num })
        } else if node_type == 1 {
            // Leaf node
            let mut items: Vec<LeafItem> = vec![];
            while offset < node_length - 8 {
                let data_item = data_item::DataItem::from_bytes(&data[offset..], None)?;
                offset += data_item.size();
                let child_page_num = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
                offset += 8;
                let page_offset = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
                offset += 8;
                items.push(LeafItem { key: data_item, child_page_num, page_offset });
            }
            Ok(BTreeNode::Leaf { items, next_page_num })
        } else {
            panic!("BTreeNode::from_page: invalid node type");
        }
    }
}
