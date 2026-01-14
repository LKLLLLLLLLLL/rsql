use std::iter;
use std::mem;
use std::sync::Mutex;
use std::sync::Arc;

use super::super::storage;
use super::super::data_item;
use super::super::errors::RsqlResult;
use super::btree_node;

/// Iterator to find entries in range [start_index, end_index)
/// Return: (page_num, page_offset)
pub struct RangeIterator<'a, F>
where
    F: Fn(u64) -> RsqlResult<storage::Page> + 'a,
{
    current_index: data_item::DataItem,
    end_index: data_item::DataItem,
    get_page: &'a F,
    current_leaf_node: btree_node::BTreeNode,
    current_item_index: usize,
}

impl<'a, F> Iterator for RangeIterator<'a, F>
where F: Fn(u64) -> RsqlResult<storage::Page> + 'a,
{
    type Item = RsqlResult<(u64, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.current_leaf_node {
            btree_node::BTreeNode::Leaf { items, next_page_num } => {
                // if current_item_index out of bound, load next page
                if self.current_item_index >= items.len() {
                    if *next_page_num == 0 { return None; }
                    
                    let next_page = (self.get_page)(*next_page_num).ok()?;
                    self.current_leaf_node = btree_node::BTreeNode::from_page(&next_page).ok()?;
                    self.current_item_index = 0;
                    return self.next(); // recursive call to get next item
                }

                let item = &items[self.current_item_index];
                
                // if item.key >= end_index, stop iteration
                if item.key >= self.end_index {
                    return None;
                }

                self.current_item_index += 1;
                // return current item
                Some(Ok((item.child_page_num, item.page_offset)))
            }
            _ => None,
        }
    }
}

/// The B-Tree index of sql table in database.
struct BTreeIndex {
    root: btree_node::BTreeNode,
}

impl BTreeIndex {
    pub fn new(
        new_page: impl FnOnce() -> RsqlResult<storage::Page>,
    ) -> RsqlResult<Self> {
        let mut page = new_page()?;
        let root_node = btree_node::BTreeNode::Leaf { items: vec![], next_page_num: 0 };
        root_node.to_page(&mut page)?;
        Ok(Self { root: root_node})
    }
    pub fn from(
        root_page: &storage::Page,
    ) -> RsqlResult<Self> {
        let root_node = btree_node::BTreeNode::from_page(root_page)?;
        Ok(Self { root: root_node})
    }
    /// Helper function to find leaf node containing the index_item
    fn find_leaf_pos<F>(
        &self,
        index: &data_item::DataItem,
        get_page: &F,
    ) -> RsqlResult<(btree_node::BTreeNode, usize)> 
    where F: Fn(u64) -> RsqlResult<storage::Page> {
        let mut current_node = self.root.clone();
        loop {
            match current_node {
                btree_node::BTreeNode::Leaf { ref items, .. } => {
                    // Found the leaf node by binary search
                    let pos = items.iter().position(|it| &it.key >= index).unwrap_or(items.len());
                    return Ok((current_node, pos));
                }
                btree_node::BTreeNode::Internal { ref items, .. } => {
                    let mut page_num = items.last().unwrap().child_page_num;
                    for item in items {
                        if index < &item.key {
                            page_num = item.child_page_num;
                            break;
                        }
                    }
                    let child_page = get_page(page_num)?;
                    current_node = btree_node::BTreeNode::from_page(&child_page)?;
                }
            }
        }
    }
    pub fn find_entry<F>(
        &self,
        index: data_item::DataItem,
        get_page: &F,
    ) -> RsqlResult<Option<u64>> 
    where F: Fn(u64) -> RsqlResult<storage::Page> {
        let (node, pos) = self.find_leaf_pos(&index, get_page)?;
        if let btree_node::BTreeNode::Leaf { items, .. } = node {
            if pos < items.len() && items[pos].key == index {
                return Ok(Some(items[pos].page_offset));
            }
        }
        Ok(None)
    }
    pub fn find_range_entry<'a>(
        &self,
        start_index: data_item::DataItem,
        end_index: data_item::DataItem,
        get_page: &'a (impl Fn(u64) -> RsqlResult<storage::Page> + 'a),
    ) -> RsqlResult<RangeIterator<'a, impl Fn(u64) -> RsqlResult<storage::Page> + 'a>> { 
        let (leaf_node, start_pos) = self.find_leaf_pos(&start_index, get_page)?;
        
        Ok(RangeIterator {
            current_index: start_index,
            end_index,
            get_page,
            current_leaf_node: leaf_node,
            current_item_index: start_pos,
        })
    }
}
