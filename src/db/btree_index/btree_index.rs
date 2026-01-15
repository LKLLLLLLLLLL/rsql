use std::sync::{Arc, RwLock};

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
pub struct BTreeIndex {
    root: u64,
}

impl BTreeIndex {
    pub fn new(
        new_page: impl FnOnce() -> RsqlResult<(Arc<RwLock<storage::Page>>, u64)>,
        write_page: impl Fn(u64, &storage::Page) -> RsqlResult<()>,
    ) -> RsqlResult<Self> {
        let (page, page_num) = new_page()?;
        let root_node = btree_node::BTreeNode::Leaf { items: vec![], next_page_num: 0 };
        let mut page = page.write().unwrap();
        root_node.to_page(&mut page)?;
        write_page(page_num, &page)?;
        Ok(Self { root: page_num})
    }
    pub fn from(
        root_page_num: u64,
    ) -> RsqlResult<Self> {
        Ok(Self { root: root_page_num})
    }
    pub fn root_page_num(&self) -> u64 {
        self.root
    }
    /// Helper function to find leaf node containing the index_item
    /// Returns (LeafNode, PositionInNode, PageNum, PathToRoot)
    fn find_leaf_pos<F>(
        &self,
        index: &data_item::DataItem,
        get_page: &F,
        find_first: bool,
    ) -> RsqlResult<(btree_node::BTreeNode, usize, u64, Vec<u64>)> 
    where F: Fn(u64) -> RsqlResult<storage::Page> {
        let mut current_page_num = self.root;
        let mut path = vec![];
        let mut current_node = {
            let root_page = get_page(self.root)?;
            btree_node::BTreeNode::from_page(&root_page)?
        };
        loop {
            match current_node {
                btree_node::BTreeNode::Leaf { ref items, .. } => {
                    let pos = items.iter().position(|it| &it.key >= index).unwrap_or(items.len());
                    return Ok((current_node, pos, current_page_num, path));
                }
                btree_node::BTreeNode::Internal { ref items, next_page_num } => {
                    path.push(current_page_num);
                    let mut page_num = next_page_num;
                    for item in items {
                        let condition = if find_first {
                             index <= &item.key
                        } else {
                             index < &item.key
                        };
                        if condition {
                            page_num = item.child_page_num;
                            break;
                        }
                    }
                    let child_page = get_page(page_num)?;
                    current_node = btree_node::BTreeNode::from_page(&child_page)?;
                    current_page_num = page_num;
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
        let (node, pos, _, _) = self.find_leaf_pos(&index, get_page, false)?;
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
        let (leaf_node, start_pos, _, _) = self.find_leaf_pos(&start_index, get_page, true)?;
        
        Ok(RangeIterator {
            current_index: start_index,
            end_index,
            get_page,
            current_leaf_node: leaf_node,
            current_item_index: start_pos,
        })
    }
    pub fn insert_entry<F, W, N>(
        &mut self,
        index: data_item::DataItem,
        page_offset: u64,
        get_page: &F,
        write_page: &W,
        new_page: &N,
    ) -> RsqlResult<()> 
    where 
        F: Fn(u64) -> RsqlResult<storage::Page>,
        W: Fn(u64, &storage::Page) -> RsqlResult<()>,
        N: Fn() -> RsqlResult<(storage::Page, u64)>
    {
        // 1. find leaf node and path from root
        let (node, pos, page_num, mut path) = self.find_leaf_pos(&index, get_page, false)?;

        // 2. support duplicate keys: removed the "Index already exists" check.
        // The new entry will be inserted at 'pos' maintaining sorted order.

        let mut current_page_num = page_num;
        let mut node_to_update = node;
        let mut item_to_insert: Option<(data_item::DataItem, u64)> = None; // (split_key, right_child_page_num)

        // Leaf insertion logic
        if let btree_node::BTreeNode::Leaf { mut items, next_page_num } = node_to_update {
            items.insert(pos, btree_node::LeafItem { 
                key: index, 
                child_page_num: 0, 
                page_offset 
            });
            let new_leaf = btree_node::BTreeNode::Leaf { items, next_page_num };
            
            if new_leaf.has_space_for(0) {
                let mut page = get_page(current_page_num)?;
                new_leaf.to_page(&mut page)?;
                write_page(current_page_num, &page)?;
                return Ok(());
            } else {
                // Leaf needs splitting
                if let btree_node::BTreeNode::Leaf { mut items, next_page_num } = new_leaf {
                    let mid = items.len() / 2;
                    let right_items = items.split_off(mid);
                    let split_key = right_items[0].key.clone();
                    
                    // Create and write right node
                    let (mut page_right, right_page_num) = new_page()?;
                    let node_right = btree_node::BTreeNode::Leaf { items: right_items, next_page_num };
                    node_right.to_page(&mut page_right)?;
                    write_page(right_page_num, &page_right)?;
                    
                    // Update and write left node (original page)
                    let node_left = btree_node::BTreeNode::Leaf { items, next_page_num: right_page_num };
                    let mut page_left = get_page(current_page_num)?;
                    node_left.to_page(&mut page_left)?;
                    write_page(current_page_num, &page_left)?;
                    
                    item_to_insert = Some((split_key, right_page_num));
                }
            }
        }

        // Propagate split to internal nodes
        while let Some((key, right_child)) = item_to_insert.clone() {
            if let Some(parent_page_num) = path.pop() {
                let parent_page = get_page(parent_page_num)?;
                let mut parent_node = btree_node::BTreeNode::from_page(&parent_page)?;
                
                if let btree_node::BTreeNode::Internal { ref mut items, ref mut next_page_num } = parent_node {
                    // Find where the split child was to insert the new divider key
                    // If we followed this internal node, the child was either in items[pos].child_page_num 
                    // or it was the next_page_num.
                    let pos = items.iter().position(|it| key < it.key).unwrap_or(items.len());
                    
                    if pos < items.len() {
                        // Split happened in a child pointed to by an item.
                        // Insert new (key, old_child_ptr) and update current item ptr to right_child.
                        let old_child = items[pos].child_page_num;
                        items[pos].child_page_num = right_child;
                        items.insert(pos, btree_node::IndexItem { key, child_page_num: old_child });
                    } else {
                        // Split happened in the next_page_num child.
                        let old_next = *next_page_num;
                        *next_page_num = right_child;
                        items.push(btree_node::IndexItem { key, child_page_num: old_next });
                    }
                }
                
                if parent_node.has_space_for(0) {
                    let mut page = get_page(parent_page_num)?;
                    parent_node.to_page(&mut page)?;
                    write_page(parent_page_num, &page)?;
                    item_to_insert = None;
                } else {
                    // Parent internal node also needs splitting
                    if let btree_node::BTreeNode::Internal { mut items, next_page_num } = parent_node {
                        let mid = items.len() / 2;
                        let mut right_items = items.split_off(mid);
                        let mid_item = right_items.remove(0); // This key is pushed to parent
                        let split_key = mid_item.key;
                        
                        // New right internal node
                        let (mut page_right, right_page_num) = new_page()?;
                        let node_right = btree_node::BTreeNode::Internal { items: right_items, next_page_num };
                        node_right.to_page(&mut page_right)?;
                        write_page(right_page_num, &page_right)?;
                        
                        // Update left internal node (original page)
                        let node_left = btree_node::BTreeNode::Internal { 
                            items, 
                            next_page_num: mid_item.child_page_num 
                        };
                        let mut page_left = get_page(parent_page_num)?;
                        node_left.to_page(&mut page_left)?;
                        write_page(parent_page_num, &page_left)?;
                        
                        item_to_insert = Some((split_key, right_page_num));
                        current_page_num = parent_page_num;
                    }
                }
            } else {
                // Split reached the root - create a new root level
                let (mut page, new_root_page_num) = new_page()?;
                let new_root = btree_node::BTreeNode::Internal {
                    items: vec![btree_node::IndexItem { key: key.clone(), child_page_num: current_page_num }],
                    next_page_num: right_child,
                };
                new_root.to_page(&mut page)?;
                write_page(new_root_page_num, &page)?;
                self.root = new_root_page_num;
                item_to_insert = None;
            }
        }
        Ok(())
    }

    pub fn update_entry<F, W>(
        &self,
        index: data_item::DataItem,
        old_offset: u64,
        new_offset: u64,
        get_page: &F,
        write_page: &W,
    ) -> RsqlResult<bool>
    where 
        F: Fn(u64) -> RsqlResult<storage::Page>,
        W: Fn(u64, &storage::Page) -> RsqlResult<()> 
    {
        let (mut node, _, mut page_num, _) = self.find_leaf_pos(&index, get_page, true)?;
        loop {
            if let btree_node::BTreeNode::Leaf { ref mut items, next_page_num } = node {
                for item in items.iter_mut() {
                    if item.key == index && item.page_offset == old_offset {
                        item.page_offset = new_offset;
                        let mut page = get_page(page_num)?;
                        node.to_page(&mut page)?;
                        write_page(page_num, &page)?;
                        return Ok(true);
                    }
                    if item.key > index {
                        return Ok(false);
                    }
                }
                if next_page_num == 0 { return Ok(false); }
                page_num = next_page_num;
                let next_page = get_page(page_num)?;
                node = btree_node::BTreeNode::from_page(&next_page)?;
            } else {
                return Ok(false);
            }
        }
    }

    pub fn delete_entry<F, W>(
        &self,
        index: data_item::DataItem,
        page_offset: u64,
        get_page: &F,
        write_page: &W,
    ) -> RsqlResult<bool>
    where 
        F: Fn(u64) -> RsqlResult<storage::Page>,
        W: Fn(u64, &storage::Page) -> RsqlResult<()> 
    {
        let (mut node, _, mut page_num, _) = self.find_leaf_pos(&index, get_page, true)?;
        loop {
            if let btree_node::BTreeNode::Leaf { ref mut items, next_page_num } = node {
                let mut found_pos = None;
                for (i, item) in items.iter().enumerate() {
                    if item.key == index && item.page_offset == page_offset {
                        found_pos = Some(i);
                        break;
                    }
                    if item.key > index {
                        return Ok(false);
                    }
                }

                if let Some(pos) = found_pos {
                    items.remove(pos);
                    let mut page = get_page(page_num)?;
                    node.to_page(&mut page)?;
                    write_page(page_num, &page)?;
                    return Ok(true);
                }

                if next_page_num == 0 { return Ok(false); }
                page_num = next_page_num;
                let next_page = get_page(page_num)?;
                node = btree_node::BTreeNode::from_page(&next_page)?;
            } else {
                return Ok(false);
            }
        }
    }
}
