use core::panic;

use crate::db::data_item;
use crate::db::common::{RsqlResult, RsqlError};
use super::super::consist_storage::ConsistStorageEngine;
use super::btree_node;
// use super::consist_storage::ConsistStorageEngine;

/// Iterator to find entries in range [start_index, end_index]
/// 
/// Return: (page_num, page_offset)
pub struct RangeIterator<'a>
{
    // current_index: data_item::DataItem,
    end_index: Option<data_item::DataItem>,
    storage: &'a ConsistStorageEngine,
    current_leaf_node: btree_node::BTreeNode,
    current_item_index: usize,
}

impl<'a> Iterator for RangeIterator<'a>
{
    type Item = RsqlResult<(u64, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.current_leaf_node {
            btree_node::BTreeNode::Leaf { items, next_page_num } => {
                // if current_item_index out of bound, load next page
                if self.current_item_index >= items.len() {
                    if *next_page_num == 0 { return None; }
                    
                    let next_page = match self.storage.read(*next_page_num){
                        Ok(p) => p,
                        Err(e) => return Some(Err(e)),
                    };
                    self.current_leaf_node = match btree_node::BTreeNode::from_page(&next_page) {
                        Ok(n) => n,
                        Err(e) => return Some(Err(e)),
                    };
                    self.current_item_index = 0;
                    return self.next(); // recursive call to get next item
                }

                let item = &items[self.current_item_index];
                
                // if item.key > end_index, stop iteration
                if let Some(ref end_idx) = self.end_index {
                    if item.key > *end_idx {
                        return None;
                    }
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
        storage: &mut ConsistStorageEngine,
        tnx_id: u64,
    ) -> RsqlResult<Self> {
        let (page_num, mut page) = storage.new_page(tnx_id)?;
        let root_node = btree_node::BTreeNode::Leaf { items: vec![], next_page_num: 0 };
        root_node.to_page(&mut page)?;
        storage.write(tnx_id, page_num, &page)?;
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
    /// Helper function to find the leaf node and position for a given index.
    /// returns the position which's key >= index if find_first is true,
    /// otherwise returns the position which's key > index.
    /// Returns (LeafNode, PositionInNode, PageNum, PathToRoot)
    fn find_leaf_pos(
        &self,
        index: &data_item::DataItem,
        storage: &ConsistStorageEngine,
        find_first: bool,
    ) -> RsqlResult<(btree_node::BTreeNode, usize, u64, Vec<u64>)> {
        let mut current_page_num = self.root;
        let mut path = vec![];
        let mut current_node = {
            let root_page = storage.read(self.root)?;
            btree_node::BTreeNode::from_page(&root_page)?
        };
        loop {
            match current_node {
                btree_node::BTreeNode::Leaf { ref items, .. } => {
                    let pos = items.iter()
                        .position(|it| {
                            if find_first {
                                index <= &it.key
                            } else {
                                index < &it.key
                            }
                        })
                        .unwrap_or(items.len());
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
                    let child_page = storage.read(page_num)?;
                    current_node = btree_node::BTreeNode::from_page(&child_page)?;
                    current_page_num = page_num;
                }
            }
        }
    }
    /// Find the smallest leaf node in the B-Tree.
    /// Returns (LeafNode, PageNum)
    fn find_smallest_leaf(
        &self,
        storage: &ConsistStorageEngine,
    ) -> RsqlResult<(btree_node::BTreeNode, u64)> {
        let mut current_page_num = self.root;
        let mut current_node = {
            let root_page = storage.read(self.root)?;
            btree_node::BTreeNode::from_page(&root_page)?
        };
        loop {
            match current_node {
                btree_node::BTreeNode::Leaf { .. } => {
                    return Ok((current_node, current_page_num));
                }
                btree_node::BTreeNode::Internal { ref items, next_page_num } => {
                    let mut page_num = next_page_num;
                    if !items.is_empty() {
                        page_num = items[0].child_page_num;
                    }
                    let child_page = storage.read(page_num)?;
                    current_node = btree_node::BTreeNode::from_page(&child_page)?;
                    current_page_num = page_num;
                }
            }
        }
    }
    /// Find the largest leaf node in the B-Tree.
    /// Returns (LeafNode, PageNum)
    fn find_largest_leaf(
        &self,
        storage: &ConsistStorageEngine,
    ) -> RsqlResult<(btree_node::BTreeNode, u64)> {
        let mut current_page_num = self.root;
        let mut current_node = {
            let root_page = storage.read(self.root)?;
            btree_node::BTreeNode::from_page(&root_page)?
        };
        loop {
            match current_node {
                btree_node::BTreeNode::Leaf { .. } => {
                    return Ok((current_node, current_page_num));
                }
                btree_node::BTreeNode::Internal { ref items, next_page_num } => {
                    let mut page_num = next_page_num;
                    if !items.is_empty() {
                        page_num = items[items.len() - 1].child_page_num;
                    }
                    let child_page = storage.read(page_num)?;
                    current_node = btree_node::BTreeNode::from_page(&child_page)?;
                    current_page_num = page_num;
                }
            }
        }
    } 

    /// Check if the given index already exists in the B-Tree.
    pub fn check_exists(
        &self,
        index: data_item::DataItem,
        storage: &ConsistStorageEngine,
    ) -> RsqlResult<bool> {
        let (node, pos, _, _) = self.find_leaf_pos(&index, storage, true)?;
        if let btree_node::BTreeNode::Leaf { items, .. } = node {
            if pos < items.len() && items[pos].key == index {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Find the first entry matching the given index.
    /// Returns (page_num, page_offset) if found.
    pub fn find_entry(
        &self,
        index: data_item::DataItem,
        storage: &ConsistStorageEngine,
    ) -> RsqlResult<Option<(u64, u64)>> {
        let (node, pos, _, _) = self.find_leaf_pos(&index, storage, true)?;
        if let btree_node::BTreeNode::Leaf { items, .. } = node {
            if pos < items.len() && items[pos].key == index {
                return Ok(Some((items[pos].child_page_num, items[pos].page_offset)));
            }
        }
        Ok(None)
    }

    pub fn find_range_entry<'a>(
        &self,
        start_index: Option<data_item::DataItem>,
        end_index: Option<data_item::DataItem>,
        storage: &'a ConsistStorageEngine,
    ) -> RsqlResult<RangeIterator<'a>> {
        let (leaf_node, start_pos) = match start_index {
            Some(idx) => {
                let (node, pos, _, _) = self.find_leaf_pos(&idx, storage, true)?;
                (node, pos)
            },
            None => {
                let (node, page_num) = self.find_smallest_leaf(storage)?;
                (node, 0)
            }
        };
        Ok(RangeIterator {
            end_index,
            storage,
            current_leaf_node: leaf_node,
            current_item_index: start_pos,
        })
    }
    pub fn insert_entry<'a>(
        &mut self,
        tnx_id: u64,
        index: data_item::DataItem,
        page_num: u64,
        page_offset: u64,
        storage: &'a mut ConsistStorageEngine,
    ) -> RsqlResult<()> {
        // 1. find leaf node and path from root
        let (node, pos, mut current_page_num, mut path) = self.find_leaf_pos(&index, storage, false)?;

        // 2. support duplicate keys: removed the "Index already exists" check.
        // The new entry will be inserted at 'pos' maintaining sorted order.
        let node_to_update = node;
        let mut item_to_insert: Option<(data_item::DataItem, u64)> = None; // (split_key, right_child_page_num)

        // Leaf insertion logic
        if let btree_node::BTreeNode::Leaf { mut items, next_page_num } = node_to_update {
            items.insert(pos, btree_node::LeafItem { 
                key: index, 
                child_page_num: page_num, 
                page_offset 
            });
            let new_leaf = btree_node::BTreeNode::Leaf { items, next_page_num };
            
            if new_leaf.has_space_for(0) {
                let mut page = storage.read(current_page_num)?;
                new_leaf.to_page(&mut page)?;
                storage.write(tnx_id, current_page_num, &page)?;
                return Ok(());
            } else {
                // Leaf needs splitting
                if let btree_node::BTreeNode::Leaf { mut items, next_page_num } = new_leaf {
                    let mid = items.len() / 2;
                    let right_items = items.split_off(mid);
                    let split_key = right_items[0].key.clone();
                    
                    // Create and write right node
                    let (right_page_num, mut right_page) = storage.new_page(tnx_id)?;
                    let node_right = btree_node::BTreeNode::Leaf { items: right_items, next_page_num };
                    node_right.to_page(&mut right_page)?;
                    storage.write(tnx_id, right_page_num, &right_page)?;
                    
                    // Update and write left node (original page)
                    let node_left = btree_node::BTreeNode::Leaf { items, next_page_num: right_page_num };
                    let mut page_left = storage.read(current_page_num)?;
                    node_left.to_page(&mut page_left)?;
                    storage.write(tnx_id, current_page_num, &page_left)?;
                    
                    item_to_insert = Some((split_key, right_page_num));
                }
            }
        }

        // Propagate split to internal nodes
        while let Some((key, right_child)) = item_to_insert.clone() {
            if let Some(parent_page_num) = path.pop() {
                let parent_page = storage.read(parent_page_num)?;
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
                    let mut page = storage.read(parent_page_num)?;
                    parent_node.to_page(&mut page)?;
                    storage.write(tnx_id, parent_page_num, &page)?;
                    item_to_insert = None;
                } else {
                    // Parent internal node also needs splitting
                    if let btree_node::BTreeNode::Internal { mut items, next_page_num } = parent_node {
                        let mid = items.len() / 2;
                        let mut right_items = items.split_off(mid);
                        let mid_item = right_items.remove(0); // This key is pushed to parent
                        let split_key = mid_item.key;
                        
                        // New right internal node
                        let (right_page_num, mut right_page) = storage.new_page(tnx_id)?;
                        let node_right = btree_node::BTreeNode::Internal { items: right_items, next_page_num };
                        node_right.to_page(&mut right_page)?;
                        storage.write(tnx_id, right_page_num, &right_page)?;
                        
                        // Update left internal node (original page)
                        let node_left = btree_node::BTreeNode::Internal { 
                            items, 
                            next_page_num: mid_item.child_page_num 
                        };
                        let mut page_left = storage.read(parent_page_num)?;
                        node_left.to_page(&mut page_left)?;
                        storage.write(tnx_id, parent_page_num, &page_left)?;
                        
                        item_to_insert = Some((split_key, right_page_num));
                        current_page_num = parent_page_num;
                    }
                }
            } else {
                // Split reached the root - create a new root level
                let (new_root_page_num, mut new_root_page) = storage.new_page(tnx_id)?;
                let new_root = btree_node::BTreeNode::Internal {
                    items: vec![btree_node::IndexItem { key: key.clone(), child_page_num: current_page_num }],
                    next_page_num: right_child,
                };
                new_root.to_page(&mut new_root_page)?;
                storage.write(tnx_id, new_root_page_num, &new_root_page)?;
                self.root = new_root_page_num;
                item_to_insert = None;
            }
        }
        Ok(())
    }

    pub fn traverse_all_entries(
        &self,
        storage: &ConsistStorageEngine,
    ) -> RsqlResult<impl Iterator<Item = RsqlResult<(u64, u64)>>>
    {
        let mut results: Vec<RsqlResult<(u64, u64)>> = Vec::new();

        // 1. Find the leftmost leaf node
        let mut page_num = self.root;
        loop {
            let page = storage.read(page_num)?;
            let node = btree_node::BTreeNode::from_page(&page)?;
            match node {
                btree_node::BTreeNode::Leaf { items, next_page_num } => {
                    // Collect all entries from this leaf and subsequent leaves
                    for item in items {
                        results.push(Ok((item.child_page_num, item.page_offset)));
                    }
                    let mut next = next_page_num;
                    while next != 0 {
                        let p = storage.read(next)?;
                        match btree_node::BTreeNode::from_page(&p)? {
                            btree_node::BTreeNode::Leaf { items: leaf_items, next_page_num } => {
                                for item in leaf_items {
                                    results.push(Ok((item.child_page_num, item.page_offset)));
                                }
                                next = next_page_num;
                            }
                            _ => {
                                return Err(RsqlError::StorageError(
                                    "Expected leaf page while traversing leaves".to_string()
                                ));
                            }
                        }
                    }
                    break;
                }
                btree_node::BTreeNode::Internal { items, next_page_num } => {
                    // Go to the leftmost child
                    page_num = if !items.is_empty() {
                        items[0].child_page_num
                    } else {
                        next_page_num
                    };
                }
            }
        }
        Ok(results.into_iter())
    }

    pub fn update_entry(
        &self,
        tnx_id: u64,
        index: data_item::DataItem,
        old_page_num: u64,
        old_offset: u64,
        new_page_num: u64,
        new_offset: u64,
        storage: &mut ConsistStorageEngine,
    ) -> RsqlResult<bool> {
        let (mut node, _, mut page_num, _) = self.find_leaf_pos(&index, storage, true)?;
        loop {
            if let btree_node::BTreeNode::Leaf { ref mut items, next_page_num } = node {
                for item in items.iter_mut() {
                    if item.key == index && 
                       item.child_page_num == old_page_num &&
                       item.page_offset == old_offset {
                        item.child_page_num = new_page_num;
                        item.page_offset = new_offset;
                        let mut page = storage.read(page_num)?;
                        node.to_page(&mut page)?;
                        storage.write(tnx_id, page_num, &page)?;
                        return Ok(true);
                    }
                    if item.key > index {
                        return Ok(false);
                    }
                }
                if next_page_num == 0 { return Ok(false); }
                page_num = next_page_num;
                let next_page = storage.read(page_num)?;
                node = btree_node::BTreeNode::from_page(&next_page)?;
            } else {
                return Ok(false);
            }
        }
    }

    pub fn delete_entry(
        &self,
        tnx_id: u64,
        index: data_item::DataItem,
        page_num: u64, // for checking duplicates
        page_offset: u64,
        storage: &mut ConsistStorageEngine,
    ) -> RsqlResult<bool> {
        let (mut node, _, mut leaf_page_num, _) = self.find_leaf_pos(&index, storage, true)?;
        loop {
            if let btree_node::BTreeNode::Leaf { ref mut items, next_page_num } = node {
                let mut found_pos = None;
                for (i, item) in items.iter().enumerate() {
                    if item.key == index && 
                       item.child_page_num == page_num &&
                       item.page_offset == page_offset {
                        found_pos = Some(i);
                        break;
                    }
                    if item.key > index {
                        return Ok(false);
                    }
                }

                if let Some(pos) = found_pos {
                    items.remove(pos);
                    let mut page = storage.read(leaf_page_num)?;
                    node.to_page(&mut page)?;
                    storage.write(tnx_id, leaf_page_num, &page)?;
                    return Ok(true);
                }

                if next_page_num == 0 { return Ok(false); }
                leaf_page_num = next_page_num;
                let next_page = storage.read(leaf_page_num)?;
                node = btree_node::BTreeNode::from_page(&next_page)?;
            } else {
                return Ok(false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::storage_engine::consist_storage::ConsistStorageEngine;
    use crate::db::data_item::DataItem;

    use tempfile::tempdir;

    #[test]
    fn test_btree_basic_insert_find_exists() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_btree_basic.db");
        let file_path_str = file_path.to_str().unwrap();

        let tnx = 1u64;
        let mut storage = ConsistStorageEngine::new(file_path_str, 1).unwrap();
        let mut idx = BTreeIndex::new(&mut storage, tnx).unwrap();

        // insert several entries including duplicate keys
        idx.insert_entry(tnx, DataItem::Integer(10), 100, 1, &mut storage).unwrap();
        idx.insert_entry(tnx, DataItem::Integer(5), 50, 1, &mut storage).unwrap();
        idx.insert_entry(tnx, DataItem::Integer(10), 101, 2, &mut storage).unwrap();
        idx.insert_entry(tnx, DataItem::Integer(15), 150, 3, &mut storage).unwrap();

        // check exists
        assert!(idx.check_exists(DataItem::Integer(10), &storage).unwrap());
        assert!(idx.check_exists(DataItem::Integer(5), &storage).unwrap());
        assert!(!idx.check_exists(DataItem::Integer(999), &storage).unwrap());

        // find_entry should return one matching entry for key 10
        let found = idx.find_entry(DataItem::Integer(10), &storage).unwrap();
        assert!(found.is_some());
    }

    #[test]
    fn test_btree_range_traverse_update_delete() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_btree_ops.db");
        let file_path_str = file_path.to_str().unwrap();

        let tnx = 1u64;
        let mut storage = ConsistStorageEngine::new(file_path_str, 2).unwrap();
        let mut idx = BTreeIndex::new(&mut storage, tnx).unwrap();

        // insert values 1..=10, with duplicates for even numbers
        for i in 1..=10u64 {
            idx.insert_entry(tnx, DataItem::Integer(i as i64), i * 10, i, &mut storage).unwrap();
            if i % 2 == 0 {
                // duplicate
                idx.insert_entry(tnx, DataItem::Integer(i as i64), i * 10 + 1, i + 100, &mut storage).unwrap();
            }
        }

        // traverse all entries and count
        let all: Vec<_> = idx.traverse_all_entries(&storage).unwrap().map(|r| r.unwrap()).collect();
        // there should be 10 + 5 duplicates = 15 entries
        assert_eq!(all.len(), 15);

        // range iterator: from 3 to 6 (inclusive) -> keys 3,4,4,5,6,6
        let start = Some(DataItem::Integer(3));
        let end = Some(DataItem::Integer(6));
        let mut it = idx.find_range_entry(start, end, &storage).unwrap();
        let mut got = Vec::new();
        while let Some(res) = it.next() {
            let pair = res.unwrap();
            got.push(pair);
        }
        // expected entries: keys 3,4(2 entries),5,6(2 entries) => total 6
        assert_eq!(got.len(), 6);

        // update one specific duplicate (key=4) change child page and offset
        let key4 = DataItem::Integer(4);
        // pick an existing duplicate entry to update: old child_page = 41, old_offset = 104
        let updated = idx.update_entry(tnx, key4.clone(), 41, 104, 9999, 999, &mut storage).unwrap();
        assert!(updated);

        // verify the updated entry is present and old one gone
        // traverse and look for new pointer
        let all_after: Vec<_> = idx.traverse_all_entries(&storage).unwrap().map(|r| r.unwrap()).collect();
        assert!(all_after.contains(&(9999u64, 999u64)));
        assert!(!all_after.contains(&(41u64, 104u64)));

        // delete the updated entry
        let deleted = idx.delete_entry(tnx, key4.clone(), 9999, 999, &mut storage).unwrap();
        assert!(deleted);

        // ensure duplicate (other copy) of key 4 still exists (child_page 40, offset 4 expected)
        let exists_still = idx.check_exists(key4.clone(), &storage).unwrap();
        assert!(exists_still);
    }

    #[test]
    fn test_btree_traverse_order_and_duplicates() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_btree_traverse.db");
        let file_path_str = file_path.to_str().unwrap();

        let tnx = 1u64;
        let mut storage = ConsistStorageEngine::new(file_path_str, 1).unwrap();
        let mut idx = BTreeIndex::new(&mut storage, tnx).unwrap();

        // insert unsorted keys with duplicates, use child_page_num to reflect key
        idx.insert_entry(tnx, DataItem::Integer(10), 100, 10, &mut storage).unwrap();
        idx.insert_entry(tnx, DataItem::Integer(5), 50, 5, &mut storage).unwrap();
        idx.insert_entry(tnx, DataItem::Integer(10), 101, 11, &mut storage).unwrap();
        idx.insert_entry(tnx, DataItem::Integer(15), 150, 15, &mut storage).unwrap();

        let all: Vec<_> = idx.traverse_all_entries(&storage).unwrap().map(|r| r.unwrap()).collect();
        // Expect order: key 5, key 10 (first), key 10 (second), key 15
        let expected = vec![(50u64,5u64),(100u64,10u64),(101u64,11u64),(150u64,15u64)];
        assert_eq!(all, expected);
    }

    #[test]
    fn test_btree_root_split_and_many_inserts() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_btree_root_split.db");
        let file_path_str = file_path.to_str().unwrap();

        let tnx = 1u64;
        // small page size parameter to encourage splits
        let mut storage = ConsistStorageEngine::new(file_path_str, 2).unwrap();
        let mut idx = BTreeIndex::new(&mut storage, tnx).unwrap();


        // Insert many keys to force splits and internal node creations
        let mut inserted = Vec::new();
        for i in 1..=60u64 {
            let key = DataItem::Integer(i as i64);
            let child = i * 10;
            let offset = i;
            idx.insert_entry(tnx, key.clone(), child, offset, &mut storage).unwrap();
            inserted.push((i, child, offset));
        }

        // After lots of inserts, ensure entries are present (structure may
        // or may not allocate a new root page depending on split policy).

        // Check that all inserted entries can be found
        for (i, child, offset) in inserted {
            let key = DataItem::Integer(i as i64);
            let found = idx.find_entry(key, &storage).unwrap();
            assert!(found.is_some());
            let (fpage, foff) = found.unwrap();
            // child/page numbers used may correspond to first matching duplicate,
            // but we used unique child values so we can assert equality
            assert_eq!(fpage, child);
            assert_eq!(foff, offset);
        }
    }

    #[test]
    fn test_btree_range_iterator_with_none_start_or_end() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_btree_range_none.db");
        let file_path_str = file_path.to_str().unwrap();

        let tnx = 1u64;
        let mut storage = ConsistStorageEngine::new(file_path_str, 2).unwrap();
        let mut idx = BTreeIndex::new(&mut storage, tnx).unwrap();

        // insert 1..=10 with duplicates on evens
        for i in 1..=10u64 {
            idx.insert_entry(tnx, DataItem::Integer(i as i64), i*100, i, &mut storage).unwrap();
            if i % 2 == 0 {
                idx.insert_entry(tnx, DataItem::Integer(i as i64), i*100+1, i+100, &mut storage).unwrap();
            }
        }

        // start = None, end = Some(3) -> should return keys <= 3
        let mut it1 = idx.find_range_entry(None, Some(DataItem::Integer(3)), &storage).unwrap();
        let res1: Vec<_> = it1.by_ref().map(|r| r.unwrap()).collect();
        // keys: 1,2,2,3 -> 4 entries
        assert_eq!(res1.len(), 4);

        // start = Some(8), end = None -> should return keys >=8 to end
        let mut it2 = idx.find_range_entry(Some(DataItem::Integer(8)), None, &storage).unwrap();
        let res2: Vec<_> = it2.by_ref().map(|r| r.unwrap()).collect();
        // keys 8,8,9,10,10 => 5 entries
        assert_eq!(res2.len(), 5);
    }

    #[test]
    fn test_update_delete_nonexistent_return_false() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_btree_update_delete_nonexist.db");
        let file_path_str = file_path.to_str().unwrap();

        let tnx = 1u64;
        let mut storage = ConsistStorageEngine::new(file_path_str, 1).unwrap();
        let mut idx = BTreeIndex::new(&mut storage, tnx).unwrap();

        // insert a single key
        idx.insert_entry(tnx, DataItem::Integer(1), 10, 1, &mut storage).unwrap();

        // try updating a non-existing key
        let updated = idx.update_entry(tnx, DataItem::Integer(999), 1, 1, 2, 2, &mut storage).unwrap();
        assert!(!updated);

        // try deleting a non-existing pointer for an existing key
        let deleted = idx.delete_entry(tnx, DataItem::Integer(1), 9999, 9999, &mut storage).unwrap();
        assert!(!deleted);
    }
}
