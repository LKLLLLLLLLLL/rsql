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
    root: u64,
}

impl BTreeIndex {
    pub fn new(
        new_page: impl FnOnce() -> RsqlResult<(storage::Page, u64)>,
        write_page: impl Fn(u64, &storage::Page) -> RsqlResult<()>,
    ) -> RsqlResult<Self> {
        let (mut page, page_num) = new_page()?;
        let root_node = btree_node::BTreeNode::Leaf { items: vec![], next_page_num: 0 };
        root_node.to_page(&mut page)?;
        write_page(page_num, &page)?;
        Ok(Self { root: page_num})
    }
    pub fn from(
        root_page_num: u64,
    ) -> RsqlResult<Self> {
        Ok(Self { root: root_page_num})
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    struct MockStorage {
        pages: HashMap<u64, storage::Page>,
        next_page_num: u64,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                pages: HashMap::new(),
                next_page_num: 1,
            }
        }
        fn new_page(&mut self) -> (storage::Page, u64) {
            let page = storage::Page::new();
            let num = self.next_page_num;
            self.next_page_num += 1;
            // The caller is expected to write to this page
            (page, num)
        }
        fn write_page(&mut self, page_num: u64, page: &storage::Page) -> RsqlResult<()> {
            let mut p = storage::Page::new();
            p.data = page.data.clone();
            self.pages.insert(page_num, p);
            Ok(())
        }
        fn get_page(&self, page_num: u64) -> RsqlResult<storage::Page> {
            if let Some(p) = self.pages.get(&page_num) {
                let mut page = storage::Page::new();
                page.data = p.data.clone();
                Ok(page)
            } else {
                Err(crate::db::errors::RsqlError::StorageError(format!("Page {} not found", page_num)))
            }
        }
    }

    #[test]
    fn test_btree_basic_insert_search() -> RsqlResult<()> {
        let storage = Rc::new(RefCell::new(MockStorage::new()));
        
        let s_ref = storage.clone();
        let new_page_init = || Ok(s_ref.borrow_mut().new_page());
        let s_ref2 = storage.clone();
        let write_page = |id, p: &_| s_ref2.borrow_mut().write_page(id, p);
        
        let mut btree = BTreeIndex::new(new_page_init, &write_page)?;
        
        let s_ref3 = storage.clone();
        let get_page = |id| s_ref3.borrow().get_page(id);
        let s_ref4 = storage.clone();
        let new_page = || Ok(s_ref4.borrow_mut().new_page());

        // Insert
        btree.insert_entry(data_item::DataItem::Inteager(10), 100, &get_page, &write_page, &new_page)?;
        btree.insert_entry(data_item::DataItem::Inteager(20), 200, &get_page, &write_page, &new_page)?;
        btree.insert_entry(data_item::DataItem::Inteager(5), 50, &get_page, &write_page, &new_page)?;

        // Search
        assert_eq!(btree.find_entry(data_item::DataItem::Inteager(10), &get_page)?, Some(100));
        assert_eq!(btree.find_entry(data_item::DataItem::Inteager(20), &get_page)?, Some(200));
        assert_eq!(btree.find_entry(data_item::DataItem::Inteager(5), &get_page)?, Some(50));
        assert_eq!(btree.find_entry(data_item::DataItem::Inteager(99), &get_page)?, None);

        Ok(())
    }

    #[test]
    fn test_btree_duplicates() -> RsqlResult<()> {
        let storage = Rc::new(RefCell::new(MockStorage::new()));
        
        let s_ref = storage.clone();
        let new_page_init = || Ok(s_ref.borrow_mut().new_page());
        let s_ref2 = storage.clone();
        let write_page = |id, p: &_| s_ref2.borrow_mut().write_page(id, p);
        
        let mut btree = BTreeIndex::new(new_page_init, &write_page)?;
        
        let s_ref3 = storage.clone();
        let get_page = |id| s_ref3.borrow().get_page(id);
        let s_ref4 = storage.clone();
        let new_page = || Ok(s_ref4.borrow_mut().new_page());

        // Insert duplicates
        btree.insert_entry(data_item::DataItem::Inteager(10), 101, &get_page, &write_page, &new_page)?;
        btree.insert_entry(data_item::DataItem::Inteager(10), 102, &get_page, &write_page, &new_page)?;
        btree.insert_entry(data_item::DataItem::Inteager(10), 103, &get_page, &write_page, &new_page)?;

        // Range Search
        let iter = btree.find_range_entry(
            data_item::DataItem::Inteager(10),
            data_item::DataItem::Inteager(11),
            &get_page
        )?;

        let mut results: Vec<u64> = iter.map(|r| r.unwrap().1).collect();
        results.sort(); // Order might vary depending on insertion logic, but all must be present
        assert_eq!(results, vec![101, 102, 103]);

        // Test Deletion of one duplicate
        let deleted = btree.delete_entry(data_item::DataItem::Inteager(10), 102, &get_page, &write_page)?;
        assert!(deleted);

        // Verify remaining
        let iter2 = btree.find_range_entry(
            data_item::DataItem::Inteager(10),
            data_item::DataItem::Inteager(11),
            &get_page
        )?;
        let mut results2: Vec<u64> = iter2.map(|r| r.unwrap().1).collect();
        results2.sort();
        assert_eq!(results2, vec![101, 103]);

        Ok(())
    }

    #[test]
    fn test_btree_split() -> RsqlResult<()> {
        let storage = Rc::new(RefCell::new(MockStorage::new()));
        let s_ref = storage.clone();
        let new_page_init = || Ok(s_ref.borrow_mut().new_page());
        let s_ref2 = storage.clone();
        let write_page = |id, p: &_| s_ref2.borrow_mut().write_page(id, p);
        
        let mut btree = BTreeIndex::new(new_page_init, &write_page)?;
        
        let s_ref3 = storage.clone();
        let get_page = |id| s_ref3.borrow().get_page(id);
        let s_ref4 = storage.clone();
        let new_page = || Ok(s_ref4.borrow_mut().new_page());

        // Insert enough items to cause splits.
        // Assuming 4096 page size and small integers.
        // Inteager item size is 9 bytes. LeafItem overhead: 8(key) + 8(child) + 8(offset) = 24 bytes?
        // DataItem::size for Inteager is 9 (1+8).
        // LeafItem size: 9 + 8 + 8 = 25 bytes.
        // Header ~ 16 bytes.
        // 4096 / 25 ~= 163 items per page.
        
        for i in 0..500 {
            btree.insert_entry(data_item::DataItem::Inteager(i), i as u64, &get_page, &write_page, &new_page)?;
        }

        // Verify all exist
        for i in 0..500 {
            let found = btree.find_entry(data_item::DataItem::Inteager(i), &get_page)?;
            assert_eq!(found, Some(i as u64));
        }

        Ok(())
    }

    #[test]
    fn test_btree_persistence_simulation() -> RsqlResult<()> {
        let storage = Rc::new(RefCell::new(MockStorage::new()));
        
        let mut root_page_num = 0;

        // Scope 1: Create Index, Insert Data, then drop Index
        {
            let s_ref = storage.clone();
            let new_page_init = || Ok(s_ref.borrow_mut().new_page());
            let s_ref2 = storage.clone();
            let write_page = |id, p: &_| s_ref2.borrow_mut().write_page(id, p);
            let s_ref3 = storage.clone();
            let get_page = |id| s_ref3.borrow().get_page(id);
            let s_ref4 = storage.clone();
            let new_page = || Ok(s_ref4.borrow_mut().new_page());

            let mut btree = BTreeIndex::new(new_page_init, &write_page)?;
            
            btree.insert_entry(data_item::DataItem::Inteager(1), 10, &get_page, &write_page, &new_page)?;
            btree.insert_entry(data_item::DataItem::Inteager(2), 20, &get_page, &write_page, &new_page)?;
            btree.insert_entry(data_item::DataItem::Inteager(3), 30, &get_page, &write_page, &new_page)?;
            
            root_page_num = btree.root;
        } // btree dropped here

        // Scope 2: Re-load Index from root_page_num
        {
            let s_ref3 = storage.clone();
            let get_page = |id| s_ref3.borrow().get_page(id);

            let btree = BTreeIndex::from(root_page_num)?;
            
            assert_eq!(btree.find_entry(data_item::DataItem::Inteager(1), &get_page)?, Some(10));
            assert_eq!(btree.find_entry(data_item::DataItem::Inteager(2), &get_page)?, Some(20));
            assert_eq!(btree.find_entry(data_item::DataItem::Inteager(3), &get_page)?, Some(30));
            assert_eq!(btree.find_entry(data_item::DataItem::Inteager(4), &get_page)?, None);
        }

        Ok(())
    }

    #[test]
    fn test_btree_large_reverse_insertion() -> RsqlResult<()> {
        let storage = Rc::new(RefCell::new(MockStorage::new()));
        let s_ref = storage.clone();
        let new_page_init = || Ok(s_ref.borrow_mut().new_page());
        let s_ref2 = storage.clone();
        let write_page = |id, p: &_| s_ref2.borrow_mut().write_page(id, p);
        
        let mut btree = BTreeIndex::new(new_page_init, &write_page)?;
        
        let s_ref3 = storage.clone();
        let get_page = |id| s_ref3.borrow().get_page(id);
        let s_ref4 = storage.clone();
        let new_page = || Ok(s_ref4.borrow_mut().new_page());

        // Insert in reverse order: 500 down to 0
        // This exercises splitting loop logic differently (always splitting at the front/left)
        for i in (0..500).rev() {
            btree.insert_entry(data_item::DataItem::Inteager(i), i as u64, &get_page, &write_page, &new_page)?;
        }

        // Verify validity
        for i in 0..500 {
            let found = btree.find_entry(data_item::DataItem::Inteager(i), &get_page)?;
            assert_eq!(found, Some(i as u64), "Failed to find key {}", i);
        }

        Ok(())
    }

    #[test]
    fn test_btree_range_crossing_pages() -> RsqlResult<()> {
        let storage = Rc::new(RefCell::new(MockStorage::new()));
        let s_ref = storage.clone();
        let new_page_init = || Ok(s_ref.borrow_mut().new_page());
        let s_ref2 = storage.clone();
        let write_page = |id, p: &_| s_ref2.borrow_mut().write_page(id, p);
        
        let mut btree = BTreeIndex::new(new_page_init, &write_page)?;
        
        let s_ref3 = storage.clone();
        let get_page = |id| s_ref3.borrow().get_page(id);
        let s_ref4 = storage.clone();
        let new_page = || Ok(s_ref4.borrow_mut().new_page());

        // Insert enough items to span multiple pages (e.g., 300 items)
        for i in 0..300 {
            btree.insert_entry(data_item::DataItem::Inteager(i), i as u64, &get_page, &write_page, &new_page)?;
        }

        // Range query from 100 to 200 (likely crossing a page boundary)
        let iter = btree.find_range_entry(
            data_item::DataItem::Inteager(100),
            data_item::DataItem::Inteager(200),
            &get_page
        )?;

        let results: Vec<u64> = iter.map(|r| r.unwrap().1).collect();
        assert_eq!(results.len(), 100);
        
        // Verify contiguous values
        for (i, val) in results.iter().enumerate() {
            assert_eq!(*val, (100 + i) as u64);
        }

        Ok(())
    }

    #[test]
    fn test_btree_many_duplicates_crossing_pages() -> RsqlResult<()> {
        let storage = Rc::new(RefCell::new(MockStorage::new()));
        let s_ref = storage.clone();
        let new_page_init = || Ok(s_ref.borrow_mut().new_page());
        let s_ref2 = storage.clone();
        let write_page = |id, p: &_| s_ref2.borrow_mut().write_page(id, p);
        
        let mut btree = BTreeIndex::new(new_page_init, &write_page)?;
        
        let s_ref3 = storage.clone();
        let get_page = |id| s_ref3.borrow().get_page(id);
        let s_ref4 = storage.clone();
        let new_page = || Ok(s_ref4.borrow_mut().new_page());

        // Insert 200 identical keys. Since leaf size is ~160, this MUST cross at least one page boundary.
        for i in 0..200 {
            btree.insert_entry(data_item::DataItem::Inteager(999), 1000 + i as u64, &get_page, &write_page, &new_page)?;
        }

        // Add some other keys to ensure boundaries are clean
        btree.insert_entry(data_item::DataItem::Inteager(1000), 2000, &get_page, &write_page, &new_page)?;
        btree.insert_entry(data_item::DataItem::Inteager(998), 888, &get_page, &write_page, &new_page)?;

        // Search for the 999 range
        let iter = btree.find_range_entry(
            data_item::DataItem::Inteager(999),
            data_item::DataItem::Inteager(1000), // exclusive end
            &get_page
        )?;

        let results: Vec<u64> = iter.map(|r| r.unwrap().1).collect();
        assert_eq!(results.len(), 200);
        
        // Ensure we got all expected offsets
        let mut offsets = results.clone();
        offsets.sort();
        for i in 0..200 {
             assert_eq!(offsets[i], 1000 + i as u64);
        }

        Ok(())
    }
}
