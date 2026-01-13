use crate::config::PAGE_SIZE_BYTES;
use std::sync::{RwLock, Arc};
use std::fs::{self, OpenOptions, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::collections::HashMap;

pub struct Page {
    pub data: Vec<u8>,
}

impl Page {
    fn new() -> Self {
        Self {
            data: vec![0u8; PAGE_SIZE_BYTES],
        }
    }
}

pub struct StorageManager {
    file_path: String,
    file_page_num: u64, // number of pages in file
    pages: HashMap<u64, Arc<RwLock<Page>>>,  // new pages to be flushed
}

impl StorageManager {
    fn create_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        } // create dirs if not exist
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            Err(e) => Err(e)
        }   // create file if not exist
    }

    fn max_page_index(&self) -> Option<u64> {
        let max_file_page_index = if self.file_page_num - 1 >= 0 {Some(self.file_page_num - 1)} else {None};
        let max_cached_page_index = self.pages.keys().max().copied();

        match (max_file_page_index, max_cached_page_index) {
            (None, None) => None,
            (Some(f), None) => Some(f),
            (None, Some(c)) => Some(c),
            (Some(f), Some(c)) => Some(f.max(c)),
        } // return the max page index of file and cache(None means empty file and cache)
    }

    fn is_page_index_valid(&self, page_index: u64) -> Result<(), io::Error> {
        let max_page_index = self.max_page_index();
        match max_page_index {
            Some(max_index) if page_index <= max_index => Ok(()),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "page index out of bounds",
            )),
        }
    }

    pub fn new(file_path: &str) -> Result<Self, io::Error> {
        Self::create_file(file_path)?;
        Ok(Self {
            file_path: file_path.to_string(),
            file_page_num: 0,
            pages: HashMap::new(), // cache of pages which has the latest data
        })
    }

    pub fn read_page(&mut self, page_index: u64) -> Result<Arc<RwLock<Page>>, io::Error> {
        self.is_page_index_valid(page_index)?;

        if let Some(page_arc) = self.pages.get(&page_index) {
            Ok(Arc::clone(page_arc))
        } else {
            let mut file = File::open(self.file_path.as_str())?;
            file.seek(SeekFrom::Start(page_index * PAGE_SIZE_BYTES as u64))?; // go to the start position of the page with page_index
            let mut buffer = vec![0u8; PAGE_SIZE_BYTES];
            file.read_exact(&mut buffer)?;
            let page = Arc::new(RwLock::new(Page {data: buffer}));
            self.pages.insert(page_index, Arc::clone(&page)); // insert the page into cache
            Ok(page)
        }
    }

    pub fn write_page(&mut self, page: Page, page_index: u64) -> Result<(), io::Error> {
        self.is_page_index_valid(page_index)?;

        let page_arc = Arc::new(RwLock::new(page));
        self.pages.insert(page_index, Arc::clone(&page_arc)); // write the page into cache
        Ok(())
    }

    pub fn new_page(&self) -> Result<(u64, Arc<RwLock<Page>>), io::Error> {
        let new_page_index = match self.max_page_index() {
            Some(max_index) => max_index + 1,
            None => 0,
        };
        let new_page = Arc::new(RwLock::new(Page::new()));
        Ok((new_page_index, new_page))
    }

    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        if self.pages.is_empty() {
            return Ok(());
        };

        let max_page_index = match self.max_page_index() {
            Some(idx) => idx,
            None => return Ok(()),
        };

        let required_file_size = (max_page_index + 1) * PAGE_SIZE_BYTES as u64;
        let file_page_num = if required_file_size % PAGE_SIZE_BYTES as u64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file size is not aligned to page size",
            ));
        }else {
            required_file_size / PAGE_SIZE_BYTES as u64
        };
        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.file_path)?;
        let current_size = file.metadata()?.len();
        if current_size < required_file_size {
            file.set_len(required_file_size)?; // extend file (fills with zeros)
        }
        for (page_index, page_arc) in &self.pages {
            let offset = page_index * PAGE_SIZE_BYTES as u64;
            file.seek(SeekFrom::Start(offset))?;
            let page_data = &page_arc.read().map_err(|_| io::Error::new(
                io::ErrorKind::InvalidData,
                "Poisoned RwLock in page cache",
            ))?.data;
            file.write_all(page_data)?; // write page data to the file
        }
        file.sync_data()?; // ensure data is written to the disk
        self.file_page_num = file_page_num; // update pages number in the file
        Ok(())
    }
}