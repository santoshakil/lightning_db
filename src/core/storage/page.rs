use crate::core::error::{Error, Result};
use crate::core::storage::PageType;
use crate::utils::retry::RetryableOperations;
use crc32fast::Hasher;
use memmap2::{MmapMut, MmapOptions};
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing;

pub const PAGE_SIZE: usize = 4096;
pub const MAGIC: u32 = 0x4C44_4200; // "LDB\0"

pub type PageId = u32;

#[derive(Debug, Clone)]
pub struct Page {
    pub id: PageId,
    pub data: Arc<[u8; PAGE_SIZE]>,
    pub dirty: bool,
    pub page_type: PageType,
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: Arc::new([0u8; PAGE_SIZE]),
            dirty: false,
            page_type: PageType::default(),
        }
    }

    pub fn with_data(id: PageId, data: [u8; PAGE_SIZE]) -> Self {
        // Extract page type from data
        let page_type = if data.len() > 8 {
            PageType::from_byte(data[8]).unwrap_or_default()
        } else {
            PageType::default()
        };

        Self {
            id,
            data: Arc::new(data),
            dirty: false,
            page_type,
        }
    }

    pub fn get_data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn get_mut_data(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.dirty = true;
        Arc::make_mut(&mut self.data)
    }

    pub fn calculate_checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.data[16..]); // Skip header including checksum field
        hasher.finalize()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }

    pub fn verify_checksum(&self) -> bool {
        let stored_checksum =
            u32::from_le_bytes([self.data[12], self.data[13], self.data[14], self.data[15]]);
        stored_checksum == self.calculate_checksum()
    }

    pub fn data(&self) -> &[u8] {
        &self.data[..]
    }

    pub fn set_data(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > PAGE_SIZE {
            return Err(Error::Memory);
        }
        let mut new_data = [0u8; PAGE_SIZE];
        new_data[..data.len()].copy_from_slice(data);
        self.data = Arc::new(new_data);
        self.dirty = true;
        Ok(())
    }
}

#[derive(Debug)]
pub struct PageManager {
    file: std::fs::File,
    mmap: MmapMut,
    free_pages: Vec<u32>,
    next_page_id: u32,
    file_size: u64,
}

use super::PageManagerTrait;

impl PageManager {
    pub fn create(path: &Path, initial_size: u64) -> Result<Self> {
        // Use retry for file creation
        let file = RetryableOperations::file_operation(|| {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .map_err(|e| Error::Io(format!("Failed to create page file: {}", e)))
        })?;

        // Set file length with retry
        RetryableOperations::file_operation(|| {
            file.set_len(initial_size)
                .map_err(|e| Error::Io(format!("Failed to set file length: {}", e)))
        })?;

        // Create memory map with retry
        // SAFETY: File is open with read/write access and has been sized.
        // The mmap will be valid for the file's lifetime (held in struct).
        let mmap = RetryableOperations::file_operation(|| {
            unsafe { MmapOptions::new().map_mut(&file) }
                .map_err(|e| Error::Io(format!("Failed to create memory map: {}", e)))
        })?;

        let mut manager = Self {
            file,
            mmap,
            free_pages: Vec::new(),
            next_page_id: 1,
            file_size: initial_size,
        };

        manager.init_header_page()?;
        Ok(manager)
    }

    pub fn open(path: &Path) -> Result<Self> {
        // Open file with retry
        let file = RetryableOperations::file_operation(|| {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(|e| Error::Io(format!("Failed to open page file: {}", e)))
        })?;

        // Get metadata with retry
        let metadata = RetryableOperations::file_operation(|| {
            file.metadata()
                .map_err(|e| Error::Io(format!("Failed to get file metadata: {}", e)))
        })?;
        let file_size = metadata.len();

        // Create memory map with retry
        // SAFETY: File is open with read/write access and exists.
        // The mmap will be valid for the file's lifetime (held in struct).
        let mmap = RetryableOperations::file_operation(|| {
            unsafe { MmapOptions::new().map_mut(&file) }
                .map_err(|e| Error::Io(format!("Failed to create memory map: {}", e)))
        })?;

        let mut manager = Self {
            file,
            mmap,
            free_pages: Vec::new(),
            next_page_id: 1,
            file_size,
        };

        manager.load_header_page()?;
        Ok(manager)
    }

    fn init_header_page(&mut self) -> Result<()> {
        // Ensure mmap has enough space
        if self.mmap.len() < PAGE_SIZE {
            return Err(Error::Memory);
        }

        let header_data = &mut self.mmap[0..PAGE_SIZE];

        // Write magic number
        header_data[0..4].copy_from_slice(&MAGIC.to_le_bytes());

        // Write version
        header_data[4..8].copy_from_slice(&1u32.to_le_bytes());

        // Write page type (header = 3)
        header_data[8..12].copy_from_slice(&3u32.to_le_bytes());

        // Calculate and write checksum
        let mut hasher = Hasher::new();
        hasher.update(&header_data[16..]);
        let checksum = hasher.finalize();
        header_data[12..16].copy_from_slice(&checksum.to_le_bytes());

        self.mmap.flush()?;
        Ok(())
    }

    fn load_header_page(&mut self) -> Result<()> {
        // Check if file has enough data for header page
        if self.mmap.len() < PAGE_SIZE {
            return Err(Error::Corruption(format!(
                "Database file too small: {} bytes, expected at least {} bytes",
                self.mmap.len(),
                PAGE_SIZE
            )));
        }

        let header_data = &self.mmap[0..PAGE_SIZE];

        // Verify magic number
        let magic = u32::from_le_bytes([
            header_data[0],
            header_data[1],
            header_data[2],
            header_data[3],
        ]);

        if magic != MAGIC {
            return Err(Error::InvalidDatabase);
        }

        // Verify checksum
        let stored_checksum = u32::from_le_bytes([
            header_data[12],
            header_data[13],
            header_data[14],
            header_data[15],
        ]);

        let mut hasher = Hasher::new();
        hasher.update(&header_data[16..]);
        let calculated_checksum = hasher.finalize();

        if stored_checksum != calculated_checksum {
            return Err(Error::ChecksumMismatch {
                expected: stored_checksum,
                actual: calculated_checksum,
            });
        }

        // Calculate next page ID based on file size
        self.next_page_id = (self.file_size / PAGE_SIZE as u64) as u32;

        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<u32> {
        let page_id = if let Some(page_id) = self.free_pages.pop() {
            page_id
        } else {
            let page_id = self.next_page_id;
            self.next_page_id += 1;

            // Grow file if needed
            let required_size = (page_id as u64 + 1) * PAGE_SIZE as u64;
            if required_size > self.file_size {
                self.grow_file(required_size)?;
            }

            page_id
        };

        // Initialize the new page with empty data
        let mut page = Page::new(page_id);
        // Set page type to 0 (leaf) by default
        let data = page.get_mut_data();
        data[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        data[4..8].copy_from_slice(&1u32.to_le_bytes()); // version
        data[8..12].copy_from_slice(&0u32.to_le_bytes()); // page type (leaf)
                                                          // checksum will be calculated on write
        data[16..20].copy_from_slice(&0u32.to_le_bytes()); // num_entries
        data[20..24].copy_from_slice(&(PAGE_SIZE as u32 - 64).to_le_bytes()); // free_space

        // Write the initialized page
        self.write_page(&page)?;

        Ok(page_id)
    }

    pub fn free_page(&mut self, page_id: u32) {
        if page_id > 0 && page_id < self.next_page_id {
            self.free_pages.push(page_id);
        }
    }

    pub fn get_page(&self, page_id: u32) -> Result<Page> {
        if page_id >= self.next_page_id {
            return Err(Error::PageNotFound(0));
        }

        let offset = page_id as usize * PAGE_SIZE;
        if offset + PAGE_SIZE > self.mmap.len() {
            return Err(Error::PageNotFound(0));
        }

        let mut page_data = [0u8; PAGE_SIZE];
        page_data.copy_from_slice(&self.mmap[offset..offset + PAGE_SIZE]);

        let page = Page::with_data(page_id, page_data);

        // Skip checksum verification for empty pages (all zeros)
        let is_empty = page_data.iter().all(|&b| b == 0);

        // Verify page checksum for data pages (skip for header page and empty pages)
        if page_id > 0 && !is_empty && !page.verify_checksum() {
            return Err(Error::Corruption(String::from("Corrupted page")));
        }

        Ok(page)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        if page.id >= self.next_page_id {
            return Err(Error::PageNotFound(0));
        }

        let offset = page.id as usize * PAGE_SIZE;
        if offset + PAGE_SIZE > self.mmap.len() {
            return Err(Error::PageNotFound(0));
        }

        // Calculate and update checksum before writing
        let mut page_data = *page.get_data();
        if page.id > 0 {
            // Don't update checksum for header page
            let checksum = {
                let mut hasher = Hasher::new();
                hasher.update(&page_data[16..]);
                hasher.finalize()
            };
            page_data[12..16].copy_from_slice(&checksum.to_le_bytes());
        }

        self.mmap[offset..offset + PAGE_SIZE].copy_from_slice(&page_data);
        self.mmap.flush()?;

        Ok(())
    }

    fn grow_file(&mut self, new_size: u64) -> Result<()> {
        // Calculate new size with some overhead
        let grow_size = std::cmp::max(new_size, self.file_size * 2);

        // Set new file length with retry
        RetryableOperations::file_operation(|| {
            self.file
                .set_len(grow_size)
                .map_err(|e| Error::Io(format!("Failed to grow file: {}", e)))
        })?;
        self.file_size = grow_size;

        // Recreate mmap with new size using retry
        // SAFETY: File is open with read/write access and has been resized.
        // Old mmap is dropped below via std::mem::replace.
        let new_mmap = RetryableOperations::file_operation(|| {
            unsafe { MmapOptions::new().map_mut(&self.file) }
                .map_err(|e| Error::Io(format!("Failed to recreate memory map after grow: {}", e)))
        })?;

        drop(std::mem::replace(&mut self.mmap, new_mmap));

        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        // Flush memory map with retry
        RetryableOperations::file_operation(|| {
            self.mmap
                .flush()
                .map_err(|e| Error::Io(format!("Failed to flush memory map: {}", e)))
        })?;

        // Sync file with retry
        RetryableOperations::file_operation(|| {
            self.file
                .sync_all()
                .map_err(|e| Error::Io(format!("Failed to sync file: {}", e)))
        })?;

        Ok(())
    }

    pub fn page_count(&self) -> u32 {
        self.next_page_id
    }

    pub fn free_page_count(&self) -> usize {
        self.free_pages.len()
    }

    /// Check if a page is in the free list
    pub fn is_free_page(&self, page_id: PageId) -> bool {
        self.free_pages.contains(&page_id)
    }
}

// Wrapper to make PageManager work with the trait
pub struct ThreadSafePageManager {
    inner: Arc<Mutex<PageManager>>,
}

impl ThreadSafePageManager {
    pub fn new(manager: PageManager) -> Self {
        Self {
            inner: Arc::new(Mutex::new(manager)),
        }
    }
}

impl PageManagerTrait for ThreadSafePageManager {
    fn allocate_page(&self) -> Result<u32> {
        self.inner
            .lock()
            .map_err(|_| Error::LockFailed {
                resource: "PageManager mutex".to_string(),
            })?
            .allocate_page()
    }

    fn free_page(&self, page_id: u32) {
        // Free page is void, so we can't propagate errors easily
        // Log and continue on lock failure
        match self.inner.lock() {
            Ok(mut guard) => guard.free_page(page_id),
            Err(_) => {
                tracing::error!(
                    "Failed to acquire lock for free_page, page {} may leak",
                    page_id
                );
            }
        }
    }

    fn get_page(&self, page_id: u32) -> Result<Page> {
        self.inner
            .lock()
            .map_err(|_| Error::LockFailed {
                resource: "PageManager mutex".to_string(),
            })?
            .get_page(page_id)
    }

    fn write_page(&self, page: &Page) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| Error::LockFailed {
                resource: "PageManager mutex".to_string(),
            })?
            .write_page(page)
    }

    fn sync(&self) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| Error::LockFailed {
                resource: "PageManager mutex".to_string(),
            })?
            .sync()
    }

    fn page_count(&self) -> u32 {
        match self.inner.lock() {
            Ok(guard) => guard.page_count(),
            Err(_) => {
                tracing::error!("Failed to acquire lock for page_count, returning 0");
                0
            }
        }
    }

    fn free_page_count(&self) -> usize {
        match self.inner.lock() {
            Ok(guard) => guard.free_page_count(),
            Err(_) => {
                tracing::error!("Failed to acquire lock for free_page_count, returning 0");
                0
            }
        }
    }
}

// PageManager cannot implement PageManagerTrait directly because it requires mutable access
// Use ThreadSafePageManager or PageManagerWrapper instead
