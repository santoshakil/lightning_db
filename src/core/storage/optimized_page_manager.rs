use crate::core::error::{Error, Result};
use super::{MmapConfig, OptimizedMmapManager, Page, PAGE_SIZE};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

/// Page manager that uses optimized memory-mapped file I/O
#[derive(Debug)]
pub struct OptimizedPageManager {
    mmap_manager: Arc<OptimizedMmapManager>,
    free_pages: Arc<RwLock<HashSet<u32>>>,
    next_page_id: Arc<RwLock<u32>>,
    page_size: usize,
}

impl OptimizedPageManager {
    pub fn create<P: AsRef<Path>>(path: P, initial_size: u64, config: MmapConfig) -> Result<Self> {
        let mmap_manager = Arc::new(OptimizedMmapManager::create(path, initial_size, config)?);

        let manager = Self {
            mmap_manager,
            free_pages: Arc::new(RwLock::new(HashSet::new())),
            next_page_id: Arc::new(RwLock::new(1)), // Page 0 is reserved for header
            page_size: PAGE_SIZE,
        };

        manager.init_header_page()?;
        info!(
            "Created optimized page manager with initial size: {} bytes",
            initial_size
        );

        Ok(manager)
    }

    pub fn open<P: AsRef<Path>>(path: P, config: MmapConfig) -> Result<Self> {
        let mmap_manager = Arc::new(OptimizedMmapManager::open(path, config)?);

        let manager = Self {
            mmap_manager,
            free_pages: Arc::new(RwLock::new(HashSet::new())),
            next_page_id: Arc::new(RwLock::new(1)),
            page_size: PAGE_SIZE,
        };

        manager.load_header_page()?;
        info!("Opened optimized page manager");

        Ok(manager)
    }

    fn init_header_page(&self) -> Result<()> {
        let mut header_data = vec![0u8; self.page_size];

        // Magic number
        header_data[0..4].copy_from_slice(&crate::core::storage::page::MAGIC.to_le_bytes());

        // Version
        header_data[4..8].copy_from_slice(&1u32.to_le_bytes());

        // Page type (header = 3)
        header_data[8..12].copy_from_slice(&3u32.to_le_bytes());

        // Calculate checksum
        let checksum = {
            use crc32fast::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(&header_data[16..]);
            hasher.finalize()
        };
        header_data[12..16].copy_from_slice(&checksum.to_le_bytes());

        // Write header page
        self.mmap_manager.write(0, &header_data)?;
        self.mmap_manager.sync()?;

        debug!("Initialized header page");
        Ok(())
    }

    fn load_header_page(&self) -> Result<()> {
        let mut header_data = vec![0u8; self.page_size];
        self.mmap_manager.read(0, &mut header_data)?;

        // Verify magic number
        let magic = u32::from_le_bytes([
            header_data[0],
            header_data[1],
            header_data[2],
            header_data[3],
        ]);

        if magic != crate::core::storage::page::MAGIC {
            return Err(Error::InvalidDatabase);
        }

        // Verify checksum
        let stored_checksum = u32::from_le_bytes([
            header_data[12],
            header_data[13],
            header_data[14],
            header_data[15],
        ]);

        let calculated_checksum = {
            use crc32fast::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(&header_data[16..]);
            hasher.finalize()
        };

        if stored_checksum != calculated_checksum {
            return Err(Error::CorruptedPage);
        }

        // Calculate next page ID based on file size
        let stats = self.mmap_manager.get_statistics();
        let next_id = (stats.file_size / self.page_size as u64) as u32;
        *self.next_page_id.write() = next_id;

        debug!("Loaded header page, next page ID: {}", next_id);
        Ok(())
    }

    pub fn allocate_page(&self) -> Result<u32> {
        // Try to reuse a free page
        {
            let mut free_pages = self.free_pages.write();
            if let Some(&page_id) = free_pages.iter().next() {
                free_pages.remove(&page_id);
                debug!("Allocated free page: {}", page_id);
                return Ok(page_id);
            }
        }

        // Allocate new page
        let mut next_id = self.next_page_id.write();
        let page_id = *next_id;
        *next_id += 1;

        // Grow file if needed
        let required_size = (*next_id as u64) * self.page_size as u64;
        let stats = self.mmap_manager.get_statistics();
        if required_size > stats.file_size {
            let new_size = (stats.file_size * 2).max(required_size);
            self.mmap_manager.grow(new_size)?;
            debug!("Grew file to {} bytes for page {}", new_size, page_id);
        }

        // Initialize the new page with empty data
        let mut page = Page::new(page_id);
        // Set page type to 0 (leaf) by default
        let data = page.get_mut_data();
        data[0..4].copy_from_slice(&crate::core::storage::MAGIC.to_le_bytes());
        data[4..8].copy_from_slice(&1u32.to_le_bytes()); // version
        data[8..12].copy_from_slice(&0u32.to_le_bytes()); // page type (leaf)
                                                          // checksum will be calculated on write
        data[16..20].copy_from_slice(&0u32.to_le_bytes()); // num_entries
        data[20..24].copy_from_slice(&(PAGE_SIZE as u32 - 64).to_le_bytes()); // free_space

        // Write the initialized page
        self.write_page(&page)?;

        debug!("Allocated and initialized new page: {}", page_id);
        Ok(page_id)
    }

    pub fn free_page(&self, page_id: u32) {
        if page_id > 0 {
            self.free_pages.write().insert(page_id);
            debug!("Freed page: {}", page_id);
        }
    }

    pub fn get_page(&self, page_id: u32) -> Result<Page> {
        let next_id = *self.next_page_id.read();
        if page_id >= next_id {
            return Err(Error::InvalidPageId);
        }

        let offset = page_id as u64 * self.page_size as u64;
        let mut page_data = vec![0u8; self.page_size];

        self.mmap_manager.read(offset, &mut page_data)?;

        // Convert to array
        let mut data_array = [0u8; PAGE_SIZE];
        data_array.copy_from_slice(&page_data[..PAGE_SIZE]);

        let page = Page::with_data(page_id, data_array);

        // Skip checksum verification for empty pages
        let is_empty = page_data.iter().all(|&b| b == 0);

        // Verify page checksum for data pages
        if page_id > 0 && !is_empty && !page.verify_checksum() {
            return Err(Error::CorruptedPage);
        }

        Ok(page)
    }

    pub fn write_page(&self, page: &Page) -> Result<()> {
        let next_id = *self.next_page_id.read();
        if page.id >= next_id {
            return Err(Error::InvalidPageId);
        }

        let offset = page.id as u64 * self.page_size as u64;

        // Calculate and update checksum before writing
        let mut page_data = *page.get_data();
        if page.id > 0 {
            let checksum = {
                use crc32fast::Hasher;
                let mut hasher = Hasher::new();
                hasher.update(&page_data[16..]);
                hasher.finalize()
            };
            page_data[12..16].copy_from_slice(&checksum.to_le_bytes());
        }

        self.mmap_manager.write(offset, &page_data)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.mmap_manager.sync()
    }

    pub fn page_count(&self) -> u32 {
        *self.next_page_id.read()
    }

    pub fn free_page_count(&self) -> usize {
        self.free_pages.read().len()
    }

    pub fn get_statistics(&self) -> OptimizedPageManagerStats {
        let mmap_stats = self.mmap_manager.get_statistics();

        OptimizedPageManagerStats {
            page_count: self.page_count(),
            free_page_count: self.free_page_count(),
            mmap_stats,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OptimizedPageManagerStats {
    pub page_count: u32,
    pub free_page_count: usize,
    pub mmap_stats: crate::core::storage::MmapStatistics,
}

use super::PageManagerTrait;

impl PageManagerTrait for OptimizedPageManager {
    fn allocate_page(&self) -> Result<u32> {
        Self::allocate_page(self)
    }

    fn free_page(&self, page_id: u32) {
        Self::free_page(self, page_id)
    }

    fn get_page(&self, page_id: u32) -> Result<Page> {
        Self::get_page(self, page_id)
    }

    fn write_page(&self, page: &Page) -> Result<()> {
        Self::write_page(self, page)
    }

    fn sync(&self) -> Result<()> {
        Self::sync(self)
    }

    fn page_count(&self) -> u32 {
        Self::page_count(self)
    }

    fn free_page_count(&self) -> usize {
        Self::free_page_count(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    #[ignore = "Test hangs on some systems"]
    fn test_optimized_page_manager() {
        let dir = tempdir().expect("Failed to create temp directory for page manager test");
        let path = dir.path().join("test.db");

        let config = MmapConfig {
            region_size: 1024 * 1024, // 1MB regions
            ..Default::default()
        };

        let manager = OptimizedPageManager::create(&path, 16 * PAGE_SIZE as u64, config)
            .expect("Failed to create optimized page manager");

        // Test page allocation
        let page_id = manager.allocate_page()
            .expect("Failed to allocate page");
        assert_eq!(page_id, 1);

        // Test page write/read
        let mut page = Page::new(page_id);
        let data = page.get_mut_data();
        data[0] = 42;
        data[1] = 43;

        manager.write_page(&page)
            .expect("Failed to write page");

        let read_page = manager.get_page(page_id)
            .expect("Failed to read page");
        assert_eq!(read_page.get_data()[0], 42);
        assert_eq!(read_page.get_data()[1], 43);

        // Test free/realloc
        manager.free_page(page_id);
        let new_id = manager.allocate_page()
            .expect("Failed to allocate page after free");
        assert_eq!(new_id, page_id);

        // Test statistics
        let stats = manager.get_statistics();
        assert!(stats.page_count > 0);
        assert!(stats.mmap_stats.total_regions > 0);
    }
}
