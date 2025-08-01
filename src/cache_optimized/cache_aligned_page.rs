//! Cache-Aligned Page Structure
//!
//! Optimized page layout for maximum CPU cache performance:
//! - 4KB pages aligned to cache line boundaries
//! - Header in first cache line for fast metadata access
//! - Data layout optimized for sequential scanning
//! - Slot directory at end for append-only performance

use super::{CACHE_LINE_SIZE, PrefetchHints, CachePerformanceStats};
use std::sync::atomic::{AtomicU32, AtomicU16, Ordering};
use std::sync::Arc;

/// Standard database page size (4KB)
pub const PAGE_SIZE: usize = 4096;

/// Number of cache lines per page
pub const CACHE_LINES_PER_PAGE: usize = PAGE_SIZE / CACHE_LINE_SIZE;

/// Page header fits in first cache line (64 bytes)
#[repr(C, align(64))]
pub struct CacheAlignedPageHeader {
    /// Page ID
    pub page_id: AtomicU32,
    /// Page type (leaf, internal, data, etc.)
    pub page_type: AtomicU16,
    /// Number of records/slots on page
    pub record_count: AtomicU16,
    /// Free space pointer (grows down from end)
    pub free_space_ptr: AtomicU16,
    /// Slot directory offset (grows up from header)
    pub slot_dir_offset: AtomicU16,
    /// Page sequence number for versioning
    pub seq_num: AtomicU32,
    /// Checksum for integrity verification
    pub checksum: AtomicU32,
    /// Transaction ID that last modified this page
    pub last_txn_id: AtomicU32,
    /// Page flags (dirty, locked, etc.)
    pub flags: AtomicU16,
    /// Reserved for future use
    pub reserved: AtomicU16,
    /// Next page in chain (for overflow)
    pub next_page: AtomicU32,
    /// Previous page in chain
    pub prev_page: AtomicU32,
    /// Level in B+Tree (0 for leaf)
    pub level: AtomicU16,
    /// Padding to cache line boundary
    _padding: [u8; 10],
}

impl CacheAlignedPageHeader {
    pub fn new(page_id: u32, page_type: u16) -> Self {
        Self {
            page_id: AtomicU32::new(page_id),
            page_type: AtomicU16::new(page_type),
            record_count: AtomicU16::new(0),
            free_space_ptr: AtomicU16::new(PAGE_SIZE as u16),
            slot_dir_offset: AtomicU16::new(CACHE_LINE_SIZE as u16),
            seq_num: AtomicU32::new(1),
            checksum: AtomicU32::new(0),
            last_txn_id: AtomicU32::new(0),
            flags: AtomicU16::new(0),
            reserved: AtomicU16::new(0),
            next_page: AtomicU32::new(0),
            prev_page: AtomicU32::new(0),
            level: AtomicU16::new(0),
            _padding: [0; 10],
        }
    }

    /// Get available free space
    pub fn free_space(&self) -> u16 {
        let free_ptr = self.free_space_ptr.load(Ordering::Acquire);
        let slot_offset = self.slot_dir_offset.load(Ordering::Acquire);
        free_ptr.saturating_sub(slot_offset)
    }

    /// Check if page can fit a record of given size
    pub fn can_fit(&self, record_size: u16) -> bool {
        let slot_size = std::mem::size_of::<SlotEntry>() as u16;
        self.free_space() >= record_size + slot_size
    }

    /// Mark page as dirty
    pub fn mark_dirty(&self) {
        self.flags.fetch_or(PageFlags::DIRTY as u16, Ordering::Release);
    }

    /// Check if page is dirty
    pub fn is_dirty(&self) -> bool {
        (self.flags.load(Ordering::Acquire) & PageFlags::DIRTY as u16) != 0
    }

    /// Increment sequence number atomically
    pub fn increment_seq(&self) -> u32 {
        self.seq_num.fetch_add(1, Ordering::AcqRel)
    }
}

/// Page type constants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PageType {
    DataPage = 1,
    IndexPage = 2,
    LeafPage = 3,
    InternalPage = 4,
    OverflowPage = 5,
}

/// Page flags
#[derive(Debug, Clone, Copy)]
#[repr(u16)]
pub enum PageFlags {
    DIRTY = 0x0001,
    LOCKED = 0x0002,
    COMPRESSED = 0x0004,
    ENCRYPTED = 0x0008,
}

/// Slot entry in slot directory (fixed size for cache efficiency)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SlotEntry {
    /// Offset of record from page start
    pub offset: u16,
    /// Length of record
    pub length: u16,
}

impl SlotEntry {
    pub fn new(offset: u16, length: u16) -> Self {
        Self { offset, length }
    }

    pub fn is_valid(&self) -> bool {
        self.offset > 0 && self.length > 0
    }
}

/// Cache-optimized database page
#[repr(C, align(64))]
pub struct CacheAlignedPage {
    /// Page header (first cache line)
    pub header: CacheAlignedPageHeader,
    /// Page data (remaining cache lines)
    pub data: [u8; PAGE_SIZE - CACHE_LINE_SIZE],
}

impl CacheAlignedPage {
    pub fn new(page_id: u32, page_type: PageType) -> Self {
        Self {
            header: CacheAlignedPageHeader::new(page_id, page_type as u16),
            data: [0; PAGE_SIZE - CACHE_LINE_SIZE],
        }
    }

    /// Insert a record into the page
    pub fn insert_record(&mut self, record: &[u8]) -> Result<u16, PageError> {
        let record_len = record.len() as u16;
        
        // Check if we have space
        if !self.header.can_fit(record_len) {
            return Err(PageError::InsufficientSpace);
        }

        // Allocate space for record (grows down from end)
        let current_free = self.header.free_space_ptr.load(Ordering::Acquire);
        let new_free = current_free - record_len;
        
        // Copy record data
        let record_offset = new_free as usize - CACHE_LINE_SIZE;
        self.data[record_offset..record_offset + record.len()].copy_from_slice(record);
        
        // Add slot entry (grows up from header)
        let slot_offset = self.header.slot_dir_offset.load(Ordering::Acquire) as usize - CACHE_LINE_SIZE;
        let slot_entry = SlotEntry::new(new_free, record_len);
        
        unsafe {
            let slot_ptr = self.data.as_mut_ptr().add(slot_offset) as *mut SlotEntry;
            std::ptr::write(slot_ptr, slot_entry);
        }

        // Update page metadata
        self.header.free_space_ptr.store(new_free, Ordering::Release);
        self.header.slot_dir_offset.fetch_add(
            std::mem::size_of::<SlotEntry>() as u16, 
            Ordering::Release
        );
        let slot_idx = self.header.record_count.fetch_add(1, Ordering::Release);
        self.header.mark_dirty();
        self.header.increment_seq();

        Ok(slot_idx)
    }

    /// Get a record by slot index
    pub fn get_record(&self, slot_idx: u16) -> Result<&[u8], PageError> {
        let record_count = self.header.record_count.load(Ordering::Acquire);
        if slot_idx >= record_count {
            return Err(PageError::InvalidSlot);
        }

        // Prefetch the slot directory area
        let slot_area_start = CACHE_LINE_SIZE;
        PrefetchHints::prefetch_read_t0(
            unsafe { self.data.as_ptr().add(slot_area_start) }
        );

        // Get slot entry
        let slot_offset = slot_area_start + (slot_idx as usize * std::mem::size_of::<SlotEntry>());
        let slot_entry = unsafe {
            let slot_ptr = self.data.as_ptr().add(slot_offset) as *const SlotEntry;
            std::ptr::read(slot_ptr)
        };

        if !slot_entry.is_valid() {
            return Err(PageError::InvalidSlot);
        }

        // Prefetch the record data
        let record_offset = slot_entry.offset as usize - CACHE_LINE_SIZE;
        PrefetchHints::prefetch_read_t0(
            unsafe { self.data.as_ptr().add(record_offset) }
        );

        // Return record slice
        let record_end = record_offset + slot_entry.length as usize;
        Ok(&self.data[record_offset..record_end])
    }

    /// Sequential scan with prefetching
    pub fn scan_records<F>(&self, mut callback: F) -> Result<(), PageError>
    where
        F: FnMut(u16, &[u8]) -> bool,
    {
        let record_count = self.header.record_count.load(Ordering::Acquire);
        
        // Prefetch slot directory
        PrefetchHints::prefetch_range(
            unsafe { self.data.as_ptr().add(CACHE_LINE_SIZE) },
            record_count as usize * std::mem::size_of::<SlotEntry>()
        );

        for slot_idx in 0..record_count {
            let record = self.get_record(slot_idx)?;
            
            // Prefetch next record
            if slot_idx + 1 < record_count {
                if let Ok(next_record) = self.get_record(slot_idx + 1) {
                    PrefetchHints::prefetch_read_t0(next_record.as_ptr());
                }
            }
            
            if !callback(slot_idx, record) {
                break; // Callback requested early termination
            }
        }

        Ok(())
    }

    /// Update a record in place
    pub fn update_record(&mut self, slot_idx: u16, new_data: &[u8]) -> Result<(), PageError> {
        let record_count = self.header.record_count.load(Ordering::Acquire);
        if slot_idx >= record_count {
            return Err(PageError::InvalidSlot);
        }

        // Get current slot entry
        let slot_offset = CACHE_LINE_SIZE + (slot_idx as usize * std::mem::size_of::<SlotEntry>());
        let mut slot_entry = unsafe {
            let slot_ptr = self.data.as_ptr().add(slot_offset) as *const SlotEntry;
            std::ptr::read(slot_ptr)
        };

        if !slot_entry.is_valid() {
            return Err(PageError::InvalidSlot);
        }

        // Check if new data fits in existing space
        if new_data.len() as u16 <= slot_entry.length {
            // In-place update
            let record_offset = slot_entry.offset as usize - CACHE_LINE_SIZE;
            self.data[record_offset..record_offset + new_data.len()].copy_from_slice(new_data);
            
            // Update slot length if smaller
            if (new_data.len() as u16) < slot_entry.length {
                slot_entry.length = new_data.len() as u16;
                unsafe {
                    let slot_ptr = self.data.as_mut_ptr().add(slot_offset) as *mut SlotEntry;
                    std::ptr::write(slot_ptr, slot_entry);
                }
            }
        } else {
            // Need to relocate record
            // For simplicity, delete old and insert new
            self.delete_record(slot_idx)?;
            self.insert_record(new_data)?;
        }

        self.header.mark_dirty();
        self.header.increment_seq();
        Ok(())
    }

    /// Delete a record
    pub fn delete_record(&mut self, slot_idx: u16) -> Result<(), PageError> {
        let record_count = self.header.record_count.load(Ordering::Acquire);
        if slot_idx >= record_count {
            return Err(PageError::InvalidSlot);
        }

        // Mark slot as invalid
        let slot_offset = CACHE_LINE_SIZE + (slot_idx as usize * std::mem::size_of::<SlotEntry>());
        let invalid_slot = SlotEntry::new(0, 0);
        
        unsafe {
            let slot_ptr = self.data.as_mut_ptr().add(slot_offset) as *mut SlotEntry;
            std::ptr::write(slot_ptr, invalid_slot);
        }

        self.header.mark_dirty();
        self.header.increment_seq();
        Ok(())
    }

    /// Compact page to reclaim deleted space
    pub fn compact(&mut self) -> Result<(), PageError> {
        let record_count = self.header.record_count.load(Ordering::Acquire);
        let mut valid_records = Vec::new();
        
        // Collect valid records
        for slot_idx in 0..record_count {
            if let Ok(record) = self.get_record(slot_idx) {
                valid_records.push(record.to_vec());
            }
        }

        // Reset page
        self.header.record_count.store(0, Ordering::Release);
        self.header.free_space_ptr.store(PAGE_SIZE as u16, Ordering::Release);
        self.header.slot_dir_offset.store(CACHE_LINE_SIZE as u16, Ordering::Release);

        // Re-insert valid records
        for record in valid_records {
            self.insert_record(&record)?;
        }

        Ok(())
    }

    /// Calculate page utilization
    pub fn utilization(&self) -> f64 {
        let total_space = PAGE_SIZE - CACHE_LINE_SIZE;
        let used_space = total_space - self.header.free_space() as usize;
        used_space as f64 / total_space as f64
    }

    /// Calculate checksum for integrity verification
    pub fn calculate_checksum(&self) -> u32 {
        let mut crc = crc32fast::Hasher::new();
        
        // Include header (except checksum field)
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                &self.header as *const _ as *const u8,
                std::mem::size_of::<CacheAlignedPageHeader>() - 4 // Exclude checksum
            )
        };
        crc.update(header_bytes);
        
        // Include data
        crc.update(&self.data);
        
        crc.finalize()
    }

    /// Verify page integrity
    pub fn verify_integrity(&self) -> bool {
        let stored_checksum = self.header.checksum.load(Ordering::Acquire);
        let calculated_checksum = self.calculate_checksum();
        stored_checksum == calculated_checksum
    }

    /// Update checksum
    pub fn update_checksum(&self) {
        let checksum = self.calculate_checksum();
        self.header.checksum.store(checksum, Ordering::Release);
    }
}

impl Default for CacheAlignedPage {
    fn default() -> Self {
        Self::new(0, PageType::DataPage)
    }
}

/// Page operation errors
#[derive(Debug, thiserror::Error)]
pub enum PageError {
    #[error("Insufficient space on page")]
    InsufficientSpace,
    #[error("Invalid slot index")]
    InvalidSlot,
    #[error("Checksum mismatch")]
    ChecksumMismatch,
    #[error("Page is full")]
    PageFull,
}

/// Page cache optimized for CPU cache performance
pub struct CacheOptimizedPageCache {
    pages: Vec<Option<Arc<CacheAlignedPage>>>,
    stats: Arc<CachePerformanceStats>,
    cache_size: usize,
}

impl CacheOptimizedPageCache {
    pub fn new(cache_size: usize) -> Self {
        Self {
            pages: vec![None; cache_size],
            stats: Arc::new(CachePerformanceStats::new()),
            cache_size,
        }
    }

    pub fn get_page(&self, page_id: u32) -> Option<Arc<CacheAlignedPage>> {
        let slot = (page_id as usize) % self.cache_size;
        
        if let Some(ref page) = self.pages[slot] {
            let cached_id = page.header.page_id.load(Ordering::Acquire);
            if cached_id == page_id {
                // Prefetch the page data
                PrefetchHints::prefetch_read_t0(page.as_ref() as *const _ as *const u8);
                self.stats.record_hit();
                return Some(Arc::clone(page));
            }
        }
        
        self.stats.record_miss();
        None
    }

    pub fn put_page(&mut self, page: Arc<CacheAlignedPage>) {
        let page_id = page.header.page_id.load(Ordering::Acquire);
        let slot = (page_id as usize) % self.cache_size;
        
        // Prefetch the slot for write
        PrefetchHints::prefetch_write(
            &self.pages[slot] as *const _ as *const u8
        );
        
        self.pages[slot] = Some(page);
    }

    pub fn get_stats(&self) -> &CachePerformanceStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_header_alignment() {
        assert_eq!(std::mem::align_of::<CacheAlignedPageHeader>(), CACHE_LINE_SIZE);
        assert_eq!(std::mem::size_of::<CacheAlignedPageHeader>(), CACHE_LINE_SIZE);
    }

    #[test]
    fn test_page_alignment() {
        assert_eq!(std::mem::align_of::<CacheAlignedPage>(), CACHE_LINE_SIZE);
        assert_eq!(std::mem::size_of::<CacheAlignedPage>(), PAGE_SIZE);
    }

    #[test]
    fn test_record_operations() {
        let mut page = CacheAlignedPage::new(1, PageType::DataPage);
        
        // Test insert
        let record1 = b"Hello, World!";
        let slot1 = page.insert_record(record1).unwrap();
        assert_eq!(slot1, 0);
        
        let record2 = b"Lightning DB";
        let slot2 = page.insert_record(record2).unwrap();
        assert_eq!(slot2, 1);
        
        // Test get
        let retrieved1 = page.get_record(slot1).unwrap();
        assert_eq!(retrieved1, record1);
        
        let retrieved2 = page.get_record(slot2).unwrap();
        assert_eq!(retrieved2, record2);
        
        // Test update
        let new_record1 = b"Updated!";
        page.update_record(slot1, new_record1).unwrap();
        let updated = page.get_record(slot1).unwrap();
        assert_eq!(updated, new_record1);
    }

    #[test]
    fn test_page_scan() {
        let mut page = CacheAlignedPage::new(1, PageType::DataPage);
        
        // Insert test records
        let records = vec![b"record1", b"record2", b"record3"];
        for record in &records {
            page.insert_record(*record).unwrap();
        }
        
        // Scan all records
        let mut scanned = Vec::new();
        page.scan_records(|_idx, record| {
            scanned.push(record.to_vec());
            true
        }).unwrap();
        
        assert_eq!(scanned.len(), records.len());
        for (i, record) in records.iter().enumerate() {
            assert_eq!(&scanned[i], record);
        }
    }

    #[test]
    fn test_page_utilization() {
        let mut page = CacheAlignedPage::new(1, PageType::DataPage);
        assert_eq!(page.utilization(), 0.0);
        
        page.insert_record(b"test record").unwrap();
        assert!(page.utilization() > 0.0);
        assert!(page.utilization() < 1.0);
    }

    #[test]
    fn test_checksum_verification() {
        let mut page = CacheAlignedPage::new(1, PageType::DataPage);
        page.insert_record(b"test data").unwrap();
        page.update_checksum();
        
        assert!(page.verify_integrity());
        
        // Manually corrupt data
        page.data[1000] = 0xFF;
        assert!(!page.verify_integrity());
    }

    #[test]
    fn test_page_cache() {
        let mut cache = CacheOptimizedPageCache::new(10);
        let page = Arc::new(CacheAlignedPage::new(42, PageType::DataPage));
        
        // Initially miss
        assert!(cache.get_page(42).is_none());
        
        // Put and hit
        cache.put_page(Arc::clone(&page));
        assert!(cache.get_page(42).is_some());
        
        // Check stats
        let stats = cache.get_stats();
        assert_eq!(stats.cache_hits.load(Ordering::Relaxed), 1);
        assert_eq!(stats.cache_misses.load(Ordering::Relaxed), 1);
    }
}