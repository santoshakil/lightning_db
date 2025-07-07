use crossbeam::queue::SegQueue;
use dashmap::{DashMap, DashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Lock-free page allocation tracker
/// Replaces RwLock<HashSet<u32>> for free page tracking
pub struct LockFreePageTracker {
    // Free pages stored in a lock-free queue
    free_pages: Arc<SegQueue<u32>>,

    // Set of allocated pages for quick lookup
    allocated_pages: Arc<DashSet<u32>>,

    // Next page ID counter
    next_page_id: AtomicU32,

    // Statistics
    total_pages: AtomicUsize,
    free_count: AtomicUsize,
    allocated_count: AtomicUsize,

    // High water mark
    max_page_id: AtomicU32,
}

impl LockFreePageTracker {
    pub fn new(starting_page_id: u32) -> Self {
        Self {
            free_pages: Arc::new(SegQueue::new()),
            allocated_pages: Arc::new(DashSet::new()),
            next_page_id: AtomicU32::new(starting_page_id),
            total_pages: AtomicUsize::new(0),
            free_count: AtomicUsize::new(0),
            allocated_count: AtomicUsize::new(0),
            max_page_id: AtomicU32::new(starting_page_id),
        }
    }

    /// Allocate a new page (lock-free)
    pub fn allocate(&self) -> PageAllocation {
        // First try to reuse a free page
        if let Some(page_id) = self.free_pages.pop() {
            self.allocated_pages.insert(page_id);
            self.free_count.fetch_sub(1, Ordering::Relaxed);
            self.allocated_count.fetch_add(1, Ordering::Relaxed);

            PageAllocation {
                page_id,
                is_new: false,
            }
        } else {
            // Allocate a new page
            let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
            self.allocated_pages.insert(page_id);
            self.total_pages.fetch_add(1, Ordering::Relaxed);
            self.allocated_count.fetch_add(1, Ordering::Relaxed);

            // Update high water mark
            self.update_max_page_id(page_id);

            PageAllocation {
                page_id,
                is_new: true,
            }
        }
    }

    /// Free a page (lock-free)
    pub fn free(&self, page_id: u32) -> bool {
        if self.allocated_pages.remove(&page_id).is_some() {
            self.free_pages.push(page_id);
            self.free_count.fetch_add(1, Ordering::Relaxed);
            self.allocated_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Batch allocate multiple pages
    pub fn allocate_batch(&self, count: usize) -> Vec<PageAllocation> {
        let mut allocations = Vec::with_capacity(count);

        // Try to get as many free pages as possible
        for _ in 0..count {
            if let Some(page_id) = self.free_pages.pop() {
                self.allocated_pages.insert(page_id);
                allocations.push(PageAllocation {
                    page_id,
                    is_new: false,
                });
            } else {
                break;
            }
        }

        let reused = allocations.len();
        if reused > 0 {
            self.free_count.fetch_sub(reused, Ordering::Relaxed);
            self.allocated_count.fetch_add(reused, Ordering::Relaxed);
        }

        // Allocate remaining as new pages
        let remaining = count - reused;
        if remaining > 0 {
            let start_id = self
                .next_page_id
                .fetch_add(remaining as u32, Ordering::SeqCst);

            for i in 0..remaining {
                let page_id = start_id + i as u32;
                self.allocated_pages.insert(page_id);
                allocations.push(PageAllocation {
                    page_id,
                    is_new: true,
                });
            }

            self.total_pages.fetch_add(remaining, Ordering::Relaxed);
            self.allocated_count.fetch_add(remaining, Ordering::Relaxed);
            self.update_max_page_id(start_id + remaining as u32 - 1);
        }

        allocations
    }

    /// Batch free multiple pages
    pub fn free_batch(&self, page_ids: &[u32]) {
        let mut freed = 0;

        for &page_id in page_ids {
            if self.allocated_pages.remove(&page_id).is_some() {
                self.free_pages.push(page_id);
                freed += 1;
            }
        }

        if freed > 0 {
            self.free_count.fetch_add(freed, Ordering::Relaxed);
            self.allocated_count.fetch_sub(freed, Ordering::Relaxed);
        }
    }

    /// Check if a page is allocated
    pub fn is_allocated(&self, page_id: u32) -> bool {
        self.allocated_pages.contains(&page_id)
    }

    /// Get current statistics
    pub fn stats(&self) -> PageTrackerStats {
        PageTrackerStats {
            total_pages: self.total_pages.load(Ordering::Relaxed),
            free_pages: self.free_count.load(Ordering::Relaxed),
            allocated_pages: self.allocated_count.load(Ordering::Relaxed),
            next_page_id: self.next_page_id.load(Ordering::Relaxed),
            max_page_id: self.max_page_id.load(Ordering::Relaxed),
        }
    }

    /// Reset tracker (for testing)
    pub fn reset(&self, starting_page_id: u32) {
        // Clear allocated pages
        self.allocated_pages.clear();

        // Clear free pages
        while self.free_pages.pop().is_some() {}

        // Reset counters
        self.next_page_id.store(starting_page_id, Ordering::SeqCst);
        self.total_pages.store(0, Ordering::Relaxed);
        self.free_count.store(0, Ordering::Relaxed);
        self.allocated_count.store(0, Ordering::Relaxed);
        self.max_page_id.store(starting_page_id, Ordering::Relaxed);
    }

    fn update_max_page_id(&self, page_id: u32) {
        loop {
            let current_max = self.max_page_id.load(Ordering::Relaxed);
            if page_id <= current_max {
                break;
            }

            if self
                .max_page_id
                .compare_exchange_weak(current_max, page_id, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PageAllocation {
    pub page_id: u32,
    pub is_new: bool,
}

#[derive(Debug, Clone)]
pub struct PageTrackerStats {
    pub total_pages: usize,
    pub free_pages: usize,
    pub allocated_pages: usize,
    pub next_page_id: u32,
    pub max_page_id: u32,
}

/// Concurrent bitmap for tracking large numbers of pages efficiently
pub struct PageBitmap {
    // Each u64 tracks 64 pages
    bits: DashMap<u32, AtomicU64>,
    pages_per_chunk: u32,
}

impl Default for PageBitmap {
    fn default() -> Self {
        Self::new()
    }
}

impl PageBitmap {
    pub fn new() -> Self {
        Self {
            bits: DashMap::new(),
            pages_per_chunk: 64,
        }
    }

    /// Set a page as allocated
    pub fn set(&self, page_id: u32) {
        let chunk_idx = page_id / self.pages_per_chunk;
        let bit_idx = page_id % self.pages_per_chunk;
        let bit_mask = 1u64 << bit_idx;

        self.bits
            .entry(chunk_idx)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_or(bit_mask, Ordering::Relaxed);
    }

    /// Clear a page as free
    pub fn clear(&self, page_id: u32) {
        let chunk_idx = page_id / self.pages_per_chunk;
        let bit_idx = page_id % self.pages_per_chunk;
        let bit_mask = !(1u64 << bit_idx);

        if let Some(chunk) = self.bits.get(&chunk_idx) {
            chunk.fetch_and(bit_mask, Ordering::Relaxed);
        }
    }

    /// Test if a page is set
    pub fn test(&self, page_id: u32) -> bool {
        let chunk_idx = page_id / self.pages_per_chunk;
        let bit_idx = page_id % self.pages_per_chunk;
        let bit_mask = 1u64 << bit_idx;

        if let Some(chunk) = self.bits.get(&chunk_idx) {
            (chunk.load(Ordering::Relaxed) & bit_mask) != 0
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::thread;

    #[test]
    fn test_page_tracker() {
        let tracker = Arc::new(LockFreePageTracker::new(1));

        // Test single allocation
        let alloc1 = tracker.allocate();
        assert_eq!(alloc1.page_id, 1);
        assert!(alloc1.is_new);

        // Test free and reuse
        tracker.free(1);
        let alloc2 = tracker.allocate();
        assert_eq!(alloc2.page_id, 1);
        assert!(!alloc2.is_new);

        // Test batch operations
        let batch = tracker.allocate_batch(10);
        assert_eq!(batch.len(), 10);

        let page_ids: Vec<u32> = batch.iter().map(|a| a.page_id).collect();
        tracker.free_batch(&page_ids);

        let stats = tracker.stats();
        assert_eq!(stats.free_pages, 10);
    }

    #[test]
    fn test_concurrent_allocation() {
        let tracker = Arc::new(LockFreePageTracker::new(1));
        let mut handles = vec![];
        let pages_per_thread = 100;
        let num_threads = 4;

        for _ in 0..num_threads {
            let tracker_clone = Arc::clone(&tracker);
            let handle = thread::spawn(move || {
                let mut allocated = Vec::new();
                let mut freed = Vec::new();

                // Allocate pages
                for _ in 0..pages_per_thread {
                    allocated.push(tracker_clone.allocate().page_id);
                }

                // Free half of them
                for i in 0..pages_per_thread / 2 {
                    tracker_clone.free(allocated[i]);
                    freed.push(allocated[i]);
                }

                // Return both allocated and freed pages
                (allocated, freed)
            });
            handles.push(handle);
        }

        let mut all_allocated = Vec::new();
        let mut all_freed = HashSet::new();

        for handle in handles {
            let (allocated, freed) = handle.join().unwrap();
            all_allocated.extend(allocated);
            for page in freed {
                all_freed.insert(page);
            }
        }

        // Check for duplicates only among non-freed pages
        let mut seen = HashSet::new();
        for page in &all_allocated {
            if !all_freed.contains(page) {
                // This page wasn't freed, so it should be unique
                assert!(seen.insert(*page), "Duplicate page allocation: {}", page);
            }
        }

        // Also verify that each allocation within a thread was unique at the time
        for i in 0..num_threads {
            let tracker_clone = Arc::clone(&tracker);
            let handle = thread::spawn(move || {
                let mut allocated_in_thread = HashSet::new();
                for _ in 0..10 {
                    let page = tracker_clone.allocate().page_id;
                    assert!(
                        allocated_in_thread.insert(page),
                        "Thread {} got duplicate page: {}",
                        i,
                        page
                    );
                    tracker_clone.free(page); // Free immediately for reuse
                }
            });
            handle.join().unwrap();
        }

        let stats = tracker.stats();
        // The number of allocated pages + free pages should equal total pages
        assert_eq!(stats.allocated_pages + stats.free_pages, stats.total_pages);
        // Verify we have a reasonable number of pages
        assert!(stats.total_pages > 0);
        assert!(stats.free_pages > 0); // Some pages should be free after the test
    }

    #[test]
    fn test_page_bitmap() {
        let bitmap = PageBitmap::new();

        // Test basic operations
        bitmap.set(100);
        bitmap.set(200);
        assert!(bitmap.test(100));
        assert!(bitmap.test(200));
        assert!(!bitmap.test(150));

        bitmap.clear(100);
        assert!(!bitmap.test(100));
        assert!(bitmap.test(200));
    }
}
