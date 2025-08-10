use crate::cache::{AdaptivePolicy, ArcCache, CachePolicy, CachedPage, LruPolicy, MemoryConfig};
use crate::error::{Error, Result};
use crate::storage::{Page, PageManager};
use crossbeam_epoch as epoch;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct MemoryPoolStats {
    pub hot_cache_hits: u64,
    pub cold_cache_hits: u64,
    pub cache_misses: u64,
    pub hot_cache_size: u64,
    pub cold_cache_size: u64,
    pub hot_cache_entries: u64,
    pub cold_cache_entries: u64,
    pub evictions: u64,
}

pub struct MemoryPool {
    arc_cache: Arc<ArcCache>,
    page_manager: Arc<RwLock<PageManager>>,
    config: MemoryConfig,
    policy: Arc<dyn CachePolicy>,

    // Background thread handles
    prefetch_thread: Option<thread::JoinHandle<()>>,
    eviction_thread: Option<thread::JoinHandle<()>>,
    shutdown: Arc<AtomicUsize>, // 0 = running, 1 = shutdown
}

impl std::fmt::Debug for MemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryPool")
            .field("config", &self.config)
            .field(
                "shutdown",
                &self.shutdown.load(std::sync::atomic::Ordering::Relaxed),
            )
            .field("prefetch_thread", &self.prefetch_thread.is_some())
            .field("eviction_thread", &self.eviction_thread.is_some())
            .finish()
    }
}

impl MemoryPool {
    pub fn new(page_manager: Arc<RwLock<PageManager>>, config: MemoryConfig) -> Self {
        let cache_capacity = config.hot_cache_size / crate::storage::PAGE_SIZE;
        let arc_cache = Arc::new(ArcCache::new(cache_capacity));

        // Choose policy based on configuration
        let policy: Arc<dyn CachePolicy> = if config.prefetch_size > 0 {
            Arc::new(AdaptivePolicy::new(3))
        } else {
            Arc::new(LruPolicy::new(1000))
        };

        let shutdown = Arc::new(AtomicUsize::new(0));

        let mut pool = Self {
            arc_cache: arc_cache.clone(),
            page_manager: page_manager.clone(),
            config: config.clone(),
            policy: policy.clone(),
            prefetch_thread: None,
            eviction_thread: None,
            shutdown: shutdown.clone(),
        };

        // Start background threads
        pool.start_background_threads();

        pool
    }

    fn start_background_threads(&mut self) {
        // Prefetch thread
        if self.config.prefetch_size > 0 {
            let arc_cache = self.arc_cache.clone();
            let _page_manager = self.page_manager.clone();
            let _policy = self.policy.clone();
            let _prefetch_size = self.config.prefetch_size;
            let shutdown = self.shutdown.clone();

            self.prefetch_thread = Some(thread::spawn(move || {
                let guard = epoch::pin();

                while shutdown.load(Ordering::Relaxed) == 0 {
                    // Sleep for a bit
                    thread::sleep(Duration::from_millis(100));

                    // Collect recent accesses for prefetching
                    let stats = arc_cache.stats();
                    let hit_rate = stats.hit_rate();

                    // Only prefetch if hit rate is good (avoiding thrashing)
                    if hit_rate > 0.5 {
                        // This is simplified - in production, track actual access patterns
                        guard.flush();
                    }
                }
            }));
        }

        // Eviction thread
        let arc_cache = self.arc_cache.clone();
        let page_manager = self.page_manager.clone();
        let eviction_batch = self.config.eviction_batch;
        let shutdown = self.shutdown.clone();

        self.eviction_thread = Some(thread::spawn(move || {
            while shutdown.load(Ordering::Relaxed) == 0 {
                thread::sleep(Duration::from_secs(1));

                // Write back dirty pages
                let dirty_pages = arc_cache.get_dirty_pages();

                if !dirty_pages.is_empty() {
                    let mut page_mgr = page_manager.write();

                    for (page_id, cached_page) in dirty_pages.iter().take(eviction_batch) {
                        // Skip cache-only pages (those with IDs >= 1000000)
                        // These are synthetic pages used for key-value caching
                        if *page_id >= 1000000 {
                            // Mark cache-only pages as clean without writing
                            arc_cache.mark_clean(*page_id);
                            continue;
                        }

                        match page_mgr.write_page(&cached_page.page) {
                            Ok(_) => arc_cache.mark_clean(*page_id),
                            Err(e) => {
                                // Log error but continue - some pages might become invalid during shutdown
                                if shutdown.load(Ordering::Relaxed) == 0 {
                                    eprintln!("Failed to write dirty page {}: {:?}", page_id, e);
                                }
                            }
                        }
                    }
                }
            }
        }));
    }

    pub fn get_page(&self, page_id: u32) -> Result<Arc<Page>> {
        // Check cache first
        if let Some(cached_page) = self.arc_cache.get(page_id) {
            self.policy.on_access(page_id);
            return Ok(cached_page.page.clone());
        }

        // Load from disk
        let page_mgr = self.page_manager.read();
        let page = page_mgr.get_page(page_id)?;
        drop(page_mgr);

        // Insert into cache
        self.arc_cache.insert(page_id, page.clone())?;
        self.policy.on_access(page_id);

        // Trigger prefetch for related pages
        if self.config.prefetch_size > 0 {
            let prefetch_pages = self.policy.should_prefetch(page_id);
            self.prefetch_pages_async(prefetch_pages);
        }

        Ok(Arc::new(page))
    }

    pub fn get_page_mut(&self, page_id: u32) -> Result<Arc<CachedPage>> {
        // Check cache first
        if let Some(cached_page) = self.arc_cache.get(page_id) {
            cached_page.mark_dirty();
            self.policy.on_access(page_id);
            return Ok(cached_page);
        }

        // Load from disk
        let page_mgr = self.page_manager.read();
        let page = page_mgr.get_page(page_id)?;
        drop(page_mgr);

        // Insert into cache and mark dirty
        self.arc_cache.insert(page_id, page)?;

        if let Some(cached_page) = self.arc_cache.get(page_id) {
            cached_page.mark_dirty();
            self.policy.on_access(page_id);
            Ok(cached_page)
        } else {
            Err(Error::Memory)
        }
    }

    pub fn allocate_page(&self) -> Result<u32> {
        let mut page_mgr = self.page_manager.write();
        let page_id = page_mgr.allocate_page()?;

        // Pre-populate cache with new page
        let new_page = Page::new(page_id);
        drop(page_mgr);

        self.arc_cache.insert(page_id, new_page)?;

        Ok(page_id)
    }

    pub fn free_page(&self, page_id: u32) -> Result<()> {
        // Remove from cache
        self.arc_cache.remove(page_id);

        // Free in page manager
        let mut page_mgr = self.page_manager.write();
        page_mgr.free_page(page_id);

        Ok(())
    }

    fn prefetch_pages_async(&self, page_ids: Vec<u32>) {
        if page_ids.is_empty() {
            return;
        }

        let arc_cache = self.arc_cache.clone();
        let page_manager = self.page_manager.clone();

        // Spawn a task to prefetch pages
        thread::spawn(move || {
            let page_mgr = page_manager.read();

            for page_id in page_ids {
                // Skip if already in cache
                if arc_cache.get(page_id).is_some() {
                    continue;
                }

                // Try to load page
                if let Ok(page) = page_mgr.get_page(page_id) {
                    let _ = arc_cache.insert(page_id, page);
                }
            }
        });
    }

    pub fn flush(&self) -> Result<()> {
        // Write all dirty pages
        let dirty_pages = self.arc_cache.get_dirty_pages();
        let mut page_mgr = self.page_manager.write();

        for (page_id, cached_page) in dirty_pages {
            // Skip cache-only pages (those with IDs >= 1000000)
            // These are synthetic pages used for key-value caching
            if page_id >= 1000000 {
                // Mark cache-only pages as clean without writing
                self.arc_cache.mark_clean(page_id);
                continue;
            }

            page_mgr.write_page(&cached_page.page)?;
            self.arc_cache.mark_clean(page_id);
        }

        page_mgr.sync()?;

        Ok(())
    }

    pub fn clear_cache(&self) {
        self.arc_cache.clear();
    }

    pub fn cache_stats(&self) -> String {
        let stats = self.arc_cache.stats();
        let size = self.arc_cache.size();
        let p = self.arc_cache.get_p();

        format!(
            "Cache Stats: size={}, p={}, hits={}, misses={}, hit_rate={:.2}%, evictions={}",
            size,
            p,
            stats.hits.load(Ordering::Relaxed),
            stats.misses.load(Ordering::Relaxed),
            stats.hit_rate() * 100.0,
            stats.evictions.load(Ordering::Relaxed)
        )
    }

    pub fn cache_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Create a hash of the key to use as page_id
        let page_id = self.key_to_page_id(key);

        // Check if this page is in cache
        if let Some(cached_page) = self.arc_cache.get(page_id) {
            // Extract value from the cached page
            // For simplicity, we'll store key-value pairs in page data
            if let Some(value) = self.extract_value_from_page(&cached_page.page, key) {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    pub fn cache_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Create a hash of the key to use as page_id
        let page_id = self.key_to_page_id(key);

        // Create or update the page with this key-value pair
        let page = if let Some(cached_page) = self.arc_cache.get(page_id) {
            // Update existing page
            let mut page = (*cached_page.page).clone();
            self.update_page_with_kv(&mut page, key, value)?;
            // Don't mark cache-only pages as dirty - they shouldn't be persisted
            if page_id < 1000000 {
                cached_page.mark_dirty();
            }
            page
        } else {
            // Create new page
            let mut page = Page::new(page_id);
            self.update_page_with_kv(&mut page, key, value)?;
            page
        };

        // Insert into cache
        self.arc_cache.insert(page_id, page)?;

        Ok(())
    }

    pub fn cache_remove(&self, key: &[u8]) -> Result<()> {
        // Create a hash of the key to use as page_id
        let page_id = self.key_to_page_id(key);

        // Simply remove the entire page from cache
        // This ensures the key won't be found in cache and will force
        // a lookup from the underlying storage layers
        self.arc_cache.remove(page_id);

        Ok(())
    }

    fn key_to_page_id(&self, key: &[u8]) -> u32 {
        // Simple hash function to map keys to page IDs
        let mut hash: u32 = 0;
        for byte in key {
            hash = hash.wrapping_mul(31).wrapping_add(*byte as u32);
        }
        // Use modulo to limit the range
        hash % 1000000 + 1000000 // Start from 1000000 to avoid conflicts with B-tree pages
    }

    fn extract_value_from_page(&self, page: &Page, key: &[u8]) -> Option<Vec<u8>> {
        // Simple implementation: assume page stores multiple key-value pairs
        // Format: [num_entries][key_len][key][value_len][value]...
        let data = page.data();
        if data.len() < 4 {
            return None;
        }

        let mut offset = 0;
        let num_entries = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        offset += 4;

        for _ in 0..num_entries {
            if offset + 4 > data.len() {
                break;
            }

            let key_len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + key_len + 4 > data.len() {
                break;
            }

            let stored_key = &data[offset..offset + key_len];
            offset += key_len;

            let value_len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + value_len > data.len() {
                break;
            }

            if stored_key == key {
                return Some(data[offset..offset + value_len].to_vec());
            }

            offset += value_len;
        }

        None
    }

    fn update_page_with_kv(&self, page: &mut Page, key: &[u8], value: &[u8]) -> Result<()> {
        // Simple implementation: store multiple key-value pairs
        let mut entries = Vec::new();

        // First, extract existing entries (excluding the one being updated)
        let data = page.data();
        if data.len() >= 4 {
            let mut offset = 0;
            let num_entries = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            offset += 4;

            for _ in 0..num_entries {
                if offset + 4 > data.len() {
                    break;
                }

                let key_len = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;

                if offset + key_len + 4 > data.len() {
                    break;
                }

                let stored_key = data[offset..offset + key_len].to_vec();
                offset += key_len;

                let value_len = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;

                if offset + value_len > data.len() {
                    break;
                }

                let stored_value = data[offset..offset + value_len].to_vec();
                offset += value_len;

                if stored_key != key {
                    entries.push((stored_key, stored_value));
                }
            }
        }

        // Add the new/updated entry
        entries.push((key.to_vec(), value.to_vec()));

        // Serialize back to page
        let mut new_data = Vec::new();
        new_data.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        for (k, v) in entries {
            new_data.extend_from_slice(&(k.len() as u32).to_le_bytes());
            new_data.extend_from_slice(&k);
            new_data.extend_from_slice(&(v.len() as u32).to_le_bytes());
            new_data.extend_from_slice(&v);
        }

        page.set_data(&new_data)?;
        Ok(())
    }

    pub fn get_cache_stats(&self) -> MemoryPoolStats {
        let stats = self.arc_cache.get_stats();
        MemoryPoolStats {
            hot_cache_hits: stats.recent_hits,
            cold_cache_hits: stats.frequent_hits,
            cache_misses: stats.misses,
            hot_cache_size: (stats.recent_entries * crate::storage::PAGE_SIZE) as u64,
            cold_cache_size: (stats.frequent_entries * crate::storage::PAGE_SIZE) as u64,
            hot_cache_entries: stats.recent_entries as u64,
            cold_cache_entries: stats.frequent_entries as u64,
            evictions: stats.evictions,
        }
    }

    pub fn shutdown(&mut self) {
        // Signal shutdown
        self.shutdown.store(1, Ordering::Relaxed);

        // Flush before shutdown
        let _ = self.flush();

        // Wait for threads to finish
        if let Some(thread) = self.prefetch_thread.take() {
            let _ = thread.join();
        }

        if let Some(thread) = self.eviction_thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for MemoryPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}
