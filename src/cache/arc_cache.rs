use crate::cache::{CacheStats, CachedPage};
use crate::error::Result;
use crate::storage::Page;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct LruList {
    entries: VecDeque<u32>,
    map: DashMap<u32, usize>, // page_id -> position in entries
}

impl LruList {
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            map: DashMap::new(),
        }
    }

    fn push_front(&mut self, page_id: u32) {
        self.entries.push_front(page_id);
        self.update_positions();
    }

    fn remove(&mut self, page_id: u32) -> bool {
        if let Some(entry) = self.map.remove(&page_id) {
            let (_, pos) = entry;
            self.entries.remove(pos);
            self.update_positions();
            true
        } else {
            false
        }
    }

    fn pop_back(&mut self) -> Option<u32> {
        if let Some(page_id) = self.entries.pop_back() {
            self.map.remove(&page_id);
            self.update_positions();
            Some(page_id)
        } else {
            None
        }
    }

    fn contains(&self, page_id: &u32) -> bool {
        self.map.contains_key(page_id)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn update_positions(&mut self) {
        self.map.clear();
        for (pos, &page_id) in self.entries.iter().enumerate() {
            self.map.insert(page_id, pos);
        }
    }
}

pub struct ArcCache {
    capacity: usize,
    p: AtomicUsize, // Target size for T1

    t1: Arc<Mutex<LruList>>, // Recent cache
    t2: Arc<Mutex<LruList>>, // Frequent cache
    b1: Arc<Mutex<LruList>>, // Ghost recent
    b2: Arc<Mutex<LruList>>, // Ghost frequent

    cache: DashMap<u32, Arc<CachedPage>>,
    stats: Arc<CacheStats>,
    timestamp: AtomicUsize,
}

impl ArcCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            p: AtomicUsize::new(capacity / 2),
            t1: Arc::new(Mutex::new(LruList::new())),
            t2: Arc::new(Mutex::new(LruList::new())),
            b1: Arc::new(Mutex::new(LruList::new())),
            b2: Arc::new(Mutex::new(LruList::new())),
            cache: DashMap::new(),
            stats: Arc::new(CacheStats::new()),
            timestamp: AtomicUsize::new(0),
        }
    }

    pub fn get(&self, page_id: u32) -> Option<Arc<CachedPage>> {
        let timestamp = self.timestamp.fetch_add(1, Ordering::Relaxed);

        if let Some(entry) = self.cache.get(&page_id) {
            let cached_page = entry.value().clone();
            cached_page.access(timestamp);

            // Move from T1 to T2 if in T1
            let mut t1 = self.t1.lock();
            if t1.contains(&page_id) {
                t1.remove(page_id);
                drop(t1);

                let mut t2 = self.t2.lock();
                t2.push_front(page_id);
            }

            self.stats.record_hit();
            Some(cached_page)
        } else {
            self.stats.record_miss();
            None
        }
    }

    pub fn insert(&self, page_id: u32, page: Page) -> Result<()> {
        let timestamp = self.timestamp.fetch_add(1, Ordering::Relaxed);
        let cached_page = Arc::new(CachedPage::new(page));
        cached_page.access(timestamp);

        // Check if page_id is in ghost lists
        let mut b1 = self.b1.lock();
        let mut b2 = self.b2.lock();

        if b1.contains(&page_id) {
            // Adapt p upward
            let delta = if b1.len() >= b2.len() {
                1
            } else {
                b2.len() / b1.len()
            };
            let p_val = self.p.load(Ordering::Relaxed);
            self.p
                .store((p_val + delta).min(self.capacity), Ordering::Relaxed);

            b1.remove(page_id);
            drop(b1);
            drop(b2);

            // Insert into T2
            let mut t2 = self.t2.lock();
            t2.push_front(page_id);
            self.cache.insert(page_id, cached_page);
            drop(t2);

            self.replace(false)?;
        } else if b2.contains(&page_id) {
            // Adapt p downward
            let delta = if b2.len() >= b1.len() {
                1
            } else {
                b1.len() / b2.len()
            };
            let p_val = self.p.load(Ordering::Relaxed);
            self.p.store(p_val.saturating_sub(delta), Ordering::Relaxed);

            b2.remove(page_id);
            drop(b1);
            drop(b2);

            // Insert into T2
            let mut t2 = self.t2.lock();
            t2.push_front(page_id);
            self.cache.insert(page_id, cached_page);
            drop(t2);

            self.replace(true)?;
        } else {
            drop(b1);
            drop(b2);

            // Not in ghost caches - new page
            let mut t1 = self.t1.lock();
            let t1_len = t1.len();
            let t2_len = self.t2.lock().len();

            if t1_len + t2_len >= self.capacity {
                drop(t1);
                self.replace(false)?;
                t1 = self.t1.lock();
            }

            t1.push_front(page_id);
            self.cache.insert(page_id, cached_page);
        }

        Ok(())
    }

    fn replace(&self, in_b2: bool) -> Result<()> {
        let p_val = self.p.load(Ordering::Relaxed);
        let mut t1 = self.t1.lock();
        let t1_len = t1.len();

        if t1_len > 0 && (t1_len > p_val || (in_b2 && t1_len == p_val)) {
            // Replace from T1
            if let Some(victim) = t1.pop_back() {
                self.cache.remove(&victim);

                // Move to B1
                drop(t1);
                let mut b1 = self.b1.lock();
                b1.push_front(victim);

                // Trim B1 if needed
                let b1_len = b1.len();
                let t2_len = self.t2.lock().len();
                if b1_len + t2_len > self.capacity {
                    b1.pop_back();
                }

                self.stats.record_eviction();
            }
        } else {
            // Replace from T2
            drop(t1);
            let mut t2 = self.t2.lock();

            if let Some(victim) = t2.pop_back() {
                self.cache.remove(&victim);

                // Move to B2
                drop(t2);
                let mut b2 = self.b2.lock();
                b2.push_front(victim);

                // Trim B2 if needed
                let b2_len = b2.len();
                let t1_len = self.t1.lock().len();
                if b2_len + t1_len > self.capacity {
                    b2.pop_back();
                }

                self.stats.record_eviction();
            }
        }

        Ok(())
    }

    pub fn remove(&self, page_id: u32) -> Option<Arc<CachedPage>> {
        if let Some((_, cached_page)) = self.cache.remove(&page_id) {
            // Remove from whichever list contains it
            let mut t1 = self.t1.lock();
            if t1.remove(page_id) {
                return Some(cached_page);
            }
            drop(t1);

            let mut t2 = self.t2.lock();
            t2.remove(page_id);

            Some(cached_page)
        } else {
            None
        }
    }

    pub fn clear(&self) {
        self.cache.clear();
        self.t1.lock().entries.clear();
        self.t2.lock().entries.clear();
        self.b1.lock().entries.clear();
        self.b2.lock().entries.clear();
        self.p.store(self.capacity / 2, Ordering::Relaxed);
        self.stats.reset();
    }

    pub fn size(&self) -> usize {
        self.cache.len()
    }

    pub fn stats(&self) -> Arc<CacheStats> {
        self.stats.clone()
    }

    pub fn get_p(&self) -> usize {
        self.p.load(Ordering::Relaxed)
    }

    pub fn get_dirty_pages(&self) -> Vec<(u32, Arc<CachedPage>)> {
        self.cache
            .iter()
            .filter(|entry| entry.value().is_dirty())
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    pub fn mark_clean(&self, page_id: u32) {
        if let Some(entry) = self.cache.get(&page_id) {
            entry.value().mark_clean();
        }
    }
}
