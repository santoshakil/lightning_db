use crate::core::error::Result;
use super::{Page, PageManagerTrait, OptimizedPageManager, MmapConfig};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use dashmap::DashMap;

const NUM_SHARDS: usize = 64;

pub struct ShardedPageManager {
    managers: Vec<Arc<OptimizedPageManager>>,
    page_cache: Arc<DashMap<u32, Arc<Page>>>,
    stats: Arc<ShardStats>,
    shard_mask: u32,
}

#[derive(Debug, Default)]
struct ShardStats {
    total_reads: AtomicU64,
    total_writes: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl ShardedPageManager {
    pub fn create<P: AsRef<Path>>(base_path: P, initial_size: u64, config: &MmapConfig) -> Result<Self> {
        let base_path = base_path.as_ref();
        let mut managers = Vec::with_capacity(NUM_SHARDS);
        
        for shard_id in 0..NUM_SHARDS {
            let shard_path = base_path.with_extension(format!("shard{}", shard_id));
            let shard_size = initial_size / NUM_SHARDS as u64;
            let manager = Arc::new(OptimizedPageManager::create(shard_path, shard_size, config.clone())?);
            managers.push(manager);
        }
        
        Ok(Self {
            managers,
            page_cache: Arc::new(DashMap::with_capacity(10000)),
            stats: Arc::new(ShardStats::default()),
            shard_mask: (NUM_SHARDS - 1) as u32,
        })
    }
    
    pub fn open<P: AsRef<Path>>(base_path: P, config: &MmapConfig) -> Result<Self> {
        let base_path = base_path.as_ref();
        let mut managers = Vec::with_capacity(NUM_SHARDS);
        
        for shard_id in 0..NUM_SHARDS {
            let shard_path = base_path.with_extension(format!("shard{}", shard_id));
            let manager = Arc::new(OptimizedPageManager::open(shard_path, config.clone())?);
            managers.push(manager);
        }
        
        Ok(Self {
            managers,
            page_cache: Arc::new(DashMap::with_capacity(10000)),
            stats: Arc::new(ShardStats::default()),
            shard_mask: (NUM_SHARDS - 1) as u32,
        })
    }
    
    #[inline(always)]
    fn get_shard(&self, page_id: u32) -> &Arc<OptimizedPageManager> {
        let shard_idx = (page_id & self.shard_mask) as usize;
        &self.managers[shard_idx]
    }
    
    fn cache_page(&self, page: Page) -> Arc<Page> {
        let page_arc = Arc::new(page);
        self.page_cache.insert(page_arc.id, Arc::clone(&page_arc));
        page_arc
    }
    
    pub fn get_statistics(&self) -> String {
        let stats = &self.stats;
        let total_reads = stats.total_reads.load(Ordering::Relaxed);
        let total_writes = stats.total_writes.load(Ordering::Relaxed);
        let cache_hits = stats.cache_hits.load(Ordering::Relaxed);
        let cache_misses = stats.cache_misses.load(Ordering::Relaxed);
        let hit_rate = if total_reads > 0 {
            (cache_hits as f64 / total_reads as f64) * 100.0
        } else {
            0.0
        };
        
        format!(
            "ShardedPageManager Stats:\n\
             Total Reads: {}\n\
             Total Writes: {}\n\
             Cache Hits: {} ({:.2}%)\n\
             Cache Misses: {}\n\
             Cache Size: {} pages",
            total_reads, total_writes, cache_hits, hit_rate, 
            cache_misses, self.page_cache.len()
        )
    }
    
    pub fn clear_cache(&self) {
        self.page_cache.clear();
    }
}

impl PageManagerTrait for ShardedPageManager {
    fn allocate_page(&self) -> Result<u32> {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let shard_idx = COUNTER.fetch_add(1, Ordering::Relaxed) as usize % NUM_SHARDS;
        let manager = &self.managers[shard_idx];
        
        let base_id = manager.allocate_page()?;
        let page_id = (base_id << 6) | (shard_idx as u32);
        Ok(page_id)
    }
    
    fn free_page(&self, page_id: u32) {
        self.page_cache.remove(&page_id);
        let shard = self.get_shard(page_id);
        let base_id = page_id >> 6;
        shard.free_page(base_id);
    }
    
    fn get_page(&self, page_id: u32) -> Result<Page> {
        self.stats.total_reads.fetch_add(1, Ordering::Relaxed);
        
        if let Some(cached) = self.page_cache.get(&page_id) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok((**cached).clone());
        }
        
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        let shard = self.get_shard(page_id);
        let base_id = page_id >> 6;
        let page = shard.get_page(base_id)?;
        
        let cached = self.cache_page(page);
        Ok((*cached).clone())
    }
    
    fn write_page(&self, page: &Page) -> Result<()> {
        self.stats.total_writes.fetch_add(1, Ordering::Relaxed);
        
        self.page_cache.insert(page.id, Arc::new(page.clone()));
        
        let shard = self.get_shard(page.id);
        let base_id = page.id >> 6;
        let mut modified_page = page.clone();
        modified_page.id = base_id;
        shard.write_page(&modified_page)
    }
    
    fn sync(&self) -> Result<()> {
        let mut results = Vec::with_capacity(NUM_SHARDS);
        
        for manager in &self.managers {
            results.push(manager.sync());
        }
        
        for result in results {
            result?;
        }
        
        Ok(())
    }
    
    fn page_count(&self) -> u32 {
        self.managers.iter()
            .map(|m| m.page_count())
            .sum()
    }
    
    fn free_page_count(&self) -> usize {
        self.managers.iter()
            .map(|m| m.free_page_count())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_sharded_page_manager() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let config = MmapConfig::default();
        let manager = ShardedPageManager::create(&db_path, 1024 * 1024, &config).unwrap();
        
        let page_id = manager.allocate_page().unwrap();
        assert!(page_id > 0);
        
        let mut page = Page::new(page_id);
        page.get_mut_data()[0] = 42;
        
        manager.write_page(&page).unwrap();
        
        let loaded_page = manager.get_page(page_id).unwrap();
        assert_eq!(loaded_page.get_data()[0], 42);
        
        manager.sync().unwrap();
    }
}