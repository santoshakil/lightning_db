use crate::core::error::{Error, Result};
use super::CompactionConfig;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct BTreePageInfo {
    pub page_id: u64,
    pub size_bytes: usize,
    pub used_bytes: usize,
    pub free_bytes: usize,
    pub fragmentation_ratio: f64,
    pub last_modified: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct BTreeCompactionStats {
    pub pages_compacted: u64,
    pub bytes_reclaimed: u64,
    pub pages_merged: u64,
    pub avg_fragmentation_before: f64,
    pub avg_fragmentation_after: f64,
    pub total_compaction_time: std::time::Duration,
}

#[derive(Debug)]
pub struct BTreeCompactor {
    config: Arc<RwLock<CompactionConfig>>,
    page_info: Arc<RwLock<HashMap<u64, BTreePageInfo>>>,
    compaction_stats: Arc<RwLock<BTreeCompactionStats>>,
}

impl BTreeCompactor {
    pub fn new(config: Arc<RwLock<CompactionConfig>>) -> Result<Self> {
        Ok(Self {
            config,
            page_info: Arc::new(RwLock::new(HashMap::new())),
            compaction_stats: Arc::new(RwLock::new(BTreeCompactionStats {
                pages_compacted: 0,
                bytes_reclaimed: 0,
                pages_merged: 0,
                avg_fragmentation_before: 0.0,
                avg_fragmentation_after: 0.0,
                total_compaction_time: std::time::Duration::from_secs(0),
            })),
        })
    }
    
    pub async fn compact_pages(&self, page_ids: Vec<u64>) -> Result<u64> {
        let start_time = std::time::Instant::now();
        let mut total_bytes_reclaimed = 0u64;
        let mut pages_processed = 0u64;
        
        // Process pages in batches to avoid holding locks too long
        const BATCH_SIZE: usize = 100;
        
        for batch in page_ids.chunks(BATCH_SIZE) {
            let batch_reclaimed = self.compact_page_batch(batch).await?;
            total_bytes_reclaimed += batch_reclaimed;
            pages_processed += batch.len() as u64;
        }
        
        // Update statistics
        let duration = start_time.elapsed();
        let mut stats = self.compaction_stats.write().await;
        stats.pages_compacted += pages_processed;
        stats.bytes_reclaimed += total_bytes_reclaimed;
        stats.total_compaction_time += duration;
        
        Ok(total_bytes_reclaimed)
    }
    
    pub async fn merge_adjacent_pages(&self) -> Result<u64> {
        let adjacent_pages = self.find_mergeable_pages().await?;
        let mut total_bytes_saved = 0u64;
        let mut pages_merged = 0u64;
        
        for (page1_id, page2_id) in adjacent_pages {
            let bytes_saved = self.merge_two_pages(page1_id, page2_id).await?;
            total_bytes_saved += bytes_saved;
            pages_merged += 1;
        }
        
        // Update statistics
        let mut stats = self.compaction_stats.write().await;
        stats.pages_merged += pages_merged;
        stats.bytes_reclaimed += total_bytes_saved;
        
        Ok(total_bytes_saved)
    }
    
    pub async fn defragment_pages(&self, fragmentation_threshold: f64) -> Result<u64> {
        let fragmented_pages = self.find_fragmented_pages(fragmentation_threshold).await?;
        let mut total_bytes_reclaimed = 0u64;
        
        for page_id in fragmented_pages {
            let bytes_reclaimed = self.defragment_page(page_id).await?;
            total_bytes_reclaimed += bytes_reclaimed;
        }
        
        Ok(total_bytes_reclaimed)
    }
    
    pub async fn get_page_fragmentation(&self) -> Result<f64> {
        let page_info = self.page_info.read().await;
        
        if page_info.is_empty() {
            return Ok(0.0);
        }
        
        let total_fragmentation: f64 = page_info.values()
            .map(|page| page.fragmentation_ratio)
            .sum();
        
        Ok(total_fragmentation / page_info.len() as f64)
    }
    
    pub async fn estimate_page_savings(&self) -> Result<u64> {
        let page_info = self.page_info.read().await;
        
        let estimated_savings: u64 = page_info.values()
            .map(|page| page.free_bytes as u64)
            .sum();
        
        Ok(estimated_savings)
    }
    
    pub async fn update_page_info(&self, page_id: u64, size_bytes: usize, used_bytes: usize) -> Result<()> {
        let free_bytes = size_bytes.saturating_sub(used_bytes);
        let fragmentation_ratio = if size_bytes > 0 {
            free_bytes as f64 / size_bytes as f64
        } else {
            0.0
        };
        
        let page_info = BTreePageInfo {
            page_id,
            size_bytes,
            used_bytes,
            free_bytes,
            fragmentation_ratio,
            last_modified: std::time::Instant::now(),
        };
        
        let mut pages = self.page_info.write().await;
        pages.insert(page_id, page_info);
        
        Ok(())
    }
    
    pub async fn get_compaction_candidates(&self, max_candidates: usize) -> Result<Vec<u64>> {
        let page_info = self.page_info.read().await;
        
        let mut candidates: Vec<_> = page_info.values()
            .filter(|page| page.fragmentation_ratio > 0.25) // 25% fragmentation threshold
            .collect();
        
        // Sort by fragmentation ratio (highest first)
        candidates.sort_by(|a, b| b.fragmentation_ratio.partial_cmp(&a.fragmentation_ratio).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(candidates.into_iter()
            .take(max_candidates)
            .map(|page| page.page_id)
            .collect())
    }
    
    pub async fn get_stats(&self) -> BTreeCompactionStats {
        self.compaction_stats.read().await.clone()
    }
    
    async fn compact_page_batch(&self, page_ids: &[u64]) -> Result<u64> {
        let mut total_reclaimed = 0u64;
        
        for &page_id in page_ids {
            // Integration point: B+tree page manager compaction required
            // Currently simulated - needs PageManager integration
            let reclaimed = self.compact_single_page(page_id).await?;
            total_reclaimed += reclaimed;
            
            // Small delay to yield control
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
        
        Ok(total_reclaimed)
    }
    
    async fn compact_single_page(&self, page_id: u64) -> Result<u64> {
        let page_info = {
            let pages = self.page_info.read().await;
            pages.get(&page_id).cloned()
        };
        
        if let Some(info) = page_info {
            // Simulate compaction process
            tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
            
            // Calculate bytes reclaimed (assume we can reclaim 80% of free space)
            let bytes_reclaimed = (info.free_bytes as f64 * 0.8) as u64;
            
            // Update page info after compaction
            let new_used_bytes = info.used_bytes + (bytes_reclaimed as usize / 2);
            let new_free_bytes = info.size_bytes.saturating_sub(new_used_bytes);
            
            let updated_info = BTreePageInfo {
                page_id,
                size_bytes: info.size_bytes,
                used_bytes: new_used_bytes,
                free_bytes: new_free_bytes,
                fragmentation_ratio: if info.size_bytes > 0 {
                    new_free_bytes as f64 / info.size_bytes as f64
                } else {
                    0.0
                },
                last_modified: std::time::Instant::now(),
            };
            
            {
                let mut pages = self.page_info.write().await;
                pages.insert(page_id, updated_info);
            }
            
            Ok(bytes_reclaimed)
        } else {
            Ok(0)
        }
    }
    
    async fn find_mergeable_pages(&self) -> Result<Vec<(u64, u64)>> {
        let page_info = self.page_info.read().await;
        let mut mergeable_pairs = Vec::new();
        
        // Simple heuristic: find pages with low utilization that could be merged
        let underutilized_pages: Vec<_> = page_info.values()
            .filter(|page| (page.used_bytes as f64 / page.size_bytes as f64) < 0.5)
            .collect();
        
        // Pair up adjacent or related pages
        for i in 0..underutilized_pages.len() {
            for j in (i + 1)..underutilized_pages.len() {
                let page1 = underutilized_pages[i];
                let page2 = underutilized_pages[j];
                
                // Check if pages can fit together
                if page1.used_bytes + page2.used_bytes < page1.size_bytes {
                    mergeable_pairs.push((page1.page_id, page2.page_id));
                    if mergeable_pairs.len() >= 100 { // Limit pairs to avoid excessive merging
                        break;
                    }
                }
            }
            
            if mergeable_pairs.len() >= 100 {
                break;
            }
        }
        
        Ok(mergeable_pairs)
    }
    
    async fn merge_two_pages(&self, page1_id: u64, page2_id: u64) -> Result<u64> {
        let (page1_info, page2_info) = {
            let pages = self.page_info.read().await;
            (
                pages.get(&page1_id).cloned(),
                pages.get(&page2_id).cloned(),
            )
        };
        
        if let (Some(page1), Some(page2)) = (page1_info, page2_info) {
            // Simulate merge operation
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            
            let combined_used = page1.used_bytes + page2.used_bytes;
            
            if combined_used <= page1.size_bytes {
                // Merge successful - page2 is eliminated
                let bytes_saved = page2.size_bytes as u64;
                
                // Update page1 with merged data
                let updated_page1 = BTreePageInfo {
                    page_id: page1_id,
                    size_bytes: page1.size_bytes,
                    used_bytes: combined_used,
                    free_bytes: page1.size_bytes.saturating_sub(combined_used),
                    fragmentation_ratio: if page1.size_bytes > 0 {
                        (page1.size_bytes.saturating_sub(combined_used)) as f64 / page1.size_bytes as f64
                    } else {
                        0.0
                    },
                    last_modified: std::time::Instant::now(),
                };
                
                {
                    let mut pages = self.page_info.write().await;
                    pages.insert(page1_id, updated_page1);
                    pages.remove(&page2_id); // Remove merged page
                }
                
                Ok(bytes_saved)
            } else {
                Ok(0) // Cannot merge
            }
        } else {
            Ok(0)
        }
    }
    
    async fn find_fragmented_pages(&self, threshold: f64) -> Result<Vec<u64>> {
        let page_info = self.page_info.read().await;
        
        let fragmented_pages: Vec<u64> = page_info.values()
            .filter(|page| page.fragmentation_ratio > threshold)
            .map(|page| page.page_id)
            .collect();
        
        Ok(fragmented_pages)
    }
    
    async fn defragment_page(&self, page_id: u64) -> Result<u64> {
        let page_info = {
            let pages = self.page_info.read().await;
            pages.get(&page_id).cloned()
        };
        
        if let Some(info) = page_info {
            // Simulate defragmentation
            tokio::time::sleep(tokio::time::Duration::from_millis(3)).await;
            
            // Defragmentation reduces internal fragmentation
            let bytes_reclaimed = (info.free_bytes as f64 * 0.7) as u64; // Reclaim 70% of fragmented space
            
            let updated_info = BTreePageInfo {
                page_id,
                size_bytes: info.size_bytes,
                used_bytes: info.used_bytes,
                free_bytes: (info.free_bytes as f64 * 0.3) as usize, // Reduce fragmentation
                fragmentation_ratio: info.fragmentation_ratio * 0.3, // Significantly reduce fragmentation
                last_modified: std::time::Instant::now(),
            };
            
            {
                let mut pages = self.page_info.write().await;
                pages.insert(page_id, updated_info);
            }
            
            Ok(bytes_reclaimed)
        } else {
            Ok(0)
        }
    }
    
    pub async fn scan_and_update_pages(&self) -> Result<()> {
        // Integration point for actual page manager to scan all pages
        // For now, simulate discovering pages
        
        let sample_pages = vec![
            (1, 4096, 2048),    // 50% utilization
            (2, 4096, 3500),    // ~85% utilization
            (3, 4096, 1000),    // ~25% utilization - good candidate
            (4, 4096, 3800),    // ~95% utilization
            (5, 4096, 1500),    // ~37% utilization
            (6, 4096, 500),     // ~12% utilization - very fragmented
            (7, 4096, 3200),    // ~78% utilization
            (8, 4096, 800),     // ~20% utilization
        ];
        
        for (page_id, size, used) in sample_pages {
            self.update_page_info(page_id, size, used).await?;
        }
        
        Ok(())
    }
    
    pub async fn get_page_info(&self, page_id: u64) -> Result<BTreePageInfo> {
        let pages = self.page_info.read().await;
        pages.get(&page_id)
            .cloned()
            .ok_or_else(|| Error::Generic(format!("Page {} not found", page_id)))
    }
    
    pub async fn get_all_pages(&self) -> Vec<BTreePageInfo> {
        let pages = self.page_info.read().await;
        pages.values().cloned().collect()
    }
    
    pub async fn remove_page(&self, page_id: u64) -> Result<()> {
        let mut pages = self.page_info.write().await;
        pages.remove(&page_id);
        Ok(())
    }
    
    pub async fn get_fragmentation_distribution(&self) -> Result<HashMap<String, usize>> {
        let pages = self.page_info.read().await;
        let mut distribution = HashMap::new();
        
        distribution.insert("low (0-25%)".to_string(), 0);
        distribution.insert("medium (25-50%)".to_string(), 0);
        distribution.insert("high (50-75%)".to_string(), 0);
        distribution.insert("very_high (75-100%)".to_string(), 0);
        
        for page in pages.values() {
            let category = if page.fragmentation_ratio < 0.25 {
                "low (0-25%)"
            } else if page.fragmentation_ratio < 0.50 {
                "medium (25-50%)"
            } else if page.fragmentation_ratio < 0.75 {
                "high (50-75%)"
            } else {
                "very_high (75-100%)"
            };
            
            *distribution.get_mut(category).unwrap() += 1;
        }
        
        Ok(distribution)
    }
}