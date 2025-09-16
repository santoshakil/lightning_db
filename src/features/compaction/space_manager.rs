use super::CompactionConfig;
use crate::core::error::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct SpaceRegion {
    pub region_id: u64,
    pub start_offset: u64,
    pub size_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub fragmentation_ratio: f64,
    pub region_type: RegionType,
    pub last_compacted: Option<std::time::Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionType {
    BTreePages,
    LSMLevel(usize),
    WAL,
    Index,
    Metadata,
}

#[derive(Debug, Clone)]
pub struct SpaceStats {
    pub total_space: u64,
    pub used_space: u64,
    pub free_space: u64,
    pub fragmented_space: u64,
    pub reclaimable_space: u64,
    pub fragmentation_ratio: f64,
    pub regions_count: usize,
    pub most_fragmented_regions: Vec<u64>,
}

#[derive(Debug)]
pub struct SpaceManager {
    _config: Arc<RwLock<CompactionConfig>>,
    regions: Arc<RwLock<HashMap<u64, SpaceRegion>>>,
    free_list: Arc<RwLock<Vec<(u64, u64)>>>, // (offset, size) pairs
    next_region_id: Arc<std::sync::atomic::AtomicU64>,
}

impl SpaceManager {
    pub fn new(config: Arc<RwLock<CompactionConfig>>) -> Result<Self> {
        Ok(Self {
            _config: config,
            regions: Arc::new(RwLock::new(HashMap::new())),
            free_list: Arc::new(RwLock::new(Vec::new())),
            next_region_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        })
    }

    pub async fn allocate_space(&self, size_bytes: u64, region_type: RegionType) -> Result<u64> {
        // Try to find suitable free space
        if let Some(offset) = self.find_free_space(size_bytes).await? {
            let region_id = self
                .next_region_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            let region = SpaceRegion {
                region_id,
                start_offset: offset,
                size_bytes,
                used_bytes: 0,
                free_bytes: size_bytes,
                fragmentation_ratio: 0.0,
                region_type,
                last_compacted: None,
            };

            {
                let mut regions = self.regions.write().await;
                regions.insert(region_id, region);
            }

            // Remove from free list
            self.remove_from_free_list(offset, size_bytes).await?;

            Ok(region_id)
        } else {
            Err(Error::ResourceExhausted {
                resource: format!("Cannot allocate {} bytes", size_bytes),
            })
        }
    }

    pub async fn deallocate_space(&self, region_id: u64) -> Result<u64> {
        let region = {
            let mut regions = self.regions.write().await;
            regions.remove(&region_id)
        };

        if let Some(region) = region {
            // Add to free list
            {
                let mut free_list = self.free_list.write().await;
                free_list.push((region.start_offset, region.size_bytes));
                // Sort and merge adjacent free blocks
                self.merge_free_blocks(&mut free_list).await;
            }

            Ok(region.size_bytes)
        } else {
            Err(Error::Generic(format!("Region {} not found", region_id)))
        }
    }

    pub async fn update_region_usage(&self, region_id: u64, used_bytes: u64) -> Result<()> {
        let mut regions = self.regions.write().await;
        if let Some(region) = regions.get_mut(&region_id) {
            region.used_bytes = used_bytes;
            region.free_bytes = region.size_bytes.saturating_sub(used_bytes);
            region.fragmentation_ratio = if region.size_bytes > 0 {
                region.free_bytes as f64 / region.size_bytes as f64
            } else {
                0.0
            };
        } else {
            return Err(Error::Generic(format!("Region {} not found", region_id)));
        }

        Ok(())
    }

    pub async fn get_space_stats(&self) -> Result<SpaceStats> {
        let regions = self.regions.read().await;
        let free_list = self.free_list.read().await;

        let mut total_space = 0u64;
        let mut used_space = 0u64;
        let mut fragmented_space = 0u64;
        let mut most_fragmented = Vec::new();

        for region in regions.values() {
            total_space += region.size_bytes;
            used_space += region.used_bytes;
            fragmented_space += region.free_bytes;

            if region.fragmentation_ratio > 0.3 {
                // 30% fragmentation threshold
                most_fragmented.push(region.region_id);
            }
        }

        // Add free list space
        let free_space: u64 = free_list.iter().map(|(_, size)| *size).sum();
        total_space += free_space;

        // Sort most fragmented by fragmentation ratio
        most_fragmented.sort_by(|&a, &b| {
            let frag_a = regions
                .get(&a)
                .map(|r| r.fragmentation_ratio)
                .unwrap_or(0.0);
            let frag_b = regions
                .get(&b)
                .map(|r| r.fragmentation_ratio)
                .unwrap_or(0.0);
            frag_b
                .partial_cmp(&frag_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        most_fragmented.truncate(10); // Top 10 most fragmented

        let fragmentation_ratio = if total_space > 0 {
            fragmented_space as f64 / total_space as f64
        } else {
            0.0
        };

        let reclaimable_space = self.calculate_reclaimable_space(&regions).await;

        Ok(SpaceStats {
            total_space,
            used_space,
            free_space,
            fragmented_space,
            reclaimable_space,
            fragmentation_ratio,
            regions_count: regions.len(),
            most_fragmented_regions: most_fragmented,
        })
    }

    pub async fn get_fragmentation_ratio(&self) -> Result<f64> {
        let stats = self.get_space_stats().await?;
        Ok(stats.fragmentation_ratio)
    }

    pub async fn estimate_reclaimable_space(&self) -> Result<u64> {
        let regions = self.regions.read().await;
        Ok(self.calculate_reclaimable_space(&regions).await)
    }

    pub async fn compact_region(&self, region_id: u64) -> Result<u64> {
        let region = {
            let regions = self.regions.read().await;
            regions.get(&region_id).cloned()
        };

        if let Some(mut region) = region {
            // Simulate compaction - reclaim fragmented space
            let bytes_reclaimed = (region.free_bytes as f64 * 0.8) as u64; // Reclaim 80% of free space

            region.free_bytes = (region.free_bytes as f64 * 0.2) as u64; // Keep 20% as buffer
            region.fragmentation_ratio = region.free_bytes as f64 / region.size_bytes as f64;
            region.last_compacted = Some(std::time::Instant::now());

            {
                let mut regions = self.regions.write().await;
                regions.insert(region_id, region);
            }

            Ok(bytes_reclaimed)
        } else {
            Err(Error::Generic(format!("Region {} not found", region_id)))
        }
    }

    pub async fn compact_fragmented_regions(&self, fragmentation_threshold: f64) -> Result<u64> {
        let fragmented_regions = {
            let regions = self.regions.read().await;
            regions
                .values()
                .filter(|r| r.fragmentation_ratio > fragmentation_threshold)
                .map(|r| r.region_id)
                .collect::<Vec<_>>()
        };

        let mut total_reclaimed = 0u64;

        for region_id in fragmented_regions {
            let reclaimed = self.compact_region(region_id).await?;
            total_reclaimed += reclaimed;
        }

        Ok(total_reclaimed)
    }

    pub async fn merge_adjacent_regions(&self, region1_id: u64, region2_id: u64) -> Result<u64> {
        let (region1, region2) = {
            let mut regions = self.regions.write().await;
            let r1 = regions.remove(&region1_id);
            let r2 = regions.remove(&region2_id);
            (r1, r2)
        };

        if let (Some(region1), Some(region2)) = (region1, region2) {
            // Check if regions are adjacent
            let adjacent = (region1.start_offset + region1.size_bytes == region2.start_offset)
                || (region2.start_offset + region2.size_bytes == region1.start_offset);

            if adjacent && region1.region_type == region2.region_type {
                let merged_region = SpaceRegion {
                    region_id: region1.region_id, // Keep first region's ID
                    start_offset: region1.start_offset.min(region2.start_offset),
                    size_bytes: region1.size_bytes + region2.size_bytes,
                    used_bytes: region1.used_bytes + region2.used_bytes,
                    free_bytes: region1.free_bytes + region2.free_bytes,
                    fragmentation_ratio: (region1.free_bytes + region2.free_bytes) as f64
                        / (region1.size_bytes + region2.size_bytes) as f64,
                    region_type: region1.region_type,
                    last_compacted: Some(std::time::Instant::now()),
                };

                {
                    let mut regions = self.regions.write().await;
                    regions.insert(region1.region_id, merged_region);
                }

                // Return space saved (one region header eliminated)
                Ok(region2.size_bytes)
            } else {
                // Put regions back if they can't be merged
                let mut regions = self.regions.write().await;
                regions.insert(region1_id, region1);
                regions.insert(region2_id, region2);

                Err(Error::Generic(
                    "Regions are not adjacent or compatible".into(),
                ))
            }
        } else {
            Err(Error::Generic("One or both regions not found".into()))
        }
    }

    pub async fn get_regions_by_type(&self, region_type: RegionType) -> Result<Vec<SpaceRegion>> {
        let regions = self.regions.read().await;
        let matching_regions = regions
            .values()
            .filter(|r| r.region_type == region_type)
            .cloned()
            .collect();

        Ok(matching_regions)
    }

    pub async fn get_most_fragmented_regions(&self, limit: usize) -> Result<Vec<SpaceRegion>> {
        let regions = self.regions.read().await;
        let mut fragmented_regions: Vec<_> = regions.values().cloned().collect();

        fragmented_regions.sort_by(|a, b| {
            b.fragmentation_ratio
                .partial_cmp(&a.fragmentation_ratio)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        fragmented_regions.truncate(limit);
        Ok(fragmented_regions)
    }

    pub async fn get_region_info(&self, region_id: u64) -> Result<SpaceRegion> {
        let regions = self.regions.read().await;
        regions
            .get(&region_id)
            .cloned()
            .ok_or_else(|| Error::Generic(format!("Region {} not found", region_id)))
    }

    async fn find_free_space(&self, size_bytes: u64) -> Result<Option<u64>> {
        let free_list = self.free_list.read().await;

        for &(offset, size) in free_list.iter() {
            if size >= size_bytes {
                return Ok(Some(offset));
            }
        }

        Ok(None)
    }

    async fn remove_from_free_list(&self, offset: u64, size_bytes: u64) -> Result<()> {
        let mut free_list = self.free_list.write().await;

        if let Some(index) = free_list
            .iter()
            .position(|&(off, size)| off <= offset && offset + size_bytes <= off + size)
        {
            let (free_offset, free_size) = free_list.remove(index);

            // Add back any remaining free space
            if free_offset < offset {
                // Space before allocated region
                free_list.push((free_offset, offset - free_offset));
            }

            let end_of_allocated = offset + size_bytes;
            let end_of_free = free_offset + free_size;

            if end_of_allocated < end_of_free {
                // Space after allocated region
                free_list.push((end_of_allocated, end_of_free - end_of_allocated));
            }

            // Sort free list
            free_list.sort_by_key(|&(off, _)| off);
        }

        Ok(())
    }

    async fn merge_free_blocks(&self, free_list: &mut Vec<(u64, u64)>) {
        if free_list.is_empty() {
            return;
        }

        free_list.sort_by_key(|&(offset, _)| offset);

        let mut merged = Vec::new();
        let mut current = free_list[0];

        for &(offset, size) in free_list.iter().skip(1) {
            if current.0 + current.1 == offset {
                // Adjacent blocks - merge them
                current.1 += size;
            } else {
                // Non-adjacent - add current and start new one
                merged.push(current);
                current = (offset, size);
            }
        }

        merged.push(current);
        *free_list = merged;
    }

    async fn calculate_reclaimable_space(&self, regions: &HashMap<u64, SpaceRegion>) -> u64 {
        let mut reclaimable = 0u64;

        for region in regions.values() {
            // Space that can be reclaimed through compaction
            let compactable_space = (region.free_bytes as f64 * 0.8) as u64; // 80% of fragmented space
            reclaimable += compactable_space;
        }

        reclaimable
    }

    pub async fn get_free_space_distribution(&self) -> Result<HashMap<String, usize>> {
        let free_list = self.free_list.read().await;
        let mut distribution = HashMap::new();

        distribution.insert("small (< 64KB)".to_string(), 0);
        distribution.insert("medium (64KB - 1MB)".to_string(), 0);
        distribution.insert("large (1MB - 16MB)".to_string(), 0);
        distribution.insert("huge (> 16MB)".to_string(), 0);

        for &(_, size) in free_list.iter() {
            let category = if size < 64 * 1024 {
                "small (< 64KB)"
            } else if size < 1024 * 1024 {
                "medium (64KB - 1MB)"
            } else if size < 16 * 1024 * 1024 {
                "large (1MB - 16MB)"
            } else {
                "huge (> 16MB)"
            };

            *distribution.get_mut(category).unwrap() += 1;
        }

        Ok(distribution)
    }

    pub async fn defragment_free_space(&self) -> Result<u64> {
        let mut free_list = self.free_list.write().await;
        let original_blocks = free_list.len();

        self.merge_free_blocks(&mut free_list).await;

        let blocks_merged = original_blocks.saturating_sub(free_list.len());

        // Return estimated bytes saved from reduced metadata overhead
        Ok(blocks_merged as u64 * 64) // Assume 64 bytes per block metadata
    }
}
