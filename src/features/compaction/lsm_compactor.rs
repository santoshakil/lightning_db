use crate::core::error::{Error, Result};
use super::CompactionConfig;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct LSMLevel {
    pub level: usize,
    pub size_bytes: u64,
    pub file_count: usize,
    pub max_size_bytes: u64,
    pub compaction_score: f64,
}

#[derive(Debug, Clone)]
pub struct LSMCompactionTask {
    pub source_level: usize,
    pub target_level: usize,
    pub estimated_bytes: u64,
    pub estimated_duration: std::time::Duration,
    pub priority: CompactionPriority,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompactionPriority {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug)]
pub struct LSMCompactor {
    config: Arc<RwLock<CompactionConfig>>,
    levels: Arc<RwLock<Vec<LSMLevel>>>,
    active_compactions: Arc<RwLock<HashMap<String, LSMCompactionTask>>>,
    compaction_stats: Arc<RwLock<LSMCompactionStats>>,
}

#[derive(Debug, Clone)]
struct LSMCompactionStats {
    total_compactions: u64,
    bytes_written: u64,
    bytes_read: u64,
    level_compactions: HashMap<usize, u64>,
    avg_compaction_time: std::time::Duration,
}

impl LSMCompactor {
    pub fn new(config: Arc<RwLock<CompactionConfig>>) -> Result<Self> {
        // Initialize with typical LSM level structure
        let levels = vec![
            LSMLevel {
                level: 0,
                size_bytes: 0,
                file_count: 0,
                max_size_bytes: 4 * 1024 * 1024,     // 4MB for L0
                compaction_score: 0.0,
            },
            LSMLevel {
                level: 1,
                size_bytes: 0,
                file_count: 0,
                max_size_bytes: 40 * 1024 * 1024,    // 40MB for L1
                compaction_score: 0.0,
            },
            LSMLevel {
                level: 2,
                size_bytes: 0,
                file_count: 0,
                max_size_bytes: 400 * 1024 * 1024,   // 400MB for L2
                compaction_score: 0.0,
            },
            LSMLevel {
                level: 3,
                size_bytes: 0,
                file_count: 0,
                max_size_bytes: 4 * 1024 * 1024 * 1024, // 4GB for L3
                compaction_score: 0.0,
            },
            LSMLevel {
                level: 4,
                size_bytes: 0,
                file_count: 0,
                max_size_bytes: 40 * 1024 * 1024 * 1024, // 40GB for L4
                compaction_score: 0.0,
            },
            LSMLevel {
                level: 5,
                size_bytes: 0,
                file_count: 0,
                max_size_bytes: 400 * 1024 * 1024 * 1024, // 400GB for L5
                compaction_score: 0.0,
            },
            LSMLevel {
                level: 6,
                size_bytes: 0,
                file_count: 0,
                max_size_bytes: u64::MAX, // Unlimited for bottom level
                compaction_score: 0.0,
            },
        ];
        
        Ok(Self {
            config,
            levels: Arc::new(RwLock::new(levels)),
            active_compactions: Arc::new(RwLock::new(HashMap::new())),
            compaction_stats: Arc::new(RwLock::new(LSMCompactionStats {
                total_compactions: 0,
                bytes_written: 0,
                bytes_read: 0,
                level_compactions: HashMap::new(),
                avg_compaction_time: std::time::Duration::from_secs(0),
            })),
        })
    }
    
    pub async fn compact_level(&self, source_level: usize, target_level: usize) -> Result<u64> {
        let task = self.create_compaction_task(source_level, target_level).await?;
        let task_id = format!("L{}->L{}", source_level, target_level);
        
        {
            let mut active = self.active_compactions.write().await;
            active.insert(task_id.clone(), task.clone());
        }
        
        let result = self.execute_level_compaction(task).await;
        
        {
            let mut active = self.active_compactions.write().await;
            active.remove(&task_id);
        }
        
        // Update statistics
        if let Ok(bytes_compacted) = result {
            let mut stats = self.compaction_stats.write().await;
            stats.total_compactions += 1;
            stats.bytes_written += bytes_compacted;
            *stats.level_compactions.entry(source_level).or_insert(0) += 1;
        }
        
        result
    }
    
    pub async fn suggest_compaction(&self) -> Option<LSMCompactionTask> {
        let levels = self.levels.read().await;
        let mut best_task: Option<LSMCompactionTask> = None;
        let mut highest_priority = CompactionPriority::Low;
        
        // Check L0 compactions (special case - file count based)
        if let Some(l0) = levels.get(0) {
            if l0.file_count >= 4 {  // Trigger L0->L1 compaction
                let priority = if l0.file_count >= 8 {
                    CompactionPriority::Critical
                } else if l0.file_count >= 6 {
                    CompactionPriority::High
                } else {
                    CompactionPriority::Medium
                };
                
                if priority >= highest_priority {
                    let task = LSMCompactionTask {
                        source_level: 0,
                        target_level: 1,
                        estimated_bytes: l0.size_bytes,
                        estimated_duration: self.estimate_compaction_time(0, 1, l0.size_bytes).await,
                        priority: priority.clone(),
                    };
                    best_task = Some(task);
                    highest_priority = priority;
                }
            }
        }
        
        // Check other levels (size-based compaction)
        for level in levels.iter().skip(1) {
            if level.level >= levels.len() - 1 {
                continue; // Can't compact the bottom level
            }
            
            let score = self.calculate_compaction_score(level).await;
            if score >= 1.0 {
                let priority = if score >= 4.0 {
                    CompactionPriority::Critical
                } else if score >= 2.0 {
                    CompactionPriority::High
                } else {
                    CompactionPriority::Medium
                };
                
                if priority >= highest_priority {
                    let task = LSMCompactionTask {
                        source_level: level.level,
                        target_level: level.level + 1,
                        estimated_bytes: level.size_bytes,
                        estimated_duration: self.estimate_compaction_time(level.level, level.level + 1, level.size_bytes).await,
                        priority: priority.clone(),
                    };
                    best_task = Some(task);
                    highest_priority = priority;
                }
            }
        }
        
        best_task
    }
    
    pub async fn compact_all_levels(&self) -> Result<u64> {
        let mut total_bytes_compacted = 0u64;
        let levels_count = {
            let levels = self.levels.read().await;
            levels.len()
        };
        
        // Compact from L0 to bottom level
        for source_level in 0..(levels_count - 1) {
            let target_level = source_level + 1;
            
            // Check if compaction is needed
            let should_compact = {
                let levels = self.levels.read().await;
                if let Some(level) = levels.get(source_level) {
                    if source_level == 0 {
                        level.file_count >= 2 // More lenient for full compaction
                    } else {
                        level.size_bytes as f64 / level.max_size_bytes as f64 >= 0.5
                    }
                } else {
                    false
                }
            };
            
            if should_compact {
                let bytes_compacted = self.compact_level(source_level, target_level).await?;
                total_bytes_compacted += bytes_compacted;
            }
        }
        
        Ok(total_bytes_compacted)
    }
    
    pub async fn get_level_fragmentation(&self) -> Result<HashMap<usize, f64>> {
        let levels = self.levels.read().await;
        let mut fragmentation = HashMap::new();
        
        for level in levels.iter() {
            let frag = if level.max_size_bytes == u64::MAX {
                0.0 // Bottom level has no size limit
            } else {
                1.0 - (level.size_bytes as f64 / level.max_size_bytes as f64)
            };
            fragmentation.insert(level.level, frag.max(0.0).min(1.0));
        }
        
        Ok(fragmentation)
    }
    
    pub async fn estimate_compaction_savings(&self) -> Result<u64> {
        let levels = self.levels.read().await;
        let mut estimated_savings = 0u64;
        
        for level in levels.iter() {
            // Estimate savings from removing duplicates and tombstones
            let duplicate_factor = match level.level {
                0 => 0.4,      // L0 has most duplicates
                1 => 0.3,
                2 => 0.2,
                _ => 0.1,
            };
            
            let tombstone_factor = 0.1; // Assume 10% tombstones across all levels
            let total_waste_factor = duplicate_factor + tombstone_factor;
            
            estimated_savings += (level.size_bytes as f64 * total_waste_factor) as u64;
        }
        
        Ok(estimated_savings)
    }
    
    pub async fn get_compaction_stats(&self) -> LSMCompactionStats {
        self.compaction_stats.read().await.clone()
    }
    
    pub async fn update_level_info(&self, level: usize, size_bytes: u64, file_count: usize) -> Result<()> {
        let mut levels = self.levels.write().await;
        if let Some(level_info) = levels.get_mut(level) {
            level_info.size_bytes = size_bytes;
            level_info.file_count = file_count;
            level_info.compaction_score = self.calculate_compaction_score(level_info).await;
        } else {
            return Err(Error::Generic(format!("Level {} does not exist", level)));
        }
        
        Ok(())
    }
    
    pub async fn get_level_info(&self, level: usize) -> Result<LSMLevel> {
        let levels = self.levels.read().await;
        levels.get(level)
            .cloned()
            .ok_or_else(|| Error::Generic(format!("Level {} not found", level)))
    }
    
    pub async fn get_all_levels(&self) -> Vec<LSMLevel> {
        self.levels.read().await.clone()
    }
    
    async fn create_compaction_task(&self, source_level: usize, target_level: usize) -> Result<LSMCompactionTask> {
        let levels = self.levels.read().await;
        let source = levels.get(source_level)
            .ok_or_else(|| Error::Generic(format!("Source level {} not found", source_level)))?;
        
        let priority = if source_level == 0 {
            if source.file_count >= 8 {
                CompactionPriority::Critical
            } else if source.file_count >= 6 {
                CompactionPriority::High
            } else {
                CompactionPriority::Medium
            }
        } else {
            let score = self.calculate_compaction_score(source).await;
            if score >= 4.0 {
                CompactionPriority::Critical
            } else if score >= 2.0 {
                CompactionPriority::High
            } else {
                CompactionPriority::Medium
            }
        };
        
        Ok(LSMCompactionTask {
            source_level,
            target_level,
            estimated_bytes: source.size_bytes,
            estimated_duration: self.estimate_compaction_time(source_level, target_level, source.size_bytes).await,
            priority,
        })
    }
    
    async fn execute_level_compaction(&self, task: LSMCompactionTask) -> Result<u64> {
        let start_time = std::time::Instant::now();
        
        // Phase 1: Read source level files
        let source_bytes = self.read_level_files(task.source_level).await?;
        
        // Phase 2: Merge with target level (if needed)
        let target_bytes = if task.target_level > 0 {
            self.read_overlapping_files(task.target_level, &task).await?
        } else {
            0
        };
        
        // Phase 3: Merge and deduplicate
        let merged_bytes = self.merge_and_deduplicate(source_bytes, target_bytes).await?;
        
        // Phase 4: Write to target level
        self.write_level_files(task.target_level, merged_bytes).await?;
        
        // Phase 5: Update level metadata
        self.update_level_after_compaction(task.source_level, task.target_level, merged_bytes).await?;
        
        let duration = start_time.elapsed();
        self.update_compaction_stats(duration, merged_bytes).await?;
        
        Ok(merged_bytes)
    }
    
    async fn read_level_files(&self, level: usize) -> Result<u64> {
        // TODO: Integrate with actual LSM tree storage
        // Simulate reading files from level
        let level_info = {
            let levels = self.levels.read().await;
            levels.get(level).cloned()
        };
        
        if let Some(info) = level_info {
            // Simulate I/O time based on file count
            let read_time = std::time::Duration::from_millis(info.file_count as u64 * 10);
            tokio::time::sleep(read_time).await;
            Ok(info.size_bytes)
        } else {
            Ok(0)
        }
    }
    
    async fn read_overlapping_files(&self, level: usize, _task: &LSMCompactionTask) -> Result<u64> {
        // TODO: Implement key range analysis to find overlapping files
        let level_info = {
            let levels = self.levels.read().await;
            levels.get(level).cloned()
        };
        
        if let Some(info) = level_info {
            // Estimate overlapping data (typically 10-50% of target level)
            let overlap_factor = if level == 1 { 0.3 } else { 0.2 };
            let overlapping_bytes = (info.size_bytes as f64 * overlap_factor) as u64;
            
            // Simulate reading overlapping files
            let read_time = std::time::Duration::from_millis(overlapping_bytes / (1024 * 1024) * 2); // 2ms per MB
            tokio::time::sleep(read_time).await;
            
            Ok(overlapping_bytes)
        } else {
            Ok(0)
        }
    }
    
    async fn merge_and_deduplicate(&self, source_bytes: u64, target_bytes: u64) -> Result<u64> {
        // Simulate merge process
        let total_input = source_bytes + target_bytes;
        
        // Simulate merge time (proportional to data size)
        let merge_time = std::time::Duration::from_millis(total_input / (1024 * 1024) * 5); // 5ms per MB
        tokio::time::sleep(merge_time).await;
        
        // Calculate output size after deduplication and tombstone removal
        let dedup_factor = 0.85; // Remove 15% duplicates/tombstones
        let output_bytes = (total_input as f64 * dedup_factor) as u64;
        
        Ok(output_bytes)
    }
    
    async fn write_level_files(&self, _level: usize, bytes: u64) -> Result<()> {
        // TODO: Integrate with actual file writing
        // Simulate write time
        let write_time = std::time::Duration::from_millis(bytes / (1024 * 1024) * 8); // 8ms per MB
        tokio::time::sleep(write_time).await;
        
        Ok(())
    }
    
    async fn update_level_after_compaction(&self, source_level: usize, target_level: usize, merged_bytes: u64) -> Result<()> {
        let mut levels = self.levels.write().await;
        
        // Clear source level (data moved to target)
        if let Some(source) = levels.get_mut(source_level) {
            source.size_bytes = 0;
            source.file_count = 0;
            source.compaction_score = 0.0;
        }
        
        // Update target level
        if let Some(target) = levels.get_mut(target_level) {
            target.size_bytes = merged_bytes;
            // Estimate new file count (assuming 32MB files)
            target.file_count = std::cmp::max(1, (merged_bytes / (32 * 1024 * 1024)) as usize);
            target.compaction_score = self.calculate_compaction_score(target).await;
        }
        
        Ok(())
    }
    
    async fn update_compaction_stats(&self, duration: std::time::Duration, bytes_compacted: u64) -> Result<()> {
        let mut stats = self.compaction_stats.write().await;
        
        stats.total_compactions += 1;
        stats.bytes_written += bytes_compacted;
        
        // Update average compaction time
        let total_time = stats.avg_compaction_time.as_secs_f64() * (stats.total_compactions - 1) as f64 + duration.as_secs_f64();
        stats.avg_compaction_time = std::time::Duration::from_secs_f64(total_time / stats.total_compactions as f64);
        
        Ok(())
    }
    
    async fn calculate_compaction_score(&self, level: &LSMLevel) -> f64 {
        if level.level == 0 {
            // L0 uses file count
            level.file_count as f64 / 4.0  // Target 4 files in L0
        } else {
            // Other levels use size ratio
            if level.max_size_bytes == u64::MAX {
                0.0  // Bottom level never needs compaction
            } else {
                level.size_bytes as f64 / level.max_size_bytes as f64
            }
        }
    }
    
    async fn estimate_compaction_time(&self, source_level: usize, _target_level: usize, bytes: u64) -> std::time::Duration {
        // Estimate based on historical data or heuristics
        let base_time_per_mb = if source_level == 0 {
            15 // L0 compactions are slower due to overlaps
        } else {
            10 // Regular level compactions
        };
        
        let mb = bytes / (1024 * 1024);
        let estimated_ms = mb * base_time_per_mb;
        
        std::time::Duration::from_millis(estimated_ms)
    }
    
    pub async fn get_active_compactions(&self) -> Vec<String> {
        let active = self.active_compactions.read().await;
        active.keys().cloned().collect()
    }
    
    pub async fn force_compact_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<u64> {
        // TODO: Implement range-specific compaction
        let _start = start_key;
        let _end = end_key;
        
        // For now, trigger a general compaction
        self.compact_all_levels().await
    }
    
    pub async fn get_level_size_ratio(&self) -> Result<f64> {
        let levels = self.levels.read().await;
        
        // Calculate size amplification ratio
        let mut total_size = 0u64;
        let mut logical_size = 0u64;
        
        for (i, level) in levels.iter().enumerate() {
            total_size += level.size_bytes;
            if i == levels.len() - 1 {
                // Bottom level represents logical data size
                logical_size = level.size_bytes;
            }
        }
        
        if logical_size > 0 {
            Ok(total_size as f64 / logical_size as f64)
        } else {
            Ok(1.0)
        }
    }
}