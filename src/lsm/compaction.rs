use crate::compression::CompressionType;
use crate::error::Result;
use crate::lsm::{Level, SSTable};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

pub trait CompactionStrategy: Send + Sync {
    fn pick_compaction(&self, levels: &[Level]) -> Option<CompactionTask>;
}

pub struct CompactionTask {
    pub source_level: usize,
    pub target_level: usize,
    pub input_sstables: Vec<Arc<SSTable>>,
}

pub struct LeveledCompaction {
    _level_size_multiplier: usize,
}

impl LeveledCompaction {
    pub fn new(level_size_multiplier: usize) -> Self {
        Self {
            _level_size_multiplier: level_size_multiplier,
        }
    }

    fn score_level(&self, level: &Level) -> f64 {
        if level.level_num == 0 {
            // L0 is special - compact based on file count
            level.sstables.len() as f64 / 4.0
        } else {
            // Other levels - compact based on size
            level.size_bytes as f64 / level.max_size_bytes as f64
        }
    }
}

impl CompactionStrategy for LeveledCompaction {
    fn pick_compaction(&self, levels: &[Level]) -> Option<CompactionTask> {
        // Find the level with highest compaction score
        let mut max_score = 0.0;
        let mut compact_level = None;

        for (i, level) in levels.iter().enumerate() {
            if i >= levels.len() - 1 {
                continue; // Can't compact last level
            }

            let score = self.score_level(level);
            if score > 1.0 && score > max_score {
                max_score = score;
                compact_level = Some(i);
            }
        }

        let source_level = compact_level?;
        let target_level = source_level + 1;

        // For L0 -> L1, we need to handle overlapping files
        if source_level == 0 {
            // Pick all L0 files for simplicity
            let input_sstables = levels[source_level].sstables.clone();

            if input_sstables.is_empty() {
                return None;
            }

            Some(CompactionTask {
                source_level,
                target_level,
                input_sstables,
            })
        } else {
            // For L1+, pick one file and all overlapping files in next level
            let source_file = levels[source_level].sstables.first()?.clone();
            let mut input_sstables = vec![source_file.clone()];

            // Find overlapping files in target level
            let overlapping = levels[target_level]
                .overlapping_sstables(source_file.min_key(), source_file.max_key());
            input_sstables.extend(overlapping);

            Some(CompactionTask {
                source_level,
                target_level,
                input_sstables,
            })
        }
    }
}

pub struct UniversalCompaction {
    size_ratio: f64,
    min_merge_width: usize,
    max_merge_width: usize,
}

impl UniversalCompaction {
    pub fn new() -> Self {
        Self {
            size_ratio: 1.0,
            min_merge_width: 2,
            max_merge_width: 10,
        }
    }

    fn find_compaction_candidates(&self, sstables: &[Arc<SSTable>]) -> Option<Vec<Arc<SSTable>>> {
        if sstables.len() < self.min_merge_width {
            return None;
        }

        // Sort by size (largest first)
        let mut sorted: Vec<_> = sstables.to_vec();
        sorted.sort_by_key(|sst| std::cmp::Reverse(sst.size_bytes()));

        // Look for runs of similar-sized files
        let mut start = 0;
        while start < sorted.len() - self.min_merge_width {
            let mut end = start + 1;
            let base_size = sorted[start].size_bytes() as f64;

            while end < sorted.len() && end - start < self.max_merge_width {
                let size_ratio = sorted[end].size_bytes() as f64 / base_size;
                if size_ratio < 1.0 / self.size_ratio || size_ratio > self.size_ratio {
                    break;
                }
                end += 1;
            }

            if end - start >= self.min_merge_width {
                return Some(sorted[start..end].to_vec());
            }

            start += 1;
        }

        None
    }
}

impl CompactionStrategy for UniversalCompaction {
    fn pick_compaction(&self, levels: &[Level]) -> Option<CompactionTask> {
        // Universal compaction only uses one level
        if levels.is_empty() {
            return None;
        }

        let candidates = self.find_compaction_candidates(&levels[0].sstables)?;

        Some(CompactionTask {
            source_level: 0,
            target_level: 0,
            input_sstables: candidates,
        })
    }
}

impl Default for UniversalCompaction {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TieredCompaction {
    tier_size_ratio: f64,
    min_tier_size: usize,
}

impl TieredCompaction {
    pub fn new() -> Self {
        Self {
            tier_size_ratio: 4.0,
            min_tier_size: 4,
        }
    }
}

impl CompactionStrategy for TieredCompaction {
    fn pick_compaction(&self, levels: &[Level]) -> Option<CompactionTask> {
        // Simplified tiered compaction
        for (i, level) in levels.iter().enumerate() {
            if i >= levels.len() - 1 {
                continue;
            }

            if level.sstables.len() >= self.min_tier_size {
                let total_size: usize = level.sstables.iter().map(|sst| sst.size_bytes()).sum();

                let next_level_size: usize = levels[i + 1]
                    .sstables
                    .iter()
                    .map(|sst| sst.size_bytes())
                    .sum();

                if total_size as f64 > next_level_size as f64 * self.tier_size_ratio {
                    return Some(CompactionTask {
                        source_level: i,
                        target_level: i + 1,
                        input_sstables: level.sstables.clone(),
                    });
                }
            }
        }

        None
    }
}

impl Default for TieredCompaction {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Compactor {
    db_path: PathBuf,
    compression_type: CompressionType,
}

impl Compactor {
    pub fn new(db_path: PathBuf, compression_type: CompressionType) -> Self {
        Self {
            db_path,
            compression_type,
        }
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn compression_type(&self) -> &CompressionType {
        &self.compression_type
    }

    pub fn compact_files(&self, level: usize, files: &[PathBuf]) -> Result<Vec<PathBuf>> {
        info!(
            "Compacting {} files at level {} in {} using {:?} compression",
            files.len(),
            level,
            self.db_path.display(),
            self.compression_type
        );

        // This would implement actual compaction logic
        // For now, just return empty vector
        Ok(Vec::new())
    }
}
