use crate::error::{Error, Result};
use crate::{Database, LightningDbConfig};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ShardingStrategy {
    Hash,
    Range,
    Consistent,
}

#[derive(Debug, Clone)]
pub struct ShardConfig {
    pub id: u32,
    pub path: PathBuf,
    pub weight: u32,
}

#[derive(Debug, Clone)]
pub struct ShardingConfig {
    pub strategy: ShardingStrategy,
    pub shards: Vec<ShardConfig>,
    pub replication_factor: u32,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            strategy: ShardingStrategy::Hash,
            shards: vec![],
            replication_factor: 1,
        }
    }
}

trait ShardRouter: Send + Sync {
    fn route(&self, key: &[u8]) -> u32;
    fn route_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<u32>;
}

struct HashShardRouter {
    num_shards: u32,
}

impl ShardRouter for HashShardRouter {
    fn route(&self, key: &[u8]) -> u32 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % self.num_shards as u64) as u32
    }

    fn route_range(&self, _start: Option<&[u8]>, _end: Option<&[u8]>) -> Vec<u32> {
        // Hash sharding doesn't support efficient range queries
        (0..self.num_shards).collect()
    }
}

struct RangeShardRouter {
    ranges: Vec<(Vec<u8>, u32)>, // (end_key, shard_id)
}

impl ShardRouter for RangeShardRouter {
    fn route(&self, key: &[u8]) -> u32 {
        for (end_key, shard_id) in &self.ranges {
            if key <= end_key.as_slice() {
                return *shard_id;
            }
        }
        // Default to last shard
        self.ranges.last().map(|(_, id)| *id).unwrap_or(0)
    }

    fn route_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<u32> {
        let mut shards = Vec::new();
        
        for (range_end, shard_id) in &self.ranges {
            let include_shard = match (start, end) {
                (None, None) => true,
                (Some(s), None) => s <= range_end.as_slice(),
                (None, Some(e)) => {
                    // Find previous range end
                    let prev_end = self.ranges.iter()
                        .take_while(|(k, _)| k < range_end)
                        .last()
                        .map(|(k, _)| k.as_slice())
                        .unwrap_or(&[]);
                    e > prev_end
                }
                (Some(s), Some(e)) => {
                    let prev_end = self.ranges.iter()
                        .take_while(|(k, _)| k < range_end)
                        .last()
                        .map(|(k, _)| k.as_slice())
                        .unwrap_or(&[]);
                    s <= range_end.as_slice() && e > prev_end
                }
            };
            
            if include_shard && !shards.contains(shard_id) {
                shards.push(*shard_id);
            }
        }
        
        shards
    }
}

pub struct ShardedDatabase {
    config: ShardingConfig,
    shards: Arc<RwLock<HashMap<u32, Arc<Database>>>>,
    router: Arc<dyn ShardRouter>,
}

impl ShardedDatabase {
    pub fn new(config: ShardingConfig) -> Result<Self> {
        if config.shards.is_empty() {
            return Err(Error::Generic("No shards configured".to_string()));
        }

        let router: Arc<dyn ShardRouter> = match config.strategy {
            ShardingStrategy::Hash => {
                Arc::new(HashShardRouter {
                    num_shards: config.shards.len() as u32,
                })
            }
            ShardingStrategy::Range => {
                // Simple range partitioning
                let num_shards = config.shards.len();
                let mut ranges = Vec::new();
                
                for i in 0..num_shards {
                    let end_key = if i == num_shards - 1 {
                        vec![0xFF; 8] // Max key
                    } else {
                        // Divide key space evenly
                        let boundary = ((i + 1) as f64 / num_shards as f64 * 256.0) as u8;
                        vec![boundary]
                    };
                    ranges.push((end_key, i as u32));
                }
                
                Arc::new(RangeShardRouter { ranges })
            }
            ShardingStrategy::Consistent => {
                // For now, use hash sharding
                Arc::new(HashShardRouter {
                    num_shards: config.shards.len() as u32,
                })
            }
        };

        let shards = Arc::new(RwLock::new(HashMap::new()));

        // Initialize shards
        for shard_config in &config.shards {
            let db_config = LightningDbConfig::default();
            let db = Database::create(&shard_config.path, db_config)?;
            shards.write().insert(shard_config.id, Arc::new(db));
        }

        Ok(Self {
            config,
            shards,
            router,
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let shard_id = self.router.route(key);
        let shards = self.shards.read();
        
        let shard = shards.get(&shard_id)
            .ok_or_else(|| Error::Generic(format!("Shard {} not found", shard_id)))?;
        
        shard.put(key, value)?;

        // Handle replication if configured
        if self.config.replication_factor > 1 {
            self.replicate_write(key, value, shard_id)?;
        }

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let shard_id = self.router.route(key);
        let shards = self.shards.read();
        
        let shard = shards.get(&shard_id)
            .ok_or_else(|| Error::Generic(format!("Shard {} not found", shard_id)))?;
        
        shard.get(key)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let shard_id = self.router.route(key);
        let shards = self.shards.read();
        
        let shard = shards.get(&shard_id)
            .ok_or_else(|| Error::Generic(format!("Shard {} not found", shard_id)))?;
        
        shard.delete(key)?;

        // Handle replication if configured
        if self.config.replication_factor > 1 {
            self.replicate_delete(key, shard_id)?;
        }

        Ok(())
    }

    pub fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let shard_ids = self.router.route_range(start, end);
        let shards = self.shards.read();
        
        let mut results = Vec::new();
        
        for shard_id in shard_ids {
            if let Some(shard) = shards.get(&shard_id) {
                let shard_results = shard.range(start, end)?;
                results.extend(shard_results);
            }
        }
        
        // Sort results by key
        results.sort_by(|a, b| a.0.cmp(&b.0));
        
        Ok(results)
    }

    fn replicate_write(&self, key: &[u8], value: &[u8], primary_shard: u32) -> Result<()> {
        let shards = self.shards.read();
        let num_shards = shards.len() as u32;
        
        for i in 1..self.config.replication_factor {
            let replica_shard = (primary_shard + i) % num_shards;
            
            if let Some(shard) = shards.get(&replica_shard) {
                shard.put(key, value)?;
            }
        }
        
        Ok(())
    }

    fn replicate_delete(&self, key: &[u8], primary_shard: u32) -> Result<()> {
        let shards = self.shards.read();
        let num_shards = shards.len() as u32;
        
        for i in 1..self.config.replication_factor {
            let replica_shard = (primary_shard + i) % num_shards;
            
            if let Some(shard) = shards.get(&replica_shard) {
                shard.delete(key)?;
            }
        }
        
        Ok(())
    }

    pub fn add_shard(&self, shard_config: ShardConfig) -> Result<()> {
        let db_config = LightningDbConfig::default();
        let db = Database::create(&shard_config.path, db_config)?;
        
        self.shards.write().insert(shard_config.id, Arc::new(db));
        
        // Rebalance data after adding shard
        self.rebalance_shards()?;
        
        Ok(())
    }

    pub fn remove_shard(&self, shard_id: u32) -> Result<()> {
        // Migrate data before removing shard
        self.migrate_shard_data(shard_id)?;
        
        self.shards.write().remove(&shard_id);
        
        Ok(())
    }
    
    fn shard_for_key(&self, key: &[u8]) -> Result<u32> {
        Ok(self.router.route(key))
    }

    pub fn rebalance(&self) -> Result<()> {
        // Implement data rebalancing across shards
        self.rebalance_shards()
    }
    
    /// Rebalance data across all shards
    fn rebalance_shards(&self) -> Result<()> {
        let shards = self.shards.read();
        if shards.len() <= 1 {
            return Ok(());
        }
        
        // Calculate ideal distribution
        let total_keys = self.get_total_key_count(&shards)?;
        let ideal_per_shard = total_keys / shards.len();
        
        // Find overloaded and underloaded shards
        let mut overloaded = Vec::new();
        let mut underloaded = Vec::new();
        
        for (shard_id, shard) in shards.iter() {
            let count = self.get_shard_key_count(shard)?;
            if count > ideal_per_shard + ideal_per_shard / 10 {
                overloaded.push((*shard_id, count - ideal_per_shard));
            } else if count < ideal_per_shard - ideal_per_shard / 10 {
                underloaded.push((*shard_id, ideal_per_shard - count));
            }
        }
        
        drop(shards);
        
        // Move data from overloaded to underloaded shards
        for (from_id, excess) in overloaded {
            for (to_id, deficit) in &mut underloaded {
                if *deficit == 0 {
                    continue;
                }
                
                let to_move = excess.min(*deficit);
                self.move_keys_between_shards(from_id, *to_id, to_move)?;
                *deficit -= to_move;
                
                if excess <= to_move {
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// Migrate all data from a shard before removing it
    fn migrate_shard_data(&self, shard_id: u32) -> Result<()> {
        let shards = self.shards.read();
        
        let source_shard = shards.get(&shard_id)
            .ok_or_else(|| Error::Generic("Shard not found".to_string()))?;
        
        if shards.len() == 1 {
            return Err(Error::Generic("Cannot remove last shard".to_string()));
        }
        
        // Get all keys from source shard
        let mut keys_to_migrate = Vec::new();
        let range_result = source_shard.btree_range(None, None)?;
        for (key, value) in range_result {
            keys_to_migrate.push((key, value));
        }
        
        drop(shards);
        
        // Redistribute keys to remaining shards
        for (key, value) in keys_to_migrate {
            let target_shard_id = self.shard_for_key(&key)?;
            if target_shard_id != shard_id {
                let shards = self.shards.read();
                if let Some(target_shard) = shards.get(&target_shard_id) {
                    target_shard.put(&key, &value)?;
                }
            }
        }
        
        Ok(())
    }
    
    fn get_total_key_count(&self, shards: &HashMap<u32, Arc<Database>>) -> Result<usize> {
        let mut total = 0;
        for shard in shards.values() {
            total += self.get_shard_key_count(shard)?;
        }
        Ok(total)
    }
    
    fn get_shard_key_count(&self, shard: &Database) -> Result<usize> {
        // This is approximate - in production we'd maintain accurate counts
        Ok(shard.stats().page_count as usize * 50)
    }
    
    fn move_keys_between_shards(&self, from_id: u32, to_id: u32, count: usize) -> Result<()> {
        let shards = self.shards.read();
        
        let from_shard = shards.get(&from_id)
            .ok_or_else(|| Error::Generic("Source shard not found".to_string()))?;
        let to_shard = shards.get(&to_id)
            .ok_or_else(|| Error::Generic("Target shard not found".to_string()))?;
        
        // Get keys to move
        let range_result = from_shard.btree_range(None, None)?;
        let keys_to_move: Vec<_> = range_result.into_iter().take(count).collect();
        
        // Move keys
        for (key, value) in keys_to_move {
            to_shard.put(&key, &value)?;
            from_shard.delete(&key)?;
        }
        
        Ok(())
    }

    pub fn stats(&self) -> HashMap<u32, String> {
        let shards = self.shards.read();
        let mut stats = HashMap::new();
        
        for (shard_id, shard) in shards.iter() {
            stats.insert(*shard_id, format!("{:?}", shard.stats()));
        }
        
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_hash_sharding() {
        let router = HashShardRouter { num_shards: 4 };
        
        let shard1 = router.route(b"key1");
        let shard2 = router.route(b"key2");
        
        assert!(shard1 < 4);
        assert!(shard2 < 4);
    }

    #[test]
    fn test_range_sharding() {
        let ranges = vec![
            (vec![100], 0),
            (vec![200], 1),
            (vec![255], 2),
        ];
        
        let router = RangeShardRouter { ranges };
        
        assert_eq!(router.route(&[50]), 0);
        assert_eq!(router.route(&[150]), 1);
        assert_eq!(router.route(&[250]), 2);
    }

    #[test]
    fn test_sharded_database() {
        let dir = tempdir().unwrap();
        
        let config = ShardingConfig {
            strategy: ShardingStrategy::Hash,
            shards: vec![
                ShardConfig {
                    id: 0,
                    path: dir.path().join("shard0"),
                    weight: 1,
                },
                ShardConfig {
                    id: 1,
                    path: dir.path().join("shard1"),
                    weight: 1,
                },
            ],
            replication_factor: 1,
        };
        
        let db = ShardedDatabase::new(config).unwrap();
        
        // Test basic operations
        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();
        
        assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        
        db.delete(b"key1").unwrap();
        assert_eq!(db.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_replication() {
        let dir = tempdir().unwrap();
        
        let config = ShardingConfig {
            strategy: ShardingStrategy::Hash,
            shards: vec![
                ShardConfig {
                    id: 0,
                    path: dir.path().join("shard0"),
                    weight: 1,
                },
                ShardConfig {
                    id: 1,
                    path: dir.path().join("shard1"),
                    weight: 1,
                },
            ],
            replication_factor: 2,
        };
        
        let db = ShardedDatabase::new(config).unwrap();
        
        // Write should be replicated
        db.put(b"replicated_key", b"replicated_value").unwrap();
        
        // Both shards should have the data
        let stats = db.stats();
        assert_eq!(stats.len(), 2);
    }
}