use crate::core::error::Error;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use parking_lot::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    ConsistentHash,
    Range,
    Hash,
    List,
    Composite,
    GeoHash,
    TimeRange,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionKey {
    pub raw_key: Vec<u8>,
    pub partition_columns: Vec<String>,
    pub routing_hint: Option<Vec<u8>>,
}

pub trait Partitioner: Send + Sync {
    fn partition(&self, key: &PartitionKey) -> Result<u32, Error>;
    fn get_partition_for_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<u32>, Error>;
    fn rebalance(&self, partitions: Vec<u32>) -> Result<Vec<u32>, Error>;
    fn get_partition_count(&self) -> usize;
}

pub fn create_partitioner(
    strategy: PartitionStrategy,
    num_partitions: usize,
) -> Result<Arc<dyn Partitioner>, Error> {
    match strategy {
        PartitionStrategy::ConsistentHash => {
            Ok(Arc::new(ConsistentHashPartitioner::new(num_partitions, 150)?))
        },
        PartitionStrategy::Range => {
            Ok(Arc::new(RangePartitioner::new(num_partitions)?))
        },
        PartitionStrategy::Hash => {
            Ok(Arc::new(HashPartitioner::new(num_partitions)))
        },
        PartitionStrategy::List => {
            Ok(Arc::new(ListPartitioner::new(num_partitions)?))
        },
        PartitionStrategy::GeoHash => {
            Ok(Arc::new(GeoHashPartitioner::new(num_partitions)?))
        },
        PartitionStrategy::TimeRange => {
            Ok(Arc::new(TimeRangePartitioner::new(num_partitions)?))
        },
        _ => Err(Error::InvalidInput("Unsupported partition strategy".to_string())),
    }
}

pub struct ConsistentHashPartitioner {
    num_partitions: usize,
    virtual_nodes: usize,
    hash_ring: Arc<RwLock<BTreeMap<u64, u32>>>,
}

impl ConsistentHashPartitioner {
    pub fn new(num_partitions: usize, virtual_nodes: usize) -> Result<Self, Error> {
        let mut hash_ring = BTreeMap::new();
        
        for partition in 0..num_partitions {
            for vnode in 0..virtual_nodes {
                let key = format!("partition-{}-vnode-{}", partition, vnode);
                let hash = Self::hash_key(key.as_bytes());
                hash_ring.insert(hash, partition as u32);
            }
        }
        
        Ok(Self {
            num_partitions,
            virtual_nodes,
            hash_ring: Arc::new(RwLock::new(hash_ring)),
        })
    }

    fn hash_key(key: &[u8]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn find_partition(&self, hash: u64) -> u32 {
        let ring = self.hash_ring.read();
        
        ring.range(hash..)
            .next()
            .or_else(|| ring.iter().next())
            .map(|(_, &partition)| partition)
            .unwrap_or(0)
    }

    pub fn add_partition(&self, partition_id: u32) -> Result<(), Error> {
        let mut ring = self.hash_ring.write();
        
        for vnode in 0..self.virtual_nodes {
            let key = format!("partition-{}-vnode-{}", partition_id, vnode);
            let hash = Self::hash_key(key.as_bytes());
            ring.insert(hash, partition_id);
        }
        
        Ok(())
    }

    pub fn remove_partition(&self, partition_id: u32) -> Result<(), Error> {
        let mut ring = self.hash_ring.write();
        ring.retain(|_, &mut v| v != partition_id);
        Ok(())
    }
}

impl Partitioner for ConsistentHashPartitioner {
    fn partition(&self, key: &PartitionKey) -> Result<u32, Error> {
        let hash = Self::hash_key(&key.raw_key);
        Ok(self.find_partition(hash))
    }

    fn get_partition_for_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<u32>, Error> {
        let start_hash = Self::hash_key(start);
        let end_hash = Self::hash_key(end);
        
        let ring = self.hash_ring.read();
        let mut partitions = std::collections::HashSet::new();
        
        if start_hash <= end_hash {
            for (_, &partition) in ring.range(start_hash..=end_hash) {
                partitions.insert(partition);
            }
        } else {
            for (_, &partition) in ring.range(start_hash..) {
                partitions.insert(partition);
            }
            for (_, &partition) in ring.range(..=end_hash) {
                partitions.insert(partition);
            }
        }
        
        Ok(partitions.into_iter().collect())
    }

    fn rebalance(&self, partitions: Vec<u32>) -> Result<Vec<u32>, Error> {
        Ok(partitions)
    }

    fn get_partition_count(&self) -> usize {
        self.num_partitions
    }
}

pub struct RangePartitioner {
    num_partitions: usize,
    ranges: Arc<RwLock<Vec<RangePartition>>>,
}

#[derive(Debug, Clone)]
struct RangePartition {
    partition_id: u32,
    start: Vec<u8>,
    end: Vec<u8>,
}

impl RangePartitioner {
    pub fn new(num_partitions: usize) -> Result<Self, Error> {
        let mut ranges = Vec::new();
        let range_size = u64::MAX / num_partitions as u64;
        
        for i in 0..num_partitions {
            let start = (i as u64 * range_size).to_be_bytes().to_vec();
            let end = if i == num_partitions - 1 {
                u64::MAX.to_be_bytes().to_vec()
            } else {
                ((i + 1) as u64 * range_size).to_be_bytes().to_vec()
            };
            
            ranges.push(RangePartition {
                partition_id: i as u32,
                start,
                end,
            });
        }
        
        Ok(Self {
            num_partitions,
            ranges: Arc::new(RwLock::new(ranges)),
        })
    }

    pub fn update_ranges(&self, new_ranges: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Error> {
        if new_ranges.len() != self.num_partitions {
            return Err(Error::InvalidInput("Number of ranges must match partition count".to_string()));
        }
        
        let mut ranges = self.ranges.write();
        ranges.clear();
        
        for (i, (start, end)) in new_ranges.into_iter().enumerate() {
            ranges.push(RangePartition {
                partition_id: i as u32,
                start,
                end,
            });
        }
        
        Ok(())
    }
}

impl Partitioner for RangePartitioner {
    fn partition(&self, key: &PartitionKey) -> Result<u32, Error> {
        let ranges = self.ranges.read();
        
        for range in ranges.iter() {
            if key.raw_key >= range.start && key.raw_key <= range.end {
                return Ok(range.partition_id);
            }
        }
        
        Ok(0)
    }

    fn get_partition_for_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<u32>, Error> {
        let ranges = self.ranges.read();
        let mut partitions = Vec::new();
        
        for range in ranges.iter() {
            let overlaps = !(end < &range.start || start > &range.end);
            if overlaps {
                partitions.push(range.partition_id);
            }
        }
        
        Ok(partitions)
    }

    fn rebalance(&self, partitions: Vec<u32>) -> Result<Vec<u32>, Error> {
        Ok(partitions)
    }

    fn get_partition_count(&self) -> usize {
        self.num_partitions
    }
}

pub struct HashPartitioner {
    num_partitions: usize,
}

impl HashPartitioner {
    pub fn new(num_partitions: usize) -> Self {
        Self { num_partitions }
    }

    fn hash_key(key: &[u8]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

impl Partitioner for HashPartitioner {
    fn partition(&self, key: &PartitionKey) -> Result<u32, Error> {
        let hash = Self::hash_key(&key.raw_key);
        Ok((hash % self.num_partitions as u64) as u32)
    }

    fn get_partition_for_range(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<u32>, Error> {
        Ok((0..self.num_partitions as u32).collect())
    }

    fn rebalance(&self, partitions: Vec<u32>) -> Result<Vec<u32>, Error> {
        Ok(partitions)
    }

    fn get_partition_count(&self) -> usize {
        self.num_partitions
    }
}

pub struct ListPartitioner {
    num_partitions: usize,
    value_to_partition: Arc<RwLock<std::collections::HashMap<Vec<u8>, u32>>>,
}

impl ListPartitioner {
    pub fn new(num_partitions: usize) -> Result<Self, Error> {
        Ok(Self {
            num_partitions,
            value_to_partition: Arc::new(RwLock::new(std::collections::HashMap::new())),
        })
    }

    pub fn add_value(&self, value: Vec<u8>, partition: u32) -> Result<(), Error> {
        if partition >= self.num_partitions as u32 {
            return Err(Error::InvalidInput(format!(
                "Partition {} exceeds max {}",
                partition, self.num_partitions
            )));
        }
        
        self.value_to_partition.write().insert(value, partition);
        Ok(())
    }

    pub fn remove_value(&self, value: &[u8]) -> Result<(), Error> {
        self.value_to_partition.write().remove(value);
        Ok(())
    }
}

impl Partitioner for ListPartitioner {
    fn partition(&self, key: &PartitionKey) -> Result<u32, Error> {
        self.value_to_partition.read()
            .get(&key.raw_key)
            .copied()
            .ok_or_else(|| Error::NotFound("Value not found in list partition".to_string()))
    }

    fn get_partition_for_range(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<u32>, Error> {
        Ok((0..self.num_partitions as u32).collect())
    }

    fn rebalance(&self, partitions: Vec<u32>) -> Result<Vec<u32>, Error> {
        Ok(partitions)
    }

    fn get_partition_count(&self) -> usize {
        self.num_partitions
    }
}

pub struct GeoHashPartitioner {
    num_partitions: usize,
    precision: usize,
    geohash_to_partition: Arc<RwLock<std::collections::HashMap<String, u32>>>,
}

impl GeoHashPartitioner {
    pub fn new(num_partitions: usize) -> Result<Self, Error> {
        Ok(Self {
            num_partitions,
            precision: 6,
            geohash_to_partition: Arc::new(RwLock::new(std::collections::HashMap::new())),
        })
    }

    fn encode_geohash(lat: f64, lon: f64, precision: usize) -> String {
        const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
        
        let mut geohash = String::new();
        let mut lat_range = (-90.0, 90.0);
        let mut lon_range = (-180.0, 180.0);
        let mut bits = 0u32;
        let mut bit = 0;
        let mut even = true;
        
        while geohash.len() < precision {
            if even {
                let mid = (lon_range.0 + lon_range.1) / 2.0;
                if lon > mid {
                    bits |= 1 << (4 - bit);
                    lon_range.0 = mid;
                } else {
                    lon_range.1 = mid;
                }
            } else {
                let mid = (lat_range.0 + lat_range.1) / 2.0;
                if lat > mid {
                    bits |= 1 << (4 - bit);
                    lat_range.0 = mid;
                } else {
                    lat_range.1 = mid;
                }
            }
            
            even = !even;
            
            if bit < 4 {
                bit += 1;
            } else {
                geohash.push(BASE32[bits as usize] as char);
                bits = 0;
                bit = 0;
            }
        }
        
        geohash
    }

    fn decode_location(key: &[u8]) -> Result<(f64, f64), Error> {
        if key.len() < 16 {
            return Err(Error::InvalidInput("Key too short for geo location".to_string()));
        }
        
        let lat = f64::from_be_bytes(key[0..8].try_into().unwrap());
        let lon = f64::from_be_bytes(key[8..16].try_into().unwrap());
        
        Ok((lat, lon))
    }
}

impl Partitioner for GeoHashPartitioner {
    fn partition(&self, key: &PartitionKey) -> Result<u32, Error> {
        let (lat, lon) = Self::decode_location(&key.raw_key)?;
        let geohash = Self::encode_geohash(lat, lon, self.precision);
        
        let partition = {
            let map = self.geohash_to_partition.read();
            map.get(&geohash).copied()
        };
        
        match partition {
            Some(p) => Ok(p),
            None => {
                let hash = ConsistentHashPartitioner::hash_key(geohash.as_bytes());
                let partition = (hash % self.num_partitions as u64) as u32;
                
                self.geohash_to_partition.write().insert(geohash, partition);
                Ok(partition)
            }
        }
    }

    fn get_partition_for_range(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<u32>, Error> {
        Ok((0..self.num_partitions as u32).collect())
    }

    fn rebalance(&self, partitions: Vec<u32>) -> Result<Vec<u32>, Error> {
        Ok(partitions)
    }

    fn get_partition_count(&self) -> usize {
        self.num_partitions
    }
}

pub struct TimeRangePartitioner {
    num_partitions: usize,
    time_window: chrono::Duration,
    epoch: chrono::DateTime<chrono::Utc>,
}

impl TimeRangePartitioner {
    pub fn new(num_partitions: usize) -> Result<Self, Error> {
        Ok(Self {
            num_partitions,
            time_window: chrono::Duration::days(7),
            epoch: chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        })
    }

    fn extract_timestamp(key: &[u8]) -> Result<chrono::DateTime<chrono::Utc>, Error> {
        if key.len() < 8 {
            return Err(Error::InvalidInput("Key too short for timestamp".to_string()));
        }
        
        let timestamp_millis = i64::from_be_bytes(key[0..8].try_into().unwrap());
        
        Ok(chrono::DateTime::from_timestamp_millis(timestamp_millis)
            .ok_or_else(|| Error::InvalidInput("Invalid timestamp".to_string()))?)
    }
}

impl Partitioner for TimeRangePartitioner {
    fn partition(&self, key: &PartitionKey) -> Result<u32, Error> {
        let timestamp = Self::extract_timestamp(&key.raw_key)?;
        let duration_since_epoch = timestamp.signed_duration_since(self.epoch);
        let windows = duration_since_epoch.num_milliseconds() / self.time_window.num_milliseconds();
        
        Ok((windows.abs() as u64 % self.num_partitions as u64) as u32)
    }

    fn get_partition_for_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<u32>, Error> {
        let start_time = Self::extract_timestamp(start)?;
        let end_time = Self::extract_timestamp(end)?;
        
        let mut partitions = std::collections::HashSet::new();
        
        let mut current = start_time;
        while current <= end_time {
            let key = current.timestamp_millis().to_be_bytes().to_vec();
            let partition_key = PartitionKey {
                raw_key: key,
                partition_columns: Vec::new(),
                routing_hint: None,
            };
            
            if let Ok(partition) = self.partition(&partition_key) {
                partitions.insert(partition);
            }
            
            current = current + self.time_window;
        }
        
        Ok(partitions.into_iter().collect())
    }

    fn rebalance(&self, partitions: Vec<u32>) -> Result<Vec<u32>, Error> {
        Ok(partitions)
    }

    fn get_partition_count(&self) -> usize {
        self.num_partitions
    }
}

pub struct CompositePartitioner {
    partitioners: Vec<Arc<dyn Partitioner>>,
    strategy: CompositionStrategy,
}

#[derive(Debug, Clone)]
pub enum CompositionStrategy {
    Nested,
    Combined,
    RoundRobin,
}

impl CompositePartitioner {
    pub fn new(
        partitioners: Vec<Arc<dyn Partitioner>>,
        strategy: CompositionStrategy,
    ) -> Self {
        Self {
            partitioners,
            strategy,
        }
    }
}

impl Partitioner for CompositePartitioner {
    fn partition(&self, key: &PartitionKey) -> Result<u32, Error> {
        match self.strategy {
            CompositionStrategy::Nested => {
                let mut result = 0u32;
                let mut multiplier = 1u32;
                
                for partitioner in &self.partitioners {
                    let partition = partitioner.partition(key)?;
                    result += partition * multiplier;
                    multiplier *= partitioner.get_partition_count() as u32;
                }
                
                Ok(result)
            },
            CompositionStrategy::Combined => {
                let mut hash = 0u64;
                
                for partitioner in &self.partitioners {
                    let partition = partitioner.partition(key)?;
                    hash ^= (partition as u64).rotate_left(hash.count_ones());
                }
                
                Ok((hash % self.get_partition_count() as u64) as u32)
            },
            CompositionStrategy::RoundRobin => {
                let idx = ConsistentHashPartitioner::hash_key(&key.raw_key) as usize;
                let partitioner = &self.partitioners[idx % self.partitioners.len()];
                partitioner.partition(key)
            },
        }
    }

    fn get_partition_for_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<u32>, Error> {
        let mut all_partitions = std::collections::HashSet::new();
        
        for partitioner in &self.partitioners {
            let partitions = partitioner.get_partition_for_range(start, end)?;
            all_partitions.extend(partitions);
        }
        
        Ok(all_partitions.into_iter().collect())
    }

    fn rebalance(&self, partitions: Vec<u32>) -> Result<Vec<u32>, Error> {
        Ok(partitions)
    }

    fn get_partition_count(&self) -> usize {
        match self.strategy {
            CompositionStrategy::Nested => {
                self.partitioners.iter()
                    .map(|p| p.get_partition_count())
                    .product()
            },
            _ => {
                self.partitioners.iter()
                    .map(|p| p.get_partition_count())
                    .max()
                    .unwrap_or(1)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistent_hash_partitioner() {
        let partitioner = ConsistentHashPartitioner::new(10, 150).unwrap();
        
        let key1 = PartitionKey {
            raw_key: b"key1".to_vec(),
            partition_columns: vec![],
            routing_hint: None,
        };
        
        let partition1 = partitioner.partition(&key1).unwrap();
        let partition2 = partitioner.partition(&key1).unwrap();
        
        assert_eq!(partition1, partition2);
    }

    #[test]
    fn test_range_partitioner() {
        let partitioner = RangePartitioner::new(4).unwrap();
        
        let key = PartitionKey {
            raw_key: 0u64.to_be_bytes().to_vec(),
            partition_columns: vec![],
            routing_hint: None,
        };
        
        let partition = partitioner.partition(&key).unwrap();
        assert_eq!(partition, 0);
    }

    #[test]
    fn test_hash_partitioner() {
        let partitioner = HashPartitioner::new(10);
        
        let key = PartitionKey {
            raw_key: b"test_key".to_vec(),
            partition_columns: vec![],
            routing_hint: None,
        };
        
        let partition = partitioner.partition(&key).unwrap();
        assert!(partition < 10);
    }
}