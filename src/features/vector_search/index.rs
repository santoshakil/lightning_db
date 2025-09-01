use async_trait::async_trait;
use crate::core::error::Error;
use serde::{Serialize, Deserialize};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum VectorIndexType {
    Flat,
    Hnsw,
    Ivf,
    LSH,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub dimensions: usize,
    pub max_elements: usize,
    pub index_type: VectorIndexType,
    pub distance_metric: super::similarity::SimilarityMetric,
    pub build_params: IndexBuildParams,
    pub search_params: IndexSearchParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexBuildParams {
    pub num_threads: usize,
    pub batch_size: usize,
    pub memory_limit_mb: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSearchParams {
    pub ef_search: usize,
    pub num_probes: usize,
    pub rerank: bool,
}

#[async_trait]
pub trait VectorIndex: Send + Sync {
    async fn insert(&self, id: u64, vector: Vec<f32>) -> Result<(), Error>;
    
    async fn insert_batch(&self, vectors: Vec<(u64, Vec<f32>)>) -> Result<(), Error>;
    
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>, Error>;
    
    async fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<(u64, f32)>, Error>;
    
    async fn delete(&self, id: u64) -> Result<(), Error>;
    
    async fn rebuild(&self) -> Result<(), Error>;
    
    async fn optimize(&self) -> Result<(), Error>;
    
    async fn flush(&self) -> Result<(), Error>;
    
    fn size(&self) -> usize;
    
    fn dimensions(&self) -> usize;
}

pub struct FlatIndex {
    dimensions: usize,
    vectors: Arc<parking_lot::RwLock<Vec<(u64, Vec<f32>)>>>,
    distance_fn: Arc<dyn super::similarity::DistanceFunction>,
}

impl FlatIndex {
    pub fn new(
        dimensions: usize,
        metric: super::similarity::SimilarityMetric,
    ) -> Result<Self, Error> {
        Ok(Self {
            dimensions,
            vectors: Arc::new(parking_lot::RwLock::new(Vec::new())),
            distance_fn: super::similarity::create_distance_function(metric),
        })
    }
}

#[async_trait]
impl VectorIndex for FlatIndex {
    async fn insert(&self, id: u64, vector: Vec<f32>) -> Result<(), Error> {
        if vector.len() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            )));
        }
        
        self.vectors.write().push((id, vector));
        Ok(())
    }

    async fn insert_batch(&self, vectors: Vec<(u64, Vec<f32>)>) -> Result<(), Error> {
        let mut store = self.vectors.write();
        
        for (id, vector) in vectors {
            if vector.len() != self.dimensions {
                return Err(Error::InvalidInput(format!(
                    "Vector dimension mismatch: expected {}, got {}",
                    self.dimensions,
                    vector.len()
                )));
            }
            store.push((id, vector));
        }
        
        Ok(())
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>, Error> {
        if query.len() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.dimensions,
                query.len()
            )));
        }
        
        let vectors = self.vectors.read();
        let mut distances: Vec<(u64, f32)> = Vec::with_capacity(vectors.len());
        
        for (id, vector) in vectors.iter() {
            let distance = self.distance_fn.distance(query, vector);
            distances.push((*id, distance));
        }
        
        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        distances.truncate(k);
        
        Ok(distances)
    }

    async fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<(u64, f32)>, Error> {
        if query.len() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.dimensions,
                query.len()
            )));
        }
        
        let vectors = self.vectors.read();
        let mut results = Vec::new();
        
        for (id, vector) in vectors.iter() {
            let distance = self.distance_fn.distance(query, vector);
            if distance <= radius {
                results.push((*id, distance));
            }
        }
        
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        Ok(results)
    }

    async fn delete(&self, id: u64) -> Result<(), Error> {
        self.vectors.write().retain(|(vec_id, _)| *vec_id != id);
        Ok(())
    }

    async fn rebuild(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn optimize(&self) -> Result<(), Error> {
        self.vectors.write().shrink_to_fit();
        Ok(())
    }

    async fn flush(&self) -> Result<(), Error> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.vectors.read().len()
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }
}

pub struct LshIndex {
    dimensions: usize,
    num_tables: usize,
    num_buckets: usize,
    tables: Arc<parking_lot::RwLock<Vec<LshTable>>>,
    vectors: Arc<parking_lot::RwLock<std::collections::HashMap<u64, Vec<f32>>>>,
}

struct LshTable {
    hash_functions: Vec<LshHashFunction>,
    buckets: std::collections::HashMap<u64, Vec<u64>>,
}

struct LshHashFunction {
    projection: Vec<f32>,
    bias: f32,
    bucket_width: f32,
}

impl LshIndex {
    pub fn new(dimensions: usize, num_tables: usize, num_buckets: usize) -> Result<Self, Error> {
        let mut tables = Vec::with_capacity(num_tables);
        
        for _ in 0..num_tables {
            tables.push(LshTable::new(dimensions, num_buckets)?);
        }
        
        Ok(Self {
            dimensions,
            num_tables,
            num_buckets,
            tables: Arc::new(parking_lot::RwLock::new(tables)),
            vectors: Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
        })
    }
}

impl LshTable {
    fn new(dimensions: usize, num_hash_functions: usize) -> Result<Self, Error> {
        use rand::Rng;
        let mut rng = rand::rng();
        
        let mut hash_functions = Vec::with_capacity(num_hash_functions);
        
        for _ in 0..num_hash_functions {
            let projection: Vec<f32> = (0..dimensions)
                .map(|_| rng.random_range(-1.0..1.0))
                .collect();
            
            let bias = rng.random_range(0.0..1.0);
            let bucket_width = 1.0;
            
            hash_functions.push(LshHashFunction {
                projection,
                bias,
                bucket_width,
            });
        }
        
        Ok(Self {
            hash_functions,
            buckets: std::collections::HashMap::new(),
        })
    }

    fn hash(&self, vector: &[f32]) -> u64 {
        let mut hash = 0u64;
        
        for (i, func) in self.hash_functions.iter().enumerate() {
            let projection = func.projection.iter()
                .zip(vector.iter())
                .map(|(a, b)| a * b)
                .sum::<f32>();
            
            let bucket = ((projection + func.bias) / func.bucket_width).floor() as i32;
            hash ^= (bucket as u64).wrapping_mul(0x9e3779b97f4a7c15 + i as u64);
        }
        
        hash
    }

    fn insert(&mut self, id: u64, vector: &[f32]) {
        let hash = self.hash(vector);
        self.buckets.entry(hash).or_insert_with(Vec::new).push(id);
    }

    fn query(&self, vector: &[f32]) -> Vec<u64> {
        let hash = self.hash(vector);
        self.buckets.get(&hash).cloned().unwrap_or_default()
    }
}

#[async_trait]
impl VectorIndex for LshIndex {
    async fn insert(&self, id: u64, vector: Vec<f32>) -> Result<(), Error> {
        if vector.len() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            )));
        }
        
        let mut tables = self.tables.write();
        for table in tables.iter_mut() {
            table.insert(id, &vector);
        }
        
        self.vectors.write().insert(id, vector);
        
        Ok(())
    }

    async fn insert_batch(&self, vectors: Vec<(u64, Vec<f32>)>) -> Result<(), Error> {
        for (id, vector) in vectors {
            self.insert(id, vector).await?;
        }
        Ok(())
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>, Error> {
        if query.len() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.dimensions,
                query.len()
            )));
        }
        
        let tables = self.tables.read();
        let mut candidates = std::collections::HashSet::new();
        
        for table in tables.iter() {
            for id in table.query(query) {
                candidates.insert(id);
            }
        }
        
        let vectors = self.vectors.read();
        let mut results = Vec::new();
        
        for id in candidates {
            if let Some(vector) = vectors.get(&id) {
                let distance = super::similarity::cosine_distance(query, vector);
                results.push((id, distance));
            }
        }
        
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        results.truncate(k);
        
        Ok(results)
    }

    async fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<(u64, f32)>, Error> {
        let all_results = self.search(query, self.vectors.read().len()).await?;
        Ok(all_results.into_iter().filter(|(_, dist)| *dist <= radius).collect())
    }

    async fn delete(&self, id: u64) -> Result<(), Error> {
        self.vectors.write().remove(&id);
        Ok(())
    }

    async fn rebuild(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn optimize(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn flush(&self) -> Result<(), Error> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.vectors.read().len()
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }
}
