use std::sync::Arc;
use std::collections::HashMap;
use parking_lot::RwLock;
use crate::core::error::Error;
use crate::core::async_page_manager::PageManagerAsync;
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot};
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorConfig {
    pub dimensions: usize,
    pub max_vectors: usize,
    pub index_type: super::index::VectorIndexType,
    pub similarity_metric: super::similarity::SimilarityMetric,
    pub enable_quantization: bool,
    pub quantization_type: Option<super::quantization::QuantizationType>,
    pub cache_size: usize,
    pub batch_size: usize,
    pub num_threads: usize,
    pub enable_gpu: bool,
    pub enable_simd: bool,
    pub memory_mapped: bool,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            dimensions: 128,
            max_vectors: 1_000_000,
            index_type: super::index::VectorIndexType::Hnsw,
            similarity_metric: super::similarity::SimilarityMetric::Cosine,
            enable_quantization: false,
            quantization_type: None,
            cache_size: 1024 * 1024 * 256,
            batch_size: 1000,
            num_threads: 4,
            enable_gpu: false,
            enable_simd: true,
            memory_mapped: false,
        }
    }
}

pub struct VectorEngine {
    config: Arc<VectorConfig>,
    storage: Arc<dyn PageManagerAsync>,
    indices: Arc<DashMap<String, Arc<dyn super::index::VectorIndex>>>,
    cache: Arc<DashMap<u64, Arc<Vec<f32>>>>,
    quantizer: Option<Arc<dyn super::quantization::Quantizer>>,
    
    metrics: Arc<VectorMetrics>,
    shutdown: Arc<tokio::sync::Notify>,
}

struct VectorMetrics {
    searches_performed: AtomicU64,
    vectors_indexed: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    avg_search_time_ms: AtomicU64,
    total_memory_bytes: AtomicUsize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResult {
    pub id: u64,
    pub distance: f32,
    pub vector: Option<Vec<f32>>,
    pub metadata: Option<HashMap<String, Vec<u8>>>,
}

impl VectorEngine {
    pub async fn new(
        config: VectorConfig,
        storage: Arc<dyn PageManagerAsync>,
    ) -> Result<Self, Error> {
        let quantizer = if config.enable_quantization {
            if let Some(qt) = &config.quantization_type {
                Some(Arc::new(super::quantization::create_quantizer(
                    qt.clone(),
                    config.dimensions,
                )?) as Arc<dyn super::quantization::Quantizer>)
            } else {
                None
            }
        } else {
            None
        };
        
        Ok(Self {
            config: Arc::new(config),
            storage,
            indices: Arc::new(DashMap::new()),
            cache: Arc::new(DashMap::new()),
            quantizer,
            metrics: Arc::new(VectorMetrics {
                searches_performed: AtomicU64::new(0),
                vectors_indexed: AtomicU64::new(0),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                avg_search_time_ms: AtomicU64::new(0),
                total_memory_bytes: AtomicUsize::new(0),
            }),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        })
    }

    pub async fn create_index(&self, name: &str) -> Result<(), Error> {
        let index = match self.config.index_type {
            super::index::VectorIndexType::Hnsw => {
                let config = super::hnsw::HnswConfig {
                    dimensions: self.config.dimensions,
                    max_connections: 16,
                    ef_construction: 200,
                    similarity_metric: self.config.similarity_metric.clone(),
                    seed: 42,
                };
                Arc::new(super::hnsw::HnswIndex::new(config)?) as Arc<dyn super::index::VectorIndex>
            },
            super::index::VectorIndexType::Ivf => {
                let config = super::ivf::IvfConfig {
                    dimensions: self.config.dimensions,
                    num_clusters: 256,
                    num_probes: 8,
                    similarity_metric: self.config.similarity_metric.clone(),
                };
                Arc::new(super::ivf::IvfIndex::new(config).await?) as Arc<dyn super::index::VectorIndex>
            },
            super::index::VectorIndexType::Flat => {
                Arc::new(super::index::FlatIndex::new(
                    self.config.dimensions,
                    self.config.similarity_metric.clone(),
                )?) as Arc<dyn super::index::VectorIndex>
            },
            super::index::VectorIndexType::LSH => {
                Arc::new(super::index::LshIndex::new(
                    self.config.dimensions,
                    128,
                    4,
                )?) as Arc<dyn super::index::VectorIndex>
            },
        };
        
        self.indices.insert(name.to_string(), index);
        Ok(())
    }

    pub async fn insert_vector(
        &self,
        index_name: &str,
        id: u64,
        vector: Vec<f32>,
        metadata: Option<HashMap<String, Vec<u8>>>,
    ) -> Result<(), Error> {
        if vector.len() != self.config.dimensions {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                vector.len()
            )));
        }
        
        let index = self.indices.get(index_name)
            .ok_or_else(|| Error::NotFound(format!("Index {} not found", index_name)))?;
        
        let processed_vector = if let Some(quantizer) = &self.quantizer {
            quantizer.encode(&vector)?
        } else {
            vector.clone()
        };
        
        index.insert(id, processed_vector).await?;
        
        self.cache.insert(id, Arc::new(vector));
        
        if let Some(meta) = metadata {
            let storage_meta = super::storage::VectorMetadata {
                id,
                dimensions: self.config.dimensions,
                created_at: chrono::Utc::now(),
                metadata: meta,
            };
            
            super::storage::store_metadata(&*self.storage, &storage_meta).await?;
        }
        
        self.metrics.vectors_indexed.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }

    pub async fn insert_batch(
        &self,
        index_name: &str,
        vectors: Vec<(u64, Vec<f32>, Option<HashMap<String, Vec<u8>>>)>,
    ) -> Result<(), Error> {
        let index = self.indices.get(index_name)
            .ok_or_else(|| Error::NotFound(format!("Index {} not found", index_name)))?;
        
        let mut batch = Vec::with_capacity(vectors.len());
        
        for (id, vector, metadata) in vectors {
            if vector.len() != self.config.dimensions {
                return Err(Error::InvalidInput(format!(
                    "Vector dimension mismatch for id {}: expected {}, got {}",
                    id, self.config.dimensions, vector.len()
                )));
            }
            
            let processed_vector = if let Some(quantizer) = &self.quantizer {
                quantizer.encode(&vector)?
            } else {
                vector.clone()
            };
            
            batch.push((id, processed_vector));
            self.cache.insert(id, Arc::new(vector));
            
            if let Some(meta) = metadata {
                let storage_meta = super::storage::VectorMetadata {
                    id,
                    dimensions: self.config.dimensions,
                    created_at: chrono::Utc::now(),
                    metadata: meta,
                };
                
                super::storage::store_metadata(&*self.storage, &storage_meta).await?;
            }
        }
        
        index.insert_batch(batch).await?;
        
        self.metrics.vectors_indexed.fetch_add(batch.len() as u64, Ordering::Relaxed);
        
        Ok(())
    }

    pub async fn search(
        &self,
        index_name: &str,
        query: Vec<f32>,
        k: usize,
        filter: Option<Box<dyn Fn(&HashMap<String, Vec<u8>>) -> bool + Send + Sync>>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        let start = Instant::now();
        
        if query.len() != self.config.dimensions {
            return Err(Error::InvalidInput(format!(
                "Query vector dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                query.len()
            )));
        }
        
        let index = self.indices.get(index_name)
            .ok_or_else(|| Error::NotFound(format!("Index {} not found", index_name)))?;
        
        let processed_query = if let Some(quantizer) = &self.quantizer {
            quantizer.encode(&query)?
        } else {
            query
        };
        
        let candidates = index.search(&processed_query, k * 2).await?;
        
        let mut results = Vec::new();
        
        for (id, distance) in candidates {
            if let Some(filter_fn) = &filter {
                if let Ok(metadata) = super::storage::load_metadata(&*self.storage, id).await {
                    if !filter_fn(&metadata.metadata) {
                        continue;
                    }
                }
            }
            
            let vector = if let Some(cached) = self.cache.get(&id) {
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                Some(cached.as_ref().clone())
            } else {
                self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            };
            
            let metadata = super::storage::load_metadata(&*self.storage, id)
                .await
                .ok()
                .map(|m| m.metadata);
            
            results.push(VectorSearchResult {
                id,
                distance,
                vector,
                metadata,
            });
            
            if results.len() >= k {
                break;
            }
        }
        
        let elapsed_ms = start.elapsed().as_millis() as u64;
        self.update_avg_search_time(elapsed_ms);
        self.metrics.searches_performed.fetch_add(1, Ordering::Relaxed);
        
        Ok(results)
    }

    pub async fn range_search(
        &self,
        index_name: &str,
        query: Vec<f32>,
        radius: f32,
        limit: Option<usize>,
    ) -> Result<Vec<VectorSearchResult>, Error> {
        if query.len() != self.config.dimensions {
            return Err(Error::InvalidInput(format!(
                "Query vector dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                query.len()
            )));
        }
        
        let index = self.indices.get(index_name)
            .ok_or_else(|| Error::NotFound(format!("Index {} not found", index_name)))?;
        
        let processed_query = if let Some(quantizer) = &self.quantizer {
            quantizer.encode(&query)?
        } else {
            query
        };
        
        let candidates = index.range_search(&processed_query, radius).await?;
        
        let mut results = Vec::new();
        let max_results = limit.unwrap_or(candidates.len());
        
        for (id, distance) in candidates.into_iter().take(max_results) {
            let vector = self.cache.get(&id).map(|v| v.as_ref().clone());
            
            let metadata = super::storage::load_metadata(&*self.storage, id)
                .await
                .ok()
                .map(|m| m.metadata);
            
            results.push(VectorSearchResult {
                id,
                distance,
                vector,
                metadata,
            });
        }
        
        Ok(results)
    }

    pub async fn delete_vector(&self, index_name: &str, id: u64) -> Result<(), Error> {
        let index = self.indices.get(index_name)
            .ok_or_else(|| Error::NotFound(format!("Index {} not found", index_name)))?;
        
        index.delete(id).await?;
        self.cache.remove(&id);
        
        super::storage::delete_metadata(&*self.storage, id).await?;
        
        Ok(())
    }

    pub async fn update_vector(
        &self,
        index_name: &str,
        id: u64,
        vector: Vec<f32>,
        metadata: Option<HashMap<String, Vec<u8>>>,
    ) -> Result<(), Error> {
        self.delete_vector(index_name, id).await?;
        self.insert_vector(index_name, id, vector, metadata).await
    }

    pub async fn rebuild_index(&self, index_name: &str) -> Result<(), Error> {
        let index = self.indices.get(index_name)
            .ok_or_else(|| Error::NotFound(format!("Index {} not found", index_name)))?;
        
        index.rebuild().await?;
        
        Ok(())
    }

    pub async fn optimize_index(&self, index_name: &str) -> Result<(), Error> {
        let index = self.indices.get(index_name)
            .ok_or_else(|| Error::NotFound(format!("Index {} not found", index_name)))?;
        
        index.optimize().await?;
        
        Ok(())
    }

    pub fn get_metrics(&self) -> VectorEngineMetrics {
        VectorEngineMetrics {
            searches_performed: self.metrics.searches_performed.load(Ordering::Relaxed),
            vectors_indexed: self.metrics.vectors_indexed.load(Ordering::Relaxed),
            cache_hits: self.metrics.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.metrics.cache_misses.load(Ordering::Relaxed),
            avg_search_time_ms: self.metrics.avg_search_time_ms.load(Ordering::Relaxed),
            total_memory_bytes: self.metrics.total_memory_bytes.load(Ordering::Relaxed),
            cache_size: self.cache.len(),
            num_indices: self.indices.len(),
        }
    }

    fn update_avg_search_time(&self, new_time_ms: u64) {
        let current_avg = self.metrics.avg_search_time_ms.load(Ordering::Relaxed);
        let searches = self.metrics.searches_performed.load(Ordering::Relaxed) + 1;
        
        let new_avg = if searches == 1 {
            new_time_ms
        } else {
            ((current_avg * (searches - 1)) + new_time_ms) / searches
        };
        
        self.metrics.avg_search_time_ms.store(new_avg, Ordering::Relaxed);
    }

    pub async fn shutdown(&self) {
        self.shutdown.notify_waiters();
        
        for index in self.indices.iter() {
            let _ = index.value().flush().await;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEngineMetrics {
    pub searches_performed: u64,
    pub vectors_indexed: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub avg_search_time_ms: u64,
    pub total_memory_bytes: usize,
    pub cache_size: usize,
    pub num_indices: usize,
}