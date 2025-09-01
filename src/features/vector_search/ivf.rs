use async_trait::async_trait;
use crate::core::error::Error;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::collections::HashMap;
use parking_lot::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IvfConfig {
    pub dimensions: usize,
    pub num_clusters: usize,
    pub num_probes: usize,
    pub similarity_metric: super::similarity::SimilarityMetric,
}

pub struct IvfIndex {
    config: Arc<IvfConfig>,
    centroids: Arc<RwLock<Vec<Vec<f32>>>>,
    inverted_lists: Arc<RwLock<Vec<Vec<(u64, Vec<f32>)>>>>,
    distance_fn: Arc<dyn super::similarity::DistanceFunction>,
    trained: Arc<RwLock<bool>>,
}

impl IvfIndex {
    pub async fn new(config: IvfConfig) -> Result<Self, Error> {
        let inverted_lists = vec![Vec::new(); config.num_clusters];
        
        Ok(Self {
            distance_fn: super::similarity::create_distance_function(config.similarity_metric.clone()),
            config: Arc::new(config),
            centroids: Arc::new(RwLock::new(Vec::new())),
            inverted_lists: Arc::new(RwLock::new(inverted_lists)),
            trained: Arc::new(RwLock::new(false)),
        })
    }

    async fn train(&self, training_vectors: Vec<Vec<f32>>) -> Result<(), Error> {
        if training_vectors.is_empty() {
            return Err(Error::InvalidInput("No training vectors provided".to_string()));
        }
        
        let centroids = self.kmeans_clustering(training_vectors, self.config.num_clusters)?;
        
        *self.centroids.write() = centroids;
        *self.trained.write() = true;
        
        Ok(())
    }

    fn kmeans_clustering(
        &self,
        vectors: Vec<Vec<f32>>,
        k: usize,
    ) -> Result<Vec<Vec<f32>>, Error> {
        use rand::seq::SliceRandom;
        
        if vectors.len() < k {
            return Err(Error::InvalidInput(format!(
                "Not enough vectors for {} clusters",
                k
            )));
        }
        
        let mut rng = rand::rng();
        let mut centroids: Vec<Vec<f32>> = vectors
            .choose_multiple(&mut rng, k)
            .cloned()
            .collect();
        
        let max_iterations = 100;
        let tolerance = 1e-4;
        
        for _ in 0..max_iterations {
            let mut clusters: Vec<Vec<Vec<f32>>> = vec![Vec::new(); k];
            
            for vector in &vectors {
                let nearest = self.find_nearest_centroid(vector, &centroids);
                clusters[nearest].push(vector.clone());
            }
            
            let mut converged = true;
            
            for i in 0..k {
                if !clusters[i].is_empty() {
                    let new_centroid = super::similarity::compute_centroid(&clusters[i]);
                    
                    let distance = self.distance_fn.distance(&centroids[i], &new_centroid);
                    if distance > tolerance {
                        converged = false;
                    }
                    
                    centroids[i] = new_centroid;
                }
            }
            
            if converged {
                break;
            }
        }
        
        Ok(centroids)
    }

    fn find_nearest_centroid(&self, vector: &[f32], centroids: &[Vec<f32>]) -> usize {
        let mut min_distance = f32::MAX;
        let mut nearest = 0;
        
        for (i, centroid) in centroids.iter().enumerate() {
            let distance = self.distance_fn.distance(vector, centroid);
            if distance < min_distance {
                min_distance = distance;
                nearest = i;
            }
        }
        
        nearest
    }

    fn find_nearest_clusters(&self, query: &[f32], num_probes: usize) -> Vec<usize> {
        let centroids = self.centroids.read();
        let mut distances: Vec<(usize, f32)> = centroids
            .iter()
            .enumerate()
            .map(|(i, centroid)| (i, self.distance_fn.distance(query, centroid)))
            .collect();
        
        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        distances.into_iter()
            .take(num_probes)
            .map(|(i, _)| i)
            .collect()
    }
}

#[async_trait]
impl super::index::VectorIndex for IvfIndex {
    async fn insert(&self, id: u64, vector: Vec<f32>) -> Result<(), Error> {
        if vector.len() != self.config.dimensions {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                vector.len()
            )));
        }
        
        if !*self.trained.read() {
            return Err(Error::InvalidOperation {
                reason: "IVF index must be trained before insertion".to_string(),
            });
        }
        
        let centroids = self.centroids.read();
        let cluster_id = self.find_nearest_centroid(&vector, &centroids);
        drop(centroids);
        
        let mut inverted_lists = self.inverted_lists.write();
        inverted_lists[cluster_id].push((id, vector));
        
        Ok(())
    }

    async fn insert_batch(&self, vectors: Vec<(u64, Vec<f32>)>) -> Result<(), Error> {
        if !*self.trained.read() && !vectors.is_empty() {
            let training_vectors: Vec<Vec<f32>> = vectors.iter()
                .map(|(_, v)| v.clone())
                .collect();
            self.train(training_vectors).await?;
        }
        
        for (id, vector) in vectors {
            self.insert(id, vector).await?;
        }
        
        Ok(())
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>, Error> {
        if query.len() != self.config.dimensions {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                query.len()
            )));
        }
        
        if !*self.trained.read() {
            return Ok(Vec::new());
        }
        
        let cluster_ids = self.find_nearest_clusters(query, self.config.num_probes);
        let inverted_lists = self.inverted_lists.read();
        
        let mut candidates = Vec::new();
        
        for cluster_id in cluster_ids {
            for (id, vector) in &inverted_lists[cluster_id] {
                let distance = self.distance_fn.distance(query, vector);
                candidates.push((*id, distance));
            }
        }
        
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates.truncate(k);
        
        Ok(candidates)
    }

    async fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<(u64, f32)>, Error> {
        if query.len() != self.config.dimensions {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                query.len()
            )));
        }
        
        if !*self.trained.read() {
            return Ok(Vec::new());
        }
        
        let centroids = self.centroids.read();
        let mut cluster_ids = Vec::new();
        
        for (i, centroid) in centroids.iter().enumerate() {
            let distance = self.distance_fn.distance(query, centroid);
            if distance <= radius * 2.0 {
                cluster_ids.push(i);
            }
        }
        drop(centroids);
        
        let inverted_lists = self.inverted_lists.read();
        let mut results = Vec::new();
        
        for cluster_id in cluster_ids {
            for (id, vector) in &inverted_lists[cluster_id] {
                let distance = self.distance_fn.distance(query, vector);
                if distance <= radius {
                    results.push((*id, distance));
                }
            }
        }
        
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        Ok(results)
    }

    async fn delete(&self, id: u64) -> Result<(), Error> {
        let mut inverted_lists = self.inverted_lists.write();
        
        for list in inverted_lists.iter_mut() {
            list.retain(|(vec_id, _)| *vec_id != id);
        }
        
        Ok(())
    }

    async fn rebuild(&self) -> Result<(), Error> {
        let inverted_lists = self.inverted_lists.read();
        let mut all_vectors = Vec::new();
        
        for list in inverted_lists.iter() {
            for (id, vector) in list {
                all_vectors.push((*id, vector.clone()));
            }
        }
        
        drop(inverted_lists);
        
        if !all_vectors.is_empty() {
            let training_vectors: Vec<Vec<f32>> = all_vectors.iter()
                .map(|(_, v)| v.clone())
                .collect();
            
            self.train(training_vectors).await?;
            
            *self.inverted_lists.write() = vec![Vec::new(); self.config.num_clusters];
            
            for (id, vector) in all_vectors {
                self.insert(id, vector).await?;
            }
        }
        
        Ok(())
    }

    async fn optimize(&self) -> Result<(), Error> {
        let mut inverted_lists = self.inverted_lists.write();
        
        for list in inverted_lists.iter_mut() {
            list.shrink_to_fit();
        }
        
        Ok(())
    }

    async fn flush(&self) -> Result<(), Error> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.inverted_lists.read()
            .iter()
            .map(|list| list.len())
            .sum()
    }

    fn dimensions(&self) -> usize {
        self.config.dimensions
    }
}

pub struct IvfPqIndex {
    config: Arc<IvfConfig>,
    centroids: Arc<RwLock<Vec<Vec<f32>>>>,
    inverted_lists: Arc<RwLock<Vec<Vec<(u64, Vec<u8>)>>>>,
    product_quantizer: Arc<super::quantization::ProductQuantizer>,
    distance_fn: Arc<dyn super::similarity::DistanceFunction>,
    trained: Arc<RwLock<bool>>,
}

impl IvfPqIndex {
    pub async fn new(
        config: IvfConfig,
        num_subquantizers: usize,
        bits_per_subquantizer: usize,
    ) -> Result<Self, Error> {
        let inverted_lists = vec![Vec::new(); config.num_clusters];
        let pq = super::quantization::ProductQuantizer::new(
            config.dimensions,
            num_subquantizers,
            bits_per_subquantizer,
        )?;
        
        Ok(Self {
            distance_fn: super::similarity::create_distance_function(config.similarity_metric.clone()),
            config: Arc::new(config),
            centroids: Arc::new(RwLock::new(Vec::new())),
            inverted_lists: Arc::new(RwLock::new(inverted_lists)),
            product_quantizer: Arc::new(pq),
            trained: Arc::new(RwLock::new(false)),
        })
    }
}
