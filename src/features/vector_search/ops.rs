use crate::core::error::Error;
use std::sync::Arc;
use parking_lot::RwLock;
use rayon::prelude::*;

pub struct VectorOps;

impl VectorOps {
    pub fn add(a: &[f32], b: &[f32]) -> Vec<f32> {
        a.iter().zip(b.iter()).map(|(x, y)| x + y).collect()
    }

    pub fn subtract(a: &[f32], b: &[f32]) -> Vec<f32> {
        a.iter().zip(b.iter()).map(|(x, y)| x - y).collect()
    }

    pub fn multiply(a: &[f32], scalar: f32) -> Vec<f32> {
        a.iter().map(|x| x * scalar).collect()
    }

    pub fn divide(a: &[f32], scalar: f32) -> Result<Vec<f32>, Error> {
        if scalar == 0.0 {
            return Err(Error::InvalidInput("Division by zero".to_string()));
        }
        Ok(a.iter().map(|x| x / scalar).collect())
    }

    pub fn mean(vectors: &[Vec<f32>]) -> Vec<f32> {
        if vectors.is_empty() {
            return Vec::new();
        }
        
        let dimensions = vectors[0].len();
        let mut result = vec![0.0; dimensions];
        
        for vector in vectors {
            for (i, &val) in vector.iter().enumerate() {
                result[i] += val;
            }
        }
        
        let count = vectors.len() as f32;
        for val in result.iter_mut() {
            *val /= count;
        }
        
        result
    }

    pub fn variance(vectors: &[Vec<f32>]) -> Vec<f32> {
        if vectors.is_empty() {
            return Vec::new();
        }
        
        let mean = Self::mean(vectors);
        let dimensions = vectors[0].len();
        let mut variance = vec![0.0; dimensions];
        
        for vector in vectors {
            for (i, &val) in vector.iter().enumerate() {
                let diff = val - mean[i];
                variance[i] += diff * diff;
            }
        }
        
        let count = vectors.len() as f32;
        for val in variance.iter_mut() {
            *val /= count;
        }
        
        variance
    }

    pub fn standard_deviation(vectors: &[Vec<f32>]) -> Vec<f32> {
        Self::variance(vectors)
            .into_iter()
            .map(|v| v.sqrt())
            .collect()
    }

    pub fn pca(vectors: &[Vec<f32>], n_components: usize) -> Result<PcaTransform, Error> {
        if vectors.is_empty() {
            return Err(Error::InvalidInput("No vectors provided for PCA".to_string()));
        }
        
        let dimensions = vectors[0].len();
        if n_components > dimensions {
            return Err(Error::InvalidInput(format!(
                "n_components {} exceeds dimensions {}",
                n_components, dimensions
            )));
        }
        
        let mean = Self::mean(vectors);
        
        let mut covariance = vec![vec![0.0; dimensions]; dimensions];
        let n = vectors.len() as f32;
        
        for vector in vectors {
            for i in 0..dimensions {
                for j in 0..dimensions {
                    covariance[i][j] += (vector[i] - mean[i]) * (vector[j] - mean[j]) / n;
                }
            }
        }
        
        let components = Self::power_iteration(&covariance, n_components)?;
        
        Ok(PcaTransform {
            mean,
            components,
            n_components,
        })
    }

    fn power_iteration(matrix: &[Vec<f32>], n_components: usize) -> Result<Vec<Vec<f32>>, Error> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        
        let dimensions = matrix.len();
        let mut components = Vec::new();
        let mut matrix = matrix.to_vec();
        
        for _ in 0..n_components {
            let mut eigenvector: Vec<f32> = (0..dimensions)
                .map(|_| rng.gen_range(-1.0..1.0))
                .collect();
            
            super::similarity::normalize(&mut eigenvector);
            
            for _ in 0..100 {
                let mut new_vector = vec![0.0; dimensions];
                
                for i in 0..dimensions {
                    for j in 0..dimensions {
                        new_vector[i] += matrix[i][j] * eigenvector[j];
                    }
                }
                
                super::similarity::normalize(&mut new_vector);
                
                let diff: f32 = eigenvector.iter()
                    .zip(new_vector.iter())
                    .map(|(a, b)| (a - b).abs())
                    .sum();
                
                eigenvector = new_vector;
                
                if diff < 1e-6 {
                    break;
                }
            }
            
            components.push(eigenvector.clone());
            
            let eigenvalue = Self::rayleigh_quotient(&matrix, &eigenvector);
            for i in 0..dimensions {
                for j in 0..dimensions {
                    matrix[i][j] -= eigenvalue * eigenvector[i] * eigenvector[j];
                }
            }
        }
        
        Ok(components)
    }

    fn rayleigh_quotient(matrix: &[Vec<f32>], vector: &[f32]) -> f32 {
        let dimensions = matrix.len();
        let mut result = vec![0.0; dimensions];
        
        for i in 0..dimensions {
            for j in 0..dimensions {
                result[i] += matrix[i][j] * vector[j];
            }
        }
        
        let numerator: f32 = vector.iter()
            .zip(result.iter())
            .map(|(v, r)| v * r)
            .sum();
        
        let denominator: f32 = vector.iter()
            .map(|v| v * v)
            .sum();
        
        numerator / denominator
    }

    pub fn random_projection(
        vectors: &[Vec<f32>],
        target_dimensions: usize,
    ) -> Result<RandomProjection, Error> {
        if vectors.is_empty() {
            return Err(Error::InvalidInput("No vectors provided".to_string()));
        }
        
        let source_dimensions = vectors[0].len();
        
        use rand::Rng;
        use rand_distr::{Distribution, Normal};
        let mut rng = rand::thread_rng();
        let normal = Normal::new(0.0, 1.0 / (target_dimensions as f32).sqrt())
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        
        let mut projection_matrix = Vec::new();
        
        for _ in 0..target_dimensions {
            let row: Vec<f32> = (0..source_dimensions)
                .map(|_| normal.sample(&mut rng))
                .collect();
            projection_matrix.push(row);
        }
        
        Ok(RandomProjection {
            projection_matrix,
            source_dimensions,
            target_dimensions,
        })
    }
}

pub struct VectorBatch {
    vectors: Arc<RwLock<Vec<Vec<f32>>>>,
    dimensions: usize,
}

impl VectorBatch {
    pub fn new(dimensions: usize) -> Self {
        Self {
            vectors: Arc::new(RwLock::new(Vec::new())),
            dimensions,
        }
    }

    pub fn add(&self, vector: Vec<f32>) -> Result<(), Error> {
        if vector.len() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            )));
        }
        
        self.vectors.write().push(vector);
        Ok(())
    }

    pub fn add_batch(&self, vectors: Vec<Vec<f32>>) -> Result<(), Error> {
        for vector in vectors {
            self.add(vector)?;
        }
        Ok(())
    }

    pub fn normalize_all(&self) {
        let mut vectors = self.vectors.write();
        vectors.par_iter_mut().for_each(|v| {
            super::similarity::normalize(v);
        });
    }

    pub fn apply_transform<F>(&self, transform: F)
    where
        F: Fn(&mut [f32]) + Sync + Send,
    {
        let mut vectors = self.vectors.write();
        vectors.par_iter_mut().for_each(|v| {
            transform(v);
        });
    }

    pub fn filter<F>(&self, predicate: F) -> Vec<Vec<f32>>
    where
        F: Fn(&[f32]) -> bool + Sync + Send,
    {
        let vectors = self.vectors.read();
        vectors.par_iter()
            .filter(|v| predicate(v))
            .cloned()
            .collect()
    }

    pub fn map<F, T>(&self, mapper: F) -> Vec<T>
    where
        F: Fn(&[f32]) -> T + Sync + Send,
        T: Send,
    {
        let vectors = self.vectors.read();
        vectors.par_iter()
            .map(|v| mapper(v))
            .collect()
    }

    pub fn reduce<F, T>(&self, init: T, reducer: F) -> T
    where
        F: Fn(T, &[f32]) -> T + Sync + Send,
        T: Send + Clone,
    {
        let vectors = self.vectors.read();
        vectors.par_iter()
            .fold(|| init.clone(), |acc, v| reducer(acc, v))
            .reduce(|| init.clone(), |a, b| reducer(a, &[]))
    }

    pub fn compute_distances(&self, query: &[f32], metric: super::similarity::SimilarityMetric) -> Vec<f32> {
        let distance_fn = super::similarity::create_distance_function(metric);
        let vectors = self.vectors.read();
        
        vectors.par_iter()
            .map(|v| distance_fn.distance(query, v))
            .collect()
    }

    pub fn find_nearest(&self, query: &[f32], k: usize, metric: super::similarity::SimilarityMetric) -> Vec<(usize, f32)> {
        let distances = self.compute_distances(query, metric);
        
        let mut indexed_distances: Vec<(usize, f32)> = distances
            .into_iter()
            .enumerate()
            .collect();
        
        indexed_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        indexed_distances.truncate(k);
        
        indexed_distances
    }

    pub fn cluster_kmeans(&self, k: usize) -> Result<Vec<usize>, Error> {
        let vectors = self.vectors.read();
        if vectors.len() < k {
            return Err(Error::InvalidInput(format!(
                "Not enough vectors ({}) for {} clusters",
                vectors.len(), k
            )));
        }
        
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        
        let mut centroids: Vec<Vec<f32>> = vectors
            .choose_multiple(&mut rng, k)
            .cloned()
            .collect();
        
        let mut assignments = vec![0; vectors.len()];
        let max_iterations = 100;
        
        for _ in 0..max_iterations {
            let new_assignments: Vec<usize> = vectors
                .par_iter()
                .map(|vector| {
                    let mut min_distance = f32::MAX;
                    let mut nearest = 0;
                    
                    for (i, centroid) in centroids.iter().enumerate() {
                        let distance = super::similarity::euclidean_distance(vector, centroid);
                        if distance < min_distance {
                            min_distance = distance;
                            nearest = i;
                        }
                    }
                    
                    nearest
                })
                .collect();
            
            if new_assignments == assignments {
                break;
            }
            
            assignments = new_assignments;
            
            for i in 0..k {
                let cluster_vectors: Vec<Vec<f32>> = vectors
                    .iter()
                    .zip(assignments.iter())
                    .filter(|(_, &assignment)| assignment == i)
                    .map(|(v, _)| v.clone())
                    .collect();
                
                if !cluster_vectors.is_empty() {
                    centroids[i] = super::similarity::compute_centroid(&cluster_vectors);
                }
            }
        }
        
        Ok(assignments)
    }

    pub fn size(&self) -> usize {
        self.vectors.read().len()
    }

    pub fn clear(&self) {
        self.vectors.write().clear();
    }

    pub fn get_vectors(&self) -> Vec<Vec<f32>> {
        self.vectors.read().clone()
    }
}

pub struct PcaTransform {
    mean: Vec<f32>,
    components: Vec<Vec<f32>>,
    n_components: usize,
}

impl PcaTransform {
    pub fn transform(&self, vector: &[f32]) -> Vec<f32> {
        let centered: Vec<f32> = vector.iter()
            .zip(self.mean.iter())
            .map(|(v, m)| v - m)
            .collect();
        
        let mut result = vec![0.0; self.n_components];
        
        for (i, component) in self.components.iter().enumerate() {
            result[i] = centered.iter()
                .zip(component.iter())
                .map(|(c, p)| c * p)
                .sum();
        }
        
        result
    }

    pub fn inverse_transform(&self, reduced: &[f32]) -> Vec<f32> {
        let mut result = self.mean.clone();
        
        for (i, &value) in reduced.iter().enumerate() {
            for (j, &component_value) in self.components[i].iter().enumerate() {
                result[j] += value * component_value;
            }
        }
        
        result
    }
}

pub struct RandomProjection {
    projection_matrix: Vec<Vec<f32>>,
    source_dimensions: usize,
    target_dimensions: usize,
}

impl RandomProjection {
    pub fn project(&self, vector: &[f32]) -> Result<Vec<f32>, Error> {
        if vector.len() != self.source_dimensions {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.source_dimensions,
                vector.len()
            )));
        }
        
        let mut result = vec![0.0; self.target_dimensions];
        
        for (i, row) in self.projection_matrix.iter().enumerate() {
            result[i] = row.iter()
                .zip(vector.iter())
                .map(|(p, v)| p * v)
                .sum();
        }
        
        Ok(result)
    }

    pub fn batch_project(&self, vectors: &[Vec<f32>]) -> Result<Vec<Vec<f32>>, Error> {
        vectors.par_iter()
            .map(|v| self.project(v))
            .collect()
    }
}