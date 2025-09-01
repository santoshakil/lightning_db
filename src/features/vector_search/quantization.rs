use crate::core::error::Error;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum QuantizationType {
    Scalar,
    Product,
    Residual,
    Binary,
}

pub trait Quantizer: Send + Sync {
    fn encode(&self, vector: &[f32]) -> Result<Vec<f32>, Error>;
    fn decode(&self, encoded: &[u8]) -> Result<Vec<f32>, Error>;
    fn encode_batch(&self, vectors: &[Vec<f32>]) -> Result<Vec<Vec<u8>>, Error>;
    fn compression_ratio(&self) -> f32;
}

pub fn create_quantizer(
    quantization_type: QuantizationType,
    dimensions: usize,
) -> Result<impl Quantizer, Error> {
    match quantization_type {
        QuantizationType::Scalar => Ok(ScalarQuantizer::new(dimensions, 8)?),
        QuantizationType::Product => Ok(ProductQuantizer::new(dimensions, 8, 8)?),
        QuantizationType::Binary => Ok(BinaryQuantizer::new(dimensions)?),
        _ => Err(Error::InvalidInput("Unsupported quantization type".to_string())),
    }
}

pub struct ScalarQuantizer {
    dimensions: usize,
    bits: usize,
    scale: Arc<RwLock<Vec<f32>>>,
    offset: Arc<RwLock<Vec<f32>>>,
}

impl ScalarQuantizer {
    pub fn new(dimensions: usize, bits: usize) -> Result<Self, Error> {
        if bits < 1 || bits > 32 {
            return Err(Error::InvalidInput(format!("Invalid bit width: {}", bits)));
        }
        
        Ok(Self {
            dimensions,
            bits,
            scale: Arc::new(RwLock::new(vec![1.0; dimensions])),
            offset: Arc::new(RwLock::new(vec![0.0; dimensions])),
        })
    }

    pub fn train(&self, vectors: &[Vec<f32>]) -> Result<(), Error> {
        if vectors.is_empty() {
            return Ok(());
        }
        
        let mut min_vals = vec![f32::MAX; self.dimensions];
        let mut max_vals = vec![f32::MIN; self.dimensions];
        
        for vector in vectors {
            for (i, &val) in vector.iter().enumerate() {
                min_vals[i] = min_vals[i].min(val);
                max_vals[i] = max_vals[i].max(val);
            }
        }
        
        let max_value = (1 << self.bits) - 1;
        let mut scale = self.scale.write();
        let mut offset = self.offset.write();
        
        for i in 0..self.dimensions {
            let range = max_vals[i] - min_vals[i];
            if range > 0.0 {
                scale[i] = max_value as f32 / range;
                offset[i] = min_vals[i];
            }
        }
        
        Ok(())
    }
}

impl Quantizer for ScalarQuantizer {
    fn encode(&self, vector: &[f32]) -> Result<Vec<f32>, Error> {
        if vector.len() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            )));
        }
        
        let scale = self.scale.read();
        let offset = self.offset.read();
        let max_value = (1 << self.bits) - 1;
        
        let encoded: Vec<f32> = vector.iter()
            .enumerate()
            .map(|(i, &val)| {
                let normalized = (val - offset[i]) * scale[i];
                normalized.max(0.0).min(max_value as f32).round()
            })
            .collect();
        
        Ok(encoded)
    }

    fn decode(&self, encoded: &[u8]) -> Result<Vec<f32>, Error> {
        let scale = self.scale.read();
        let offset = self.offset.read();
        
        let decoded: Vec<f32> = encoded.iter()
            .enumerate()
            .map(|(i, &val)| {
                (val as f32 / scale[i % self.dimensions]) + offset[i % self.dimensions]
            })
            .collect();
        
        Ok(decoded)
    }

    fn encode_batch(&self, vectors: &[Vec<f32>]) -> Result<Vec<Vec<u8>>, Error> {
        vectors.iter()
            .map(|v| {
                let encoded = self.encode(v)?;
                Ok(encoded.into_iter().map(|x| x as u8).collect())
            })
            .collect()
    }

    fn compression_ratio(&self) -> f32 {
        32.0 / self.bits as f32
    }
}

pub struct ProductQuantizer {
    dimensions: usize,
    num_subquantizers: usize,
    bits_per_subquantizer: usize,
    codebooks: Arc<RwLock<Vec<Vec<Vec<f32>>>>>,
    subvector_dims: usize,
}

impl ProductQuantizer {
    pub fn new(
        dimensions: usize,
        num_subquantizers: usize,
        bits_per_subquantizer: usize,
    ) -> Result<Self, Error> {
        if dimensions % num_subquantizers != 0 {
            return Err(Error::InvalidInput(format!(
                "Dimensions {} must be divisible by num_subquantizers {}",
                dimensions, num_subquantizers
            )));
        }
        
        let subvector_dims = dimensions / num_subquantizers;
        let num_centroids = 1 << bits_per_subquantizer;
        
        let codebooks = vec![vec![vec![0.0; subvector_dims]; num_centroids]; num_subquantizers];
        
        Ok(Self {
            dimensions,
            num_subquantizers,
            bits_per_subquantizer,
            codebooks: Arc::new(RwLock::new(codebooks)),
            subvector_dims,
        })
    }

    pub fn train(&self, vectors: &[Vec<f32>]) -> Result<(), Error> {
        if vectors.is_empty() {
            return Ok(());
        }
        
        let num_centroids = 1 << self.bits_per_subquantizer;
        let mut new_codebooks = Vec::new();
        
        for sq in 0..self.num_subquantizers {
            let start_idx = sq * self.subvector_dims;
            let end_idx = start_idx + self.subvector_dims;
            
            let subvectors: Vec<Vec<f32>> = vectors.iter()
                .map(|v| v[start_idx..end_idx].to_vec())
                .collect();
            
            let centroids = self.kmeans(subvectors, num_centroids)?;
            new_codebooks.push(centroids);
        }
        
        *self.codebooks.write() = new_codebooks;
        
        Ok(())
    }

    fn kmeans(&self, vectors: Vec<Vec<f32>>, k: usize) -> Result<Vec<Vec<f32>>, Error> {
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
        
        let max_iterations = 50;
        
        for _ in 0..max_iterations {
            let mut clusters: Vec<Vec<Vec<f32>>> = vec![Vec::new(); k];
            
            for vector in &vectors {
                let nearest = self.find_nearest_centroid(vector, &centroids);
                clusters[nearest].push(vector.clone());
            }
            
            for i in 0..k {
                if !clusters[i].is_empty() {
                    centroids[i] = super::similarity::compute_centroid(&clusters[i]);
                }
            }
        }
        
        Ok(centroids)
    }

    fn find_nearest_centroid(&self, vector: &[f32], centroids: &[Vec<f32>]) -> usize {
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
    }

    pub fn encode_vector(&self, vector: &[f32]) -> Result<Vec<u8>, Error> {
        if vector.len() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            )));
        }
        
        let codebooks = self.codebooks.read();
        let mut codes = Vec::with_capacity(self.num_subquantizers);
        
        for sq in 0..self.num_subquantizers {
            let start_idx = sq * self.subvector_dims;
            let end_idx = start_idx + self.subvector_dims;
            let subvector = &vector[start_idx..end_idx];
            
            let code = self.find_nearest_centroid(subvector, &codebooks[sq]);
            codes.push(code as u8);
        }
        
        Ok(codes)
    }

    pub fn decode_vector(&self, codes: &[u8]) -> Result<Vec<f32>, Error> {
        if codes.len() != self.num_subquantizers {
            return Err(Error::InvalidInput(format!(
                "Invalid code length: expected {}, got {}",
                self.num_subquantizers,
                codes.len()
            )));
        }
        
        let codebooks = self.codebooks.read();
        let mut vector = Vec::with_capacity(self.dimensions);
        
        for (sq, &code) in codes.iter().enumerate() {
            let centroid = &codebooks[sq][code as usize];
            vector.extend_from_slice(centroid);
        }
        
        Ok(vector)
    }
}

impl Quantizer for ProductQuantizer {
    fn encode(&self, vector: &[f32]) -> Result<Vec<f32>, Error> {
        let codes = self.encode_vector(vector)?;
        Ok(codes.into_iter().map(|c| c as f32).collect())
    }

    fn decode(&self, encoded: &[u8]) -> Result<Vec<f32>, Error> {
        self.decode_vector(encoded)
    }

    fn encode_batch(&self, vectors: &[Vec<f32>]) -> Result<Vec<Vec<u8>>, Error> {
        vectors.iter()
            .map(|v| self.encode_vector(v))
            .collect()
    }

    fn compression_ratio(&self) -> f32 {
        (self.dimensions * 32) as f32 / (self.num_subquantizers * self.bits_per_subquantizer) as f32
    }
}

pub struct BinaryQuantizer {
    dimensions: usize,
    thresholds: Arc<RwLock<Vec<f32>>>,
}

impl BinaryQuantizer {
    pub fn new(dimensions: usize) -> Result<Self, Error> {
        Ok(Self {
            dimensions,
            thresholds: Arc::new(RwLock::new(vec![0.0; dimensions])),
        })
    }

    pub fn train(&self, vectors: &[Vec<f32>]) -> Result<(), Error> {
        if vectors.is_empty() {
            return Ok(());
        }
        
        let mut thresholds = vec![0.0; self.dimensions];
        
        for dim in 0..self.dimensions {
            let mut values: Vec<f32> = vectors.iter()
                .map(|v| v[dim])
                .collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            
            let median_idx = values.len() / 2;
            thresholds[dim] = values[median_idx];
        }
        
        *self.thresholds.write() = thresholds;
        
        Ok(())
    }
}

impl Quantizer for BinaryQuantizer {
    fn encode(&self, vector: &[f32]) -> Result<Vec<f32>, Error> {
        if vector.len() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            )));
        }
        
        let thresholds = self.thresholds.read();
        let mut binary = Vec::with_capacity((self.dimensions + 7) / 8);
        
        for chunk in vector.chunks(8) {
            let mut byte = 0u8;
            for (i, &val) in chunk.iter().enumerate() {
                if val > thresholds[i] {
                    byte |= 1 << i;
                }
            }
            binary.push(byte as f32);
        }
        
        Ok(binary)
    }

    fn decode(&self, encoded: &[u8]) -> Result<Vec<f32>, Error> {
        let thresholds = self.thresholds.read();
        let mut vector = Vec::with_capacity(self.dimensions);
        
        for (byte_idx, &byte) in encoded.iter().enumerate() {
            for bit in 0..8 {
                let dim = byte_idx * 8 + bit;
                if dim >= self.dimensions {
                    break;
                }
                
                let val = if (byte >> bit) & 1 == 1 {
                    thresholds[dim] + 1.0
                } else {
                    thresholds[dim] - 1.0
                };
                vector.push(val);
            }
        }
        
        Ok(vector)
    }

    fn encode_batch(&self, vectors: &[Vec<f32>]) -> Result<Vec<Vec<u8>>, Error> {
        vectors.iter()
            .map(|v| {
                let encoded = self.encode(v)?;
                Ok(encoded.into_iter().map(|x| x as u8).collect())
            })
            .collect()
    }

    fn compression_ratio(&self) -> f32 {
        32.0
    }
}
