use serde::{Serialize, Deserialize};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SimilarityMetric {
    Euclidean,
    Cosine,
    DotProduct,
    Manhattan,
    Hamming,
    Jaccard,
}

pub trait DistanceFunction: Send + Sync {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32;
    fn similarity(&self, a: &[f32], b: &[f32]) -> f32;
}

pub fn create_distance_function(metric: SimilarityMetric) -> Arc<dyn DistanceFunction> {
    match metric {
        SimilarityMetric::Euclidean => Arc::new(EuclideanDistance),
        SimilarityMetric::Cosine => Arc::new(CosineDistance),
        SimilarityMetric::DotProduct => Arc::new(DotProductDistance),
        SimilarityMetric::Manhattan => Arc::new(ManhattanDistance),
        SimilarityMetric::Hamming => Arc::new(HammingDistance),
        SimilarityMetric::Jaccard => Arc::new(JaccardDistance),
    }
}

struct EuclideanDistance;

impl DistanceFunction for EuclideanDistance {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        euclidean_distance(a, b)
    }

    fn similarity(&self, a: &[f32], b: &[f32]) -> f32 {
        1.0 / (1.0 + self.distance(a, b))
    }
}

struct CosineDistance;

impl DistanceFunction for CosineDistance {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        cosine_distance(a, b)
    }

    fn similarity(&self, a: &[f32], b: &[f32]) -> f32 {
        1.0 - self.distance(a, b)
    }
}

struct DotProductDistance;

impl DistanceFunction for DotProductDistance {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        -dot_product(a, b)
    }

    fn similarity(&self, a: &[f32], b: &[f32]) -> f32 {
        dot_product(a, b)
    }
}

struct ManhattanDistance;

impl DistanceFunction for ManhattanDistance {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        manhattan_distance(a, b)
    }

    fn similarity(&self, a: &[f32], b: &[f32]) -> f32 {
        1.0 / (1.0 + self.distance(a, b))
    }
}

struct HammingDistance;

impl DistanceFunction for HammingDistance {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        hamming_distance(a, b)
    }

    fn similarity(&self, a: &[f32], b: &[f32]) -> f32 {
        1.0 - (self.distance(a, b) / a.len() as f32)
    }
}

struct JaccardDistance;

impl DistanceFunction for JaccardDistance {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        jaccard_distance(a, b)
    }

    fn similarity(&self, a: &[f32], b: &[f32]) -> f32 {
        1.0 - self.distance(a, b)
    }
}

#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { euclidean_distance_avx2(a, b) };
        }
    }
    
    euclidean_distance_scalar(a, b)
}

#[inline]
fn euclidean_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let diff = x - y;
            diff * diff
        })
        .sum::<f32>()
        .sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn euclidean_distance_avx2(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    
    let mut sum = _mm256_setzero_ps();
    let chunks = a.len() / 8;
    
    for i in 0..chunks {
        let a_vec = _mm256_loadu_ps(a.as_ptr().add(i * 8));
        let b_vec = _mm256_loadu_ps(b.as_ptr().add(i * 8));
        let diff = _mm256_sub_ps(a_vec, b_vec);
        let squared = _mm256_mul_ps(diff, diff);
        sum = _mm256_add_ps(sum, squared);
    }
    
    let mut result = [0f32; 8];
    _mm256_storeu_ps(result.as_mut_ptr(), sum);
    let mut total = result.iter().sum::<f32>();
    
    for i in (chunks * 8)..a.len() {
        let diff = a[i] - b[i];
        total += diff * diff;
    }
    
    total.sqrt()
}

#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product(a, b);
    let norm_a = norm(a);
    let norm_b = norm(b);
    
    if norm_a == 0.0 || norm_b == 0.0 {
        1.0
    } else {
        1.0 - (dot / (norm_a * norm_b))
    }
}

#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { dot_product_avx2(a, b) };
        }
    }
    
    dot_product_scalar(a, b)
}

#[inline]
fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn dot_product_avx2(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    
    let mut sum = _mm256_setzero_ps();
    let chunks = a.len() / 8;
    
    for i in 0..chunks {
        let a_vec = _mm256_loadu_ps(a.as_ptr().add(i * 8));
        let b_vec = _mm256_loadu_ps(b.as_ptr().add(i * 8));
        let product = _mm256_mul_ps(a_vec, b_vec);
        sum = _mm256_add_ps(sum, product);
    }
    
    let mut result = [0f32; 8];
    _mm256_storeu_ps(result.as_mut_ptr(), sum);
    let mut total = result.iter().sum::<f32>();
    
    for i in (chunks * 8)..a.len() {
        total += a[i] * b[i];
    }
    
    total
}

#[inline]
pub fn norm(a: &[f32]) -> f32 {
    a.iter().map(|x| x * x).sum::<f32>().sqrt()
}

#[inline]
pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).abs())
        .sum()
}

#[inline]
pub fn hamming_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .filter(|(x, y)| (x - y).abs() > 1e-6)
        .count() as f32
}

#[inline]
pub fn jaccard_distance(a: &[f32], b: &[f32]) -> f32 {
    let threshold = 0.5;
    let mut intersection = 0;
    let mut union = 0;
    
    for i in 0..a.len() {
        let a_bit = a[i] > threshold;
        let b_bit = b[i] > threshold;
        
        if a_bit && b_bit {
            intersection += 1;
        }
        if a_bit || b_bit {
            union += 1;
        }
    }
    
    if union == 0 {
        0.0
    } else {
        1.0 - (intersection as f32 / union as f32)
    }
}

pub fn normalize(vector: &mut [f32]) {
    let norm_val = norm(vector);
    if norm_val > 0.0 {
        for val in vector.iter_mut() {
            *val /= norm_val;
        }
    }
}

pub fn batch_normalize(vectors: &mut [Vec<f32>]) {
    for vector in vectors.iter_mut() {
        normalize(vector);
    }
}

pub fn compute_centroid(vectors: &[Vec<f32>]) -> Vec<f32> {
    if vectors.is_empty() {
        return Vec::new();
    }
    
    let dimensions = vectors[0].len();
    let mut centroid = vec![0.0; dimensions];
    
    for vector in vectors {
        for (i, val) in vector.iter().enumerate() {
            centroid[i] += val;
        }
    }
    
    let count = vectors.len() as f32;
    for val in centroid.iter_mut() {
        *val /= count;
    }
    
    centroid
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_distance() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let distance = euclidean_distance(&a, &b);
        assert!((distance - 5.196).abs() < 0.001);
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let distance = cosine_distance(&a, &b);
        assert!(distance < 0.05);
    }

    #[test]
    fn test_normalize() {
        let mut vector = vec![3.0, 4.0];
        normalize(&mut vector);
        assert!((vector[0] - 0.6).abs() < 0.001);
        assert!((vector[1] - 0.8).abs() < 0.001);
    }
}