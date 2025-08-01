//! Adaptive Compression with Hardware Acceleration
//!
//! This module provides intelligent compression that automatically selects the best 
//! compression algorithm based on data characteristics and available hardware capabilities.
//! It supports multiple compression algorithms with hardware acceleration when available.

use crate::{Result, Error};
use std::sync::Arc;
use std::time::{Instant, Duration};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

pub mod algorithms;
pub mod hardware_detection;
pub mod adaptive_selector;
pub mod benchmark;
pub mod streaming;

pub use algorithms::*;
pub use hardware_detection::*;
pub use adaptive_selector::*;
pub use benchmark::*;
pub use streaming::*;

// Import algorithm implementations
use algorithms::{
    NoCompression, LZ4Compression, ZstdCompression, SnappyCompression,
    HardwareAcceleratedCompression,
};

/// Compression level settings
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompressionLevel {
    /// Fastest compression, lowest compression ratio
    Fastest = 1,
    /// Fast compression, good for real-time scenarios  
    Fast = 3,
    /// Balanced compression and speed
    Balanced = 6,
    /// Higher compression, slower speed
    High = 9,
    /// Maximum compression, slowest speed
    Maximum = 12,
}

impl CompressionLevel {
    /// Get compression level as integer
    pub fn as_int(self) -> i32 {
        self as i32
    }
    
    /// Get compression level from integer
    pub fn from_int(level: i32) -> Self {
        match level {
            1 => CompressionLevel::Fastest,
            2..=3 => CompressionLevel::Fast,
            4..=6 => CompressionLevel::Balanced,
            7..=9 => CompressionLevel::High,
            _ => CompressionLevel::Maximum,
        }
    }
}

/// Compression algorithm types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 compression - fast compression/decompression
    LZ4,
    /// Zstandard compression - good compression ratio and speed
    Zstd,
    /// Snappy compression - very fast compression
    Snappy,
    /// LZMA compression - high compression ratio, slow
    LZMA,
    /// Brotli compression - good compression ratio for text
    Brotli,
    /// Custom hardware-accelerated compression
    HardwareAccelerated,
}

impl CompressionAlgorithm {
    /// Get algorithm name
    pub fn name(&self) -> &'static str {
        match self {
            CompressionAlgorithm::None => "none",
            CompressionAlgorithm::LZ4 => "lz4",
            CompressionAlgorithm::Zstd => "zstd",
            CompressionAlgorithm::Snappy => "snappy",
            CompressionAlgorithm::LZMA => "lzma",
            CompressionAlgorithm::Brotli => "brotli",
            CompressionAlgorithm::HardwareAccelerated => "hardware",
        }
    }
    
    /// Check if algorithm is hardware accelerated
    pub fn is_hardware_accelerated(&self) -> bool {
        matches!(self, CompressionAlgorithm::HardwareAccelerated)
    }
    
    /// Get recommended level for algorithm
    pub fn recommended_level(&self) -> CompressionLevel {
        match self {
            CompressionAlgorithm::None => CompressionLevel::Fastest,
            CompressionAlgorithm::LZ4 => CompressionLevel::Fast,
            CompressionAlgorithm::Zstd => CompressionLevel::Balanced,
            CompressionAlgorithm::Snappy => CompressionLevel::Fast,
            CompressionAlgorithm::LZMA => CompressionLevel::High,
            CompressionAlgorithm::Brotli => CompressionLevel::High,
            CompressionAlgorithm::HardwareAccelerated => CompressionLevel::Balanced,
        }
    }
}

/// Data type characteristics for compression optimization
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// Binary data with low entropy
    Binary,
    /// Text data (UTF-8, ASCII)
    Text,
    /// JSON structured data
    Json,
    /// Numeric data (integers, floats)
    Numeric,
    /// Already compressed data
    Compressed,
    /// Mixed/unknown data type
    Mixed,
}

/// Compression result with metrics
#[derive(Debug, Clone)]
pub struct CompressionResult {
    /// Compressed data
    pub data: Vec<u8>,
    /// Original size in bytes
    pub original_size: usize,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// Compression ratio (compressed_size / original_size)
    pub compression_ratio: f64,
    /// Time taken to compress
    pub compression_time: Duration,
    /// Algorithm used
    pub algorithm: CompressionAlgorithm,
    /// Level used
    pub level: CompressionLevel,
}

impl CompressionResult {
    /// Calculate compression efficiency (ratio / time_ms)
    pub fn efficiency(&self) -> f64 {
        let time_ms = self.compression_time.as_secs_f64() * 1000.0;
        if time_ms > 0.0 {
            (1.0 - self.compression_ratio) / time_ms
        } else {
            0.0
        }
    }
    
    /// Calculate space savings in bytes
    pub fn space_saved(&self) -> usize {
        self.original_size.saturating_sub(self.compressed_size)
    }
    
    /// Calculate space savings as percentage
    pub fn space_saved_percent(&self) -> f64 {
        if self.original_size > 0 {
            (self.space_saved() as f64 / self.original_size as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Decompression result with metrics
#[derive(Debug, Clone)]
pub struct DecompressionResult {
    /// Decompressed data
    pub data: Vec<u8>,
    /// Decompressed size in bytes
    pub size: usize,
    /// Time taken to decompress
    pub decompression_time: Duration,
    /// Algorithm used
    pub algorithm: CompressionAlgorithm,
}

/// Compression statistics for adaptive selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStats {
    /// Algorithm performance history
    pub algorithm_stats: HashMap<CompressionAlgorithm, AlgorithmStats>,
    /// Data type statistics
    pub data_type_stats: HashMap<DataType, DataTypeStats>,
    /// Hardware capabilities
    pub hardware_capabilities: HardwareCapabilities,
    /// Total compressions performed
    pub total_compressions: u64,
    /// Total bytes compressed
    pub total_bytes_compressed: u64,
    /// Total time spent compressing
    pub total_compression_time: Duration,
}

/// Performance statistics for a specific algorithm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmStats {
    /// Number of times used
    pub usage_count: u64,
    /// Average compression ratio
    pub avg_compression_ratio: f64,
    /// Average compression time (ms)
    pub avg_compression_time_ms: f64,
    /// Average decompression time (ms)  
    pub avg_decompression_time_ms: f64,
    /// Average efficiency score
    pub avg_efficiency: f64,
    /// Success rate (0.0 to 1.0)
    pub success_rate: f64,
    /// Last updated timestamp
    pub last_updated: u64,
}

/// Statistics for a specific data type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataTypeStats {
    /// Sample count
    pub sample_count: u64,
    /// Average entropy (0.0 to 8.0 bits per byte)
    pub avg_entropy: f64,
    /// Best performing algorithm
    pub best_algorithm: CompressionAlgorithm,
    /// Average compression ratio for best algorithm
    pub best_compression_ratio: f64,
    /// Compression effectiveness by algorithm
    pub algorithm_effectiveness: HashMap<CompressionAlgorithm, f64>,
}

impl Default for CompressionStats {
    fn default() -> Self {
        Self {
            algorithm_stats: HashMap::new(),
            data_type_stats: HashMap::new(),
            hardware_capabilities: HardwareCapabilities::detect(),
            total_compressions: 0,
            total_bytes_compressed: 0,
            total_compression_time: Duration::from_secs(0),
        }
    }
}

/// Main adaptive compression engine
pub struct AdaptiveCompressionEngine {
    /// Available algorithms
    algorithms: HashMap<CompressionAlgorithm, Box<dyn CompressionAlgorithmTrait + Send + Sync>>,
    /// Performance statistics
    stats: Arc<parking_lot::RwLock<CompressionStats>>,
    /// Adaptive selector
    selector: AdaptiveSelector,
    /// Hardware capabilities
    hardware: HardwareCapabilities,
    /// Benchmark suite
    benchmark: CompressionBenchmark,
}

impl AdaptiveCompressionEngine {
    /// Create a new adaptive compression engine
    pub fn new() -> Result<Self> {
        let hardware = HardwareCapabilities::detect();
        let mut algorithms: HashMap<CompressionAlgorithm, Box<dyn CompressionAlgorithmTrait + Send + Sync>> = HashMap::new();
        
        // Register available algorithms
        algorithms.insert(CompressionAlgorithm::None, Box::new(NoCompression::new()));
        algorithms.insert(CompressionAlgorithm::LZ4, Box::new(LZ4Compression::new()));
        algorithms.insert(CompressionAlgorithm::Zstd, Box::new(ZstdCompression::new()));
        algorithms.insert(CompressionAlgorithm::Snappy, Box::new(SnappyCompression::new()));
        
        // Add hardware-accelerated algorithms if available
        if hardware.has_compression_acceleration() {
            algorithms.insert(
                CompressionAlgorithm::HardwareAccelerated,
                Box::new(HardwareAcceleratedCompression::new(&hardware)?)
            );
        }
        
        // Add LZMA if available
        #[cfg(feature = "lzma")]
        algorithms.insert(CompressionAlgorithm::LZMA, Box::new(LZMACompression::new()));
        
        // Add Brotli if available
        #[cfg(feature = "brotli")]
        algorithms.insert(CompressionAlgorithm::Brotli, Box::new(BrotliCompression::new()));
        
        let stats = Arc::new(parking_lot::RwLock::new(CompressionStats::default()));
        let selector = AdaptiveSelector::new(Arc::clone(&stats));
        let benchmark = CompressionBenchmark::new();
        
        Ok(Self {
            algorithms,
            stats,
            selector,
            hardware,
            benchmark,
        })
    }
    
    /// Compress data with automatic algorithm selection
    pub fn compress(&self, data: &[u8]) -> Result<CompressionResult> {
        if data.is_empty() {
            return Ok(CompressionResult {
                data: Vec::new(),
                original_size: 0,
                compressed_size: 0,
                compression_ratio: 1.0,
                compression_time: Duration::from_nanos(0),
                algorithm: CompressionAlgorithm::None,
                level: CompressionLevel::Fastest,
            });
        }
        
        // Analyze data characteristics
        let data_type = self.analyze_data_type(data);
        let entropy = self.calculate_entropy(data);
        
        // Select best algorithm and level
        let (algorithm, level) = self.selector.select_algorithm(data, &data_type, entropy);
        
        // Perform compression
        let result = self.compress_with_algorithm(data, algorithm, level)?;
        
        // Update statistics
        self.update_stats(&result, &data_type);
        
        Ok(result)
    }
    
    /// Compress data with specific algorithm and level
    pub fn compress_with_algorithm(
        &self,
        data: &[u8],
        algorithm: CompressionAlgorithm,
        level: CompressionLevel,
    ) -> Result<CompressionResult> {
        let compressor = self.algorithms.get(&algorithm)
            .ok_or_else(|| Error::Generic(format!("Algorithm {:?} not available", algorithm)))?;
        
        let start = Instant::now();
        let compressed_data = compressor.compress(data, level)?;
        let compression_time = start.elapsed();
        
        let original_size = data.len();
        let compressed_size = compressed_data.len();
        let compression_ratio = if original_size > 0 {
            compressed_size as f64 / original_size as f64
        } else {
            1.0
        };
        
        Ok(CompressionResult {
            data: compressed_data,
            original_size,
            compressed_size,
            compression_ratio,
            compression_time,
            algorithm,
            level,
        })
    }
    
    /// Decompress data
    pub fn decompress(
        &self,
        compressed_data: &[u8],
        algorithm: CompressionAlgorithm,
    ) -> Result<DecompressionResult> {
        if compressed_data.is_empty() {
            return Ok(DecompressionResult {
                data: Vec::new(),
                size: 0,
                decompression_time: Duration::from_nanos(0),
                algorithm,
            });
        }
        
        let compressor = self.algorithms.get(&algorithm)
            .ok_or_else(|| Error::Generic(format!("Algorithm {:?} not available", algorithm)))?;
        
        let start = Instant::now();
        let decompressed_data = compressor.decompress(compressed_data)?;
        let decompression_time = start.elapsed();
        
        Ok(DecompressionResult {
            size: decompressed_data.len(),
            data: decompressed_data,
            decompression_time,
            algorithm,
        })
    }
    
    /// Benchmark all available algorithms
    pub fn benchmark_algorithms(&self, test_data: &[u8]) -> Result<BenchmarkResults> {
        self.benchmark.run_comprehensive_benchmark(test_data, &self.algorithms)
    }
    
    /// Get compression statistics
    pub fn get_stats(&self) -> CompressionStats {
        self.stats.read().clone()
    }
    
    /// Reset statistics
    pub fn reset_stats(&self) {
        let mut stats = self.stats.write();
        *stats = CompressionStats::default();
    }
    
    /// Get available algorithms
    pub fn available_algorithms(&self) -> Vec<CompressionAlgorithm> {
        self.algorithms.keys().copied().collect()
    }
    
    /// Get hardware capabilities
    pub fn hardware_capabilities(&self) -> &HardwareCapabilities {
        &self.hardware
    }
    
    /// Analyze data type from content
    fn analyze_data_type(&self, data: &[u8]) -> DataType {
        if data.is_empty() {
            return DataType::Mixed;
        }
        
        // Check if it's already compressed (high entropy)
        let entropy = self.calculate_entropy(data);
        if entropy > 7.5 {
            return DataType::Compressed;
        }
        
        // Check for text patterns
        if self.is_text_data(data) {
            if self.is_json_data(data) {
                return DataType::Json;
            }
            return DataType::Text;
        }
        
        // Check for numeric patterns
        if self.is_numeric_data(data) {
            return DataType::Numeric;
        }
        
        // Default to binary
        DataType::Binary
    }
    
    /// Calculate entropy of data (bits per byte)
    fn calculate_entropy(&self, data: &[u8]) -> f64 {
        if data.is_empty() {
            return 0.0;
        }
        
        let mut counts = [0u32; 256];
        for &byte in data {
            counts[byte as usize] += 1;
        }
        
        let len = data.len() as f64;
        let mut entropy = 0.0;
        
        for count in counts.iter() {
            if *count > 0 {
                let p = *count as f64 / len;
                entropy -= p * p.log2();
            }
        }
        
        entropy
    }
    
    /// Check if data is text
    fn is_text_data(&self, data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }
        
        // Check for valid UTF-8 and printable characters
        if let Ok(text) = std::str::from_utf8(data) {
            let printable_count = text.chars()
                .filter(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
                .count();
            printable_count as f64 / text.chars().count() as f64 > 0.8
        } else {
            false
        }
    }
    
    /// Check if data is JSON
    fn is_json_data(&self, data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }
        
        // Simple heuristic: starts with { or [ and has common JSON characters
        let first_char = data[0];
        if first_char == b'{' || first_char == b'[' {
            let json_chars = data.iter()
                .filter(|&&b| matches!(b, b'{' | b'}' | b'[' | b']' | b':' | b',' | b'"'))
                .count();
            json_chars as f64 / data.len() as f64 > 0.1
        } else {
            false
        }
    }
    
    /// Check if data is numeric
    fn is_numeric_data(&self, data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }
        
        // Check for patterns that suggest numeric data
        let numeric_chars = data.iter()
            .filter(|&&b| matches!(b, b'0'..=b'9' | b'.' | b'-' | b'+' | b'e' | b'E'))
            .count();
        numeric_chars as f64 / data.len() as f64 > 0.6
    }
    
    /// Update compression statistics
    fn update_stats(&self, result: &CompressionResult, data_type: &DataType) {
        let mut stats = self.stats.write();
        
        // Update global stats
        stats.total_compressions += 1;
        stats.total_bytes_compressed += result.original_size as u64;
        stats.total_compression_time += result.compression_time;
        
        // Update algorithm stats
        let algo_stats = stats.algorithm_stats.entry(result.algorithm).or_insert_with(|| {
            AlgorithmStats {
                usage_count: 0,
                avg_compression_ratio: 0.0,
                avg_compression_time_ms: 0.0,
                avg_decompression_time_ms: 0.0,
                avg_efficiency: 0.0,
                success_rate: 1.0,
                last_updated: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        });
        
        // Update algorithm statistics using exponential moving average
        let alpha = 0.1; // Smoothing factor
        algo_stats.usage_count += 1;
        algo_stats.avg_compression_ratio = 
            alpha * result.compression_ratio + (1.0 - alpha) * algo_stats.avg_compression_ratio;
        algo_stats.avg_compression_time_ms = 
            alpha * result.compression_time.as_secs_f64() * 1000.0 + 
            (1.0 - alpha) * algo_stats.avg_compression_time_ms;
        algo_stats.avg_efficiency = 
            alpha * result.efficiency() + (1.0 - alpha) * algo_stats.avg_efficiency;
        
        // Update data type stats
        let dtype_stats = stats.data_type_stats.entry(data_type.clone()).or_insert_with(|| {
            DataTypeStats {
                sample_count: 0,
                avg_entropy: 0.0,
                best_algorithm: result.algorithm,
                best_compression_ratio: result.compression_ratio,
                algorithm_effectiveness: HashMap::new(),
            }
        });
        
        dtype_stats.sample_count += 1;
        let effectiveness = result.efficiency();
        let current_effectiveness = dtype_stats.algorithm_effectiveness
            .entry(result.algorithm)
            .or_insert(effectiveness);
        *current_effectiveness = alpha * effectiveness + (1.0 - alpha) * *current_effectiveness;
        
        // Update best algorithm if this one is better
        if result.efficiency() > dtype_stats.algorithm_effectiveness
            .get(&dtype_stats.best_algorithm)
            .copied()
            .unwrap_or(0.0) {
            dtype_stats.best_algorithm = result.algorithm;
            dtype_stats.best_compression_ratio = result.compression_ratio;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_compression_levels() {
        assert_eq!(CompressionLevel::Fastest.as_int(), 1);
        assert_eq!(CompressionLevel::Maximum.as_int(), 12);
        assert_eq!(CompressionLevel::from_int(5), CompressionLevel::Balanced);
    }
    
    #[test]
    fn test_algorithm_names() {
        assert_eq!(CompressionAlgorithm::LZ4.name(), "lz4");
        assert_eq!(CompressionAlgorithm::Zstd.name(), "zstd");
        assert!(CompressionAlgorithm::HardwareAccelerated.is_hardware_accelerated());
    }
    
    #[test]
    fn test_compression_result_metrics() {
        let result = CompressionResult {
            data: vec![1, 2, 3],
            original_size: 100,
            compressed_size: 50,
            compression_ratio: 0.5,
            compression_time: Duration::from_millis(10),
            algorithm: CompressionAlgorithm::LZ4,
            level: CompressionLevel::Balanced,
        };
        
        assert_eq!(result.space_saved(), 50);
        assert_eq!(result.space_saved_percent(), 50.0);
        assert!(result.efficiency() > 0.0);
    }
}