use crate::compression::{CompressionType, Compressor, get_compressor};
use crate::error::Result;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use tracing::{debug, info};

/// Enhanced adaptive compression that learns from usage patterns
#[derive(Debug, Clone)]
pub struct AdaptiveCompressionConfig {
    /// Minimum data size to consider compression
    pub min_size: usize,
    /// Maximum time to spend on compression analysis
    pub max_analysis_time: Duration,
    /// Number of samples to keep for pattern learning
    pub sample_history_size: usize,
    /// Update statistics every N compressions
    pub stats_update_interval: usize,
    /// Enable machine learning-based prediction
    pub enable_ml_prediction: bool,
}

impl Default for AdaptiveCompressionConfig {
    fn default() -> Self {
        Self {
            min_size: 128,
            max_analysis_time: Duration::from_millis(10),
            sample_history_size: 1000,
            stats_update_interval: 100,
            enable_ml_prediction: false,
        }
    }
}

/// Statistics for a specific compression type
#[derive(Debug, Default)]
struct CompressionTypeStats {
    total_compressions: AtomicU64,
    total_original_bytes: AtomicU64,
    total_compressed_bytes: AtomicU64,
    total_compression_time_us: AtomicU64,
    total_decompression_time_us: AtomicU64,
    success_count: AtomicU64,
    failure_count: AtomicU64,
}

impl CompressionTypeStats {
    fn average_compression_ratio(&self) -> f64 {
        let original = self.total_original_bytes.load(Ordering::Relaxed) as f64;
        let compressed = self.total_compressed_bytes.load(Ordering::Relaxed) as f64;
        if original > 0.0 {
            1.0 - (compressed / original)
        } else {
            0.0
        }
    }
    
    fn average_compression_speed_mbps(&self) -> f64 {
        let bytes = self.total_original_bytes.load(Ordering::Relaxed) as f64;
        let time_us = self.total_compression_time_us.load(Ordering::Relaxed) as f64;
        if time_us > 0.0 {
            (bytes / 1_048_576.0) / (time_us / 1_000_000.0)
        } else {
            0.0
        }
    }
}

/// Pattern recognition for data types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataPattern {
    Json,
    Xml,
    Binary,
    Text,
    Numeric,
    TimeSeries,
    LogData,
    Unknown,
}

/// Data sample for pattern learning
#[derive(Debug, Clone)]
struct DataSample {
    pattern: DataPattern,
    entropy: f64,
    size: usize,
    best_compression: CompressionType,
    compression_ratio: f64,
    timestamp: Instant,
}

/// Advanced adaptive compression system
pub struct AdaptiveCompressionEngine {
    config: AdaptiveCompressionConfig,
    
    // Statistics per compression type
    stats: Arc<DashMap<CompressionType, CompressionTypeStats>>,
    
    // Pattern learning
    samples: Arc<RwLock<VecDeque<DataSample>>>,
    pattern_preferences: Arc<DashMap<DataPattern, CompressionType>>,
    
    // Counters
    compression_count: AtomicUsize,
    
    // Cached compressors
    compressors: DashMap<CompressionType, Arc<dyn Compressor>>,
}

impl AdaptiveCompressionEngine {
    pub fn new(config: AdaptiveCompressionConfig) -> Self {
        let stats = Arc::new(DashMap::new());
        for comp_type in &[
            CompressionType::None,
            CompressionType::Lz4,
            CompressionType::Snappy,
            CompressionType::Zstd,
        ] {
            stats.insert(*comp_type, CompressionTypeStats::default());
        }
        
        Self {
            config,
            stats,
            samples: Arc::new(RwLock::new(VecDeque::new())),
            pattern_preferences: Arc::new(DashMap::new()),
            compression_count: AtomicUsize::new(0),
            compressors: DashMap::new(),
        }
    }
    
    /// Compress data with adaptive algorithm selection
    pub fn compress(&self, data: &[u8]) -> Result<(Vec<u8>, CompressionType)> {
        // Skip compression for small data
        if data.len() < self.config.min_size {
            return Ok((data.to_vec(), CompressionType::None));
        }
        
        let start = Instant::now();
        
        // Analyze data pattern
        let pattern = self.detect_pattern(data);
        let entropy = self.calculate_entropy(data);
        
        // Choose compression type
        let compression_type = if self.config.enable_ml_prediction {
            self.predict_best_compression(data, &pattern, entropy)
        } else {
            self.heuristic_selection(data, &pattern, entropy)
        };
        
        // Get or create compressor
        let compressor = self.get_compressor(compression_type);
        
        // Compress
        let compress_start = Instant::now();
        let compressed = compressor.compress(data)?;
        let compress_time = compress_start.elapsed();
        
        // Update statistics
        self.update_stats(
            compression_type,
            data.len(),
            compressed.len(),
            compress_time,
            Duration::ZERO,
        );
        
        // Learn from this compression
        if self.compression_count.fetch_add(1, Ordering::Relaxed) % self.config.stats_update_interval == 0 {
            self.learn_from_sample(DataSample {
                pattern: pattern.clone(),
                entropy,
                size: data.len(),
                best_compression: compression_type,
                compression_ratio: 1.0 - (compressed.len() as f64 / data.len() as f64),
                timestamp: start,
            });
        }
        
        debug!(
            "Compressed {} bytes to {} bytes using {:?} (pattern: {:?}, entropy: {:.2})",
            data.len(), compressed.len(), compression_type, pattern, entropy
        );
        
        Ok((compressed, compression_type))
    }
    
    /// Decompress data
    pub fn decompress(&self, data: &[u8], compression_type: CompressionType) -> Result<Vec<u8>> {
        let compressor = self.get_compressor(compression_type);
        
        let decompress_start = Instant::now();
        let decompressed = compressor.decompress(data)?;
        let decompress_time = decompress_start.elapsed();
        
        // Update decompression stats
        if let Some(stats) = self.stats.get(&compression_type) {
            stats.total_decompression_time_us.fetch_add(
                decompress_time.as_micros() as u64,
                Ordering::Relaxed
            );
        }
        
        Ok(decompressed)
    }
    
    /// Detect data pattern using heuristics
    fn detect_pattern(&self, data: &[u8]) -> DataPattern {
        let sample_size = data.len().min(1024);
        let sample = &data[..sample_size];
        
        // Check for JSON
        if self.looks_like_json(sample) {
            return DataPattern::Json;
        }
        
        // Check for XML
        if self.looks_like_xml(sample) {
            return DataPattern::Xml;
        }
        
        // Check for log data
        if self.looks_like_logs(sample) {
            return DataPattern::LogData;
        }
        
        // Check for numeric/time series
        if self.looks_like_numeric(sample) {
            if self.has_timestamp_pattern(sample) {
                return DataPattern::TimeSeries;
            }
            return DataPattern::Numeric;
        }
        
        // Check for text
        if self.is_mostly_text(sample) {
            return DataPattern::Text;
        }
        
        // Check for binary
        if self.has_binary_structure(sample) {
            return DataPattern::Binary;
        }
        
        DataPattern::Unknown
    }
    
    /// Calculate Shannon entropy
    fn calculate_entropy(&self, data: &[u8]) -> f64 {
        let mut byte_counts = [0u64; 256];
        for &byte in data {
            byte_counts[byte as usize] += 1;
        }
        
        let total = data.len() as f64;
        let mut entropy = 0.0;
        
        for &count in &byte_counts {
            if count > 0 {
                let p = count as f64 / total;
                entropy -= p * p.log2();
            }
        }
        
        entropy
    }
    
    /// Heuristic-based compression selection
    fn heuristic_selection(&self, data: &[u8], pattern: &DataPattern, entropy: f64) -> CompressionType {
        // Check pattern preferences first
        if let Some(pref) = self.pattern_preferences.get(pattern) {
            return *pref;
        }
        
        // Pattern-specific rules
        match pattern {
            DataPattern::Json | DataPattern::Xml => {
                // Structured text compresses well with Zstd
                if data.len() > 10240 {
                    CompressionType::Zstd
                } else {
                    CompressionType::Snappy
                }
            }
            DataPattern::LogData => {
                // Logs have repetitive patterns
                CompressionType::Zstd
            }
            DataPattern::TimeSeries | DataPattern::Numeric => {
                // Numeric data with patterns
                if entropy < 4.0 {
                    CompressionType::Zstd
                } else {
                    CompressionType::Lz4
                }
            }
            DataPattern::Text => {
                // General text
                if data.len() > 4096 {
                    CompressionType::Snappy
                } else {
                    CompressionType::Lz4
                }
            }
            DataPattern::Binary => {
                // Binary data - check entropy
                if entropy < 5.0 {
                    CompressionType::Lz4
                } else {
                    CompressionType::None
                }
            }
            DataPattern::Unknown => {
                // Fallback to entropy-based selection
                if entropy < 3.0 {
                    CompressionType::Zstd
                } else if entropy < 6.0 {
                    CompressionType::Lz4
                } else {
                    CompressionType::None
                }
            }
        }
    }
    
    /// ML-based compression prediction (placeholder for future enhancement)
    fn predict_best_compression(&self, data: &[u8], pattern: &DataPattern, entropy: f64) -> CompressionType {
        // For now, use enhanced heuristics
        // In future, this could use a trained model based on collected samples
        
        // Analyze recent samples with similar characteristics
        let samples = self.samples.read();
        let similar_samples: Vec<_> = samples.iter()
            .filter(|s| {
                s.pattern == *pattern &&
                (s.entropy - entropy).abs() < 1.0 &&
                (s.size as f64 / data.len() as f64).abs() < 2.0
            })
            .collect();
        
        if similar_samples.len() >= 5 {
            // Find most successful compression type
            use std::collections::HashMap;
            let mut type_scores: HashMap<CompressionType, f64> = HashMap::new();
            
            for sample in similar_samples {
                let score = type_scores.entry(sample.best_compression).or_insert(0.0);
                *score += sample.compression_ratio;
            }
            
            type_scores.into_iter()
                .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
                .map(|(comp_type, _)| comp_type)
                .unwrap_or_else(|| self.heuristic_selection(data, pattern, entropy))
        } else {
            self.heuristic_selection(data, pattern, entropy)
        }
    }
    
    /// Pattern detection helpers
    fn looks_like_json(&self, data: &[u8]) -> bool {
        let text = String::from_utf8_lossy(data);
        let trimmed = text.trim();
        (trimmed.starts_with('{') && trimmed.contains('"')) ||
        (trimmed.starts_with('[') && trimmed.contains('"'))
    }
    
    fn looks_like_xml(&self, data: &[u8]) -> bool {
        let text = String::from_utf8_lossy(data);
        text.contains('<') && text.contains('>') && 
        (text.contains("</") || text.contains("/>"))
    }
    
    fn looks_like_logs(&self, data: &[u8]) -> bool {
        let text = String::from_utf8_lossy(data);
        // Common log patterns
        text.contains("INFO") || text.contains("ERROR") || 
        text.contains("WARN") || text.contains("DEBUG") ||
        text.contains("] ") || text.contains(" - ")
    }
    
    fn looks_like_numeric(&self, data: &[u8]) -> bool {
        let mut numeric_count = 0;
        let sample_size = data.len().min(256);
        
        for &byte in &data[..sample_size] {
            if byte.is_ascii_digit() || byte == b'.' || byte == b'-' || byte == b'+' {
                numeric_count += 1;
            }
        }
        
        numeric_count > sample_size / 3
    }
    
    fn has_timestamp_pattern(&self, data: &[u8]) -> bool {
        let text = String::from_utf8_lossy(data);
        // Simple timestamp patterns
        text.contains("20") && (text.contains(':') || text.contains('-'))
    }
    
    fn is_mostly_text(&self, data: &[u8]) -> bool {
        let mut text_chars = 0;
        let sample_size = data.len().min(512);
        
        for &byte in &data[..sample_size] {
            if byte.is_ascii_graphic() || byte.is_ascii_whitespace() {
                text_chars += 1;
            }
        }
        
        text_chars > sample_size * 3 / 4
    }
    
    fn has_binary_structure(&self, data: &[u8]) -> bool {
        // Check for common binary patterns (magic numbers, etc.)
        if data.len() >= 4 {
            let first_4 = &data[..4];
            // Check for common binary formats
            matches!(first_4, 
                [0x89, 0x50, 0x4E, 0x47] | // PNG
                [0xFF, 0xD8, 0xFF, _] |     // JPEG
                [0x50, 0x4B, 0x03, 0x04]    // ZIP
            )
        } else {
            false
        }
    }
    
    /// Update compression statistics
    fn update_stats(
        &self,
        compression_type: CompressionType,
        original_size: usize,
        compressed_size: usize,
        compress_time: Duration,
        decompress_time: Duration,
    ) {
        if let Some(stats) = self.stats.get(&compression_type) {
            stats.total_compressions.fetch_add(1, Ordering::Relaxed);
            stats.total_original_bytes.fetch_add(original_size as u64, Ordering::Relaxed);
            stats.total_compressed_bytes.fetch_add(compressed_size as u64, Ordering::Relaxed);
            stats.total_compression_time_us.fetch_add(compress_time.as_micros() as u64, Ordering::Relaxed);
            stats.total_decompression_time_us.fetch_add(decompress_time.as_micros() as u64, Ordering::Relaxed);
            stats.success_count.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    /// Learn from compression sample
    fn learn_from_sample(&self, sample: DataSample) {
        // Add to sample history
        {
            let mut samples = self.samples.write();
            samples.push_back(sample.clone());
            
            // Maintain size limit
            while samples.len() > self.config.sample_history_size {
                samples.pop_front();
            }
        }
        
        // Update pattern preferences if this was particularly good
        if sample.compression_ratio > 0.3 {
            self.pattern_preferences.insert(sample.pattern, sample.best_compression);
        }
    }
    
    /// Get or create compressor
    fn get_compressor(&self, compression_type: CompressionType) -> Arc<dyn Compressor> {
        self.compressors.entry(compression_type)
            .or_insert_with(|| get_compressor(compression_type))
            .clone()
    }
    
    /// Get compression statistics
    pub fn get_stats(&self) -> Vec<(CompressionType, CompressionTypeStats)> {
        self.stats.iter()
            .map(|entry| {
                let comp_type = *entry.key();
                let stats = CompressionTypeStats {
                    total_compressions: AtomicU64::new(entry.value().total_compressions.load(Ordering::Relaxed)),
                    total_original_bytes: AtomicU64::new(entry.value().total_original_bytes.load(Ordering::Relaxed)),
                    total_compressed_bytes: AtomicU64::new(entry.value().total_compressed_bytes.load(Ordering::Relaxed)),
                    total_compression_time_us: AtomicU64::new(entry.value().total_compression_time_us.load(Ordering::Relaxed)),
                    total_decompression_time_us: AtomicU64::new(entry.value().total_decompression_time_us.load(Ordering::Relaxed)),
                    success_count: AtomicU64::new(entry.value().success_count.load(Ordering::Relaxed)),
                    failure_count: AtomicU64::new(entry.value().failure_count.load(Ordering::Relaxed)),
                };
                (comp_type, stats)
            })
            .collect()
    }
    
    /// Print compression statistics summary
    pub fn print_stats_summary(&self) {
        info!("Adaptive Compression Statistics:");
        for (comp_type, stats) in self.get_stats() {
            let total = stats.total_compressions.load(Ordering::Relaxed);
            if total > 0 {
                info!(
                    "  {:?}: {} compressions, {:.1}% avg ratio, {:.1} MB/s",
                    comp_type,
                    total,
                    stats.average_compression_ratio() * 100.0,
                    stats.average_compression_speed_mbps()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_pattern_detection() {
        let engine = AdaptiveCompressionEngine::new(AdaptiveCompressionConfig::default());
        
        // Test JSON detection
        let json_data = br#"{"name": "test", "value": 123}"#;
        assert_eq!(engine.detect_pattern(json_data), DataPattern::Json);
        
        // Test XML detection
        let xml_data = br#"<root><item>test</item></root>"#;
        assert_eq!(engine.detect_pattern(xml_data), DataPattern::Xml);
        
        // Test log detection
        let log_data = b"2024-01-01 12:00:00 INFO Starting application";
        assert_eq!(engine.detect_pattern(log_data), DataPattern::LogData);
    }
    
    #[test]
    fn test_adaptive_compression() {
        let engine = AdaptiveCompressionEngine::new(AdaptiveCompressionConfig::default());
        
        // Test with different data types
        let test_cases = vec![
            (b"Small text".to_vec(), CompressionType::None), // Too small
            (br#"{"key": "value"}"#.repeat(100), CompressionType::Zstd), // JSON
            (b"AAAAAAAAAA".repeat(100), CompressionType::Zstd), // Repetitive
        ];
        
        for (data, _expected) in test_cases {
            let result = engine.compress(&data);
            assert!(result.is_ok());
            
            let (compressed, comp_type) = result.unwrap();
            let decompressed = engine.decompress(&compressed, comp_type).unwrap();
            assert_eq!(decompressed, data);
        }
    }
}