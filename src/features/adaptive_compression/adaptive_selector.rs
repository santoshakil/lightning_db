//! Adaptive Algorithm Selection
//!
//! Intelligent selection of compression algorithms based on data characteristics,
//! hardware capabilities, and performance history.

use super::{
    AlgorithmStats, CompressionAlgorithm, CompressionLevel, CompressionStats, DataType,
    DataTypeStats, HardwareCapabilities,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Algorithm selection strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectionStrategy {
    /// Optimize for maximum compression ratio
    MaxCompression,
    /// Optimize for maximum speed
    MaxSpeed,
    /// Balance compression ratio and speed
    Balanced,
    /// Optimize for minimum memory usage
    MinMemory,
    /// Optimize for minimum CPU usage
    MinCpu,
    /// Adaptive based on system state
    Adaptive,
}

/// Selection context for algorithm choice
#[derive(Debug, Clone)]
pub struct SelectionContext {
    /// Data characteristics
    pub data_type: DataType,
    /// Data size in bytes
    pub data_size: usize,
    /// Data entropy (0.0 to 8.0)
    pub entropy: f64,
    /// Current system load (0.0 to 1.0)
    pub system_load: f64,
    /// Available memory in MB
    pub available_memory_mb: u64,
    /// Storage type and speed
    pub storage_speed: StorageSpeed,
    /// Time constraint (optional)
    pub time_constraint_ms: Option<u64>,
    /// Quality constraint (minimum compression ratio)
    pub min_compression_ratio: Option<f64>,
}

/// Storage speed characteristics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageSpeed {
    /// Very fast storage (NVMe, high-end SSD)
    VeryFast,
    /// Fast storage (SSD)
    Fast,
    /// Medium storage (SATA SSD, fast HDD)
    Medium,
    /// Slow storage (HDD, network)
    Slow,
}

/// Algorithm selection result
#[derive(Debug, Clone)]
pub struct SelectionResult {
    /// Selected algorithm
    pub algorithm: CompressionAlgorithm,
    /// Selected compression level
    pub level: CompressionLevel,
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
    /// Estimated compression ratio
    pub estimated_ratio: f64,
    /// Estimated compression time (microseconds)
    pub estimated_time_us: f64,
    /// Selection reasoning
    pub reasoning: String,
}

/// Adaptive algorithm selector
pub struct AdaptiveSelector {
    /// Hardware capabilities
    hardware: HardwareCapabilities,
    /// Performance statistics
    stats: Arc<RwLock<CompressionStats>>,
    /// Selection strategy
    strategy: SelectionStrategy,
    /// Algorithm weights and preferences
    weights: AlgorithmWeights,
    /// Learning parameters
    learning: LearningParameters,
}

/// Algorithm performance weights
#[derive(Debug, Clone)]
struct AlgorithmWeights {
    /// Weight for compression ratio (higher is better)
    compression_ratio: f64,
    /// Weight for compression speed (higher is better)
    compression_speed: f64,
    /// Weight for decompression speed (higher is better)
    decompression_speed: f64,
    /// Weight for memory usage (lower is better)
    memory_usage: f64,
    /// Weight for CPU usage (lower is better)
    cpu_usage: f64,
    /// Weight for historical performance (higher is better)
    historical_performance: f64,
}

/// Learning and adaptation parameters
#[derive(Debug, Clone)]
struct LearningParameters {
    /// Learning rate for algorithm performance updates
    learning_rate: f64,
    /// Exploration vs exploitation balance (0.0 = exploit, 1.0 = explore)
    exploration_rate: f64,
    /// Minimum samples required for reliable statistics
    min_samples: u64,
    /// Confidence threshold for algorithm selection
    confidence_threshold: f64,
}

impl AdaptiveSelector {
    /// Create a new adaptive selector
    pub fn new(stats: Arc<RwLock<CompressionStats>>) -> Self {
        let hardware = HardwareCapabilities::detect();

        Self {
            hardware,
            stats,
            strategy: SelectionStrategy::Adaptive,
            weights: AlgorithmWeights::balanced(),
            learning: LearningParameters::default(),
        }
    }

    /// Create selector with specific strategy
    pub fn with_strategy(
        stats: Arc<RwLock<CompressionStats>>,
        strategy: SelectionStrategy,
    ) -> Self {
        let hardware = HardwareCapabilities::detect();
        let weights = match strategy {
            SelectionStrategy::MaxCompression => AlgorithmWeights::max_compression(),
            SelectionStrategy::MaxSpeed => AlgorithmWeights::max_speed(),
            SelectionStrategy::Balanced => AlgorithmWeights::balanced(),
            SelectionStrategy::MinMemory => AlgorithmWeights::min_memory(),
            SelectionStrategy::MinCpu => AlgorithmWeights::min_cpu(),
            SelectionStrategy::Adaptive => AlgorithmWeights::adaptive(),
        };

        Self {
            hardware,
            stats,
            strategy,
            weights,
            learning: LearningParameters::default(),
        }
    }

    /// Select algorithm and level for given data
    pub fn select_algorithm(
        &self,
        data: &[u8],
        data_type: &DataType,
        entropy: f64,
    ) -> (CompressionAlgorithm, CompressionLevel) {
        let context = SelectionContext {
            data_type: data_type.clone(),
            data_size: data.len(),
            entropy,
            system_load: self.estimate_system_load(),
            available_memory_mb: self.hardware.memory.available_gb * 1024,
            storage_speed: self.classify_storage_speed(),
            time_constraint_ms: None,
            min_compression_ratio: None,
        };

        let result = self.select_with_context(&context);
        (result.algorithm, result.level)
    }

    /// Select algorithm with detailed context
    pub fn select_with_context(&self, context: &SelectionContext) -> SelectionResult {
        let candidates = self.get_algorithm_candidates();
        let mut best_result: Option<SelectionResult> = None;
        let mut best_score = f64::NEG_INFINITY;

        for algorithm in candidates {
            for level in self.get_level_candidates(&algorithm, context) {
                let result = self.evaluate_algorithm(algorithm, level, context);
                let score = self.calculate_score(&result, context);

                if score > best_score {
                    best_score = score;
                    best_result = Some(result);
                }
            }
        }

        best_result.unwrap_or_else(|| {
            // Fallback to safe default
            SelectionResult {
                algorithm: CompressionAlgorithm::LZ4,
                level: CompressionLevel::Fast,
                confidence: 0.5,
                estimated_ratio: 0.6,
                estimated_time_us: 100.0,
                reasoning: "Fallback to safe default".to_string(),
            }
        })
    }

    /// Get available algorithm candidates
    fn get_algorithm_candidates(&self) -> Vec<CompressionAlgorithm> {
        let mut candidates = vec![
            CompressionAlgorithm::None,
            CompressionAlgorithm::LZ4,
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Snappy,
        ];

        // Add hardware-accelerated if available
        if self.hardware.has_compression_acceleration() {
            candidates.push(CompressionAlgorithm::HardwareAccelerated);
        }

        // Add optional algorithms if enabled
        #[cfg(feature = "lzma")]
        candidates.push(CompressionAlgorithm::LZMA);

        #[cfg(feature = "brotli")]
        candidates.push(CompressionAlgorithm::Brotli);

        candidates
    }

    /// Get compression level candidates for algorithm
    fn get_level_candidates(
        &self,
        algorithm: &CompressionAlgorithm,
        context: &SelectionContext,
    ) -> Vec<CompressionLevel> {
        let all_levels = vec![
            CompressionLevel::Fastest,
            CompressionLevel::Fast,
            CompressionLevel::Balanced,
            CompressionLevel::High,
            CompressionLevel::Maximum,
        ];

        // Filter levels based on constraints
        all_levels
            .into_iter()
            .filter(|level| self.level_meets_constraints(algorithm, level, context))
            .collect()
    }

    /// Check if level meets context constraints
    fn level_meets_constraints(
        &self,
        algorithm: &CompressionAlgorithm,
        level: &CompressionLevel,
        context: &SelectionContext,
    ) -> bool {
        // Time constraint check
        if let Some(time_limit_ms) = context.time_constraint_ms {
            let estimated_time_us = self.estimate_compression_time(
                algorithm,
                level,
                context.data_size,
                context.entropy,
            );
            if estimated_time_us > (time_limit_ms * 1000) as f64 {
                return false;
            }
        }

        // Memory constraint check
        let estimated_memory_mb = self.estimate_memory_usage(algorithm, level, context.data_size);
        if estimated_memory_mb > context.available_memory_mb as f64 {
            return false;
        }

        // System load check
        if context.system_load > 0.8 && level.as_int() > 6 {
            return false; // Avoid high compression levels under heavy load
        }

        true
    }

    /// Evaluate algorithm performance for context
    fn evaluate_algorithm(
        &self,
        algorithm: CompressionAlgorithm,
        level: CompressionLevel,
        context: &SelectionContext,
    ) -> SelectionResult {
        let stats = self.stats.read();

        // Get historical performance data
        let algo_stats = stats.algorithm_stats.get(&algorithm);
        let data_type_stats = stats.data_type_stats.get(&context.data_type);

        // Estimate performance metrics
        let estimated_ratio = self.estimate_compression_ratio(
            algorithm,
            level,
            context.entropy,
            algo_stats,
            data_type_stats,
        );

        let estimated_time_us =
            self.estimate_compression_time(&algorithm, &level, context.data_size, context.entropy);

        // Calculate confidence based on historical data
        let confidence = self.calculate_confidence(algo_stats, data_type_stats);

        // Generate reasoning
        let reasoning = self.generate_reasoning(algorithm, level, context, confidence);

        SelectionResult {
            algorithm,
            level,
            confidence,
            estimated_ratio,
            estimated_time_us,
            reasoning,
        }
    }

    /// Calculate selection score for result
    fn calculate_score(&self, result: &SelectionResult, context: &SelectionContext) -> f64 {
        let mut score = 0.0;

        // Compression ratio score (higher ratio = smaller size = better)
        let ratio_score = (1.0 - result.estimated_ratio) * 100.0;
        score += ratio_score * self.weights.compression_ratio;

        // Speed score (lower time = better)
        let time_score = 1000.0 / (1.0 + result.estimated_time_us / 1000.0);
        score += time_score * self.weights.compression_speed;

        // Confidence score
        score += result.confidence * 50.0 * self.weights.historical_performance;

        // Algorithm-specific bonuses
        score += self.get_algorithm_bonus(result.algorithm, context);

        // Data type compatibility bonus
        score += self.get_data_type_bonus(result.algorithm, &context.data_type);

        // Hardware optimization bonus
        if result.algorithm.is_hardware_accelerated()
            && self.hardware.has_compression_acceleration()
        {
            score += 25.0;
        }

        // Storage speed optimization
        score += self.get_storage_optimization_bonus(result.algorithm, context.storage_speed);

        // Exploration bonus for less-used algorithms
        if result.confidence < self.learning.confidence_threshold {
            score += self.learning.exploration_rate * 10.0;
        }

        score
    }

    /// Get algorithm-specific bonus score
    fn get_algorithm_bonus(
        &self,
        algorithm: CompressionAlgorithm,
        context: &SelectionContext,
    ) -> f64 {
        match algorithm {
            CompressionAlgorithm::None => {
                // Bonus for very small data or very high entropy
                if context.data_size < 1024 || context.entropy > 7.5 {
                    20.0
                } else {
                    0.0
                }
            }
            CompressionAlgorithm::LZ4 => {
                // Bonus for real-time scenarios and medium-sized data
                if context.data_size < 1024 * 1024 {
                    15.0
                } else {
                    10.0
                }
            }
            CompressionAlgorithm::Zstd => {
                // Bonus for good balance and large data
                if context.data_size > 64 * 1024 {
                    12.0
                } else {
                    8.0
                }
            }
            CompressionAlgorithm::Snappy => {
                // Bonus for very fast compression needs
                if context.system_load > 0.7 {
                    18.0
                } else {
                    10.0
                }
            }
            CompressionAlgorithm::LZMA => {
                // Bonus for archival and low entropy data
                if context.entropy < 5.0 && context.storage_speed == StorageSpeed::Slow {
                    15.0
                } else {
                    0.0
                }
            }
            CompressionAlgorithm::Brotli => {
                // Bonus for text data
                if matches!(context.data_type, DataType::Text | DataType::Json) {
                    12.0
                } else {
                    5.0
                }
            }
            CompressionAlgorithm::HardwareAccelerated => {
                // Large bonus if hardware acceleration available
                20.0
            }
        }
    }

    /// Get data type compatibility bonus
    fn get_data_type_bonus(&self, algorithm: CompressionAlgorithm, data_type: &DataType) -> f64 {
        match (algorithm, data_type) {
            (CompressionAlgorithm::Brotli, DataType::Text) => 15.0,
            (CompressionAlgorithm::Brotli, DataType::Json) => 12.0,
            (CompressionAlgorithm::Zstd, DataType::Binary) => 10.0,
            (CompressionAlgorithm::LZ4, DataType::Numeric) => 8.0,
            (CompressionAlgorithm::None, DataType::Compressed) => 20.0,
            _ => 0.0,
        }
    }

    /// Get storage speed optimization bonus
    fn get_storage_optimization_bonus(
        &self,
        algorithm: CompressionAlgorithm,
        storage_speed: StorageSpeed,
    ) -> f64 {
        match (algorithm, storage_speed) {
            // Fast storage: prioritize speed
            (CompressionAlgorithm::LZ4, StorageSpeed::VeryFast) => 10.0,
            (CompressionAlgorithm::Snappy, StorageSpeed::VeryFast) => 12.0,
            (CompressionAlgorithm::None, StorageSpeed::VeryFast) => 8.0,

            // Slow storage: prioritize compression
            (CompressionAlgorithm::Zstd, StorageSpeed::Slow) => 15.0,
            (CompressionAlgorithm::LZMA, StorageSpeed::Slow) => 12.0,
            (CompressionAlgorithm::Brotli, StorageSpeed::Slow) => 10.0,

            _ => 0.0,
        }
    }

    /// Estimate compression ratio
    fn estimate_compression_ratio(
        &self,
        algorithm: CompressionAlgorithm,
        level: CompressionLevel,
        entropy: f64,
        algo_stats: Option<&AlgorithmStats>,
        data_type_stats: Option<&DataTypeStats>,
    ) -> f64 {
        // Start with entropy-based estimation
        let mut ratio = match algorithm {
            CompressionAlgorithm::None => 1.0,
            CompressionAlgorithm::LZ4 => {
                if entropy < 6.0 {
                    0.4
                } else if entropy < 7.0 {
                    0.6
                } else {
                    0.8
                }
            }
            CompressionAlgorithm::Zstd => {
                let base = if entropy < 5.0 {
                    0.2
                } else if entropy < 6.0 {
                    0.3
                } else if entropy < 7.0 {
                    0.5
                } else {
                    0.7
                };
                // Adjust for compression level
                base * (1.0 - (level.as_int() as f64 - 1.0) * 0.05)
            }
            CompressionAlgorithm::Snappy => {
                if entropy < 6.0 {
                    0.5
                } else if entropy < 7.0 {
                    0.7
                } else {
                    0.9
                }
            }
            CompressionAlgorithm::LZMA => {
                if entropy < 4.0 {
                    0.1
                } else if entropy < 6.0 {
                    0.2
                } else if entropy < 7.0 {
                    0.4
                } else {
                    0.6
                }
            }
            CompressionAlgorithm::Brotli => {
                if entropy < 5.0 {
                    0.15
                } else if entropy < 6.0 {
                    0.25
                } else if entropy < 7.0 {
                    0.4
                } else {
                    0.6
                }
            }
            CompressionAlgorithm::HardwareAccelerated => 0.4, // Similar to Zstd
        };

        // Adjust based on historical data
        if let Some(stats) = algo_stats {
            if stats.usage_count >= self.learning.min_samples {
                // Weight historical data higher with more samples
                let weight =
                    (stats.usage_count as f64 / (stats.usage_count as f64 + 10.0)).min(0.8);
                ratio = ratio * (1.0 - weight) + stats.avg_compression_ratio * weight;
            }
        }

        // Adjust based on data type statistics
        if let Some(data_stats) = data_type_stats {
            if let Some(effectiveness) = data_stats.algorithm_effectiveness.get(&algorithm) {
                if data_stats.sample_count >= self.learning.min_samples {
                    // Incorporate data type specific performance
                    let weight = 0.3;
                    ratio = ratio * (1.0 - weight) + (1.0 - effectiveness) * weight;
                }
            }
        }

        ratio.clamp(0.01, 1.0) // Clamp to reasonable range
    }

    /// Estimate compression time in microseconds
    fn estimate_compression_time(
        &self,
        algorithm: &CompressionAlgorithm,
        level: &CompressionLevel,
        data_size: usize,
        entropy: f64,
    ) -> f64 {
        // Base compression speed (MB/s)
        let base_speed_mb_s = match algorithm {
            CompressionAlgorithm::None => 10000.0, // Memory copy speed
            CompressionAlgorithm::LZ4 => 600.0,
            CompressionAlgorithm::Zstd => match level {
                CompressionLevel::Fastest => 400.0,
                CompressionLevel::Fast => 300.0,
                CompressionLevel::Balanced => 200.0,
                CompressionLevel::High => 100.0,
                CompressionLevel::Maximum => 50.0,
            },
            CompressionAlgorithm::Snappy => 800.0,
            CompressionAlgorithm::LZMA => 15.0,
            CompressionAlgorithm::Brotli => 25.0,
            CompressionAlgorithm::HardwareAccelerated => 1000.0,
        };

        // Adjust for hardware performance
        let hardware_factor = self.hardware.compression_performance_factor();
        let adjusted_speed = base_speed_mb_s * hardware_factor;

        // Adjust for entropy (high entropy data is harder to compress)
        let entropy_factor = 1.0 + (entropy - 4.0) * 0.1;
        let final_speed = adjusted_speed / entropy_factor;

        // Calculate time
        let data_mb = data_size as f64 / (1024.0 * 1024.0);
        let time_seconds = data_mb / final_speed;
        time_seconds * 1_000_000.0 // Convert to microseconds
    }

    /// Estimate memory usage in MB
    fn estimate_memory_usage(
        &self,
        algorithm: &CompressionAlgorithm,
        level: &CompressionLevel,
        data_size: usize,
    ) -> f64 {
        let data_mb = data_size as f64 / (1024.0 * 1024.0);

        match algorithm {
            CompressionAlgorithm::None => data_mb, // Just input buffer
            CompressionAlgorithm::LZ4 => data_mb * 1.1, // Small overhead
            CompressionAlgorithm::Zstd => {
                let overhead = match level {
                    CompressionLevel::Fastest => 1.2,
                    CompressionLevel::Fast => 1.5,
                    CompressionLevel::Balanced => 2.0,
                    CompressionLevel::High => 4.0,
                    CompressionLevel::Maximum => 8.0,
                };
                data_mb * overhead
            }
            CompressionAlgorithm::Snappy => data_mb * 1.2,
            CompressionAlgorithm::LZMA => data_mb * 10.0, // High memory usage
            CompressionAlgorithm::Brotli => data_mb * 3.0,
            CompressionAlgorithm::HardwareAccelerated => data_mb * 1.5,
        }
    }

    /// Calculate confidence score
    fn calculate_confidence(
        &self,
        algo_stats: Option<&AlgorithmStats>,
        data_type_stats: Option<&DataTypeStats>,
    ) -> f64 {
        let mut confidence = 0.5; // Base confidence

        // Algorithm usage confidence
        if let Some(stats) = algo_stats {
            let usage_confidence = (stats.usage_count as f64
                / (stats.usage_count as f64 + self.learning.min_samples as f64))
                .min(1.0);
            confidence = confidence * 0.5 + usage_confidence * 0.5;

            // Success rate factor
            confidence *= stats.success_rate;
        }

        // Data type confidence
        if let Some(data_stats) = data_type_stats {
            let sample_confidence = (data_stats.sample_count as f64
                / (data_stats.sample_count as f64 + self.learning.min_samples as f64))
                .min(1.0);
            confidence = confidence * 0.7 + sample_confidence * 0.3;
        }

        confidence.clamp(0.1, 1.0)
    }

    /// Generate human-readable reasoning
    fn generate_reasoning(
        &self,
        algorithm: CompressionAlgorithm,
        _level: CompressionLevel,
        context: &SelectionContext,
        confidence: f64,
    ) -> String {
        let mut reasons = Vec::new();

        // Data characteristics
        match context.data_type {
            DataType::Text => reasons.push("Text data detected"),
            DataType::Json => reasons.push("JSON data detected"),
            DataType::Binary => reasons.push("Binary data detected"),
            DataType::Numeric => reasons.push("Numeric data detected"),
            DataType::Compressed => reasons.push("Already compressed data detected"),
            DataType::Mixed => reasons.push("Mixed data type detected"),
        }

        // Entropy consideration
        if context.entropy < 5.0 {
            reasons.push("Low entropy data - good compression potential");
        } else if context.entropy > 7.0 {
            reasons.push("High entropy data - limited compression potential");
        }

        // Algorithm choice reasoning
        match algorithm {
            CompressionAlgorithm::None => {
                reasons.push("No compression - data already compressed or very small");
            }
            CompressionAlgorithm::LZ4 => {
                reasons.push("LZ4 selected for fast compression/decompression");
            }
            CompressionAlgorithm::Zstd => {
                reasons.push("Zstd selected for balanced compression and speed");
            }
            CompressionAlgorithm::Snappy => {
                reasons.push("Snappy selected for maximum speed");
            }
            CompressionAlgorithm::LZMA => {
                reasons.push("LZMA selected for maximum compression ratio");
            }
            CompressionAlgorithm::Brotli => {
                reasons.push("Brotli selected for text compression optimization");
            }
            CompressionAlgorithm::HardwareAccelerated => {
                reasons.push("Hardware acceleration available and utilized");
            }
        }

        // System considerations
        if context.system_load > 0.8 {
            reasons.push("High system load - prioritizing low CPU usage");
        }

        if context.available_memory_mb < 1024 {
            reasons.push("Limited memory - avoiding high-memory algorithms");
        }

        // Confidence level
        if confidence > 0.8 {
            reasons.push("High confidence based on historical performance");
        } else if confidence < 0.4 {
            reasons.push("Low confidence - algorithm selection may be suboptimal");
        }

        reasons.join("; ")
    }

    /// Estimate current system load
    fn estimate_system_load(&self) -> f64 {
        // Simplified system load estimation
        // In a real implementation, this would check CPU usage, memory pressure, etc.
        0.3 // Default moderate load
    }

    /// Classify storage speed based on hardware capabilities
    fn classify_storage_speed(&self) -> StorageSpeed {
        let storage = &self.hardware.storage;

        if storage.seq_read_mb_s > 2000 {
            StorageSpeed::VeryFast
        } else if storage.seq_read_mb_s > 500 {
            StorageSpeed::Fast
        } else if storage.seq_read_mb_s > 200 {
            StorageSpeed::Medium
        } else {
            StorageSpeed::Slow
        }
    }

    /// Update selection strategy
    pub fn set_strategy(&mut self, strategy: SelectionStrategy) {
        self.strategy = strategy;
        self.weights = match strategy {
            SelectionStrategy::MaxCompression => AlgorithmWeights::max_compression(),
            SelectionStrategy::MaxSpeed => AlgorithmWeights::max_speed(),
            SelectionStrategy::Balanced => AlgorithmWeights::balanced(),
            SelectionStrategy::MinMemory => AlgorithmWeights::min_memory(),
            SelectionStrategy::MinCpu => AlgorithmWeights::min_cpu(),
            SelectionStrategy::Adaptive => AlgorithmWeights::adaptive(),
        };
    }

    /// Get current strategy
    pub fn strategy(&self) -> SelectionStrategy {
        self.strategy
    }
}

impl AlgorithmWeights {
    fn balanced() -> Self {
        Self {
            compression_ratio: 1.0,
            compression_speed: 1.0,
            decompression_speed: 0.8,
            memory_usage: 0.5,
            cpu_usage: 0.5,
            historical_performance: 0.7,
        }
    }

    fn max_compression() -> Self {
        Self {
            compression_ratio: 2.0,
            compression_speed: 0.3,
            decompression_speed: 0.5,
            memory_usage: 0.2,
            cpu_usage: 0.2,
            historical_performance: 0.8,
        }
    }

    fn max_speed() -> Self {
        Self {
            compression_ratio: 0.3,
            compression_speed: 2.0,
            decompression_speed: 2.0,
            memory_usage: 0.8,
            cpu_usage: 1.0,
            historical_performance: 0.9,
        }
    }

    fn min_memory() -> Self {
        Self {
            compression_ratio: 0.8,
            compression_speed: 0.8,
            decompression_speed: 0.8,
            memory_usage: 2.0,
            cpu_usage: 0.5,
            historical_performance: 0.7,
        }
    }

    fn min_cpu() -> Self {
        Self {
            compression_ratio: 0.5,
            compression_speed: 1.5,
            decompression_speed: 1.5,
            memory_usage: 0.5,
            cpu_usage: 2.0,
            historical_performance: 0.8,
        }
    }

    fn adaptive() -> Self {
        Self {
            compression_ratio: 1.2,
            compression_speed: 1.2,
            decompression_speed: 1.0,
            memory_usage: 0.8,
            cpu_usage: 0.8,
            historical_performance: 1.0,
        }
    }
}

impl Default for LearningParameters {
    fn default() -> Self {
        Self {
            learning_rate: 0.1,
            exploration_rate: 0.1,
            min_samples: 10,
            confidence_threshold: 0.7,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use std::sync::Arc;

    #[test]
    fn test_adaptive_selector_creation() {
        let stats = Arc::new(RwLock::new(CompressionStats::default()));
        let selector = AdaptiveSelector::new(Arc::clone(&stats));

        assert_eq!(selector.strategy(), SelectionStrategy::Adaptive);
    }

    #[test]
    fn test_algorithm_selection() {
        let stats = Arc::new(RwLock::new(CompressionStats::default()));
        let selector = AdaptiveSelector::new(Arc::clone(&stats));

        let data = b"Hello, World! This is test data for compression.";
        let data_type = DataType::Text;
        let entropy = 4.5;

        let (algorithm, level) = selector.select_algorithm(data, &data_type, entropy);

        // Should select a reasonable algorithm and level
        assert_ne!(algorithm, CompressionAlgorithm::None); // Should compress text
        assert!(level.as_int() > 0);
    }

    #[test]
    fn test_selection_strategies() {
        let stats = Arc::new(RwLock::new(CompressionStats::default()));

        let strategies = vec![
            SelectionStrategy::MaxCompression,
            SelectionStrategy::MaxSpeed,
            SelectionStrategy::Balanced,
            SelectionStrategy::MinMemory,
            SelectionStrategy::MinCpu,
        ];

        for strategy in strategies {
            let selector = AdaptiveSelector::with_strategy(Arc::clone(&stats), strategy);
            assert_eq!(selector.strategy(), strategy);
        }
    }

    #[test]
    fn test_context_based_selection() {
        let stats = Arc::new(RwLock::new(CompressionStats::default()));
        let selector = AdaptiveSelector::new(Arc::clone(&stats));

        let context = SelectionContext {
            data_type: DataType::Text,
            data_size: 1024,
            entropy: 4.0,
            system_load: 0.5,
            available_memory_mb: 2048,
            storage_speed: StorageSpeed::Fast,
            time_constraint_ms: Some(100),
            min_compression_ratio: Some(0.5),
        };

        let result = selector.select_with_context(&context);

        assert!(result.confidence >= 0.0 && result.confidence <= 1.0);
        assert!(result.estimated_ratio >= 0.0 && result.estimated_ratio <= 1.0);
        assert!(result.estimated_time_us > 0.0);
        assert!(!result.reasoning.is_empty());
    }

    #[test]
    fn test_storage_speed_classification() {
        let stats = Arc::new(RwLock::new(CompressionStats::default()));
        let selector = AdaptiveSelector::new(Arc::clone(&stats));

        let storage_speed = selector.classify_storage_speed();

        // Should be one of the valid storage speeds
        matches!(
            storage_speed,
            StorageSpeed::VeryFast | StorageSpeed::Fast | StorageSpeed::Medium | StorageSpeed::Slow
        );
    }

    #[test]
    fn test_algorithm_weights() {
        let balanced = AlgorithmWeights::balanced();
        let max_compression = AlgorithmWeights::max_compression();
        let max_speed = AlgorithmWeights::max_speed();

        // Max compression should prioritize ratio over speed
        assert!(max_compression.compression_ratio > max_compression.compression_speed);

        // Max speed should prioritize speed over ratio
        assert!(max_speed.compression_speed > max_speed.compression_ratio);

        // Balanced should have similar weights
        assert!((balanced.compression_ratio - balanced.compression_speed).abs() < 0.5);
    }
}
