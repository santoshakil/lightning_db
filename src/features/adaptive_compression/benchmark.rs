//! Compression Algorithm Benchmarking
//!
//! Comprehensive benchmarking suite for measuring and comparing compression algorithm
//! performance across different data types, sizes, and system conditions.

use super::{CompressionAlgorithm, CompressionAlgorithmTrait, CompressionLevel, DataType};
use crate::core::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Benchmark configuration
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of iterations for each test
    pub iterations: usize,
    /// Warm-up iterations before measurement
    pub warmup_iterations: usize,
    /// Maximum benchmark duration per test
    pub max_duration: Duration,
    /// Test data sizes in bytes
    pub test_sizes: Vec<usize>,
    /// Compression levels to test
    pub compression_levels: Vec<CompressionLevel>,
    /// Data types to generate and test
    pub data_types: Vec<DataType>,
    /// Whether to include memory usage measurements
    pub measure_memory: bool,
    /// Whether to include detailed timing breakdown
    pub detailed_timing: bool,
}

/// Benchmark results for a specific test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Algorithm tested
    pub algorithm: CompressionAlgorithm,
    /// Compression level used
    pub level: CompressionLevel,
    /// Data type tested
    pub data_type: DataType,
    /// Original data size
    pub original_size: usize,
    /// Compressed size
    pub compressed_size: usize,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Compression time statistics
    pub compression_time: TimeStatistics,
    /// Decompression time statistics
    pub decompression_time: TimeStatistics,
    /// Throughput in MB/s
    pub compression_throughput: f64,
    pub decompression_throughput: f64,
    /// Memory usage statistics
    pub memory_usage: MemoryStatistics,
    /// Efficiency score
    pub efficiency_score: f64,
    /// Number of test iterations
    pub iterations: usize,
}

/// Time measurement statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeStatistics {
    /// Mean time in nanoseconds
    pub mean_ns: f64,
    /// Median time in nanoseconds
    pub median_ns: f64,
    /// Minimum time in nanoseconds
    pub min_ns: u64,
    /// Maximum time in nanoseconds
    pub max_ns: u64,
    /// Standard deviation in nanoseconds
    pub std_dev_ns: f64,
    /// 95th percentile in nanoseconds
    pub p95_ns: f64,
    /// 99th percentile in nanoseconds
    pub p99_ns: f64,
}

/// Memory usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStatistics {
    /// Peak memory usage in bytes
    pub peak_bytes: u64,
    /// Average memory usage in bytes
    pub average_bytes: u64,
    /// Memory overhead as ratio of input size
    pub overhead_ratio: f64,
}

/// Complete benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
    /// Individual test results
    pub results: Vec<BenchmarkResult>,
    /// Summary statistics by algorithm
    pub algorithm_summary: HashMap<CompressionAlgorithm, AlgorithmSummary>,
    /// Best algorithm for each data type
    pub best_by_data_type: HashMap<DataType, CompressionAlgorithm>,
    /// Overall recommendations
    pub recommendations: BenchmarkRecommendations,
    /// Benchmark metadata
    pub metadata: BenchmarkMetadata,
}

/// Algorithm summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmSummary {
    /// Algorithm name
    pub algorithm: CompressionAlgorithm,
    /// Average compression ratio across all tests
    pub avg_compression_ratio: f64,
    /// Average compression speed in MB/s
    pub avg_compression_speed: f64,
    /// Average decompression speed in MB/s
    pub avg_decompression_speed: f64,
    /// Average efficiency score
    pub avg_efficiency: f64,
    /// Number of tests performed
    pub test_count: usize,
    /// Best use cases
    pub best_use_cases: Vec<String>,
}

/// Benchmark recommendations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkRecommendations {
    /// Best overall algorithm
    pub best_overall: CompressionAlgorithm,
    /// Best for speed
    pub best_for_speed: CompressionAlgorithm,
    /// Best for compression ratio
    pub best_for_ratio: CompressionAlgorithm,
    /// Best for balanced performance
    pub best_balanced: CompressionAlgorithm,
    /// Recommendations by use case
    pub use_case_recommendations: HashMap<String, CompressionAlgorithm>,
}

/// Benchmark metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkMetadata {
    /// When the benchmark was run
    pub timestamp: u64,
    /// Total benchmark duration
    pub duration: Duration,
    /// System information
    pub system_info: String,
    /// Benchmark configuration used
    pub config_summary: String,
}

/// Compression benchmark suite
pub struct CompressionBenchmark {
    config: BenchmarkConfig,
}

impl Default for CompressionBenchmark {
    fn default() -> Self {
        Self {
            config: BenchmarkConfig::default(),
        }
    }
}

impl CompressionBenchmark {
    /// Create a new benchmark suite
    pub fn new() -> Self {
        Self::default()
    }

    /// Create benchmark with custom configuration
    pub fn with_config(config: BenchmarkConfig) -> Self {
        Self { config }
    }

    /// Run comprehensive benchmark on all algorithms
    pub fn run_comprehensive_benchmark(
        &self,
        test_data: &[u8],
        algorithms: &HashMap<
            CompressionAlgorithm,
            Box<dyn CompressionAlgorithmTrait + Send + Sync>,
        >,
    ) -> Result<BenchmarkResults> {
        let start_time = Instant::now();
        let mut results = Vec::new();

        // Generate test datasets
        let test_datasets = self.generate_test_datasets(test_data)?;

        // Benchmark each algorithm
        for (algorithm, compressor) in algorithms.iter() {
            for dataset in &test_datasets {
                for &level in &self.config.compression_levels {
                    let result = self.benchmark_algorithm_on_dataset(
                        *algorithm,
                        compressor.as_ref(),
                        level,
                        dataset,
                    )?;
                    results.push(result);
                }
            }
        }

        // Analyze results
        let algorithm_summary = self.compute_algorithm_summaries(&results);
        let best_by_data_type = self.find_best_by_data_type(&results);
        let recommendations = self.generate_recommendations(&results, &algorithm_summary);
        let metadata = BenchmarkMetadata {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            duration: start_time.elapsed(),
            system_info: Self::get_system_info(),
            config_summary: format!(
                "{} iterations, {} warmup, {} algorithms",
                self.config.iterations,
                self.config.warmup_iterations,
                algorithms.len()
            ),
        };

        Ok(BenchmarkResults {
            results,
            algorithm_summary,
            best_by_data_type,
            recommendations,
            metadata,
        })
    }

    /// Benchmark a single algorithm
    pub fn benchmark_algorithm(
        &self,
        algorithm: CompressionAlgorithm,
        compressor: &dyn CompressionAlgorithmTrait,
        test_data: &[u8],
    ) -> Result<Vec<BenchmarkResult>> {
        let mut results = Vec::new();

        for &level in &self.config.compression_levels {
            for &size in &self.config.test_sizes {
                let data = if test_data.len() >= size {
                    &test_data[..size]
                } else {
                    test_data
                };

                let dataset = TestDataset {
                    data: data.to_vec(),
                    data_type: DataType::Mixed, // Default
                    _size: size,
                };

                let result =
                    self.benchmark_algorithm_on_dataset(algorithm, compressor, level, &dataset)?;
                results.push(result);
            }
        }

        Ok(results)
    }

    /// Benchmark algorithm on specific dataset
    fn benchmark_algorithm_on_dataset(
        &self,
        algorithm: CompressionAlgorithm,
        compressor: &dyn CompressionAlgorithmTrait,
        level: CompressionLevel,
        dataset: &TestDataset,
    ) -> Result<BenchmarkResult> {
        // Warm-up runs
        for _ in 0..self.config.warmup_iterations {
            let _ = compressor.compress(&dataset.data, level)?;
        }

        let mut compression_times = Vec::new();
        let mut decompression_times = Vec::new();
        let mut compressed_sizes = Vec::new();
        let mut memory_usage = Vec::new();

        // Benchmark runs
        for _ in 0..self.config.iterations {
            // Measure compression
            let start = Instant::now();
            let compressed = compressor.compress(&dataset.data, level)?;
            let compression_time = start.elapsed();

            compressed_sizes.push(compressed.len());
            compression_times.push(compression_time.as_nanos() as u64);

            // Measure decompression
            let start = Instant::now();
            let decompressed = compressor.decompress(&compressed)?;
            let decompression_time = start.elapsed();

            decompression_times.push(decompression_time.as_nanos() as u64);

            // Verify correctness
            if decompressed != dataset.data {
                return Err(Error::Compression(format!(
                    "Compression/decompression mismatch for algorithm {:?}",
                    algorithm
                )));
            }

            // Measure memory usage (simplified)
            if self.config.measure_memory {
                let memory_estimate = self.estimate_memory_usage(&dataset.data, &compressed);
                memory_usage.push(memory_estimate);
            }
        }

        // Calculate statistics
        let compression_time_stats = Self::calculate_time_statistics(&compression_times);
        let decompression_time_stats = Self::calculate_time_statistics(&decompression_times);

        let avg_compressed_size =
            compressed_sizes.iter().sum::<usize>() as f64 / compressed_sizes.len() as f64;
        let compression_ratio = avg_compressed_size / dataset.data.len() as f64;

        let compression_throughput =
            Self::calculate_throughput(dataset.data.len(), compression_time_stats.mean_ns);
        let decompression_throughput =
            Self::calculate_throughput(dataset.data.len(), decompression_time_stats.mean_ns);

        let memory_stats = if self.config.measure_memory && !memory_usage.is_empty() {
            let peak = *memory_usage.iter().max().unwrap_or(&0);
            let average = memory_usage.iter().sum::<u64>() as f64 / memory_usage.len() as f64;
            MemoryStatistics {
                peak_bytes: peak,
                average_bytes: average as u64,
                overhead_ratio: average / dataset.data.len() as f64,
            }
        } else {
            MemoryStatistics {
                peak_bytes: 0,
                average_bytes: 0,
                overhead_ratio: 0.0,
            }
        };

        let efficiency_score = Self::calculate_efficiency_score(
            compression_ratio,
            compression_throughput,
            decompression_throughput,
        );

        Ok(BenchmarkResult {
            algorithm,
            level,
            data_type: dataset.data_type.clone(),
            original_size: dataset.data.len(),
            compressed_size: avg_compressed_size as usize,
            compression_ratio,
            compression_time: compression_time_stats,
            decompression_time: decompression_time_stats,
            compression_throughput,
            decompression_throughput,
            memory_usage: memory_stats,
            efficiency_score,
            iterations: self.config.iterations,
        })
    }

    /// Generate test datasets
    fn generate_test_datasets(&self, base_data: &[u8]) -> Result<Vec<TestDataset>> {
        let mut datasets = Vec::new();

        for data_type in &self.config.data_types {
            for &size in &self.config.test_sizes {
                let data = self.generate_data_for_type(data_type.clone(), size, base_data)?;
                datasets.push(TestDataset {
                    data,
                    data_type: data_type.clone(),
                    _size: size,
                });
            }
        }

        Ok(datasets)
    }

    /// Generate data for specific type
    fn generate_data_for_type(
        &self,
        data_type: DataType,
        size: usize,
        base_data: &[u8],
    ) -> Result<Vec<u8>> {
        match data_type {
            DataType::Text => {
                let text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                    .repeat(size / 50 + 1);
                Ok(text.as_bytes()[..size.min(text.len())].to_vec())
            }
            DataType::Json => {
                let json_template = r#"{"id": 12345, "name": "Test Object", "value": 67.89, "active": true, "tags": ["tag1", "tag2", "tag3"]}"#;
                let repeated = json_template.repeat(size / json_template.len() + 1);
                Ok(repeated.as_bytes()[..size.min(repeated.len())].to_vec())
            }
            DataType::Numeric => {
                let mut data = Vec::with_capacity(size);
                for i in 0..(size / 8) {
                    data.extend_from_slice(&(i as u64).to_le_bytes());
                }
                data.resize(size, 0);
                Ok(data)
            }
            DataType::Binary => {
                if base_data.len() >= size {
                    Ok(base_data[..size].to_vec())
                } else {
                    let mut data = base_data.to_vec();
                    data.resize(size, 0);
                    Ok(data)
                }
            }
            DataType::Compressed => {
                // Generate high-entropy data
                let mut data = Vec::with_capacity(size);
                for i in 0..size {
                    data.push(((i * 17 + 37) % 256) as u8);
                }
                Ok(data)
            }
            DataType::Mixed => {
                if base_data.len() >= size {
                    Ok(base_data[..size].to_vec())
                } else {
                    let mut data = base_data.to_vec();
                    data.resize(size, 0);
                    Ok(data)
                }
            }
        }
    }

    /// Calculate time statistics
    fn calculate_time_statistics(times_ns: &[u64]) -> TimeStatistics {
        if times_ns.is_empty() {
            return TimeStatistics {
                mean_ns: 0.0,
                median_ns: 0.0,
                min_ns: 0,
                max_ns: 0,
                std_dev_ns: 0.0,
                p95_ns: 0.0,
                p99_ns: 0.0,
            };
        }

        let mut sorted_times = times_ns.to_vec();
        sorted_times.sort_unstable();

        let mean = sorted_times.iter().sum::<u64>() as f64 / sorted_times.len() as f64;
        let median = sorted_times[sorted_times.len() / 2] as f64;
        let min = sorted_times[0];
        let max = sorted_times[sorted_times.len() - 1];

        // Calculate standard deviation
        let variance = sorted_times
            .iter()
            .map(|&t| (t as f64 - mean).powi(2))
            .sum::<f64>()
            / sorted_times.len() as f64;
        let std_dev = variance.sqrt();

        // Calculate percentiles
        let p95_index = (sorted_times.len() as f64 * 0.95) as usize;
        let p99_index = (sorted_times.len() as f64 * 0.99) as usize;
        let p95 = sorted_times[p95_index.min(sorted_times.len() - 1)] as f64;
        let p99 = sorted_times[p99_index.min(sorted_times.len() - 1)] as f64;

        TimeStatistics {
            mean_ns: mean,
            median_ns: median,
            min_ns: min,
            max_ns: max,
            std_dev_ns: std_dev,
            p95_ns: p95,
            p99_ns: p99,
        }
    }

    /// Calculate throughput in MB/s
    fn calculate_throughput(data_size: usize, time_ns: f64) -> f64 {
        if time_ns <= 0.0 {
            return 0.0;
        }

        let data_mb = data_size as f64 / (1024.0 * 1024.0);
        let time_s = time_ns / 1_000_000_000.0;
        data_mb / time_s
    }

    /// Calculate efficiency score
    fn calculate_efficiency_score(
        compression_ratio: f64,
        compression_throughput: f64,
        decompression_throughput: f64,
    ) -> f64 {
        // Weighted combination of compression ratio and speed
        let ratio_score = (1.0 - compression_ratio) * 100.0; // Higher compression is better
        let speed_score = (compression_throughput + decompression_throughput * 2.0) / 10.0; // Favor decompression speed

        // Geometric mean for balanced scoring
        (ratio_score * speed_score).sqrt()
    }

    /// Estimate memory usage
    fn estimate_memory_usage(&self, original: &[u8], compressed: &[u8]) -> u64 {
        // Simplified estimation: original + compressed + some overhead
        (original.len() + compressed.len()) as u64 + 1024 // 1KB overhead
    }

    /// Compute algorithm summaries
    fn compute_algorithm_summaries(
        &self,
        results: &[BenchmarkResult],
    ) -> HashMap<CompressionAlgorithm, AlgorithmSummary> {
        let mut summaries = HashMap::new();

        for algorithm in results
            .iter()
            .map(|r| r.algorithm)
            .collect::<std::collections::HashSet<_>>()
        {
            let algo_results: Vec<_> = results
                .iter()
                .filter(|r| r.algorithm == algorithm)
                .collect();

            if algo_results.is_empty() {
                continue;
            }

            let avg_compression_ratio = algo_results
                .iter()
                .map(|r| r.compression_ratio)
                .sum::<f64>()
                / algo_results.len() as f64;

            let avg_compression_speed = algo_results
                .iter()
                .map(|r| r.compression_throughput)
                .sum::<f64>()
                / algo_results.len() as f64;

            let avg_decompression_speed = algo_results
                .iter()
                .map(|r| r.decompression_throughput)
                .sum::<f64>()
                / algo_results.len() as f64;

            let avg_efficiency = algo_results.iter().map(|r| r.efficiency_score).sum::<f64>()
                / algo_results.len() as f64;

            let best_use_cases = self.determine_best_use_cases(&algo_results);

            summaries.insert(
                algorithm,
                AlgorithmSummary {
                    algorithm,
                    avg_compression_ratio,
                    avg_compression_speed,
                    avg_decompression_speed,
                    avg_efficiency,
                    test_count: algo_results.len(),
                    best_use_cases,
                },
            );
        }

        summaries
    }

    /// Find best algorithm for each data type
    fn find_best_by_data_type(
        &self,
        results: &[BenchmarkResult],
    ) -> HashMap<DataType, CompressionAlgorithm> {
        let mut best_by_type = HashMap::new();

        for data_type in results
            .iter()
            .map(|r| r.data_type.clone())
            .collect::<std::collections::HashSet<_>>()
        {
            let type_results: Vec<_> = results
                .iter()
                .filter(|r| r.data_type == data_type)
                .collect();

            if let Some(best) = type_results.iter().max_by(|a, b| {
                a.efficiency_score
                    .partial_cmp(&b.efficiency_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }) {
                best_by_type.insert(data_type, best.algorithm);
            }
        }

        best_by_type
    }

    /// Generate recommendations
    fn generate_recommendations(
        &self,
        _results: &[BenchmarkResult],
        summaries: &HashMap<CompressionAlgorithm, AlgorithmSummary>,
    ) -> BenchmarkRecommendations {
        let best_overall = summaries
            .iter()
            .max_by(|a, b| {
                a.1.avg_efficiency
                    .partial_cmp(&b.1.avg_efficiency)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(algo, _)| *algo)
            .unwrap_or(CompressionAlgorithm::LZ4);

        let best_for_speed = summaries
            .iter()
            .max_by(|a, b| {
                a.1.avg_compression_speed
                    .partial_cmp(&b.1.avg_compression_speed)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(algo, _)| *algo)
            .unwrap_or(CompressionAlgorithm::Snappy);

        let best_for_ratio = summaries
            .iter()
            .min_by(|a, b| {
                a.1.avg_compression_ratio
                    .partial_cmp(&b.1.avg_compression_ratio)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(algo, _)| *algo)
            .unwrap_or(CompressionAlgorithm::Zstd);

        let best_balanced = summaries
            .iter()
            .max_by(|a, b| {
                let score_a =
                    (1.0 - a.1.avg_compression_ratio) + a.1.avg_compression_speed / 1000.0;
                let score_b =
                    (1.0 - b.1.avg_compression_ratio) + b.1.avg_compression_speed / 1000.0;
                score_a
                    .partial_cmp(&score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(algo, _)| *algo)
            .unwrap_or(CompressionAlgorithm::LZ4);

        let mut use_case_recommendations = HashMap::new();
        use_case_recommendations.insert("real-time".to_string(), best_for_speed);
        use_case_recommendations.insert("archival".to_string(), best_for_ratio);
        use_case_recommendations.insert("general-purpose".to_string(), best_balanced);
        use_case_recommendations.insert("streaming".to_string(), CompressionAlgorithm::LZ4);
        use_case_recommendations.insert(
            "memory-constrained".to_string(),
            CompressionAlgorithm::Snappy,
        );

        BenchmarkRecommendations {
            best_overall,
            best_for_speed,
            best_for_ratio,
            best_balanced,
            use_case_recommendations,
        }
    }

    /// Determine best use cases for algorithm
    fn determine_best_use_cases(&self, results: &[&BenchmarkResult]) -> Vec<String> {
        let mut use_cases = Vec::new();

        if let Some(result) = results.iter().max_by(|a, b| {
            a.compression_throughput
                .partial_cmp(&b.compression_throughput)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            if result.compression_throughput > 500.0 {
                use_cases.push("High-speed compression".to_string());
            }
        }

        if let Some(result) = results.iter().min_by(|a, b| {
            a.compression_ratio
                .partial_cmp(&b.compression_ratio)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            if result.compression_ratio < 0.5 {
                use_cases.push("High compression ratio".to_string());
            }
        }

        let avg_efficiency =
            results.iter().map(|r| r.efficiency_score).sum::<f64>() / results.len() as f64;
        if avg_efficiency > 50.0 {
            use_cases.push("Balanced performance".to_string());
        }

        if use_cases.is_empty() {
            use_cases.push("General purpose".to_string());
        }

        use_cases
    }

    /// Get system information
    fn get_system_info() -> String {
        format!(
            "CPU cores: {}, Platform: {}",
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1),
            std::env::consts::OS
        )
    }
}

/// Test dataset
#[derive(Debug, Clone)]
struct TestDataset {
    data: Vec<u8>,
    data_type: DataType,
    _size: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            iterations: 10,
            warmup_iterations: 3,
            max_duration: Duration::from_secs(300), // 5 minutes
            test_sizes: vec![1024, 8192, 65536, 1048576], // 1KB to 1MB
            compression_levels: vec![
                CompressionLevel::Fast,
                CompressionLevel::Balanced,
                CompressionLevel::High,
            ],
            data_types: vec![
                DataType::Text,
                DataType::Binary,
                DataType::Numeric,
                DataType::Json,
            ],
            measure_memory: true,
            detailed_timing: true,
        }
    }
}

impl Default for MemoryStatistics {
    fn default() -> Self {
        Self {
            peak_bytes: 0,
            average_bytes: 0,
            overhead_ratio: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::adaptive_compression::{LZ4Compression, SnappyCompression};

    #[test]
    fn test_benchmark_config() {
        let config = BenchmarkConfig::default();
        assert!(config.iterations > 0);
        // warmup_iterations is usize, so always >= 0
        assert!(!config.test_sizes.is_empty());
        assert!(!config.compression_levels.is_empty());
        assert!(!config.data_types.is_empty());
    }

    #[test]
    fn test_time_statistics() {
        let times = vec![100, 200, 150, 300, 250, 180, 220];
        let stats = CompressionBenchmark::calculate_time_statistics(&times);

        assert!(stats.mean_ns > 0.0);
        assert!(stats.median_ns > 0.0);
        assert_eq!(stats.min_ns, 100);
        assert_eq!(stats.max_ns, 300);
        assert!(stats.std_dev_ns > 0.0);
    }

    #[test]
    fn test_throughput_calculation() {
        let throughput = CompressionBenchmark::calculate_throughput(1048576, 1_000_000_000.0); // 1MB in 1 second
        assert!((throughput - 1.0).abs() < 0.01); // Should be ~1 MB/s
    }

    #[test]
    fn test_efficiency_score() {
        let score = CompressionBenchmark::calculate_efficiency_score(0.5, 100.0, 200.0);
        assert!(score > 0.0);
        assert!(score < 1000.0); // Reasonable range
    }

    #[test]
    #[ignore] // TODO: Fix hanging issue
    fn test_benchmark_single_algorithm() {
        let benchmark = CompressionBenchmark::new();
        let compressor = LZ4Compression::new();
        let test_data = b"Hello, World! This is a test string for compression benchmarking.";

        let results = benchmark
            .benchmark_algorithm(CompressionAlgorithm::LZ4, &compressor, test_data)
            .unwrap();

        assert!(!results.is_empty());

        for result in results {
            assert_eq!(result.algorithm, CompressionAlgorithm::LZ4);
            assert!(result.compression_ratio > 0.0);
            assert!(result.compression_ratio <= 1.0);
            assert!(result.compression_throughput > 0.0);
            assert!(result.decompression_throughput > 0.0);
            assert_eq!(result.iterations, benchmark.config.iterations);
        }
    }

    #[test]
    fn test_data_generation() {
        let benchmark = CompressionBenchmark::new();
        let base_data = b"test data";

        let text_data = benchmark
            .generate_data_for_type(DataType::Text, 100, base_data)
            .unwrap();
        assert_eq!(text_data.len(), 100);

        let json_data = benchmark
            .generate_data_for_type(DataType::Json, 200, base_data)
            .unwrap();
        assert_eq!(json_data.len(), 200);

        let numeric_data = benchmark
            .generate_data_for_type(DataType::Numeric, 64, base_data)
            .unwrap();
        assert_eq!(numeric_data.len(), 64);
    }

    #[test]
    fn test_comprehensive_benchmark() {
        let benchmark = CompressionBenchmark::new();
        let mut algorithms: HashMap<
            CompressionAlgorithm,
            Box<dyn CompressionAlgorithmTrait + Send + Sync>,
        > = HashMap::new();

        algorithms.insert(CompressionAlgorithm::LZ4, Box::new(LZ4Compression::new()));
        algorithms.insert(
            CompressionAlgorithm::Snappy,
            Box::new(SnappyCompression::new()),
        );

        let test_data =
            b"This is comprehensive test data for benchmarking multiple compression algorithms.";

        let results = benchmark
            .run_comprehensive_benchmark(test_data, &algorithms)
            .unwrap();

        assert!(!results.results.is_empty());
        assert!(!results.algorithm_summary.is_empty());
        assert!(!results.best_by_data_type.is_empty());

        // Verify we have results for all algorithms
        assert!(results
            .algorithm_summary
            .contains_key(&CompressionAlgorithm::LZ4));
        assert!(results
            .algorithm_summary
            .contains_key(&CompressionAlgorithm::Snappy));

        // Verify recommendations are reasonable
        let rec = &results.recommendations;
        assert!(algorithms.contains_key(&rec.best_overall));
        assert!(algorithms.contains_key(&rec.best_for_speed));
        assert!(algorithms.contains_key(&rec.best_for_ratio));
        assert!(algorithms.contains_key(&rec.best_balanced));
    }
}
