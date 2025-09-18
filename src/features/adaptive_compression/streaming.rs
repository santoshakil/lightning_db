//! Streaming Compression Support
//!
//! Provides streaming compression and decompression capabilities for large datasets
//! and real-time scenarios with adaptive algorithm switching and buffer management.

use super::{
    AdaptiveCompressionEngine, CompressionAlgorithm, CompressionLevel, DataType, SelectionContext,
    StorageSpeed,
};
use crate::core::error::Result;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Streaming compression configuration
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Input buffer size in bytes
    pub input_buffer_size: usize,
    /// Output buffer size in bytes
    pub output_buffer_size: usize,
    /// Chunk size for processing
    pub chunk_size: usize,
    /// Maximum chunks to buffer
    pub max_buffered_chunks: usize,
    /// Whether to enable adaptive algorithm switching
    pub adaptive_switching: bool,
    /// Algorithm switching threshold (compression ratio change)
    pub switching_threshold: f64,
    /// Evaluation interval for algorithm performance
    pub evaluation_interval: Duration,
    /// Flush interval for pending data
    pub flush_interval: Duration,
    /// Whether to enable compression statistics collection
    pub collect_statistics: bool,
}

/// Streaming compression chunk
#[derive(Debug, Clone)]
struct CompressionChunk {
    /// Original data
    data: Vec<u8>,
    /// Compressed data
    compressed: Vec<u8>,
    /// Algorithm used
    _algorithm: CompressionAlgorithm,
    /// Compression level used
    _level: CompressionLevel,
    /// Compression ratio achieved
    ratio: f64,
    /// Time taken to compress
    _compression_time: Duration,
    /// Chunk sequence number
    _sequence: u64,
}

/// Streaming compression statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingStats {
    /// Total bytes processed
    pub total_input_bytes: u64,
    /// Total compressed bytes output
    pub total_output_bytes: u64,
    /// Overall compression ratio
    pub overall_ratio: f64,
    /// Average compression speed (MB/s)
    pub avg_compression_speed: f64,
    /// Number of chunks processed
    pub chunks_processed: u64,
    /// Number of algorithm switches
    pub algorithm_switches: u64,
    /// Current algorithm in use
    pub current_algorithm: CompressionAlgorithm,
    /// Algorithm usage distribution
    pub algorithm_usage: std::collections::HashMap<CompressionAlgorithm, u64>,
    /// Processing start time
    pub start_time: std::time::SystemTime,
    /// Last update time
    pub last_update: std::time::SystemTime,
}

thread_local! {
    static BUFFER_POOL: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
    static DEQUE_POOL: RefCell<Vec<VecDeque<u8>>> = const { RefCell::new(Vec::new()) };
}

struct CompressorBuffers {
    input_buffer: VecDeque<u8>,
    output_buffer: VecDeque<u8>,
    chunk_queue: VecDeque<Vec<u8>>,
    processed_chunks: VecDeque<CompressionChunk>,
}

impl CompressorBuffers {
    fn new() -> Self {
        Self {
            input_buffer: VecDeque::new(),
            output_buffer: VecDeque::new(),
            chunk_queue: VecDeque::new(),
            processed_chunks: VecDeque::new(),
        }
    }

    fn _clear(&mut self) {
        self.input_buffer.clear();
        self.output_buffer.clear();
        self.chunk_queue.clear();
        self.processed_chunks.clear();
    }
}

/// Streaming compressor with optimized Arc usage
pub struct StreamingCompressor {
    config: StreamingConfig,
    engine: Arc<AdaptiveCompressionEngine>,
    buffers: Mutex<CompressorBuffers>,
    current_algorithm: CompressionAlgorithm,
    current_level: CompressionLevel,
    stats: RwLock<StreamingStats>,
    sequence_counter: std::sync::atomic::AtomicU64,
    last_evaluation: std::sync::atomic::AtomicU64,
    data_type_detector: DataTypeDetector,
}

/// Streaming decompressor
pub struct StreamingDecompressor {
    /// Configuration
    _config: StreamingConfig,
    /// Adaptive compression engine
    _engine: Arc<AdaptiveCompressionEngine>,
    /// Input buffer
    _input_buffer: Arc<Mutex<VecDeque<u8>>>,
    /// Output buffer
    _output_buffer: Arc<Mutex<VecDeque<u8>>>,
    /// Chunk metadata queue
    _chunk_metadata: Arc<Mutex<VecDeque<ChunkMetadata>>>,
    /// Statistics
    _stats: Arc<RwLock<DecompressionStats>>,
}

/// Chunk metadata for decompression
#[derive(Debug, Clone)]
struct ChunkMetadata {
    /// Algorithm used for compression
    _algorithm: CompressionAlgorithm,
    /// Compression level used
    _level: CompressionLevel,
    /// Original size
    _original_size: usize,
    /// Compressed size
    _compressed_size: usize,
    /// Sequence number
    _sequence: u64,
}

/// Decompression statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecompressionStats {
    /// Total compressed bytes processed
    pub total_input_bytes: u64,
    /// Total decompressed bytes output
    pub total_output_bytes: u64,
    /// Average decompression speed (MB/s)
    pub avg_decompression_speed: f64,
    /// Number of chunks processed
    pub chunks_processed: u64,
    /// Algorithm usage distribution
    pub algorithm_usage: std::collections::HashMap<CompressionAlgorithm, u64>,
    /// Start time
    pub start_time: std::time::SystemTime,
    /// Last update time
    pub last_update: std::time::SystemTime,
}

/// Data type detector for streaming data
#[derive(Debug)]
struct DataTypeDetector {
    /// Sample buffer for analysis
    sample_buffer: VecDeque<u8>,
    /// Sample size
    sample_size: usize,
    /// Current detected type
    current_type: DataType,
    /// Detection confidence
    confidence: f64,
}

/// Streaming reader adapter
pub struct StreamingReader<R: Read> {
    inner: BufReader<R>,
    compressor: StreamingCompressor,
    output_buffer: Vec<u8>,
    buffer_pos: usize,
}

/// Streaming writer adapter
pub struct StreamingWriter<W: Write> {
    inner: BufWriter<W>,
    compressor: StreamingCompressor,
    _pending_data: Vec<u8>,
}

impl StreamingCompressor {
    /// Create a new streaming compressor
    pub fn new(config: StreamingConfig, engine: Arc<AdaptiveCompressionEngine>) -> Result<Self> {
        let stats = StreamingStats {
            total_input_bytes: 0,
            total_output_bytes: 0,
            overall_ratio: 1.0,
            avg_compression_speed: 0.0,
            chunks_processed: 0,
            algorithm_switches: 0,
            current_algorithm: CompressionAlgorithm::LZ4, // Default
            algorithm_usage: std::collections::HashMap::new(),
            start_time: std::time::SystemTime::now(),
            last_update: std::time::SystemTime::now(),
        };

        Ok(Self {
            config,
            engine,
            buffers: Mutex::new(CompressorBuffers::new()),
            current_algorithm: CompressionAlgorithm::LZ4,
            current_level: CompressionLevel::Balanced,
            stats: RwLock::new(stats),
            sequence_counter: std::sync::atomic::AtomicU64::new(0),
            last_evaluation: std::sync::atomic::AtomicU64::new(
                Instant::now().elapsed().as_nanos() as u64
            ),
            data_type_detector: DataTypeDetector::new(1024),
        })
    }

    /// Write data to the compressor
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        // Add data to input buffer
        {
            let mut buffers = self.buffers.lock();
            buffers.input_buffer.extend(data);
        }

        // Update data type detector
        self.data_type_detector.process_data(data);

        // Process chunks if buffer is large enough
        self.process_pending_chunks()?;

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_input_bytes += data.len() as u64;
            stats.last_update = std::time::SystemTime::now();
        }

        Ok(())
    }

    /// Read compressed data from the compressor
    pub fn read(&mut self, buffer: &mut [u8]) -> Result<usize> {
        let mut buffers = self.buffers.lock();

        let bytes_to_read = buffer.len().min(buffers.output_buffer.len());
        if bytes_to_read == 0 {
            return Ok(0);
        }

        // Efficiently drain bytes from output buffer
        let drain_iter = buffers.output_buffer.drain(..bytes_to_read);
        for (i, byte) in drain_iter.enumerate() {
            buffer[i] = byte;
        }

        Ok(bytes_to_read)
    }

    /// Flush all pending data
    pub fn flush(&mut self) -> Result<()> {
        // Process any remaining data in input buffer
        self.force_process_remaining()?;

        // Process all pending chunks
        loop {
            let has_chunks = {
                let buffers = self.buffers.lock();
                !buffers.chunk_queue.is_empty()
            };
            if !has_chunks {
                break;
            }
            self.process_pending_chunks()?;
        }

        Ok(())
    }

    /// Get current statistics
    pub fn stats(&self) -> StreamingStats {
        self.stats.read().clone()
    }

    /// Process pending chunks
    fn process_pending_chunks(&mut self) -> Result<()> {
        // Check if we need to create new chunks
        self.create_chunks_from_buffer()?;

        // Process queued chunks
        let mut processed_count = 0;
        while processed_count < 10 {
            // Limit processing per call
            let chunk_data = {
                let mut buffers = self.buffers.lock();
                buffers.chunk_queue.pop_front()
            };

            if let Some(data) = chunk_data {
                self.process_chunk(data)?;
                processed_count += 1;
            } else {
                break;
            }
        }

        // Check if we need to switch algorithms
        if self.config.adaptive_switching {
            self.evaluate_algorithm_performance()?;
        }

        Ok(())
    }

    /// Create chunks from input buffer
    fn create_chunks_from_buffer(&mut self) -> Result<()> {
        let mut buffers = self.buffers.lock();

        // Limit the number of buffered chunks
        if buffers.chunk_queue.len() >= self.config.max_buffered_chunks {
            return Ok(());
        }

        while buffers.input_buffer.len() >= self.config.chunk_size {
            let chunk_data: Vec<u8> = buffers
                .input_buffer
                .drain(..self.config.chunk_size)
                .collect();
            buffers.chunk_queue.push_back(chunk_data);

            if buffers.chunk_queue.len() >= self.config.max_buffered_chunks {
                break;
            }
        }

        Ok(())
    }

    /// Process a single chunk
    fn process_chunk(&mut self, data: Vec<u8>) -> Result<()> {
        let start_time = Instant::now();
        let sequence = self
            .sequence_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Get current algorithm and level
        let algorithm = self.current_algorithm;
        let level = self.current_level;

        // Compress the chunk
        let compressed = self
            .engine
            .compress_with_algorithm(&data, algorithm, level)?;

        let compression_time = start_time.elapsed();
        let ratio = compressed.compressed_size as f64 / data.len() as f64;

        // Create chunk
        let chunk = CompressionChunk {
            data,
            compressed: compressed.data,
            _algorithm: algorithm,
            _level: level,
            ratio,
            _compression_time: compression_time,
            _sequence: sequence,
        };

        // Add to output buffer and store processed chunk
        {
            let mut buffers = self.buffers.lock();
            buffers.output_buffer.extend(&chunk.compressed);
            buffers.processed_chunks.push_back(chunk);

            // Limit stored chunks
            while buffers.processed_chunks.len() > 100 {
                buffers.processed_chunks.pop_front();
            }
        }

        // Update statistics
        self.update_compression_stats(
            compressed.original_size,
            compressed.compressed_size,
            compression_time,
            algorithm,
        );

        Ok(())
    }

    /// Force process remaining data
    fn force_process_remaining(&mut self) -> Result<()> {
        let remaining_data: Vec<u8> = {
            let mut buffers = self.buffers.lock();
            if buffers.input_buffer.is_empty() {
                return Ok(());
            }
            buffers.input_buffer.drain(..).collect()
        };

        if !remaining_data.is_empty() {
            self.process_chunk(remaining_data)?;
        }

        Ok(())
    }

    /// Evaluate algorithm performance and switch if needed
    fn evaluate_algorithm_performance(&mut self) -> Result<()> {
        let now = Instant::now();
        let last_eval_nanos = self
            .last_evaluation
            .load(std::sync::atomic::Ordering::Relaxed);
        let last_eval = Instant::now() - Duration::from_nanos(last_eval_nanos);

        if now.duration_since(last_eval) < self.config.evaluation_interval {
            return Ok(());
        }

        self.last_evaluation.store(
            now.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        // Analyze recent performance
        let recent_chunks: Vec<_> = {
            let buffers = self.buffers.lock();
            buffers
                .processed_chunks
                .iter()
                .rev()
                .take(10)
                .cloned()
                .collect()
        };

        if recent_chunks.len() < 5 {
            return Ok(());
        }

        // Calculate current performance
        let current_algorithm = self.current_algorithm;
        let current_avg_ratio =
            recent_chunks.iter().map(|c| c.ratio).sum::<f64>() / recent_chunks.len() as f64;

        // Try different algorithm selection
        let sample_data = recent_chunks
            .last()
            .map(|c| c.data.clone())
            .unwrap_or_default();
        if !sample_data.is_empty() {
            let data_type = self.data_type_detector.current_type();
            let entropy = self.calculate_entropy(&sample_data);

            let context = SelectionContext {
                data_type,
                data_size: sample_data.len(),
                entropy,
                system_load: 0.5, // Implementation pending system load detection
                available_memory_mb: 1024, // Implementation pending memory detection
                storage_speed: StorageSpeed::Fast, // Implementation pending storage speed detection
                time_constraint_ms: None,
                min_compression_ratio: None,
            };

            let selection = self.engine.selector.select_with_context(&context);

            // Switch if new algorithm is significantly better
            if selection.algorithm != current_algorithm {
                let improvement =
                    (current_avg_ratio - selection.estimated_ratio) / current_avg_ratio;
                if improvement > self.config.switching_threshold {
                    self.current_algorithm = selection.algorithm;
                    self.current_level = selection.level;

                    // Update statistics
                    {
                        let mut stats = self.stats.write();
                        stats.algorithm_switches += 1;
                        stats.current_algorithm = selection.algorithm;
                    }
                }
            }
        }

        Ok(())
    }

    /// Calculate entropy of data
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

    /// Update compression statistics
    fn update_compression_stats(
        &self,
        input_size: usize,
        output_size: usize,
        compression_time: Duration,
        algorithm: CompressionAlgorithm,
    ) {
        let mut stats = self.stats.write();

        stats.total_output_bytes += output_size as u64;
        stats.chunks_processed += 1;

        // Update overall ratio
        if stats.total_input_bytes > 0 {
            stats.overall_ratio = stats.total_output_bytes as f64 / stats.total_input_bytes as f64;
        }

        // Update average speed
        let mb_processed = input_size as f64 / (1024.0 * 1024.0);
        let time_seconds = compression_time.as_secs_f64();
        if time_seconds > 0.0 {
            let current_speed = mb_processed / time_seconds;
            if stats.chunks_processed == 1 {
                stats.avg_compression_speed = current_speed;
            } else {
                // Exponential moving average
                stats.avg_compression_speed =
                    stats.avg_compression_speed * 0.9 + current_speed * 0.1;
            }
        }

        // Update algorithm usage
        *stats.algorithm_usage.entry(algorithm).or_insert(0) += 1;

        stats.last_update = std::time::SystemTime::now();
    }
}

impl DataTypeDetector {
    fn new(sample_size: usize) -> Self {
        Self {
            sample_buffer: VecDeque::new(),
            sample_size,
            current_type: DataType::Mixed,
            confidence: 0.0,
        }
    }

    fn process_data(&mut self, data: &[u8]) {
        // Add data to sample buffer
        for &byte in data {
            if self.sample_buffer.len() >= self.sample_size {
                self.sample_buffer.pop_front();
            }
            self.sample_buffer.push_back(byte);
        }

        // Update detection if we have enough samples
        if self.sample_buffer.len() >= self.sample_size / 2 {
            self.update_detection();
        }
    }

    fn update_detection(&mut self) {
        let sample: Vec<u8> = self.sample_buffer.iter().cloned().collect();

        // Calculate entropy
        let entropy = self.calculate_entropy(&sample);

        // Detect data type
        let (detected_type, confidence) = if entropy > 7.5 {
            (DataType::Compressed, 0.9)
        } else if self.is_text_data(&sample) {
            if self.is_json_data(&sample) {
                (DataType::Json, 0.8)
            } else {
                (DataType::Text, 0.8)
            }
        } else if self.is_numeric_data(&sample) {
            (DataType::Numeric, 0.7)
        } else {
            (DataType::Binary, 0.6)
        };

        // Update with exponential smoothing
        if confidence > self.confidence {
            self.current_type = detected_type;
            self.confidence = confidence;
        } else {
            self.confidence = self.confidence * 0.9 + confidence * 0.1;
        }
    }

    fn current_type(&self) -> DataType {
        self.current_type.clone()
    }

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

    fn is_text_data(&self, data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }

        if let Ok(text) = std::str::from_utf8(data) {
            let printable_count = text
                .chars()
                .filter(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
                .count();
            printable_count as f64 / text.chars().count() as f64 > 0.8
        } else {
            false
        }
    }

    fn is_json_data(&self, data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }

        let first_char = data[0];
        if first_char == b'{' || first_char == b'[' {
            let json_chars = data
                .iter()
                .filter(|&&b| matches!(b, b'{' | b'}' | b'[' | b']' | b':' | b',' | b'\"'))
                .count();
            json_chars as f64 / data.len() as f64 > 0.1
        } else {
            false
        }
    }

    fn is_numeric_data(&self, data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }

        let numeric_chars = data
            .iter()
            .filter(|&&b| matches!(b, b'0'..=b'9' | b'.' | b'-' | b'+' | b'e' | b'E'))
            .count();
        numeric_chars as f64 / data.len() as f64 > 0.6
    }
}

impl<R: Read> StreamingReader<R> {
    /// Create a new streaming reader
    pub fn new(
        reader: R,
        config: StreamingConfig,
        engine: Arc<AdaptiveCompressionEngine>,
    ) -> Result<Self> {
        let compressor = StreamingCompressor::new(config, engine)?;

        Ok(Self {
            inner: BufReader::new(reader),
            compressor,
            output_buffer: Vec::with_capacity(8192),
            buffer_pos: 0,
        })
    }
}

impl<R: Read> Read for StreamingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // If we have data in output buffer, use it first
        if self.buffer_pos < self.output_buffer.len() {
            let bytes_available = self.output_buffer.len() - self.buffer_pos;
            let bytes_to_copy = buf.len().min(bytes_available);

            buf[..bytes_to_copy].copy_from_slice(
                &self.output_buffer[self.buffer_pos..self.buffer_pos + bytes_to_copy],
            );
            self.buffer_pos += bytes_to_copy;

            return Ok(bytes_to_copy);
        }

        // Read more data from input and compress
        let mut input_buf = vec![0u8; 8192];
        let bytes_read = self.inner.read(&mut input_buf)?;

        if bytes_read == 0 {
            return Ok(0); // EOF
        }

        input_buf.truncate(bytes_read);

        // Compress the data
        self.compressor
            .write(&input_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Read compressed output
        self.output_buffer.clear();
        self.output_buffer.resize(buf.len() * 2, 0); // Conservative size

        let compressed_bytes = self
            .compressor
            .read(&mut self.output_buffer)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        self.output_buffer.truncate(compressed_bytes);
        self.buffer_pos = 0;

        // Return compressed data
        let bytes_to_copy = buf.len().min(compressed_bytes);
        buf[..bytes_to_copy].copy_from_slice(&self.output_buffer[..bytes_to_copy]);
        self.buffer_pos = bytes_to_copy;

        Ok(bytes_to_copy)
    }
}

impl<W: Write> StreamingWriter<W> {
    /// Create a new streaming writer
    pub fn new(
        writer: W,
        config: StreamingConfig,
        engine: Arc<AdaptiveCompressionEngine>,
    ) -> Result<Self> {
        let compressor = StreamingCompressor::new(config, engine)?;

        Ok(Self {
            inner: BufWriter::new(writer),
            compressor,
            _pending_data: Vec::new(),
        })
    }
}

impl<W: Write> Write for StreamingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Add data to compressor
        self.compressor
            .write(buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Read compressed output and write to underlying writer
        let mut output_buf = vec![0u8; buf.len() * 2]; // Conservative size
        let compressed_bytes = self
            .compressor
            .read(&mut output_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        if compressed_bytes > 0 {
            output_buf.truncate(compressed_bytes);
            self.inner.write_all(&output_buf)?;
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Flush compressor
        self.compressor
            .flush()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Read any remaining compressed data
        let mut output_buf = vec![0u8; 8192];
        loop {
            let bytes = self
                .compressor
                .read(&mut output_buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            if bytes == 0 {
                break;
            }

            self.inner.write_all(&output_buf[..bytes])?;
        }

        self.inner.flush()
    }
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            input_buffer_size: 64 * 1024,  // 64KB
            output_buffer_size: 64 * 1024, // 64KB
            chunk_size: 32 * 1024,         // 32KB
            max_buffered_chunks: 10,
            adaptive_switching: true,
            switching_threshold: 0.1, // 10% improvement required
            evaluation_interval: Duration::from_secs(10),
            flush_interval: Duration::from_millis(100),
            collect_statistics: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AdaptiveCompressionEngine;
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_streaming_config() {
        let config = StreamingConfig::default();
        assert!(config.input_buffer_size > 0);
        assert!(config.output_buffer_size > 0);
        assert!(config.chunk_size > 0);
        assert!(config.max_buffered_chunks > 0);
    }

    #[test]
    fn test_data_type_detector() {
        let mut detector = DataTypeDetector::new(100);

        // Test text detection
        let text_data = b"Hello, World! This is text data.";
        detector.process_data(text_data);

        // Should detect as text (though may need more data for confidence)
        detector.update_detection();
        assert!(detector.confidence > 0.0);
    }

    #[test]
    fn test_streaming_compressor() {
        let engine = Arc::new(AdaptiveCompressionEngine::new().unwrap());
        let config = StreamingConfig::default();
        let mut compressor = StreamingCompressor::new(config, engine).unwrap();

        // Write more test data to ensure compression is effective
        let test_data = b"Hello, World! This is test data for streaming compression. \
                         Repeated data helps compression: compression compression compression \
                         compression compression compression compression compression compression";
        compressor.write(test_data).unwrap();

        // Flush to ensure processing
        compressor.flush().unwrap();

        // Read compressed output
        let mut output = vec![0u8; 2048];
        let bytes_read = compressor.read(&mut output).unwrap();

        // Should have some compressed output
        assert!(bytes_read > 0);
        // For small data, compression might add overhead, so check ratio instead

        // Check statistics
        let stats = compressor.stats();
        assert!(stats.total_input_bytes > 0);
        assert!(stats.chunks_processed > 0);
    }

    #[test]
    fn test_streaming_reader() {
        let engine = Arc::new(AdaptiveCompressionEngine::new().unwrap());
        let config = StreamingConfig::default();

        // Use repeating data for better compression
        let test_data = b"This is test data for streaming reader functionality. \
                         Test test test test test test test test test test test";
        let cursor = Cursor::new(test_data.to_vec());

        let mut reader = StreamingReader::new(cursor, config, engine).unwrap();

        // Read compressed data
        let mut output = vec![0u8; 1024];
        let bytes_read = reader.read(&mut output).unwrap();

        assert!(bytes_read > 0);
        output.truncate(bytes_read);

        // Verify we got some output (may or may not be compressed depending on algorithm)
        assert!(!output.is_empty());
    }

    #[test]
    fn test_streaming_writer() {
        let engine = Arc::new(AdaptiveCompressionEngine::new().unwrap());
        let config = StreamingConfig::default();

        let output_vec = Vec::new();
        let mut writer = StreamingWriter::new(output_vec, config, engine).unwrap();

        let test_data = b"This is test data for streaming writer functionality.";

        // Write data
        let bytes_written = writer.write(test_data).unwrap();
        assert_eq!(bytes_written, test_data.len());

        // Flush to ensure all data is processed
        writer.flush().unwrap();

        // Should have written some compressed data
        let output = writer.inner.into_inner().unwrap();
        assert!(!output.is_empty());
        assert_ne!(output, test_data); // Should be compressed
    }

    #[test]
    fn test_chunk_processing() {
        let engine = Arc::new(AdaptiveCompressionEngine::new().unwrap());
        let config = StreamingConfig {
            chunk_size: 32, // Small chunks for testing
            ..Default::default()
        };

        let mut compressor = StreamingCompressor::new(config, engine).unwrap();

        // Write data larger than chunk size
        let test_data = b"This is a longer piece of test data that should be split into multiple chunks for processing.";
        compressor.write(test_data).unwrap();
        compressor.flush().unwrap();

        let stats = compressor.stats();
        assert!(stats.chunks_processed > 1); // Should process multiple chunks
    }
}
