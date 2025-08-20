use crate::core::error::Result;
use crate::performance::optimizations::simd::safe::crc32;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use bytes::Bytes;
use std::collections::VecDeque;

/// Async checksum pipeline that overlaps I/O with checksum calculation
/// This provides significant performance improvements for large data operations
pub struct AsyncChecksumPipeline {
    // Channel for sending pages to checksum workers
    checksum_tx: mpsc::UnboundedSender<ChecksumTask>,
    // Channel for receiving completed checksums
    result_rx: mpsc::UnboundedReceiver<ChecksumResult>,
    // Worker handles for cleanup
    workers: Vec<JoinHandle<()>>,
    // Semaphore to limit concurrent operations
    semaphore: Arc<Semaphore>,
}

#[derive(Debug)]
struct ChecksumTask {
    page_id: u32,
    data: Bytes,
    result_tx: mpsc::UnboundedSender<ChecksumResult>,
}

#[derive(Debug)]
pub struct ChecksumResult {
    pub page_id: u32,
    pub checksum: u32,
    pub data: Bytes,
}

impl AsyncChecksumPipeline {
    /// Create a new async checksum pipeline
    pub fn new(num_workers: usize) -> Self {
        let (checksum_tx, mut checksum_rx) = mpsc::unbounded_channel::<ChecksumTask>();
        let (result_tx, result_rx) = mpsc::unbounded_channel::<ChecksumResult>();
        let semaphore = Arc::new(Semaphore::new(num_workers * 2)); // Allow some queueing

        let mut workers = Vec::with_capacity(num_workers);

        // Spawn checksum worker tasks
        for worker_id in 0..num_workers {
            let mut task_rx = checksum_rx.clone();
            let worker_result_tx = result_tx.clone();
            let worker_semaphore = semaphore.clone();

            let handle = tokio::spawn(async move {
                Self::checksum_worker(worker_id, &mut task_rx, worker_result_tx, worker_semaphore).await;
            });

            workers.push(handle);
        }

        // Drop the original receiver to ensure workers get all tasks
        drop(checksum_rx);

        Self {
            checksum_tx,
            result_rx,
            workers,
            semaphore,
        }
    }

    /// Submit a page for async checksum calculation
    pub async fn submit_page(&self, page_id: u32, data: Bytes) -> Result<()> {
        // Acquire semaphore permit to prevent unbounded memory usage
        let _permit = self.semaphore.acquire().await.map_err(|_| {
            crate::core::error::Error::Other("Pipeline shutting down".into())
        })?;

        let task = ChecksumTask {
            page_id,
            data,
            result_tx: self.checksum_tx.clone(),
        };

        self.checksum_tx.send(task).map_err(|_| {
            crate::core::error::Error::Other("Failed to submit checksum task".into())
        })?;

        Ok(())
    }

    /// Receive next completed checksum result
    pub async fn receive_result(&mut self) -> Option<ChecksumResult> {
        self.result_rx.recv().await
    }

    /// Process multiple pages in pipeline fashion
    pub async fn process_pages_pipelined(&mut self, pages: Vec<(u32, Bytes)>) -> Result<Vec<ChecksumResult>> {
        let mut results = Vec::with_capacity(pages.len());
        let mut pending_count = 0;

        // Submit all pages for processing
        for (page_id, data) in pages {
            self.submit_page(page_id, data).await?;
            pending_count += 1;
        }

        // Collect results as they complete
        while pending_count > 0 {
            if let Some(result) = self.receive_result().await {
                results.push(result);
                pending_count -= 1;
            } else {
                break;
            }
        }

        // Sort results by page_id to maintain order
        results.sort_by_key(|r| r.page_id);
        Ok(results)
    }

    /// Checksum worker task that processes pages asynchronously
    async fn checksum_worker(
        worker_id: usize,
        task_rx: &mut mpsc::UnboundedReceiver<ChecksumTask>,
        result_tx: mpsc::UnboundedSender<ChecksumResult>,
        semaphore: Arc<Semaphore>,
    ) {
        let mut local_buffer = Vec::with_capacity(4096);

        while let Some(task) = task_rx.recv().await {
            // Acquire permit (will be released when dropped)
            let _permit = semaphore.acquire().await.unwrap();

            // Calculate checksum using SIMD-optimized function
            let checksum = Self::calculate_checksum_optimized(&task.data, &mut local_buffer).await;

            let result = ChecksumResult {
                page_id: task.page_id,
                checksum,
                data: task.data,
            };

            // Send result back
            if let Err(_) = result_tx.send(result) {
                // Pipeline shutdown, exit worker
                break;
            }
        }

        println!("Checksum worker {} shutting down", worker_id);
    }

    /// Optimized checksum calculation that yields periodically for better async behavior
    async fn calculate_checksum_optimized(data: &[u8], buffer: &mut Vec<u8>) -> u32 {
        const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks

        if data.len() <= CHUNK_SIZE {
            // Small data, calculate directly
            crc32(data)
        } else {
            // Large data, process in chunks and yield periodically
            let mut checksum = 0u32;
            
            for chunk in data.chunks(CHUNK_SIZE) {
                // Calculate checksum for chunk
                let chunk_checksum = crc32(chunk);
                
                // Combine checksums (simple XOR for demonstration)
                checksum ^= chunk_checksum;
                
                // Yield to allow other tasks to run
                tokio::task::yield_now().await;
            }
            
            checksum
        }
    }

    /// Graceful shutdown of the pipeline
    pub async fn shutdown(self) -> Result<()> {
        // Drop sender to signal workers to finish
        drop(self.checksum_tx);

        // Wait for all workers to complete
        for worker in self.workers {
            let _ = worker.await;
        }

        Ok(())
    }
}

/// Batch checksum calculator for multiple pages
pub struct BatchChecksumCalculator {
    pipeline: AsyncChecksumPipeline,
    batch_size: usize,
    pending_batch: VecDeque<(u32, Bytes)>,
}

impl BatchChecksumCalculator {
    pub fn new(num_workers: usize, batch_size: usize) -> Self {
        Self {
            pipeline: AsyncChecksumPipeline::new(num_workers),
            batch_size,
            pending_batch: VecDeque::with_capacity(batch_size),
        }
    }

    /// Add a page to the batch
    pub async fn add_page(&mut self, page_id: u32, data: Bytes) -> Result<Option<Vec<ChecksumResult>>> {
        self.pending_batch.push_back((page_id, data));

        if self.pending_batch.len() >= self.batch_size {
            self.flush_batch().await
        } else {
            Ok(None)
        }
    }

    /// Flush pending batch and return results
    pub async fn flush_batch(&mut self) -> Result<Option<Vec<ChecksumResult>>> {
        if self.pending_batch.is_empty() {
            return Ok(None);
        }

        let batch: Vec<_> = self.pending_batch.drain(..).collect();
        let results = self.pipeline.process_pages_pipelined(batch).await?;
        Ok(Some(results))
    }

    /// Process all remaining pages and shutdown
    pub async fn finish(mut self) -> Result<Vec<ChecksumResult>> {
        let final_results = if !self.pending_batch.is_empty() {
            self.flush_batch().await?.unwrap_or_default()
        } else {
            Vec::new()
        };

        self.pipeline.shutdown().await?;
        Ok(final_results)
    }
}

/// Stream-based checksum processor for large files
pub struct StreamChecksumProcessor {
    pipeline: AsyncChecksumPipeline,
    window_size: usize,
}

impl StreamChecksumProcessor {
    pub fn new(num_workers: usize, window_size: usize) -> Self {
        Self {
            pipeline: AsyncChecksumPipeline::new(num_workers),
            window_size,
        }
    }

    /// Process a stream of pages with sliding window
    pub async fn process_stream<I>(&mut self, pages: I) -> Result<Vec<ChecksumResult>>
    where
        I: IntoIterator<Item = (u32, Bytes)>,
    {
        let mut results = Vec::new();
        let mut in_flight = 0;
        let mut page_iter = pages.into_iter();

        // Fill initial window
        while in_flight < self.window_size {
            if let Some((page_id, data)) = page_iter.next() {
                self.pipeline.submit_page(page_id, data).await?;
                in_flight += 1;
            } else {
                break;
            }
        }

        // Process remaining pages with sliding window
        for (page_id, data) in page_iter {
            // Submit new page
            self.pipeline.submit_page(page_id, data).await?;
            
            // Receive one result to maintain window size
            if let Some(result) = self.pipeline.receive_result().await {
                results.push(result);
            }
        }

        // Collect remaining results
        while in_flight > 0 {
            if let Some(result) = self.pipeline.receive_result().await {
                results.push(result);
                in_flight -= 1;
            } else {
                break;
            }
        }

        // Sort by page_id
        results.sort_by_key(|r| r.page_id);
        Ok(results)
    }

    pub async fn shutdown(self) -> Result<()> {
        self.pipeline.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_async_checksum_pipeline() {
        let mut pipeline = AsyncChecksumPipeline::new(2);

        // Submit a few pages
        let data1 = Bytes::from(vec![1u8; 1024]);
        let data2 = Bytes::from(vec![2u8; 1024]);
        let data3 = Bytes::from(vec![3u8; 1024]);

        pipeline.submit_page(1, data1.clone()).await.unwrap();
        pipeline.submit_page(2, data2.clone()).await.unwrap();
        pipeline.submit_page(3, data3.clone()).await.unwrap();

        // Collect results
        let mut results = Vec::new();
        for _ in 0..3 {
            if let Some(result) = pipeline.receive_result().await {
                results.push(result);
            }
        }

        assert_eq!(results.len(), 3);
        pipeline.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_checksum_calculator() {
        let mut calculator = BatchChecksumCalculator::new(2, 3);

        // Add pages one by one
        let data1 = Bytes::from(vec![1u8; 1024]);
        let data2 = Bytes::from(vec![2u8; 1024]);
        let data3 = Bytes::from(vec![3u8; 1024]);

        assert!(calculator.add_page(1, data1).await.unwrap().is_none());
        assert!(calculator.add_page(2, data2).await.unwrap().is_none());
        
        // Third page should trigger batch processing
        let results = calculator.add_page(3, data3).await.unwrap();
        assert!(results.is_some());
        assert_eq!(results.unwrap().len(), 3);

        calculator.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_checksum_processor() {
        let mut processor = StreamChecksumProcessor::new(2, 3);

        let pages = vec![
            (1, Bytes::from(vec![1u8; 1024])),
            (2, Bytes::from(vec![2u8; 1024])),
            (3, Bytes::from(vec![3u8; 1024])),
            (4, Bytes::from(vec![4u8; 1024])),
            (5, Bytes::from(vec![5u8; 1024])),
        ];

        let results = processor.process_stream(pages).await.unwrap();
        assert_eq!(results.len(), 5);

        // Verify results are sorted by page_id
        for (i, result) in results.iter().enumerate() {
            assert_eq!(result.page_id, (i + 1) as u32);
        }

        processor.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_large_data_chunking() {
        let large_data = vec![0xAA; 256 * 1024]; // 256KB
        let mut buffer = Vec::new();
        
        let checksum = AsyncChecksumPipeline::calculate_checksum_optimized(&large_data, &mut buffer).await;
        
        // Verify we get a non-zero checksum
        assert_ne!(checksum, 0);
    }
}