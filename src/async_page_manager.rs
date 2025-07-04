use crate::async_storage::{AsyncStorage, AsyncIOConfig, AsyncIOStats};
use crate::error::{Error, Result};
use crate::storage::{Page, PAGE_SIZE};
use async_trait::async_trait;
use tokio::sync::RwLock;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::timeout;

/// Async page manager for non-blocking I/O operations
pub struct AsyncPageManager {
    file: Arc<RwLock<Option<File>>>,
    _file_path: std::path::PathBuf,
    page_count: AtomicU64,
    free_pages: Arc<RwLock<VecDeque<u32>>>,
    config: AsyncIOConfig,
    stats: Arc<RwLock<AsyncIOStats>>,
    io_semaphore: Arc<Semaphore>,
    write_coalescer: Option<mpsc::Sender<WriteRequest>>,
}

#[derive(Debug)]
struct WriteRequest {
    page: Page,
    response: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
struct _CoalescedWrite {
    pages: Vec<Page>,
    responses: Vec<oneshot::Sender<Result<()>>>,
}

impl AsyncPageManager {
    /// Helper to acquire semaphore permit with proper error handling
    async fn acquire_io_permit(&self) -> Result<tokio::sync::SemaphorePermit<'_>> {
        match timeout(Duration::from_secs(30), self.io_semaphore.acquire()).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(Error::InvalidOperation {
                reason: "I/O semaphore closed".to_string(),
            }),
            Err(_) => Err(Error::Timeout("I/O semaphore acquisition timed out".to_string())),
        }
    }

    /// Create a new async page manager
    pub async fn create<P: AsRef<Path>>(
        path: P,
        initial_size: u64,
        config: AsyncIOConfig,
    ) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        
        // Create the file
        let file = File::create(&file_path).await?;
        
        // Set initial size
        file.set_len(initial_size).await?;
        file.sync_all().await?;
        
        let page_count = initial_size / PAGE_SIZE as u64;
        let mut free_pages = VecDeque::new();
        
        // Pages 0 is reserved for metadata, so start from 1
        for i in 1..page_count {
            free_pages.push_back(i as u32);
        }
        
        let manager = Self {
            file: Arc::new(RwLock::new(Some(file))),
            _file_path: file_path,
            page_count: AtomicU64::new(page_count),
            free_pages: Arc::new(RwLock::new(free_pages)),
            io_semaphore: Arc::new(Semaphore::new(config.max_concurrent_ops)),
            config: config.clone(),
            stats: Arc::new(RwLock::new(AsyncIOStats::default())),
            write_coalescer: None,
        };
        
        Ok(manager)
    }
    
    /// Open an existing async page manager
    pub async fn open<P: AsRef<Path>>(path: P, config: AsyncIOConfig) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        
        let file = File::options()
            .read(true)
            .write(true)
            .open(&file_path)
            .await
            ?;
        
        let file_len = file.metadata().await?.len();
        let page_count = file_len / PAGE_SIZE as u64;
        
        // TODO: Recover free pages from metadata
        let free_pages = VecDeque::new();
        
        let mut manager = Self {
            file: Arc::new(RwLock::new(Some(file))),
            _file_path: file_path,
            page_count: AtomicU64::new(page_count),
            free_pages: Arc::new(RwLock::new(free_pages)),
            io_semaphore: Arc::new(Semaphore::new(config.max_concurrent_ops)),
            config: config.clone(),
            stats: Arc::new(RwLock::new(AsyncIOStats::default())),
            write_coalescer: None,
        };
        
        // Start write coalescer if enabled
        if config.enable_write_coalescing {
            manager.start_write_coalescer().await;
        }
        
        Ok(manager)
    }
    
    /// Start the write coalescing background task
    async fn start_write_coalescer(&mut self) {
        let (tx, mut rx) = mpsc::channel::<WriteRequest>(self.config.buffer_size);
        self.write_coalescer = Some(tx);
        
        let file = self.file.clone();
        let config = self.config.clone();
        let stats = self.stats.clone();
        
        tokio::spawn(async move {
            let mut pending_writes = Vec::new();
            let window_duration = Duration::from_millis(config.write_coalescing_window_ms);
            
            loop {
                // Collect writes for the coalescing window
                let deadline = Instant::now() + window_duration;
                
                while Instant::now() < deadline {
                    match timeout(Duration::from_millis(1), rx.recv()).await {
                        Ok(Some(write_req)) => {
                            pending_writes.push(write_req);
                            if pending_writes.len() >= config.buffer_size {
                                break;
                            }
                        }
                        Ok(None) => return, // Channel closed
                        Err(_) => break,    // Timeout
                    }
                }
                
                if !pending_writes.is_empty() {
                    Self::process_coalesced_writes(&file, &mut pending_writes, &stats).await;
                    stats.write().await.write_coalescing_hits += 1;
                }
            }
        });
    }
    
    /// Process a batch of coalesced writes
    async fn process_coalesced_writes(
        file: &Arc<RwLock<Option<File>>>,
        writes: &mut Vec<WriteRequest>,
        stats: &Arc<RwLock<AsyncIOStats>>,
    ) {
        let start = Instant::now();
        
        // Sort writes by page ID for sequential I/O
        writes.sort_by_key(|w| w.page.id);
        
        let result = {
            let mut file_guard = file.write().await;
            if let Some(ref mut file) = *file_guard {
                // Write all pages in sequence
                let mut success = true;
                for write in writes.iter() {
                    let offset = write.page.id as u64 * PAGE_SIZE as u64;
                    if (file.seek(SeekFrom::Start(offset)).await).is_err() {
                        success = false;
                        break;
                    }
                    if (file.write_all(write.page.get_data()).await).is_err() {
                        success = false;
                        break;
                    }
                }
                
                if success {
                    if file.sync_data().await.is_err() {
                        success = false;
                    }
                }
                
                if success { Ok(()) } else { Err(Error::Io("Write failed".to_string())) }
            } else {
                Err(Error::Generic("File not open".to_string()))
            }
        };
        
        // Send responses
        for write in writes.drain(..) {
            let _ = write.response.send(result.clone());
        }
        
        // Update stats
        let elapsed = start.elapsed();
        let mut stats_guard = stats.write().await;
        stats_guard.total_writes += writes.len() as u64;
        stats_guard.write_latency_avg_us = 
            (stats_guard.write_latency_avg_us + elapsed.as_micros() as u64) / 2;
    }
    
    /// Get current I/O statistics
    pub async fn get_stats(&self) -> AsyncIOStats {
        self.stats.read().await.clone()
    }
    
    /// Reset I/O statistics
    pub async fn reset_stats(&self) {
        *self.stats.write().await = AsyncIOStats::default();
    }
}

#[async_trait]
impl AsyncStorage for AsyncPageManager {
    async fn allocate_page(&self) -> Result<u32> {
        let _permit = self.acquire_io_permit().await?;
        
        let mut free_pages = self.free_pages.write().await;
        if let Some(page_id) = free_pages.pop_front() {
            Ok(page_id)
        } else {
            // Grow the file
            let new_page_id = self.page_count.fetch_add(1, Ordering::SeqCst) as u32;
            
            // Extend file size
            let new_size = (new_page_id + 1) as u64 * PAGE_SIZE as u64;
            {
                let mut file_guard = self.file.write().await;
                if let Some(ref mut file) = *file_guard {
                    file.set_len(new_size).await?;
                }
            }
            
            Ok(new_page_id)
        }
    }
    
    async fn free_page(&self, page_id: u32) -> Result<()> {
        let _permit = self.acquire_io_permit().await?;
        
        let mut free_pages = self.free_pages.write().await;
        free_pages.push_back(page_id);
        Ok(())
    }
    
    async fn read_page(&self, page_id: u32) -> Result<Page> {
        let _permit = self.acquire_io_permit().await?;
        let start = Instant::now();
        
        let mut data = vec![0u8; PAGE_SIZE];
        let offset = page_id as u64 * PAGE_SIZE as u64;
        
        {
            let mut file_guard = self.file.write().await;
            if let Some(ref mut file) = *file_guard {
                file.seek(SeekFrom::Start(offset)).await?;
                file.read_exact(&mut data).await?;
            } else {
                return Err(Error::Generic("File not open".to_string()));
            }
        }
        
        // Update stats
        let elapsed = start.elapsed();
        {
            let mut stats = self.stats.write().await;
            stats.total_reads += 1;
            stats.read_latency_avg_us = 
                (stats.read_latency_avg_us + elapsed.as_micros() as u64) / 2;
        }
        
        let mut page_data = [0u8; PAGE_SIZE];
        page_data.copy_from_slice(&data);
        Ok(Page::with_data(page_id, page_data))
    }
    
    async fn write_page(&self, page: &Page) -> Result<()> {
        // Use write coalescer if available
        if let Some(ref coalescer) = self.write_coalescer {
            let (tx, rx) = oneshot::channel();
            let write_req = WriteRequest {
                page: page.clone(),
                response: tx,
            };
            
            coalescer.send(write_req).await.map_err(|_| 
                Error::Generic("Write coalescer channel closed".to_string()))?;
            
            rx.await.map_err(|_| 
                Error::Generic("Write response channel closed".to_string()))?
        } else {
            // Direct write
            let _permit = self.acquire_io_permit().await?;
            let start = Instant::now();
            
            let offset = page.id as u64 * PAGE_SIZE as u64;
            
            {
                let mut file_guard = self.file.write().await;
                if let Some(ref mut file) = *file_guard {
                    file.seek(SeekFrom::Start(offset)).await?;
                    file.write_all(page.get_data()).await?;
                } else {
                    return Err(Error::Generic("File not open".to_string()));
                }
            }
            
            // Update stats
            let elapsed = start.elapsed();
            {
                let mut stats = self.stats.write().await;
                stats.total_writes += 1;
                stats.write_latency_avg_us = 
                    (stats.write_latency_avg_us + elapsed.as_micros() as u64) / 2;
            }
            
            Ok(())
        }
    }
    
    async fn sync(&self) -> Result<()> {
        let _permit = self.acquire_io_permit().await?;
        let start = Instant::now();
        
        {
            let mut file_guard = self.file.write().await;
            if let Some(ref mut file) = *file_guard {
                file.sync_all().await?;
            }
        }
        
        // Update stats
        let elapsed = start.elapsed();
        {
            let mut stats = self.stats.write().await;
            stats.total_syncs += 1;
            stats.sync_latency_avg_us = 
                (stats.sync_latency_avg_us + elapsed.as_micros() as u64) / 2;
        }
        
        Ok(())
    }
    
    fn page_count(&self) -> u32 {
        self.page_count.load(Ordering::Relaxed) as u32
    }
    
    fn free_page_count(&self) -> usize {
        // Can't use async in trait method, so return 0 as placeholder
        // This would need to be refactored to async fn in the trait
        0
    }
}