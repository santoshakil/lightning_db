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
            .await?;
        
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
        let (tx, mut rx) = mpsc::channel::<WriteRequest>(128);
        self.write_coalescer = Some(tx);
        
        let file = self.file.clone();
        let config = self.config.clone();
        let stats = self.stats.clone();
        
        tokio::spawn(async move {
            let mut pending_writes = Vec::new();
            let mut last_flush = Instant::now();
            
            loop {
                // Wait for writes or timeout
                let timeout_duration = Duration::from_millis(config.write_coalesce_window_ms);
                let received = timeout(timeout_duration, rx.recv()).await;
                
                match received {
                    Ok(Some(req)) => {
                        pending_writes.push(req);
                        
                        // Check if we should flush
                        if pending_writes.len() >= config.write_coalesce_max_pages 
                            || last_flush.elapsed() >= timeout_duration {
                            Self::flush_coalesced_writes(&file, &mut pending_writes, &stats).await;
                            last_flush = Instant::now();
                        }
                    }
                    Ok(None) => {
                        // Channel closed, flush remaining and exit
                        if !pending_writes.is_empty() {
                            Self::flush_coalesced_writes(&file, &mut pending_writes, &stats).await;
                        }
                        break;
                    }
                    Err(_) => {
                        // Timeout, flush if we have pending writes
                        if !pending_writes.is_empty() {
                            Self::flush_coalesced_writes(&file, &mut pending_writes, &stats).await;
                            last_flush = Instant::now();
                        }
                    }
                }
            }
        });
    }
    
    /// Flush coalesced writes to disk
    async fn flush_coalesced_writes(
        file: &Arc<RwLock<Option<File>>>,
        writes: &mut Vec<WriteRequest>,
        stats: &Arc<RwLock<AsyncIOStats>>,
    ) {
        if writes.is_empty() {
            return;
        }
        
        let start = Instant::now();
        
        // Sort writes by page ID for sequential I/O
        writes.sort_by_key(|w| w.page.id);
        
        // Execute writes
        let mut file_guard = file.write().await;
        if let Some(file) = file_guard.as_mut() {
            for write in writes.drain(..) {
                let offset = write.page.id as u64 * PAGE_SIZE as u64;
                let result = file.seek(SeekFrom::Start(offset)).await
                    .and_then(|_| file.write_all(&write.page.data).await)
                    .map_err(|e| Error::Io(e));
                
                let _ = write.response.send(result);
            }
            
            // Sync to disk if configured
            if let Err(e) = file.sync_data().await {
                eprintln!("Failed to sync coalesced writes: {}", e);
            }
        }
        
        // Update stats
        let mut stats_guard = stats.write().await;
        stats_guard.total_writes += writes.len() as u64;
        stats_guard.total_write_time += start.elapsed();
        stats_guard.coalesced_writes += 1;
    }
    
    /// Helper to acquire semaphore permit with proper error handling
    async fn acquire_io_permit(&self) -> Result<tokio::sync::SemaphorePermit<'_>> {
        match timeout(Duration::from_secs(30), self.io_semaphore.acquire()).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(Error::InvalidOperation {
                reason: "I/O semaphore closed".to_string(),
            }),
            Err(_) => Err(Error::Timeout),
        }
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
            let mut file_guard = self.file.write().await;
            if let Some(file) = file_guard.as_mut() {
                file.set_len(new_size).await?;
            } else {
                return Err(Error::InvalidDatabase);
            }
            
            Ok(new_page_id)
        }
    }
    
    async fn read_page(&self, page_id: u32) -> Result<Page> {
        let _permit = self.acquire_io_permit().await?;
        
        let start = Instant::now();
        let offset = page_id as u64 * PAGE_SIZE as u64;
        
        let mut page = Page::new(page_id);
        
        let file_guard = self.file.read().await;
        if let Some(file) = file_guard.as_ref() {
            let mut file = file.try_clone().await?;
            file.seek(SeekFrom::Start(offset)).await?;
            file.read_exact(&mut page.data).await?;
        } else {
            return Err(Error::InvalidDatabase);
        }
        
        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_reads += 1;
        stats.total_read_time += start.elapsed();
        
        page.verify_checksum()?;
        Ok(page)
    }
    
    async fn write_page(&self, page: &Page) -> Result<()> {
        let _permit = self.acquire_io_permit().await?;
        
        // If write coalescing is enabled, send to coalescer
        if let Some(ref tx) = self.write_coalescer {
            let (response_tx, response_rx) = oneshot::channel();
            let req = WriteRequest {
                page: page.clone(),
                response: response_tx,
            };
            
            tx.send(req).await.map_err(|_| Error::InvalidOperation {
                reason: "Write coalescer channel closed".to_string(),
            })?;
            
            response_rx.await.map_err(|_| Error::InvalidOperation {
                reason: "Write coalescer response channel closed".to_string(),
            })??;
            
            return Ok(());
        }
        
        // Direct write
        let start = Instant::now();
        let offset = page.id as u64 * PAGE_SIZE as u64;
        
        let mut file_guard = self.file.write().await;
        if let Some(file) = file_guard.as_mut() {
            file.seek(SeekFrom::Start(offset)).await?;
            file.write_all(&page.data).await?;
            
            if self.config.sync_on_write {
                file.sync_data().await?;
            }
        } else {
            return Err(Error::InvalidDatabase);
        }
        
        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_writes += 1;
        stats.total_write_time += start.elapsed();
        
        Ok(())
    }
    
    async fn free_page(&self, page_id: u32) -> Result<()> {
        let _permit = self.acquire_io_permit().await?;
        
        let mut free_pages = self.free_pages.write().await;
        free_pages.push_back(page_id);
        Ok(())
    }
    
    async fn sync(&self) -> Result<()> {
        let _permit = self.acquire_io_permit().await?;
        
        let file_guard = self.file.read().await;
        if let Some(file) = file_guard.as_ref() {
            file.sync_all().await?;
        }
        Ok(())
    }
    
    async fn get_stats(&self) -> AsyncIOStats {
        self.stats.read().await.clone()
    }
    
    async fn prefetch_pages(&self, page_ids: &[u32]) -> Result<()> {
        // Sort page IDs for sequential access
        let mut sorted_ids = page_ids.to_vec();
        sorted_ids.sort_unstable();
        
        for &page_id in &sorted_ids {
            let _permit = self.acquire_io_permit().await?;
            
            let offset = page_id as u64 * PAGE_SIZE as u64;
            let mut buffer = vec![0u8; PAGE_SIZE];
            
            let file_guard = self.file.read().await;
            if let Some(file) = file_guard.as_ref() {
                let mut file = file.try_clone().await?;
                file.seek(SeekFrom::Start(offset)).await?;
                file.read_exact(&mut buffer).await?;
                
                // Verify but don't store (OS cache will keep it)
                let page = Page::from_bytes(page_id, buffer)?;
                page.verify_checksum()?;
            }
        }
        
        Ok(())
    }
    
    async fn close(&self) -> Result<()> {
        // Stop write coalescer if running
        if let Some(ref tx) = self.write_coalescer {
            drop(tx.clone()); // Drop sender to signal shutdown
        }
        
        // Close file
        let mut file_guard = self.file.write().await;
        if let Some(file) = file_guard.take() {
            file.sync_all().await?;
        }
        
        Ok(())
    }
}