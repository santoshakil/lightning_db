use crate::core::error::{Error, Result};
use super::{CompactionConfig, CompactionProgress, CompactionState, CompactionType};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{RwLock, Semaphore};
use std::collections::HashMap;

#[derive(Debug)]
pub struct OnlineCompactor {
    config: Arc<RwLock<CompactionConfig>>,
    active_compactions: Arc<RwLock<HashMap<u64, CompactionContext>>>,
    concurrency_limiter: Arc<Semaphore>,
}

#[derive(Debug)]
struct CompactionContext {
    compaction_id: u64,
    started_at: Instant,
    progress: Arc<RwLock<CompactionProgress>>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl OnlineCompactor {
    pub fn new(config: Arc<RwLock<CompactionConfig>>) -> Result<Self> {
        let max_concurrent = {
            let cfg = futures::executor::block_on(config.read());
            cfg.max_concurrent_compactions
        };
        
        Ok(Self {
            config,
            active_compactions: Arc::new(RwLock::new(HashMap::new())),
            concurrency_limiter: Arc::new(Semaphore::new(max_concurrent)),
        })
    }
    
    pub async fn compact(&self, compaction_id: u64) -> Result<u64> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| Error::ResourceExhausted { resource: "Too many concurrent compactions".into() })?;
        
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let progress = Arc::new(RwLock::new(CompactionProgress {
            compaction_id,
            compaction_type: CompactionType::Online,
            state: CompactionState::Running,
            progress_pct: 0.0,
            bytes_processed: 0,
            bytes_total: 0,
            items_processed: 0,
            items_total: 0,
            started_at: Instant::now(),
            estimated_completion: None,
            error: None,
        }));
        
        let context = CompactionContext {
            compaction_id,
            started_at: Instant::now(),
            progress: progress.clone(),
            cancel_token: cancel_token.clone(),
        };
        
        {
            let mut active = self.active_compactions.write().await;
            active.insert(compaction_id, context);
        }
        
        let result = self.perform_online_compaction(compaction_id, progress.clone(), cancel_token).await;
        
        // Clean up
        {
            let mut active = self.active_compactions.write().await;
            active.remove(&compaction_id);
        }
        
        result
    }
    
    pub async fn minor_compact(&self, compaction_id: u64) -> Result<u64> {
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| Error::ResourceExhausted { resource: "Too many concurrent compactions".into() })?;
        
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let progress = Arc::new(RwLock::new(CompactionProgress {
            compaction_id,
            compaction_type: CompactionType::Minor,
            state: CompactionState::Running,
            progress_pct: 0.0,
            bytes_processed: 0,
            bytes_total: 0,
            items_processed: 0,
            items_total: 0,
            started_at: Instant::now(),
            estimated_completion: None,
            error: None,
        }));
        
        let context = CompactionContext {
            compaction_id,
            started_at: Instant::now(),
            progress: progress.clone(),
            cancel_token: cancel_token.clone(),
        };
        
        {
            let mut active = self.active_compactions.write().await;
            active.insert(compaction_id, context);
        }
        
        let result = self.perform_minor_compaction(compaction_id, progress.clone(), cancel_token).await;
        
        // Clean up
        {
            let mut active = self.active_compactions.write().await;
            active.remove(&compaction_id);
        }
        
        result
    }
    
    pub async fn cancel(&self, compaction_id: u64) -> Result<()> {
        let active = self.active_compactions.read().await;
        if let Some(context) = active.get(&compaction_id) {
            context.cancel_token.cancel();
            
            let mut progress = context.progress.write().await;
            progress.state = CompactionState::Cancelled;
            
            Ok(())
        } else {
            Err(Error::Generic(format!("Compaction {} not found", compaction_id)))
        }
    }
    
    async fn perform_online_compaction(
        &self,
        _compaction_id: u64,
        progress: Arc<RwLock<CompactionProgress>>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<u64> {
        let config = self.config.read().await;
        let batch_size = config.online_compaction_batch_size;
        drop(config);
        
        // Simulate compaction phases
        let phases = vec![
            ("Analyzing fragmentation", 10),
            ("Identifying candidates", 20),
            ("Compacting pages", 60),
            ("Updating indexes", 10),
        ];
        
        let mut total_processed = 0u64;
        let mut bytes_reclaimed = 0u64;
        
        for (phase_name, phase_weight) in phases {
            if cancel_token.is_cancelled() {
                return Err(Error::Cancelled);
            }
            
            // Update progress
            {
                let mut p = progress.write().await;
                p.progress_pct = total_processed as f64;
            }
            
            match phase_name {
                "Analyzing fragmentation" => {
                    bytes_reclaimed += self.analyze_fragmentation(&cancel_token).await?;
                },
                "Identifying candidates" => {
                    bytes_reclaimed += self.identify_compaction_candidates(&cancel_token).await?;
                },
                "Compacting pages" => {
                    let result = self.compact_pages_online(batch_size, &progress, &cancel_token).await?;
                    bytes_reclaimed += result;
                },
                "Updating indexes" => {
                    bytes_reclaimed += self.update_indexes_online(&cancel_token).await?;
                },
                _ => {}
            }
            
            total_processed += phase_weight;
            
            // Small delay to allow other operations
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
        
        // Final progress update
        {
            let mut p = progress.write().await;
            p.state = CompactionState::Complete;
            p.progress_pct = 100.0;
            p.bytes_processed = bytes_reclaimed;
        }
        
        Ok(bytes_reclaimed)
    }
    
    async fn perform_minor_compaction(
        &self,
        _compaction_id: u64,
        progress: Arc<RwLock<CompactionProgress>>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<u64> {
        let config = self.config.read().await;
        let batch_size = config.online_compaction_batch_size / 2; // Smaller batches for minor compaction
        drop(config);
        
        if cancel_token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        
        // Minor compaction focuses on recent changes and hot pages
        let mut bytes_reclaimed = 0u64;
        
        // Phase 1: Compact recent writes (50% of work)
        {
            let mut p = progress.write().await;
            p.progress_pct = 0.0;
        }
        
        bytes_reclaimed += self.compact_recent_writes(batch_size, &cancel_token).await?;
        
        {
            let mut p = progress.write().await;
            p.progress_pct = 50.0;
        }
        
        if cancel_token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        
        // Phase 2: Merge adjacent pages (50% of work)
        bytes_reclaimed += self.merge_adjacent_pages(batch_size, &cancel_token).await?;
        
        {
            let mut p = progress.write().await;
            p.state = CompactionState::Complete;
            p.progress_pct = 100.0;
            p.bytes_processed = bytes_reclaimed;
        }
        
        Ok(bytes_reclaimed)
    }
    
    async fn analyze_fragmentation(&self, cancel_token: &tokio_util::sync::CancellationToken) -> Result<u64> {
        if cancel_token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        
        // Integration point for actual page manager to analyze fragmentation
        // For now, simulate the analysis
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        // Return estimated bytes that can be reclaimed
        Ok(1024 * 1024) // 1MB simulation
    }
    
    async fn identify_compaction_candidates(&self, cancel_token: &tokio_util::sync::CancellationToken) -> Result<u64> {
        if cancel_token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        
        // Integration point for actual storage layers to identify fragmented pages/regions
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        
        // Return estimated bytes from candidate identification
        Ok(2 * 1024 * 1024) // 2MB simulation
    }
    
    async fn compact_pages_online(
        &self,
        batch_size: usize,
        progress: &Arc<RwLock<CompactionProgress>>,
        cancel_token: &tokio_util::sync::CancellationToken,
    ) -> Result<u64> {
        let mut total_reclaimed = 0u64;
        let estimated_batches = 1000; // Simulate 1000 batches
        
        for batch_idx in 0..estimated_batches {
            if cancel_token.is_cancelled() {
                return Err(Error::Cancelled);
            }
            
            // Process a batch of pages
            let batch_reclaimed = self.compact_page_batch(batch_size).await?;
            total_reclaimed += batch_reclaimed;
            
            // Update progress within the compacting phase (60% of total work)
            let phase_progress = (batch_idx + 1) as f64 / estimated_batches as f64 * 60.0;
            {
                let mut p = progress.write().await;
                p.progress_pct = 30.0 + phase_progress; // 30% from previous phases
                p.bytes_processed = total_reclaimed;
                p.items_processed = (batch_idx + 1) as u64 * batch_size as u64;
                p.items_total = estimated_batches as u64 * batch_size as u64;
            }
            
            // Yield control to allow other operations
            if batch_idx % 10 == 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        }
        
        Ok(total_reclaimed)
    }
    
    async fn compact_page_batch(&self, batch_size: usize) -> Result<u64> {
        // Integration point for actual B+tree page compaction
        // Simulate processing a batch of pages
        let _pages_in_batch = batch_size;
        
        // Simulate compaction work
        tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
        
        // Return bytes reclaimed from this batch
        Ok(4096) // Simulate 4KB reclaimed per batch
    }
    
    async fn update_indexes_online(&self, cancel_token: &tokio_util::sync::CancellationToken) -> Result<u64> {
        if cancel_token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        
        // Integration point for actual index manager to update affected indexes
        tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;
        
        // Return bytes reclaimed from index updates
        Ok(512 * 1024) // 512KB simulation
    }
    
    async fn compact_recent_writes(&self, batch_size: usize, cancel_token: &tokio_util::sync::CancellationToken) -> Result<u64> {
        if cancel_token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        
        // Implementation pending WAL integration
        let _batch_size = batch_size;
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
        
        Ok(1024 * 1024) // 1MB simulation
    }
    
    async fn merge_adjacent_pages(&self, batch_size: usize, cancel_token: &tokio_util::sync::CancellationToken) -> Result<u64> {
        if cancel_token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        
        // Implementation pending page manager integration
        let _batch_size = batch_size;
        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
        
        Ok(2 * 1024 * 1024) // 2MB simulation
    }
    
    pub async fn get_active_compactions(&self) -> Vec<u64> {
        let active = self.active_compactions.read().await;
        active.keys().cloned().collect()
    }
    
    pub async fn get_compaction_progress(&self, compaction_id: u64) -> Option<CompactionProgress> {
        let active = self.active_compactions.read().await;
        if let Some(context) = active.get(&compaction_id) {
            let progress = context.progress.read().await;
            Some(progress.clone())
        } else {
            None
        }
    }
}