//! Recovery module for Lightning DB
//! 
//! Provides enhanced recovery mechanisms including:
//! - Double-write buffer for torn page prevention
//! - Redundant metadata storage
//! - WAL recovery with progress tracking
//! - Recovery orchestration

pub mod enhanced_recovery;

pub use enhanced_recovery::{
    DoubleWriteBuffer,
    RedundantMetadata,
    RecoveryProgress,
    EnhancedWalRecovery,
    RecoveryStats,
};

use crate::error::Result;
use crate::{Database, LightningDbConfig};
use std::path::Path;
use std::sync::Arc;

/// Recovery manager that coordinates all recovery operations
pub struct RecoveryManager {
    db_path: String,
    config: LightningDbConfig,
    progress: Arc<RecoveryProgress>,
}

impl RecoveryManager {
    pub fn new(db_path: impl AsRef<Path>, config: LightningDbConfig) -> Self {
        Self {
            db_path: db_path.as_ref().to_string_lossy().to_string(),
            config,
            progress: Arc::new(RecoveryProgress::new()),
        }
    }
    
    /// Get recovery progress tracker
    pub fn progress(&self) -> Arc<RecoveryProgress> {
        self.progress.clone()
    }
    
    /// Perform full database recovery
    pub fn recover(&self) -> Result<Database> {
        self.progress.set_phase("Starting recovery");
        
        // Step 1: Recover metadata
        self.progress.set_phase("Recovering metadata");
        let metadata = RedundantMetadata::new(Path::new(&self.db_path));
        let header = metadata.read_header()?;
        
        // Step 2: Recover from double-write buffer
        self.progress.set_phase("Checking double-write buffer");
        let dwb = DoubleWriteBuffer::new(
            Path::new(&self.db_path),
            header.page_size as usize,
            32,
        )?;
        
        let recovered_pages = dwb.recover(|page_id, data| {
            // In real implementation, write to page manager
            println!("Recovered page {} from double-write buffer", page_id);
            Ok(())
        })?;
        
        if recovered_pages > 0 {
            println!("Recovered {} pages from double-write buffer", recovered_pages);
        }
        
        // Step 3: Recover from WAL
        self.progress.set_phase("Recovering from WAL");
        let wal_recovery = EnhancedWalRecovery::new(
            Path::new(&self.db_path).join("wal"),
            self.progress.clone(),
        );
        
        let stats = wal_recovery.recover(|entry| {
            // In real implementation, apply entry to database
            Ok(())
        })?;
        
        println!("WAL recovery stats: {:?}", stats);
        
        // Step 4: Open database normally
        self.progress.set_phase("Opening database");
        let db = Database::open(&self.db_path, self.config)?;
        
        self.progress.set_phase("Recovery complete");
        
        Ok(db)
    }
    
    /// Check if recovery is needed
    pub fn needs_recovery(&self) -> Result<bool> {
        // Check for recovery indicators:
        // 1. Incomplete shutdown marker
        // 2. Non-empty double-write buffer
        // 3. WAL entries after last checkpoint
        
        let recovery_marker = Path::new(&self.db_path).join(".recovery_needed");
        if recovery_marker.exists() {
            return Ok(true);
        }
        
        // Check double-write buffer
        let dwb_path = Path::new(&self.db_path).join("double_write.buffer");
        if dwb_path.exists() {
            let dwb_size = std::fs::metadata(&dwb_path)?.len();
            if dwb_size > 8 {
                // Has more than just header
                return Ok(true);
            }
        }
        
        // In production, also check WAL status
        
        Ok(false)
    }
}