use std::sync::Arc;
use crate::{Database, Result};

pub struct MaintenanceManager {
    db: Arc<Database>,
}

impl MaintenanceManager {
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
    
    pub fn compact(&self) -> Result<u64> {
        self.db.compact()
    }
    
    pub fn compact_async(&self) -> Result<u64> {
        self.db.compact_async()
    }
    
    pub fn compact_major(&self) -> Result<u64> {
        self.db.compact_major()
    }
    
    pub fn compact_incremental(&self) -> Result<u64> {
        self.db.compact_incremental()
    }
    
    pub fn set_auto_compaction(&self, enabled: bool, interval_secs: Option<u64>) -> Result<()> {
        self.db.set_auto_compaction(enabled, interval_secs)
    }
    
    pub fn garbage_collect(&self) -> Result<u64> {
        self.db.garbage_collect()
    }
    
    pub fn start_background_maintenance(&self) -> Result<()> {
        self.db.start_background_maintenance()
    }
    
    pub fn stop_background_maintenance(&self) -> Result<()> {
        self.db.stop_background_maintenance()
    }
    
    pub fn get_compaction_report(&self) -> Result<String> {
        self.db.get_compaction_report()
    }
    
    pub fn flush_lsm(&self) -> Result<()> {
        self.db.flush_lsm()
    }
    
    pub fn flush_write_buffer(&self) -> Result<()> {
        self.db.flush_write_buffer()
    }
    
    pub fn compact_lsm(&self) -> Result<()> {
        self.db.compact_lsm()
    }
    
    pub fn cleanup_old_versions(&self, before_timestamp: u64) {
        self.db.cleanup_old_versions(before_timestamp)
    }
    
    pub fn start_version_cleanup(db: Arc<Database>) -> Arc<Database> {
        Database::start_version_cleanup(db)
    }
}