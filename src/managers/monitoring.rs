use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;
use crate::{Database, Result, DatabaseStats, StorageStats, CacheStatsInfo, 
            TransactionStats, PerformanceStats};
use crate::features::statistics::{MetricsSnapshot, MetricsReporter, MetricsCollector};
use crate::performance::prefetch::PrefetchStatistics;
use crate::features::monitoring::production_hooks::{HealthStatus, MonitoringEvent, OperationType, MonitoringHook};
use crate::core::lsm::DeltaCompressionStats;

pub struct MonitoringManager {
    db: Arc<Database>,
}

impl MonitoringManager {
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
    
    pub fn stats(&self) -> DatabaseStats {
        self.db.stats()
    }
    
    pub fn get_stats(&self) -> Result<DatabaseStats> {
        self.db.get_stats()
    }
    
    pub fn get_performance_stats(&self) -> Result<PerformanceStats> {
        self.db.get_performance_stats()
    }
    
    pub fn get_storage_stats(&self) -> Result<StorageStats> {
        self.db.get_storage_stats()
    }
    
    pub fn get_cache_stats(&self) -> Result<CacheStatsInfo> {
        self.db.get_cache_stats()
    }
    
    pub fn cache_stats(&self) -> Option<String> {
        self.db.cache_stats()
    }
    
    pub fn get_transaction_stats(&self) -> Result<TransactionStats> {
        self.db.get_transaction_stats()
    }
    
    pub fn get_metrics(&self) -> MetricsSnapshot {
        self.db.get_metrics()
    }
    
    pub fn get_metrics_reporter(&self) -> MetricsReporter {
        self.db.get_metrics_reporter()
    }
    
    pub fn metrics_collector(&self) -> Arc<MetricsCollector> {
        self.db.metrics_collector()
    }
    
    pub fn lsm_stats(&self) -> Option<crate::core::lsm::LSMStats> {
        self.db.lsm_stats()
    }
    
    pub fn delta_compression_stats(&self) -> Option<DeltaCompressionStats> {
        self.db.delta_compression_stats()
    }
    
    pub fn get_prefetch_statistics(&self) -> Option<PrefetchStatistics> {
        self.db.get_prefetch_statistics()
    }
    
    pub fn get_production_metrics(&self) -> HashMap<String, f64> {
        self.db.get_production_metrics()
    }
    
    pub fn production_health_check(&self) -> HealthStatus {
        self.db.production_health_check()
    }
    
    pub fn register_monitoring_hook(&self, hook: Arc<dyn MonitoringHook>) -> Result<()> {
        self.db.register_monitoring_hook(hook);
        Ok(())
    }
    
    pub fn set_operation_threshold(&self, operation: OperationType, threshold: Duration) -> Result<()> {
        self.db.set_operation_threshold(operation, threshold);
        Ok(())
    }
    
    pub fn emit_monitoring_event(&self, event: MonitoringEvent) {
        self.db.emit_monitoring_event(event)
    }
    
    pub fn get_encryption_stats(&self) -> Option<crate::features::encryption::EncryptionStats> {
        self.db.get_encryption_stats()
    }
    
    pub fn get_fragmentation_stats(&self) -> Result<HashMap<String, f64>> {
        self.db.get_fragmentation_stats()
    }
}