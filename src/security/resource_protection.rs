use crate::security::{SecurityError, SecurityResult};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{error, warn};

#[derive(Debug, Clone)]
pub struct ResourceQuotas {
    pub max_memory_mb: u64,
    pub max_disk_gb: u64,
    pub max_cpu_percent: f64,
    pub max_connections: usize,
    pub max_queries_per_second: u32,
    pub max_query_duration_ms: u64,
    pub max_result_size_mb: u64,
    pub max_concurrent_transactions: usize,
}

impl Default for ResourceQuotas {
    fn default() -> Self {
        Self {
            max_memory_mb: 1024,
            max_disk_gb: 100,
            max_cpu_percent: 80.0,
            max_connections: 1000,
            max_queries_per_second: 1000,
            max_query_duration_ms: 30000,
            max_result_size_mb: 100,
            max_concurrent_transactions: 100,
        }
    }
}

pub struct ResourceProtectionManager {
    quotas: ResourceQuotas,
    memory_tracker: Arc<RwLock<MemoryTracker>>,
    connection_semaphore: Arc<Semaphore>,
    transaction_semaphore: Arc<Semaphore>,
    query_rate_limiter: Arc<RwLock<QueryRateLimiter>>,
    active_queries: Arc<RwLock<HashMap<String, QueryExecution>>>,
    resource_monitor: Arc<RwLock<ResourceMonitor>>,
}

struct MemoryTracker {
    allocated_mb: u64,
    peak_usage_mb: u64,
    allocations: HashMap<String, MemoryAllocation>,
}

struct MemoryAllocation {
    size_mb: u64,
    allocated_at: Instant,
    context: String,
}

struct QueryRateLimiter {
    queries_per_second: u32,
    query_timestamps: Vec<Instant>,
    window_duration: Duration,
}

struct QueryExecution {
    id: String,
    started_at: Instant,
    timeout_at: Instant,
    memory_allocated_mb: u64,
}

struct ResourceMonitor {
    memory_usage_history: Vec<(Instant, u64)>,
    cpu_usage_history: Vec<(Instant, f64)>,
    connection_count_history: Vec<(Instant, usize)>,
    last_cleanup: Instant,
    cleanup_interval: Duration,
}

impl ResourceProtectionManager {
    pub fn new(quotas: ResourceQuotas) -> Self {
        let connection_semaphore = Arc::new(Semaphore::new(quotas.max_connections));
        let transaction_semaphore = Arc::new(Semaphore::new(quotas.max_concurrent_transactions));
        
        let memory_tracker = MemoryTracker {
            allocated_mb: 0,
            peak_usage_mb: 0,
            allocations: HashMap::new(),
        };
        
        let query_rate_limiter = QueryRateLimiter {
            queries_per_second: quotas.max_queries_per_second,
            query_timestamps: Vec::new(),
            window_duration: Duration::from_secs(1),
        };
        
        let resource_monitor = ResourceMonitor {
            memory_usage_history: Vec::new(),
            cpu_usage_history: Vec::new(),
            connection_count_history: Vec::new(),
            last_cleanup: Instant::now(),
            cleanup_interval: Duration::from_secs(300),
        };

        Self {
            quotas,
            memory_tracker: Arc::new(RwLock::new(memory_tracker)),
            connection_semaphore,
            transaction_semaphore,
            query_rate_limiter: Arc::new(RwLock::new(query_rate_limiter)),
            active_queries: Arc::new(RwLock::new(HashMap::new())),
            resource_monitor: Arc::new(RwLock::new(resource_monitor)),
        }
    }

    pub async fn acquire_connection(&self) -> SecurityResult<ConnectionGuard<'_>> {
        let permit = self.connection_semaphore.acquire().await
            .map_err(|_| SecurityError::ResourceQuotaExceeded("Connection semaphore closed".to_string()))?;

        Ok(ConnectionGuard {
            _permit: permit,
            manager: self.clone(),
        })
    }

    pub async fn acquire_transaction(&self) -> SecurityResult<TransactionGuard<'_>> {
        let permit = self.transaction_semaphore.acquire().await
            .map_err(|_| SecurityError::ResourceQuotaExceeded("Transaction semaphore closed".to_string()))?;

        Ok(TransactionGuard {
            _permit: permit,
            manager: self.clone(),
        })
    }

    pub fn allocate_memory(&self, size_mb: u64, context: String) -> SecurityResult<MemoryGuard> {
        let mut tracker = self.memory_tracker.write().unwrap();
        
        if tracker.allocated_mb + size_mb > self.quotas.max_memory_mb {
            return Err(SecurityError::ResourceQuotaExceeded(
                format!("Memory allocation would exceed quota: {}MB requested, {}MB available",
                        size_mb, self.quotas.max_memory_mb - tracker.allocated_mb)
            ));
        }

        let allocation_id = uuid::Uuid::new_v4().to_string();
        let allocation = MemoryAllocation {
            size_mb,
            allocated_at: Instant::now(),
            context: context.clone(),
        };

        tracker.allocations.insert(allocation_id.clone(), allocation);
        tracker.allocated_mb += size_mb;
        
        if tracker.allocated_mb > tracker.peak_usage_mb {
            tracker.peak_usage_mb = tracker.allocated_mb;
        }

        Ok(MemoryGuard {
            allocation_id,
            size_mb,
            manager: self.clone(),
        })
    }

    pub fn check_query_rate_limit(&self) -> SecurityResult<()> {
        let mut limiter = self.query_rate_limiter.write().unwrap();
        let now = Instant::now();
        
        let window_duration = limiter.window_duration;
        limiter.query_timestamps.retain(|&timestamp| {
            now.duration_since(timestamp) <= window_duration
        });
        
        if limiter.query_timestamps.len() >= limiter.queries_per_second as usize {
            return Err(SecurityError::RateLimitExceeded(
                format!("Query rate limit exceeded: {} queries per second", limiter.queries_per_second)
            ));
        }
        
        limiter.query_timestamps.push(now);
        Ok(())
    }

    pub fn start_query(&self, query_id: String, memory_mb: u64) -> SecurityResult<QueryGuard> {
        self.check_query_rate_limit()?;
        
        let now = Instant::now();
        let timeout_at = now + Duration::from_millis(self.quotas.max_query_duration_ms);
        
        let execution = QueryExecution {
            id: query_id.clone(),
            started_at: now,
            timeout_at,
            memory_allocated_mb: memory_mb,
        };
        
        let mut queries = self.active_queries.write().unwrap();
        queries.insert(query_id.clone(), execution);
        
        Ok(QueryGuard {
            query_id,
            manager: self.clone(),
        })
    }

    pub fn check_query_timeout(&self, query_id: &str) -> SecurityResult<()> {
        let queries = self.active_queries.read().unwrap();
        if let Some(execution) = queries.get(query_id) {
            if Instant::now() > execution.timeout_at {
                return Err(SecurityError::ResourceQuotaExceeded(
                    format!("Query {} exceeded timeout limit", query_id)
                ));
            }
        }
        Ok(())
    }

    pub fn validate_result_size(&self, size_mb: u64) -> SecurityResult<()> {
        if size_mb > self.quotas.max_result_size_mb {
            return Err(SecurityError::ResourceQuotaExceeded(
                format!("Result size {}MB exceeds limit of {}MB", size_mb, self.quotas.max_result_size_mb)
            ));
        }
        Ok(())
    }

    pub fn get_resource_usage(&self) -> ResourceUsage {
        let memory_tracker = self.memory_tracker.read().unwrap();
        let queries = self.active_queries.read().unwrap();
        
        ResourceUsage {
            memory_used_mb: memory_tracker.allocated_mb,
            memory_peak_mb: memory_tracker.peak_usage_mb,
            memory_limit_mb: self.quotas.max_memory_mb,
            active_connections: self.quotas.max_connections - self.connection_semaphore.available_permits(),
            max_connections: self.quotas.max_connections,
            active_transactions: self.quotas.max_concurrent_transactions - self.transaction_semaphore.available_permits(),
            max_transactions: self.quotas.max_concurrent_transactions,
            active_queries: queries.len(),
            memory_allocations: memory_tracker.allocations.len(),
        }
    }

    pub fn cleanup_expired_resources(&self) {
        let mut monitor = self.resource_monitor.write().unwrap();
        let now = Instant::now();
        
        if now.duration_since(monitor.last_cleanup) < monitor.cleanup_interval {
            return;
        }
        
        {
            let mut queries = self.active_queries.write().unwrap();
            let expired_queries: Vec<String> = queries.iter()
                .filter(|(_, execution)| now > execution.timeout_at)
                .map(|(id, _)| id.clone())
                .collect();
            
            for query_id in expired_queries {
                queries.remove(&query_id);
                warn!("Removed expired query: {}", query_id);
            }
        }
        
        {
            let mut memory_tracker = self.memory_tracker.write().unwrap();
            let stale_threshold = Duration::from_secs(3600);
            let stale_allocations: Vec<String> = memory_tracker.allocations.iter()
                .filter(|(_, allocation)| now.duration_since(allocation.allocated_at) > stale_threshold)
                .map(|(id, _)| id.clone())
                .collect();
            
            for allocation_id in stale_allocations {
                if let Some(allocation) = memory_tracker.allocations.remove(&allocation_id) {
                    memory_tracker.allocated_mb = memory_tracker.allocated_mb.saturating_sub(allocation.size_mb);
                    warn!("Cleaned up stale memory allocation: {} ({}MB)", allocation_id, allocation.size_mb);
                }
            }
        }
        
        monitor.last_cleanup = now;
    }

    pub fn enforce_emergency_limits(&self) -> SecurityResult<()> {
        let usage = self.get_resource_usage();
        
        if usage.memory_used_mb as f64 / usage.memory_limit_mb as f64 > 0.95 {
            error!("Emergency: Memory usage at {}%, enforcing strict limits", 
                   (usage.memory_used_mb as f64 / usage.memory_limit_mb as f64) * 100.0);
            
            let mut queries = self.active_queries.write().unwrap();
            let half_queries: Vec<String> = queries.keys().take(queries.len() / 2).cloned().collect();
            
            for query_id in half_queries {
                queries.remove(&query_id);
            }
            
            return Err(SecurityError::ResourceQuotaExceeded("Emergency resource protection activated".to_string()));
        }
        
        Ok(())
    }

    fn deallocate_memory(&self, allocation_id: &str, size_mb: u64) {
        let mut tracker = self.memory_tracker.write().unwrap();
        if tracker.allocations.remove(allocation_id).is_some() {
            tracker.allocated_mb = tracker.allocated_mb.saturating_sub(size_mb);
        }
    }

    fn finish_query(&self, query_id: &str) {
        let mut queries = self.active_queries.write().unwrap();
        queries.remove(query_id);
    }
}

pub struct ConnectionGuard<'a> {
    _permit: tokio::sync::SemaphorePermit<'a>,
    manager: ResourceProtectionManager,
}

pub struct TransactionGuard<'a> {
    _permit: tokio::sync::SemaphorePermit<'a>,
    manager: ResourceProtectionManager,
}

pub struct MemoryGuard {
    allocation_id: String,
    size_mb: u64,
    manager: ResourceProtectionManager,
}

impl Drop for MemoryGuard {
    fn drop(&mut self) {
        self.manager.deallocate_memory(&self.allocation_id, self.size_mb);
    }
}

pub struct QueryGuard {
    query_id: String,
    manager: ResourceProtectionManager,
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        self.manager.finish_query(&self.query_id);
    }
}

#[derive(Debug, Clone)]
pub struct ResourceUsage {
    pub memory_used_mb: u64,
    pub memory_peak_mb: u64,
    pub memory_limit_mb: u64,
    pub active_connections: usize,
    pub max_connections: usize,
    pub active_transactions: usize,
    pub max_transactions: usize,
    pub active_queries: usize,
    pub memory_allocations: usize,
}

impl Clone for ResourceProtectionManager {
    fn clone(&self) -> Self {
        Self {
            quotas: self.quotas.clone(),
            memory_tracker: Arc::clone(&self.memory_tracker),
            connection_semaphore: Arc::clone(&self.connection_semaphore),
            transaction_semaphore: Arc::clone(&self.transaction_semaphore),
            query_rate_limiter: Arc::clone(&self.query_rate_limiter),
            active_queries: Arc::clone(&self.active_queries),
            resource_monitor: Arc::clone(&self.resource_monitor),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_connection_limiting() {
        let quotas = ResourceQuotas {
            max_connections: 2,
            ..Default::default()
        };
        let manager = ResourceProtectionManager::new(quotas);
        
        let _conn1 = manager.acquire_connection().await.unwrap();
        let _conn2 = manager.acquire_connection().await.unwrap();
        
        let usage = manager.get_resource_usage();
        assert_eq!(usage.active_connections, 2);
        assert_eq!(usage.max_connections, 2);
    }

    #[test]
    fn test_memory_allocation() {
        let quotas = ResourceQuotas {
            max_memory_mb: 100,
            ..Default::default()
        };
        let manager = ResourceProtectionManager::new(quotas);
        
        let _guard1 = manager.allocate_memory(50, "test1".to_string()).unwrap();
        let _guard2 = manager.allocate_memory(30, "test2".to_string()).unwrap();
        
        let result = manager.allocate_memory(30, "test3".to_string());
        assert!(result.is_err());
        
        let usage = manager.get_resource_usage();
        assert_eq!(usage.memory_used_mb, 80);
    }

    #[test]
    fn test_query_rate_limiting() {
        let quotas = ResourceQuotas {
            max_queries_per_second: 2,
            ..Default::default()
        };
        let manager = ResourceProtectionManager::new(quotas);
        
        assert!(manager.check_query_rate_limit().is_ok());
        assert!(manager.check_query_rate_limit().is_ok());
        assert!(manager.check_query_rate_limit().is_err());
    }

    #[test]
    fn test_query_execution() {
        let quotas = ResourceQuotas {
            max_queries_per_second: 100,
            max_query_duration_ms: 1000,
            ..Default::default()
        };
        let manager = ResourceProtectionManager::new(quotas);
        
        let query_id = "test_query".to_string();
        let _guard = manager.start_query(query_id.clone(), 10).unwrap();
        
        assert!(manager.check_query_timeout(&query_id).is_ok());
        
        let usage = manager.get_resource_usage();
        assert_eq!(usage.active_queries, 1);
    }
}