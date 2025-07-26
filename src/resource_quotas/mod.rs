//! Resource Quotas and Rate Limiting for Lightning DB
//!
//! This module provides comprehensive resource management including:
//! - Memory usage quotas
//! - Disk space limits
//! - Operation rate limiting
//! - Connection limits
//! - CPU usage throttling
//! - Per-user/tenant quotas

use crate::{Error, Result};
use std::sync::{Arc, RwLock, atomic::{AtomicU64, AtomicUsize, Ordering}};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime};
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use parking_lot::Mutex;
use tracing::{info, warn};

pub mod rate_limiter;
pub mod memory_quota;
pub mod disk_quota;
pub mod connection_limiter;
pub mod cpu_throttle;

/// Resource quota configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaConfig {
    /// Enable resource quotas
    pub enabled: bool,
    
    /// Memory quotas
    pub memory_quota_mb: Option<u64>,
    pub cache_quota_mb: Option<u64>,
    pub transaction_memory_limit_mb: Option<u64>,
    
    /// Disk quotas
    pub disk_quota_gb: Option<u64>,
    pub wal_size_limit_mb: Option<u64>,
    pub temp_space_limit_mb: Option<u64>,
    
    /// Rate limits (operations per second)
    pub read_ops_per_sec: Option<u64>,
    pub write_ops_per_sec: Option<u64>,
    pub scan_ops_per_sec: Option<u64>,
    pub transaction_ops_per_sec: Option<u64>,
    
    /// Connection limits
    pub max_connections: Option<usize>,
    pub max_idle_connections: Option<usize>,
    pub connection_timeout_sec: Option<u64>,
    
    /// CPU limits
    pub cpu_quota_percent: Option<f64>,
    pub background_cpu_percent: Option<f64>,
    
    /// Burst configuration
    pub allow_burst: bool,
    pub burst_multiplier: f64,
    pub burst_duration_sec: u64,
    
    /// Per-tenant quotas
    pub enable_tenant_quotas: bool,
    pub default_tenant_quota: Option<TenantQuota>,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            memory_quota_mb: None,
            cache_quota_mb: None,
            transaction_memory_limit_mb: Some(100),
            disk_quota_gb: None,
            wal_size_limit_mb: Some(1024),
            temp_space_limit_mb: Some(512),
            read_ops_per_sec: None,
            write_ops_per_sec: None,
            scan_ops_per_sec: None,
            transaction_ops_per_sec: None,
            max_connections: Some(1000),
            max_idle_connections: Some(100),
            connection_timeout_sec: Some(300),
            cpu_quota_percent: None,
            background_cpu_percent: Some(25.0),
            allow_burst: true,
            burst_multiplier: 2.0,
            burst_duration_sec: 10,
            enable_tenant_quotas: false,
            default_tenant_quota: None,
        }
    }
}

/// Per-tenant resource quota
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantQuota {
    pub tenant_id: String,
    pub memory_mb: u64,
    pub disk_gb: u64,
    pub read_ops_per_sec: u64,
    pub write_ops_per_sec: u64,
    pub max_connections: usize,
    pub priority: QuotaPriority,
}

/// Quota priority levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum QuotaPriority {
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

/// Resource usage statistics
#[derive(Debug, Clone, Serialize)]
pub struct ResourceUsage {
    pub memory_used_mb: u64,
    pub memory_quota_mb: Option<u64>,
    pub cache_used_mb: u64,
    pub cache_quota_mb: Option<u64>,
    pub disk_used_gb: f64,
    pub disk_quota_gb: Option<u64>,
    pub active_connections: usize,
    pub max_connections: Option<usize>,
    pub read_ops_current: u64,
    pub write_ops_current: u64,
    pub cpu_usage_percent: f64,
    pub violations: Vec<QuotaViolation>,
}

/// Quota violation information
#[derive(Debug, Clone, Serialize)]
pub struct QuotaViolation {
    pub timestamp: SystemTime,
    pub resource_type: ResourceType,
    pub requested: u64,
    pub limit: u64,
    pub tenant_id: Option<String>,
    pub action_taken: ViolationAction,
}

/// Resource types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Memory,
    Cache,
    Disk,
    WalSize,
    TempSpace,
    ReadOps,
    WriteOps,
    ScanOps,
    TransactionOps,
    Connections,
    CpuUsage,
}

/// Actions taken on quota violations
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ViolationAction {
    Rejected,
    Throttled,
    Queued,
    Warned,
}

/// Main resource quota manager
#[derive(Debug)]
pub struct QuotaManager {
    config: QuotaConfig,
    
    // Resource trackers
    memory_used: Arc<AtomicU64>,
    cache_used: Arc<AtomicU64>,
    disk_used: Arc<AtomicU64>,
    active_connections: Arc<AtomicUsize>,
    
    // Rate limiters
    rate_limiter: Arc<rate_limiter::RateLimiter>,
    
    // Memory quota manager
    memory_quota: Arc<memory_quota::MemoryQuotaManager>,
    
    // Disk quota manager
    disk_quota: Arc<disk_quota::DiskQuotaManager>,
    
    // Connection limiter
    connection_limiter: Arc<connection_limiter::ConnectionLimiter>,
    
    // CPU throttle
    cpu_throttle: Arc<cpu_throttle::CpuThrottle>,
    
    // Tenant quotas
    tenant_quotas: Arc<DashMap<String, TenantQuota>>,
    tenant_usage: Arc<DashMap<String, TenantResourceUsage>>,
    
    // Violation history
    violations: Arc<Mutex<VecDeque<QuotaViolation>>>,
    
    // Statistics
    stats: Arc<RwLock<QuotaStatistics>>,
}

/// Per-tenant resource usage
#[derive(Debug)]
struct TenantResourceUsage {
    memory_used: AtomicU64,
    disk_used: AtomicU64,
    read_ops: AtomicU64,
    write_ops: AtomicU64,
    connections: AtomicUsize,
    last_update: Mutex<Instant>,
}

impl Default for TenantResourceUsage {
    fn default() -> Self {
        Self {
            memory_used: AtomicU64::new(0),
            disk_used: AtomicU64::new(0),
            read_ops: AtomicU64::new(0),
            write_ops: AtomicU64::new(0),
            connections: AtomicUsize::new(0),
            last_update: Mutex::new(Instant::now()),
        }
    }
}

/// Quota statistics
#[derive(Debug)]
pub struct QuotaStatistics {
    total_requests: u64,
    accepted_requests: u64,
    rejected_requests: u64,
    throttled_requests: u64,
    quota_violations: HashMap<ResourceType, u64>,
    last_reset: Instant,
}

impl Default for QuotaStatistics {
    fn default() -> Self {
        Self {
            total_requests: 0,
            accepted_requests: 0,
            rejected_requests: 0,
            throttled_requests: 0,
            quota_violations: HashMap::new(),
            last_reset: Instant::now(),
        }
    }
}

impl QuotaManager {
    /// Create a new quota manager
    pub fn new(config: QuotaConfig) -> Result<Self> {
        if !config.enabled {
            return Ok(Self::disabled());
        }
        
        let rate_limiter = Arc::new(rate_limiter::RateLimiter::new(
            config.read_ops_per_sec,
            config.write_ops_per_sec,
            config.scan_ops_per_sec,
            config.transaction_ops_per_sec,
            config.allow_burst,
            config.burst_multiplier,
            Duration::from_secs(config.burst_duration_sec),
        )?);
        
        let memory_quota = Arc::new(memory_quota::MemoryQuotaManager::new(
            config.memory_quota_mb,
            config.cache_quota_mb,
            config.transaction_memory_limit_mb,
        )?);
        
        let disk_quota = Arc::new(disk_quota::DiskQuotaManager::new(
            config.disk_quota_gb,
            config.wal_size_limit_mb,
            config.temp_space_limit_mb,
        )?);
        
        let connection_limiter = Arc::new(connection_limiter::ConnectionLimiter::new(
            config.max_connections,
            config.max_idle_connections,
            config.connection_timeout_sec.map(Duration::from_secs),
        )?);
        
        let cpu_throttle = Arc::new(cpu_throttle::CpuThrottle::new(
            config.cpu_quota_percent,
            config.background_cpu_percent,
        )?);
        
        Ok(Self {
            config,
            memory_used: Arc::new(AtomicU64::new(0)),
            cache_used: Arc::new(AtomicU64::new(0)),
            disk_used: Arc::new(AtomicU64::new(0)),
            active_connections: Arc::new(AtomicUsize::new(0)),
            rate_limiter,
            memory_quota,
            disk_quota,
            connection_limiter,
            cpu_throttle,
            tenant_quotas: Arc::new(DashMap::new()),
            tenant_usage: Arc::new(DashMap::new()),
            violations: Arc::new(Mutex::new(VecDeque::with_capacity(1000))),
            stats: Arc::new(RwLock::new(QuotaStatistics::default())),
        })
    }
    
    /// Create a disabled quota manager
    pub fn disabled() -> Self {
        Self::new(QuotaConfig::default()).unwrap()
    }
    
    /// Check if a read operation is allowed
    pub fn check_read_allowed(&self, tenant_id: Option<&str>, size_bytes: u64) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        
        self.update_stats(|stats| stats.total_requests += 1);
        
        // Check rate limit
        if let Some(tenant_id) = tenant_id {
            if !self.rate_limiter.check_tenant_read(tenant_id)? {
                self.record_violation(ResourceType::ReadOps, 1, Some(tenant_id), ViolationAction::Throttled);
                return Err(Error::QuotaExceeded("Read rate limit exceeded".to_string()));
            }
        } else if !self.rate_limiter.check_read()? {
            self.record_violation(ResourceType::ReadOps, 1, None, ViolationAction::Throttled);
            return Err(Error::QuotaExceeded("Read rate limit exceeded".to_string()));
        }
        
        // Check memory quota for read buffer
        if !self.memory_quota.check_allocation(size_bytes)? {
            self.record_violation(ResourceType::Memory, size_bytes, tenant_id, ViolationAction::Rejected);
            return Err(Error::QuotaExceeded("Memory quota exceeded".to_string()));
        }
        
        self.update_stats(|stats| stats.accepted_requests += 1);
        Ok(())
    }
    
    /// Check if a write operation is allowed
    pub fn check_write_allowed(&self, tenant_id: Option<&str>, size_bytes: u64) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        
        self.update_stats(|stats| stats.total_requests += 1);
        
        // Check rate limit
        if let Some(tenant_id) = tenant_id {
            if !self.rate_limiter.check_tenant_write(tenant_id)? {
                self.record_violation(ResourceType::WriteOps, 1, Some(tenant_id), ViolationAction::Throttled);
                return Err(Error::QuotaExceeded("Write rate limit exceeded".to_string()));
            }
        } else if !self.rate_limiter.check_write()? {
            self.record_violation(ResourceType::WriteOps, 1, None, ViolationAction::Throttled);
            return Err(Error::QuotaExceeded("Write rate limit exceeded".to_string()));
        }
        
        // Check disk quota
        if !self.disk_quota.check_write(size_bytes)? {
            self.record_violation(ResourceType::Disk, size_bytes, tenant_id, ViolationAction::Rejected);
            return Err(Error::QuotaExceeded("Disk quota exceeded".to_string()));
        }
        
        // Check memory quota for write buffer
        if !self.memory_quota.check_allocation(size_bytes)? {
            self.record_violation(ResourceType::Memory, size_bytes, tenant_id, ViolationAction::Rejected);
            return Err(Error::QuotaExceeded("Memory quota exceeded".to_string()));
        }
        
        self.update_stats(|stats| stats.accepted_requests += 1);
        Ok(())
    }
    
    /// Check if a new connection is allowed
    pub fn check_connection_allowed(&self, tenant_id: Option<&str>) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        
        if !self.connection_limiter.check_new_connection(tenant_id)? {
            self.record_violation(ResourceType::Connections, 1, tenant_id, ViolationAction::Rejected);
            return Err(Error::QuotaExceeded("Connection limit exceeded".to_string()));
        }
        
        Ok(())
    }
    
    /// Record resource allocation
    pub fn record_allocation(&self, resource_type: ResourceType, amount: u64, tenant_id: Option<&str>) {
        if !self.config.enabled {
            return;
        }
        
        match resource_type {
            ResourceType::Memory => {
                self.memory_used.fetch_add(amount, Ordering::Relaxed);
                if let Some(tenant_id) = tenant_id {
                    if let Some(usage) = self.tenant_usage.get(tenant_id) {
                        usage.memory_used.fetch_add(amount, Ordering::Relaxed);
                    }
                }
            }
            ResourceType::Cache => {
                self.cache_used.fetch_add(amount, Ordering::Relaxed);
            }
            ResourceType::Disk => {
                self.disk_used.fetch_add(amount, Ordering::Relaxed);
                if let Some(tenant_id) = tenant_id {
                    if let Some(usage) = self.tenant_usage.get(tenant_id) {
                        usage.disk_used.fetch_add(amount, Ordering::Relaxed);
                    }
                }
            }
            _ => {}
        }
    }
    
    /// Record resource deallocation
    pub fn record_deallocation(&self, resource_type: ResourceType, amount: u64, tenant_id: Option<&str>) {
        if !self.config.enabled {
            return;
        }
        
        match resource_type {
            ResourceType::Memory => {
                self.memory_used.fetch_sub(amount, Ordering::Relaxed);
                if let Some(tenant_id) = tenant_id {
                    if let Some(usage) = self.tenant_usage.get(tenant_id) {
                        usage.memory_used.fetch_sub(amount, Ordering::Relaxed);
                    }
                }
            }
            ResourceType::Cache => {
                self.cache_used.fetch_sub(amount, Ordering::Relaxed);
            }
            ResourceType::Disk => {
                self.disk_used.fetch_sub(amount, Ordering::Relaxed);
                if let Some(tenant_id) = tenant_id {
                    if let Some(usage) = self.tenant_usage.get(tenant_id) {
                        usage.disk_used.fetch_sub(amount, Ordering::Relaxed);
                    }
                }
            }
            _ => {}
        }
    }
    
    /// Set tenant quota
    pub fn set_tenant_quota(&self, quota: TenantQuota) -> Result<()> {
        if !self.config.enable_tenant_quotas {
            return Err(Error::Config("Tenant quotas not enabled".to_string()));
        }
        
        let tenant_id = quota.tenant_id.clone();
        self.tenant_quotas.insert(tenant_id.clone(), quota);
        self.tenant_usage.entry(tenant_id.clone()).or_default();
        
        info!("Set quota for tenant: {}", tenant_id);
        Ok(())
    }
    
    /// Get current resource usage
    pub fn get_resource_usage(&self) -> ResourceUsage {
        let violations = self.violations.lock()
            .iter()
            .cloned()
            .collect();
        
        ResourceUsage {
            memory_used_mb: self.memory_used.load(Ordering::Relaxed) / (1024 * 1024),
            memory_quota_mb: self.config.memory_quota_mb,
            cache_used_mb: self.cache_used.load(Ordering::Relaxed) / (1024 * 1024),
            cache_quota_mb: self.config.cache_quota_mb,
            disk_used_gb: self.disk_used.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0 * 1024.0),
            disk_quota_gb: self.config.disk_quota_gb,
            active_connections: self.active_connections.load(Ordering::Relaxed),
            max_connections: self.config.max_connections,
            read_ops_current: self.rate_limiter.get_current_read_rate(),
            write_ops_current: self.rate_limiter.get_current_write_rate(),
            cpu_usage_percent: self.cpu_throttle.get_current_usage(),
            violations,
        }
    }
    
    /// Get quota statistics
    pub fn get_statistics(&self) -> QuotaStatistics {
        self.stats.read().unwrap().clone()
    }
    
    /// Reset statistics
    pub fn reset_statistics(&self) {
        let mut stats = self.stats.write().unwrap();
        *stats = QuotaStatistics {
            last_reset: Instant::now(),
            ..Default::default()
        };
    }
    
    // Helper methods
    
    fn record_violation(
        &self,
        resource_type: ResourceType,
        requested: u64,
        tenant_id: Option<&str>,
        action: ViolationAction,
    ) {
        let violation = QuotaViolation {
            timestamp: SystemTime::now(),
            resource_type,
            requested,
            limit: self.get_limit_for_resource(resource_type),
            tenant_id: tenant_id.map(String::from),
            action_taken: action,
        };
        
        let mut violations = self.violations.lock();
        violations.push_back(violation);
        
        // Keep only last 1000 violations
        while violations.len() > 1000 {
            violations.pop_front();
        }
        
        self.update_stats(|stats| {
            match action {
                ViolationAction::Rejected => stats.rejected_requests += 1,
                ViolationAction::Throttled => stats.throttled_requests += 1,
                _ => {}
            }
            *stats.quota_violations.entry(resource_type).or_insert(0) += 1;
        });
        
        warn!(
            "Quota violation: {:?} requested {} (limit: {}), action: {:?}",
            resource_type, requested, self.get_limit_for_resource(resource_type), action
        );
    }
    
    fn get_limit_for_resource(&self, resource_type: ResourceType) -> u64 {
        match resource_type {
            ResourceType::Memory => self.config.memory_quota_mb.unwrap_or(u64::MAX) * 1024 * 1024,
            ResourceType::Cache => self.config.cache_quota_mb.unwrap_or(u64::MAX) * 1024 * 1024,
            ResourceType::Disk => self.config.disk_quota_gb.unwrap_or(u64::MAX) * 1024 * 1024 * 1024,
            ResourceType::ReadOps => self.config.read_ops_per_sec.unwrap_or(u64::MAX),
            ResourceType::WriteOps => self.config.write_ops_per_sec.unwrap_or(u64::MAX),
            ResourceType::Connections => self.config.max_connections.unwrap_or(usize::MAX) as u64,
            _ => u64::MAX,
        }
    }
    
    fn update_stats<F>(&self, f: F)
    where
        F: FnOnce(&mut QuotaStatistics),
    {
        let mut stats = self.stats.write().unwrap();
        f(&mut *stats);
    }
}

impl Clone for QuotaStatistics {
    fn clone(&self) -> Self {
        Self {
            total_requests: self.total_requests,
            accepted_requests: self.accepted_requests,
            rejected_requests: self.rejected_requests,
            throttled_requests: self.throttled_requests,
            quota_violations: self.quota_violations.clone(),
            last_reset: self.last_reset,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_quota_config_default() {
        let config = QuotaConfig::default();
        assert!(!config.enabled);
        assert!(config.allow_burst);
        assert_eq!(config.burst_multiplier, 2.0);
    }
    
    #[test]
    fn test_disabled_quota_manager() {
        let manager = QuotaManager::disabled();
        
        // All operations should be allowed when disabled
        assert!(manager.check_read_allowed(None, 1024).is_ok());
        assert!(manager.check_write_allowed(None, 1024).is_ok());
        assert!(manager.check_connection_allowed(None).is_ok());
    }
}