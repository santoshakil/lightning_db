//! Connection limiting and management for Lightning DB

use crate::{Error, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::DashMap;
use tracing::{info, warn};

/// Connection information
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub id: u64,
    pub tenant_id: Option<String>,
    pub established_at: Instant,
    pub last_activity: Instant,
    pub request_count: u64,
}

/// Connection limiter
#[derive(Debug)]
pub struct ConnectionLimiter {
    // Limits
    max_connections: Option<usize>,
    max_idle_connections: Option<usize>,
    connection_timeout: Option<Duration>,
    
    // Current state
    active_connections: AtomicUsize,
    connections: Arc<DashMap<u64, ConnectionInfo>>,
    next_connection_id: AtomicUsize,
    
    // Per-tenant limits
    tenant_limits: Arc<DashMap<String, TenantConnectionLimit>>,
    tenant_connections: Arc<DashMap<String, AtomicUsize>>,
}

#[derive(Debug, Clone)]
struct TenantConnectionLimit {
    max_connections: usize,
    max_idle_connections: usize,
    priority: i32,
}

impl ConnectionLimiter {
    pub fn new(
        max_connections: Option<usize>,
        max_idle_connections: Option<usize>,
        connection_timeout: Option<Duration>,
    ) -> Result<Self> {
        Ok(Self {
            max_connections,
            max_idle_connections,
            connection_timeout,
            active_connections: AtomicUsize::new(0),
            connections: Arc::new(DashMap::new()),
            next_connection_id: AtomicUsize::new(1),
            tenant_limits: Arc::new(DashMap::new()),
            tenant_connections: Arc::new(DashMap::new()),
        })
    }
    
    /// Check if a new connection is allowed
    pub fn check_new_connection(&self, tenant_id: Option<&str>) -> Result<bool> {
        // Check global limit
        if let Some(max) = self.max_connections {
            let current = self.active_connections.load(Ordering::Relaxed);
            if current >= max {
                warn!("Global connection limit reached: {} >= {}", current, max);
                return Ok(false);
            }
        }
        
        // Check tenant-specific limit
        if let Some(tenant_id) = tenant_id {
            if let Some(limit) = self.tenant_limits.get(tenant_id) {
                let tenant_count = self.tenant_connections
                    .entry(tenant_id.to_string())
                    .or_insert_with(|| AtomicUsize::new(0));
                
                let current = tenant_count.load(Ordering::Relaxed);
                if current >= limit.max_connections {
                    warn!("Tenant {} connection limit reached: {} >= {}", 
                        tenant_id, current, limit.max_connections);
                    return Ok(false);
                }
            }
        }
        
        Ok(true)
    }
    
    /// Accept a new connection
    pub fn accept_connection(&self, tenant_id: Option<String>) -> Result<u64> {
        if !self.check_new_connection(tenant_id.as_deref())? {
            return Err(Error::QuotaExceeded("Connection limit exceeded".to_string()));
        }
        
        let conn_id = self.next_connection_id.fetch_add(1, Ordering::SeqCst) as u64;
        let now = Instant::now();
        
        let info = ConnectionInfo {
            id: conn_id,
            tenant_id: tenant_id.clone(),
            established_at: now,
            last_activity: now,
            request_count: 0,
        };
        
        self.connections.insert(conn_id, info);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        
        if let Some(tenant_id) = tenant_id {
            self.tenant_connections
                .entry(tenant_id)
                .or_insert_with(|| AtomicUsize::new(0))
                .fetch_add(1, Ordering::Relaxed);
        }
        
        info!("Accepted connection {}", conn_id);
        Ok(conn_id)
    }
    
    /// Close a connection
    pub fn close_connection(&self, conn_id: u64) -> Result<()> {
        if let Some((_, info)) = self.connections.remove(&conn_id) {
            self.active_connections.fetch_sub(1, Ordering::Relaxed);
            
            if let Some(tenant_id) = info.tenant_id {
                if let Some(tenant_count) = self.tenant_connections.get(&tenant_id) {
                    tenant_count.fetch_sub(1, Ordering::Relaxed);
                }
            }
            
            info!("Closed connection {}", conn_id);
        }
        
        Ok(())
    }
    
    /// Update connection activity
    pub fn update_activity(&self, conn_id: u64) -> Result<()> {
        if let Some(mut info) = self.connections.get_mut(&conn_id) {
            info.last_activity = Instant::now();
            info.request_count += 1;
        }
        Ok(())
    }
    
    /// Clean up idle connections
    pub fn cleanup_idle_connections(&self) -> Result<usize> {
        let now = Instant::now();
        let mut closed = 0;
        
        // Collect connections to close
        let to_close: Vec<u64> = self.connections
            .iter()
            .filter_map(|entry| {
                let info = entry.value();
                
                // Check timeout
                if let Some(timeout) = self.connection_timeout {
                    if now.duration_since(info.last_activity) > timeout {
                        return Some(info.id);
                    }
                }
                
                None
            })
            .collect();
        
        // Close collected connections
        for conn_id in to_close {
            self.close_connection(conn_id)?;
            closed += 1;
        }
        
        // Check idle connection limit
        if let Some(max_idle) = self.max_idle_connections {
            let idle_connections: Vec<(u64, Duration)> = self.connections
                .iter()
                .map(|entry| {
                    let info = entry.value();
                    (info.id, now.duration_since(info.last_activity))
                })
                .filter(|(_, idle_time)| idle_time > &Duration::from_secs(60))
                .collect();
            
            if idle_connections.len() > max_idle {
                // Close oldest idle connections
                let mut sorted = idle_connections;
                sorted.sort_by_key(|(_, idle_time)| std::cmp::Reverse(*idle_time));
                
                for (conn_id, _) in sorted.iter().skip(max_idle) {
                    self.close_connection(*conn_id)?;
                    closed += 1;
                }
            }
        }
        
        if closed > 0 {
            info!("Cleaned up {} idle connections", closed);
        }
        
        Ok(closed)
    }
    
    /// Set per-tenant connection limits
    pub fn set_tenant_limit(
        &self,
        tenant_id: String,
        max_connections: usize,
        max_idle_connections: usize,
        priority: i32,
    ) -> Result<()> {
        let limit = TenantConnectionLimit {
            max_connections,
            max_idle_connections,
            priority,
        };
        
        self.tenant_limits.insert(tenant_id.clone(), limit);
        self.tenant_connections.entry(tenant_id).or_insert_with(|| AtomicUsize::new(0));
        
        Ok(())
    }
    
    /// Get connection statistics
    pub fn get_stats(&self) -> ConnectionStats {
        let connections: Vec<ConnectionInfo> = self.connections
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        
        let tenant_stats: Vec<(String, usize)> = self.tenant_connections
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect();
        
        ConnectionStats {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            max_connections: self.max_connections,
            connections,
            tenant_stats,
        }
    }
}

#[derive(Debug)]
pub struct ConnectionStats {
    pub active_connections: usize,
    pub max_connections: Option<usize>,
    pub connections: Vec<ConnectionInfo>,
    pub tenant_stats: Vec<(String, usize)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_connection_limit() {
        let limiter = ConnectionLimiter::new(Some(2), None, None).unwrap();
        
        // Should accept up to limit
        let conn1 = limiter.accept_connection(None).unwrap();
        let _conn2 = limiter.accept_connection(None).unwrap();
        
        // Should reject beyond limit
        assert!(!limiter.check_new_connection(None).unwrap());
        assert!(limiter.accept_connection(None).is_err());
        
        // Should allow after closing
        limiter.close_connection(conn1).unwrap();
        assert!(limiter.check_new_connection(None).unwrap());
        let _conn3 = limiter.accept_connection(None).unwrap();
    }
    
    #[test]
    fn test_tenant_connection_limit() {
        let limiter = ConnectionLimiter::new(Some(10), None, None).unwrap();
        
        // Set tenant limit
        limiter.set_tenant_limit("tenant1".to_string(), 2, 1, 1).unwrap();
        
        // Should respect tenant limit
        let _conn1 = limiter.accept_connection(Some("tenant1".to_string())).unwrap();
        let _conn2 = limiter.accept_connection(Some("tenant1".to_string())).unwrap();
        
        // Should reject beyond tenant limit
        assert!(!limiter.check_new_connection(Some("tenant1")).unwrap());
        
        // Other tenants should still work
        assert!(limiter.check_new_connection(Some("tenant2")).unwrap());
    }
    
    #[test]
    fn test_idle_cleanup() {
        let limiter = ConnectionLimiter::new(
            None,
            Some(1),
            Some(Duration::from_millis(100))
        ).unwrap();
        
        // Create connections
        let conn1 = limiter.accept_connection(None).unwrap();
        let _conn2 = limiter.accept_connection(None).unwrap();
        
        // Update activity on conn1
        limiter.update_activity(conn1).unwrap();
        
        // Wait for timeout
        std::thread::sleep(Duration::from_millis(150));
        
        // Cleanup should remove conn2
        let cleaned = limiter.cleanup_idle_connections().unwrap();
        assert_eq!(cleaned, 1);
        
        let stats = limiter.get_stats();
        assert_eq!(stats.active_connections, 1);
    }
}