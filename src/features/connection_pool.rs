//! Connection pooling for Lightning DB
//! 
//! Provides efficient connection management with:
//! - Connection reuse to minimize overhead
//! - Health checking and automatic recovery
//! - Load balancing across connections
//! - Configurable pool sizes and timeouts

use crate::core::error::{Error, Result};
use crate::{Database, LightningDbConfig};
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};
use std::thread;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{debug, error, info, warn};

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Minimum number of connections to maintain
    pub min_connections: usize,
    /// Maximum number of connections allowed
    pub max_connections: usize,
    /// Maximum time to wait for a connection
    pub connection_timeout: Duration,
    /// Time before idle connections are closed
    pub idle_timeout: Duration,
    /// Maximum lifetime of a connection
    pub max_lifetime: Duration,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Enable connection warmup on startup
    pub warmup_connections: bool,
    /// Connection retry configuration
    pub retry_config: RetryConfig,
    /// Load balancing strategy
    pub load_balancing: LoadBalancingStrategy,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 10,
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_hours(24),
            health_check_interval: Duration::from_secs(30),
            warmup_connections: true,
            retry_config: RetryConfig::default(),
            load_balancing: LoadBalancingStrategy::RoundRobin,
        }
    }
}

/// Connection retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        }
    }
}

/// Load balancing strategy for connection selection
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LoadBalancingStrategy {
    /// Round-robin selection
    RoundRobin,
    /// Least recently used
    LeastRecentlyUsed,
    /// Least connections
    LeastConnections,
    /// Random selection
    Random,
    /// Weighted by performance
    WeightedPerformance,
}

/// Pooled database connection
pub struct PooledConnection {
    inner: Arc<Database>,
    pool: Weak<ConnectionPoolInner>,
    id: u64,
    created_at: Instant,
    last_used: Arc<Mutex<Instant>>,
    use_count: Arc<AtomicU64>,
    health_status: Arc<AtomicBool>,
    performance_score: Arc<AtomicU64>,
}

impl PooledConnection {
    fn new(
        db: Arc<Database>,
        pool: Weak<ConnectionPoolInner>,
        id: u64,
    ) -> Self {
        Self {
            inner: db,
            pool,
            id,
            created_at: Instant::now(),
            last_used: Arc::new(Mutex::new(Instant::now())),
            use_count: Arc::new(AtomicU64::new(0)),
            health_status: Arc::new(AtomicBool::new(true)),
            performance_score: Arc::new(AtomicU64::new(100)),
        }
    }
    
    /// Check if connection has exceeded max lifetime
    pub fn is_expired(&self, max_lifetime: Duration) -> bool {
        self.created_at.elapsed() > max_lifetime
    }
    
    /// Check if connection is idle
    pub fn is_idle(&self, idle_timeout: Duration) -> bool {
        self.last_used.lock().elapsed() > idle_timeout
    }
    
    /// Update last used time
    fn touch(&self) {
        *self.last_used.lock() = Instant::now();
        self.use_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Get the underlying database
    pub fn get(&self) -> &Database {
        self.touch();
        &self.inner
    }
    
    /// Check connection health
    pub async fn check_health(&self) -> bool {
        // Perform a simple operation to verify connection works
        match self.inner.get(b"__health_check__") {
            Ok(_) => {
                self.health_status.store(true, Ordering::Relaxed);
                true
            }
            Err(_) => {
                self.health_status.store(false, Ordering::Relaxed);
                false
            }
        }
    }
    
    /// Update performance score based on operation latency
    pub fn update_performance(&self, latency: Duration) {
        let score = if latency < Duration::from_millis(1) {
            100
        } else if latency < Duration::from_millis(10) {
            90
        } else if latency < Duration::from_millis(100) {
            70
        } else if latency < Duration::from_secs(1) {
            50
        } else {
            20
        };
        
        // Exponential moving average
        let old_score = self.performance_score.load(Ordering::Relaxed);
        let new_score = (old_score * 7 + score * 3) / 10;
        self.performance_score.store(new_score, Ordering::Relaxed);
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            pool.return_connection(self.id);
        }
    }
}

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct ConnectionPoolStats {
    pub total_connections: usize,
    pub active_connections: usize,
    pub idle_connections: usize,
    pub pending_requests: usize,
    pub total_created: u64,
    pub total_closed: u64,
    pub total_errors: u64,
    pub avg_wait_time: Duration,
    pub avg_use_time: Duration,
    pub health_check_failures: u64,
}

struct ConnectionPoolInner {
    config: ConnectionPoolConfig,
    db_path: PathBuf,
    db_config: LightningDbConfig,
    
    // Connection storage
    connections: RwLock<Vec<Arc<PooledConnection>>>,
    available: Mutex<VecDeque<u64>>,
    in_use: Mutex<Vec<u64>>,
    
    // Synchronization
    semaphore: Arc<Semaphore>,
    shutdown: AtomicBool,
    
    // Statistics
    next_id: AtomicU64,
    total_created: AtomicU64,
    total_closed: AtomicU64,
    total_errors: AtomicU64,
    total_wait_time: AtomicU64,
    total_use_time: AtomicU64,
    health_check_failures: AtomicU64,
    
    // Load balancing
    round_robin_counter: AtomicUsize,
}

impl ConnectionPoolInner {
    fn return_connection(&self, id: u64) {
        let mut available = self.available.lock();
        let mut in_use = self.in_use.lock();
        
        if let Some(pos) = in_use.iter().position(|&x| x == id) {
            in_use.remove(pos);
            available.push_back(id);
        }
    }
}

/// Database connection pool
pub struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
    health_check_handle: Option<thread::JoinHandle<()>>,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new<P: AsRef<Path>>(
        db_path: P,
        db_config: LightningDbConfig,
        pool_config: ConnectionPoolConfig,
    ) -> Result<Self> {
        let inner = Arc::new(ConnectionPoolInner {
            config: pool_config.clone(),
            db_path: db_path.as_ref().to_path_buf(),
            db_config,
            connections: RwLock::new(Vec::new()),
            available: Mutex::new(VecDeque::new()),
            in_use: Mutex::new(Vec::new()),
            semaphore: Arc::new(Semaphore::new(pool_config.max_connections)),
            shutdown: AtomicBool::new(false),
            next_id: AtomicU64::new(0),
            total_created: AtomicU64::new(0),
            total_closed: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            total_wait_time: AtomicU64::new(0),
            total_use_time: AtomicU64::new(0),
            health_check_failures: AtomicU64::new(0),
            round_robin_counter: AtomicUsize::new(0),
        });
        
        // Initialize minimum connections
        for _ in 0..pool_config.min_connections {
            Self::create_connection(&inner)?;
        }
        
        // Warmup connections if configured
        if pool_config.warmup_connections {
            Self::warmup_connections(&inner)?;
        }
        
        // Start health check thread
        let health_check_handle = if pool_config.health_check_interval > Duration::ZERO {
            Some(Self::start_health_checker(inner.clone()))
        } else {
            None
        };
        
        Ok(Self {
            inner,
            health_check_handle,
        })
    }
    
    /// Create a new connection and add it to the pool
    fn create_connection(inner: &Arc<ConnectionPoolInner>) -> Result<()> {
        let db = Database::open(&inner.db_path, inner.db_config.clone())?;
        let id = inner.next_id.fetch_add(1, Ordering::SeqCst);
        
        let conn = Arc::new(PooledConnection::new(
            Arc::new(db),
            Arc::downgrade(inner),
            id,
        ));
        
        inner.connections.write().push(conn);
        inner.available.lock().push_back(id);
        inner.total_created.fetch_add(1, Ordering::Relaxed);
        
        info!("Created new connection {}", id);
        Ok(())
    }
    
    /// Warmup connections by performing initial operations
    fn warmup_connections(inner: &Arc<ConnectionPoolInner>) -> Result<()> {
        info!("Warming up {} connections", inner.connections.read().len());
        
        for conn in inner.connections.read().iter() {
            // Perform a simple read to warm up caches
            let _ = conn.get().get(b"__warmup__");
        }
        
        Ok(())
    }
    
    /// Start background health checker
    fn start_health_checker(inner: Arc<ConnectionPoolInner>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            while !inner.shutdown.load(Ordering::Relaxed) {
                thread::sleep(inner.config.health_check_interval);
                
                let connections = inner.connections.read().clone();
                for conn in connections {
                    let healthy = rt.block_on(conn.check_health());
                    
                    if !healthy {
                        inner.health_check_failures.fetch_add(1, Ordering::Relaxed);
                        warn!("Connection {} failed health check", conn.id);
                        
                        // TODO: Replace unhealthy connection
                    }
                }
                
                // Clean up expired or idle connections
                Self::cleanup_connections(&inner);
            }
        })
    }
    
    /// Clean up expired or idle connections
    fn cleanup_connections(inner: &Arc<ConnectionPoolInner>) {
        let mut connections = inner.connections.write();
        let mut available = inner.available.lock();
        
        let mut to_remove = Vec::new();
        
        for (i, conn) in connections.iter().enumerate() {
            if conn.is_expired(inner.config.max_lifetime) {
                to_remove.push(i);
                info!("Removing expired connection {}", conn.id);
            } else if conn.is_idle(inner.config.idle_timeout) 
                && connections.len() > inner.config.min_connections {
                to_remove.push(i);
                info!("Removing idle connection {}", conn.id);
            }
        }
        
        // Remove connections in reverse order to maintain indices
        for &i in to_remove.iter().rev() {
            let conn = connections.remove(i);
            available.retain(|&id| id != conn.id);
            inner.total_closed.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    /// Get a connection from the pool
    pub async fn get(&self) -> Result<ConnectionGuard<'_>> {
        let start = Instant::now();
        
        // Acquire semaphore permit
        let permit = self.inner.semaphore
            .acquire()
            .await
            .map_err(|_| Error::ResourceExhausted("Connection pool exhausted".to_string()))?;
        
        let wait_time = start.elapsed();
        self.inner.total_wait_time.fetch_add(
            wait_time.as_micros() as u64,
            Ordering::Relaxed
        );
        
        // Get available connection
        let conn_id = {
            let mut available = self.inner.available.lock();
            let mut in_use = self.inner.in_use.lock();
            
            if let Some(id) = self.select_connection(&available) {
                available.retain(|&x| x != id);
                in_use.push(id);
                Some(id)
            } else {
                None
            }
        };
        
        // Create new connection if needed
        let conn_id = if let Some(id) = conn_id {
            id
        } else if self.inner.connections.read().len() < self.inner.config.max_connections {
            Self::create_connection(&self.inner)?;
            
            let mut available = self.inner.available.lock();
            let mut in_use = self.inner.in_use.lock();
            
            let id = available.pop_front()
                .ok_or_else(|| Error::Internal("Failed to get created connection".to_string()))?;
            in_use.push(id);
            id
        } else {
            return Err(Error::ResourceExhausted("No connections available".to_string()));
        };
        
        // Find the connection
        let connections = self.inner.connections.read();
        let conn = connections.iter()
            .find(|c| c.id == conn_id)
            .ok_or_else(|| Error::Internal("Connection not found".to_string()))?
            .clone();
        
        Ok(ConnectionGuard {
            connection: conn,
            permit: Some(permit),
            start_time: Instant::now(),
            pool: &self.inner,
        })
    }
    
    /// Select a connection based on load balancing strategy
    fn select_connection(&self, available: &VecDeque<u64>) -> Option<u64> {
        if available.is_empty() {
            return None;
        }
        
        match self.inner.config.load_balancing {
            LoadBalancingStrategy::RoundRobin => {
                let idx = self.inner.round_robin_counter.fetch_add(1, Ordering::Relaxed) 
                    % available.len();
                available.get(idx).copied()
            }
            LoadBalancingStrategy::LeastRecentlyUsed => {
                // Select the first available (oldest)
                available.front().copied()
            }
            LoadBalancingStrategy::LeastConnections => {
                // Select connection with lowest use count
                let connections = self.inner.connections.read();
                available.iter()
                    .min_by_key(|&&id| {
                        connections.iter()
                            .find(|c| c.id == id)
                            .map(|c| c.use_count.load(Ordering::Relaxed))
                            .unwrap_or(u64::MAX)
                    })
                    .copied()
            }
            LoadBalancingStrategy::Random => {
                use rand::Rng;
                let idx = rand::rng().gen_range(0..available.len());
                available.get(idx).copied()
            }
            LoadBalancingStrategy::WeightedPerformance => {
                // Select connection with best performance score
                let connections = self.inner.connections.read();
                available.iter()
                    .max_by_key(|&&id| {
                        connections.iter()
                            .find(|c| c.id == id)
                            .map(|c| c.performance_score.load(Ordering::Relaxed))
                            .unwrap_or(0)
                    })
                    .copied()
            }
        }
    }
    
    /// Get pool statistics
    pub fn stats(&self) -> ConnectionPoolStats {
        let connections = self.inner.connections.read();
        let available = self.inner.available.lock();
        let in_use = self.inner.in_use.lock();
        
        let total_wait = self.inner.total_wait_time.load(Ordering::Relaxed);
        let total_use = self.inner.total_use_time.load(Ordering::Relaxed);
        let total_created = self.inner.total_created.load(Ordering::Relaxed);
        
        ConnectionPoolStats {
            total_connections: connections.len(),
            active_connections: in_use.len(),
            idle_connections: available.len(),
            pending_requests: 0, // TODO: Track pending
            total_created: self.inner.total_created.load(Ordering::Relaxed),
            total_closed: self.inner.total_closed.load(Ordering::Relaxed),
            total_errors: self.inner.total_errors.load(Ordering::Relaxed),
            avg_wait_time: if total_created > 0 {
                Duration::from_micros(total_wait / total_created)
            } else {
                Duration::ZERO
            },
            avg_use_time: if total_created > 0 {
                Duration::from_micros(total_use / total_created)
            } else {
                Duration::ZERO
            },
            health_check_failures: self.inner.health_check_failures.load(Ordering::Relaxed),
        }
    }
    
    /// Resize the pool
    pub async fn resize(&self, min: usize, max: usize) -> Result<()> {
        // Update configuration
        // Note: This is simplified - in production you'd need proper synchronization
        
        // Ensure we have at least min connections
        let current = self.inner.connections.read().len();
        if current < min {
            for _ in current..min {
                Self::create_connection(&self.inner)?;
            }
        }
        
        Ok(())
    }
    
    /// Shutdown the pool
    pub fn shutdown(&mut self) {
        info!("Shutting down connection pool");
        
        self.inner.shutdown.store(true, Ordering::Relaxed);
        
        if let Some(handle) = self.health_check_handle.take() {
            let _ = handle.join();
        }
        
        // Close all connections
        let connections = self.inner.connections.write();
        for conn in connections.iter() {
            debug!("Closing connection {}", conn.id);
        }
    }
}

/// Guard for a pooled connection
pub struct ConnectionGuard<'a> {
    connection: Arc<PooledConnection>,
    permit: Option<SemaphorePermit<'a>>,
    start_time: Instant,
    pool: &'a ConnectionPoolInner,
}

impl<'a> ConnectionGuard<'a> {
    /// Get the database connection
    pub fn get(&self) -> &Database {
        self.connection.get()
    }
    
    /// Execute an operation and update performance metrics
    pub fn execute<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Database) -> Result<R>,
    {
        let start = Instant::now();
        let result = f(self.connection.get());
        let latency = start.elapsed();
        
        self.connection.update_performance(latency);
        
        result
    }
    
    /// Mark this connection as unhealthy
    pub fn mark_unhealthy(&self) {
        self.connection.health_status.store(false, Ordering::Relaxed);
    }
}

impl<'a> Drop for ConnectionGuard<'a> {
    fn drop(&mut self) {
        let use_time = self.start_time.elapsed();
        self.pool.total_use_time.fetch_add(
            use_time.as_micros() as u64,
            Ordering::Relaxed
        );
        
        // Permit will be dropped automatically, releasing the connection
    }
}

impl<'a> std::ops::Deref for ConnectionGuard<'a> {
    type Target = Database;
    
    fn deref(&self) -> &Self::Target {
        self.connection.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_connection_pool_basic() {
        let temp_dir = TempDir::new().unwrap();
        let db_config = LightningDbConfig::default();
        let pool_config = ConnectionPoolConfig {
            min_connections: 2,
            max_connections: 5,
            ..Default::default()
        };
        
        let pool = ConnectionPool::new(
            temp_dir.path(),
            db_config,
            pool_config,
        ).unwrap();
        
        // Get a connection
        let conn = pool.get().await.unwrap();
        conn.put(b"key", b"value").unwrap();
        drop(conn);
        
        // Get another connection and verify data
        let conn = pool.get().await.unwrap();
        assert_eq!(conn.get(b"key").unwrap().as_deref(), Some(b"value".as_ref()));
    }
    
    #[tokio::test]
    async fn test_connection_pool_concurrent() {
        let temp_dir = TempDir::new().unwrap();
        let db_config = LightningDbConfig::default();
        let pool_config = ConnectionPoolConfig {
            min_connections: 1,
            max_connections: 3,
            ..Default::default()
        };
        
        let pool = Arc::new(ConnectionPool::new(
            temp_dir.path(),
            db_config,
            pool_config,
        ).unwrap());
        
        // Spawn multiple tasks
        let mut handles = Vec::new();
        for i in 0..10 {
            let pool = pool.clone();
            let handle = tokio::spawn(async move {
                let conn = pool.get().await.unwrap();
                let key = format!("key_{}", i);
                let value = format!("value_{}", i);
                conn.put(key.as_bytes(), value.as_bytes()).unwrap();
            });
            handles.push(handle);
        }
        
        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }
        
        // Verify all data
        for i in 0..10 {
            let conn = pool.get().await.unwrap();
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            assert_eq!(conn.get(key.as_bytes()).unwrap().unwrap(), value.as_bytes());
        }
    }
    
    #[tokio::test]
    async fn test_pool_statistics() {
        let temp_dir = TempDir::new().unwrap();
        let db_config = LightningDbConfig::default();
        let pool_config = ConnectionPoolConfig {
            min_connections: 2,
            max_connections: 5,
            ..Default::default()
        };
        
        let pool = ConnectionPool::new(
            temp_dir.path(),
            db_config,
            pool_config,
        ).unwrap();
        
        let stats = pool.stats();
        assert_eq!(stats.total_connections, 2);
        assert_eq!(stats.idle_connections, 2);
        assert_eq!(stats.active_connections, 0);
        
        // Get a connection
        let _conn = pool.get().await.unwrap();
        
        let stats = pool.stats();
        assert_eq!(stats.active_connections, 1);
        assert_eq!(stats.idle_connections, 1);
    }
}