use crate::error::{Error, Result};
use std::sync::Arc;
use std::time::Duration;

/// Consistency level for database operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyLevel {
    /// Eventually consistent - fastest, no guarantees
    Eventual,
    /// Read your own writes within a session
    Session,
    /// Strong consistency - all reads see latest committed writes
    Strong,
    /// Linearizable - strongest guarantee, operations appear atomic and ordered
    Linearizable,
}

/// Configuration for consistency behavior
#[derive(Debug, Clone)]
pub struct ConsistencyConfig {
    /// Default consistency level
    pub default_level: ConsistencyLevel,
    /// Timeout for achieving consistency
    pub consistency_timeout: Duration,
    /// Enable read repair for eventual consistency
    pub enable_read_repair: bool,
    /// Maximum clock skew tolerance
    pub max_clock_skew: Duration,
}

impl Default for ConsistencyConfig {
    fn default() -> Self {
        Self {
            default_level: ConsistencyLevel::Strong,
            consistency_timeout: Duration::from_millis(100),
            enable_read_repair: true,
            max_clock_skew: Duration::from_millis(10),
        }
    }
}

/// Manager for handling consistency across operations
pub struct ConsistencyManager {
    config: ConsistencyConfig,
    pub(crate) clock: Arc<HybridLogicalClock>,
    session_manager: Arc<SessionManager>,
}

impl ConsistencyManager {
    pub fn new(config: ConsistencyConfig) -> Self {
        Self {
            config,
            clock: Arc::new(HybridLogicalClock::new()),
            session_manager: Arc::new(SessionManager::new()),
        }
    }
    
    /// Wait for a write to be visible at the given consistency level
    pub fn wait_for_write_visibility(
        &self,
        write_timestamp: u64,
        level: ConsistencyLevel,
    ) -> Result<()> {
        match level {
            ConsistencyLevel::Eventual => {
                // No waiting needed
                Ok(())
            }
            ConsistencyLevel::Session => {
                // Ensure session sees its own writes
                self.session_manager.wait_for_timestamp(write_timestamp, self.config.consistency_timeout)
            }
            ConsistencyLevel::Strong => {
                // Wait for write to be durable
                self.wait_for_durability(write_timestamp)
            }
            ConsistencyLevel::Linearizable => {
                // Wait for global ordering
                self.wait_for_linearization(write_timestamp)
            }
        }
    }
    
    /// Get read timestamp based on consistency level
    pub fn get_read_timestamp(&self, level: ConsistencyLevel, session_id: Option<u64>) -> u64 {
        match level {
            ConsistencyLevel::Eventual => {
                // Read from any available version
                self.clock.get_timestamp() - self.config.max_clock_skew.as_millis() as u64
            }
            ConsistencyLevel::Session => {
                // Read at session's last write timestamp
                if let Some(sid) = session_id {
                    self.session_manager.get_session_timestamp(sid)
                } else {
                    self.clock.get_timestamp()
                }
            }
            ConsistencyLevel::Strong | ConsistencyLevel::Linearizable => {
                // Read at latest timestamp
                self.clock.get_timestamp()
            }
        }
    }
    
    /// Check if a read repair is needed
    pub fn should_repair_read(
        &self,
        level: ConsistencyLevel,
        version_timestamp: u64,
        current_timestamp: u64,
    ) -> bool {
        if !self.config.enable_read_repair {
            return false;
        }
        
        match level {
            ConsistencyLevel::Eventual => {
                // Repair if version is too old
                current_timestamp - version_timestamp > self.config.max_clock_skew.as_millis() as u64 * 10
            }
            _ => false, // No repair needed for stronger consistency levels
        }
    }
    
    fn wait_for_durability(&self, timestamp: u64) -> Result<()> {
        let start = std::time::Instant::now();
        
        // Mark the timestamp as durable after some processing
        // In a real implementation, this would wait for actual durability
        self.clock.mark_durable(timestamp);
        
        while start.elapsed() < self.config.consistency_timeout {
            if self.clock.is_durable(timestamp) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        
        Err(Error::Timeout("Consistency timeout waiting for durability".to_string()))
    }
    
    fn wait_for_linearization(&self, timestamp: u64) -> Result<()> {
        let start = std::time::Instant::now();
        
        while start.elapsed() < self.config.consistency_timeout {
            if self.clock.is_linearized(timestamp) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        
        Err(Error::Timeout("Consistency timeout waiting for linearization".to_string()))
    }
}

/// Hybrid Logical Clock for distributed consistency
pub(crate) struct HybridLogicalClock {
    physical_time: std::sync::atomic::AtomicU64,
    logical_time: std::sync::atomic::AtomicU64,
    durable_timestamp: std::sync::atomic::AtomicU64,
    linearized_timestamp: std::sync::atomic::AtomicU64,
}

impl HybridLogicalClock {
    pub(crate) fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        Self {
            physical_time: std::sync::atomic::AtomicU64::new(now),
            logical_time: std::sync::atomic::AtomicU64::new(0),
            durable_timestamp: std::sync::atomic::AtomicU64::new(0),
            linearized_timestamp: std::sync::atomic::AtomicU64::new(0),
        }
    }
    
    pub(crate) fn get_timestamp(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        let prev_physical = self.physical_time.load(std::sync::atomic::Ordering::Acquire);
        
        if now > prev_physical {
            self.physical_time.store(now, std::sync::atomic::Ordering::Release);
            self.logical_time.store(0, std::sync::atomic::Ordering::Release);
            (now << 16) | 0
        } else {
            let logical = self.logical_time.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1;
            (prev_physical << 16) | (logical & 0xFFFF)
        }
    }
    
    fn is_durable(&self, timestamp: u64) -> bool {
        self.durable_timestamp.load(std::sync::atomic::Ordering::Acquire) >= timestamp
    }
    
    pub(crate) fn mark_durable(&self, timestamp: u64) {
        self.durable_timestamp.fetch_max(timestamp, std::sync::atomic::Ordering::AcqRel);
    }
    
    fn is_linearized(&self, timestamp: u64) -> bool {
        self.linearized_timestamp.load(std::sync::atomic::Ordering::Acquire) >= timestamp
    }
    
    #[allow(dead_code)]
    pub(crate) fn mark_linearized(&self, timestamp: u64) {
        self.linearized_timestamp.fetch_max(timestamp, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Session manager for session consistency
struct SessionManager {
    sessions: parking_lot::RwLock<std::collections::HashMap<u64, SessionState>>,
    _next_session_id: std::sync::atomic::AtomicU64,
}

#[derive(Debug)]
struct SessionState {
    last_write_timestamp: u64,
    _last_read_timestamp: u64,
    _created_at: std::time::Instant,
}

impl SessionManager {
    fn new() -> Self {
        Self {
            sessions: parking_lot::RwLock::new(std::collections::HashMap::new()),
            _next_session_id: std::sync::atomic::AtomicU64::new(1),
        }
    }
    
    #[allow(dead_code)]
    fn create_session(&self) -> u64 {
        let session_id = self._next_session_id.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let state = SessionState {
            last_write_timestamp: 0,
            _last_read_timestamp: 0,
            _created_at: std::time::Instant::now(),
        };
        
        self.sessions.write().insert(session_id, state);
        session_id
    }
    
    #[allow(dead_code)]
    fn update_write_timestamp(&self, session_id: u64, timestamp: u64) {
        if let Some(session) = self.sessions.write().get_mut(&session_id) {
            session.last_write_timestamp = session.last_write_timestamp.max(timestamp);
        }
    }
    
    fn get_session_timestamp(&self, session_id: u64) -> u64 {
        self.sessions
            .read()
            .get(&session_id)
            .map(|s| s.last_write_timestamp)
            .unwrap_or(0)
    }
    
    fn wait_for_timestamp(&self, _timestamp: u64, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        
        while start.elapsed() < timeout {
            // In a real implementation, this would wait for replication
            // For now, we'll simulate immediate consistency
            return Ok(());
        }
        
        Err(Error::Timeout("Session consistency timeout".to_string()))
    }
    
    #[allow(dead_code)]
    fn cleanup_old_sessions(&self, max_age: Duration) {
        let now = std::time::Instant::now();
        self.sessions.write().retain(|_, session| {
            now.duration_since(session._created_at) < max_age
        });
    }
}

/// Read repair for eventual consistency
pub struct ReadRepair {
    consistency_manager: Arc<ConsistencyManager>,
}

impl ReadRepair {
    pub fn new(consistency_manager: Arc<ConsistencyManager>) -> Self {
        Self { consistency_manager }
    }
    
    /// Perform read repair if needed
    pub fn repair_if_needed(
        &self,
        key: &[u8],
        versions: Vec<(u64, Vec<u8>)>,
        level: ConsistencyLevel,
    ) -> Option<Vec<u8>> {
        if versions.is_empty() {
            return None;
        }
        
        // Find the most recent version
        let (latest_timestamp, latest_value) = versions
            .iter()
            .max_by_key(|(ts, _)| ts)
            .unwrap();
        
        let current_timestamp = self.consistency_manager.clock.get_timestamp();
        
        if self.consistency_manager.should_repair_read(level, *latest_timestamp, current_timestamp) {
            // In a real implementation, this would propagate the latest value
            // to nodes with older versions
            tracing::debug!("Read repair needed for key: {:?}", key);
        }
        
        Some(latest_value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_consistency_levels() {
        let config = ConsistencyConfig::default();
        let manager = ConsistencyManager::new(config);
        
        // Test different read timestamps
        let eventual_ts = manager.get_read_timestamp(ConsistencyLevel::Eventual, None);
        let strong_ts = manager.get_read_timestamp(ConsistencyLevel::Strong, None);
        
        assert!(strong_ts >= eventual_ts);
    }
    
    #[test]
    fn test_session_consistency() {
        let config = ConsistencyConfig::default();
        let manager = ConsistencyManager::new(config);
        
        let session_id = manager.session_manager.create_session();
        let write_ts = manager.clock.get_timestamp();
        
        manager.session_manager.update_write_timestamp(session_id, write_ts);
        
        let read_ts = manager.get_read_timestamp(ConsistencyLevel::Session, Some(session_id));
        assert_eq!(read_ts, write_ts);
    }
    
    #[test]
    fn test_hybrid_logical_clock() {
        let clock = HybridLogicalClock::new();
        
        let ts1 = clock.get_timestamp();
        let ts2 = clock.get_timestamp();
        let ts3 = clock.get_timestamp();
        
        assert!(ts2 > ts1);
        assert!(ts3 > ts2);
        
        // Test durability marking
        clock.mark_durable(ts2);
        assert!(clock.is_durable(ts1));
        assert!(clock.is_durable(ts2));
        assert!(!clock.is_durable(ts3));
    }
}