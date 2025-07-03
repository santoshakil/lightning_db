use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::path::Path;
use crate::error::{Error, Result};

/// Operational safety guards for production deployments
pub struct SafetyGuards {
    /// Enable read-only mode
    pub read_only_mode: AtomicBool,
    
    /// Enable maintenance mode (rejects user operations)
    pub maintenance_mode: AtomicBool,
    
    /// Enable emergency shutdown
    pub emergency_shutdown: AtomicBool,
    
    /// Enable gradual shutdown (stops accepting new operations)
    pub graceful_shutdown: AtomicBool,
    
    /// Circuit breaker state
    pub circuit_breaker: Arc<CircuitBreaker>,
    
    /// Operation rate limiter
    pub rate_limiter: Arc<RateLimiter>,
    
    /// Backup guard
    pub backup_guard: Arc<BackupGuard>,
    
    /// Data retention guard
    pub retention_guard: Arc<RetentionGuard>,
    
    /// Corruption guard
    pub corruption_guard: Arc<CorruptionGuard>,
}

/// Circuit breaker to prevent cascading failures
pub struct CircuitBreaker {
    state: Mutex<CircuitState>,
    failure_threshold: u32,
    success_threshold: u32,
    timeout: Duration,
    half_open_max_calls: u32,
}

#[derive(Debug, Clone, PartialEq)]
enum CircuitState {
    Closed {
        failure_count: u32,
    },
    Open {
        opened_at: Instant,
    },
    HalfOpen {
        success_count: u32,
        failure_count: u32,
        calls_allowed: u32,
    },
}

/// Rate limiter using token bucket algorithm
pub struct RateLimiter {
    tokens: AtomicU64,
    max_tokens: u64,
    refill_rate: u64, // tokens per second
    last_refill: Mutex<Instant>,
}

/// Backup guard to ensure safe backup operations
pub struct BackupGuard {
    backup_in_progress: AtomicBool,
    last_backup: Mutex<Option<SystemTime>>,
    min_backup_interval: Duration,
    backup_lock: Mutex<()>,
}

/// Data retention guard to prevent accidental data loss
pub struct RetentionGuard {
    deletion_enabled: AtomicBool,
    bulk_deletion_threshold: usize,
    deletion_confirmation_required: AtomicBool,
    protected_keys: Mutex<Vec<String>>,
}

/// Corruption detection and prevention guard
pub struct CorruptionGuard {
    corruption_detected: AtomicBool,
    auto_quarantine: AtomicBool,
    quarantine_path: Mutex<Option<String>>,
    checksum_verification_enabled: AtomicBool,
}

impl SafetyGuards {
    pub fn new() -> Self {
        Self {
            read_only_mode: AtomicBool::new(false),
            maintenance_mode: AtomicBool::new(false),
            emergency_shutdown: AtomicBool::new(false),
            graceful_shutdown: AtomicBool::new(false),
            circuit_breaker: Arc::new(CircuitBreaker::new(
                5,    // failure threshold
                3,    // success threshold
                Duration::from_secs(60), // timeout
                10,   // half-open max calls
            )),
            rate_limiter: Arc::new(RateLimiter::new(
                10000, // max tokens
                1000,  // tokens per second
            )),
            backup_guard: Arc::new(BackupGuard::new(Duration::from_secs(3600))),
            retention_guard: Arc::new(RetentionGuard::new(1000)),
            corruption_guard: Arc::new(CorruptionGuard::new()),
        }
    }
    
    /// Check if write operations are allowed
    pub fn check_write_allowed(&self, key: &[u8]) -> Result<()> {
        // Check emergency shutdown
        if self.emergency_shutdown.load(Ordering::Relaxed) {
            return Err(Error::InvalidOperation { 
                reason: "Emergency shutdown active".to_string() 
            });
        }
        
        // Check graceful shutdown
        if self.graceful_shutdown.load(Ordering::Relaxed) {
            return Err(Error::InvalidOperation { 
                reason: "Graceful shutdown in progress".to_string() 
            });
        }
        
        // Check maintenance mode
        if self.maintenance_mode.load(Ordering::Relaxed) {
            return Err(Error::InvalidOperation { 
                reason: "Maintenance mode active".to_string() 
            });
        }
        
        // Check read-only mode
        if self.read_only_mode.load(Ordering::Relaxed) {
            return Err(Error::InvalidOperation { 
                reason: "Database is in read-only mode".to_string() 
            });
        }
        
        // Check corruption guard
        if self.corruption_guard.corruption_detected.load(Ordering::Relaxed) {
            return Err(Error::InvalidOperation { 
                reason: "Corruption detected - writes disabled".to_string() 
            });
        }
        
        // Check circuit breaker
        self.circuit_breaker.check()?;
        
        // Check rate limiter
        self.rate_limiter.acquire(1)?;
        
        Ok(())
    }
    
    /// Check if read operations are allowed
    pub fn check_read_allowed(&self) -> Result<()> {
        // Check emergency shutdown
        if self.emergency_shutdown.load(Ordering::Relaxed) {
            return Err(Error::InvalidOperation { 
                reason: "Emergency shutdown active".to_string() 
            });
        }
        
        // Check circuit breaker
        self.circuit_breaker.check()?;
        
        // Check rate limiter (use less tokens for reads)
        self.rate_limiter.acquire(1)?;
        
        Ok(())
    }
    
    /// Check if deletion is allowed
    pub fn check_delete_allowed(&self, key: &[u8]) -> Result<()> {
        // First check if writes are allowed
        self.check_write_allowed(key)?;
        
        // Check retention guard
        self.retention_guard.check_deletion_allowed(key)?;
        
        Ok(())
    }
    
    /// Check if bulk operations are allowed
    pub fn check_bulk_operation_allowed(&self, operation_count: usize) -> Result<()> {
        // Check if we have enough rate limit tokens
        self.rate_limiter.acquire(operation_count as u64)?;
        
        // Check circuit breaker
        self.circuit_breaker.check()?;
        
        Ok(())
    }
    
    /// Enable read-only mode
    pub fn enable_read_only_mode(&self) {
        self.read_only_mode.store(true, Ordering::Relaxed);
        println!("⚠️  Safety Guard: Read-only mode ENABLED");
    }
    
    /// Disable read-only mode
    pub fn disable_read_only_mode(&self) {
        self.read_only_mode.store(false, Ordering::Relaxed);
        println!("✅ Safety Guard: Read-only mode DISABLED");
    }
    
    /// Enable maintenance mode
    pub fn enable_maintenance_mode(&self) {
        self.maintenance_mode.store(true, Ordering::Relaxed);
        println!("🔧 Safety Guard: Maintenance mode ENABLED");
    }
    
    /// Disable maintenance mode
    pub fn disable_maintenance_mode(&self) {
        self.maintenance_mode.store(false, Ordering::Relaxed);
        println!("✅ Safety Guard: Maintenance mode DISABLED");
    }
    
    /// Trigger emergency shutdown
    pub fn emergency_shutdown(&self) {
        self.emergency_shutdown.store(true, Ordering::Relaxed);
        println!("🚨 Safety Guard: EMERGENCY SHUTDOWN ACTIVATED");
    }
    
    /// Start graceful shutdown
    pub fn start_graceful_shutdown(&self) {
        self.graceful_shutdown.store(true, Ordering::Relaxed);
        println!("🛑 Safety Guard: Graceful shutdown initiated");
    }
    
    /// Record operation failure
    pub fn record_failure(&self) {
        self.circuit_breaker.record_failure();
    }
    
    /// Record operation success
    pub fn record_success(&self) {
        self.circuit_breaker.record_success();
    }
}

impl CircuitBreaker {
    fn new(failure_threshold: u32, success_threshold: u32, timeout: Duration, half_open_max_calls: u32) -> Self {
        Self {
            state: Mutex::new(CircuitState::Closed { failure_count: 0 }),
            failure_threshold,
            success_threshold,
            timeout,
            half_open_max_calls,
        }
    }
    
    pub fn check(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        
        match &*state {
            CircuitState::Open { opened_at } => {
                if opened_at.elapsed() >= self.timeout {
                    // Transition to half-open
                    *state = CircuitState::HalfOpen {
                        success_count: 0,
                        failure_count: 0,
                        calls_allowed: self.half_open_max_calls,
                    };
                    Ok(())
                } else {
                    Err(Error::InvalidOperation {
                        reason: "Circuit breaker is OPEN".to_string(),
                    })
                }
            }
            CircuitState::HalfOpen { calls_allowed, .. } => {
                if *calls_allowed > 0 {
                    Ok(())
                } else {
                    Err(Error::InvalidOperation {
                        reason: "Circuit breaker is HALF-OPEN (max calls reached)".to_string(),
                    })
                }
            }
            CircuitState::Closed { .. } => Ok(()),
        }
    }
    
    fn record_success(&self) {
        let mut state = self.state.lock().unwrap();
        
        match &mut *state {
            CircuitState::HalfOpen { success_count, calls_allowed, .. } => {
                *success_count += 1;
                *calls_allowed = calls_allowed.saturating_sub(1);
                
                if *success_count >= self.success_threshold {
                    // Transition to closed
                    *state = CircuitState::Closed { failure_count: 0 };
                    println!("✅ Circuit Breaker: Transitioned to CLOSED");
                }
            }
            CircuitState::Closed { failure_count } => {
                // Reset failure count on success
                *failure_count = 0;
            }
            CircuitState::Open { .. } => {}
        }
    }
    
    fn record_failure(&self) {
        let mut state = self.state.lock().unwrap();
        
        match &mut *state {
            CircuitState::Closed { failure_count } => {
                *failure_count += 1;
                
                if *failure_count >= self.failure_threshold {
                    // Transition to open
                    *state = CircuitState::Open {
                        opened_at: Instant::now(),
                    };
                    println!("🚨 Circuit Breaker: Transitioned to OPEN");
                }
            }
            CircuitState::HalfOpen { failure_count, .. } => {
                *failure_count += 1;
                
                // Any failure in half-open goes back to open
                *state = CircuitState::Open {
                    opened_at: Instant::now(),
                };
                println!("🚨 Circuit Breaker: Transitioned back to OPEN");
            }
            CircuitState::Open { .. } => {}
        }
    }
}

impl RateLimiter {
    fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(max_tokens),
            max_tokens,
            refill_rate,
            last_refill: Mutex::new(Instant::now()),
        }
    }
    
    pub fn acquire(&self, tokens: u64) -> Result<()> {
        // Refill tokens
        self.refill();
        
        // Try to acquire tokens
        let mut current = self.tokens.load(Ordering::Relaxed);
        loop {
            if current < tokens {
                return Err(Error::Throttled(Duration::from_millis(
                    ((tokens - current) * 1000) / self.refill_rate
                )));
            }
            
            match self.tokens.compare_exchange(
                current,
                current - tokens,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }
    
    fn refill(&self) {
        let mut last_refill = self.last_refill.lock().unwrap();
        let now = Instant::now();
        let elapsed = now - *last_refill;
        
        if elapsed >= Duration::from_millis(100) {
            let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;
            let current = self.tokens.load(Ordering::Relaxed);
            let new_tokens = (current + tokens_to_add).min(self.max_tokens);
            
            self.tokens.store(new_tokens, Ordering::Relaxed);
            *last_refill = now;
        }
    }
}

impl BackupGuard {
    fn new(min_interval: Duration) -> Self {
        Self {
            backup_in_progress: AtomicBool::new(false),
            last_backup: Mutex::new(None),
            min_backup_interval: min_interval,
            backup_lock: Mutex::new(()),
        }
    }
    
    pub fn start_backup(&self) -> Result<BackupHandle> {
        // Acquire backup lock
        let _lock = self.backup_lock.lock().unwrap();
        
        // Check if backup already in progress
        if self.backup_in_progress.load(Ordering::Relaxed) {
            return Err(Error::InvalidOperation {
                reason: "Backup already in progress".to_string(),
            });
        }
        
        // Check minimum interval
        if let Some(last) = *self.last_backup.lock().unwrap() {
            let elapsed = SystemTime::now().duration_since(last).unwrap_or_default();
            if elapsed < self.min_backup_interval {
                return Err(Error::InvalidOperation {
                    reason: format!(
                        "Minimum backup interval not met ({:?} remaining)",
                        self.min_backup_interval - elapsed
                    ),
                });
            }
        }
        
        // Mark backup as in progress
        self.backup_in_progress.store(true, Ordering::Relaxed);
        
        Ok(BackupHandle {
            guard: self,
        })
    }
}

pub struct BackupHandle<'a> {
    guard: &'a BackupGuard,
}

impl<'a> Drop for BackupHandle<'a> {
    fn drop(&mut self) {
        self.guard.backup_in_progress.store(false, Ordering::Relaxed);
        *self.guard.last_backup.lock().unwrap() = Some(SystemTime::now());
    }
}

impl RetentionGuard {
    fn new(bulk_threshold: usize) -> Self {
        Self {
            deletion_enabled: AtomicBool::new(true),
            bulk_deletion_threshold: bulk_threshold,
            deletion_confirmation_required: AtomicBool::new(false),
            protected_keys: Mutex::new(Vec::new()),
        }
    }
    
    fn check_deletion_allowed(&self, key: &[u8]) -> Result<()> {
        if !self.deletion_enabled.load(Ordering::Relaxed) {
            return Err(Error::InvalidOperation {
                reason: "Deletion is disabled".to_string(),
            });
        }
        
        // Check if key is protected
        let key_str = String::from_utf8_lossy(key);
        let protected = self.protected_keys.lock().unwrap();
        for pattern in protected.iter() {
            if key_str.contains(pattern) {
                return Err(Error::InvalidOperation {
                    reason: format!("Key '{}' is protected from deletion", key_str),
                });
            }
        }
        
        Ok(())
    }
    
    pub fn protect_key_pattern(&self, pattern: String) {
        self.protected_keys.lock().unwrap().push(pattern);
    }
    
    pub fn enable_deletion_confirmation(&self) {
        self.deletion_confirmation_required.store(true, Ordering::Relaxed);
    }
}

impl CorruptionGuard {
    fn new() -> Self {
        Self {
            corruption_detected: AtomicBool::new(false),
            auto_quarantine: AtomicBool::new(true),
            quarantine_path: Mutex::new(None),
            checksum_verification_enabled: AtomicBool::new(true),
        }
    }
    
    pub fn report_corruption(&self, location: &str, details: &str) {
        println!("🚨 CORRUPTION DETECTED at {}: {}", location, details);
        self.corruption_detected.store(true, Ordering::Relaxed);
        
        if self.auto_quarantine.load(Ordering::Relaxed) {
            // In a real implementation, would move corrupted data to quarantine
            println!("🔒 Auto-quarantine activated");
        }
    }
    
    pub fn clear_corruption_flag(&self) {
        self.corruption_detected.store(false, Ordering::Relaxed);
        println!("✅ Corruption flag cleared");
    }
}

impl Default for SafetyGuards {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_circuit_breaker() {
        let cb = CircuitBreaker::new(2, 2, Duration::from_millis(100), 5);
        
        // Should start closed
        assert!(cb.check().is_ok());
        
        // Record failures
        cb.record_failure();
        cb.record_failure();
        
        // Should now be open
        assert!(cb.check().is_err());
        
        // Wait for timeout
        std::thread::sleep(Duration::from_millis(150));
        
        // Should transition to half-open
        assert!(cb.check().is_ok());
    }
    
    #[test]
    fn test_rate_limiter() {
        let rl = RateLimiter::new(10, 10);
        
        // Should allow initial requests
        assert!(rl.acquire(5).is_ok());
        assert!(rl.acquire(5).is_ok());
        
        // Should be throttled
        assert!(rl.acquire(5).is_err());
        
        // Wait for refill
        std::thread::sleep(Duration::from_millis(500));
        
        // Should allow again
        assert!(rl.acquire(5).is_ok());
    }
}