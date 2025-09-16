//! Token bucket based rate limiter for database operations

use crate::core::error::Result;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Token bucket for rate limiting
#[derive(Debug)]
struct TokenBucket {
    capacity: u64,
    tokens: AtomicU64,
    refill_rate: u64,
    last_refill: RwLock<Instant>,
    burst_capacity: u64,
    burst_active: RwLock<Option<Instant>>,
}

impl TokenBucket {
    fn new(capacity: u64, refill_rate: u64, burst_multiplier: f64) -> Self {
        let burst_capacity = (capacity as f64 * burst_multiplier) as u64;
        Self {
            capacity,
            tokens: AtomicU64::new(capacity),
            refill_rate,
            last_refill: RwLock::new(Instant::now()),
            burst_capacity,
            burst_active: RwLock::new(None),
        }
    }

    fn try_consume(&self, tokens: u64) -> bool {
        self.refill();

        let mut current = self.tokens.load(Ordering::Relaxed);
        loop {
            if current < tokens {
                return false;
            }

            match self.tokens.compare_exchange_weak(
                current,
                current - tokens,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn refill(&self) {
        let now = Instant::now();
        let mut last_refill = self.last_refill.write();

        let elapsed = now.duration_since(*last_refill);
        if elapsed < Duration::from_millis(10) {
            return; // Don't refill too frequently
        }

        let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;
        if tokens_to_add == 0 {
            return;
        }

        *last_refill = now;
        drop(last_refill);

        // Determine capacity based on burst status
        let capacity = {
            let mut burst = self.burst_active.write();
            match &*burst {
                Some(burst_start) if now.duration_since(*burst_start) < Duration::from_secs(10) => {
                    self.burst_capacity
                }
                _ => {
                    *burst = None;
                    self.capacity
                }
            }
        };

        let current = self.tokens.load(Ordering::Relaxed);
        let new_tokens = (current + tokens_to_add).min(capacity);
        self.tokens.store(new_tokens, Ordering::Relaxed);
    }

    fn activate_burst(&self) {
        let mut burst = self.burst_active.write();
        if burst.is_none() {
            *burst = Some(Instant::now());
            // Immediately increase available tokens to burst capacity
            let current = self.tokens.load(Ordering::Relaxed);
            if current < self.burst_capacity {
                self.tokens.store(self.burst_capacity, Ordering::Relaxed);
            }
        }
    }

    fn get_available_tokens(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed)
    }
}

/// Rate limiter for database operations
#[derive(Debug)]
pub struct RateLimiter {
    // Global rate limiters
    read_bucket: Option<Arc<TokenBucket>>,
    write_bucket: Option<Arc<TokenBucket>>,
    scan_bucket: Option<Arc<TokenBucket>>,
    transaction_bucket: Option<Arc<TokenBucket>>,

    // Per-tenant rate limiters
    tenant_limiters: Arc<DashMap<String, TenantRateLimiter>>,

    // Configuration
    allow_burst: bool,
    _burst_duration: Duration,
}

#[derive(Debug)]
struct TenantRateLimiter {
    read_bucket: Arc<TokenBucket>,
    write_bucket: Arc<TokenBucket>,
    _scan_bucket: Arc<TokenBucket>,
    _transaction_bucket: Arc<TokenBucket>,
}

impl RateLimiter {
    pub fn new(
        read_ops_per_sec: Option<u64>,
        write_ops_per_sec: Option<u64>,
        scan_ops_per_sec: Option<u64>,
        transaction_ops_per_sec: Option<u64>,
        allow_burst: bool,
        burst_multiplier: f64,
        burst_duration: Duration,
    ) -> Result<Self> {
        let read_bucket =
            read_ops_per_sec.map(|rate| Arc::new(TokenBucket::new(rate, rate, burst_multiplier)));
        let write_bucket =
            write_ops_per_sec.map(|rate| Arc::new(TokenBucket::new(rate, rate, burst_multiplier)));
        let scan_bucket =
            scan_ops_per_sec.map(|rate| Arc::new(TokenBucket::new(rate, rate, burst_multiplier)));
        let transaction_bucket = transaction_ops_per_sec
            .map(|rate| Arc::new(TokenBucket::new(rate, rate, burst_multiplier)));

        Ok(Self {
            read_bucket,
            write_bucket,
            scan_bucket,
            transaction_bucket,
            tenant_limiters: Arc::new(DashMap::new()),
            allow_burst,
            _burst_duration: burst_duration,
        })
    }

    /// Check if a read operation is allowed
    pub fn check_read(&self) -> Result<bool> {
        if let Some(ref bucket) = self.read_bucket {
            Ok(bucket.try_consume(1))
        } else {
            Ok(true)
        }
    }

    /// Check if a write operation is allowed
    pub fn check_write(&self) -> Result<bool> {
        if let Some(ref bucket) = self.write_bucket {
            Ok(bucket.try_consume(1))
        } else {
            Ok(true)
        }
    }

    /// Check if a scan operation is allowed
    pub fn check_scan(&self) -> Result<bool> {
        if let Some(ref bucket) = self.scan_bucket {
            Ok(bucket.try_consume(1))
        } else {
            Ok(true)
        }
    }

    /// Check if a transaction operation is allowed
    pub fn check_transaction(&self) -> Result<bool> {
        if let Some(ref bucket) = self.transaction_bucket {
            Ok(bucket.try_consume(1))
        } else {
            Ok(true)
        }
    }

    /// Check if a tenant read operation is allowed
    pub fn check_tenant_read(&self, tenant_id: &str) -> Result<bool> {
        if let Some(limiter) = self.tenant_limiters.get(tenant_id) {
            Ok(limiter.read_bucket.try_consume(1))
        } else {
            self.check_read()
        }
    }

    /// Check if a tenant write operation is allowed
    pub fn check_tenant_write(&self, tenant_id: &str) -> Result<bool> {
        if let Some(limiter) = self.tenant_limiters.get(tenant_id) {
            Ok(limiter.write_bucket.try_consume(1))
        } else {
            self.check_write()
        }
    }

    /// Configure rate limits for a tenant
    pub fn set_tenant_limits(
        &self,
        tenant_id: String,
        read_ops_per_sec: u64,
        write_ops_per_sec: u64,
        scan_ops_per_sec: u64,
        transaction_ops_per_sec: u64,
    ) -> Result<()> {
        let burst_multiplier = if self.allow_burst { 2.0 } else { 1.0 };

        let limiter = TenantRateLimiter {
            read_bucket: Arc::new(TokenBucket::new(
                read_ops_per_sec,
                read_ops_per_sec,
                burst_multiplier,
            )),
            write_bucket: Arc::new(TokenBucket::new(
                write_ops_per_sec,
                write_ops_per_sec,
                burst_multiplier,
            )),
            _scan_bucket: Arc::new(TokenBucket::new(
                scan_ops_per_sec,
                scan_ops_per_sec,
                burst_multiplier,
            )),
            _transaction_bucket: Arc::new(TokenBucket::new(
                transaction_ops_per_sec,
                transaction_ops_per_sec,
                burst_multiplier,
            )),
        };

        self.tenant_limiters.insert(tenant_id, limiter);
        Ok(())
    }

    /// Activate burst mode
    pub fn activate_burst(&self) {
        if !self.allow_burst {
            return;
        }

        if let Some(ref bucket) = self.read_bucket {
            bucket.activate_burst();
        }
        if let Some(ref bucket) = self.write_bucket {
            bucket.activate_burst();
        }
        if let Some(ref bucket) = self.scan_bucket {
            bucket.activate_burst();
        }
        if let Some(ref bucket) = self.transaction_bucket {
            bucket.activate_burst();
        }
    }

    /// Get current read rate (operations per second)
    pub fn get_current_read_rate(&self) -> u64 {
        self.read_bucket
            .as_ref()
            .map(|b| b.get_available_tokens())
            .unwrap_or(0)
    }

    /// Get current write rate (operations per second)
    pub fn get_current_write_rate(&self) -> u64 {
        self.write_bucket
            .as_ref()
            .map(|b| b.get_available_tokens())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_token_bucket_basic() {
        let bucket = TokenBucket::new(10, 10, 1.0);

        // Should be able to consume up to capacity
        for _ in 0..10 {
            assert!(bucket.try_consume(1));
        }

        // Should fail when empty
        assert!(!bucket.try_consume(1));
    }

    #[test]
    fn test_token_bucket_refill() {
        let bucket = TokenBucket::new(10, 100, 1.0); // 100 tokens/sec

        // Consume all tokens
        assert!(bucket.try_consume(10));
        assert!(!bucket.try_consume(1));

        // Wait for refill
        thread::sleep(Duration::from_millis(100));

        // Should have ~10 tokens refilled
        assert!(bucket.try_consume(5));
    }

    #[test]
    fn test_rate_limiter() {
        let limiter = RateLimiter::new(
            Some(100), // 100 reads/sec
            Some(50),  // 50 writes/sec
            None,      // No scan limit
            None,      // No transaction limit
            false,     // No burst
            1.0,
            Duration::from_secs(10),
        )
        .unwrap();

        // Read operations should be limited
        let mut allowed = 0;
        for _ in 0..200 {
            if limiter.check_read().unwrap() {
                allowed += 1;
            }
        }
        assert!(allowed <= 100);

        // Write operations should be limited
        allowed = 0;
        for _ in 0..100 {
            if limiter.check_write().unwrap() {
                allowed += 1;
            }
        }
        assert!(allowed <= 50);

        // Scan operations should not be limited
        for _ in 0..1000 {
            assert!(limiter.check_scan().unwrap());
        }
    }
}
