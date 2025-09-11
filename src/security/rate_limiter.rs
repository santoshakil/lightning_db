use crate::security::{SecurityError, SecurityResult};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub max_attempts: u32,
    pub time_window: Duration,
    pub lockout_duration: Duration,
    pub exponential_backoff: bool,
    pub global_rate_limit: Option<u32>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            time_window: Duration::from_secs(300),
            lockout_duration: Duration::from_secs(900),
            exponential_backoff: true,
            global_rate_limit: Some(1000),
        }
    }
}

#[derive(Debug, Clone)]
struct AttemptRecord {
    count: u32,
    first_attempt: Instant,
    last_attempt: Instant,
    locked_until: Option<Instant>,
    lockout_count: u32,
}

pub struct TimingAttackRateLimiter {
    attempts: Arc<DashMap<String, AttemptRecord>>,
    config: Arc<RateLimitConfig>,
    global_counter: Arc<Mutex<u32>>,
    global_window_start: Arc<Mutex<Instant>>,
}

impl TimingAttackRateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            attempts: Arc::new(DashMap::new()),
            config: Arc::new(config),
            global_counter: Arc::new(Mutex::new(0)),
            global_window_start: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub async fn check_and_record_attempt(&self, identifier: &str) -> SecurityResult<()> {
        self.cleanup_expired_records().await;

        if let Some(global_limit) = self.config.global_rate_limit {
            self.check_global_rate_limit(global_limit).await?;
        }

        let now = Instant::now();
        let mut entry = self
            .attempts
            .entry(identifier.to_string())
            .or_insert_with(|| AttemptRecord {
                count: 0,
                first_attempt: now,
                last_attempt: now,
                locked_until: None,
                lockout_count: 0,
            });

        if let Some(locked_until) = entry.locked_until {
            if now < locked_until {
                let remaining = locked_until.duration_since(now);
                return Err(SecurityError::RateLimitExceeded(format!(
                    "Account locked for {} more seconds",
                    remaining.as_secs()
                )));
            } else {
                entry.locked_until = None;
                entry.count = 0;
                entry.first_attempt = now;
            }
        }

        if now.duration_since(entry.first_attempt) > self.config.time_window {
            entry.count = 1;
            entry.first_attempt = now;
        } else {
            entry.count += 1;
        }

        entry.last_attempt = now;

        if entry.count > self.config.max_attempts {
            let lockout_duration = if self.config.exponential_backoff {
                self.config.lockout_duration * 2_u32.pow(entry.lockout_count.min(5))
            } else {
                self.config.lockout_duration
            };

            entry.locked_until = Some(now + lockout_duration);
            entry.lockout_count += 1;

            return Err(SecurityError::RateLimitExceeded(format!(
                "Too many attempts. Locked for {} seconds",
                lockout_duration.as_secs()
            )));
        }

        Ok(())
    }

    pub async fn is_locked(&self, identifier: &str) -> bool {
        if let Some(record) = self.attempts.get(identifier) {
            if let Some(locked_until) = record.locked_until {
                return Instant::now() < locked_until;
            }
        }
        false
    }

    pub async fn get_remaining_attempts(&self, identifier: &str) -> u32 {
        if let Some(record) = self.attempts.get(identifier) {
            if record.locked_until.is_some() && Instant::now() < record.locked_until.unwrap() {
                return 0;
            }

            let window_elapsed = Instant::now().duration_since(record.first_attempt);
            if window_elapsed > self.config.time_window {
                return self.config.max_attempts;
            }

            return self.config.max_attempts.saturating_sub(record.count);
        }

        self.config.max_attempts
    }

    pub async fn reset_attempts(&self, identifier: &str) {
        self.attempts.remove(identifier);
    }

    async fn check_global_rate_limit(&self, limit: u32) -> SecurityResult<()> {
        let mut counter = self.global_counter.lock().await;
        let mut window_start = self.global_window_start.lock().await;

        let now = Instant::now();
        if now.duration_since(*window_start) > Duration::from_secs(60) {
            *counter = 0;
            *window_start = now;
        }

        if *counter >= limit {
            return Err(SecurityError::RateLimitExceeded(
                "Global rate limit exceeded. Please try again later.".to_string(),
            ));
        }

        *counter += 1;
        Ok(())
    }

    async fn cleanup_expired_records(&self) {
        let now = Instant::now();
        let cleanup_threshold = Duration::from_secs(3600);

        self.attempts.retain(|_, record| {
            if let Some(locked_until) = record.locked_until {
                now < locked_until + cleanup_threshold
            } else {
                now.duration_since(record.last_attempt) < cleanup_threshold
            }
        });
    }

    pub fn get_stats(&self) -> RateLimiterStats {
        let active_locks = self
            .attempts
            .iter()
            .filter(|entry| {
                if let Some(locked_until) = entry.locked_until {
                    Instant::now() < locked_until
                } else {
                    false
                }
            })
            .count();

        let total_tracked = self.attempts.len();

        RateLimiterStats {
            active_locks,
            total_tracked_identifiers: total_tracked,
            config: self.config.as_ref().clone(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RateLimiterStats {
    pub active_locks: usize,
    pub total_tracked_identifiers: usize,
    pub config: RateLimitConfig,
}

pub struct AuthRateLimiter {
    password_limiter: TimingAttackRateLimiter,
    token_limiter: TimingAttackRateLimiter,
    mfa_limiter: TimingAttackRateLimiter,
    ip_limiter: TimingAttackRateLimiter,
}

impl AuthRateLimiter {
    pub fn new() -> Self {
        let password_config = RateLimitConfig {
            max_attempts: 5,
            time_window: Duration::from_secs(300),
            lockout_duration: Duration::from_secs(900),
            exponential_backoff: true,
            global_rate_limit: Some(100),
        };

        let token_config = RateLimitConfig {
            max_attempts: 20,
            time_window: Duration::from_secs(60),
            lockout_duration: Duration::from_secs(300),
            exponential_backoff: false,
            global_rate_limit: Some(1000),
        };

        let mfa_config = RateLimitConfig {
            max_attempts: 3,
            time_window: Duration::from_secs(300),
            lockout_duration: Duration::from_secs(1800),
            exponential_backoff: true,
            global_rate_limit: Some(50),
        };

        let ip_config = RateLimitConfig {
            max_attempts: 50,
            time_window: Duration::from_secs(300),
            lockout_duration: Duration::from_secs(600),
            exponential_backoff: true,
            global_rate_limit: Some(500),
        };

        Self {
            password_limiter: TimingAttackRateLimiter::new(password_config),
            token_limiter: TimingAttackRateLimiter::new(token_config),
            mfa_limiter: TimingAttackRateLimiter::new(mfa_config),
            ip_limiter: TimingAttackRateLimiter::new(ip_config),
        }
    }

    pub async fn check_password_attempt(&self, username: &str, ip: &str) -> SecurityResult<()> {
        self.ip_limiter.check_and_record_attempt(ip).await?;
        self.password_limiter
            .check_and_record_attempt(username)
            .await
    }

    pub async fn check_token_attempt(&self, token_prefix: &str, ip: &str) -> SecurityResult<()> {
        self.ip_limiter.check_and_record_attempt(ip).await?;
        self.token_limiter
            .check_and_record_attempt(token_prefix)
            .await
    }

    pub async fn check_mfa_attempt(&self, username: &str, ip: &str) -> SecurityResult<()> {
        self.ip_limiter.check_and_record_attempt(ip).await?;
        self.mfa_limiter.check_and_record_attempt(username).await
    }

    pub async fn reset_user_limits(&self, username: &str) {
        self.password_limiter.reset_attempts(username).await;
        self.mfa_limiter.reset_attempts(username).await;
    }

    pub fn get_comprehensive_stats(&self) -> AuthRateLimiterStats {
        AuthRateLimiterStats {
            password_stats: self.password_limiter.get_stats(),
            token_stats: self.token_limiter.get_stats(),
            mfa_stats: self.mfa_limiter.get_stats(),
            ip_stats: self.ip_limiter.get_stats(),
        }
    }
}

impl Default for AuthRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize)]
pub struct AuthRateLimiterStats {
    pub password_stats: RateLimiterStats,
    pub token_stats: RateLimiterStats,
    pub mfa_stats: RateLimiterStats,
    pub ip_stats: RateLimiterStats,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_rate_limiting() {
        let config = RateLimitConfig {
            max_attempts: 3,
            time_window: Duration::from_secs(10),
            lockout_duration: Duration::from_secs(5),
            exponential_backoff: false,
            global_rate_limit: None,
        };

        let limiter = TimingAttackRateLimiter::new(config);
        let identifier = "test_user";

        assert!(limiter.check_and_record_attempt(identifier).await.is_ok());
        assert!(limiter.check_and_record_attempt(identifier).await.is_ok());
        assert!(limiter.check_and_record_attempt(identifier).await.is_ok());

        assert!(limiter.check_and_record_attempt(identifier).await.is_err());
        assert!(limiter.is_locked(identifier).await);

        sleep(Duration::from_secs(6)).await;

        assert!(!limiter.is_locked(identifier).await);
        assert!(limiter.check_and_record_attempt(identifier).await.is_ok());
    }

    #[tokio::test]
    async fn test_window_reset() {
        let config = RateLimitConfig {
            max_attempts: 2,
            time_window: Duration::from_secs(1),
            lockout_duration: Duration::from_secs(5),
            exponential_backoff: false,
            global_rate_limit: None,
        };

        let limiter = TimingAttackRateLimiter::new(config);
        let identifier = "test_user_2";

        assert!(limiter.check_and_record_attempt(identifier).await.is_ok());
        assert!(limiter.check_and_record_attempt(identifier).await.is_ok());

        sleep(Duration::from_secs(2)).await;

        assert!(limiter.check_and_record_attempt(identifier).await.is_ok());
    }

    #[tokio::test]
    async fn test_exponential_backoff() {
        let config = RateLimitConfig {
            max_attempts: 1,
            time_window: Duration::from_secs(10),
            lockout_duration: Duration::from_millis(100),
            exponential_backoff: true,
            global_rate_limit: None,
        };

        let limiter = TimingAttackRateLimiter::new(config);
        let identifier = "test_user_3";

        assert!(limiter.check_and_record_attempt(identifier).await.is_ok());
        assert!(limiter.check_and_record_attempt(identifier).await.is_err());

        sleep(Duration::from_millis(150)).await;

        assert!(limiter.check_and_record_attempt(identifier).await.is_ok());
        assert!(limiter.check_and_record_attempt(identifier).await.is_err());

        sleep(Duration::from_millis(250)).await;

        assert!(!limiter.is_locked(identifier).await);
    }
}
