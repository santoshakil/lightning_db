//! CPU usage throttling for Lightning DB

use crate::{Error, Result};
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::debug;

/// CPU throttle manager
#[derive(Debug)]
pub struct CpuThrottle {
    // CPU limits (as percentage 0-100)
    cpu_quota_percent: Option<f64>,
    background_cpu_percent: Option<f64>,

    // Current tracking
    current_usage: Arc<RwLock<f64>>,
    last_measurement: Arc<Mutex<Instant>>,

    // Throttling state
    throttle_duration: Arc<RwLock<Duration>>,
    throttle_active: Arc<RwLock<bool>>,

    // Statistics
    throttle_events: AtomicU64,
    total_throttle_time: Arc<RwLock<Duration>>,
}

impl CpuThrottle {
    pub fn new(
        cpu_quota_percent: Option<f64>,
        background_cpu_percent: Option<f64>,
    ) -> Result<Self> {
        if let Some(quota) = cpu_quota_percent {
            if quota <= 0.0 || quota > 100.0 {
                return Err(Error::Config("Invalid CPU quota percentage".to_string()));
            }
        }

        if let Some(bg_quota) = background_cpu_percent {
            if bg_quota <= 0.0 || bg_quota > 100.0 {
                return Err(Error::Config(
                    "Invalid background CPU percentage".to_string(),
                ));
            }
        }

        Ok(Self {
            cpu_quota_percent,
            background_cpu_percent,
            current_usage: Arc::new(RwLock::new(0.0)),
            last_measurement: Arc::new(Mutex::new(Instant::now())),
            throttle_duration: Arc::new(RwLock::new(Duration::from_millis(1))),
            throttle_active: Arc::new(RwLock::new(false)),
            throttle_events: AtomicU64::new(0),
            total_throttle_time: Arc::new(RwLock::new(Duration::ZERO)),
        })
    }

    /// Check if CPU usage is within quota
    pub fn check_cpu_quota(&self) -> Result<bool> {
        let Some(quota) = self.cpu_quota_percent else {
            return Ok(true);
        };

        let usage = self.get_current_usage();

        if usage > quota {
            debug!("CPU usage {:.1}% exceeds quota {:.1}%", usage, quota);
            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// Apply CPU throttling if needed
    pub fn apply_throttle(&self, is_background: bool) -> Result<()> {
        let quota = if is_background {
            self.background_cpu_percent.or(self.cpu_quota_percent)
        } else {
            self.cpu_quota_percent
        };

        let Some(limit) = quota else {
            return Ok(());
        };

        let usage = self.get_current_usage();

        if usage > limit {
            let throttle_ratio = (usage - limit) / usage;
            let sleep_time = self.calculate_throttle_duration(throttle_ratio);

            if sleep_time > Duration::ZERO {
                debug!(
                    "Throttling for {:?} (usage: {:.1}%, limit: {:.1}%)",
                    sleep_time, usage, limit
                );

                *self.throttle_active.write() = true;
                self.throttle_events.fetch_add(1, Ordering::Relaxed);

                thread::sleep(sleep_time);

                *self.throttle_active.write() = false;
                *self.total_throttle_time.write() += sleep_time;
            }
        }

        Ok(())
    }

    /// Update CPU usage measurement
    pub fn update_usage(&self, usage_percent: f64) -> Result<()> {
        *self.current_usage.write() = usage_percent;
        *self.last_measurement.lock() = Instant::now();

        // Adjust throttle duration based on usage
        if let Some(quota) = self.cpu_quota_percent {
            if usage_percent > quota {
                // Increase throttle duration
                let mut duration = self.throttle_duration.write();
                *duration = (*duration * 2).min(Duration::from_millis(100));
            } else if usage_percent < quota * 0.8 {
                // Decrease throttle duration
                let mut duration = self.throttle_duration.write();
                *duration = (*duration / 2).max(Duration::from_millis(1));
            }
        }

        Ok(())
    }

    /// Get current CPU usage
    pub fn get_current_usage(&self) -> f64 {
        // In production, this would query actual CPU usage
        // For now, return the last measured value
        *self.current_usage.read()
    }

    /// Calculate throttle duration based on overage
    fn calculate_throttle_duration(&self, overage_ratio: f64) -> Duration {
        let base_duration = *self.throttle_duration.read();
        let multiplier = 1.0 + overage_ratio * 10.0; // More aggressive throttling for higher overage

        Duration::from_secs_f64(base_duration.as_secs_f64() * multiplier)
    }

    /// Get CPU usage from system
    #[cfg(target_os = "linux")]
    pub fn measure_system_cpu(&self) -> Result<f64> {
        use std::fs;

        let stat = fs::read_to_string("/proc/stat")?;
        let cpu_line = stat
            .lines()
            .find(|line| line.starts_with("cpu "))
            .ok_or_else(|| Error::Io("Failed to read CPU stats".to_string()))?;

        let values: Vec<u64> = cpu_line
            .split_whitespace()
            .skip(1)
            .filter_map(|v| v.parse().ok())
            .collect();

        if values.len() < 4 {
            return Err(Error::Io("Invalid CPU stat format".to_string()));
        }

        let user = values[0];
        let nice = values[1];
        let system = values[2];
        let idle = values[3];

        let total = user + nice + system + idle;
        let busy = user + nice + system;

        let usage = (busy as f64 / total as f64) * 100.0;
        self.update_usage(usage)?;

        Ok(usage)
    }

    #[cfg(not(target_os = "linux"))]
    pub fn measure_system_cpu(&self) -> Result<f64> {
        // Placeholder for non-Linux systems
        Ok(50.0)
    }

    /// Get throttle statistics
    pub fn get_stats(&self) -> CpuThrottleStats {
        CpuThrottleStats {
            current_usage: self.get_current_usage(),
            cpu_quota: self.cpu_quota_percent,
            background_quota: self.background_cpu_percent,
            throttle_active: *self.throttle_active.read(),
            throttle_events: self.throttle_events.load(Ordering::Relaxed),
            total_throttle_time: *self.total_throttle_time.read(),
            current_throttle_duration: *self.throttle_duration.read(),
        }
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        self.throttle_events.store(0, Ordering::Relaxed);
        *self.total_throttle_time.write() = Duration::ZERO;
    }
}

#[derive(Debug, Clone)]
pub struct CpuThrottleStats {
    pub current_usage: f64,
    pub cpu_quota: Option<f64>,
    pub background_quota: Option<f64>,
    pub throttle_active: bool,
    pub throttle_events: u64,
    pub total_throttle_time: Duration,
    pub current_throttle_duration: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_quota_check() {
        let throttle = CpuThrottle::new(Some(50.0), None).unwrap();

        // Under quota
        throttle.update_usage(30.0).unwrap();
        assert!(throttle.check_cpu_quota().unwrap());

        // Over quota
        throttle.update_usage(70.0).unwrap();
        assert!(!throttle.check_cpu_quota().unwrap());
    }

    #[test]
    fn test_throttle_duration_adjustment() {
        let throttle = CpuThrottle::new(Some(50.0), None).unwrap();

        let initial_duration = *throttle.throttle_duration.read();

        // High usage should increase throttle duration
        throttle.update_usage(80.0).unwrap();
        let high_duration = *throttle.throttle_duration.read();
        assert!(high_duration > initial_duration);

        // Low usage should decrease throttle duration
        throttle.update_usage(30.0).unwrap();
        let low_duration = *throttle.throttle_duration.read();
        assert!(low_duration < high_duration);
    }

    #[test]
    fn test_background_throttle() {
        let throttle = CpuThrottle::new(Some(80.0), Some(25.0)).unwrap();

        throttle.update_usage(30.0).unwrap();

        // Should throttle background operations at lower threshold
        let start = Instant::now();
        throttle.apply_throttle(true).unwrap();
        let elapsed = start.elapsed();

        // Should have throttled
        assert!(elapsed > Duration::from_millis(1));
        assert_eq!(throttle.throttle_events.load(Ordering::Relaxed), 1);
    }
}
