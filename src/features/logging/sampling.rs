use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tracing::Level;

#[derive(Debug, Clone)]
pub struct SamplingConfig {
    pub enabled: bool,
    pub default_rate: f64,
    pub level_rates: HashMap<Level, f64>,
    pub operation_rates: HashMap<String, f64>,
    pub adaptive_sampling: AdaptiveSamplingConfig,
}

#[derive(Debug, Clone)]
pub struct AdaptiveSamplingConfig {
    pub enabled: bool,
    pub target_samples_per_second: u64,
    pub adjustment_interval: Duration,
    pub min_rate: f64,
    pub max_rate: f64,
}

impl Default for AdaptiveSamplingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            target_samples_per_second: 1000,
            adjustment_interval: Duration::from_secs(60),
            min_rate: 0.001,
            max_rate: 1.0,
        }
    }
}

pub struct Sampler {
    config: SamplingConfig,
    operation_stats: Arc<RwLock<HashMap<String, OperationSamplingStats>>>,
    last_adjustment: Arc<RwLock<Instant>>,
}

#[derive(Debug, Clone)]
struct OperationSamplingStats {
    total_requests: u64,
    sampled_requests: u64,
    current_rate: f64,
    last_reset: Instant,
    recent_request_count: u64,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        let mut level_rates = HashMap::new();
        level_rates.insert(Level::TRACE, 0.01);
        level_rates.insert(Level::DEBUG, 0.1);
        level_rates.insert(Level::INFO, 1.0);
        level_rates.insert(Level::WARN, 1.0);
        level_rates.insert(Level::ERROR, 1.0);

        Self {
            enabled: true,
            default_rate: 0.1,
            level_rates,
            operation_rates: HashMap::new(),
            adaptive_sampling: AdaptiveSamplingConfig {
                enabled: true,
                target_samples_per_second: 100,
                adjustment_interval: Duration::from_secs(60),
                min_rate: 0.001,
                max_rate: 1.0,
            },
        }
    }
}

impl Sampler {
    pub fn new(config: SamplingConfig) -> Self {
        Self {
            config,
            operation_stats: Arc::new(RwLock::new(HashMap::new())),
            last_adjustment: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub fn should_sample(&self, level: Level, operation: Option<&str>) -> bool {
        if !self.config.enabled {
            return true;
        }

        let rate = self.get_sampling_rate(level, operation);

        if rate >= 1.0 {
            return true;
        }

        if rate <= 0.0 {
            return false;
        }

        // Update stats
        if let Some(op) = operation {
            self.update_operation_stats(op, rate);
        }

        rand::rng().random::<f64>() < rate
    }

    fn get_sampling_rate(&self, level: Level, operation: Option<&str>) -> f64 {
        // Check for operation-specific rate first
        if let Some(op) = operation {
            if let Some(&rate) = self.config.operation_rates.get(op) {
                return self.adjust_rate_if_adaptive(op, rate);
            }
        }

        // Check for level-specific rate
        if let Some(&rate) = self.config.level_rates.get(&level) {
            return rate;
        }

        // Use default rate
        self.config.default_rate
    }

    fn adjust_rate_if_adaptive(&self, operation: &str, base_rate: f64) -> f64 {
        if !self.config.adaptive_sampling.enabled {
            return base_rate;
        }

        let stats = self.operation_stats.read().unwrap();
        if let Some(op_stats) = stats.get(operation) {
            return op_stats.current_rate;
        }

        base_rate
    }

    fn update_operation_stats(&self, operation: &str, rate: f64) {
        let mut stats = self.operation_stats.write().unwrap();
        let entry = stats
            .entry(operation.to_string())
            .or_insert_with(|| OperationSamplingStats {
                total_requests: 0,
                sampled_requests: 0,
                current_rate: rate,
                last_reset: Instant::now(),
                recent_request_count: 0,
            });

        entry.total_requests += 1;
        entry.recent_request_count += 1;

        if rand::rng().random::<f64>() < rate {
            entry.sampled_requests += 1;
        }
    }

    pub fn adjust_rates(&self) {
        if !self.config.adaptive_sampling.enabled {
            return;
        }

        let mut last_adjustment = self.last_adjustment.write().unwrap();
        let now = Instant::now();

        if now.duration_since(*last_adjustment) < self.config.adaptive_sampling.adjustment_interval
        {
            return;
        }

        *last_adjustment = now;

        let mut stats = self.operation_stats.write().unwrap();
        let target_rate = self.config.adaptive_sampling.target_samples_per_second as f64;
        let interval_secs = self
            .config
            .adaptive_sampling
            .adjustment_interval
            .as_secs_f64();

        for (operation, op_stats) in stats.iter_mut() {
            let current_sample_rate = op_stats.recent_request_count as f64 / interval_secs;

            if current_sample_rate > target_rate * 1.1 {
                // Too many samples, reduce rate
                op_stats.current_rate *= 0.8;
                op_stats.current_rate = op_stats
                    .current_rate
                    .max(self.config.adaptive_sampling.min_rate);
            } else if current_sample_rate < target_rate * 0.9 {
                // Too few samples, increase rate
                op_stats.current_rate *= 1.2;
                op_stats.current_rate = op_stats
                    .current_rate
                    .min(self.config.adaptive_sampling.max_rate);
            }

            // Reset recent counters
            op_stats.recent_request_count = 0;
            op_stats.last_reset = now;

            tracing::debug!(
                operation = operation,
                current_rate = op_stats.current_rate,
                sample_rate_per_sec = current_sample_rate,
                target_rate = target_rate,
                "Adjusted sampling rate"
            );
        }
    }

    pub fn get_sampling_stats(&self) -> SamplingStats {
        let stats = self.operation_stats.read().unwrap();
        let mut operation_stats = HashMap::new();

        for (operation, op_stats) in stats.iter() {
            operation_stats.insert(
                operation.clone(),
                OperationStats {
                    total_requests: op_stats.total_requests,
                    sampled_requests: op_stats.sampled_requests,
                    current_rate: op_stats.current_rate,
                    sampling_ratio: if op_stats.total_requests > 0 {
                        op_stats.sampled_requests as f64 / op_stats.total_requests as f64
                    } else {
                        0.0
                    },
                },
            );
        }

        SamplingStats {
            enabled: self.config.enabled,
            adaptive_enabled: self.config.adaptive_sampling.enabled,
            operation_stats,
        }
    }

    pub fn reset_stats(&self) {
        let mut stats = self.operation_stats.write().unwrap();
        stats.clear();
    }
}

#[derive(Debug, Clone)]
pub struct SamplingStats {
    pub enabled: bool,
    pub adaptive_enabled: bool,
    pub operation_stats: HashMap<String, OperationStats>,
}

#[derive(Debug, Clone)]
pub struct OperationStats {
    pub total_requests: u64,
    pub sampled_requests: u64,
    pub current_rate: f64,
    pub sampling_ratio: f64,
}

pub struct SamplingDecision {
    pub should_sample: bool,
    pub rate: f64,
    pub reason: String,
}

pub struct AdvancedSampler {
    base_sampler: Sampler,
    burst_detection: BurstDetector,
    rate_limiter: RateLimiter,
}

struct BurstDetector {
    window_size: Duration,
    threshold: u64,
    request_times: Arc<RwLock<Vec<Instant>>>,
}

struct RateLimiter {
    max_samples_per_second: u64,
    current_period: Arc<RwLock<Instant>>,
    current_count: Arc<RwLock<u64>>,
}

impl BurstDetector {
    fn new(window_size: Duration, threshold: u64) -> Self {
        Self {
            window_size,
            threshold,
            request_times: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn is_burst(&self) -> bool {
        let now = Instant::now();
        let mut times = self.request_times.write().unwrap();

        // Add current request
        times.push(now);

        // Remove old requests outside the window
        let cutoff = now - self.window_size;
        times.retain(|&time| time > cutoff);

        times.len() as u64 > self.threshold
    }
}

impl RateLimiter {
    fn new(max_samples_per_second: u64) -> Self {
        Self {
            max_samples_per_second,
            current_period: Arc::new(RwLock::new(Instant::now())),
            current_count: Arc::new(RwLock::new(0)),
        }
    }

    fn should_allow(&self) -> bool {
        let now = Instant::now();
        let mut period = self.current_period.write().unwrap();
        let mut count = self.current_count.write().unwrap();

        // Check if we need to reset the period
        if now.duration_since(*period) >= Duration::from_secs(1) {
            *period = now;
            *count = 0;
        }

        if *count < self.max_samples_per_second {
            *count += 1;
            true
        } else {
            false
        }
    }
}

impl AdvancedSampler {
    pub fn new(config: SamplingConfig, max_samples_per_second: u64) -> Self {
        Self {
            base_sampler: Sampler::new(config),
            burst_detection: BurstDetector::new(Duration::from_secs(5), 1000),
            rate_limiter: RateLimiter::new(max_samples_per_second),
        }
    }

    pub fn make_sampling_decision(
        &self,
        level: Level,
        operation: Option<&str>,
    ) -> SamplingDecision {
        // First check rate limiter
        if !self.rate_limiter.should_allow() {
            return SamplingDecision {
                should_sample: false,
                rate: 0.0,
                reason: "Rate limit exceeded".to_string(),
            };
        }

        // Check for burst conditions
        if self.burst_detection.is_burst() {
            return SamplingDecision {
                should_sample: false,
                rate: 0.0,
                reason: "Burst detected - temporarily reducing sampling".to_string(),
            };
        }

        // Use base sampling logic
        let should_sample = self.base_sampler.should_sample(level, operation);
        let rate = self.base_sampler.get_sampling_rate(level, operation);

        SamplingDecision {
            should_sample,
            rate,
            reason: if should_sample {
                "Normal sampling".to_string()
            } else {
                "Filtered by sampling rate".to_string()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_sampling() {
        let mut config = SamplingConfig::default();
        config.level_rates.insert(Level::DEBUG, 0.5);

        let sampler = Sampler::new(config);

        // Test multiple samples to verify rate is approximately correct
        let total_samples = 10000;
        let mut sampled_count = 0;

        for _ in 0..total_samples {
            if sampler.should_sample(Level::DEBUG, None) {
                sampled_count += 1;
            }
        }

        let actual_rate = sampled_count as f64 / total_samples as f64;
        assert!((actual_rate - 0.5).abs() < 0.05); // Within 5% of expected rate
    }

    #[test]
    fn test_operation_specific_sampling() {
        let mut config = SamplingConfig::default();
        config.operation_rates.insert("fast_op".to_string(), 0.1);
        config.operation_rates.insert("slow_op".to_string(), 1.0);

        let sampler = Sampler::new(config);

        assert!(sampler.should_sample(Level::INFO, Some("slow_op")));

        // Test fast_op sampling
        let mut sampled_count = 0;
        for _ in 0..1000 {
            if sampler.should_sample(Level::INFO, Some("fast_op")) {
                sampled_count += 1;
            }
        }

        let actual_rate = sampled_count as f64 / 1000.0;
        assert!((actual_rate - 0.1).abs() < 0.05);
    }

    #[test]
    fn test_rate_limiter() {
        let rate_limiter = RateLimiter::new(10);

        // Should allow first 10 requests
        for _ in 0..10 {
            assert!(rate_limiter.should_allow());
        }

        // Should reject 11th request
        assert!(!rate_limiter.should_allow());
    }

    #[test]
    fn test_burst_detector() {
        let burst_detector = BurstDetector::new(Duration::from_millis(100), 5);

        // Should not detect burst initially
        assert!(!burst_detector.is_burst());

        // Add requests quickly to trigger burst detection
        for _ in 0..6 {
            burst_detector.is_burst();
        }

        // Should detect burst now
        assert!(burst_detector.is_burst());
    }

    #[test]
    fn test_sampling_stats() {
        let config = SamplingConfig::default();
        let sampler = Sampler::new(config);

        // Generate some sampling activity
        for _ in 0..100 {
            sampler.should_sample(Level::INFO, Some("test_op"));
        }

        let stats = sampler.get_sampling_stats();
        assert!(stats.enabled);
        assert!(stats.operation_stats.contains_key("test_op"));

        let op_stats = &stats.operation_stats["test_op"];
        assert_eq!(op_stats.total_requests, 100);
    }
}
