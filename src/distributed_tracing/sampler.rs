//! Trace Sampling Strategies
//!
//! Provides various sampling strategies to control trace collection overhead
//! while maintaining observability for critical operations.

use super::{TraceContext, TraceSampler};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use rand::{Rng, RngCore, SeedableRng};
use rand::rngs::StdRng;

/// Advanced sampling decision with reasoning
#[derive(Debug, Clone)]
pub struct SamplingDecision {
    pub should_sample: bool,
    pub reason: String,
    pub sampling_rate: f64,
    pub attributes: HashMap<String, String>,
}

impl SamplingDecision {
    pub fn sample(reason: String, rate: f64) -> Self {
        Self {
            should_sample: true,
            reason,
            sampling_rate: rate,
            attributes: HashMap::new(),
        }
    }
    
    pub fn drop(reason: String) -> Self {
        Self {
            should_sample: false,
            reason,
            sampling_rate: 0.0,
            attributes: HashMap::new(),
        }
    }
    
    pub fn with_attribute(mut self, key: String, value: String) -> Self {
        self.attributes.insert(key, value);
        self
    }
}

/// Enhanced sampler trait with detailed decision information
pub trait AdvancedSampler {
    fn sampling_decision(&self, context: &TraceContext, operation_name: &str) -> SamplingDecision;
}

/// Implement AdvancedSampler for the basic TraceSampler trait
impl<T: TraceSampler> AdvancedSampler for T {
    fn sampling_decision(&self, context: &TraceContext, operation_name: &str) -> SamplingDecision {
        if self.should_sample(context, operation_name) {
            SamplingDecision::sample("basic_sampler".to_string(), 1.0)
        } else {
            SamplingDecision::drop("basic_sampler".to_string())
        }
    }
}

/// Probability-based sampler with configurable rates
pub struct ProbabilitySampler {
    sampling_rate: f64,
    rng: Arc<Mutex<StdRng>>,
}

impl ProbabilitySampler {
    pub fn new(sampling_rate: f64) -> Self {
        Self {
            sampling_rate: sampling_rate.clamp(0.0, 1.0),
            rng: Arc::new(Mutex::new(StdRng::seed_from_u64(rand::rng().next_u64()))),
        }
    }
}

impl TraceSampler for ProbabilitySampler {
    fn should_sample(&self, _context: &TraceContext, _operation_name: &str) -> bool {
        if let Ok(mut rng) = self.rng.lock() {
            rng.random::<f64>() < self.sampling_rate
        } else {
            false
        }
    }
}


/// Rate-limiting sampler that limits samples per time window
pub struct RateLimitingSampler {
    max_samples_per_second: u64,
    window_duration: Duration,
    samples: Arc<Mutex<Vec<Instant>>>,
}

impl RateLimitingSampler {
    pub fn new(max_samples_per_second: u64) -> Self {
        Self {
            max_samples_per_second,
            window_duration: Duration::from_secs(1),
            samples: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    pub fn with_window(mut self, window: Duration) -> Self {
        self.window_duration = window;
        self
    }
    
    fn can_sample(&self) -> bool {
        if let Ok(mut samples) = self.samples.lock() {
            let now = Instant::now();
            
            // Remove old samples outside the window
            samples.retain(|&sample_time| now.duration_since(sample_time) <= self.window_duration);
            
            // Check if we can add another sample
            if samples.len() < self.max_samples_per_second as usize {
                samples.push(now);
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}

impl TraceSampler for RateLimitingSampler {
    fn should_sample(&self, _context: &TraceContext, _operation_name: &str) -> bool {
        self.can_sample()
    }
}


/// Operation-aware sampler with different rates for different operations
pub struct OperationBasedSampler {
    default_rate: f64,
    operation_rates: Arc<RwLock<HashMap<String, f64>>>,
    fallback_sampler: ProbabilitySampler,
}

impl OperationBasedSampler {
    pub fn new(default_rate: f64) -> Self {
        Self {
            default_rate: default_rate.clamp(0.0, 1.0),
            operation_rates: Arc::new(RwLock::new(HashMap::new())),
            fallback_sampler: ProbabilitySampler::new(default_rate),
        }
    }
    
    /// Set sampling rate for specific operation
    pub fn set_operation_rate(&self, operation: String, rate: f64) -> &Self {
        if let Ok(mut rates) = self.operation_rates.write() {
            rates.insert(operation, rate.clamp(0.0, 1.0));
        }
        self
    }
    
    /// Set high sampling rate for critical operations
    pub fn high_priority_operations(&self, operations: Vec<String>) -> &Self {
        for op in operations {
            self.set_operation_rate(op, 1.0);
        }
        self
    }
    
    /// Set low sampling rate for noisy operations
    pub fn low_priority_operations(&self, operations: Vec<String>) -> &Self {
        for op in operations {
            self.set_operation_rate(op, 0.1);
        }
        self
    }
    
    fn get_rate_for_operation(&self, operation_name: &str) -> f64 {
        if let Ok(rates) = self.operation_rates.read() {
            rates.get(operation_name).copied().unwrap_or(self.default_rate)
        } else {
            self.default_rate
        }
    }
}

impl TraceSampler for OperationBasedSampler {
    fn should_sample(&self, _context: &TraceContext, operation_name: &str) -> bool {
        let rate = self.get_rate_for_operation(operation_name);
        rand::rng().random::<f64>() < rate
    }
}


/// Adaptive sampler that adjusts rates based on system load
pub struct AdaptiveSampler {
    base_rate: f64,
    min_rate: f64,
    max_rate: f64,
    current_rate: Arc<Mutex<f64>>,
    load_monitor: Arc<Mutex<SystemLoadMonitor>>,
}

impl AdaptiveSampler {
    pub fn new(base_rate: f64) -> Self {
        Self {
            base_rate: base_rate.clamp(0.0, 1.0),
            min_rate: 0.01,
            max_rate: 1.0,
            current_rate: Arc::new(Mutex::new(base_rate)),
            load_monitor: Arc::new(Mutex::new(SystemLoadMonitor::new())),
        }
    }
    
    pub fn with_rate_bounds(mut self, min_rate: f64, max_rate: f64) -> Self {
        self.min_rate = min_rate.clamp(0.0, 1.0);
        self.max_rate = max_rate.clamp(0.0, 1.0);
        self
    }
    
    /// Update sampling rate based on system metrics
    pub fn update_rate_based_on_load(&self) {
        if let (Ok(mut current_rate), Ok(mut monitor)) = (
            self.current_rate.lock(),
            self.load_monitor.lock()
        ) {
            let load_factor = monitor.get_load_factor();
            
            // Adjust rate based on load (higher load = lower sampling rate)
            let adjustment = if load_factor > 0.8 {
                0.5 // Reduce sampling by 50% under high load
            } else if load_factor > 0.6 {
                0.8 // Reduce sampling by 20% under medium load
            } else {
                1.0 // Normal sampling under low load
            };
            
            let new_rate = (self.base_rate * adjustment).clamp(self.min_rate, self.max_rate);
            *current_rate = new_rate;
        }
    }
    
    fn get_current_rate(&self) -> f64 {
        if let Ok(rate) = self.current_rate.lock() {
            *rate
        } else {
            self.base_rate
        }
    }
}

impl TraceSampler for AdaptiveSampler {
    fn should_sample(&self, _context: &TraceContext, _operation_name: &str) -> bool {
        let rate = self.get_current_rate();
        rand::rng().random::<f64>() < rate
    }
}


/// Composite sampler that combines multiple sampling strategies
pub struct CompositeSampler {
    samplers: Vec<Box<dyn AdvancedSampler + Send + Sync>>,
    strategy: CompositeStrategy,
}

#[derive(Debug, Clone)]
pub enum CompositeStrategy {
    /// All samplers must agree to sample
    All,
    /// Any sampler can decide to sample
    Any,
    /// Use the first sampler that has a decision
    First,
    /// Weighted combination of sampler decisions
    Weighted(Vec<f64>),
}

impl CompositeSampler {
    pub fn new(strategy: CompositeStrategy) -> Self {
        Self {
            samplers: Vec::new(),
            strategy,
        }
    }
    
    pub fn add_sampler(mut self, sampler: Box<dyn AdvancedSampler + Send + Sync>) -> Self {
        self.samplers.push(sampler);
        self
    }
    
    pub fn with_probability(self, rate: f64) -> Self {
        self.add_sampler(Box::new(ProbabilitySampler::new(rate)))
    }
    
    pub fn with_rate_limit(self, max_per_second: u64) -> Self {
        self.add_sampler(Box::new(RateLimitingSampler::new(max_per_second)))
    }
    
    pub fn with_operation_based(self, default_rate: f64) -> Self {
        self.add_sampler(Box::new(OperationBasedSampler::new(default_rate)))
    }
}


impl TraceSampler for CompositeSampler {
    fn should_sample(&self, context: &TraceContext, operation_name: &str) -> bool {
        if self.samplers.is_empty() {
            return false;
        }
        
        let decisions: Vec<bool> = self.samplers
            .iter()
            .map(|sampler| sampler.sampling_decision(context, operation_name).should_sample)
            .collect();
        
        match &self.strategy {
            CompositeStrategy::All => decisions.iter().all(|&d| d),
            CompositeStrategy::Any => decisions.iter().any(|&d| d),
            CompositeStrategy::First => decisions.first().copied().unwrap_or(false),
            CompositeStrategy::Weighted(weights) => {
                let weighted_sum: f64 = decisions.iter()
                    .zip(weights.iter())
                    .map(|(decision, weight)| if *decision { *weight } else { 0.0 })
                    .sum();
                
                let total_weight: f64 = weights.iter().sum();
                if total_weight > 0.0 {
                    weighted_sum / total_weight > 0.5
                } else {
                    false
                }
            }
        }
    }
}

/// System load monitor for adaptive sampling
struct SystemLoadMonitor {
    last_update: Instant,
    current_load: f64,
}

impl SystemLoadMonitor {
    fn new() -> Self {
        Self {
            last_update: Instant::now(),
            current_load: 0.0,
        }
    }
    
    fn get_load_factor(&mut self) -> f64 {
        let now = Instant::now();
        
        // Update load every second
        if now.duration_since(self.last_update) >= Duration::from_secs(1) {
            self.current_load = self.measure_system_load();
            self.last_update = now;
        }
        
        self.current_load
    }
    
    fn measure_system_load(&self) -> f64 {
        // Simplified load measurement
        // In production, this would use system APIs to measure:
        // - CPU usage
        // - Memory usage
        // - I/O wait time
        // - Number of active traces
        
        // For now, return a mock value
        0.3
    }
}

/// Sampler configuration for easy setup
#[derive(Debug, Clone)]
pub struct SamplerConfig {
    pub default_rate: f64,
    pub high_priority_operations: Vec<String>,
    pub low_priority_operations: Vec<String>,
    pub max_samples_per_second: Option<u64>,
    pub adaptive: bool,
}

impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            default_rate: 0.1, // 10% sampling by default
            high_priority_operations: vec![
                "db.transaction.commit".to_string(),
                "db.transaction.rollback".to_string(),
                "db.recovery".to_string(),
                "db.backup".to_string(),
            ],
            low_priority_operations: vec![
                "db.get".to_string(),
                "db.cache.hit".to_string(),
            ],
            max_samples_per_second: Some(100),
            adaptive: true,
        }
    }
}

impl SamplerConfig {
    pub fn build(self) -> Box<dyn AdvancedSampler + Send + Sync> {
        let mut composite = CompositeSampler::new(CompositeStrategy::All);
        
        // Add operation-based sampler
        let op_sampler = OperationBasedSampler::new(self.default_rate);
        op_sampler.high_priority_operations(self.high_priority_operations);
        op_sampler.low_priority_operations(self.low_priority_operations);
        composite = composite.add_sampler(Box::new(op_sampler));
        
        // Add rate limiting if configured
        if let Some(max_rate) = self.max_samples_per_second {
            composite = composite.add_sampler(Box::new(RateLimitingSampler::new(max_rate)));
        }
        
        // Add adaptive sampling if enabled
        if self.adaptive {
            composite = composite.add_sampler(Box::new(AdaptiveSampler::new(self.default_rate)));
        }
        
        Box::new(composite)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_tracing::TraceContext;

    #[test]
    fn test_probability_sampler() {
        let sampler = ProbabilitySampler::new(0.5);
        let context = TraceContext::new_root();
        
        let mut sampled = 0;
        for _ in 0..1000 {
            if sampler.should_sample(&context, "test") {
                sampled += 1;
            }
        }
        
        // Should be approximately 500 with some variance
        assert!(sampled >= 400 && sampled <= 600);
    }

    #[test]
    fn test_rate_limiting_sampler() {
        let sampler = RateLimitingSampler::new(5);
        let context = TraceContext::new_root();
        
        let mut sampled = 0;
        for _ in 0..10 {
            if sampler.should_sample(&context, "test") {
                sampled += 1;
            }
        }
        
        assert_eq!(sampled, 5); // Should only sample 5 times
    }

    #[test]
    fn test_operation_based_sampler() {
        let sampler = OperationBasedSampler::new(0.1);
        sampler.set_operation_rate("critical_op".to_string(), 1.0);
        sampler.set_operation_rate("low_priority".to_string(), 0.01);
        
        let context = TraceContext::new_root();
        
        // Critical operation should always be sampled
        let mut critical_sampled = 0;
        for _ in 0..100 {
            if sampler.should_sample(&context, "critical_op") {
                critical_sampled += 1;
            }
        }
        assert!(critical_sampled >= 95); // Should be close to 100%
        
        // Low priority operation should rarely be sampled
        let mut low_priority_sampled = 0;
        for _ in 0..1000 {
            if sampler.should_sample(&context, "low_priority") {
                low_priority_sampled += 1;
            }
        }
        assert!(low_priority_sampled <= 50); // Should be close to 1%
    }

    #[test]
    fn test_sampling_decision() {
        let decision = SamplingDecision::sample("test_reason".to_string(), 0.5)
            .with_attribute("key".to_string(), "value".to_string());
        
        assert!(decision.should_sample);
        assert_eq!(decision.reason, "test_reason");
        assert_eq!(decision.sampling_rate, 0.5);
        assert_eq!(decision.attributes.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_composite_sampler_all() {
        let sampler = CompositeSampler::new(CompositeStrategy::All)
            .with_probability(1.0)  // Always sample
            .with_probability(0.0); // Never sample
        
        let context = TraceContext::new_root();
        
        // Should never sample because one sampler always says no
        assert!(!sampler.should_sample(&context, "test"));
    }

    #[test]
    fn test_composite_sampler_any() {
        let sampler = CompositeSampler::new(CompositeStrategy::Any)
            .with_probability(1.0)  // Always sample
            .with_probability(0.0); // Never sample
        
        let context = TraceContext::new_root();
        
        // Should always sample because one sampler always says yes
        assert!(sampler.should_sample(&context, "test"));
    }

    #[test]
    fn test_sampler_config() {
        let config = SamplerConfig {
            default_rate: 0.2,
            high_priority_operations: vec!["critical".to_string()],
            low_priority_operations: vec!["noise".to_string()],
            max_samples_per_second: Some(50),
            adaptive: false,
        };
        
        let sampler = config.build();
        let context = TraceContext::new_root();
        
        // Should create a working composite sampler
        let decision = sampler.sampling_decision(&context, "test");
        assert!(!decision.reason.is_empty());
    }
}