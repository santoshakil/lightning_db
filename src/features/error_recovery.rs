use crate::core::error::{Error, Result};
use parking_lot::{RwLock, Mutex};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use std::future::Future;
use tokio::time::sleep;
use tracing::{debug, info, warn, error};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCategory {
    Transient,
    Recoverable,
    Critical,
    Fatal,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryStrategy {
    Retry,
    CircuitBreaker,
    Fallback,
    Checkpoint,
    Restart,
    None,
}

#[derive(Debug, Clone)]
pub struct ErrorClassifier {
    rules: Vec<ClassificationRule>,
}

#[derive(Debug, Clone)]
struct ClassificationRule {
    pattern: String,
    category: ErrorCategory,
    strategy: RecoveryStrategy,
}

impl ErrorClassifier {
    pub fn new() -> Self {
        let mut rules = Vec::new();
        
        rules.push(ClassificationRule {
            pattern: "connection refused".to_string(),
            category: ErrorCategory::Transient,
            strategy: RecoveryStrategy::Retry,
        });
        
        rules.push(ClassificationRule {
            pattern: "timeout".to_string(),
            category: ErrorCategory::Transient,
            strategy: RecoveryStrategy::CircuitBreaker,
        });
        
        rules.push(ClassificationRule {
            pattern: "out of memory".to_string(),
            category: ErrorCategory::Critical,
            strategy: RecoveryStrategy::Restart,
        });
        
        rules.push(ClassificationRule {
            pattern: "corruption".to_string(),
            category: ErrorCategory::Fatal,
            strategy: RecoveryStrategy::Checkpoint,
        });
        
        Self { rules }
    }
    
    pub fn classify(&self, error: &Error) -> (ErrorCategory, RecoveryStrategy) {
        let error_str = error.to_string().to_lowercase();
        
        for rule in &self.rules {
            if error_str.contains(&rule.pattern) {
                return (rule.category, rule.strategy);
            }
        }
        
        (ErrorCategory::Unknown, RecoveryStrategy::None)
    }
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub exponential_base: f64,
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            exponential_base: 2.0,
            jitter: true,
        }
    }
}

pub struct RetryManager {
    config: RetryConfig,
    attempts: AtomicUsize,
    last_attempt: Mutex<Option<Instant>>,
}

impl RetryManager {
    pub fn new(config: RetryConfig) -> Self {
        Self {
            config,
            attempts: AtomicUsize::new(0),
            last_attempt: Mutex::new(None),
        }
    }
    
    pub async fn execute<F, Fut, T>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt = 0;
        
        loop {
            attempt += 1;
            self.attempts.store(attempt, Ordering::Relaxed);
            
            match operation().await {
                Ok(result) => {
                    if attempt > 1 {
                        info!("Operation succeeded after {} attempts", attempt);
                    }
                    return Ok(result);
                }
                Err(e) if attempt >= self.config.max_attempts => {
                    error!("Operation failed after {} attempts: {}", attempt, e);
                    return Err(e);
                }
                Err(e) => {
                    let delay = self.calculate_delay(attempt);
                    warn!(
                        "Attempt {} failed: {}. Retrying in {:?}",
                        attempt, e, delay
                    );
                    
                    *self.last_attempt.lock() = Some(Instant::now());
                    sleep(delay).await;
                }
            }
        }
    }
    
    fn calculate_delay(&self, attempt: usize) -> Duration {
        let mut delay = self.config.initial_delay.as_millis() as f64
            * self.config.exponential_base.powi(attempt as i32 - 1);
        
        if self.config.jitter {
            use rand::Rng;
            let mut rng = rand::rng();
            delay *= rng.gen_range(0.5..1.5);
        }
        
        let delay = Duration::from_millis(delay as u64);
        delay.min(self.config.max_delay)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

pub struct CircuitBreaker {
    state: Arc<RwLock<CircuitState>>,
    failure_count: AtomicUsize,
    success_count: AtomicUsize,
    last_failure_time: Arc<RwLock<Option<Instant>>>,
    config: CircuitBreakerConfig,
    statistics: Arc<CircuitBreakerStats>,
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: usize,
    pub success_threshold: usize,
    pub timeout: Duration,
    pub half_open_max_calls: usize,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            timeout: Duration::from_secs(60),
            half_open_max_calls: 3,
        }
    }
}

#[derive(Debug, Default)]
struct CircuitBreakerStats {
    total_calls: AtomicU64,
    successful_calls: AtomicU64,
    failed_calls: AtomicU64,
    rejected_calls: AtomicU64,
    state_transitions: AtomicU64,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitState::Closed)),
            failure_count: AtomicUsize::new(0),
            success_count: AtomicUsize::new(0),
            last_failure_time: Arc::new(RwLock::new(None)),
            config,
            statistics: Arc::new(CircuitBreakerStats::default()),
        }
    }
    
    pub async fn call<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.statistics.total_calls.fetch_add(1, Ordering::Relaxed);
        
        if !self.allow_request() {
            self.statistics.rejected_calls.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Generic("Circuit breaker is open".to_string()));
        }
        
        match operation().await {
            Ok(result) => {
                self.on_success();
                self.statistics.successful_calls.fetch_add(1, Ordering::Relaxed);
                Ok(result)
            }
            Err(e) => {
                self.on_failure();
                self.statistics.failed_calls.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }
    
    fn allow_request(&self) -> bool {
        let state = *self.state.read();
        
        match state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last_failure) = *self.last_failure_time.read() {
                    if last_failure.elapsed() >= self.config.timeout {
                        self.transition_to(CircuitState::HalfOpen);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                self.success_count.load(Ordering::Relaxed) + 
                self.failure_count.load(Ordering::Relaxed) < self.config.half_open_max_calls
            }
        }
    }
    
    fn on_success(&self) {
        let state = *self.state.read();
        
        match state {
            CircuitState::Closed => {
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::HalfOpen => {
                let success_count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                
                if success_count >= self.config.success_threshold {
                    self.transition_to(CircuitState::Closed);
                    self.reset_counts();
                }
            }
            _ => {}
        }
    }
    
    fn on_failure(&self) {
        let state = *self.state.read();
        
        match state {
            CircuitState::Closed => {
                let failure_count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                
                if failure_count >= self.config.failure_threshold {
                    self.transition_to(CircuitState::Open);
                    *self.last_failure_time.write() = Some(Instant::now());
                }
            }
            CircuitState::HalfOpen => {
                self.transition_to(CircuitState::Open);
                *self.last_failure_time.write() = Some(Instant::now());
                self.reset_counts();
            }
            _ => {}
        }
    }
    
    fn transition_to(&self, new_state: CircuitState) {
        let mut state = self.state.write();
        if *state != new_state {
            info!("Circuit breaker transitioning from {:?} to {:?}", *state, new_state);
            *state = new_state;
            self.statistics.state_transitions.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    fn reset_counts(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);
    }
    
    pub fn get_state(&self) -> CircuitState {
        *self.state.read()
    }
    
    pub fn reset(&self) {
        self.transition_to(CircuitState::Closed);
        self.reset_counts();
        *self.last_failure_time.write() = None;
    }
}

pub struct Checkpoint {
    id: u64,
    timestamp: SystemTime,
    state: Vec<u8>,
    metadata: HashMap<String, String>,
}

pub struct CheckpointManager {
    checkpoints: Arc<RwLock<VecDeque<Checkpoint>>>,
    max_checkpoints: usize,
    next_id: AtomicU64,
}

impl CheckpointManager {
    pub fn new(max_checkpoints: usize) -> Self {
        Self {
            checkpoints: Arc::new(RwLock::new(VecDeque::new())),
            max_checkpoints,
            next_id: AtomicU64::new(1),
        }
    }
    
    pub fn create_checkpoint(&self, state: Vec<u8>, metadata: HashMap<String, String>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        
        let checkpoint = Checkpoint {
            id,
            timestamp: SystemTime::now(),
            state,
            metadata,
        };
        
        let mut checkpoints = self.checkpoints.write();
        
        if checkpoints.len() >= self.max_checkpoints {
            checkpoints.pop_front();
        }
        
        checkpoints.push_back(checkpoint);
        
        info!("Created checkpoint {}", id);
        id
    }
    
    pub fn restore_checkpoint(&self, id: u64) -> Option<Vec<u8>> {
        let checkpoints = self.checkpoints.read();
        
        checkpoints
            .iter()
            .find(|cp| cp.id == id)
            .map(|cp| {
                info!("Restoring checkpoint {}", id);
                cp.state.clone()
            })
    }
    
    pub fn get_latest_checkpoint(&self) -> Option<Vec<u8>> {
        let checkpoints = self.checkpoints.read();
        
        checkpoints.back().map(|cp| {
            info!("Restoring latest checkpoint {}", cp.id);
            cp.state.clone()
        })
    }
    
    pub fn list_checkpoints(&self) -> Vec<(u64, SystemTime, HashMap<String, String>)> {
        let checkpoints = self.checkpoints.read();
        
        checkpoints
            .iter()
            .map(|cp| (cp.id, cp.timestamp, cp.metadata.clone()))
            .collect()
    }
    
    pub fn delete_checkpoint(&self, id: u64) -> bool {
        let mut checkpoints = self.checkpoints.write();
        
        if let Some(pos) = checkpoints.iter().position(|cp| cp.id == id) {
            checkpoints.remove(pos);
            info!("Deleted checkpoint {}", id);
            true
        } else {
            false
        }
    }
}

pub struct RecoveryCoordinator {
    error_classifier: ErrorClassifier,
    retry_managers: HashMap<String, Arc<RetryManager>>,
    circuit_breakers: HashMap<String, Arc<CircuitBreaker>>,
    checkpoint_manager: Arc<CheckpointManager>,
    fallback_handlers: HashMap<String, Arc<dyn Fn() -> Result<Vec<u8>> + Send + Sync>>,
    statistics: Arc<RecoveryStatistics>,
}

#[derive(Debug, Default)]
struct RecoveryStatistics {
    total_recoveries: AtomicU64,
    successful_recoveries: AtomicU64,
    failed_recoveries: AtomicU64,
    checkpoints_created: AtomicU64,
    checkpoints_restored: AtomicU64,
}

impl RecoveryCoordinator {
    pub fn new() -> Self {
        Self {
            error_classifier: ErrorClassifier::new(),
            retry_managers: HashMap::new(),
            circuit_breakers: HashMap::new(),
            checkpoint_manager: Arc::new(CheckpointManager::new(10)),
            fallback_handlers: HashMap::new(),
            statistics: Arc::new(RecoveryStatistics::default()),
        }
    }
    
    pub fn register_retry_manager(&mut self, name: String, config: RetryConfig) {
        self.retry_managers.insert(name, Arc::new(RetryManager::new(config)));
    }
    
    pub fn register_circuit_breaker(&mut self, name: String, config: CircuitBreakerConfig) {
        self.circuit_breakers.insert(name, Arc::new(CircuitBreaker::new(config)));
    }
    
    pub fn register_fallback<F>(&mut self, name: String, handler: F)
    where
        F: Fn() -> Result<Vec<u8>> + Send + Sync + 'static,
    {
        self.fallback_handlers.insert(name, Arc::new(handler));
    }
    
    pub async fn handle_error(&self, error: Error, context: &str) -> Result<Vec<u8>> {
        self.statistics.total_recoveries.fetch_add(1, Ordering::Relaxed);
        
        let (category, strategy) = self.error_classifier.classify(&error);
        
        info!(
            "Handling error in context '{}': category={:?}, strategy={:?}",
            context, category, strategy
        );
        
        let result = match strategy {
            RecoveryStrategy::Retry => {
                if let Some(retry_manager) = self.retry_managers.get(context) {
                    retry_manager.execute(|| async {
                        Err(error.clone())
                    }).await
                } else {
                    Err(error)
                }
            }
            RecoveryStrategy::CircuitBreaker => {
                if let Some(circuit_breaker) = self.circuit_breakers.get(context) {
                    circuit_breaker.call(|| async {
                        Err(error.clone())
                    }).await
                } else {
                    Err(error)
                }
            }
            RecoveryStrategy::Fallback => {
                if let Some(handler) = self.fallback_handlers.get(context) {
                    handler()
                } else {
                    Err(error)
                }
            }
            RecoveryStrategy::Checkpoint => {
                if let Some(state) = self.checkpoint_manager.get_latest_checkpoint() {
                    self.statistics.checkpoints_restored.fetch_add(1, Ordering::Relaxed);
                    Ok(state)
                } else {
                    Err(error)
                }
            }
            RecoveryStrategy::Restart => {
                warn!("Restart strategy requested for error: {}", error);
                Err(error)
            }
            RecoveryStrategy::None => Err(error),
        };
        
        match &result {
            Ok(_) => {
                self.statistics.successful_recoveries.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.statistics.failed_recoveries.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        result
    }
    
    pub fn create_checkpoint(&self, state: Vec<u8>, metadata: HashMap<String, String>) -> u64 {
        self.statistics.checkpoints_created.fetch_add(1, Ordering::Relaxed);
        self.checkpoint_manager.create_checkpoint(state, metadata)
    }
    
    pub fn get_statistics(&self) -> RecoveryStats {
        RecoveryStats {
            total_recoveries: self.statistics.total_recoveries.load(Ordering::Relaxed),
            successful_recoveries: self.statistics.successful_recoveries.load(Ordering::Relaxed),
            failed_recoveries: self.statistics.failed_recoveries.load(Ordering::Relaxed),
            checkpoints_created: self.statistics.checkpoints_created.load(Ordering::Relaxed),
            checkpoints_restored: self.statistics.checkpoints_restored.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryStats {
    pub total_recoveries: u64,
    pub successful_recoveries: u64,
    pub failed_recoveries: u64,
    pub checkpoints_created: u64,
    pub checkpoints_restored: u64,
}

pub struct SelfHealingSystem {
    health_checks: HashMap<String, Arc<dyn Fn() -> bool + Send + Sync>>,
    repair_actions: HashMap<String, Arc<dyn Fn() -> Result<()> + Send + Sync>>,
    check_interval: Duration,
    running: Arc<AtomicBool>,
    worker_handle: Option<std::thread::JoinHandle<()>>,
}

impl SelfHealingSystem {
    pub fn new(check_interval: Duration) -> Self {
        Self {
            health_checks: HashMap::new(),
            repair_actions: HashMap::new(),
            check_interval,
            running: Arc::new(AtomicBool::new(false)),
            worker_handle: None,
        }
    }
    
    pub fn register_health_check<F>(&mut self, name: String, check: F)
    where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        self.health_checks.insert(name, Arc::new(check));
    }
    
    pub fn register_repair_action<F>(&mut self, name: String, action: F)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        self.repair_actions.insert(name, Arc::new(action));
    }
    
    pub fn start(&mut self) {
        if self.running.load(Ordering::Relaxed) {
            return;
        }
        
        self.running.store(true, Ordering::Relaxed);
        
        let running = self.running.clone();
        let check_interval = self.check_interval;
        let health_checks = self.health_checks.clone();
        let repair_actions = self.repair_actions.clone();
        
        let handle = std::thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                for (name, check) in &health_checks {
                    if !check() {
                        warn!("Health check '{}' failed", name);
                        
                        if let Some(repair) = repair_actions.get(name) {
                            match repair() {
                                Ok(()) => info!("Repair action '{}' succeeded", name),
                                Err(e) => error!("Repair action '{}' failed: {}", name, e),
                            }
                        }
                    }
                }
                
                std::thread::sleep(check_interval);
            }
        });
        
        self.worker_handle = Some(handle);
        info!("Self-healing system started");
    }
    
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        
        if let Some(handle) = self.worker_handle.take() {
            handle.join().ok();
        }
        
        info!("Self-healing system stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_retry_manager() {
        let config = RetryConfig {
            max_attempts: 3,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
            exponential_base: 2.0,
            jitter: false,
        };
        
        let retry_manager = RetryManager::new(config);
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        
        let result = retry_manager.execute(|| {
            let count = counter_clone.fetch_add(1, Ordering::Relaxed);
            async move {
                if count < 2 {
                    Err(Error::Generic("Simulated failure".to_string()))
                } else {
                    Ok(42)
                }
            }
        }).await;
        
        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }
    
    #[tokio::test]
    async fn test_circuit_breaker() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_millis(100),
            half_open_max_calls: 3,
        };
        
        let circuit_breaker = CircuitBreaker::new(config);
        
        for _ in 0..2 {
            let _ = circuit_breaker.call(|| async {
                Err::<(), Error>(Error::Generic("Failed".to_string()))
            }).await;
        }
        
        assert_eq!(circuit_breaker.get_state(), CircuitState::Open);
        
        let result = circuit_breaker.call(|| async {
            Ok(42)
        }).await;
        
        assert!(result.is_err());
        
        tokio::time::sleep(Duration::from_millis(150)).await;
        
        for _ in 0..2 {
            let _ = circuit_breaker.call(|| async {
                Ok(42)
            }).await;
        }
        
        assert_eq!(circuit_breaker.get_state(), CircuitState::Closed);
    }
    
    #[test]
    fn test_checkpoint_manager() {
        let manager = CheckpointManager::new(3);
        
        let id1 = manager.create_checkpoint(vec![1, 2, 3], HashMap::new());
        let id2 = manager.create_checkpoint(vec![4, 5, 6], HashMap::new());
        
        let restored = manager.restore_checkpoint(id1);
        assert_eq!(restored, Some(vec![1, 2, 3]));
        
        let latest = manager.get_latest_checkpoint();
        assert_eq!(latest, Some(vec![4, 5, 6]));
        
        assert!(manager.delete_checkpoint(id2));
        assert!(!manager.delete_checkpoint(999));
    }
    
    #[test]
    fn test_error_classifier() {
        let classifier = ErrorClassifier::new();
        
        let error = Error::Generic("Connection refused".to_string());
        let (category, strategy) = classifier.classify(&error);
        assert_eq!(category, ErrorCategory::Transient);
        assert_eq!(strategy, RecoveryStrategy::Retry);
        
        let error = Error::Generic("Out of memory".to_string());
        let (category, strategy) = classifier.classify(&error);
        assert_eq!(category, ErrorCategory::Critical);
        assert_eq!(strategy, RecoveryStrategy::Restart);
    }
    
    #[test]
    fn test_self_healing_system() {
        let mut system = SelfHealingSystem::new(Duration::from_millis(100));
        
        let healthy = Arc::new(AtomicBool::new(true));
        let healthy_clone = healthy.clone();
        
        system.register_health_check("test".to_string(), move || {
            healthy_clone.load(Ordering::Relaxed)
        });
        
        let repaired = Arc::new(AtomicBool::new(false));
        let repaired_clone = repaired.clone();
        
        system.register_repair_action("test".to_string(), move || {
            repaired_clone.store(true, Ordering::Relaxed);
            Ok(())
        });
        
        system.start();
        healthy.store(false, Ordering::Relaxed);
        
        std::thread::sleep(Duration::from_millis(200));
        
        assert!(repaired.load(Ordering::Relaxed));
        
        system.stop();
    }
}