use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::cmp::Ordering as CmpOrdering;

/// Default rebalancer implementation with intelligent load balancing
pub struct DefaultRebalancer {
    /// Current rebalance operations
    active_operations: RwLock<HashMap<String, RebalanceOperation>>,
    
    /// Rebalance operation counter
    operation_counter: AtomicU64,
    
    /// Progress tracking
    progress: RwLock<RebalanceProgress>,
    
    /// Configuration
    config: RebalanceConfig,
}

/// Individual rebalance operation
#[derive(Debug, Clone)]
pub struct RebalanceOperation {
    /// Operation ID
    pub id: String,
    
    /// Data movement plan
    pub movement: DataMovement,
    
    /// Current state
    pub state: RebalanceState,
    
    /// Progress percentage (0-100)
    pub progress: f64,
    
    /// Start time
    pub started_at: Instant,
    
    /// Estimated completion time
    pub eta: Duration,
    
    /// Error message if failed
    pub error: Option<String>,
}

/// Rebalance operation state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceState {
    /// Pending execution
    Pending,
    /// Currently transferring data
    Transferring,
    /// Verifying data consistency
    Verifying,
    /// Completed successfully
    Completed,
    /// Failed with error
    Failed,
    /// Cancelled by user
    Cancelled,
}

/// Rebalancer configuration
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    /// Maximum concurrent data movements
    pub max_concurrent_movements: u32,
    
    /// Target load balance threshold
    pub load_balance_threshold: f64,
    
    /// Minimum data size to trigger rebalancing
    pub min_rebalance_size: u64,
    
    /// Maximum time for a single operation
    pub operation_timeout: Duration,
    
    /// Data transfer rate limit (bytes/sec)
    pub transfer_rate_limit: Option<u64>,
    
    /// Enable load-based rebalancing
    pub enable_load_balancing: bool,
    
    /// Enable capacity-based rebalancing
    pub enable_capacity_balancing: bool,
    
    /// Safety margin for resource usage
    pub safety_margin: f64,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_movements: 2,
            load_balance_threshold: 20.0, // 20% difference
            min_rebalance_size: 100 * 1024 * 1024, // 100MB
            operation_timeout: Duration::from_secs(3600), // 1 hour
            transfer_rate_limit: Some(100 * 1024 * 1024), // 100MB/s
            enable_load_balancing: true,
            enable_capacity_balancing: true,
            safety_margin: 0.1, // 10% margin
        }
    }
}

/// Shard load metrics for rebalancing decisions
#[derive(Debug, Clone)]
pub struct ShardLoad {
    pub shard_id: ShardId,
    pub key_count: u64,
    pub data_size: u64,
    pub read_ops_per_sec: f64,
    pub write_ops_per_sec: f64,
    pub cpu_usage: f64,
    pub memory_usage: u64,
    pub network_usage: f64,
    pub load_score: f64, // Composite score
}

impl DefaultRebalancer {
    pub fn new() -> Self {
        Self {
            active_operations: RwLock::new(HashMap::new()),
            operation_counter: AtomicU64::new(0),
            progress: RwLock::new(RebalanceProgress {
                completed: 0,
                total: 0,
                data_transferred: 0,
                eta: Duration::ZERO,
                current_operation: None,
            }),
            config: RebalanceConfig::default(),
        }
    }
    
    /// Calculate load score for a shard
    fn calculate_load_score(&self, shard_stats: &ShardStats) -> f64 {
        // Weighted composite score considering multiple factors
        let size_weight = 0.3;
        let ops_weight = 0.3;
        let cpu_weight = 0.2;
        let memory_weight = 0.2;
        
        // Normalize metrics (0-1 scale)
        let size_score = (shard_stats.data_size as f64 / (10_i64 * 1024 * 1024 * 1024) as f64).min(1.0); // 10GB max
        let ops_score = ((shard_stats.read_ops + shard_stats.write_ops) / 10000.0).min(1.0); // 10K ops/sec max
        let cpu_score = shard_stats.cpu_usage / 100.0;
        let memory_score = (shard_stats.memory_usage as f64 / (1024_i64 * 1024 * 1024) as f64).min(1.0); // 1GB max
        
        size_weight * size_score + 
        ops_weight * ops_score + 
        cpu_weight * cpu_score + 
        memory_weight * memory_score
    }
    
    /// Analyze cluster load distribution
    fn analyze_cluster_load(&self, topology: &ClusterTopology) -> Vec<ShardLoad> {
        let shards = topology.shards.read();
        let mut shard_loads = Vec::new();
        
        for (shard_id, shard_info) in shards.iter() {
            let load_score = self.calculate_load_score(&shard_info.stats);
            
            shard_loads.push(ShardLoad {
                shard_id: *shard_id,
                key_count: shard_info.stats.key_count,
                data_size: shard_info.stats.data_size,
                read_ops_per_sec: shard_info.stats.read_ops,
                write_ops_per_sec: shard_info.stats.write_ops,
                cpu_usage: shard_info.stats.cpu_usage,
                memory_usage: shard_info.stats.memory_usage,
                network_usage: shard_info.stats.network_usage,
                load_score,
            });
        }
        
        // Sort by load score (highest first)
        shard_loads.sort_by(|a, b| b.load_score.partial_cmp(&a.load_score).unwrap_or(CmpOrdering::Equal));
        
        shard_loads
    }
    
    /// Identify overloaded and underloaded shards
    fn identify_imbalanced_shards(&self, shard_loads: &[ShardLoad]) -> (Vec<ShardLoad>, Vec<ShardLoad>) {
        if shard_loads.is_empty() {
            return (Vec::new(), Vec::new());
        }
        
        // Calculate average load
        let total_load: f64 = shard_loads.iter().map(|s| s.load_score).sum();
        let avg_load = total_load / shard_loads.len() as f64;
        
        let threshold = self.config.load_balance_threshold / 100.0;
        
        let mut overloaded = Vec::new();
        let mut underloaded = Vec::new();
        
        for shard_load in shard_loads {
            let deviation = (shard_load.load_score - avg_load) / avg_load;
            
            if deviation > threshold {
                overloaded.push(shard_load.clone());
            } else if deviation < -threshold {
                underloaded.push(shard_load.clone());
            }
        }
        
        (overloaded, underloaded)
    }
    
    /// Generate data movement plan
    fn generate_movement_plan(&self, overloaded: &[ShardLoad], underloaded: &[ShardLoad]) -> Vec<DataMovement> {
        let mut movements = Vec::new();
        let mut available_capacity: Vec<(ShardId, f64)> = underloaded.iter()
            .map(|s| (s.shard_id, 1.0 - s.load_score)) // Available capacity
            .collect();
        
        for overloaded_shard in overloaded {
            let excess_load = overloaded_shard.load_score - 0.8; // Target 80% load
            if excess_load <= 0.0 {
                continue;
            }
            
            // Find best target shards for this excess load
            let mut remaining_excess = excess_load;
            
            for (target_shard, capacity) in &mut available_capacity {
                if remaining_excess <= 0.0 {
                    break;
                }
                
                let transfer_amount = remaining_excess.min(*capacity);
                if transfer_amount < 0.05 { // Minimum 5% load transfer
                    continue;
                }
                
                // Convert load percentage to estimated data size
                let estimated_size = (transfer_amount * overloaded_shard.data_size as f64) as u64;
                
                if estimated_size >= self.config.min_rebalance_size {
                    movements.push(DataMovement {
                        from_shard: overloaded_shard.shard_id,
                        to_shard: *target_shard,
                        key_range: KeyRange {
                            // Simplified: move a range based on estimated size
                            start: PartitionKey::Binary(vec![0]),
                            end: PartitionKey::Binary(vec![255]),
                        },
                        estimated_size,
                        priority: ((excess_load * 100.0) as u32).min(100),
                    });
                    
                    *capacity -= transfer_amount;
                    remaining_excess -= transfer_amount;
                }
            }
        }
        
        // Sort by priority (highest first)
        movements.sort_by(|a, b| b.priority.cmp(&a.priority));
        
        movements
    }
    
    /// Estimate completion time for movements
    fn estimate_completion_time(&self, movements: &[DataMovement]) -> Duration {
        let total_data: u64 = movements.iter().map(|m| m.estimated_size).sum();
        
        let transfer_rate = self.config.transfer_rate_limit.unwrap_or(100 * 1024 * 1024); // 100MB/s default
        let concurrent_ops = self.config.max_concurrent_movements as u64;
        
        let effective_rate = transfer_rate * concurrent_ops;
        let seconds = (total_data / effective_rate).max(1);
        
        Duration::from_secs(seconds)
    }
    
    /// Execute a single data movement
    async fn execute_movement(&self, movement: DataMovement) -> Result<()> {
        let operation_id = format!("rebalance_{}", self.operation_counter.fetch_add(1, Ordering::SeqCst));
        
        println!("Starting data movement: {} -> {} ({} bytes)", 
                 movement.from_shard, movement.to_shard, movement.estimated_size);
        
        let operation = RebalanceOperation {
            id: operation_id.clone(),
            movement: movement.clone(),
            state: RebalanceState::Transferring,
            progress: 0.0,
            started_at: Instant::now(),
            eta: Duration::from_secs(60), // Estimate 1 minute
            error: None,
        };
        
        // Add to active operations
        self.active_operations.write().insert(operation_id.clone(), operation);
        
        // Simulate data transfer with progress updates
        let total_bytes = movement.estimated_size;
        let chunk_size = 1024 * 1024; // 1MB chunks
        let chunks = (total_bytes / chunk_size).max(1);
        
        for i in 0..chunks {
            // Simulate chunk transfer
            tokio::time::sleep(Duration::from_millis(10)).await;
            
            let progress = ((i + 1) as f64 / chunks as f64) * 100.0;
            
            // Update operation progress
            if let Some(op) = self.active_operations.write().get_mut(&operation_id) {
                op.progress = progress;
                op.eta = Duration::from_secs(((chunks - i - 1) * 10 / 1000).max(1));
            }
            
            // Apply rate limiting if configured
            if let Some(rate_limit) = self.config.transfer_rate_limit {
                let sleep_time = Duration::from_millis((chunk_size * 1000 / rate_limit).max(1));
                tokio::time::sleep(sleep_time).await;
            }
        }
        
        // Mark as completed
        if let Some(op) = self.active_operations.write().get_mut(&operation_id) {
            op.state = RebalanceState::Completed;
            op.progress = 100.0;
            op.eta = Duration::ZERO;
        }
        
        println!("Completed data movement: {} -> {}", movement.from_shard, movement.to_shard);
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl ShardRebalancer for DefaultRebalancer {
    fn needs_rebalancing(&self, topology: &ClusterTopology) -> Result<bool> {
        if !self.config.enable_load_balancing && !self.config.enable_capacity_balancing {
            return Ok(false);
        }
        
        let shard_loads = self.analyze_cluster_load(topology);
        let (overloaded, underloaded) = self.identify_imbalanced_shards(&shard_loads);
        
        // Check if we have significant imbalance
        let needs_rebalancing = !overloaded.is_empty() && !underloaded.is_empty();
        
        if needs_rebalancing {
            println!("Rebalancing needed: {} overloaded shards, {} underloaded shards", 
                     overloaded.len(), underloaded.len());
        }
        
        Ok(needs_rebalancing)
    }
    
    fn generate_plan(&self, topology: &ClusterTopology) -> Result<RebalancePlan> {
        let shard_loads = self.analyze_cluster_load(topology);
        let (overloaded, underloaded) = self.identify_imbalanced_shards(&shard_loads);
        
        if overloaded.is_empty() || underloaded.is_empty() {
            return Ok(RebalancePlan {
                movements: Vec::new(),
                estimated_duration: Duration::ZERO,
                total_data_size: 0,
                priority: 0,
            });
        }
        
        let movements = self.generate_movement_plan(&overloaded, &underloaded);
        let estimated_duration = self.estimate_completion_time(&movements);
        let total_data_size: u64 = movements.iter().map(|m| m.estimated_size).sum();
        
        // Calculate priority based on imbalance severity
        let avg_overload: f64 = overloaded.iter().map(|s| s.load_score).sum::<f64>() / overloaded.len() as f64;
        let priority = (avg_overload * 100.0) as u32;
        
        println!("Generated rebalance plan: {} movements, {} bytes, estimated duration: {:?}", 
                 movements.len(), total_data_size, estimated_duration);
        
        Ok(RebalancePlan {
            movements,
            estimated_duration,
            total_data_size,
            priority,
        })
    }
    
    async fn execute_plan(&self, plan: RebalancePlan) -> Result<()> {
        if plan.movements.is_empty() {
            println!("No movements to execute");
            return Ok(());
        }
        
        println!("Executing rebalance plan with {} movements", plan.movements.len());
        
        // Update progress
        {
            let mut progress = self.progress.write();
            progress.total = plan.movements.len() as u32;
            progress.completed = 0;
            progress.data_transferred = 0;
            progress.eta = plan.estimated_duration;
            progress.current_operation = plan.movements.first().cloned();
        }
        
        // Execute movements with concurrency limit
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent_movements as usize));
        let mut tasks = Vec::new();
        
        let total_movements = plan.movements.len();
        for movement in plan.movements {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let rebalancer = self.clone();
            
            let task = tokio::spawn(async move {
                let result = rebalancer.execute_movement(movement).await;
                drop(permit); // Release semaphore
                result
            });
            
            tasks.push(task);
        }
        
        // Wait for all movements to complete
        let mut completed = 0;
        for task in tasks {
            match task.await {
                Ok(Ok(())) => {
                    completed += 1;
                    
                    // Update progress
                    let mut progress = self.progress.write();
                    progress.completed = completed;
                    
                    println!("Completed {}/{} movements", completed, progress.total);
                }
                Ok(Err(e)) => {
                    eprintln!("Movement failed: {}", e);
                }
                Err(e) => {
                    eprintln!("Task failed: {}", e);
                }
            }
        }
        
        // Clean up completed operations
        let mut active_ops = self.active_operations.write();
        active_ops.retain(|_, op| op.state != RebalanceState::Completed);
        
        println!("Rebalance plan execution completed: {}/{} movements successful", 
                 completed, total_movements);
        
        Ok(())
    }
    
    fn get_progress(&self) -> RebalanceProgress {
        self.progress.read().clone()
    }
}

impl Clone for DefaultRebalancer {
    fn clone(&self) -> Self {
        Self {
            active_operations: RwLock::new(self.active_operations.read().clone()),
            operation_counter: AtomicU64::new(self.operation_counter.load(Ordering::SeqCst)),
            progress: RwLock::new(self.progress.read().clone()),
            config: self.config.clone(),
        }
    }
}

/// Smart rebalancer with machine learning predictions
pub struct SmartRebalancer {
    base_rebalancer: DefaultRebalancer,
    
    /// Historical load patterns
    load_history: RwLock<Vec<LoadSnapshot>>,
    
    /// Prediction model
    predictor: Arc<dyn LoadPredictor>,
    
    /// Smart configuration
    smart_config: SmartRebalanceConfig,
}

/// Historical load snapshot
#[derive(Debug, Clone)]
pub struct LoadSnapshot {
    pub timestamp: u64,
    pub shard_loads: Vec<ShardLoad>,
    pub cluster_metrics: ClusterMetrics,
}

/// Cluster-wide metrics
#[derive(Debug, Clone)]
pub struct ClusterMetrics {
    pub total_ops_per_sec: f64,
    pub avg_latency: f64,
    pub error_rate: f64,
    pub memory_utilization: f64,
    pub cpu_utilization: f64,
}

/// Smart rebalancing configuration
#[derive(Debug, Clone)]
pub struct SmartRebalanceConfig {
    /// Enable predictive rebalancing
    pub enable_prediction: bool,
    
    /// Prediction horizon in seconds
    pub prediction_horizon: u64,
    
    /// Minimum confidence for predictions
    pub min_prediction_confidence: f64,
    
    /// Enable workload pattern detection
    pub enable_pattern_detection: bool,
    
    /// Historical data retention period
    pub history_retention: Duration,
}

impl Default for SmartRebalanceConfig {
    fn default() -> Self {
        Self {
            enable_prediction: true,
            prediction_horizon: 3600, // 1 hour
            min_prediction_confidence: 0.7,
            enable_pattern_detection: true,
            history_retention: Duration::from_secs(7 * 24 * 3600), // 1 week
        }
    }
}

/// Trait for load prediction
pub trait LoadPredictor: Send + Sync {
    /// Predict future load for shards
    fn predict_load(&self, history: &[LoadSnapshot], horizon: Duration) -> Result<Vec<ShardLoad>>;
    
    /// Get prediction confidence
    fn get_confidence(&self) -> f64;
    
    /// Train model with new data
    fn train(&mut self, data: &[LoadSnapshot]) -> Result<()>;
}

/// Simple trend-based predictor
pub struct TrendPredictor {
    confidence: f64,
}

impl TrendPredictor {
    pub fn new() -> Self {
        Self { confidence: 0.8 }
    }
    
    /// Calculate trend for a metric
    fn calculate_trend(&self, values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }
        
        let n = values.len() as f64;
        let sum_x: f64 = (0..values.len()).map(|i| i as f64).sum();
        let sum_y: f64 = values.iter().sum();
        let sum_xy: f64 = values.iter().enumerate().map(|(i, &y)| i as f64 * y).sum();
        let sum_x2: f64 = (0..values.len()).map(|i| (i as f64).powi(2)).sum();
        
        // Linear regression slope
        (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2))
    }
}

impl LoadPredictor for TrendPredictor {
    fn predict_load(&self, history: &[LoadSnapshot], horizon: Duration) -> Result<Vec<ShardLoad>> {
        if history.len() < 2 {
            return Err(Error::InvalidInput("Insufficient historical data".to_string()));
        }
        
        let horizon_steps = (horizon.as_secs() / 300) as usize; // 5-minute steps
        let mut predictions = Vec::new();
        
        // Get unique shard IDs
        let mut shard_ids = std::collections::HashSet::new();
        for snapshot in history {
            for shard_load in &snapshot.shard_loads {
                shard_ids.insert(shard_load.shard_id);
            }
        }
        
        for shard_id in shard_ids {
            // Extract historical data for this shard
            let mut load_scores = Vec::new();
            let mut data_sizes = Vec::new();
            let mut read_ops = Vec::new();
            let mut write_ops = Vec::new();
            
            for snapshot in history {
                if let Some(shard_load) = snapshot.shard_loads.iter().find(|s| s.shard_id == shard_id) {
                    load_scores.push(shard_load.load_score);
                    data_sizes.push(shard_load.data_size as f64);
                    read_ops.push(shard_load.read_ops_per_sec);
                    write_ops.push(shard_load.write_ops_per_sec);
                }
            }
            
            if load_scores.is_empty() {
                continue;
            }
            
            // Calculate trends
            let load_trend = self.calculate_trend(&load_scores);
            let size_trend = self.calculate_trend(&data_sizes);
            let read_trend = self.calculate_trend(&read_ops);
            let write_trend = self.calculate_trend(&write_ops);
            
            // Project forward
            let current_load = load_scores.last().copied().unwrap_or(0.0);
            let current_size = data_sizes.last().copied().unwrap_or(0.0) as u64;
            let current_reads = read_ops.last().copied().unwrap_or(0.0);
            let current_writes = write_ops.last().copied().unwrap_or(0.0);
            
            let predicted_load = (current_load + load_trend * horizon_steps as f64).max(0.0).min(1.0);
            let predicted_size = (current_size as f64 + size_trend * horizon_steps as f64).max(0.0) as u64;
            let predicted_reads = (current_reads + read_trend * horizon_steps as f64).max(0.0);
            let predicted_writes = (current_writes + write_trend * horizon_steps as f64).max(0.0);
            
            predictions.push(ShardLoad {
                shard_id,
                key_count: 0, // Not predicted
                data_size: predicted_size,
                read_ops_per_sec: predicted_reads,
                write_ops_per_sec: predicted_writes,
                cpu_usage: 0.0, // Not predicted
                memory_usage: 0, // Not predicted
                network_usage: 0.0, // Not predicted
                load_score: predicted_load,
            });
        }
        
        Ok(predictions)
    }
    
    fn get_confidence(&self) -> f64 {
        self.confidence
    }
    
    fn train(&mut self, _data: &[LoadSnapshot]) -> Result<()> {
        // Simple predictor doesn't need training
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_load_score_calculation() {
        let rebalancer = DefaultRebalancer::new();
        
        let stats = ShardStats {
            key_count: 1_000_000,
            data_size: 1024 * 1024 * 1024, // 1GB
            read_ops: 1000.0,
            write_ops: 500.0,
            cpu_usage: 50.0,
            memory_usage: 512 * 1024 * 1024, // 512MB
            avg_latency: 10.0,
            network_usage: 10.0,
            last_updated: 0,
        };
        
        let score = rebalancer.calculate_load_score(&stats);
        assert!(score > 0.0 && score <= 1.0);
    }
    
    #[test]
    fn test_rebalance_config_defaults() {
        let config = RebalanceConfig::default();
        assert_eq!(config.max_concurrent_movements, 2);
        assert_eq!(config.load_balance_threshold, 20.0);
        assert!(config.enable_load_balancing);
    }
    
    #[test]
    fn test_trend_calculation() {
        let predictor = TrendPredictor::new();
        
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let trend = predictor.calculate_trend(&values);
        assert!((trend - 1.0).abs() < 0.1); // Should be approximately 1.0
        
        let flat_values = vec![5.0, 5.0, 5.0, 5.0];
        let flat_trend = predictor.calculate_trend(&flat_values);
        assert!(flat_trend.abs() < 0.1); // Should be approximately 0.0
    }
}