use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unified router that provides a single interface for all routing strategies
pub struct UnifiedRouter {
    /// Current strategy in use
    strategy: ShardingStrategy,
    
    /// Hash-based router
    hash_router: strategies::HashBasedRouter,
    
    /// Range-based router
    range_router: strategies::RangeBasedRouter,
    
    /// Directory-based router
    directory_router: strategies::DirectoryBasedRouter,
    
    /// Hybrid router
    hybrid_router: strategies::HybridRouter,
    
    /// Routing statistics
    stats: RwLock<RoutingStats>,
    
    /// Version tracking for cache invalidation
    version: AtomicU64,
}

impl UnifiedRouter {
    /// Create new unified router
    pub fn new(strategy: ShardingStrategy) -> Self {
        Self {
            strategy,
            hash_router: strategies::HashBasedRouter::new(),
            range_router: strategies::RangeBasedRouter::new(),
            directory_router: strategies::DirectoryBasedRouter::new(),
            hybrid_router: strategies::HybridRouter::new(),
            stats: RwLock::new(RoutingStats::default()),
            version: AtomicU64::new(0),
        }
    }
    
    /// Switch routing strategy
    pub fn switch_strategy(&mut self, new_strategy: ShardingStrategy, topology: &ClusterTopology) -> Result<()> {
        if self.strategy != new_strategy {
            println!("Switching routing strategy from {:?} to {:?}", self.strategy, new_strategy);
            
            self.strategy = new_strategy;
            self.version.fetch_add(1, Ordering::SeqCst);
            
            // Update the new strategy with current topology
            self.update_routing(topology)?;
        }
        
        Ok(())
    }
    
    /// Get current routing strategy
    pub fn current_strategy(&self) -> ShardingStrategy {
        self.strategy
    }
    
    /// Get the appropriate router for current strategy
    fn get_router(&self) -> &dyn ShardRouter {
        match self.strategy {
            ShardingStrategy::Hash => &self.hash_router,
            ShardingStrategy::Range => &self.range_router,
            ShardingStrategy::Directory => &self.directory_router,
            ShardingStrategy::Hybrid => &self.hybrid_router,
        }
    }
    
    /// Validate routing consistency
    pub fn validate_routing(&self, topology: &ClusterTopology) -> Result<bool> {
        let shards = topology.shards.read();
        
        // Test routing for sample keys
        let test_keys = vec![
            PartitionKey::String("test_key_1".to_string()),
            PartitionKey::String("test_key_2".to_string()),
            PartitionKey::Integer(12345),
            PartitionKey::Binary(vec![1, 2, 3, 4]),
        ];
        
        for key in &test_keys {
            match self.route(key) {
                Ok(shard_id) => {
                    // Verify shard exists
                    if !shards.contains_key(&shard_id) {
                        eprintln!("Routing validation failed: shard {} does not exist for key {}", shard_id, key);
                        return Ok(false);
                    }
                }
                Err(_) => {
                    eprintln!("Routing validation failed: could not route key {}", key);
                    return Ok(false);
                }
            }
        }
        
        println!("Routing validation passed for {} test keys", test_keys.len());
        Ok(true)
    }
    
    /// Analyze routing distribution
    pub fn analyze_distribution(&self, sample_keys: &[PartitionKey]) -> RoutingAnalysis {
        let mut shard_counts: HashMap<ShardId, u64> = HashMap::new();
        let mut failed_routes = 0;
        
        for key in sample_keys {
            match self.route(key) {
                Ok(shard_id) => {
                    *shard_counts.entry(shard_id).or_insert(0) += 1;
                }
                Err(_) => {
                    failed_routes += 1;
                }
            }
        }
        
        let total_successful = sample_keys.len() - failed_routes;
        let num_shards = shard_counts.len();
        
        // Calculate distribution metrics
        let expected_per_shard = total_successful as f64 / num_shards as f64;
        let mut variance_sum = 0.0;
        
        for count in shard_counts.values() {
            let diff = *count as f64 - expected_per_shard;
            variance_sum += diff * diff;
        }
        
        let variance = if num_shards > 1 {
            variance_sum / (num_shards - 1) as f64
        } else {
            0.0
        };
        
        let std_deviation = variance.sqrt();
        let coefficient_of_variation = if expected_per_shard > 0.0 {
            std_deviation / expected_per_shard
        } else {
            0.0
        };
        
        RoutingAnalysis {
            total_keys: sample_keys.len(),
            successful_routes: total_successful,
            failed_routes,
            shard_distribution: shard_counts,
            expected_per_shard,
            std_deviation,
            coefficient_of_variation,
            balance_score: 1.0 - coefficient_of_variation, // Higher is better
        }
    }
}

impl ShardRouter for UnifiedRouter {
    fn route(&self, key: &PartitionKey) -> Result<ShardId> {
        let start = Instant::now();
        
        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_requests += 1;
        }
        
        // Route using current strategy
        let result = self.get_router().route(key);
        
        // Update stats
        {
            let mut stats = self.stats.write();
            match &result {
                Ok(_) => {
                    stats.cache_hits += 1;
                    let latency = start.elapsed().as_micros() as f64;
                    stats.avg_latency = (stats.avg_latency * (stats.total_requests - 1) as f64 + latency) / stats.total_requests as f64;
                }
                Err(_) => {
                    stats.failed_routes += 1;
                }
            }
        }
        
        result
    }
    
    fn route_range(&self, start: &PartitionKey, end: &PartitionKey) -> Result<Vec<ShardId>> {
        self.get_router().route_range(start, end)
    }
    
    fn update_routing(&self, topology: &ClusterTopology) -> Result<()> {
        // Update all routers to ensure consistency
        self.hash_router.update_routing(topology)?;
        self.range_router.update_routing(topology)?;
        self.directory_router.update_routing(topology)?;
        self.hybrid_router.update_routing(topology)?;
        
        self.version.fetch_add(1, Ordering::SeqCst);
        
        Ok(())
    }
    
    fn get_stats(&self) -> RoutingStats {
        let mut combined_stats = self.stats.read().clone();
        
        // Add strategy-specific stats
        let strategy_stats = self.get_router().get_stats();
        combined_stats.cache_hits += strategy_stats.cache_hits;
        combined_stats.cache_misses += strategy_stats.cache_misses;
        combined_stats.failed_routes += strategy_stats.failed_routes;
        
        combined_stats
    }
}

/// Routing distribution analysis
#[derive(Debug)]
pub struct RoutingAnalysis {
    /// Total number of keys analyzed
    pub total_keys: usize,
    /// Successfully routed keys
    pub successful_routes: usize,
    /// Failed routing attempts
    pub failed_routes: usize,
    /// Distribution across shards
    pub shard_distribution: HashMap<ShardId, u64>,
    /// Expected keys per shard
    pub expected_per_shard: f64,
    /// Standard deviation of distribution
    pub std_deviation: f64,
    /// Coefficient of variation (std_dev / mean)
    pub coefficient_of_variation: f64,
    /// Balance score (1.0 = perfect, 0.0 = completely unbalanced)
    pub balance_score: f64,
}

impl RoutingAnalysis {
    /// Print detailed analysis
    pub fn print_analysis(&self) {
        println!("=== Routing Distribution Analysis ===");
        println!("Total Keys: {}", self.total_keys);
        println!("Successful Routes: {}", self.successful_routes);
        println!("Failed Routes: {}", self.failed_routes);
        println!("Expected per Shard: {:.2}", self.expected_per_shard);
        println!("Standard Deviation: {:.2}", self.std_deviation);
        println!("Coefficient of Variation: {:.4}", self.coefficient_of_variation);
        println!("Balance Score: {:.4} (higher is better)", self.balance_score);
        
        println!("\nShard Distribution:");
        let mut sorted_shards: Vec<_> = self.shard_distribution.iter().collect();
        sorted_shards.sort_by_key(|(shard_id, _)| *shard_id);
        
        for (shard_id, count) in sorted_shards {
            let percentage = (*count as f64 / self.successful_routes as f64) * 100.0;
            let deviation = *count as f64 - self.expected_per_shard;
            println!("  Shard {}: {} keys ({:.1}%, deviation: {:.1})", 
                     shard_id, count, percentage, deviation);
        }
        
        if self.balance_score >= 0.9 {
            println!("\n✅ Excellent balance");
        } else if self.balance_score >= 0.7 {
            println!("\n⚠️  Good balance, minor optimization possible");
        } else if self.balance_score >= 0.5 {
            println!("\n⚠️  Moderate imbalance detected");
        } else {
            println!("\n❌ Significant imbalance - rebalancing recommended");
        }
    }
}

/// Smart routing advisor that analyzes patterns and suggests optimizations
pub struct RoutingAdvisor {
    /// Historical routing patterns
    patterns: RwLock<Vec<RoutingPattern>>,
    
    /// Access frequency by key prefix
    prefix_frequency: RwLock<HashMap<String, u64>>,
    
    /// Configuration
    config: AdvisorConfig,
}

/// Configuration for routing advisor
#[derive(Debug, Clone)]
pub struct AdvisorConfig {
    /// Maximum patterns to track
    pub max_patterns: usize,
    /// Pattern retention time
    pub retention_period: Duration,
    /// Minimum frequency for recommendations
    pub min_frequency_threshold: u64,
    /// Enable hot key detection
    pub enable_hot_key_detection: bool,
}

impl Default for AdvisorConfig {
    fn default() -> Self {
        Self {
            max_patterns: 10000,
            retention_period: Duration::from_secs(3600), // 1 hour
            min_frequency_threshold: 100,
            enable_hot_key_detection: true,
        }
    }
}

/// Routing pattern for analysis
#[derive(Debug, Clone)]
pub struct RoutingPattern {
    /// Key pattern (prefix)
    pub pattern: String,
    /// Access frequency
    pub frequency: u64,
    /// Access timestamps
    pub timestamps: Vec<u64>,
    /// Target shards
    pub target_shards: HashMap<ShardId, u64>,
    /// Current strategy performance
    pub avg_latency: f64,
    /// Last updated
    pub last_updated: u64,
}

impl RoutingAdvisor {
    /// Create new routing advisor
    pub fn new() -> Self {
        Self {
            patterns: RwLock::new(Vec::new()),
            prefix_frequency: RwLock::new(HashMap::new()),
            config: AdvisorConfig::default(),
        }
    }
    
    /// Record access pattern
    pub fn record_access(&self, key: &PartitionKey, shard_id: ShardId, latency: f64) {
        let pattern = self.extract_pattern(key);
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Update prefix frequency
        {
            let mut freq = self.prefix_frequency.write();
            *freq.entry(pattern.clone()).or_insert(0) += 1;
        }
        
        // Update patterns
        {
            let mut patterns = self.patterns.write();
            
            if let Some(existing) = patterns.iter_mut().find(|p| p.pattern == pattern) {
                existing.frequency += 1;
                existing.timestamps.push(timestamp);
                existing.target_shards.entry(shard_id).and_modify(|c| *c += 1).or_insert(1);
                existing.avg_latency = (existing.avg_latency * (existing.frequency - 1) as f64 + latency) / existing.frequency as f64;
                existing.last_updated = timestamp;
                
                // Keep only recent timestamps
                let cutoff = timestamp.saturating_sub(self.config.retention_period.as_secs());
                existing.timestamps.retain(|&t| t >= cutoff);
            } else {
                let mut target_shards = HashMap::new();
                target_shards.insert(shard_id, 1);
                
                patterns.push(RoutingPattern {
                    pattern,
                    frequency: 1,
                    timestamps: vec![timestamp],
                    target_shards,
                    avg_latency: latency,
                    last_updated: timestamp,
                });
            }
            
            // Limit pattern count
            if patterns.len() > self.config.max_patterns {
                patterns.sort_by(|a, b| b.frequency.cmp(&a.frequency));
                patterns.truncate(self.config.max_patterns);
            }
        }
    }
    
    /// Extract pattern from key
    fn extract_pattern(&self, key: &PartitionKey) -> String {
        match key {
            PartitionKey::String(s) => {
                // Extract prefix up to first separator
                if let Some(pos) = s.find(':') {
                    s[..pos].to_string()
                } else {
                    // Use first 3 characters as pattern
                    s.chars().take(3).collect()
                }
            }
            PartitionKey::Integer(i) => {
                // Group by magnitude
                if *i < 1000 {
                    "small_int".to_string()
                } else if *i < 1_000_000 {
                    "medium_int".to_string()
                } else {
                    "large_int".to_string()
                }
            }
            PartitionKey::Binary(b) => {
                // Use first byte as pattern
                if let Some(first) = b.first() {
                    format!("binary_{:02x}", first)
                } else {
                    "empty_binary".to_string()
                }
            }
            PartitionKey::Composite(keys) => {
                // Combine patterns of component keys
                keys.iter()
                    .map(|k| self.extract_pattern(k))
                    .collect::<Vec<_>>()
                    .join("_")
            }
        }
    }
    
    /// Generate routing recommendations
    pub fn generate_recommendations(&self, topology: &ClusterTopology) -> Vec<RoutingRecommendation> {
        let patterns = self.patterns.read();
        let mut recommendations = Vec::new();
        
        for pattern in patterns.iter() {
            if pattern.frequency < self.config.min_frequency_threshold {
                continue;
            }
            
            // Analyze pattern
            if self.config.enable_hot_key_detection && self.is_hot_pattern(pattern) {
                recommendations.push(RoutingRecommendation {
                    pattern: pattern.pattern.clone(),
                    recommendation_type: RecommendationType::HotKeyDetected,
                    description: format!("Pattern '{}' is accessed {} times with high frequency", 
                                       pattern.pattern, pattern.frequency),
                    suggested_action: "Consider splitting this pattern across multiple shards or implementing caching".to_string(),
                    confidence: 0.9,
                });
            }
            
            // Check for imbalanced distribution
            if self.is_imbalanced_pattern(pattern, topology) {
                recommendations.push(RoutingRecommendation {
                    pattern: pattern.pattern.clone(),
                    recommendation_type: RecommendationType::ImbalancedDistribution,
                    description: format!("Pattern '{}' shows uneven distribution across shards", pattern.pattern),
                    suggested_action: "Consider directory-based routing for better load distribution".to_string(),
                    confidence: 0.7,
                });
            }
            
            // Check for high latency
            if pattern.avg_latency > 1000.0 { // > 1ms
                recommendations.push(RoutingRecommendation {
                    pattern: pattern.pattern.clone(),
                    recommendation_type: RecommendationType::HighLatency,
                    description: format!("Pattern '{}' has high average latency: {:.2}μs", 
                                       pattern.pattern, pattern.avg_latency),
                    suggested_action: "Consider co-locating related data or optimizing shard placement".to_string(),
                    confidence: 0.8,
                });
            }
        }
        
        recommendations.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());
        recommendations
    }
    
    /// Check if pattern is hot (high frequency access)
    fn is_hot_pattern(&self, pattern: &RoutingPattern) -> bool {
        if pattern.timestamps.len() < 10 {
            return false;
        }
        
        // Check access rate in last minute
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let recent_count = pattern.timestamps.iter()
            .filter(|&&t| t >= now.saturating_sub(60))
            .count();
        
        recent_count > 50 // More than 50 accesses per minute
    }
    
    /// Check if pattern has imbalanced distribution
    fn is_imbalanced_pattern(&self, pattern: &RoutingPattern, _topology: &ClusterTopology) -> bool {
        if pattern.target_shards.len() < 2 {
            return false;
        }
        
        let counts: Vec<u64> = pattern.target_shards.values().cloned().collect();
        let total: u64 = counts.iter().sum();
        let avg = total as f64 / counts.len() as f64;
        
        // Calculate coefficient of variation
        let variance: f64 = counts.iter()
            .map(|&c| (c as f64 - avg).powi(2))
            .sum::<f64>() / counts.len() as f64;
        
        let std_dev = variance.sqrt();
        let coefficient_of_variation = std_dev / avg;
        
        coefficient_of_variation > 0.5 // More than 50% variation
    }
}

/// Routing recommendation
#[derive(Debug, Clone)]
pub struct RoutingRecommendation {
    /// Pattern this applies to
    pub pattern: String,
    /// Type of recommendation
    pub recommendation_type: RecommendationType,
    /// Human-readable description
    pub description: String,
    /// Suggested action
    pub suggested_action: String,
    /// Confidence level (0.0-1.0)
    pub confidence: f64,
}

/// Types of routing recommendations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecommendationType {
    /// Hot key detected
    HotKeyDetected,
    /// Imbalanced distribution
    ImbalancedDistribution,
    /// High latency
    HighLatency,
    /// Strategy optimization
    StrategyOptimization,
    /// Capacity planning
    CapacityPlanning,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_unified_router() {
        let router = UnifiedRouter::new(ShardingStrategy::Hash);
        assert_eq!(router.current_strategy(), ShardingStrategy::Hash);
    }
    
    #[test]
    fn test_routing_analysis() {
        let mut distribution = HashMap::new();
        distribution.insert(1, 100);
        distribution.insert(2, 90);
        distribution.insert(3, 110);
        
        let analysis = RoutingAnalysis {
            total_keys: 300,
            successful_routes: 300,
            failed_routes: 0,
            shard_distribution: distribution,
            expected_per_shard: 100.0,
            std_deviation: 10.0,
            coefficient_of_variation: 0.1,
            balance_score: 0.9,
        };
        
        assert!(analysis.balance_score > 0.8);
    }
    
    #[test]
    fn test_pattern_extraction() {
        let advisor = RoutingAdvisor::new();
        
        let key1 = PartitionKey::String("user:123".to_string());
        assert_eq!(advisor.extract_pattern(&key1), "user");
        
        let key2 = PartitionKey::Integer(12345);
        assert_eq!(advisor.extract_pattern(&key2), "medium_int");
        
        let key3 = PartitionKey::Binary(vec![0xFF, 0x00]);
        assert_eq!(advisor.extract_pattern(&key3), "binary_ff");
    }
}