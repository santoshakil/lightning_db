//! Machine Learning Auto-Tuning Engine
//!
//! Implements intelligent performance optimization using machine learning algorithms
//! to automatically tune Lightning DB parameters based on workload patterns.

use crate::performance_tuning::{PerformanceMetrics, TuningParameters, WorkloadType};
use crate::Result;
use rand::{rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

/// Machine learning auto-tuner for Lightning DB performance optimization
pub struct MLAutoTuner {
    /// Current configuration parameters
    config: TuningParameters,
    /// Historical performance metrics
    metrics_history: VecDeque<PerformanceDataPoint>,
    /// Workload pattern classifier
    workload_classifier: WorkloadClassifier,
    /// Parameter optimizer using genetic algorithm
    optimizer: GeneticOptimizer,
    /// Bayesian optimization for hyperparameter tuning
    bayesian_optimizer: BayesianOptimizer,
    /// Neural network for performance prediction
    performance_predictor: PerformancePredictor,
    /// Reinforcement learning agent
    rl_agent: ReinforcementLearningAgent,
    /// Auto-tuning configuration
    tuning_config: AutoTuningConfig,
    /// Performance baseline for comparison
    baseline_metrics: Option<PerformanceMetrics>,
}

/// Configuration for auto-tuning behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoTuningConfig {
    /// Enable auto-tuning
    pub enabled: bool,
    /// Minimum time between tuning adjustments
    pub tuning_interval: Duration,
    /// Maximum number of historical data points to keep
    pub max_history_size: usize,
    /// Minimum improvement threshold to apply changes
    pub improvement_threshold: f64,
    /// Learning rate for gradient-based optimization
    pub learning_rate: f64,
    /// Enable different optimization algorithms
    pub enable_genetic_algorithm: bool,
    pub enable_bayesian_optimization: bool,
    pub enable_neural_network: bool,
    pub enable_reinforcement_learning: bool,
    /// Safety limits to prevent extreme configurations
    pub safety_limits: SafetyLimits,
}

/// Safety limits for parameter tuning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyLimits {
    pub min_cache_size_mb: usize,
    pub max_cache_size_mb: usize,
    pub min_write_buffer_size: usize,
    pub max_write_buffer_size: usize,
    pub min_compaction_threads: usize,
    pub max_compaction_threads: usize,
    pub min_bloom_filter_bits: u32,
    pub max_bloom_filter_bits: u32,
}

/// Performance data point with timestamp and metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceDataPoint {
    pub timestamp: SystemTime,
    pub metrics: PerformanceMetrics,
    pub configuration: TuningParameters,
    pub workload_type: WorkloadType,
    pub performance_score: f64,
}

/// Workload pattern classifier using feature extraction
#[derive(Debug, Clone)]
pub struct WorkloadClassifier {
    /// Extracted features from workload patterns
    feature_extractors: Vec<FeatureExtractor>,
    /// Classification model (simple decision tree)
    decision_tree: DecisionTree,
    /// Historical workload classifications
    classification_history: VecDeque<WorkloadClassification>,
}

/// Feature extractor for workload analysis
#[derive(Debug, Clone)]
pub struct FeatureExtractor {
    pub name: String,
    pub extractor_fn: fn(&PerformanceMetrics) -> f64,
}

/// Simple decision tree for workload classification
#[derive(Debug, Clone)]
pub struct DecisionTree {
    pub nodes: Vec<DecisionNode>,
    pub root_index: usize,
}

/// Decision tree node
#[derive(Debug, Clone)]
pub struct DecisionNode {
    pub feature_index: usize,
    pub threshold: f64,
    pub left_child: Option<usize>,
    pub right_child: Option<usize>,
    pub classification: Option<WorkloadType>,
}

/// Workload classification result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadClassification {
    pub workload_type: WorkloadType,
    pub confidence: f64,
    pub timestamp: SystemTime,
}

/// Genetic algorithm optimizer for parameter tuning
#[derive(Debug, Clone)]
pub struct GeneticOptimizer {
    /// Population of parameter configurations
    population: Vec<ParameterGenome>,
    /// Population size
    population_size: usize,
    /// Mutation rate
    mutation_rate: f64,
    /// Crossover rate
    crossover_rate: f64,
    /// Number of generations to evolve
    max_generations: usize,
    /// Current generation
    current_generation: usize,
    /// Elite individuals to preserve
    elite_size: usize,
}

/// Parameter genome for genetic algorithm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterGenome {
    pub parameters: TuningParameters,
    pub fitness: f64,
    pub generation: usize,
}

/// Bayesian optimization for hyperparameter tuning
#[derive(Debug, Clone)]
pub struct BayesianOptimizer {
    /// Gaussian process model
    gaussian_process: GaussianProcess,
    /// Acquisition function for next parameter selection
    acquisition_function: AcquisitionFunction,
    /// Historical evaluations
    evaluations: Vec<BayesianEvaluation>,
    /// Number of iterations
    max_iterations: usize,
}

/// Gaussian process model for Bayesian optimization
#[derive(Debug, Clone)]
pub struct GaussianProcess {
    /// Training inputs (parameter vectors)
    inputs: Vec<Vec<f64>>,
    /// Training outputs (performance scores)
    outputs: Vec<f64>,
    /// Kernel hyperparameters
    kernel_params: KernelParameters,
}

/// Kernel parameters for Gaussian process
#[derive(Debug, Clone)]
pub struct KernelParameters {
    pub length_scale: f64,
    pub signal_variance: f64,
    pub noise_variance: f64,
}

/// Acquisition function types
#[derive(Debug, Clone)]
pub enum AcquisitionFunction {
    ExpectedImprovement { xi: f64 },
    UpperConfidenceBound { kappa: f64 },
    ProbabilityOfImprovement,
}

/// Bayesian optimization evaluation
#[derive(Debug, Clone)]
pub struct BayesianEvaluation {
    pub parameters: Vec<f64>,
    pub performance_score: f64,
    pub timestamp: SystemTime,
}

/// Neural network for performance prediction
#[derive(Debug, Clone)]
pub struct PerformancePredictor {
    /// Network layers
    layers: Vec<Layer>,
    /// Learning rate
    learning_rate: f64,
    /// Training history
    training_history: Vec<TrainingEpoch>,
}

/// Neural network layer
#[derive(Debug, Clone)]
pub struct Layer {
    pub weights: Vec<Vec<f64>>,
    pub biases: Vec<f64>,
    pub activation: ActivationFunction,
}

/// Activation function types
#[derive(Debug, Clone)]
pub enum ActivationFunction {
    ReLU,
    Sigmoid,
    Tanh,
    Linear,
}

/// Training epoch information
#[derive(Debug, Clone)]
pub struct TrainingEpoch {
    pub epoch: usize,
    pub loss: f64,
    pub accuracy: f64,
    pub timestamp: SystemTime,
}

/// Reinforcement learning agent for online optimization
#[derive(Debug, Clone)]
pub struct ReinforcementLearningAgent {
    /// Q-learning table
    q_table: HashMap<StateAction, f64>,
    /// Learning rate
    learning_rate: f64,
    /// Discount factor
    discount_factor: f64,
    /// Exploration rate (epsilon)
    exploration_rate: f64,
    /// Exploration decay
    exploration_decay: f64,
    /// Current state
    current_state: Option<PerformanceState>,
    /// Action space
    action_space: Vec<TuningAction>,
}

/// State representation for RL
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct PerformanceState {
    pub read_ops_bucket: usize,
    pub write_ops_bucket: usize,
    pub memory_usage_bucket: usize,
    pub latency_bucket: usize,
    pub workload_type: WorkloadType,
}

/// Action representation for RL
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TuningAction {
    pub parameter: String,
    pub adjustment: ParameterAdjustment,
}

/// Parameter adjustment types
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ParameterAdjustment {
    Increase,
    Decrease,
    NoChange,
}

/// State-action pair for Q-learning
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct StateAction {
    pub state: PerformanceState,
    pub action: TuningAction,
}

impl MLAutoTuner {
    /// Create new ML auto-tuner
    pub fn new(config: AutoTuningConfig) -> Self {
        let safety_limits = &config.safety_limits;

        Self {
            config: TuningParameters::default(),
            metrics_history: VecDeque::with_capacity(config.max_history_size),
            workload_classifier: WorkloadClassifier::new(),
            optimizer: GeneticOptimizer::new(20, 0.1, 0.8, 50, 5),
            bayesian_optimizer: BayesianOptimizer::new(50),
            performance_predictor: PerformancePredictor::new(&[10, 20, 10, 1]),
            rl_agent: ReinforcementLearningAgent::new(0.1, 0.95, 0.1, 0.995),
            tuning_config: config,
            baseline_metrics: None,
        }
    }

    /// Add new performance metrics for analysis
    pub fn add_metrics(&mut self, metrics: PerformanceMetrics) -> Result<()> {
        let timestamp = SystemTime::now();

        // Classify workload type
        let workload_classification = self.workload_classifier.classify(&metrics);

        // Calculate performance score
        let performance_score = self.calculate_performance_score(&metrics);

        // Create data point
        let data_point = PerformanceDataPoint {
            timestamp,
            metrics: metrics.clone(),
            configuration: self.config.clone(),
            workload_type: workload_classification.workload_type.clone(),
            performance_score,
        };

        // Add to history
        self.metrics_history.push_back(data_point.clone());
        if self.metrics_history.len() > self.tuning_config.max_history_size {
            self.metrics_history.pop_front();
        }

        // Set baseline if not set
        if self.baseline_metrics.is_none() {
            self.baseline_metrics = Some(metrics.clone());
        }

        // Train models with new data
        self.train_models(&data_point)?;

        Ok(())
    }

    /// Optimize configuration using ML algorithms
    pub fn optimize_configuration(&mut self) -> Result<Option<TuningParameters>> {
        if !self.tuning_config.enabled || self.metrics_history.len() < 10 {
            return Ok(None);
        }

        let mut best_config = self.config.clone();
        let mut best_score = self.calculate_current_performance_score();
        let mut improvement_found = false;

        // Genetic Algorithm Optimization
        if self.tuning_config.enable_genetic_algorithm {
            if let Some(ga_config) = self.optimize_with_genetic_algorithm()? {
                let predicted_score = self.predict_performance(&ga_config)?;
                if predicted_score > best_score + self.tuning_config.improvement_threshold {
                    best_config = ga_config;
                    best_score = predicted_score;
                    improvement_found = true;
                }
            }
        }

        // Bayesian Optimization
        if self.tuning_config.enable_bayesian_optimization {
            if let Some(bo_config) = self.optimize_with_bayesian_optimization()? {
                let predicted_score = self.predict_performance(&bo_config)?;
                if predicted_score > best_score + self.tuning_config.improvement_threshold {
                    best_config = bo_config;
                    best_score = predicted_score;
                    improvement_found = true;
                }
            }
        }

        // Reinforcement Learning
        if self.tuning_config.enable_reinforcement_learning {
            if let Some(rl_config) = self.optimize_with_reinforcement_learning()? {
                let predicted_score = self.predict_performance(&rl_config)?;
                if predicted_score > best_score + self.tuning_config.improvement_threshold {
                    best_config = rl_config;
                    best_score = predicted_score;
                    improvement_found = true;
                }
            }
        }

        if improvement_found {
            // Validate against safety limits
            let safe_config = self.apply_safety_limits(best_config);
            self.config = safe_config.clone();
            Ok(Some(safe_config))
        } else {
            Ok(None)
        }
    }

    /// Get current tuning recommendations
    pub fn get_recommendations(&self) -> Vec<TuningRecommendation> {
        let mut recommendations = Vec::new();

        if let Some(latest_metrics) = self.metrics_history.back() {
            // Analyze current performance
            let current_score = latest_metrics.performance_score;

            if let Some(baseline) = &self.baseline_metrics {
                let baseline_score = self.calculate_performance_score(baseline);

                if current_score < baseline_score * 0.9 {
                    recommendations.push(TuningRecommendation {
                        parameter: "overall_performance".to_string(),
                        current_value: current_score.to_string(),
                        recommended_value: "optimized".to_string(),
                        reasoning: "Performance has degraded significantly from baseline"
                            .to_string(),
                        confidence: 0.8,
                        impact: RecommendationImpact::High,
                    });
                }
            }

            // Memory utilization analysis
            if latest_metrics.metrics.memory_usage_mb > 1500 {
                let recommended_cache_size = (self.config.cache_size_mb as f64 * 0.8) as usize;
                recommendations.push(TuningRecommendation {
                    parameter: "cache_size_mb".to_string(),
                    current_value: self.config.cache_size_mb.to_string(),
                    recommended_value: recommended_cache_size.to_string(),
                    reasoning: format!(
                        "High memory usage: {} MB",
                        latest_metrics.metrics.memory_usage_mb
                    ),
                    confidence: 0.9,
                    impact: RecommendationImpact::Medium,
                });
            }

            // Write performance analysis
            if latest_metrics.metrics.write_ops_per_sec < 1000.0 {
                recommendations.push(TuningRecommendation {
                    parameter: "write_buffer_size".to_string(),
                    current_value: self.config.write_buffer_size.to_string(),
                    recommended_value: (self.config.write_buffer_size * 2).to_string(),
                    reasoning: "Low write throughput detected".to_string(),
                    confidence: 0.7,
                    impact: RecommendationImpact::Medium,
                });
            }
        }

        recommendations
    }

    /// Calculate performance score from metrics
    fn calculate_performance_score(&self, metrics: &PerformanceMetrics) -> f64 {
        // Weighted combination of performance factors
        let read_score = (metrics.read_ops_per_sec / 10000.0).min(1.0);
        let write_score = (metrics.write_ops_per_sec / 5000.0).min(1.0);
        let mixed_score = (metrics.mixed_ops_per_sec / 15000.0).min(1.0);
        let latency_score = (2.0 - metrics.read_latency_us.p50).max(0.0).min(1.0);
        let memory_score = 1.0 - (metrics.memory_usage_mb as f64 / 2000.0).min(1.0);
        let cpu_score = 1.0 - (metrics.cpu_usage_percent / 100.0).min(1.0);

        // Weighted average
        let score = 0.25 * read_score
            + 0.25 * write_score
            + 0.2 * mixed_score
            + 0.15 * latency_score
            + 0.1 * memory_score
            + 0.05 * cpu_score;

        score.max(0.0).min(1.0)
    }

    /// Get current performance score
    fn calculate_current_performance_score(&self) -> f64 {
        if let Some(latest_metrics) = self.metrics_history.back() {
            latest_metrics.performance_score
        } else {
            0.0
        }
    }

    /// Train ML models with new data
    fn train_models(&mut self, data_point: &PerformanceDataPoint) -> Result<()> {
        // Train workload classifier
        self.workload_classifier.train(data_point)?;

        // Train performance predictor
        if self.tuning_config.enable_neural_network {
            self.performance_predictor.train(data_point)?;
        }

        // Update reinforcement learning agent
        if self.tuning_config.enable_reinforcement_learning {
            self.rl_agent.update(data_point)?;
        }

        Ok(())
    }

    /// Predict performance for given configuration
    fn predict_performance(&self, config: &TuningParameters) -> Result<f64> {
        if self.tuning_config.enable_neural_network {
            self.performance_predictor.predict(config)
        } else {
            // Simple heuristic prediction
            Ok(0.5) // Placeholder
        }
    }

    /// Optimize using genetic algorithm
    fn optimize_with_genetic_algorithm(&mut self) -> Result<Option<TuningParameters>> {
        self.optimizer.evolve(&self.metrics_history)
    }

    /// Optimize using Bayesian optimization
    fn optimize_with_bayesian_optimization(&mut self) -> Result<Option<TuningParameters>> {
        self.bayesian_optimizer.optimize(&self.metrics_history)
    }

    /// Optimize using reinforcement learning
    fn optimize_with_reinforcement_learning(&mut self) -> Result<Option<TuningParameters>> {
        self.rl_agent.get_optimal_action(&self.metrics_history)
    }

    /// Apply safety limits to configuration
    fn apply_safety_limits(&self, mut config: TuningParameters) -> TuningParameters {
        let limits = &self.tuning_config.safety_limits;

        config.cache_size_mb = config
            .cache_size_mb
            .max(limits.min_cache_size_mb)
            .min(limits.max_cache_size_mb);

        config.write_buffer_size = config
            .write_buffer_size
            .max(limits.min_write_buffer_size)
            .min(limits.max_write_buffer_size);

        config.compaction_threads = config
            .compaction_threads
            .max(limits.min_compaction_threads)
            .min(limits.max_compaction_threads);

        config.bloom_filter_bits_per_key = config
            .bloom_filter_bits_per_key
            .max(limits.min_bloom_filter_bits)
            .min(limits.max_bloom_filter_bits);

        config
    }
}

/// Tuning recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningRecommendation {
    pub parameter: String,
    pub current_value: String,
    pub recommended_value: String,
    pub reasoning: String,
    pub confidence: f64,
    pub impact: RecommendationImpact,
}

/// Impact level of recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecommendationImpact {
    Low,
    Medium,
    High,
    Critical,
}

impl Default for AutoTuningConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tuning_interval: Duration::from_secs(300), // 5 minutes
            max_history_size: 1000,
            improvement_threshold: 0.05, // 5% improvement
            learning_rate: 0.01,
            enable_genetic_algorithm: true,
            enable_bayesian_optimization: true,
            enable_neural_network: true,
            enable_reinforcement_learning: true,
            safety_limits: SafetyLimits::default(),
        }
    }
}

impl Default for SafetyLimits {
    fn default() -> Self {
        Self {
            min_cache_size_mb: 10,
            max_cache_size_mb: 8192,
            min_write_buffer_size: 1024,
            max_write_buffer_size: 67108864, // 64MB
            min_compaction_threads: 1,
            max_compaction_threads: 16,
            min_bloom_filter_bits: 8,
            max_bloom_filter_bits: 20,
        }
    }
}

// Placeholder implementations for ML components
impl WorkloadClassifier {
    fn new() -> Self {
        Self {
            feature_extractors: Self::create_feature_extractors(),
            decision_tree: DecisionTree::new(),
            classification_history: VecDeque::new(),
        }
    }

    fn create_feature_extractors() -> Vec<FeatureExtractor> {
        vec![
            FeatureExtractor {
                name: "read_write_ratio".to_string(),
                extractor_fn: |m| m.read_ops_per_sec / (m.write_ops_per_sec + 1.0),
            },
            FeatureExtractor {
                name: "average_latency".to_string(),
                extractor_fn: |m| m.read_latency_us.p50,
            },
            FeatureExtractor {
                name: "cpu_usage".to_string(),
                extractor_fn: |m| m.cpu_usage_percent / 100.0,
            },
            FeatureExtractor {
                name: "memory_usage_ratio".to_string(),
                extractor_fn: |m| m.memory_usage_mb as f64 / 1000.0,
            },
        ]
    }

    fn classify(&mut self, metrics: &PerformanceMetrics) -> WorkloadClassification {
        // Extract features
        let features: Vec<f64> = self
            .feature_extractors
            .iter()
            .map(|extractor| (extractor.extractor_fn)(metrics))
            .collect();

        // Simple classification logic
        let workload_type = if features[0] > 3.0 {
            // High read/write ratio
            WorkloadType::ReadHeavy
        } else if features[0] < 0.5 {
            // Low read/write ratio
            WorkloadType::WriteHeavy
        } else {
            WorkloadType::Mixed
        };

        let classification = WorkloadClassification {
            workload_type,
            confidence: 0.8,
            timestamp: SystemTime::now(),
        };

        self.classification_history
            .push_back(classification.clone());
        if self.classification_history.len() > 100 {
            self.classification_history.pop_front();
        }

        classification
    }

    fn train(&mut self, _data_point: &PerformanceDataPoint) -> Result<()> {
        // Training implementation would update decision tree
        Ok(())
    }
}

impl DecisionTree {
    fn new() -> Self {
        Self {
            nodes: vec![
                DecisionNode {
                    feature_index: 0, // read_write_ratio
                    threshold: 2.0,
                    left_child: Some(1),
                    right_child: Some(2),
                    classification: None,
                },
                DecisionNode {
                    feature_index: 0,
                    threshold: 0.5,
                    left_child: None,
                    right_child: None,
                    classification: Some(WorkloadType::WriteHeavy),
                },
                DecisionNode {
                    feature_index: 0,
                    threshold: 0.0,
                    left_child: None,
                    right_child: None,
                    classification: Some(WorkloadType::ReadHeavy),
                },
            ],
            root_index: 0,
        }
    }
}

// Additional placeholder implementations for other ML components
impl GeneticOptimizer {
    fn new(pop_size: usize, mut_rate: f64, cross_rate: f64, max_gen: usize, elite: usize) -> Self {
        Self {
            population: Vec::new(),
            population_size: pop_size,
            mutation_rate: mut_rate,
            crossover_rate: cross_rate,
            max_generations: max_gen,
            current_generation: 0,
            elite_size: elite,
        }
    }

    /// Initialize random population
    fn initialize_population(&mut self) -> Result<()> {
        self.population.clear();

        for _ in 0..self.population_size {
            let parameters = TuningParameters {
                cache_size_mb: rng().random_range(64..2048), // 64MB to 2GB
                write_buffer_size: (rng().random_range(1024..32768) as usize)
                    .next_power_of_two(), // 1KB to 32KB (power of 2)
                compaction_threads: rng().random_range(1..=8), // 1 to 8 threads
                bloom_filter_bits_per_key: rng().random_range(8..=20), // 8 to 20 bits
                prefetch_distance: (rng().random_range(8..64) as usize).next_power_of_two(), // 8 to 64 (power of 2)
                write_batch_size: rng().random_range(100..5000), // 100 to 5000
            };

            let genome = ParameterGenome {
                parameters,
                fitness: 0.0,
                generation: 0,
            };

            self.population.push(genome);
        }

        Ok(())
    }

    /// Evaluate fitness for all individuals in population
    fn evaluate_population(&mut self, history: &VecDeque<PerformanceDataPoint>) -> Result<()> {
        // Calculate fitness for each genome separately to avoid borrowing issues
        let mut fitness_scores = Vec::new();
        for genome in &self.population {
            let fitness = self.calculate_fitness(&genome.parameters, history);
            fitness_scores.push(fitness);
        }

        // Update fitness scores
        for (genome, fitness) in self.population.iter_mut().zip(fitness_scores) {
            genome.fitness = fitness;
        }
        Ok(())
    }

    /// Calculate fitness score for given parameters
    fn calculate_fitness(
        &self,
        parameters: &TuningParameters,
        history: &VecDeque<PerformanceDataPoint>,
    ) -> f64 {
        // Simple fitness function based on recent performance history
        // Higher scores for configurations that would likely perform better

        let mut score = 0.0;
        let recent_history: Vec<_> = history.iter().rev().take(10).collect();

        if recent_history.is_empty() {
            return 0.5; // Default score
        }

        for data_point in &recent_history {
            let metrics = &data_point.metrics;

            // Cache effectiveness score
            let cache_score = if parameters.cache_size_mb >= 256 {
                0.8 // Good cache size
            } else if parameters.cache_size_mb >= 128 {
                0.6 // Decent cache size
            } else {
                0.3 // Small cache
            };

            // Write performance score based on workload
            let write_score = if metrics.write_ops_per_sec > 1000.0 {
                // Write-heavy workload
                if parameters.write_buffer_size >= 8192 && parameters.compaction_threads >= 4 {
                    0.9
                } else {
                    0.5
                }
            } else {
                // Read-heavy workload
                if parameters.cache_size_mb >= 512 {
                    0.8
                } else {
                    0.6
                }
            };

            // Bloom filter efficiency
            let bloom_score = if parameters.bloom_filter_bits_per_key >= 12 {
                0.8
            } else if parameters.bloom_filter_bits_per_key >= 10 {
                0.6
            } else {
                0.4
            };

            // Combined score for this data point
            let point_score = (cache_score * 0.4 + write_score * 0.4 + bloom_score * 0.2)
                * data_point.performance_score;
            score += point_score;
        }

        score / recent_history.len() as f64
    }

    /// Tournament selection for parent selection
    fn tournament_selection(&self) -> Result<ParameterGenome> {
        let tournament_size = 3;
        let mut best = &self.population[0];

        for _ in 0..tournament_size {
            let candidate_idx = rng().random_range(0..self.population.len());
            let candidate = &self.population[candidate_idx];
            if candidate.fitness > best.fitness {
                best = candidate;
            }
        }

        Ok(best.clone())
    }

    /// Crossover two parents to create offspring
    fn crossover(
        &self,
        parent1: &ParameterGenome,
        parent2: &ParameterGenome,
    ) -> Result<ParameterGenome> {
        let p1 = &parent1.parameters;
        let p2 = &parent2.parameters;

        // Uniform crossover - randomly pick each parameter from either parent
        let parameters = TuningParameters {
            cache_size_mb: if rand::random::<bool>() {
                p1.cache_size_mb
            } else {
                p2.cache_size_mb
            },
            write_buffer_size: if rand::random::<bool>() {
                p1.write_buffer_size
            } else {
                p2.write_buffer_size
            },
            compaction_threads: if rand::random::<bool>() {
                p1.compaction_threads
            } else {
                p2.compaction_threads
            },
            bloom_filter_bits_per_key: if rand::random::<bool>() {
                p1.bloom_filter_bits_per_key
            } else {
                p2.bloom_filter_bits_per_key
            },
            prefetch_distance: if rand::random::<bool>() {
                p1.prefetch_distance
            } else {
                p2.prefetch_distance
            },
            write_batch_size: if rand::random::<bool>() {
                p1.write_batch_size
            } else {
                p2.write_batch_size
            },
        };

        Ok(ParameterGenome {
            parameters,
            fitness: 0.0,
            generation: 0,
        })
    }

    /// Mutate parameters with small random changes
    fn mutate(&self, genome: &mut ParameterGenome) -> Result<()> {
        let params = &mut genome.parameters;

        // Mutate each parameter with 20% probability
        if rand::random::<f64>() < 0.2 {
            // Cache size mutation (±25%)
            let variation = (params.cache_size_mb as f64 * 0.25) as usize;
            let delta = rng().random_range(0..=variation * 2);
            params.cache_size_mb = (params.cache_size_mb + delta)
                .saturating_sub(variation)
                .max(64)
                .min(8192);
        }

        if rand::random::<f64>() < 0.2 {
            // Write buffer size mutation (power of 2 constraint)
            let factor = if rand::random::<bool>() { 2 } else { 1 };
            if rand::random::<bool>() {
                params.write_buffer_size = (params.write_buffer_size * factor).min(65536);
            } else {
                params.write_buffer_size = (params.write_buffer_size / factor).max(1024);
            }
        }

        if rand::random::<f64>() < 0.2 {
            // Compaction threads mutation (±1)
            if rand::random::<bool>() {
                params.compaction_threads = (params.compaction_threads + 1).min(16);
            } else {
                params.compaction_threads = params.compaction_threads.saturating_sub(1).max(1);
            }
        }

        if rand::random::<f64>() < 0.2 {
            // Bloom filter bits mutation (±2)
            let delta = rand::random::<u32>() % 5; // 0-4
            if rand::random::<bool>() {
                params.bloom_filter_bits_per_key =
                    (params.bloom_filter_bits_per_key + delta).min(20);
            } else {
                params.bloom_filter_bits_per_key = params
                    .bloom_filter_bits_per_key
                    .saturating_sub(delta)
                    .max(8);
            }
        }

        if rand::random::<f64>() < 0.2 {
            // Prefetch distance mutation (power of 2 constraint)
            let factor = if rand::random::<bool>() { 2 } else { 1 };
            if rand::random::<bool>() {
                params.prefetch_distance = (params.prefetch_distance * factor).min(128);
            } else {
                params.prefetch_distance = (params.prefetch_distance / factor).max(8);
            }
        }

        if rand::random::<f64>() < 0.2 {
            // Write batch size mutation (±20%)
            let variation = (params.write_batch_size as f64 * 0.2) as usize;
            let delta = rng().random_range(0..=variation * 2);
            params.write_batch_size = (params.write_batch_size + delta)
                .saturating_sub(variation)
                .max(100)
                .min(10000);
        }

        Ok(())
    }

    fn evolve(
        &mut self,
        history: &VecDeque<PerformanceDataPoint>,
    ) -> Result<Option<TuningParameters>> {
        if history.len() < 5 {
            return Ok(None); // Need some historical data
        }

        // Initialize population if empty
        if self.population.is_empty() {
            self.initialize_population()?;
        }

        // Evaluate fitness for each individual
        self.evaluate_population(history)?;

        // Sort population by fitness (descending)
        self.population.sort_by(|a, b| {
            b.fitness
                .partial_cmp(&a.fitness)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let best_fitness_before = self.population[0].fitness;

        // Evolve for one generation
        let mut new_population = Vec::new();

        // Keep elite individuals
        for i in 0..self.elite_size.min(self.population.len()) {
            new_population.push(self.population[i].clone());
        }

        // Generate offspring to fill the rest of the population
        while new_population.len() < self.population_size {
            // Tournament selection for parents
            let parent1 = self.tournament_selection()?;
            let parent2 = self.tournament_selection()?;

            // Crossover
            let mut offspring = self.crossover(&parent1, &parent2)?;

            // Mutation
            if rand::random::<f64>() < self.mutation_rate {
                self.mutate(&mut offspring)?;
            }

            // Set generation
            offspring.generation = self.current_generation + 1;

            new_population.push(offspring);
        }

        // Replace population
        self.population = new_population;
        self.current_generation += 1;

        // Evaluate new population
        self.evaluate_population(history)?;
        self.population.sort_by(|a, b| {
            b.fitness
                .partial_cmp(&a.fitness)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let best_fitness_after = self.population[0].fitness;

        // Return best configuration if improved
        if best_fitness_after > best_fitness_before * 1.01 {
            // 1% improvement threshold
            Ok(Some(self.population[0].parameters.clone()))
        } else {
            Ok(None)
        }
    }
}

impl BayesianOptimizer {
    fn new(max_iter: usize) -> Self {
        Self {
            gaussian_process: GaussianProcess::new(),
            acquisition_function: AcquisitionFunction::ExpectedImprovement { xi: 0.01 },
            evaluations: Vec::new(),
            max_iterations: max_iter,
        }
    }

    fn optimize(
        &mut self,
        history: &VecDeque<PerformanceDataPoint>,
    ) -> Result<Option<TuningParameters>> {
        if history.len() < 3 {
            return Ok(None); // Need some data for Bayesian optimization
        }

        // Extract parameter vectors and performance scores from history
        let recent_history: Vec<_> = history.iter().rev().take(20).collect();

        for data_point in &recent_history {
            let param_vector = self.parameters_to_vector(&data_point.configuration);
            let performance_score = data_point.performance_score;

            // Add to Gaussian process training data
            self.gaussian_process.inputs.push(param_vector);
            self.gaussian_process.outputs.push(performance_score);
        }

        // If we have enough data, optimize using Expected Improvement
        if self.gaussian_process.inputs.len() >= 5 {
            let best_candidate = self.find_best_candidate()?;
            return Ok(Some(self.vector_to_parameters(&best_candidate)));
        }

        // Otherwise, use random exploration
        self.random_exploration()
    }

    /// Convert TuningParameters to normalized parameter vector
    fn parameters_to_vector(&self, params: &TuningParameters) -> Vec<f64> {
        vec![
            (params.cache_size_mb as f64).ln() / 16.0, // Log-scale normalization
            (params.write_buffer_size as f64).ln() / 16.0,
            params.compaction_threads as f64 / 16.0,
            params.bloom_filter_bits_per_key as f64 / 20.0,
            (params.prefetch_distance as f64).ln() / 8.0,
            (params.write_batch_size as f64).ln() / 12.0,
        ]
    }

    /// Convert normalized parameter vector to TuningParameters
    fn vector_to_parameters(&self, vector: &[f64]) -> TuningParameters {
        TuningParameters {
            cache_size_mb: ((vector[0] * 16.0).exp() as usize).clamp(64, 8192),
            write_buffer_size: ((vector[1] * 16.0).exp() as usize)
                .clamp(1024, 65536)
                .next_power_of_two(),
            compaction_threads: ((vector[2] * 16.0) as usize).clamp(1, 16),
            bloom_filter_bits_per_key: ((vector[3] * 20.0) as u32).clamp(8, 20),
            prefetch_distance: ((vector[4] * 8.0).exp() as usize)
                .clamp(8, 128)
                .next_power_of_two(),
            write_batch_size: ((vector[5] * 12.0).exp() as usize).clamp(100, 10000),
        }
    }

    /// Find the best candidate using Expected Improvement acquisition function
    fn find_best_candidate(&self) -> Result<Vec<f64>> {
        let mut best_candidate = vec![0.5; 6]; // Start with middle values
        let mut best_acquisition = -f64::INFINITY;

        // Grid search over parameter space (simplified)
        let grid_size = 8;
        for i in 0..grid_size {
            for j in 0..grid_size {
                for k in 0..grid_size {
                    let candidate = vec![
                        i as f64 / (grid_size - 1) as f64,
                        j as f64 / (grid_size - 1) as f64,
                        k as f64 / (grid_size - 1) as f64,
                        0.5, // Fixed middle values for other dimensions for simplicity
                        0.5,
                        0.5,
                    ];

                    let acquisition_value = self.expected_improvement(&candidate)?;
                    if acquisition_value > best_acquisition {
                        best_acquisition = acquisition_value;
                        best_candidate = candidate;
                    }
                }
            }
        }

        Ok(best_candidate)
    }

    /// Calculate Expected Improvement acquisition function
    fn expected_improvement(&self, candidate: &[f64]) -> Result<f64> {
        if self.gaussian_process.outputs.is_empty() {
            return Ok(0.0);
        }

        // Simple GP prediction (mean and variance)
        let (mean, variance) = self.gaussian_process_predict(candidate)?;
        let best_observed = self
            .gaussian_process
            .outputs
            .iter()
            .fold(f64::NEG_INFINITY, |a, &b| a.max(b));

        // Expected Improvement calculation
        let sigma = variance.sqrt();
        if sigma < 1e-6 {
            return Ok(0.0);
        }

        let xi = 0.01; // Exploration parameter
        let z = (mean - best_observed - xi) / sigma;

        // Simplified EI calculation (without proper normal distribution functions)
        let ei = sigma * (z * self.phi(z) + self.gaussian_pdf(z));
        Ok(ei.max(0.0))
    }

    /// Simple Gaussian Process prediction
    fn gaussian_process_predict(&self, candidate: &[f64]) -> Result<(f64, f64)> {
        if self.gaussian_process.inputs.is_empty() {
            return Ok((0.5, 1.0)); // Default mean and variance
        }

        // Simplified GP: weighted average based on distance
        let mut weighted_sum = 0.0;
        let mut weight_sum = 0.0;

        for (i, input) in self.gaussian_process.inputs.iter().enumerate() {
            let distance = self.euclidean_distance(candidate, input);
            let weight = (-distance * distance
                / (2.0 * self.gaussian_process.kernel_params.length_scale))
                .exp();

            weighted_sum += weight * self.gaussian_process.outputs[i];
            weight_sum += weight;
        }

        let mean = if weight_sum > 1e-6 {
            weighted_sum / weight_sum
        } else {
            0.5
        };

        // Simple variance estimation
        let variance = self.gaussian_process.kernel_params.signal_variance
            * (1.0 - weight_sum / self.gaussian_process.inputs.len() as f64).max(0.1);

        Ok((mean, variance))
    }

    /// Euclidean distance between two parameter vectors
    fn euclidean_distance(&self, a: &[f64], b: &[f64]) -> f64 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y) * (x - y))
            .sum::<f64>()
            .sqrt()
    }

    /// Cumulative distribution function for standard normal
    fn phi(&self, z: f64) -> f64 {
        0.5 * (1.0 + self.erf(z / 2.0_f64.sqrt()))
    }

    /// Probability density function for standard normal
    fn gaussian_pdf(&self, z: f64) -> f64 {
        (-0.5 * z * z).exp() / (2.0 * std::f64::consts::PI).sqrt()
    }

    /// Error function approximation
    fn erf(&self, x: f64) -> f64 {
        // Abramowitz and Stegun approximation
        let a1 = 0.254829592;
        let a2 = -0.284496736;
        let a3 = 1.421413741;
        let a4 = -1.453152027;
        let a5 = 1.061405429;
        let p = 0.3275911;

        let sign = if x < 0.0 { -1.0 } else { 1.0 };
        let x = x.abs();

        let t = 1.0 / (1.0 + p * x);
        let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

        sign * y
    }

    /// Random exploration for early stages
    fn random_exploration(&self) -> Result<Option<TuningParameters>> {
        let params = TuningParameters {
            cache_size_mb: rng().random_range(128..2048), // 128MB to 2GB
            write_buffer_size: (rng().random_range(2048..32768) as usize).next_power_of_two(),
            compaction_threads: rng().random_range(2..=8), // 2 to 8 threads
            bloom_filter_bits_per_key: rng().random_range(10..=18), // 10 to 18 bits
            prefetch_distance: (rng().random_range(16..64) as usize).next_power_of_two(),
            write_batch_size: rng().random_range(500..3500), // 500 to 3500
        };

        Ok(Some(params))
    }
}

impl GaussianProcess {
    fn new() -> Self {
        Self {
            inputs: Vec::new(),
            outputs: Vec::new(),
            kernel_params: KernelParameters {
                length_scale: 1.0,
                signal_variance: 1.0,
                noise_variance: 0.1,
            },
        }
    }
}

impl PerformancePredictor {
    fn new(layer_sizes: &[usize]) -> Self {
        let mut layers = Vec::new();

        for i in 0..layer_sizes.len() - 1 {
            let input_size = layer_sizes[i];
            let output_size = layer_sizes[i + 1];

            layers.push(Layer {
                weights: vec![vec![0.1; input_size]; output_size],
                biases: vec![0.0; output_size],
                activation: if i == layer_sizes.len() - 2 {
                    ActivationFunction::Linear
                } else {
                    ActivationFunction::ReLU
                },
            });
        }

        Self {
            layers,
            learning_rate: 0.001,
            training_history: Vec::new(),
        }
    }

    fn train(&mut self, data_point: &PerformanceDataPoint) -> Result<()> {
        // Convert parameters to input vector
        let input = self.parameters_to_input_vector(&data_point.configuration);
        let target = data_point.performance_score;

        // Forward pass
        let output = self.forward(&input)?;

        // Calculate loss (mean squared error)
        let loss = (output[0] - target).powi(2);

        // Backward pass (simplified gradient descent)
        self.backward(&input, &[target])?;

        // Record training progress
        let epoch = TrainingEpoch {
            epoch: self.training_history.len(),
            loss,
            accuracy: 1.0 - (output[0] - target).abs(), // Simple accuracy measure
            timestamp: SystemTime::now(),
        };
        self.training_history.push(epoch);

        Ok(())
    }

    fn predict(&self, config: &TuningParameters) -> Result<f64> {
        let input = self.parameters_to_input_vector(config);
        let output = self.forward(&input)?;
        Ok(output[0].clamp(0.0, 1.0))
    }

    /// Convert parameters to input vector for neural network
    fn parameters_to_input_vector(&self, params: &TuningParameters) -> Vec<f64> {
        vec![
            (params.cache_size_mb as f64).ln() / 16.0, // Log-normalized
            (params.write_buffer_size as f64).ln() / 16.0,
            params.compaction_threads as f64 / 16.0,
            params.bloom_filter_bits_per_key as f64 / 20.0,
            (params.prefetch_distance as f64).ln() / 8.0,
            (params.write_batch_size as f64).ln() / 12.0,
            // Add some derived features
            (params.cache_size_mb as f64 / params.write_buffer_size as f64).ln() / 10.0, // Cache/buffer ratio
            (params.compaction_threads as f64 * params.bloom_filter_bits_per_key as f64) / 100.0, // Thread-bloom interaction
            params.prefetch_distance as f64 / params.write_batch_size as f64, // Prefetch efficiency
            1.0,                                                              // Bias term
        ]
    }

    /// Forward pass through the neural network
    fn forward(&self, input: &[f64]) -> Result<Vec<f64>> {
        let mut current_values = input.to_vec();

        for layer in &self.layers {
            current_values = self.apply_layer(&current_values, layer)?;
        }

        Ok(current_values)
    }

    /// Apply a single layer transformation
    fn apply_layer(&self, input: &[f64], layer: &Layer) -> Result<Vec<f64>> {
        let mut output = vec![0.0; layer.biases.len()];

        // Matrix multiplication: output = weights * input + biases
        for (i, output_neuron) in output.iter_mut().enumerate() {
            *output_neuron = layer.biases[i];
            for (j, &input_val) in input.iter().enumerate() {
                if j < layer.weights[i].len() {
                    *output_neuron += layer.weights[i][j] * input_val;
                }
            }
        }

        // Apply activation function
        for value in &mut output {
            *value = self.apply_activation(*value, &layer.activation);
        }

        Ok(output)
    }

    /// Apply activation function
    fn apply_activation(&self, x: f64, activation: &ActivationFunction) -> f64 {
        match activation {
            ActivationFunction::ReLU => x.max(0.0),
            ActivationFunction::Sigmoid => 1.0 / (1.0 + (-x).exp()),
            ActivationFunction::Tanh => x.tanh(),
            ActivationFunction::Linear => x,
        }
    }

    /// Derivative of activation function
    fn activation_derivative(&self, x: f64, activation: &ActivationFunction) -> f64 {
        match activation {
            ActivationFunction::ReLU => {
                if x > 0.0 {
                    1.0
                } else {
                    0.0
                }
            }
            ActivationFunction::Sigmoid => {
                let sigmoid = 1.0 / (1.0 + (-x).exp());
                sigmoid * (1.0 - sigmoid)
            }
            ActivationFunction::Tanh => 1.0 - x.tanh().powi(2),
            ActivationFunction::Linear => 1.0,
        }
    }

    /// Backward pass for training (simplified gradient descent)
    fn backward(&mut self, input: &[f64], target: &[f64]) -> Result<()> {
        // Forward pass to get intermediate values
        let mut layer_inputs = vec![input.to_vec()];
        let mut layer_outputs = vec![];

        let mut current = input.to_vec();
        for layer in &self.layers {
            let output = self.apply_layer(&current, layer)?;
            layer_outputs.push(output.clone());
            layer_inputs.push(output.clone());
            current = output;
        }

        // Backward pass
        let mut deltas = vec![vec![0.0; target.len()]];

        // Output layer error
        for i in 0..target.len() {
            deltas[0][i] = 2.0 * (layer_outputs.last().unwrap()[i] - target[i]);
            // MSE derivative
        }

        // Backpropagate errors
        for layer_idx in (0..self.layers.len()).rev() {
            let layer_input = &layer_inputs[layer_idx];
            let layer_output = &layer_outputs[layer_idx];

            // Calculate gradients and update weights (avoid borrowing conflicts)
            let num_weights = self.layers[layer_idx].weights.len();
            let mut weight_updates = Vec::new();
            let mut bias_updates = Vec::new();

            for i in 0..num_weights {
                let activation_deriv =
                    self.activation_derivative(layer_output[i], &self.layers[layer_idx].activation);
                let mut weight_row_updates = Vec::new();

                for j in 0..self.layers[layer_idx].weights[i].len() {
                    if j < layer_input.len() {
                        let gradient = deltas[0][i] * activation_deriv * layer_input[j];
                        weight_row_updates.push(-self.learning_rate * gradient);
                    } else {
                        weight_row_updates.push(0.0);
                    }
                }
                weight_updates.push(weight_row_updates);

                // Calculate bias update
                let bias_gradient = deltas[0][i] * activation_deriv;
                bias_updates.push(-self.learning_rate * bias_gradient);
            }

            // Apply weight updates
            for (i, weight_row_updates) in weight_updates.iter().enumerate() {
                for (j, &update) in weight_row_updates.iter().enumerate() {
                    self.layers[layer_idx].weights[i][j] += update;
                }
            }

            // Apply bias updates
            for (i, &bias_update) in bias_updates.iter().enumerate() {
                self.layers[layer_idx].biases[i] += bias_update;
            }

            // Prepare deltas for next layer (if not input layer)
            if layer_idx > 0 {
                let mut next_deltas = vec![0.0; layer_input.len()];
                for j in 0..layer_input.len() {
                    for i in 0..deltas[0].len() {
                        if j < self.layers[layer_idx].weights[i].len() {
                            let activation_deriv = self.activation_derivative(
                                layer_output[i],
                                &self.layers[layer_idx].activation,
                            );
                            next_deltas[j] += deltas[0][i]
                                * activation_deriv
                                * self.layers[layer_idx].weights[i][j];
                        }
                    }
                }
                deltas[0] = next_deltas;
            }
        }

        Ok(())
    }
}

impl ReinforcementLearningAgent {
    fn new(lr: f64, discount: f64, exploration: f64, decay: f64) -> Self {
        Self {
            q_table: HashMap::new(),
            learning_rate: lr,
            discount_factor: discount,
            exploration_rate: exploration,
            exploration_decay: decay,
            current_state: None,
            action_space: Self::create_action_space(),
        }
    }

    fn create_action_space() -> Vec<TuningAction> {
        vec![
            TuningAction {
                parameter: "cache_size_mb".to_string(),
                adjustment: ParameterAdjustment::Increase,
            },
            TuningAction {
                parameter: "cache_size_mb".to_string(),
                adjustment: ParameterAdjustment::Decrease,
            },
            TuningAction {
                parameter: "write_buffer_size".to_string(),
                adjustment: ParameterAdjustment::Increase,
            },
            TuningAction {
                parameter: "write_buffer_size".to_string(),
                adjustment: ParameterAdjustment::Decrease,
            },
        ]
    }

    fn update(&mut self, data_point: &PerformanceDataPoint) -> Result<()> {
        let state = self.metrics_to_state(&data_point.metrics);
        let reward = self.calculate_reward(data_point);

        // Update current state-action value if we have a previous state-action pair
        if let Some(prev_state) = &self.current_state {
            // Find the action that led to this state
            let action = self.infer_action_from_config(&data_point.configuration);
            let state_action = StateAction {
                state: prev_state.clone(),
                action,
            };

            // Q-learning update: Q(s,a) = Q(s,a) + α[r + γ*max(Q(s',a')) - Q(s,a)]
            let current_q = *self.q_table.get(&state_action).unwrap_or(&0.0);
            let max_next_q = self.get_max_q_value(&state);
            let new_q = current_q
                + self.learning_rate * (reward + self.discount_factor * max_next_q - current_q);

            self.q_table.insert(state_action, new_q);
        }

        // Update current state
        self.current_state = Some(state);

        // Decay exploration rate
        self.exploration_rate *= self.exploration_decay;
        self.exploration_rate = self.exploration_rate.max(0.01); // Minimum exploration

        Ok(())
    }

    fn get_optimal_action(
        &mut self,
        history: &VecDeque<PerformanceDataPoint>,
    ) -> Result<Option<TuningParameters>> {
        if history.is_empty() {
            return Ok(None);
        }

        let latest_metrics = &history.back().unwrap().metrics;
        let current_state = self.metrics_to_state(latest_metrics);

        // ε-greedy action selection
        let action = if rand::random::<f64>() < self.exploration_rate {
            // Explore: choose random action
            self.random_action()
        } else {
            // Exploit: choose best known action
            self.get_best_action(&current_state)
        };

        // Convert action to parameter configuration
        let current_config = &history.back().unwrap().configuration;
        let new_config = self.apply_action(current_config, &action);

        Ok(Some(new_config))
    }

    /// Convert performance metrics to discrete state representation
    fn metrics_to_state(&self, metrics: &PerformanceMetrics) -> PerformanceState {
        // Discretize metrics into buckets for state representation
        let read_ops_bucket = if metrics.read_ops_per_sec < 1000.0 {
            0
        } else if metrics.read_ops_per_sec < 5000.0 {
            1
        } else if metrics.read_ops_per_sec < 10000.0 {
            2
        } else {
            3
        };

        let write_ops_bucket = if metrics.write_ops_per_sec < 500.0 {
            0
        } else if metrics.write_ops_per_sec < 2000.0 {
            1
        } else if metrics.write_ops_per_sec < 5000.0 {
            2
        } else {
            3
        };

        let memory_usage_bucket = if metrics.memory_usage_mb < 512 {
            0
        } else if metrics.memory_usage_mb < 1024 {
            1
        } else if metrics.memory_usage_mb < 2048 {
            2
        } else {
            3
        };

        let latency_bucket = if metrics.read_latency_us.p50 < 1.0 {
            0
        } else if metrics.read_latency_us.p50 < 5.0 {
            1
        } else if metrics.read_latency_us.p50 < 10.0 {
            2
        } else {
            3
        };

        // Determine workload type from read/write ratio
        let workload_type = if metrics.read_ops_per_sec > metrics.write_ops_per_sec * 3.0 {
            WorkloadType::ReadHeavy
        } else if metrics.write_ops_per_sec > metrics.read_ops_per_sec * 2.0 {
            WorkloadType::WriteHeavy
        } else {
            WorkloadType::Mixed
        };

        PerformanceState {
            read_ops_bucket,
            write_ops_bucket,
            memory_usage_bucket,
            latency_bucket,
            workload_type,
        }
    }

    /// Calculate reward based on performance improvement
    fn calculate_reward(&self, data_point: &PerformanceDataPoint) -> f64 {
        // Reward is based on performance score
        let base_reward = data_point.performance_score;

        // Bonus for high throughput
        let throughput_bonus = if data_point.metrics.mixed_ops_per_sec > 10000.0 {
            0.2
        } else if data_point.metrics.mixed_ops_per_sec > 5000.0 {
            0.1
        } else {
            0.0
        };

        // Penalty for high latency
        let latency_penalty = if data_point.metrics.read_latency_us.p50 > 10.0 {
            -0.2
        } else if data_point.metrics.read_latency_us.p50 > 5.0 {
            -0.1
        } else {
            0.0
        };

        // Penalty for high memory usage
        let memory_penalty = if data_point.metrics.memory_usage_mb > 2048 {
            -0.1
        } else {
            0.0
        };

        (base_reward + throughput_bonus + latency_penalty + memory_penalty).clamp(-1.0, 1.0)
    }

    /// Infer action from configuration change
    fn infer_action_from_config(&self, _config: &TuningParameters) -> TuningAction {
        // Simplified: return a default action
        // In a real implementation, this would compare with previous config
        TuningAction {
            parameter: "cache_size_mb".to_string(),
            adjustment: ParameterAdjustment::NoChange,
        }
    }

    /// Get maximum Q-value for a given state
    fn get_max_q_value(&self, state: &PerformanceState) -> f64 {
        self.action_space
            .iter()
            .map(|action| {
                let state_action = StateAction {
                    state: state.clone(),
                    action: action.clone(),
                };
                *self.q_table.get(&state_action).unwrap_or(&0.0)
            })
            .fold(f64::NEG_INFINITY, f64::max)
            .max(0.0) // Default to 0 if no actions found
    }

    /// Get best action for a given state
    fn get_best_action(&self, state: &PerformanceState) -> TuningAction {
        let mut best_action = &self.action_space[0];
        let mut best_q = f64::NEG_INFINITY;

        for action in &self.action_space {
            let state_action = StateAction {
                state: state.clone(),
                action: action.clone(),
            };
            let q_value = *self.q_table.get(&state_action).unwrap_or(&0.0);
            if q_value > best_q {
                best_q = q_value;
                best_action = action;
            }
        }

        best_action.clone()
    }

    /// Choose random action for exploration
    fn random_action(&self) -> TuningAction {
        let idx = rng().random_range(0..self.action_space.len());
        self.action_space[idx].clone()
    }

    /// Apply action to configuration to get new configuration
    fn apply_action(
        &self,
        current_config: &TuningParameters,
        action: &TuningAction,
    ) -> TuningParameters {
        let mut new_config = current_config.clone();

        match action.parameter.as_str() {
            "cache_size_mb" => match action.adjustment {
                ParameterAdjustment::Increase => {
                    new_config.cache_size_mb = (new_config.cache_size_mb * 2).min(8192);
                }
                ParameterAdjustment::Decrease => {
                    new_config.cache_size_mb = (new_config.cache_size_mb / 2).max(64);
                }
                ParameterAdjustment::NoChange => {}
            },
            "write_buffer_size" => match action.adjustment {
                ParameterAdjustment::Increase => {
                    new_config.write_buffer_size = (new_config.write_buffer_size * 2).min(65536);
                }
                ParameterAdjustment::Decrease => {
                    new_config.write_buffer_size = (new_config.write_buffer_size / 2).max(1024);
                }
                ParameterAdjustment::NoChange => {}
            },
            "compaction_threads" => match action.adjustment {
                ParameterAdjustment::Increase => {
                    new_config.compaction_threads = (new_config.compaction_threads + 1).min(16);
                }
                ParameterAdjustment::Decrease => {
                    new_config.compaction_threads =
                        new_config.compaction_threads.saturating_sub(1).max(1);
                }
                ParameterAdjustment::NoChange => {}
            },
            "bloom_filter_bits_per_key" => match action.adjustment {
                ParameterAdjustment::Increase => {
                    new_config.bloom_filter_bits_per_key =
                        (new_config.bloom_filter_bits_per_key + 2).min(20);
                }
                ParameterAdjustment::Decrease => {
                    new_config.bloom_filter_bits_per_key = new_config
                        .bloom_filter_bits_per_key
                        .saturating_sub(2)
                        .max(8);
                }
                ParameterAdjustment::NoChange => {}
            },
            _ => {} // Unknown parameter
        }

        new_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::performance_tuning::{LatencyMetrics, PerformanceMetrics};

    #[test]
    fn test_ml_autotuner_creation() {
        let config = AutoTuningConfig::default();
        let tuner = MLAutoTuner::new(config);

        assert!(tuner.tuning_config.enabled);
        assert_eq!(tuner.metrics_history.len(), 0);
    }

    #[test]
    fn test_performance_score_calculation() {
        let config = AutoTuningConfig::default();
        let tuner = MLAutoTuner::new(config);

        let metrics = PerformanceMetrics {
            read_ops_per_sec: 5000.0,
            write_ops_per_sec: 2500.0,
            mixed_ops_per_sec: 7500.0,
            read_latency_us: LatencyMetrics {
                p50: 1.0,
                p95: 2.5,
                p99: 5.0,
                p999: 10.0,
                max: 20.0,
            },
            write_latency_us: LatencyMetrics {
                p50: 1.2,
                p95: 3.0,
                p99: 6.0,
                p999: 12.0,
                max: 24.0,
            },
            memory_usage_mb: 100,
            cpu_usage_percent: 50.0,
        };

        let score = tuner.calculate_performance_score(&metrics);
        assert!(score > 0.0 && score <= 1.0);
    }

    #[test]
    fn test_workload_classification() {
        let mut classifier = WorkloadClassifier::new();

        let read_heavy_metrics = PerformanceMetrics {
            read_ops_per_sec: 10000.0,
            write_ops_per_sec: 1000.0,
            mixed_ops_per_sec: 11000.0,
            read_latency_us: LatencyMetrics {
                p50: 0.5,
                p95: 1.2,
                p99: 2.5,
                p999: 5.0,
                max: 10.0,
            },
            write_latency_us: LatencyMetrics {
                p50: 0.8,
                p95: 2.0,
                p99: 4.0,
                p999: 8.0,
                max: 16.0,
            },
            memory_usage_mb: 200,
            cpu_usage_percent: 30.0,
        };

        let classification = classifier.classify(&read_heavy_metrics);
        assert_eq!(classification.workload_type, WorkloadType::ReadHeavy);
        assert!(classification.confidence > 0.0);
    }

    #[test]
    fn test_safety_limits() {
        let config = AutoTuningConfig::default();
        let tuner = MLAutoTuner::new(config);

        let mut unsafe_config = TuningParameters::default();
        unsafe_config.cache_size_mb = 16384; // Too large
        unsafe_config.compaction_threads = 50; // Too many

        let safe_config = tuner.apply_safety_limits(unsafe_config);

        assert!(safe_config.cache_size_mb <= tuner.tuning_config.safety_limits.max_cache_size_mb);
        assert!(
            safe_config.compaction_threads
                <= tuner.tuning_config.safety_limits.max_compaction_threads
        );
    }
}
