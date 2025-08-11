//! Auto-Tuning Engine
//!
//! Uses machine learning and heuristics to automatically optimize database
//! configuration based on workload patterns and system performance.

use super::{HardwareInfo, TuningConfig, WorkloadProfile};
use crate::Result;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Auto-tuning engine
pub struct AutoTuner {
    config: TuningConfig,
    tuning_history: Vec<TuningAttempt>,
    best_configs: HashMap<String, ConfigScore>,
    learning_rate: f64,
    exploration_rate: f64,
}

/// Tuning strategy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TuningStrategy {
    HillClimbing,          // Local optimization
    SimulatedAnnealing,    // Global optimization with random exploration
    GeneticAlgorithm,      // Population-based optimization
    BayesianOptimization,  // Model-based optimization
    ReinforcementLearning, // Learn from feedback
}

/// Configuration parameter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigParameter {
    pub name: String,
    pub value: ParameterValue,
    pub min_value: ParameterValue,
    pub max_value: ParameterValue,
    pub step: Option<ParameterValue>,
    pub importance: f64, // 0.0 to 1.0
}

/// Parameter value types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ParameterValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Enum(String, Vec<String>), // (current, options)
}

/// Tuning attempt record
#[derive(Debug, Clone)]
struct TuningAttempt {
    timestamp: std::time::SystemTime,
    parameters: HashMap<String, ParameterValue>,
    performance_score: f64,
    resource_usage: ResourceUsage,
    workload_fingerprint: String,
}

/// Resource usage metrics
#[derive(Debug, Clone)]
struct ResourceUsage {
    memory_mb: u64,
    cpu_percent: f64,
    disk_io_mb_per_sec: f64,
}

/// Configuration score
#[derive(Debug, Clone)]
struct ConfigScore {
    parameters: HashMap<String, ParameterValue>,
    score: f64,
    attempts: usize,
    last_updated: std::time::SystemTime,
}

impl AutoTuner {
    /// Create a new auto-tuner
    pub fn new(config: TuningConfig) -> Self {
        Self {
            config,
            tuning_history: Vec::new(),
            best_configs: HashMap::new(),
            learning_rate: 0.1,
            exploration_rate: 0.2,
        }
    }

    /// Get next configuration to try
    pub fn suggest_next_config(
        &mut self,
        current_params: &HashMap<String, ParameterValue>,
        current_score: f64,
        hardware: &HardwareInfo,
        workload: &WorkloadProfile,
    ) -> Result<HashMap<String, ParameterValue>> {
        let strategy = self.select_strategy(workload);
        let workload_key = self.get_workload_fingerprint(workload);

        // Record current attempt
        self.record_attempt(current_params.clone(), current_score, workload_key.clone());

        // Generate next configuration based on strategy
        match strategy {
            TuningStrategy::HillClimbing => {
                self.hill_climbing_step(current_params, current_score, hardware, workload)
            }
            TuningStrategy::SimulatedAnnealing => {
                self.simulated_annealing_step(current_params, current_score, hardware, workload)
            }
            TuningStrategy::BayesianOptimization => {
                self.bayesian_optimization_step(current_params, current_score, hardware, workload)
            }
            _ => {
                // Default to hill climbing
                self.hill_climbing_step(current_params, current_score, hardware, workload)
            }
        }
    }

    /// Select tuning strategy based on workload
    fn select_strategy(&self, workload: &WorkloadProfile) -> TuningStrategy {
        // Use different strategies based on workload characteristics
        match workload.workload_type {
            super::WorkloadType::OLTP => TuningStrategy::HillClimbing,
            super::WorkloadType::OLAP => TuningStrategy::BayesianOptimization,
            _ => TuningStrategy::SimulatedAnnealing,
        }
    }

    /// Get workload fingerprint for caching
    fn get_workload_fingerprint(&self, workload: &WorkloadProfile) -> String {
        format!(
            "{:?}_r{:.0}_w{:.0}_seq{:.0}",
            workload.workload_type,
            workload.read_ratio * 100.0,
            workload.write_ratio * 100.0,
            workload.sequential_access_ratio * 100.0
        )
    }

    /// Record tuning attempt
    fn record_attempt(
        &mut self,
        parameters: HashMap<String, ParameterValue>,
        score: f64,
        workload_key: String,
    ) {
        self.tuning_history.push(TuningAttempt {
            timestamp: std::time::SystemTime::now(),
            parameters: parameters.clone(),
            performance_score: score,
            resource_usage: ResourceUsage {
                memory_mb: 0, // Would be filled from actual metrics
                cpu_percent: 0.0,
                disk_io_mb_per_sec: 0.0,
            },
            workload_fingerprint: workload_key.clone(),
        });

        // Update best configuration for this workload
        if let Some(best) = self.best_configs.get_mut(&workload_key) {
            if score > best.score {
                best.parameters = parameters;
                best.score = score;
                best.attempts += 1;
                best.last_updated = std::time::SystemTime::now();
            }
        } else {
            self.best_configs.insert(
                workload_key,
                ConfigScore {
                    parameters,
                    score,
                    attempts: 1,
                    last_updated: std::time::SystemTime::now(),
                },
            );
        }
    }

    /// Hill climbing optimization step
    fn hill_climbing_step(
        &self,
        current_params: &HashMap<String, ParameterValue>,
        _current_score: f64,
        hardware: &HardwareInfo,
        workload: &WorkloadProfile,
    ) -> Result<HashMap<String, ParameterValue>> {
        let mut new_params = current_params.clone();
        let tunable_params = self.get_tunable_parameters(hardware, workload);

        // Randomly select a parameter to modify
        let mut rng = rand::rng();
        if let Some(param) = tunable_params.get(rng.random_range(0..tunable_params.len())) {
            new_params.insert(
                param.name.clone(),
                self.adjust_parameter(&param.value, &param, 1.0)?,
            );
        }

        Ok(new_params)
    }

    /// Simulated annealing optimization step
    fn simulated_annealing_step(
        &self,
        current_params: &HashMap<String, ParameterValue>,
        _current_score: f64,
        hardware: &HardwareInfo,
        workload: &WorkloadProfile,
    ) -> Result<HashMap<String, ParameterValue>> {
        let mut new_params = current_params.clone();
        let tunable_params = self.get_tunable_parameters(hardware, workload);

        // Calculate temperature (decreases over time)
        let temperature = 1.0 / (self.tuning_history.len() as f64 + 1.0).sqrt();

        let mut rng = rand::rng();

        // Potentially modify multiple parameters based on temperature
        for param in &tunable_params {
            if rng.gen::<f64>() < temperature * param.importance {
                new_params.insert(
                    param.name.clone(),
                    self.adjust_parameter(&param.value, param, temperature)?,
                );
            }
        }

        Ok(new_params)
    }

    /// Bayesian optimization step
    fn bayesian_optimization_step(
        &self,
        current_params: &HashMap<String, ParameterValue>,
        _current_score: f64,
        hardware: &HardwareInfo,
        workload: &WorkloadProfile,
    ) -> Result<HashMap<String, ParameterValue>> {
        // Simplified Bayesian optimization
        // In production, would use Gaussian processes or similar

        let mut new_params = current_params.clone();
        let workload_key = self.get_workload_fingerprint(workload);

        // If we have a good configuration for this workload, use it as a base
        if let Some(best_config) = self.best_configs.get(&workload_key) {
            if best_config.attempts > 5 {
                // Use best known configuration with small perturbations
                new_params = best_config.parameters.clone();

                // Add small random perturbations
                let tunable_params = self.get_tunable_parameters(hardware, workload);
                let mut rng = rand::rng();

                if rng.gen::<f64>() < self.exploration_rate {
                    if let Some(param) = tunable_params.get(rng.random_range(0..tunable_params.len()))
                    {
                        new_params.insert(
                            param.name.clone(),
                            self.adjust_parameter(&param.value, param, 0.1)?,
                        );
                    }
                }

                return Ok(new_params);
            }
        }

        // Otherwise, use exploration
        self.simulated_annealing_step(current_params, _current_score, hardware, workload)
    }

    /// Get tunable parameters based on hardware and workload
    fn get_tunable_parameters(
        &self,
        hardware: &HardwareInfo,
        workload: &WorkloadProfile,
    ) -> Vec<ConfigParameter> {
        let mut params = Vec::new();

        // Cache size (critical for performance)
        params.push(ConfigParameter {
            name: "cache_size".to_string(),
            value: ParameterValue::Integer((hardware.available_memory_mb / 4 * 1024 * 1024) as i64),
            min_value: ParameterValue::Integer(0),
            max_value: ParameterValue::Integer((hardware.available_memory_mb * 1024 * 1024) as i64),
            step: Some(ParameterValue::Integer(64 * 1024 * 1024)), // 64MB steps
            importance: 0.9,
        });

        // Prefetch settings
        if workload.sequential_access_ratio > 0.3 {
            params.push(ConfigParameter {
                name: "prefetch_distance".to_string(),
                value: ParameterValue::Integer(32),
                min_value: ParameterValue::Integer(0),
                max_value: ParameterValue::Integer(256),
                step: Some(ParameterValue::Integer(8)),
                importance: 0.7,
            });

            params.push(ConfigParameter {
                name: "prefetch_workers".to_string(),
                value: ParameterValue::Integer(2),
                min_value: ParameterValue::Integer(0),
                max_value: ParameterValue::Integer(hardware.cpu_count as i64 / 2),
                step: Some(ParameterValue::Integer(1)),
                importance: 0.5,
            });
        }

        // Write batch size
        if workload.write_ratio > 0.2 {
            params.push(ConfigParameter {
                name: "write_batch_size".to_string(),
                value: ParameterValue::Integer(1000),
                min_value: ParameterValue::Integer(1),
                max_value: ParameterValue::Integer(100000),
                step: Some(ParameterValue::Integer(100)),
                importance: 0.6,
            });
        }

        // Compression settings
        params.push(ConfigParameter {
            name: "compression_type".to_string(),
            value: ParameterValue::Enum(
                "zstd".to_string(),
                vec![
                    "none".to_string(),
                    "lz4".to_string(),
                    "zstd".to_string(),
                    "snappy".to_string(),
                ],
            ),
            min_value: ParameterValue::Enum("none".to_string(), vec![]),
            max_value: ParameterValue::Enum("zstd".to_string(), vec![]),
            step: None,
            importance: 0.4,
        });

        params
    }

    /// Adjust parameter value
    fn adjust_parameter(
        &self,
        current: &ParameterValue,
        param: &ConfigParameter,
        magnitude: f64,
    ) -> Result<ParameterValue> {
        let mut rng = rand::rng();

        match (current, &param.min_value, &param.max_value) {
            (
                ParameterValue::Integer(val),
                ParameterValue::Integer(min),
                ParameterValue::Integer(max),
            ) => {
                let step = match &param.step {
                    Some(ParameterValue::Integer(s)) => *s,
                    _ => 1,
                };

                let range = ((*max - *min) as f64 * magnitude * 0.2) as i64;
                let delta = rng.random_range(-range..=range) / step * step;
                let new_val = (*val + delta).clamp(*min, *max);

                Ok(ParameterValue::Integer(new_val))
            }
            (
                ParameterValue::Float(val),
                ParameterValue::Float(min),
                ParameterValue::Float(max),
            ) => {
                let range = (*max - *min) * magnitude * 0.2;
                let delta = rng.random_range(-range..=range);
                let new_val = (*val + delta).clamp(*min, *max);

                Ok(ParameterValue::Float(new_val))
            }
            (ParameterValue::Boolean(val), _, _) => {
                if rng.gen::<f64>() < magnitude * 0.1 {
                    Ok(ParameterValue::Boolean(!val))
                } else {
                    Ok(ParameterValue::Boolean(*val))
                }
            }
            (ParameterValue::Enum(current, options), _, _) => {
                if rng.gen::<f64>() < magnitude * 0.2 && !options.is_empty() {
                    let new_idx = rng.random_range(0..options.len());
                    Ok(ParameterValue::Enum(
                        options[new_idx].clone(),
                        options.clone(),
                    ))
                } else {
                    Ok(ParameterValue::Enum(current.clone(), options.clone()))
                }
            }
            _ => Ok(current.clone()),
        }
    }

    /// Get learning progress
    pub fn get_progress(&self) -> TuningProgress {
        let total_attempts = self.tuning_history.len();
        let successful_attempts = self
            .tuning_history
            .iter()
            .filter(|a| a.performance_score > 0.0)
            .count();

        let best_score = self
            .tuning_history
            .iter()
            .map(|a| a.performance_score)
            .fold(0.0, f64::max);

        let improvement_rate = if total_attempts > 10 {
            let recent_avg = self
                .tuning_history
                .iter()
                .rev()
                .take(5)
                .map(|a| a.performance_score)
                .sum::<f64>()
                / 5.0;

            let initial_avg = self
                .tuning_history
                .iter()
                .take(5)
                .map(|a| a.performance_score)
                .sum::<f64>()
                / 5.0;

            if initial_avg > 0.0 {
                (recent_avg - initial_avg) / initial_avg
            } else {
                0.0
            }
        } else {
            0.0
        };

        TuningProgress {
            total_attempts,
            successful_attempts,
            best_score,
            improvement_rate,
            workloads_tuned: self.best_configs.len(),
        }
    }
}

/// Tuning progress metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningProgress {
    pub total_attempts: usize,
    pub successful_attempts: usize,
    pub best_score: f64,
    pub improvement_rate: f64,
    pub workloads_tuned: usize,
}

impl ParameterValue {
    /// Convert to configuration value
    pub fn to_config_value(&self) -> String {
        match self {
            ParameterValue::Integer(v) => v.to_string(),
            ParameterValue::Float(v) => v.to_string(),
            ParameterValue::Boolean(v) => v.to_string(),
            ParameterValue::Enum(v, _) => v.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_tuner_creation() {
        let config = TuningConfig::default();
        let _tuner = AutoTuner::new(config);
    }

    #[test]
    fn test_parameter_adjustment() {
        let tuner = AutoTuner::new(TuningConfig::default());

        let param = ConfigParameter {
            name: "test_param".to_string(),
            value: ParameterValue::Integer(100),
            min_value: ParameterValue::Integer(0),
            max_value: ParameterValue::Integer(1000),
            step: Some(ParameterValue::Integer(10)),
            importance: 0.5,
        };

        let adjusted = tuner.adjust_parameter(&param.value, &param, 1.0).unwrap();
        match adjusted {
            ParameterValue::Integer(v) => {
                assert!(v >= 0 && v <= 1000);
                assert_eq!(v % 10, 0); // Should respect step size
            }
            _ => panic!("Expected integer parameter"),
        }
    }
}
