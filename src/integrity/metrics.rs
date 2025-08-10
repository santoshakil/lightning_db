use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};

/// Comprehensive metrics for data integrity validation
pub struct IntegrityMetrics {
    // Global counters
    pub total_validations: AtomicU64,
    pub successful_validations: AtomicU64,
    pub failed_validations: AtomicU64,
    pub skipped_validations: AtomicU64,
    pub timeout_validations: AtomicU64,
    
    // Timing metrics
    pub total_validation_time_micros: AtomicU64,
    pub max_validation_time_micros: AtomicU64,
    pub min_validation_time_micros: AtomicU64,
    
    // Error tracking
    pub checksum_errors: AtomicU64,
    pub structural_errors: AtomicU64,
    pub reference_errors: AtomicU64,
    pub temporal_errors: AtomicU64,
    pub critical_errors: AtomicU64,
    
    // Operation-specific metrics
    operations: RwLock<HashMap<String, OperationMetrics>>,
    
    // Validation path metrics
    paths: RwLock<HashMap<String, PathMetrics>>,
    
    // Historical data (recent operations)
    history: RwLock<ValidationHistory>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationMetrics {
    pub total_calls: u64,
    pub successful_calls: u64,
    pub failed_calls: u64,
    pub total_duration_micros: u64,
    pub max_duration_micros: u64,
    pub min_duration_micros: u64,
    pub average_duration_micros: u64,
    pub last_call_time: chrono::DateTime<chrono::Utc>,
    pub error_breakdown: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathMetrics {
    pub validation_count: u64,
    pub error_count: u64,
    pub last_error_time: Option<chrono::DateTime<chrono::Utc>>,
    pub error_rate: f64,
    pub avg_validation_time_micros: u64,
}

#[derive(Debug)]
struct ValidationHistory {
    entries: Vec<ValidationEntry>,
    max_entries: usize,
}

#[derive(Debug, Clone)]
struct ValidationEntry {
    timestamp: Instant,
    operation: String,
    path: String,
    success: bool,
    duration: Duration,
    error_type: Option<String>,
}

impl IntegrityMetrics {
    pub fn new() -> Self {
        Self {
            total_validations: AtomicU64::new(0),
            successful_validations: AtomicU64::new(0),
            failed_validations: AtomicU64::new(0),
            skipped_validations: AtomicU64::new(0),
            timeout_validations: AtomicU64::new(0),
            total_validation_time_micros: AtomicU64::new(0),
            max_validation_time_micros: AtomicU64::new(0),
            min_validation_time_micros: AtomicU64::new(u64::MAX),
            checksum_errors: AtomicU64::new(0),
            structural_errors: AtomicU64::new(0),
            reference_errors: AtomicU64::new(0),
            temporal_errors: AtomicU64::new(0),
            critical_errors: AtomicU64::new(0),
            operations: RwLock::new(HashMap::new()),
            paths: RwLock::new(HashMap::new()),
            history: RwLock::new(ValidationHistory::new()),
        }
    }

    /// Record a validation operation
    pub fn record_validation(
        &self,
        operation: &str,
        success: bool,
        duration: Duration,
    ) {
        self.record_validation_with_path(operation, "unknown", success, duration, None);
    }

    /// Record a validation operation with path and error information
    pub fn record_validation_with_path(
        &self,
        operation: &str,
        path: &str,
        success: bool,
        duration: Duration,
        error_type: Option<&str>,
    ) {
        // Update global counters
        self.total_validations.fetch_add(1, Ordering::Relaxed);
        
        if success {
            self.successful_validations.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_validations.fetch_add(1, Ordering::Relaxed);
        }

        // Update timing metrics
        let duration_micros = duration.as_micros() as u64;
        self.total_validation_time_micros.fetch_add(duration_micros, Ordering::Relaxed);
        
        // Update max timing (using compare and swap for accuracy)
        loop {
            let current_max = self.max_validation_time_micros.load(Ordering::Relaxed);
            if duration_micros <= current_max {
                break;
            }
            if self.max_validation_time_micros.compare_exchange_weak(
                current_max,
                duration_micros,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ).is_ok() {
                break;
            }
        }

        // Update min timing
        loop {
            let current_min = self.min_validation_time_micros.load(Ordering::Relaxed);
            if duration_micros >= current_min {
                break;
            }
            if self.min_validation_time_micros.compare_exchange_weak(
                current_min,
                duration_micros,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ).is_ok() {
                break;
            }
        }

        // Update operation-specific metrics
        if let Ok(mut operations) = self.operations.write() {
            let metrics = operations.entry(operation.to_string()).or_insert_with(|| {
                OperationMetrics {
                    total_calls: 0,
                    successful_calls: 0,
                    failed_calls: 0,
                    total_duration_micros: 0,
                    max_duration_micros: 0,
                    min_duration_micros: u64::MAX,
                    average_duration_micros: 0,
                    last_call_time: chrono::Utc::now(),
                    error_breakdown: HashMap::new(),
                }
            });

            metrics.total_calls += 1;
            metrics.total_duration_micros += duration_micros;
            metrics.max_duration_micros = metrics.max_duration_micros.max(duration_micros);
            metrics.min_duration_micros = metrics.min_duration_micros.min(duration_micros);
            metrics.average_duration_micros = metrics.total_duration_micros / metrics.total_calls;
            metrics.last_call_time = chrono::Utc::now();

            if success {
                metrics.successful_calls += 1;
            } else {
                metrics.failed_calls += 1;
                if let Some(error) = error_type {
                    *metrics.error_breakdown.entry(error.to_string()).or_insert(0) += 1;
                    
                    // Update error type counters
                    match error {
                        e if e.contains("checksum") => {
                            self.checksum_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        e if e.contains("structural") => {
                            self.structural_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        e if e.contains("reference") => {
                            self.reference_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        e if e.contains("temporal") => {
                            self.temporal_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        e if e.contains("critical") || e.contains("fatal") => {
                            self.critical_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                }
            }
        }

        // Update path metrics
        if let Ok(mut paths) = self.paths.write() {
            let path_metrics = paths.entry(path.to_string()).or_insert_with(|| {
                PathMetrics {
                    validation_count: 0,
                    error_count: 0,
                    last_error_time: None,
                    error_rate: 0.0,
                    avg_validation_time_micros: 0,
                }
            });

            path_metrics.validation_count += 1;
            if !success {
                path_metrics.error_count += 1;
                path_metrics.last_error_time = Some(chrono::Utc::now());
            }
            path_metrics.error_rate = path_metrics.error_count as f64 / path_metrics.validation_count as f64;
            path_metrics.avg_validation_time_micros = 
                (path_metrics.avg_validation_time_micros * (path_metrics.validation_count - 1) + duration_micros) 
                / path_metrics.validation_count;
        }

        // Add to history
        if let Ok(mut history) = self.history.write() {
            history.add_entry(ValidationEntry {
                timestamp: Instant::now(),
                operation: operation.to_string(),
                path: path.to_string(),
                success,
                duration,
                error_type: error_type.map(|s| s.to_string()),
            });
        }
    }

    /// Record a skipped validation
    pub fn record_skipped(&self, reason: &str) {
        self.skipped_validations.fetch_add(1, Ordering::Relaxed);
        tracing::debug!("Validation skipped: {}", reason);
    }

    /// Record a validation timeout
    pub fn record_timeout(&self, operation: &str, duration: Duration) {
        self.timeout_validations.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            "Validation timeout for operation {} after {:?}",
            operation,
            duration
        );
    }

    /// Get current metrics snapshot
    pub fn get_snapshot(&self) -> MetricsSnapshot {
        let total = self.total_validations.load(Ordering::Relaxed);
        let successful = self.successful_validations.load(Ordering::Relaxed);
        let failed = self.failed_validations.load(Ordering::Relaxed);
        let skipped = self.skipped_validations.load(Ordering::Relaxed);
        let timeouts = self.timeout_validations.load(Ordering::Relaxed);

        let total_time = self.total_validation_time_micros.load(Ordering::Relaxed);
        let max_time = self.max_validation_time_micros.load(Ordering::Relaxed);
        let min_time = self.min_validation_time_micros.load(Ordering::Relaxed);

        let success_rate = if total > 0 { successful as f64 / total as f64 } else { 1.0 };
        let avg_time = if total > 0 { total_time / total } else { 0 };

        let operations = self.operations.read().unwrap().clone();
        let paths = self.paths.read().unwrap().clone();

        MetricsSnapshot {
            timestamp: chrono::Utc::now(),
            total_validations: total,
            successful_validations: successful,
            failed_validations: failed,
            skipped_validations: skipped,
            timeout_validations: timeouts,
            success_rate,
            avg_validation_time_micros: avg_time,
            max_validation_time_micros: max_time,
            min_validation_time_micros: if min_time == u64::MAX { 0 } else { min_time },
            checksum_errors: self.checksum_errors.load(Ordering::Relaxed),
            structural_errors: self.structural_errors.load(Ordering::Relaxed),
            reference_errors: self.reference_errors.load(Ordering::Relaxed),
            temporal_errors: self.temporal_errors.load(Ordering::Relaxed),
            critical_errors: self.critical_errors.load(Ordering::Relaxed),
            operations,
            paths,
        }
    }

    /// Get error rate for a specific path
    pub fn get_path_error_rate(&self, path: &str) -> f64 {
        if let Ok(paths) = self.paths.read() {
            paths.get(path).map_or(0.0, |metrics| metrics.error_rate)
        } else {
            0.0
        }
    }

    /// Check if a path has frequent errors
    pub fn is_problematic_path(&self, path: &str, error_threshold: f64) -> bool {
        self.get_path_error_rate(path) > error_threshold
    }

    /// Get recent validation activity
    pub fn get_recent_activity(&self, since: Duration) -> Vec<ValidationSummary> {
        let cutoff = Instant::now() - since;
        
        if let Ok(history) = self.history.read() {
            history.entries
                .iter()
                .filter(|entry| entry.timestamp > cutoff)
                .map(|entry| ValidationSummary {
                    operation: entry.operation.clone(),
                    path: entry.path.clone(),
                    success: entry.success,
                    duration: entry.duration,
                    error_type: entry.error_type.clone(),
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_validations.store(0, Ordering::Relaxed);
        self.successful_validations.store(0, Ordering::Relaxed);
        self.failed_validations.store(0, Ordering::Relaxed);
        self.skipped_validations.store(0, Ordering::Relaxed);
        self.timeout_validations.store(0, Ordering::Relaxed);
        self.total_validation_time_micros.store(0, Ordering::Relaxed);
        self.max_validation_time_micros.store(0, Ordering::Relaxed);
        self.min_validation_time_micros.store(u64::MAX, Ordering::Relaxed);
        self.checksum_errors.store(0, Ordering::Relaxed);
        self.structural_errors.store(0, Ordering::Relaxed);
        self.reference_errors.store(0, Ordering::Relaxed);
        self.temporal_errors.store(0, Ordering::Relaxed);
        self.critical_errors.store(0, Ordering::Relaxed);

        if let Ok(mut operations) = self.operations.write() {
            operations.clear();
        }

        if let Ok(mut paths) = self.paths.write() {
            paths.clear();
        }

        if let Ok(mut history) = self.history.write() {
            history.clear();
        }
    }
}

impl ValidationHistory {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            max_entries: 10000, // Keep last 10k validations
        }
    }

    fn add_entry(&mut self, entry: ValidationEntry) {
        self.entries.push(entry);
        
        // Keep bounded size
        if self.entries.len() > self.max_entries {
            self.entries.drain(0..self.entries.len() - self.max_entries);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub total_validations: u64,
    pub successful_validations: u64,
    pub failed_validations: u64,
    pub skipped_validations: u64,
    pub timeout_validations: u64,
    pub success_rate: f64,
    pub avg_validation_time_micros: u64,
    pub max_validation_time_micros: u64,
    pub min_validation_time_micros: u64,
    pub checksum_errors: u64,
    pub structural_errors: u64,
    pub reference_errors: u64,
    pub temporal_errors: u64,
    pub critical_errors: u64,
    pub operations: HashMap<String, OperationMetrics>,
    pub paths: HashMap<String, PathMetrics>,
}

#[derive(Debug, Clone)]
pub struct ValidationSummary {
    pub operation: String,
    pub path: String,
    pub success: bool,
    pub duration: Duration,
    pub error_type: Option<String>,
}

impl MetricsSnapshot {
    /// Generate a human-readable report
    pub fn generate_report(&self) -> String {
        let mut report = String::new();
        
        report.push_str("=== Lightning DB Integrity Metrics Report ===\n\n");
        report.push_str(&format!("Timestamp: {}\n", self.timestamp.format("%Y-%m-%d %H:%M:%S UTC")));
        
        report.push_str("\n--- Overall Statistics ---\n");
        report.push_str(&format!("Total Validations: {}\n", self.total_validations));
        report.push_str(&format!("Successful: {} ({:.2}%)\n", 
            self.successful_validations, 
            self.success_rate * 100.0
        ));
        report.push_str(&format!("Failed: {}\n", self.failed_validations));
        report.push_str(&format!("Skipped: {}\n", self.skipped_validations));
        report.push_str(&format!("Timeouts: {}\n", self.timeout_validations));
        
        report.push_str("\n--- Performance Metrics ---\n");
        report.push_str(&format!("Average Validation Time: {:.2} μs\n", self.avg_validation_time_micros));
        report.push_str(&format!("Max Validation Time: {} μs\n", self.max_validation_time_micros));
        report.push_str(&format!("Min Validation Time: {} μs\n", self.min_validation_time_micros));
        
        report.push_str("\n--- Error Breakdown ---\n");
        report.push_str(&format!("Checksum Errors: {}\n", self.checksum_errors));
        report.push_str(&format!("Structural Errors: {}\n", self.structural_errors));
        report.push_str(&format!("Reference Errors: {}\n", self.reference_errors));
        report.push_str(&format!("Temporal Errors: {}\n", self.temporal_errors));
        report.push_str(&format!("Critical Errors: {}\n", self.critical_errors));
        
        if !self.operations.is_empty() {
            report.push_str("\n--- Top Operations (by call count) ---\n");
            let mut ops: Vec<_> = self.operations.iter().collect();
            ops.sort_by(|a, b| b.1.total_calls.cmp(&a.1.total_calls));
            
            for (op_name, metrics) in ops.iter().take(10) {
                report.push_str(&format!(
                    "{}: {} calls, {:.2}% success, {:.2} μs avg\n",
                    op_name,
                    metrics.total_calls,
                    (metrics.successful_calls as f64 / metrics.total_calls as f64) * 100.0,
                    metrics.average_duration_micros
                ));
            }
        }

        if !self.paths.is_empty() {
            report.push_str("\n--- Problematic Paths (error rate > 5%) ---\n");
            let problematic_paths: Vec<_> = self.paths.iter()
                .filter(|(_, metrics)| metrics.error_rate > 0.05)
                .collect();
                
            if problematic_paths.is_empty() {
                report.push_str("No problematic paths detected.\n");
            } else {
                for (path, metrics) in problematic_paths {
                    report.push_str(&format!(
                        "{}: {:.2}% error rate ({}/{} validations)\n",
                        path,
                        metrics.error_rate * 100.0,
                        metrics.error_count,
                        metrics.validation_count
                    ));
                }
            }
        }
        
        report
    }

    /// Check if metrics indicate system health issues
    pub fn has_health_issues(&self) -> bool {
        self.success_rate < 0.95 || // Less than 95% success rate
        self.critical_errors > 0 ||  // Any critical errors
        self.avg_validation_time_micros > 10000 // Average > 10ms
    }

    /// Get recommendations based on current metrics
    pub fn get_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.success_rate < 0.95 {
            recommendations.push("Low validation success rate detected. Consider investigating failed validations.".to_string());
        }

        if self.critical_errors > 0 {
            recommendations.push("Critical integrity errors detected. Immediate investigation required.".to_string());
        }

        if self.avg_validation_time_micros > 10000 {
            recommendations.push("High average validation time. Consider optimizing validation routines or reducing validation frequency.".to_string());
        }

        if self.timeout_validations > self.total_validations / 20 { // > 5%
            recommendations.push("High validation timeout rate. Consider increasing timeout thresholds or optimizing validation performance.".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("System integrity validation appears healthy.".to_string());
        }

        recommendations
    }
}