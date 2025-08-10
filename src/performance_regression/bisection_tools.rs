//! Performance Regression Bisection Tools
//!
//! Provides automated bisection capabilities to help identify the root cause
//! of performance regressions by systematically testing different versions,
//! configurations, or time periods.

use super::{RegressionDetectionResult, RegressionDetectorConfig};
use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// Bisection tools for root cause analysis
pub struct BisectionTools {
    config: RegressionDetectorConfig,
    active_sessions: HashMap<String, BisectionSession>,
}

/// Bisection session for tracking a root cause investigation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BisectionSession {
    pub session_id: String,
    pub created_at: SystemTime,
    pub regression: RegressionDetectionResult,
    pub bisection_type: BisectionType,
    pub current_state: BisectionState,
    pub test_points: Vec<BisectionTestPoint>,
    pub good_point: Option<BisectionTestPoint>,
    pub bad_point: Option<BisectionTestPoint>,
    pub suspects: Vec<RootCauseSuspect>,
    pub recommendations: Vec<BisectionRecommendation>,
    pub completion_status: BisectionStatus,
}

/// Types of bisection analysis
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BisectionType {
    Temporal,      // Bisect by time to find when regression started
    Version,       // Bisect by code version/commit
    Configuration, // Bisect by configuration changes
    DataSet,       // Bisect by data characteristics
    Load,          // Bisect by load levels
    Environment,   // Bisect by environment factors
}

/// Current state of bisection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BisectionState {
    pub current_iteration: u32,
    pub total_iterations: u32,
    pub narrowed_range: TimeRange,
    pub confidence_level: f64,
    pub next_test_point: Option<BisectionTestPoint>,
    pub remaining_suspects: usize,
}

/// Time range for bisection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: SystemTime,
    pub end: SystemTime,
    pub duration_hours: f64,
}

/// Single test point in bisection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BisectionTestPoint {
    pub timestamp: SystemTime,
    pub test_id: String,
    pub test_result: Option<BisectionTestResult>,
    pub metadata: HashMap<String, String>,
    pub performance_metrics: Option<TestPerformanceMetrics>,
}

/// Result of a bisection test
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BisectionTestResult {
    Good,         // Performance is acceptable
    Bad,          // Performance shows regression
    Inconclusive, // Results are unclear
    Failed,       // Test could not be completed
}

/// Performance metrics from a bisection test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestPerformanceMetrics {
    pub duration_micros: u64,
    pub throughput_ops_per_sec: f64,
    pub memory_usage_bytes: u64,
    pub cpu_usage_percent: f64,
    pub error_rate: f64,
    pub additional_metrics: HashMap<String, f64>,
}

/// Root cause suspect
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootCauseSuspect {
    pub suspect_type: SuspectType,
    pub description: String,
    pub probability: f64,
    pub evidence: Vec<String>,
    pub time_range: Option<TimeRange>,
    pub related_changes: Vec<String>,
}

/// Types of root cause suspects
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SuspectType {
    CodeChange,          // Code modification
    ConfigurationChange, // Configuration update
    DataChange,          // Data structure or content change
    EnvironmentChange,   // Infrastructure or environment change
    LoadChange,          // Traffic or load pattern change
    DependencyChange,    // External dependency change
    HardwareChange,      // Hardware or resource change
}

/// Bisection recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BisectionRecommendation {
    pub recommendation_type: RecommendationType,
    pub title: String,
    pub description: String,
    pub priority: Priority,
    pub estimated_effort: EffortLevel,
    pub expected_impact: Impact,
}

/// Types of recommendations
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RecommendationType {
    Investigation, // Further investigation needed
    Rollback,      // Rollback a change
    Configuration, // Adjust configuration
    Optimization,  // Apply optimization
    Monitoring,    // Add monitoring
    Testing,       // Add testing
}

/// Priority levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low,
    Medium,
    High,
    Critical,
}

/// Effort levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EffortLevel {
    Minimal,  // < 1 hour
    Low,      // 1-4 hours
    Medium,   // 4-16 hours
    High,     // 1-3 days
    VeryHigh, // > 3 days
}

/// Impact levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Impact {
    Low,
    Medium,
    High,
    Critical,
}

/// Bisection completion status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BisectionStatus {
    InProgress,
    Completed,
    Failed,
    Cancelled,
    Inconclusive,
}

impl BisectionTools {
    /// Create new bisection tools
    pub fn new(config: RegressionDetectorConfig) -> Self {
        Self {
            config,
            active_sessions: HashMap::new(),
        }
    }

    /// Start a new bisection session
    pub fn start_bisection(
        &mut self,
        regression: &RegressionDetectionResult,
    ) -> Result<BisectionSession> {
        let session_id = self.generate_session_id(regression);
        let bisection_type = self.determine_bisection_type(regression);

        let mut session = BisectionSession {
            session_id: session_id.clone(),
            created_at: SystemTime::now(),
            regression: regression.clone(),
            bisection_type,
            current_state: BisectionState {
                current_iteration: 0,
                total_iterations: self.estimate_total_iterations(bisection_type),
                narrowed_range: self.create_initial_range(regression),
                confidence_level: 0.0,
                next_test_point: None,
                remaining_suspects: 0,
            },
            test_points: Vec::new(),
            good_point: None,
            bad_point: None,
            suspects: Vec::new(),
            recommendations: Vec::new(),
            completion_status: BisectionStatus::InProgress,
        };

        // Initialize the bisection
        self.initialize_bisection(&mut session)?;

        self.active_sessions.insert(session_id, session.clone());
        Ok(session)
    }

    /// Get bisection session by ID
    pub fn get_session(&self, session_id: &str) -> Option<&BisectionSession> {
        self.active_sessions.get(session_id)
    }

    /// Continue bisection with test result
    pub fn continue_bisection(
        &mut self,
        session_id: &str,
        test_point_id: &str,
        result: BisectionTestResult,
        metrics: Option<TestPerformanceMetrics>,
    ) -> Result<BisectionSession> {
        let session = self.active_sessions.get_mut(session_id).ok_or_else(|| {
            crate::Error::Generic(format!("Bisection session not found: {}", session_id))
        })?;

        // Update test point with result
        if let Some(test_point) = session
            .test_points
            .iter_mut()
            .find(|tp| tp.test_id == test_point_id)
        {
            test_point.test_result = Some(result);
            test_point.performance_metrics = metrics;
        }

        // Process the result
        Self::process_test_result_static(session, test_point_id, result)?;

        // Check if bisection is complete
        if Self::is_bisection_complete_static(session) {
            Self::complete_bisection_static(session)?;
        } else {
            // Plan next test
            Self::plan_next_test_static(session)?;
        }

        Ok(session.clone())
    }

    /// Generate unique session ID
    fn generate_session_id(&self, regression: &RegressionDetectionResult) -> String {
        format!(
            "bisection_{}_{}",
            regression.operation_type,
            regression
                .detection_timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        )
    }

    /// Determine appropriate bisection type
    fn determine_bisection_type(&self, regression: &RegressionDetectionResult) -> BisectionType {
        // Simple heuristic - in practice this would be more sophisticated
        if regression.current_performance.trace_id.is_some() {
            BisectionType::Temporal
        } else if regression.degradation_percentage > 0.5 {
            BisectionType::Version
        } else {
            BisectionType::Configuration
        }
    }

    /// Estimate total iterations needed
    fn estimate_total_iterations(&self, bisection_type: BisectionType) -> u32 {
        match bisection_type {
            BisectionType::Temporal => 8, // log2(256) = 8 for ~10 days with hour precision
            BisectionType::Version => 10, // Assuming ~1000 commits to bisect
            BisectionType::Configuration => 6, // Fewer config options
            BisectionType::DataSet => 7,
            BisectionType::Load => 5,
            BisectionType::Environment => 4,
        }
    }

    /// Create initial time range for bisection
    fn create_initial_range(&self, regression: &RegressionDetectionResult) -> TimeRange {
        // Default to 7 days before regression detection
        let end = regression.detection_timestamp;
        let start = end - Duration::from_secs(7 * 24 * 3600);

        TimeRange {
            start,
            end,
            duration_hours: 7.0 * 24.0,
        }
    }

    /// Initialize bisection session
    fn initialize_bisection(&self, session: &mut BisectionSession) -> Result<()> {
        // Set initial good and bad points
        session.bad_point = Some(BisectionTestPoint {
            timestamp: session.regression.detection_timestamp,
            test_id: "initial_bad".to_string(),
            test_result: Some(BisectionTestResult::Bad),
            metadata: HashMap::new(),
            performance_metrics: Some(TestPerformanceMetrics {
                duration_micros: session.regression.current_performance.duration_micros,
                throughput_ops_per_sec: session
                    .regression
                    .current_performance
                    .throughput_ops_per_sec,
                memory_usage_bytes: session.regression.current_performance.memory_usage_bytes,
                cpu_usage_percent: session.regression.current_performance.cpu_usage_percent,
                error_rate: session.regression.current_performance.error_rate,
                additional_metrics: session
                    .regression
                    .current_performance
                    .additional_metrics
                    .clone(),
            }),
        });

        // Create initial suspects
        session.suspects = self.generate_initial_suspects(&session.regression);
        session.current_state.remaining_suspects = session.suspects.len();

        // Plan first test
        Self::plan_next_test_static(session)?;

        Ok(())
    }

    /// Generate initial root cause suspects
    fn generate_initial_suspects(
        &self,
        regression: &RegressionDetectionResult,
    ) -> Vec<RootCauseSuspect> {
        let mut suspects = Vec::new();

        // Code change suspect
        suspects.push(RootCauseSuspect {
            suspect_type: SuspectType::CodeChange,
            description: "Recent code changes may have introduced performance regression"
                .to_string(),
            probability: 0.7,
            evidence: vec![
                format!(
                    "Performance degraded by {:.1}%",
                    regression.degradation_percentage * 100.0
                ),
                "Operation type affected: ".to_string() + &regression.operation_type,
            ],
            time_range: Some(TimeRange {
                start: regression.detection_timestamp - Duration::from_secs(24 * 3600),
                end: regression.detection_timestamp,
                duration_hours: 24.0,
            }),
            related_changes: Vec::new(),
        });

        // Configuration change suspect
        suspects.push(RootCauseSuspect {
            suspect_type: SuspectType::ConfigurationChange,
            description: "Configuration changes may have impacted performance".to_string(),
            probability: 0.5,
            evidence: vec!["Performance change pattern suggests configuration impact".to_string()],
            time_range: Some(TimeRange {
                start: regression.detection_timestamp - Duration::from_secs(72 * 3600),
                end: regression.detection_timestamp,
                duration_hours: 72.0,
            }),
            related_changes: Vec::new(),
        });

        // Environment change suspect
        if regression.current_performance.memory_usage_bytes
            > regression.baseline_performance.memory_baseline * 2
        {
            suspects.push(RootCauseSuspect {
                suspect_type: SuspectType::EnvironmentChange,
                description: "Environment or resource changes may be causing performance issues"
                    .to_string(),
                probability: 0.6,
                evidence: vec![format!(
                    "Memory usage increased from {} MB to {} MB",
                    regression.baseline_performance.memory_baseline / (1024 * 1024),
                    regression.current_performance.memory_usage_bytes / (1024 * 1024)
                )],
                time_range: Some(TimeRange {
                    start: regression.detection_timestamp - Duration::from_secs(48 * 3600),
                    end: regression.detection_timestamp,
                    duration_hours: 48.0,
                }),
                related_changes: Vec::new(),
            });
        }

        suspects
    }

    /// Plan the next test point (static version)
    fn plan_next_test_static(session: &mut BisectionSession) -> Result<()> {
        if session.completion_status != BisectionStatus::InProgress {
            return Ok(());
        }

        let next_timestamp = match session.bisection_type {
            BisectionType::Temporal => Self::plan_temporal_bisection_static(session)?,
            BisectionType::Version => Self::plan_version_bisection_static(session)?,
            BisectionType::Configuration => Self::plan_configuration_bisection_static(session)?,
            _ => Self::plan_generic_bisection_static(session)?,
        };

        let test_point = BisectionTestPoint {
            timestamp: next_timestamp,
            test_id: format!("test_{}", session.current_state.current_iteration + 1),
            test_result: None,
            metadata: Self::create_test_metadata_static(session, next_timestamp),
            performance_metrics: None,
        };

        session.current_state.next_test_point = Some(test_point.clone());
        session.test_points.push(test_point);
        session.current_state.current_iteration += 1;

        Ok(())
    }

    /// Plan temporal bisection (binary search by time)
    fn plan_temporal_bisection_static(session: &BisectionSession) -> Result<SystemTime> {
        let range = &session.current_state.narrowed_range;

        // Binary search midpoint
        let range_duration = range.end.duration_since(range.start).unwrap_or_default();
        let midpoint = range.start + range_duration / 2;

        Ok(midpoint)
    }

    /// Plan version bisection
    fn plan_version_bisection_static(session: &BisectionSession) -> Result<SystemTime> {
        // For version bisection, we'd typically work with commit hashes
        // For simplicity, we'll use temporal approach as a proxy
        Self::plan_temporal_bisection_static(session)
    }

    /// Plan configuration bisection
    fn plan_configuration_bisection_static(session: &BisectionSession) -> Result<SystemTime> {
        // Configuration bisection would test different config combinations
        // For now, use temporal approach
        Self::plan_temporal_bisection_static(session)
    }

    /// Plan generic bisection
    fn plan_generic_bisection_static(session: &BisectionSession) -> Result<SystemTime> {
        Self::plan_temporal_bisection_static(session)
    }

    /// Create test metadata (static version)
    fn create_test_metadata_static(
        session: &BisectionSession,
        timestamp: SystemTime,
    ) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "bisection_type".to_string(),
            format!("{:?}", session.bisection_type),
        );
        metadata.insert(
            "iteration".to_string(),
            session.current_state.current_iteration.to_string(),
        );
        metadata.insert(
            "operation_type".to_string(),
            session.regression.operation_type.clone(),
        );
        metadata.insert("test_timestamp".to_string(), format!("{:?}", timestamp));
        metadata
    }

    /// Process test result and update bisection state (static version)
    fn process_test_result_static(
        session: &mut BisectionSession,
        test_point_id: &str,
        result: BisectionTestResult,
    ) -> Result<()> {
        let test_point = session
            .test_points
            .iter()
            .find(|tp| tp.test_id == test_point_id)
            .ok_or_else(|| crate::Error::Generic("Test point not found".to_string()))?
            .clone();

        match result {
            BisectionTestResult::Good => {
                session.good_point = Some(test_point.clone());
                // Narrow range - the issue is after this point
                session.current_state.narrowed_range.start = test_point.timestamp;
            }
            BisectionTestResult::Bad => {
                // The issue is before or at this point
                session.current_state.narrowed_range.end = test_point.timestamp;
            }
            BisectionTestResult::Inconclusive | BisectionTestResult::Failed => {
                // Don't update range, but note the inconclusive result
                // We might need a different approach
            }
        }

        // Update confidence based on range narrowing
        let initial_duration = session
            .current_state
            .narrowed_range
            .end
            .duration_since(session.current_state.narrowed_range.start)
            .unwrap_or_default()
            .as_secs_f64()
            / 3600.0;

        let original_duration = 7.0 * 24.0; // 7 days in hours
        session.current_state.confidence_level = 1.0 - (initial_duration / original_duration);

        // Update duration
        session.current_state.narrowed_range.duration_hours = initial_duration;

        Ok(())
    }

    /// Check if bisection is complete (static version)
    fn is_bisection_complete_static(session: &BisectionSession) -> bool {
        // Complete if we've narrowed down to a small enough range
        let min_range_hours = match session.bisection_type {
            BisectionType::Temporal => 1.0,      // 1 hour
            BisectionType::Version => 0.5,       // 30 minutes
            BisectionType::Configuration => 2.0, // 2 hours
            _ => 1.0,
        };

        session.current_state.narrowed_range.duration_hours <= min_range_hours
            || session.current_state.current_iteration >= session.current_state.total_iterations
            || session.current_state.confidence_level >= 0.95
    }

    /// Complete bisection and generate final recommendations (static version)
    fn complete_bisection_static(session: &mut BisectionSession) -> Result<()> {
        session.completion_status = BisectionStatus::Completed;

        // Generate final recommendations
        session.recommendations = Self::generate_final_recommendations_static(session);

        // Update suspects based on findings
        Self::update_suspect_probabilities_static(session);

        Ok(())
    }

    /// Generate final recommendations (static version)
    fn generate_final_recommendations_static(
        session: &BisectionSession,
    ) -> Vec<BisectionRecommendation> {
        let mut recommendations = Vec::new();

        // Time-based recommendation
        if session.current_state.confidence_level > 0.8 {
            let range = &session.current_state.narrowed_range;
            recommendations.push(BisectionRecommendation {
                recommendation_type: RecommendationType::Investigation,
                title: "Investigate changes in identified time window".to_string(),
                description: format!(
                    "Performance regression likely occurred between {:?} and {:?} ({:.1} hours window)",
                    range.start, range.end, range.duration_hours
                ),
                priority: Priority::High,
                estimated_effort: EffortLevel::Medium,
                expected_impact: Impact::High,
            });
        }

        // Rollback recommendation for high-confidence code changes
        if let Some(suspect) = session
            .suspects
            .iter()
            .find(|s| matches!(s.suspect_type, SuspectType::CodeChange) && s.probability > 0.8)
        {
            recommendations.push(BisectionRecommendation {
                recommendation_type: RecommendationType::Rollback,
                title: "Consider rolling back recent code changes".to_string(),
                description: format!(
                    "High probability ({:.0}%) that code changes caused regression: {}",
                    suspect.probability * 100.0,
                    suspect.description
                ),
                priority: Priority::Critical,
                estimated_effort: EffortLevel::Low,
                expected_impact: Impact::Critical,
            });
        }

        // Monitoring recommendation
        recommendations.push(BisectionRecommendation {
            recommendation_type: RecommendationType::Monitoring,
            title: "Add enhanced monitoring for this operation".to_string(),
            description: format!(
                "Add detailed monitoring for '{}' operation to catch future regressions earlier",
                session.regression.operation_type
            ),
            priority: Priority::Medium,
            estimated_effort: EffortLevel::Low,
            expected_impact: Impact::Medium,
        });

        // Testing recommendation
        recommendations.push(BisectionRecommendation {
            recommendation_type: RecommendationType::Testing,
            title: "Add performance tests".to_string(),
            description: "Add automated performance tests to prevent similar regressions"
                .to_string(),
            priority: Priority::Medium,
            estimated_effort: EffortLevel::High,
            expected_impact: Impact::High,
        });

        recommendations
    }

    /// Update suspect probabilities based on bisection results (static version)
    fn update_suspect_probabilities_static(session: &mut BisectionSession) {
        let narrowed_range = &session.current_state.narrowed_range;

        for suspect in &mut session.suspects {
            if let Some(suspect_range) = &suspect.time_range {
                // Check if suspect's time range overlaps with narrowed range
                let overlap = Self::calculate_time_overlap_static(suspect_range, narrowed_range);

                if overlap > 0.8 {
                    suspect.probability = (suspect.probability * 1.5).min(1.0);
                } else if overlap > 0.3 {
                    suspect.probability = suspect.probability * 1.1;
                } else {
                    suspect.probability = suspect.probability * 0.5;
                }
            }
        }

        // Sort suspects by probability
        session
            .suspects
            .sort_by(|a, b| b.probability.partial_cmp(&a.probability).unwrap());
    }

    /// Calculate time overlap between two ranges (static version)
    fn calculate_time_overlap_static(range1: &TimeRange, range2: &TimeRange) -> f64 {
        let overlap_start = range1.start.max(range2.start);
        let overlap_end = range1.end.min(range2.end);

        if overlap_start >= overlap_end {
            return 0.0;
        }

        let overlap_duration = overlap_end
            .duration_since(overlap_start)
            .unwrap_or_default()
            .as_secs_f64();
        let total_duration = range1
            .end
            .duration_since(range1.start)
            .unwrap_or_default()
            .as_secs_f64();

        if total_duration > 0.0 {
            overlap_duration / total_duration
        } else {
            0.0
        }
    }

    /// Get all active bisection sessions
    pub fn get_active_sessions(&self) -> Vec<&BisectionSession> {
        self.active_sessions
            .values()
            .filter(|s| s.completion_status == BisectionStatus::InProgress)
            .collect()
    }

    /// Cancel a bisection session
    pub fn cancel_session(&mut self, session_id: &str) -> Result<()> {
        if let Some(session) = self.active_sessions.get_mut(session_id) {
            session.completion_status = BisectionStatus::Cancelled;
            Ok(())
        } else {
            Err(crate::Error::Generic(format!(
                "Session not found: {}",
                session_id
            )))
        }
    }

    /// Calculate time overlap between two ranges (public for testing)
    pub fn calculate_time_overlap(&self, range1: &TimeRange, range2: &TimeRange) -> f64 {
        Self::calculate_time_overlap_static(range1, range2)
    }

    /// Get bisection statistics
    pub fn get_bisection_statistics(&self) -> BisectionStatistics {
        let total_sessions = self.active_sessions.len();
        let completed_sessions = self
            .active_sessions
            .values()
            .filter(|s| s.completion_status == BisectionStatus::Completed)
            .count();
        let in_progress_sessions = self
            .active_sessions
            .values()
            .filter(|s| s.completion_status == BisectionStatus::InProgress)
            .count();

        let avg_iterations = if completed_sessions > 0 {
            self.active_sessions
                .values()
                .filter(|s| s.completion_status == BisectionStatus::Completed)
                .map(|s| s.current_state.current_iteration)
                .sum::<u32>() as f64
                / completed_sessions as f64
        } else {
            0.0
        };

        BisectionStatistics {
            total_sessions,
            completed_sessions,
            in_progress_sessions,
            avg_iterations,
            success_rate: if total_sessions > 0 {
                completed_sessions as f64 / total_sessions as f64
            } else {
                0.0
            },
        }
    }
}

/// Statistics for bisection sessions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BisectionStatistics {
    pub total_sessions: usize,
    pub completed_sessions: usize,
    pub in_progress_sessions: usize,
    pub avg_iterations: f64,
    pub success_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::super::{PerformanceBaseline, PerformanceMetric, RegressionSeverity};
    use super::*;

    fn create_test_regression() -> RegressionDetectionResult {
        let current_performance = PerformanceMetric {
            timestamp: SystemTime::now(),
            operation_type: "test_op".to_string(),
            duration_micros: 1500,
            throughput_ops_per_sec: 80.0,
            memory_usage_bytes: 2048,
            cpu_usage_percent: 7.5,
            error_rate: 0.0,
            additional_metrics: HashMap::new(),
            trace_id: Some("test-trace-123".to_string()),
            span_id: Some("test-span-456".to_string()),
        };

        let baseline_performance = PerformanceBaseline {
            operation_type: "test_op".to_string(),
            created_at: SystemTime::now() - Duration::from_secs(3600),
            sample_count: 100,
            mean_duration_micros: 1000.0,
            std_deviation_micros: 100.0,
            p50_duration_micros: 1000,
            p95_duration_micros: 1200,
            p99_duration_micros: 1300,
            mean_throughput: 100.0,
            memory_baseline: 1024,
            cpu_baseline: 5.0,
            confidence_interval: (950.0, 1050.0),
        };

        RegressionDetectionResult {
            detected: true,
            severity: RegressionSeverity::Major,
            operation_type: "test_op".to_string(),
            current_performance,
            baseline_performance,
            degradation_percentage: 0.5,
            statistical_confidence: 0.95,
            recommended_actions: vec!["Investigate recent changes".to_string()],
            detection_timestamp: SystemTime::now(),
        }
    }

    #[test]
    fn test_bisection_tools_creation() {
        let config = RegressionDetectorConfig::default();
        let _tools = BisectionTools::new(config);
    }

    #[test]
    fn test_start_bisection() {
        let config = RegressionDetectorConfig::default();
        let mut tools = BisectionTools::new(config);

        let regression = create_test_regression();
        let session = tools.start_bisection(&regression).unwrap();

        assert_eq!(session.regression.operation_type, "test_op");
        assert_eq!(session.completion_status, BisectionStatus::InProgress);
        assert!(!session.suspects.is_empty());
        assert!(session.current_state.next_test_point.is_some());
    }

    #[test]
    fn test_continue_bisection() {
        let config = RegressionDetectorConfig::default();
        let mut tools = BisectionTools::new(config);

        let regression = create_test_regression();
        let session = tools.start_bisection(&regression).unwrap();
        let session_id = session.session_id.clone();
        let test_id = session
            .current_state
            .next_test_point
            .as_ref()
            .unwrap()
            .test_id
            .clone();

        let metrics = TestPerformanceMetrics {
            duration_micros: 1200,
            throughput_ops_per_sec: 90.0,
            memory_usage_bytes: 1500,
            cpu_usage_percent: 6.0,
            error_rate: 0.0,
            additional_metrics: HashMap::new(),
        };

        let updated_session = tools
            .continue_bisection(
                &session_id,
                &test_id,
                BisectionTestResult::Good,
                Some(metrics),
            )
            .unwrap();

        assert!(updated_session.good_point.is_some());
        assert_eq!(updated_session.current_state.current_iteration, 1);
    }

    #[test]
    fn test_bisection_type_determination() {
        let config = RegressionDetectorConfig::default();
        let tools = BisectionTools::new(config);

        let regression = create_test_regression();
        let bisection_type = tools.determine_bisection_type(&regression);

        assert_eq!(bisection_type, BisectionType::Temporal);
    }

    #[test]
    fn test_suspect_generation() {
        let config = RegressionDetectorConfig::default();
        let tools = BisectionTools::new(config);

        let regression = create_test_regression();
        let suspects = tools.generate_initial_suspects(&regression);

        assert!(!suspects.is_empty());
        assert!(suspects
            .iter()
            .any(|s| matches!(s.suspect_type, SuspectType::CodeChange)));
    }

    #[test]
    fn test_time_overlap_calculation() {
        let config = RegressionDetectorConfig::default();
        let tools = BisectionTools::new(config);

        let now = SystemTime::now();
        let range1 = TimeRange {
            start: now,
            end: now + Duration::from_secs(3600),
            duration_hours: 1.0,
        };

        let range2 = TimeRange {
            start: now + Duration::from_secs(1800), // 30 min later
            end: now + Duration::from_secs(5400),   // 90 min later
            duration_hours: 1.0,
        };

        let overlap = tools.calculate_time_overlap(&range1, &range2);
        assert!(overlap > 0.0 && overlap < 1.0);
    }

    #[test]
    fn test_session_cancellation() {
        let config = RegressionDetectorConfig::default();
        let mut tools = BisectionTools::new(config);

        let regression = create_test_regression();
        let session = tools.start_bisection(&regression).unwrap();
        let session_id = session.session_id.clone();

        assert!(tools.cancel_session(&session_id).is_ok());

        let cancelled_session = tools.get_session(&session_id).unwrap();
        assert_eq!(
            cancelled_session.completion_status,
            BisectionStatus::Cancelled
        );
    }

    #[test]
    fn test_bisection_statistics() {
        let config = RegressionDetectorConfig::default();
        let mut tools = BisectionTools::new(config);

        let regression = create_test_regression();
        let _session = tools.start_bisection(&regression).unwrap();

        let stats = tools.get_bisection_statistics();
        assert_eq!(stats.total_sessions, 1);
        assert_eq!(stats.in_progress_sessions, 1);
        assert_eq!(stats.completed_sessions, 0);
    }
}
