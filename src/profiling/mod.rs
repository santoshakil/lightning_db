//! Performance Profiling Framework
//!
//! This module provides comprehensive performance profiling capabilities for Lightning DB,
//! including CPU profiling, memory tracking, I/O monitoring, and flamegraph generation.
//! Designed for production use with minimal overhead.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use tracing::{info, warn, error, instrument};

pub mod cpu_profiler;
pub mod memory_profiler;
pub mod io_profiler;
pub mod flamegraph;
pub mod metrics_collector;
pub mod regression_detector;

/// Central profiling coordinator
pub struct ProfilerCoordinator {
    config: ProfilingConfig,
    cpu_profiler: Option<Arc<cpu_profiler::CpuProfiler>>,
    memory_profiler: Option<Arc<memory_profiler::MemoryProfiler>>,
    io_profiler: Option<Arc<io_profiler::IoProfiler>>,
    metrics_collector: Arc<metrics_collector::MetricsCollector>,
    regression_detector: Arc<regression_detector::RegressionDetector>,
    output_dir: PathBuf,
    active: Arc<std::sync::atomic::AtomicBool>,
    session_id: String,
}

/// Profiling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfilingConfig {
    /// Enable CPU profiling
    pub enable_cpu_profiling: bool,
    /// CPU sampling frequency (samples per second)
    pub cpu_sample_rate: u32,
    /// Enable memory profiling
    pub enable_memory_profiling: bool,
    /// Memory sampling interval
    pub memory_sample_interval: Duration,
    /// Enable I/O profiling
    pub enable_io_profiling: bool,
    /// Maximum profile duration
    pub max_profile_duration: Duration,
    /// Output directory for profiles
    pub output_directory: PathBuf,
    /// Enable flamegraph generation
    pub enable_flamegraphs: bool,
    /// Enable regression detection
    pub enable_regression_detection: bool,
    /// Profile data retention period
    pub retention_period: Duration,
    /// Maximum memory usage for profiling (bytes)
    pub max_memory_usage: usize,
    /// Profiling overhead limit (percentage of CPU)
    pub overhead_limit: f64,
}

impl Default for ProfilingConfig {
    fn default() -> Self {
        Self {
            enable_cpu_profiling: true,
            cpu_sample_rate: 99, // 99 Hz to avoid alignment with system timers
            enable_memory_profiling: true,
            memory_sample_interval: Duration::from_millis(100),
            enable_io_profiling: true,
            max_profile_duration: Duration::from_minutes(60),
            output_directory: PathBuf::from("./profiles"),
            enable_flamegraphs: true,
            enable_regression_detection: true,
            retention_period: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            max_memory_usage: 100 * 1024 * 1024, // 100MB
            overhead_limit: 1.0, // 1% CPU overhead
        }
    }
}

/// Profile session information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileSession {
    pub session_id: String,
    pub start_time: SystemTime,
    pub end_time: Option<SystemTime>,
    pub duration: Option<Duration>,
    pub config: ProfilingConfig,
    pub cpu_profile_path: Option<PathBuf>,
    pub memory_profile_path: Option<PathBuf>,
    pub io_profile_path: Option<PathBuf>,
    pub flamegraph_path: Option<PathBuf>,
    pub metrics_summary: MetricsSummary,
}

/// Summary of collected metrics during profiling
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricsSummary {
    pub total_samples: u64,
    pub cpu_samples: u64,
    pub memory_samples: u64,
    pub io_operations: u64,
    pub peak_memory_usage: u64,
    pub total_bytes_read: u64,
    pub total_bytes_written: u64,
    pub hot_functions: Vec<HotFunction>,
    pub performance_score: f64,
}

/// Information about performance-critical functions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotFunction {
    pub function_name: String,
    pub module_name: String,
    pub cpu_percentage: f64,
    pub sample_count: u64,
    pub exclusive_time: Duration,
    pub inclusive_time: Duration,
}

impl ProfilerCoordinator {
    /// Create a new profiler coordinator
    pub fn new(config: ProfilingConfig) -> Result<Self, ProfilingError> {
        // Create output directory
        std::fs::create_dir_all(&config.output_directory)
            .map_err(|e| ProfilingError::IoError(format!("Failed to create output directory: {}", e)))?;

        let session_id = generate_session_id();
        let output_dir = config.output_directory.join(&session_id);
        std::fs::create_dir_all(&output_dir)
            .map_err(|e| ProfilingError::IoError(format!("Failed to create session directory: {}", e)))?;

        let metrics_collector = Arc::new(metrics_collector::MetricsCollector::new());
        let regression_detector = Arc::new(regression_detector::RegressionDetector::new(
            config.output_directory.clone()
        ));

        Ok(Self {
            cpu_profiler: if config.enable_cpu_profiling {
                Some(Arc::new(cpu_profiler::CpuProfiler::new(config.cpu_sample_rate)?))
            } else {
                None
            },
            memory_profiler: if config.enable_memory_profiling {
                Some(Arc::new(memory_profiler::MemoryProfiler::new(config.memory_sample_interval)?))
            } else {
                None
            },
            io_profiler: if config.enable_io_profiling {
                Some(Arc::new(io_profiler::IoProfiler::new()?))
            } else {
                None
            },
            metrics_collector,
            regression_detector,
            config,
            output_dir,
            active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            session_id,
        })
    }

    /// Start profiling session
    #[instrument(skip(self))]
    pub fn start_profiling(&self) -> Result<String, ProfilingError> {
        if self.active.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(ProfilingError::AlreadyActive);
        }

        info!("Starting profiling session: {}", self.session_id);
        self.active.store(true, std::sync::atomic::Ordering::Relaxed);

        // Start CPU profiler
        if let Some(ref cpu_profiler) = self.cpu_profiler {
            cpu_profiler.start()?;
        }

        // Start memory profiler
        if let Some(ref memory_profiler) = self.memory_profiler {
            memory_profiler.start()?;
        }

        // Start I/O profiler
        if let Some(ref io_profiler) = self.io_profiler {
            io_profiler.start()?;
        }

        // Start metrics collection
        self.metrics_collector.start();

        Ok(self.session_id.clone())
    }

    /// Stop profiling session and generate reports
    #[instrument(skip(self))]
    pub fn stop_profiling(&self) -> Result<ProfileSession, ProfilingError> {
        if !self.active.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(ProfilingError::NotActive);
        }

        info!("Stopping profiling session: {}", self.session_id);
        let start_time = SystemTime::now();
        
        // Stop all profilers
        if let Some(ref cpu_profiler) = self.cpu_profiler {
            cpu_profiler.stop()?;
        }

        if let Some(ref memory_profiler) = self.memory_profiler {
            memory_profiler.stop()?;
        }

        if let Some(ref io_profiler) = self.io_profiler {
            io_profiler.stop()?;
        }

        self.metrics_collector.stop();
        self.active.store(false, std::sync::atomic::Ordering::Relaxed);

        // Generate reports
        let mut session = ProfileSession {
            session_id: self.session_id.clone(),
            start_time,
            end_time: Some(SystemTime::now()),
            duration: Some(start_time.elapsed().unwrap_or_default()),
            config: self.config.clone(),
            cpu_profile_path: None,
            memory_profile_path: None,
            io_profile_path: None,
            flamegraph_path: None,
            metrics_summary: MetricsSummary::default(),
        };

        // Export CPU profile
        if let Some(ref cpu_profiler) = self.cpu_profiler {
            let cpu_path = self.output_dir.join("cpu_profile.pb");
            cpu_profiler.export_profile(&cpu_path)?;
            session.cpu_profile_path = Some(cpu_path);
        }

        // Export memory profile
        if let Some(ref memory_profiler) = self.memory_profiler {
            let memory_path = self.output_dir.join("memory_profile.json");
            memory_profiler.export_profile(&memory_path)?;
            session.memory_profile_path = Some(memory_path);
        }

        // Export I/O profile
        if let Some(ref io_profiler) = self.io_profiler {
            let io_path = self.output_dir.join("io_profile.json");
            io_profiler.export_profile(&io_path)?;
            session.io_profile_path = Some(io_path);
        }

        // Generate flamegraph
        if self.config.enable_flamegraphs {
            if let Some(ref cpu_path) = session.cpu_profile_path {
                let flamegraph_path = self.output_dir.join("flamegraph.svg");
                flamegraph::generate_flamegraph(cpu_path, &flamegraph_path)?;
                session.flamegraph_path = Some(flamegraph_path);
            }
        }

        // Collect metrics summary
        session.metrics_summary = self.metrics_collector.get_summary();

        // Save session metadata
        let session_path = self.output_dir.join("session.json");
        let session_json = serde_json::to_string_pretty(&session)
            .map_err(|e| ProfilingError::SerializationError(e.to_string()))?;
        std::fs::write(&session_path, session_json)
            .map_err(|e| ProfilingError::IoError(format!("Failed to save session: {}", e)))?;

        // Run regression detection
        if self.config.enable_regression_detection {
            match self.regression_detector.analyze_session(&session) {
                Ok(regressions) => {
                    if !regressions.is_empty() {
                        warn!("Performance regressions detected: {:?}", regressions);
                    }
                }
                Err(e) => {
                    error!("Regression analysis failed: {}", e);
                }
            }
        }

        // Cleanup old profiles
        self.cleanup_old_profiles()?;

        info!("Profiling session completed: {}", self.session_id);
        Ok(session)
    }

    /// Get current profiling status
    pub fn get_status(&self) -> ProfilingStatus {
        ProfilingStatus {
            active: self.active.load(std::sync::atomic::Ordering::Relaxed),
            session_id: if self.active.load(std::sync::atomic::Ordering::Relaxed) {
                Some(self.session_id.clone())
            } else {
                None
            },
            config: self.config.clone(),
            current_metrics: if self.active.load(std::sync::atomic::Ordering::Relaxed) {
                Some(self.metrics_collector.get_current_metrics())
            } else {
                None
            },
        }
    }

    /// List all profile sessions
    pub fn list_sessions(&self) -> Result<Vec<ProfileSession>, ProfilingError> {
        let mut sessions = Vec::new();

        for entry in std::fs::read_dir(&self.config.output_directory)
            .map_err(|e| ProfilingError::IoError(format!("Failed to read profiles directory: {}", e)))?
        {
            let entry = entry.map_err(|e| ProfilingError::IoError(e.to_string()))?;
            let path = entry.path();

            if path.is_dir() {
                let session_file = path.join("session.json");
                if session_file.exists() {
                    match std::fs::read_to_string(&session_file) {
                        Ok(content) => {
                            match serde_json::from_str::<ProfileSession>(&content) {
                                Ok(session) => sessions.push(session),
                                Err(e) => {
                                    warn!("Failed to parse session file {}: {}", session_file.display(), e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to read session file {}: {}", session_file.display(), e);
                        }
                    }
                }
            }
        }

        // Sort by start time, newest first
        sessions.sort_by(|a, b| b.start_time.cmp(&a.start_time));
        Ok(sessions)
    }

    /// Get detailed analysis for a specific session
    pub fn analyze_session(&self, session_id: &str) -> Result<SessionAnalysis, ProfilingError> {
        let session_dir = self.config.output_directory.join(session_id);
        let session_file = session_dir.join("session.json");

        if !session_file.exists() {
            return Err(ProfilingError::SessionNotFound(session_id.to_string()));
        }

        let session_content = std::fs::read_to_string(&session_file)
            .map_err(|e| ProfilingError::IoError(format!("Failed to read session: {}", e)))?;
        let session: ProfileSession = serde_json::from_str(&session_content)
            .map_err(|e| ProfilingError::SerializationError(e.to_string()))?;

        let mut analysis = SessionAnalysis {
            session,
            cpu_hotspots: Vec::new(),
            memory_hotspots: Vec::new(),
            io_bottlenecks: Vec::new(),
            recommendations: Vec::new(),
        };

        // Analyze CPU profile
        if let Some(ref cpu_path) = analysis.session.cpu_profile_path {
            analysis.cpu_hotspots = cpu_profiler::analyze_cpu_profile(cpu_path)?;
        }

        // Analyze memory profile
        if let Some(ref memory_path) = analysis.session.memory_profile_path {
            analysis.memory_hotspots = memory_profiler::analyze_memory_profile(memory_path)?;
        }

        // Analyze I/O profile
        if let Some(ref io_path) = analysis.session.io_profile_path {
            analysis.io_bottlenecks = io_profiler::analyze_io_profile(io_path)?;
        }

        // Generate recommendations
        analysis.recommendations = generate_recommendations(&analysis);

        Ok(analysis)
    }

    /// Cleanup old profile sessions
    fn cleanup_old_profiles(&self) -> Result<(), ProfilingError> {
        let cutoff_time = SystemTime::now() - self.config.retention_period;

        for entry in std::fs::read_dir(&self.config.output_directory)
            .map_err(|e| ProfilingError::IoError(format!("Failed to read profiles directory: {}", e)))?
        {
            let entry = entry.map_err(|e| ProfilingError::IoError(e.to_string()))?;
            let path = entry.path();

            if path.is_dir() {
                let session_file = path.join("session.json");
                if session_file.exists() {
                    if let Ok(metadata) = session_file.metadata() {
                        if let Ok(created) = metadata.created() {
                            if created < cutoff_time {
                                info!("Cleaning up old profile session: {}", path.display());
                                if let Err(e) = std::fs::remove_dir_all(&path) {
                                    warn!("Failed to remove old profile {}: {}", path.display(), e);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Current profiling status
#[derive(Debug, Serialize)]
pub struct ProfilingStatus {
    pub active: bool,
    pub session_id: Option<String>,
    pub config: ProfilingConfig,
    pub current_metrics: Option<metrics_collector::CurrentMetrics>,
}

/// Detailed session analysis
#[derive(Debug, Serialize)]
pub struct SessionAnalysis {
    pub session: ProfileSession,
    pub cpu_hotspots: Vec<cpu_profiler::CpuHotspot>,
    pub memory_hotspots: Vec<memory_profiler::MemoryHotspot>,
    pub io_bottlenecks: Vec<io_profiler::IoBottleneck>,
    pub recommendations: Vec<PerformanceRecommendation>,
}

/// Performance optimization recommendation
#[derive(Debug, Clone, Serialize)]
pub struct PerformanceRecommendation {
    pub category: RecommendationCategory,
    pub severity: RecommendationSeverity,
    pub title: String,
    pub description: String,
    pub potential_impact: String,
    pub implementation_effort: ImplementationEffort,
}

#[derive(Debug, Clone, Serialize)]
pub enum RecommendationCategory {
    CpuOptimization,
    MemoryOptimization,
    IoOptimization,
    AlgorithmImprovement,
    CacheOptimization,
    ConcurrencyImprovement,
}

#[derive(Debug, Clone, Serialize)]
pub enum RecommendationSeverity {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Serialize)]
pub enum ImplementationEffort {
    Low,
    Medium,
    High,
    VeryHigh,
}

/// Generate performance recommendations based on analysis
fn generate_recommendations(analysis: &SessionAnalysis) -> Vec<PerformanceRecommendation> {
    let mut recommendations = Vec::new();

    // CPU recommendations
    for hotspot in &analysis.cpu_hotspots {
        if hotspot.cpu_percentage > 20.0 {
            recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::CpuOptimization,
                severity: if hotspot.cpu_percentage > 50.0 {
                    RecommendationSeverity::Critical
                } else {
                    RecommendationSeverity::High
                },
                title: format!("Optimize hot function: {}", hotspot.function_name),
                description: format!(
                    "Function {} consumes {:.1}% of CPU time. Consider optimization.",
                    hotspot.function_name, hotspot.cpu_percentage
                ),
                potential_impact: format!("Up to {:.1}% performance improvement", hotspot.cpu_percentage * 0.8),
                implementation_effort: ImplementationEffort::Medium,
            });
        }
    }

    // Memory recommendations
    for hotspot in &analysis.memory_hotspots {
        if hotspot.allocation_rate > 1024.0 * 1024.0 * 10.0 { // 10MB/s
            recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::MemoryOptimization,
                severity: RecommendationSeverity::High,
                title: format!("Reduce allocations in: {}", hotspot.location),
                description: format!(
                    "High allocation rate of {} bytes/sec detected",
                    hotspot.allocation_rate
                ),
                potential_impact: "Reduced GC pressure and improved latency".to_string(),
                implementation_effort: ImplementationEffort::Medium,
            });
        }
    }

    recommendations
}

/// Generate a unique session ID
fn generate_session_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    
    format!("profile_{}_{}", timestamp, fastrand::u32(10000, 99999))
}

/// Profiling errors
#[derive(Debug, thiserror::Error)]
pub enum ProfilingError {
    #[error("I/O error: {0}")]
    IoError(String),
    
    #[error("Profiling already active")]
    AlreadyActive,
    
    #[error("Profiling not active")]
    NotActive,
    
    #[error("Session not found: {0}")]
    SessionNotFound(String),
    
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    #[error("CPU profiler error: {0}")]
    CpuProfilerError(String),
    
    #[error("Memory profiler error: {0}")]
    MemoryProfilerError(String),
    
    #[error("I/O profiler error: {0}")]
    IoProfilerError(String),
    
    #[error("Flamegraph generation error: {0}")]
    FlamegraphError(String),
}

// Extensions to Duration for convenience
trait DurationExt {
    fn from_minutes(minutes: u64) -> Duration;
    fn from_days(days: u64) -> Duration;
}

impl DurationExt for Duration {
    fn from_minutes(minutes: u64) -> Duration {
        Duration::from_secs(minutes * 60)
    }
    
    fn from_days(days: u64) -> Duration {
        Duration::from_secs(days * 24 * 60 * 60)
    }
}

/// Fast random number generator for session IDs
mod fastrand {
    use std::sync::atomic::{AtomicU64, Ordering};
    
    static STATE: AtomicU64 = AtomicU64::new(1);
    
    pub fn u32(range_start: u32, range_end: u32) -> u32 {
        let mut x = STATE.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        STATE.store(x, Ordering::Relaxed);
        
        let range_size = range_end - range_start;
        range_start + ((x as u32) % range_size)
    }
}