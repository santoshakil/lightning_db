use super::{CompactionConfig, CompactionState, CompactionStats, CompactionType};
use crate::core::error::Result;
use chrono::Timelike;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedCompactionStats {
    pub by_type: HashMap<CompactionType, TypeStats>,
    pub by_hour: Vec<HourlyStats>,
    pub by_day: Vec<DailyStats>,
    pub performance_metrics: PerformanceMetrics,
    pub resource_usage: ResourceUsage,
    pub error_analysis: ErrorAnalysis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeStats {
    pub compaction_type: CompactionType,
    pub count: u64,
    pub total_bytes_processed: u64,
    pub total_bytes_reclaimed: u64,
    pub avg_duration: Duration,
    pub success_rate: f64,
    pub last_execution: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyStats {
    pub hour: u8, // 0-23
    pub compactions_count: u64,
    pub bytes_reclaimed: u64,
    pub avg_duration: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyStats {
    pub day_offset: u32, // Days ago (0 = today)
    pub compactions_count: u64,
    pub bytes_reclaimed: u64,
    pub total_duration: Duration,
    pub peak_hour: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub throughput_mb_per_sec: f64,
    pub io_efficiency: f64,        // bytes reclaimed / bytes processed
    pub compaction_frequency: f64, // compactions per hour
    pub fragmentation_reduction: f64,
    pub space_amplification: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub cpu_time: Duration,
    pub peak_memory_mb: u64,
    pub disk_io_mb: u64,
    pub concurrent_operations: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorAnalysis {
    pub total_errors: u64,
    pub error_types: HashMap<String, u64>,
    pub error_rate: f64,
    pub most_common_error: Option<String>,
}

#[derive(Debug)]
pub struct StatsCollector {
    config: Arc<RwLock<CompactionConfig>>,
    stats: Arc<RwLock<CompactionStats>>,
    detailed_stats: Arc<RwLock<DetailedCompactionStats>>,
    hourly_buckets: Arc<RwLock<[HourlyStats; 24]>>,
    daily_buckets: Arc<RwLock<[DailyStats; 30]>>, // 30 days
    collection_start_time: SystemTime,
}

impl StatsCollector {
    pub fn new(
        config: Arc<RwLock<CompactionConfig>>,
        stats: Arc<RwLock<CompactionStats>>,
    ) -> Result<Self> {
        let mut hourly_buckets = Vec::with_capacity(24);
        for hour in 0..24 {
            hourly_buckets.push(HourlyStats {
                hour,
                compactions_count: 0,
                bytes_reclaimed: 0,
                avg_duration: Duration::from_secs(0),
            });
        }

        let mut daily_buckets = Vec::with_capacity(30);
        for day in 0..30 {
            daily_buckets.push(DailyStats {
                day_offset: day,
                compactions_count: 0,
                bytes_reclaimed: 0,
                total_duration: Duration::from_secs(0),
                peak_hour: 0,
            });
        }

        let detailed_stats = DetailedCompactionStats {
            by_type: HashMap::new(),
            by_hour: Vec::new(),
            by_day: Vec::new(),
            performance_metrics: PerformanceMetrics {
                throughput_mb_per_sec: 0.0,
                io_efficiency: 0.0,
                compaction_frequency: 0.0,
                fragmentation_reduction: 0.0,
                space_amplification: 1.0,
            },
            resource_usage: ResourceUsage {
                cpu_time: Duration::from_secs(0),
                peak_memory_mb: 0,
                disk_io_mb: 0,
                concurrent_operations: 0,
            },
            error_analysis: ErrorAnalysis {
                total_errors: 0,
                error_types: HashMap::new(),
                error_rate: 0.0,
                most_common_error: None,
            },
        };

        Ok(Self {
            config,
            stats,
            detailed_stats: Arc::new(RwLock::new(detailed_stats)),
            hourly_buckets: Arc::new(RwLock::new(hourly_buckets.try_into().unwrap())),
            daily_buckets: Arc::new(RwLock::new(daily_buckets.try_into().unwrap())),
            collection_start_time: SystemTime::now(),
        })
    }

    pub async fn record_compaction(
        &self,
        compaction_type: CompactionType,
        bytes_processed: u64,
        bytes_reclaimed: u64,
        duration: Duration,
        state: CompactionState,
    ) -> Result<()> {
        // Update basic stats
        {
            let mut stats = self.stats.write().await;
            stats.total_compactions += 1;
            stats.bytes_compacted += bytes_processed;
            stats.space_reclaimed += bytes_reclaimed;
            stats.last_compaction = Some(Instant::now());

            match state {
                CompactionState::Complete => stats.successful_compactions += 1,
                CompactionState::Failed => stats.failed_compactions += 1,
                _ => {}
            }

            // Update average time
            let total_time = stats.avg_compaction_time.as_secs_f64()
                * (stats.total_compactions - 1) as f64
                + duration.as_secs_f64();
            stats.avg_compaction_time =
                Duration::from_secs_f64(total_time / stats.total_compactions as f64);
        }

        // Update detailed stats
        self.update_type_stats(
            compaction_type.clone(),
            bytes_processed,
            bytes_reclaimed,
            duration,
            state,
        )
        .await?;
        self.update_hourly_stats(bytes_reclaimed, duration).await?;
        self.update_daily_stats(bytes_reclaimed, duration).await?;
        self.update_performance_metrics(bytes_processed, bytes_reclaimed, duration)
            .await?;

        Ok(())
    }

    pub async fn record_error(&self, error_type: String) -> Result<()> {
        let mut detailed = self.detailed_stats.write().await;

        detailed.error_analysis.total_errors += 1;
        *detailed
            .error_analysis
            .error_types
            .entry(error_type.clone())
            .or_insert(0) += 1;

        // Update most common error
        let max_count = detailed
            .error_analysis
            .error_types
            .values()
            .max()
            .unwrap_or(&0);
        if let Some((error, _count)) = detailed
            .error_analysis
            .error_types
            .iter()
            .find(|(_, &c)| c == *max_count)
        {
            detailed.error_analysis.most_common_error = Some(error.clone());
        }

        // Update error rate
        let total_operations = {
            let stats = self.stats.read().await;
            stats.total_compactions
        };

        if total_operations > 0 {
            detailed.error_analysis.error_rate =
                detailed.error_analysis.total_errors as f64 / total_operations as f64;
        }

        Ok(())
    }

    pub async fn get_detailed_stats(&self) -> DetailedCompactionStats {
        let mut detailed = self.detailed_stats.read().await.clone();

        // Update current hourly and daily data
        detailed.by_hour = self.hourly_buckets.read().await.to_vec();
        detailed.by_day = self.daily_buckets.read().await.to_vec();

        detailed
    }

    pub async fn get_performance_report(&self) -> Result<String> {
        let detailed = self.get_detailed_stats().await;
        let stats = self.stats.read().await;

        let mut report = String::new();

        report.push_str("=== Lightning DB Compaction Performance Report ===\n\n");

        // Basic Stats
        report.push_str(&format!("Total Compactions: {}\n", stats.total_compactions));
        report.push_str(&format!(
            "Successful: {} ({:.2}%)\n",
            stats.successful_compactions,
            if stats.total_compactions > 0 {
                stats.successful_compactions as f64 / stats.total_compactions as f64 * 100.0
            } else {
                0.0
            }
        ));
        report.push_str(&format!(
            "Failed: {} ({:.2}%)\n",
            stats.failed_compactions,
            if stats.total_compactions > 0 {
                stats.failed_compactions as f64 / stats.total_compactions as f64 * 100.0
            } else {
                0.0
            }
        ));
        report.push_str(&format!(
            "Space Reclaimed: {:.2} MB\n",
            stats.space_reclaimed as f64 / (1024.0 * 1024.0)
        ));
        report.push_str(&format!(
            "Average Duration: {:.2}s\n\n",
            stats.avg_compaction_time.as_secs_f64()
        ));

        // Performance Metrics
        report.push_str("Performance Metrics:\n");
        report.push_str(&format!(
            "  Throughput: {:.2} MB/s\n",
            detailed.performance_metrics.throughput_mb_per_sec
        ));
        report.push_str(&format!(
            "  I/O Efficiency: {:.2}%\n",
            detailed.performance_metrics.io_efficiency * 100.0
        ));
        report.push_str(&format!(
            "  Compaction Frequency: {:.2}/hour\n",
            detailed.performance_metrics.compaction_frequency
        ));
        report.push_str(&format!(
            "  Space Amplification: {:.2}x\n\n",
            detailed.performance_metrics.space_amplification
        ));

        // By Type
        report.push_str("By Compaction Type:\n");
        for (comp_type, type_stats) in &detailed.by_type {
            report.push_str(&format!(
                "  {:?}: {} operations, {:.2} MB reclaimed, {:.2}s avg\n",
                comp_type,
                type_stats.count,
                type_stats.total_bytes_reclaimed as f64 / (1024.0 * 1024.0),
                type_stats.avg_duration.as_secs_f64()
            ));
        }
        report.push('\n');

        // Resource Usage
        report.push_str("Resource Usage:\n");
        report.push_str(&format!(
            "  CPU Time: {:.2}s\n",
            detailed.resource_usage.cpu_time.as_secs_f64()
        ));
        report.push_str(&format!(
            "  Peak Memory: {} MB\n",
            detailed.resource_usage.peak_memory_mb
        ));
        report.push_str(&format!(
            "  Disk I/O: {} MB\n",
            detailed.resource_usage.disk_io_mb
        ));
        report.push_str(&format!(
            "  Max Concurrent: {}\n\n",
            detailed.resource_usage.concurrent_operations
        ));

        // Error Analysis
        if detailed.error_analysis.total_errors > 0 {
            report.push_str("Error Analysis:\n");
            report.push_str(&format!(
                "  Total Errors: {}\n",
                detailed.error_analysis.total_errors
            ));
            report.push_str(&format!(
                "  Error Rate: {:.2}%\n",
                detailed.error_analysis.error_rate * 100.0
            ));
            if let Some(common_error) = &detailed.error_analysis.most_common_error {
                report.push_str(&format!("  Most Common: {}\n", common_error));
            }
            report.push('\n');
        }

        // Recommendations
        report.push_str("Recommendations:\n");
        if detailed.performance_metrics.io_efficiency < 0.7 {
            report
                .push_str("  - Consider tuning compaction thresholds to improve I/O efficiency\n");
        }
        if detailed.performance_metrics.compaction_frequency > 10.0 {
            report.push_str(
                "  - High compaction frequency detected - consider increasing write buffer size\n",
            );
        }
        if detailed.error_analysis.error_rate > 0.05 {
            report.push_str("  - Error rate is high - investigate most common error types\n");
        }
        if detailed.performance_metrics.space_amplification > 3.0 {
            report.push_str("  - High space amplification - consider more aggressive compaction\n");
        }

        Ok(report)
    }

    pub async fn export_stats_json(&self) -> Result<String> {
        let detailed = self.get_detailed_stats().await;
        let stats = self.stats.read().await;

        let combined = serde_json::json!({
            "basic_stats": {
                "total_compactions": stats.total_compactions,
                "successful_compactions": stats.successful_compactions,
                "failed_compactions": stats.failed_compactions,
                "bytes_compacted": stats.bytes_compacted,
                "space_reclaimed": stats.space_reclaimed,
                "avg_compaction_time_secs": stats.avg_compaction_time.as_secs_f64(),
                "last_compaction": stats.last_compaction.map(|t| t.elapsed().as_secs())
            },
            "performance_metrics": detailed.performance_metrics,
            "resource_usage": detailed.resource_usage,
            "error_analysis": detailed.error_analysis,
            "by_type": detailed.by_type,
            "hourly_distribution": detailed.by_hour,
            "daily_trend": detailed.by_day
        });

        Ok(serde_json::to_string_pretty(&combined)?)
    }

    pub async fn reset_stats(&self) -> Result<()> {
        {
            let mut stats = self.stats.write().await;
            *stats = CompactionStats {
                total_compactions: 0,
                successful_compactions: 0,
                failed_compactions: 0,
                bytes_compacted: 0,
                space_reclaimed: 0,
                avg_compaction_time: Duration::from_secs(0),
                last_compaction: None,
                current_operations: Vec::new(),
            };
        }

        {
            let mut detailed = self.detailed_stats.write().await;
            detailed.by_type.clear();
            detailed.performance_metrics = PerformanceMetrics {
                throughput_mb_per_sec: 0.0,
                io_efficiency: 0.0,
                compaction_frequency: 0.0,
                fragmentation_reduction: 0.0,
                space_amplification: 1.0,
            };
            detailed.error_analysis = ErrorAnalysis {
                total_errors: 0,
                error_types: HashMap::new(),
                error_rate: 0.0,
                most_common_error: None,
            };
        }

        // Reset hourly buckets
        {
            let mut hourly = self.hourly_buckets.write().await;
            for hour in 0..24 {
                hourly[hour] = HourlyStats {
                    hour: hour as u8,
                    compactions_count: 0,
                    bytes_reclaimed: 0,
                    avg_duration: Duration::from_secs(0),
                };
            }
        }

        // Reset daily buckets
        {
            let mut daily = self.daily_buckets.write().await;
            for day in 0..30 {
                daily[day] = DailyStats {
                    day_offset: day as u32,
                    compactions_count: 0,
                    bytes_reclaimed: 0,
                    total_duration: Duration::from_secs(0),
                    peak_hour: 0,
                };
            }
        }

        Ok(())
    }

    async fn update_type_stats(
        &self,
        compaction_type: CompactionType,
        bytes_processed: u64,
        bytes_reclaimed: u64,
        duration: Duration,
        state: CompactionState,
    ) -> Result<()> {
        let mut detailed = self.detailed_stats.write().await;

        let type_stats = detailed
            .by_type
            .entry(compaction_type.clone())
            .or_insert(TypeStats {
                compaction_type: compaction_type.clone(),
                count: 0,
                total_bytes_processed: 0,
                total_bytes_reclaimed: 0,
                avg_duration: Duration::from_secs(0),
                success_rate: 1.0,
                last_execution: None,
            });

        type_stats.count += 1;
        type_stats.total_bytes_processed += bytes_processed;
        type_stats.total_bytes_reclaimed += bytes_reclaimed;
        type_stats.last_execution = Some(SystemTime::now());

        // Update average duration
        let total_time = type_stats.avg_duration.as_secs_f64() * (type_stats.count - 1) as f64
            + duration.as_secs_f64();
        type_stats.avg_duration = Duration::from_secs_f64(total_time / type_stats.count as f64);

        // Update success rate based on state
        match state {
            CompactionState::Complete => {
                // Success rate improves
            }
            CompactionState::Failed => {
                // Recalculate success rate (this is simplified)
                type_stats.success_rate = (type_stats.success_rate * (type_stats.count - 1) as f64)
                    / type_stats.count as f64;
            }
            _ => {}
        }

        Ok(())
    }

    async fn update_hourly_stats(&self, bytes_reclaimed: u64, duration: Duration) -> Result<()> {
        let current_hour = chrono::Utc::now().hour() as usize;
        let mut hourly = self.hourly_buckets.write().await;

        if current_hour < 24 {
            let hour_stats = &mut hourly[current_hour];
            hour_stats.compactions_count += 1;
            hour_stats.bytes_reclaimed += bytes_reclaimed;

            // Update average duration
            let total_time = hour_stats.avg_duration.as_secs_f64()
                * (hour_stats.compactions_count - 1) as f64
                + duration.as_secs_f64();
            hour_stats.avg_duration =
                Duration::from_secs_f64(total_time / hour_stats.compactions_count as f64);
        }

        Ok(())
    }

    async fn update_daily_stats(&self, bytes_reclaimed: u64, duration: Duration) -> Result<()> {
        let mut daily = self.daily_buckets.write().await;
        let today_stats = &mut daily[0]; // Today is always index 0

        today_stats.compactions_count += 1;
        today_stats.bytes_reclaimed += bytes_reclaimed;
        today_stats.total_duration += duration;

        // Update peak hour
        let _current_hour = chrono::Utc::now().hour();
        let hourly = self.hourly_buckets.read().await;
        if let Some(peak) = (0..24).max_by_key(|&h| hourly[h].compactions_count) {
            today_stats.peak_hour = peak as u8;
        }

        Ok(())
    }

    async fn update_performance_metrics(
        &self,
        bytes_processed: u64,
        bytes_reclaimed: u64,
        duration: Duration,
    ) -> Result<()> {
        let mut detailed = self.detailed_stats.write().await;
        let stats = self.stats.read().await;

        // Update throughput (MB/s)
        if duration.as_secs_f64() > 0.0 {
            let mb_processed = bytes_processed as f64 / (1024.0 * 1024.0);
            detailed.performance_metrics.throughput_mb_per_sec =
                mb_processed / duration.as_secs_f64();
        }

        // Update I/O efficiency
        if bytes_processed > 0 {
            detailed.performance_metrics.io_efficiency =
                bytes_reclaimed as f64 / bytes_processed as f64;
        }

        // Update compaction frequency (compactions per hour)
        let uptime_hours = self
            .collection_start_time
            .elapsed()
            .unwrap_or_default()
            .as_secs_f64()
            / 3600.0;
        if uptime_hours > 0.0 {
            detailed.performance_metrics.compaction_frequency =
                stats.total_compactions as f64 / uptime_hours;
        }

        // Update space amplification (simplified calculation)
        if stats.space_reclaimed > 0 {
            detailed.performance_metrics.space_amplification =
                stats.bytes_compacted as f64 / stats.space_reclaimed as f64;
        }

        Ok(())
    }

    pub async fn get_trend_analysis(&self, days: u32) -> Result<HashMap<String, f64>> {
        let daily = self.daily_buckets.read().await;
        let mut trends = HashMap::new();

        let days_to_analyze = (days.min(30) as usize).max(1);

        // Calculate trends over the specified period
        let recent_days: Vec<_> = daily.iter().take(days_to_analyze).collect();

        if recent_days.len() >= 2 {
            let first = recent_days.last().unwrap();
            let last = recent_days.first().unwrap();

            // Compaction count trend
            let count_trend = if first.compactions_count > 0 {
                (last.compactions_count as f64 - first.compactions_count as f64)
                    / first.compactions_count as f64
                    * 100.0
            } else {
                0.0
            };
            trends.insert("compaction_count_trend_pct".to_string(), count_trend);

            // Bytes reclaimed trend
            let bytes_trend = if first.bytes_reclaimed > 0 {
                (last.bytes_reclaimed as f64 - first.bytes_reclaimed as f64)
                    / first.bytes_reclaimed as f64
                    * 100.0
            } else {
                0.0
            };
            trends.insert("bytes_reclaimed_trend_pct".to_string(), bytes_trend);

            // Duration trend
            let duration_trend = if first.total_duration.as_secs_f64() > 0.0 {
                (last.total_duration.as_secs_f64() - first.total_duration.as_secs_f64())
                    / first.total_duration.as_secs_f64()
                    * 100.0
            } else {
                0.0
            };
            trends.insert("duration_trend_pct".to_string(), duration_trend);
        }

        Ok(trends)
    }
}

use chrono;
use serde_json;
