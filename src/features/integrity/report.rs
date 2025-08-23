use super::{
    scanner::ScanResult,
    detector::{DetectionResult, CorruptionType, CorruptionSeverity},
    quarantine::QuarantineStats,
    IntegrityStats,
};
use crate::core::error::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReportFormat {
    Json,
    Text,
    Html,
    Csv,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorruptionReport {
    pub report_id: String,
    pub generated_at: SystemTime,
    pub database_info: DatabaseInfo,
    pub scan_summary: ScanSummary,
    pub corruption_details: Vec<CorruptionDetail>,
    pub integrity_stats: IntegrityStats,
    pub quarantine_info: QuarantineStats,
    pub recommendations: Vec<String>,
    pub risk_assessment: RiskAssessment,
    pub recovery_status: RecoveryStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub version: String,
    pub total_pages: u64,
    pub total_size: u64,
    pub last_backup: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanSummary {
    pub scan_type: String,
    pub pages_scanned: u64,
    pub scan_duration: Option<Duration>,
    pub corruption_count: usize,
    pub corruption_percentage: f64,
    pub worst_severity: Option<CorruptionSeverity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorruptionDetail {
    pub page_id: u64,
    pub corruption_type: CorruptionType,
    pub severity: CorruptionSeverity,
    pub description: String,
    pub affected_data_size: usize,
    pub recovery_hint: Option<String>,
    pub discovered_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskAssessment {
    pub overall_risk: RiskLevel,
    pub data_loss_probability: f64,
    pub performance_impact: PerformanceImpact,
    pub business_impact: BusinessImpact,
    pub urgency: UrgencyLevel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
    Catastrophic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PerformanceImpact {
    None,
    Minimal,
    Moderate,
    Severe,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BusinessImpact {
    None,
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UrgencyLevel {
    Low,
    Medium,
    High,
    Immediate,
    Emergency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryStatus {
    pub recoverable_pages: u64,
    pub unrecoverable_pages: u64,
    pub recovery_strategies_available: Vec<String>,
    pub estimated_recovery_time: Option<Duration>,
    pub backup_availability: BackupAvailability,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupAvailability {
    pub has_recent_backup: bool,
    pub last_backup_age: Option<Duration>,
    pub backup_completeness: f64,
}

pub struct ReportGenerator {
    report_counter: std::sync::atomic::AtomicU64,
}

impl ReportGenerator {
    pub fn new() -> Self {
        Self {
            report_counter: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub async fn generate_report(
        &self,
        scan_result: &ScanResult,
        integrity_stats: &IntegrityStats,
    ) -> crate::Result<CorruptionReport> {
        let report_id = self.generate_report_id();
        
        let database_info = self.get_database_info().await;
        let scan_summary = self.create_scan_summary(scan_result);
        let corruption_details = self.extract_corruption_details(&scan_result.corruptions);
        let risk_assessment = self.assess_risk(&scan_result.corruptions);
        let recovery_status = self.assess_recovery_status(&scan_result.corruptions);
        let recommendations = self.generate_recommendations(&scan_result.corruptions, &risk_assessment);

        Ok(CorruptionReport {
            report_id,
            generated_at: SystemTime::now(),
            database_info,
            scan_summary,
            corruption_details,
            integrity_stats: integrity_stats.clone(),
            quarantine_info: QuarantineStats {
                total_entries: 0,
                by_status: HashMap::new(),
                by_corruption_type: HashMap::new(),
                oldest_entry: None,
                newest_entry: None,
                total_data_size: 0,
            }, // Will be populated by caller
            recommendations,
            risk_assessment,
            recovery_status,
        })
    }

    pub async fn generate_status_report(
        &self,
        integrity_stats: &IntegrityStats,
        quarantine_stats: &QuarantineStats,
    ) -> crate::Result<CorruptionReport> {
        let report_id = self.generate_report_id();
        
        let database_info = self.get_database_info().await;
        
        // Create a status-only report
        let scan_summary = ScanSummary {
            scan_type: "Status Report".to_string(),
            pages_scanned: integrity_stats.total_scans,
            scan_duration: integrity_stats.scan_duration,
            corruption_count: quarantine_stats.total_entries,
            corruption_percentage: 0.0, // Calculate based on available data
            worst_severity: None,
        };

        let risk_assessment = self.assess_risk_from_stats(integrity_stats, quarantine_stats);
        let recommendations = self.generate_status_recommendations(integrity_stats, quarantine_stats);

        Ok(CorruptionReport {
            report_id,
            generated_at: SystemTime::now(),
            database_info,
            scan_summary,
            corruption_details: Vec::new(),
            integrity_stats: integrity_stats.clone(),
            quarantine_info: quarantine_stats.clone(),
            recommendations,
            risk_assessment,
            recovery_status: RecoveryStatus {
                recoverable_pages: 0,
                unrecoverable_pages: quarantine_stats.total_entries as u64,
                recovery_strategies_available: vec![
                    "WAL Recovery".to_string(),
                    "Backup Restore".to_string(),
                ],
                estimated_recovery_time: Some(Duration::from_secs(30 * 60)),
                backup_availability: BackupAvailability {
                    has_recent_backup: true,
                    last_backup_age: Some(Duration::from_secs(24 * 3600)),
                    backup_completeness: 1.0,
                },
            },
        })
    }

    pub async fn format_report(
        &self,
        report: &CorruptionReport,
        format: ReportFormat,
    ) -> crate::Result<String> {
        match format {
            ReportFormat::Json => self.format_as_json(report),
            ReportFormat::Text => self.format_as_text(report),
            ReportFormat::Html => self.format_as_html(report),
            ReportFormat::Csv => self.format_as_csv(report),
        }
    }

    fn format_as_json(&self, report: &CorruptionReport) -> crate::Result<String> {
        serde_json::to_string_pretty(report)
            .map_err(|e| Error::Generic(format!("Serialization error: {}", e)))
    }

    fn format_as_text(&self, report: &CorruptionReport) -> crate::Result<String> {
        let mut output = String::new();

        output.push_str("=== LIGHTNING DB INTEGRITY REPORT ===\n\n");
        output.push_str(&format!("Report ID: {}\n", report.report_id));
        output.push_str(&format!("Generated: {:?}\n", report.generated_at));
        output.push_str(&format!("Database: {}\n\n", report.database_info.name));

        output.push_str("=== SCAN SUMMARY ===\n");
        output.push_str(&format!("Scan Type: {}\n", report.scan_summary.scan_type));
        output.push_str(&format!("Pages Scanned: {}\n", report.scan_summary.pages_scanned));
        output.push_str(&format!("Corruptions Found: {}\n", report.scan_summary.corruption_count));
        output.push_str(&format!("Corruption Rate: {:.2}%\n", report.scan_summary.corruption_percentage));
        
        if let Some(duration) = report.scan_summary.scan_duration {
            output.push_str(&format!("Scan Duration: {:?}\n", duration));
        }
        output.push('\n');

        output.push_str("=== RISK ASSESSMENT ===\n");
        output.push_str(&format!("Overall Risk: {:?}\n", report.risk_assessment.overall_risk));
        output.push_str(&format!("Data Loss Probability: {:.1}%\n", 
            report.risk_assessment.data_loss_probability * 100.0));
        output.push_str(&format!("Performance Impact: {:?}\n", report.risk_assessment.performance_impact));
        output.push_str(&format!("Urgency: {:?}\n", report.risk_assessment.urgency));
        output.push('\n');

        if !report.corruption_details.is_empty() {
            output.push_str("=== CORRUPTION DETAILS ===\n");
            for (i, detail) in report.corruption_details.iter().enumerate() {
                output.push_str(&format!("{}. Page {}: {:?} ({:?})\n", 
                    i + 1, detail.page_id, detail.corruption_type, detail.severity));
                output.push_str(&format!("   {}\n", detail.description));
                if let Some(hint) = &detail.recovery_hint {
                    output.push_str(&format!("   Recovery: {}\n", hint));
                }
                output.push('\n');
            }
        }

        output.push_str("=== RECOMMENDATIONS ===\n");
        for (i, recommendation) in report.recommendations.iter().enumerate() {
            output.push_str(&format!("{}. {}\n", i + 1, recommendation));
        }
        output.push('\n');

        output.push_str("=== RECOVERY STATUS ===\n");
        output.push_str(&format!("Recoverable Pages: {}\n", report.recovery_status.recoverable_pages));
        output.push_str(&format!("Unrecoverable Pages: {}\n", report.recovery_status.unrecoverable_pages));
        output.push_str("Available Recovery Strategies:\n");
        for strategy in &report.recovery_status.recovery_strategies_available {
            output.push_str(&format!("  - {}\n", strategy));
        }

        Ok(output)
    }

    fn format_as_html(&self, report: &CorruptionReport) -> crate::Result<String> {
        let mut html = String::new();
        
        html.push_str("<!DOCTYPE html>\n<html>\n<head>\n");
        html.push_str("<title>Lightning DB Integrity Report</title>\n");
        html.push_str("<style>\n");
        html.push_str("body { font-family: Arial, sans-serif; margin: 20px; }\n");
        html.push_str(".header { background-color: #f0f0f0; padding: 10px; border-radius: 5px; }\n");
        html.push_str(".section { margin: 20px 0; }\n");
        html.push_str(".corruption { background-color: #ffe6e6; padding: 10px; margin: 5px 0; border-radius: 3px; }\n");
        html.push_str(".warning { color: #ff9900; }\n");
        html.push_str(".error { color: #cc0000; }\n");
        html.push_str(".critical { color: #990000; font-weight: bold; }\n");
        html.push_str("table { border-collapse: collapse; width: 100%; }\n");
        html.push_str("th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n");
        html.push_str("th { background-color: #f2f2f2; }\n");
        html.push_str("</style>\n</head>\n<body>\n");

        html.push_str("<div class='header'>\n");
        html.push_str("<h1>Lightning DB Integrity Report</h1>\n");
        html.push_str(&format!("<p>Report ID: {}</p>\n", report.report_id));
        html.push_str(&format!("<p>Generated: {:?}</p>\n", report.generated_at));
        html.push_str("</div>\n");

        html.push_str("<div class='section'>\n<h2>Scan Summary</h2>\n");
        html.push_str("<table>\n");
        html.push_str("<tr><th>Metric</th><th>Value</th></tr>\n");
        html.push_str(&format!("<tr><td>Pages Scanned</td><td>{}</td></tr>\n", report.scan_summary.pages_scanned));
        html.push_str(&format!("<tr><td>Corruptions Found</td><td>{}</td></tr>\n", report.scan_summary.corruption_count));
        html.push_str(&format!("<tr><td>Corruption Rate</td><td>{:.2}%</td></tr>\n", report.scan_summary.corruption_percentage));
        html.push_str("</table>\n</div>\n");

        if !report.corruption_details.is_empty() {
            html.push_str("<div class='section'>\n<h2>Corruption Details</h2>\n");
            for detail in &report.corruption_details {
                let severity_class = match detail.severity {
                    CorruptionSeverity::Low => "",
                    CorruptionSeverity::Medium => "warning",
                    CorruptionSeverity::High => "error",
                    CorruptionSeverity::Critical => "critical",
                };
                
                html.push_str(&format!("<div class='corruption {}'>\n", severity_class));
                html.push_str(&format!("<h3>Page {} - {:?}</h3>\n", detail.page_id, detail.corruption_type));
                html.push_str(&format!("<p>{}</p>\n", detail.description));
                if let Some(hint) = &detail.recovery_hint {
                    html.push_str(&format!("<p><strong>Recovery:</strong> {}</p>\n", hint));
                }
                html.push_str("</div>\n");
            }
            html.push_str("</div>\n");
        }

        html.push_str("</body>\n</html>");
        Ok(html)
    }

    fn format_as_csv(&self, report: &CorruptionReport) -> crate::Result<String> {
        let mut csv = String::new();
        
        csv.push_str("Page ID,Corruption Type,Severity,Description,Recovery Hint\n");
        
        for detail in &report.corruption_details {
            csv.push_str(&format!(
                "{},{:?},{:?},\"{}\",\"{}\"\n",
                detail.page_id,
                detail.corruption_type,
                detail.severity,
                detail.description.replace("\"", "\"\""),
                detail.recovery_hint.as_deref().unwrap_or("").replace("\"", "\"\"")
            ));
        }

        Ok(csv)
    }

    fn generate_report_id(&self) -> String {
        let counter = self.report_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        format!("integrity_report_{}_{}", timestamp, counter)
    }

    async fn get_database_info(&self) -> DatabaseInfo {
        DatabaseInfo {
            name: "Lightning DB".to_string(),
            version: "1.0.0".to_string(),
            total_pages: 1000, // Would be queried from database
            total_size: 4096000, // Would be calculated
            last_backup: Some(SystemTime::now() - Duration::from_secs(24 * 3600)),
        }
    }

    fn create_scan_summary(&self, scan_result: &ScanResult) -> ScanSummary {
        let corruption_percentage = if scan_result.pages_scanned > 0 {
            (scan_result.corruptions.len() as f64 / scan_result.pages_scanned as f64) * 100.0
        } else {
            0.0
        };

        let worst_severity = scan_result.corruptions.iter()
            .map(|c| c.severity)
            .max();

        ScanSummary {
            scan_type: format!("{:?}", scan_result.scan_type),
            pages_scanned: scan_result.pages_scanned,
            scan_duration: scan_result.duration,
            corruption_count: scan_result.corruptions.len(),
            corruption_percentage,
            worst_severity,
        }
    }

    fn extract_corruption_details(&self, corruptions: &[DetectionResult]) -> Vec<CorruptionDetail> {
        corruptions.iter().map(|corruption| {
            CorruptionDetail {
                page_id: corruption.page_id,
                corruption_type: corruption.corruption_type.clone(),
                severity: corruption.severity,
                description: corruption.description.clone(),
                affected_data_size: corruption.affected_data.len(),
                recovery_hint: corruption.recovery_hint.clone(),
                discovered_at: corruption.timestamp,
            }
        }).collect()
    }

    fn assess_risk(&self, corruptions: &[DetectionResult]) -> RiskAssessment {
        let critical_count = corruptions.iter()
            .filter(|c| c.severity == CorruptionSeverity::Critical)
            .count();
        
        let high_count = corruptions.iter()
            .filter(|c| c.severity == CorruptionSeverity::High)
            .count();

        let overall_risk = if critical_count > 0 {
            if critical_count > 5 {
                RiskLevel::Catastrophic
            } else {
                RiskLevel::Critical
            }
        } else if high_count > 0 {
            RiskLevel::High
        } else if corruptions.len() > 10 {
            RiskLevel::Medium
        } else {
            RiskLevel::Low
        };

        let data_loss_probability = match overall_risk {
            RiskLevel::Low => 0.01,
            RiskLevel::Medium => 0.05,
            RiskLevel::High => 0.25,
            RiskLevel::Critical => 0.75,
            RiskLevel::Catastrophic => 0.95,
        };

        let performance_impact = if critical_count > 0 {
            PerformanceImpact::Severe
        } else if high_count > 3 {
            PerformanceImpact::Moderate
        } else if corruptions.len() > 5 {
            PerformanceImpact::Minimal
        } else {
            PerformanceImpact::None
        };

        let urgency = match overall_risk {
            RiskLevel::Catastrophic => UrgencyLevel::Emergency,
            RiskLevel::Critical => UrgencyLevel::Immediate,
            RiskLevel::High => UrgencyLevel::High,
            RiskLevel::Medium => UrgencyLevel::Medium,
            RiskLevel::Low => UrgencyLevel::Low,
        };

        RiskAssessment {
            overall_risk,
            data_loss_probability,
            performance_impact,
            business_impact: match overall_risk {
                RiskLevel::Catastrophic | RiskLevel::Critical => BusinessImpact::Critical,
                RiskLevel::High => BusinessImpact::High,
                RiskLevel::Medium => BusinessImpact::Medium,
                RiskLevel::Low => BusinessImpact::Low,
            },
            urgency,
        }
    }

    fn assess_recovery_status(&self, corruptions: &[DetectionResult]) -> RecoveryStatus {
        let recoverable = corruptions.iter()
            .filter(|c| c.recovery_hint.is_some())
            .count() as u64;
        
        let unrecoverable = corruptions.len() as u64 - recoverable;

        RecoveryStatus {
            recoverable_pages: recoverable,
            unrecoverable_pages: unrecoverable,
            recovery_strategies_available: vec![
                "WAL Recovery".to_string(),
                "Backup Restore".to_string(),
                "Partial Reconstruction".to_string(),
            ],
            estimated_recovery_time: Some(Duration::from_secs(corruptions.len() as u64 * 2 * 60)),
            backup_availability: BackupAvailability {
                has_recent_backup: true,
                last_backup_age: Some(Duration::from_secs(24 * 3600)),
                backup_completeness: 0.95,
            },
        }
    }

    fn generate_recommendations(&self, corruptions: &[DetectionResult], risk: &RiskAssessment) -> Vec<String> {
        let mut recommendations = Vec::new();

        match risk.overall_risk {
            RiskLevel::Catastrophic | RiskLevel::Critical => {
                recommendations.push("IMMEDIATE ACTION REQUIRED: Stop all write operations".to_string());
                recommendations.push("Create emergency backup of current state".to_string());
                recommendations.push("Contact database administrator immediately".to_string());
            }
            RiskLevel::High => {
                recommendations.push("Schedule immediate maintenance window for repairs".to_string());
                recommendations.push("Enable automatic backups if not already enabled".to_string());
                recommendations.push("Monitor database closely for additional corruption".to_string());
            }
            RiskLevel::Medium => {
                recommendations.push("Plan maintenance window within 24-48 hours".to_string());
                recommendations.push("Increase backup frequency".to_string());
                recommendations.push("Review database configuration for optimization".to_string());
            }
            RiskLevel::Low => {
                recommendations.push("Schedule routine maintenance to address minor issues".to_string());
                recommendations.push("Continue regular monitoring".to_string());
            }
        }

        // Add specific recommendations based on corruption types
        let corruption_types: std::collections::HashSet<_> = corruptions.iter()
            .map(|c| c.corruption_type.clone())
            .collect();

        for corruption_type in corruption_types {
            match corruption_type {
                CorruptionType::ChecksumMismatch => {
                    recommendations.push("Consider hardware diagnostics for storage devices".to_string());
                }
                CorruptionType::BTreeStructureViolation => {
                    recommendations.push("Rebuild affected indexes during maintenance window".to_string());
                }
                CorruptionType::WalInconsistency => {
                    recommendations.push("Review WAL configuration and disk space".to_string());
                }
                _ => {}
            }
        }

        recommendations
    }

    fn assess_risk_from_stats(&self, _integrity_stats: &IntegrityStats, quarantine_stats: &QuarantineStats) -> RiskAssessment {
        let overall_risk = if quarantine_stats.total_entries > 10 {
            RiskLevel::High
        } else if quarantine_stats.total_entries > 5 {
            RiskLevel::Medium
        } else {
            RiskLevel::Low
        };

        RiskAssessment {
            overall_risk,
            data_loss_probability: quarantine_stats.total_entries as f64 * 0.01,
            performance_impact: PerformanceImpact::Minimal,
            business_impact: BusinessImpact::Low,
            urgency: UrgencyLevel::Medium,
        }
    }

    fn generate_status_recommendations(&self, _integrity_stats: &IntegrityStats, quarantine_stats: &QuarantineStats) -> Vec<String> {
        let mut recommendations = Vec::new();

        if quarantine_stats.total_entries > 0 {
            recommendations.push("Review quarantined data for recovery opportunities".to_string());
            recommendations.push("Schedule integrity scanning to prevent future corruption".to_string());
        } else {
            recommendations.push("Database integrity appears good".to_string());
            recommendations.push("Continue regular monitoring and maintenance".to_string());
        }

        recommendations
    }
}