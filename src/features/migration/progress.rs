use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, Instant},
};
use serde::{Deserialize, Serialize};
use parking_lot::RwLock;
use super::{MigrationVersion, MigrationStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgress {
    pub total_migrations: usize,
    pub completed_migrations: usize,
    pub current_migration: Option<MigrationVersion>,
    pub current_step: String,
    pub progress_percentage: u8,
    pub started_at: Option<SystemTime>,
    pub estimated_completion: Option<SystemTime>,
    pub migration_details: HashMap<MigrationVersion, MigrationProgressDetail>,
}

impl MigrationProgress {
    pub fn new() -> Self {
        Self {
            total_migrations: 0,
            completed_migrations: 0,
            current_migration: None,
            current_step: "Initializing".to_string(),
            progress_percentage: 0,
            started_at: None,
            estimated_completion: None,
            migration_details: HashMap::new(),
        }
    }
    
    pub fn start(&mut self, total_migrations: usize) {
        self.total_migrations = total_migrations;
        self.completed_migrations = 0;
        self.progress_percentage = 0;
        self.started_at = Some(SystemTime::now());
        self.current_step = "Starting migration".to_string();
        self.update_estimated_completion();
    }
    
    pub fn update_migration_progress(
        &mut self,
        version: MigrationVersion,
        step: &str,
        percentage: u8,
    ) {
        self.current_migration = Some(version);
        self.current_step = step.to_string();
        
        let detail = self.migration_details.entry(version).or_insert_with(|| {
            MigrationProgressDetail::new(version)
        });
        
        detail.current_step = step.to_string();
        detail.progress_percentage = percentage;
        detail.updated_at = SystemTime::now();
        
        if percentage == 100 && detail.status != MigrationStatus::Completed {
            detail.status = MigrationStatus::Completed;
            detail.completed_at = Some(SystemTime::now());
            self.completed_migrations += 1;
        }
        
        self.calculate_overall_progress();
        self.update_estimated_completion();
    }
    
    pub fn mark_migration_failed(&mut self, version: MigrationVersion, error: &str) {
        let detail = self.migration_details.entry(version).or_insert_with(|| {
            MigrationProgressDetail::new(version)
        });
        
        detail.status = MigrationStatus::Failed;
        detail.error_message = Some(error.to_string());
        detail.completed_at = Some(SystemTime::now());
        detail.progress_percentage = 0;
        
        self.current_step = format!("Migration {} failed: {}", version, error);
    }
    
    pub fn complete(&mut self) {
        self.progress_percentage = 100;
        self.current_step = "Migration completed".to_string();
        self.current_migration = None;
        self.estimated_completion = Some(SystemTime::now());
    }
    
    fn calculate_overall_progress(&mut self) {
        if self.total_migrations == 0 {
            self.progress_percentage = 0;
            return;
        }
        
        let overall_progress = (self.completed_migrations as f64 / self.total_migrations as f64) * 100.0;
        
        if let Some(current_version) = self.current_migration {
            if let Some(current_detail) = self.migration_details.get(&current_version) {
                let current_contribution = (current_detail.progress_percentage as f64 / 100.0) 
                    * (1.0 / self.total_migrations as f64) * 100.0;
                self.progress_percentage = (overall_progress + current_contribution).min(100.0) as u8;
            } else {
                self.progress_percentage = overall_progress as u8;
            }
        } else {
            self.progress_percentage = overall_progress as u8;
        }
    }
    
    fn update_estimated_completion(&mut self) {
        if let Some(start_time) = self.started_at {
            let elapsed = SystemTime::now()
                .duration_since(start_time)
                .unwrap_or_default();
            
            if self.progress_percentage > 0 {
                let total_estimated = elapsed.mul_f64(100.0 / self.progress_percentage as f64);
                self.estimated_completion = Some(start_time + total_estimated);
            }
        }
    }
    
    pub fn get_elapsed_time(&self) -> Option<Duration> {
        self.started_at.and_then(|start| {
            SystemTime::now().duration_since(start).ok()
        })
    }
    
    pub fn get_remaining_time(&self) -> Option<Duration> {
        self.estimated_completion.and_then(|completion| {
            completion.duration_since(SystemTime::now()).ok()
        })
    }
    
    pub fn get_migration_detail(&self, version: MigrationVersion) -> Option<&MigrationProgressDetail> {
        self.migration_details.get(&version)
    }
    
    pub fn is_complete(&self) -> bool {
        self.progress_percentage == 100
    }
    
    pub fn has_errors(&self) -> bool {
        self.migration_details.values()
            .any(|detail| detail.status == MigrationStatus::Failed)
    }
    
    pub fn get_failed_migrations(&self) -> Vec<&MigrationProgressDetail> {
        self.migration_details.values()
            .filter(|detail| detail.status == MigrationStatus::Failed)
            .collect()
    }
}

impl Default for MigrationProgress {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgressDetail {
    pub version: MigrationVersion,
    pub status: MigrationStatus,
    pub current_step: String,
    pub progress_percentage: u8,
    pub started_at: SystemTime,
    pub updated_at: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub error_message: Option<String>,
    pub estimated_duration: Option<Duration>,
}

impl MigrationProgressDetail {
    pub fn new(version: MigrationVersion) -> Self {
        let now = SystemTime::now();
        Self {
            version,
            status: MigrationStatus::Pending,
            current_step: "Pending".to_string(),
            progress_percentage: 0,
            started_at: now,
            updated_at: now,
            completed_at: None,
            error_message: None,
            estimated_duration: None,
        }
    }
    
    pub fn get_duration(&self) -> Duration {
        let end_time = self.completed_at.unwrap_or_else(SystemTime::now);
        end_time.duration_since(self.started_at).unwrap_or_default()
    }
    
    pub fn is_running(&self) -> bool {
        matches!(self.status, MigrationStatus::Running)
    }
    
    pub fn is_completed(&self) -> bool {
        matches!(self.status, MigrationStatus::Completed)
    }
    
    pub fn is_failed(&self) -> bool {
        matches!(self.status, MigrationStatus::Failed)
    }
}

pub struct ProgressTracker {
    progress: Arc<RwLock<MigrationProgress>>,
    update_interval: Duration,
    last_update: Instant,
}

impl ProgressTracker {
    pub fn new(progress: Arc<RwLock<MigrationProgress>>) -> Self {
        Self {
            progress,
            update_interval: Duration::from_millis(100),
            last_update: Instant::now(),
        }
    }
    
    pub fn with_update_interval(mut self, interval: Duration) -> Self {
        self.update_interval = interval;
        self
    }
    
    pub fn start_migration_batch(&self, total_migrations: usize) {
        let mut progress = self.progress.write();
        progress.start(total_migrations);
    }
    
    pub fn update_migration(
        &mut self,
        version: MigrationVersion,
        step: &str,
        percentage: u8,
    ) {
        if self.should_update() {
            let mut progress = self.progress.write();
            progress.update_migration_progress(version, step, percentage);
            self.last_update = Instant::now();
        }
    }
    
    pub fn mark_migration_failed(&self, version: MigrationVersion, error: &str) {
        let mut progress = self.progress.write();
        progress.mark_migration_failed(version, error);
    }
    
    pub fn complete_migration_batch(&self) {
        let mut progress = self.progress.write();
        progress.complete();
    }
    
    pub fn get_progress_snapshot(&self) -> MigrationProgress {
        let progress = self.progress.read();
        progress.clone()
    }
    
    fn should_update(&self) -> bool {
        self.last_update.elapsed() >= self.update_interval
    }
}

pub struct ProgressReporter;

impl ProgressReporter {
    pub fn generate_progress_report(progress: &MigrationProgress) -> ProgressReport {
        let elapsed = progress.get_elapsed_time();
        let remaining = progress.get_remaining_time();
        let failed_migrations = progress.get_failed_migrations();
        
        ProgressReport {
            overall_progress: progress.progress_percentage,
            total_migrations: progress.total_migrations,
            completed_migrations: progress.completed_migrations,
            current_migration: progress.current_migration,
            current_step: progress.current_step.clone(),
            elapsed_time: elapsed,
            estimated_remaining: remaining,
            has_errors: progress.has_errors(),
            failed_count: failed_migrations.len(),
            migration_details: progress.migration_details.clone(),
        }
    }
    
    pub fn format_progress_summary(progress: &MigrationProgress) -> String {
        let percentage = progress.progress_percentage;
        let completed = progress.completed_migrations;
        let total = progress.total_migrations;
        
        let elapsed_str = progress.get_elapsed_time()
            .map(|d| format!("{:.1}s", d.as_secs_f64()))
            .unwrap_or_else(|| "0s".to_string());
        
        let remaining_str = progress.get_remaining_time()
            .map(|d| format!("{:.1}s", d.as_secs_f64()))
            .unwrap_or_else(|| "unknown".to_string());
        
        format!(
            "Progress: {}% ({}/{}) - Elapsed: {} - Remaining: {} - Step: {}",
            percentage,
            completed,
            total,
            elapsed_str,
            remaining_str,
            progress.current_step
        )
    }
    
    pub fn format_detailed_report(progress: &MigrationProgress) -> String {
        let mut report = String::new();
        
        report.push_str(&format!("Migration Progress Report\n"));
        report.push_str(&format!("========================\n\n"));
        
        report.push_str(&format!("Overall Progress: {}%\n", progress.progress_percentage));
        report.push_str(&format!("Completed: {}/{}\n", progress.completed_migrations, progress.total_migrations));
        report.push_str(&format!("Current Step: {}\n", progress.current_step));
        
        if let Some(elapsed) = progress.get_elapsed_time() {
            report.push_str(&format!("Elapsed Time: {:.1}s\n", elapsed.as_secs_f64()));
        }
        
        if let Some(remaining) = progress.get_remaining_time() {
            report.push_str(&format!("Estimated Remaining: {:.1}s\n", remaining.as_secs_f64()));
        }
        
        report.push_str("\nMigration Details:\n");
        report.push_str("------------------\n");
        
        let mut sorted_details: Vec<_> = progress.migration_details.values().collect();
        sorted_details.sort_by_key(|detail| detail.version);
        
        for detail in sorted_details {
            report.push_str(&format!(
                "  {} - {:?} ({}%) - {}\n",
                detail.version,
                detail.status,
                detail.progress_percentage,
                detail.current_step
            ));
            
            if let Some(ref error) = detail.error_message {
                report.push_str(&format!("    Error: {}\n", error));
            }
        }
        
        if progress.has_errors() {
            let failed = progress.get_failed_migrations();
            report.push_str(&format!("\nFailed Migrations ({}): \n", failed.len()));
            for detail in failed {
                report.push_str(&format!("  {} - {}\n", detail.version, 
                    detail.error_message.as_deref().unwrap_or("Unknown error")));
            }
        }
        
        report
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressReport {
    pub overall_progress: u8,
    pub total_migrations: usize,
    pub completed_migrations: usize,
    pub current_migration: Option<MigrationVersion>,
    pub current_step: String,
    pub elapsed_time: Option<Duration>,
    pub estimated_remaining: Option<Duration>,
    pub has_errors: bool,
    pub failed_count: usize,
    pub migration_details: HashMap<MigrationVersion, MigrationProgressDetail>,
}

impl ProgressReport {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
    
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}