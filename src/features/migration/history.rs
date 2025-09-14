use super::{MigrationStatus, MigrationVersion};
use crate::core::error::{DatabaseError, DatabaseResult};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::Path,
    time::{Duration, SystemTime},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationHistoryEntry {
    pub version: MigrationVersion,
    pub name: String,
    pub status: MigrationStatus,
    pub applied_at: Option<SystemTime>,
    pub rolled_back_at: Option<SystemTime>,
    pub duration: Option<Duration>,
    pub checksum: String,
    pub error_message: Option<String>,
    pub applied_by: String,
}

impl MigrationHistoryEntry {
    pub fn new(
        version: MigrationVersion,
        name: String,
        checksum: String,
        applied_by: String,
    ) -> Self {
        Self {
            version,
            name,
            status: MigrationStatus::Pending,
            applied_at: None,
            rolled_back_at: None,
            duration: None,
            checksum,
            error_message: None,
            applied_by,
        }
    }

    pub fn mark_success(&mut self, duration: Duration) {
        self.status = MigrationStatus::Completed;
        self.applied_at = Some(SystemTime::now());
        self.duration = Some(duration);
        self.error_message = None;
    }

    pub fn mark_failure(&mut self, duration: Duration, error: &str) {
        self.status = MigrationStatus::Failed;
        self.applied_at = Some(SystemTime::now());
        self.duration = Some(duration);
        self.error_message = Some(error.to_string());
    }

    pub fn mark_rolled_back(&mut self, duration: Duration) {
        self.status = MigrationStatus::RolledBack;
        self.rolled_back_at = Some(SystemTime::now());
        self.duration = Some(duration);
    }

    pub fn is_applied(&self) -> bool {
        matches!(self.status, MigrationStatus::Completed)
    }

    pub fn is_failed(&self) -> bool {
        matches!(self.status, MigrationStatus::Failed)
    }

    pub fn is_rolled_back(&self) -> bool {
        matches!(self.status, MigrationStatus::RolledBack)
    }
}

#[derive(Debug)]
pub struct HistoryManager {
    history_cache: std::sync::Arc<parking_lot::RwLock<HashMap<String, Vec<MigrationHistoryEntry>>>>,
}

impl Default for HistoryManager {
    fn default() -> Self {
        Self::new()
    }
}

impl HistoryManager {
    pub fn new() -> Self {
        Self {
            history_cache: std::sync::Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    pub fn get_history(&self, db_path: &str) -> DatabaseResult<Vec<MigrationHistoryEntry>> {
        {
            let cache = self.history_cache.read();
            if let Some(history) = cache.get(db_path) {
                return Ok(history.clone());
            }
        }

        let history = self.load_history_from_disk(db_path)?;

        {
            let mut cache = self.history_cache.write();
            cache.insert(db_path.to_string(), history.clone());
        }

        Ok(history)
    }

    pub fn get_current_version(&self, db_path: &str) -> DatabaseResult<MigrationVersion> {
        let history = self.get_history(db_path)?;

        let current_version = history
            .iter()
            .filter(|entry| entry.is_applied())
            .map(|entry| entry.version)
            .max()
            .unwrap_or(MigrationVersion::INITIAL);

        Ok(current_version)
    }

    pub fn get_migration_statuses(
        &self,
        db_path: &str,
    ) -> DatabaseResult<HashMap<MigrationVersion, MigrationStatus>> {
        let history = self.get_history(db_path)?;

        let statuses: HashMap<_, _> = history
            .iter()
            .map(|entry| (entry.version, entry.status.clone()))
            .collect();

        Ok(statuses)
    }

    pub fn record_migration_success(
        &self,
        db_path: &str,
        version: MigrationVersion,
        duration: Duration,
    ) -> DatabaseResult<()> {
        self.update_migration_entry(db_path, version, |entry| {
            entry.mark_success(duration);
        })
    }

    pub fn record_migration_failure(
        &self,
        db_path: &str,
        version: MigrationVersion,
        duration: Duration,
        error: &str,
    ) -> DatabaseResult<()> {
        self.update_migration_entry(db_path, version, |entry| {
            entry.mark_failure(duration, error);
        })
    }

    pub fn record_rollback_success(
        &self,
        db_path: &str,
        version: MigrationVersion,
        duration: Duration,
    ) -> DatabaseResult<()> {
        self.update_migration_entry(db_path, version, |entry| {
            entry.mark_rolled_back(duration);
        })
    }

    pub fn record_rollback_failure(
        &self,
        db_path: &str,
        version: MigrationVersion,
        duration: Duration,
        error: &str,
    ) -> DatabaseResult<()> {
        self.update_migration_entry(db_path, version, |entry| {
            entry.mark_failure(duration, error);
        })
    }

    pub fn add_migration_entry(
        &self,
        db_path: &str,
        entry: MigrationHistoryEntry,
    ) -> DatabaseResult<()> {
        let mut history = self.get_history(db_path)?;

        if history.iter().any(|e| e.version == entry.version) {
            return Err(DatabaseError::Migration(format!(
                "Migration {} already exists in history",
                entry.version
            )));
        }

        history.push(entry);
        history.sort_by_key(|e| e.version);

        self.save_history_to_disk(db_path, &history)?;

        {
            let mut cache = self.history_cache.write();
            cache.insert(db_path.to_string(), history);
        }

        Ok(())
    }

    pub fn remove_migration_entry(
        &self,
        db_path: &str,
        version: MigrationVersion,
    ) -> DatabaseResult<()> {
        let mut history = self.get_history(db_path)?;

        history.retain(|e| e.version != version);

        self.save_history_to_disk(db_path, &history)?;

        {
            let mut cache = self.history_cache.write();
            cache.insert(db_path.to_string(), history);
        }

        Ok(())
    }

    pub fn get_migration_entry(
        &self,
        db_path: &str,
        version: MigrationVersion,
    ) -> DatabaseResult<Option<MigrationHistoryEntry>> {
        let history = self.get_history(db_path)?;

        let entry = history.iter().find(|e| e.version == version).cloned();

        Ok(entry)
    }

    pub fn get_applied_migrations(
        &self,
        db_path: &str,
    ) -> DatabaseResult<Vec<MigrationHistoryEntry>> {
        let history = self.get_history(db_path)?;

        let applied: Vec<_> = history
            .into_iter()
            .filter(|entry| entry.is_applied())
            .collect();

        Ok(applied)
    }

    pub fn get_failed_migrations(
        &self,
        db_path: &str,
    ) -> DatabaseResult<Vec<MigrationHistoryEntry>> {
        let history = self.get_history(db_path)?;

        let failed: Vec<_> = history
            .into_iter()
            .filter(|entry| entry.is_failed())
            .collect();

        Ok(failed)
    }

    pub fn get_migrations_since(
        &self,
        db_path: &str,
        since_version: MigrationVersion,
    ) -> DatabaseResult<Vec<MigrationHistoryEntry>> {
        let history = self.get_history(db_path)?;

        let since_migrations: Vec<_> = history
            .into_iter()
            .filter(|entry| entry.version > since_version)
            .collect();

        Ok(since_migrations)
    }

    pub fn get_migrations_between(
        &self,
        db_path: &str,
        from_version: MigrationVersion,
        to_version: MigrationVersion,
    ) -> DatabaseResult<Vec<MigrationHistoryEntry>> {
        let history = self.get_history(db_path)?;

        let between_migrations: Vec<_> = history
            .into_iter()
            .filter(|entry| entry.version > from_version && entry.version <= to_version)
            .collect();

        Ok(between_migrations)
    }

    pub fn clear_history(&self, db_path: &str) -> DatabaseResult<()> {
        let empty_history = Vec::new();
        self.save_history_to_disk(db_path, &empty_history)?;

        {
            let mut cache = self.history_cache.write();
            cache.insert(db_path.to_string(), empty_history);
        }

        Ok(())
    }

    pub fn export_history(&self, db_path: &str, export_path: &str) -> DatabaseResult<()> {
        let history = self.get_history(db_path)?;

        let json_content = serde_json::to_string_pretty(&history)
            .map_err(|e| DatabaseError::Serialization(e.to_string()))?;

        std::fs::write(export_path, json_content)
            .map_err(|e| DatabaseError::Io(e.to_string()))?;

        Ok(())
    }

    pub fn import_history(&self, db_path: &str, import_path: &str) -> DatabaseResult<()> {
        let json_content = std::fs::read_to_string(import_path)
            .map_err(|e| DatabaseError::Io(e.to_string()))?;

        let history: Vec<MigrationHistoryEntry> = serde_json::from_str(&json_content)
            .map_err(|e| DatabaseError::Serialization(e.to_string()))?;

        self.save_history_to_disk(db_path, &history)?;

        {
            let mut cache = self.history_cache.write();
            cache.insert(db_path.to_string(), history);
        }

        Ok(())
    }

    pub fn generate_history_report(&self, db_path: &str) -> DatabaseResult<HistoryReport> {
        let history = self.get_history(db_path)?;

        let total_migrations = history.len();
        let applied_count = history.iter().filter(|e| e.is_applied()).count();
        let failed_count = history.iter().filter(|e| e.is_failed()).count();
        let rolled_back_count = history.iter().filter(|e| e.is_rolled_back()).count();

        let total_duration = history
            .iter()
            .filter_map(|e| e.duration)
            .fold(Duration::ZERO, |acc, d| acc + d);

        let current_version = self.get_current_version(db_path)?;

        let first_migration = history
            .iter()
            .filter(|e| e.is_applied())
            .min_by_key(|e| e.version)
            .and_then(|e| e.applied_at);

        let last_migration = history
            .iter()
            .filter(|e| e.is_applied())
            .max_by_key(|e| e.version)
            .and_then(|e| e.applied_at);

        let recent_failures = history.iter().filter(|e| e.is_failed()).cloned().collect();

        Ok(HistoryReport {
            database_path: db_path.to_string(),
            current_version,
            total_migrations,
            applied_count,
            failed_count,
            rolled_back_count,
            total_duration,
            first_migration,
            last_migration,
            recent_failures,
        })
    }

    fn update_migration_entry<F>(
        &self,
        db_path: &str,
        version: MigrationVersion,
        update_fn: F,
    ) -> DatabaseResult<()>
    where
        F: FnOnce(&mut MigrationHistoryEntry),
    {
        let mut history = self.get_history(db_path)?;

        if let Some(entry) = history.iter_mut().find(|e| e.version == version) {
            update_fn(entry);
        } else {
            return Err(DatabaseError::Migration(format!(
                "Migration {} not found in history",
                version
            )));
        }

        self.save_history_to_disk(db_path, &history)?;

        {
            let mut cache = self.history_cache.write();
            cache.insert(db_path.to_string(), history);
        }

        Ok(())
    }

    fn load_history_from_disk(&self, db_path: &str) -> DatabaseResult<Vec<MigrationHistoryEntry>> {
        let history_file = self.get_history_file_path(db_path);

        if !Path::new(&history_file).exists() {
            return Ok(Vec::new());
        }

        let content = std::fs::read_to_string(&history_file)
            .map_err(|e| DatabaseError::Io(e.to_string()))?;

        if content.trim().is_empty() {
            return Ok(Vec::new());
        }

        let history: Vec<MigrationHistoryEntry> = serde_json::from_str(&content)
            .map_err(|e| DatabaseError::Serialization(e.to_string()))?;

        Ok(history)
    }

    fn save_history_to_disk(
        &self,
        db_path: &str,
        history: &[MigrationHistoryEntry],
    ) -> DatabaseResult<()> {
        let history_file = self.get_history_file_path(db_path);

        if let Some(parent) = Path::new(&history_file).parent() {
            std::fs::create_dir_all(parent).map_err(|e| DatabaseError::Io(e.to_string()))?;
        }

        let content = serde_json::to_string_pretty(history)
            .map_err(|e| DatabaseError::Serialization(e.to_string()))?;

        std::fs::write(&history_file, content)
            .map_err(|e| DatabaseError::Io(e.to_string()))?;

        Ok(())
    }

    fn get_history_file_path(&self, db_path: &str) -> String {
        format!("{}/migration_history.json", db_path)
    }

    pub fn invalidate_cache(&self, db_path: &str) {
        let mut cache = self.history_cache.write();
        cache.remove(db_path);
    }

    pub fn validate_history_integrity(&self, db_path: &str) -> DatabaseResult<Vec<String>> {
        let history = self.get_history(db_path)?;
        let mut issues = Vec::new();

        let mut seen_versions = std::collections::HashSet::new();
        for entry in &history {
            if seen_versions.contains(&entry.version) {
                issues.push(format!("Duplicate version: {}", entry.version));
            }
            seen_versions.insert(entry.version);
        }

        for window in history.windows(2) {
            if window[0].version >= window[1].version {
                issues.push(format!(
                    "Version order violation: {} should come before {}",
                    window[0].version, window[1].version
                ));
            }
        }

        for entry in &history {
            if entry.is_applied() && entry.applied_at.is_none() {
                issues.push(format!(
                    "Applied migration {} missing applied_at timestamp",
                    entry.version
                ));
            }

            if entry.is_failed() && entry.error_message.is_none() {
                issues.push(format!(
                    "Failed migration {} missing error message",
                    entry.version
                ));
            }
        }

        Ok(issues)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryReport {
    pub database_path: String,
    pub current_version: MigrationVersion,
    pub total_migrations: usize,
    pub applied_count: usize,
    pub failed_count: usize,
    pub rolled_back_count: usize,
    pub total_duration: Duration,
    pub first_migration: Option<SystemTime>,
    pub last_migration: Option<SystemTime>,
    pub recent_failures: Vec<MigrationHistoryEntry>,
}

impl HistoryReport {
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        let mut report = String::new();

        report.push_str("Migration History Report\n");
        report.push_str("=======================\n\n");

        report.push_str(&format!("Database: {}\n", self.database_path));
        report.push_str(&format!("Current Version: {}\n", self.current_version));
        report.push_str(&format!("Total Migrations: {}\n", self.total_migrations));
        report.push_str(&format!("Applied: {}\n", self.applied_count));
        report.push_str(&format!("Failed: {}\n", self.failed_count));
        report.push_str(&format!("Rolled Back: {}\n", self.rolled_back_count));
        report.push_str(&format!(
            "Total Duration: {:.2}s\n",
            self.total_duration.as_secs_f64()
        ));

        if let Some(first) = self.first_migration {
            report.push_str(&format!("First Migration: {:?}\n", first));
        }

        if let Some(last) = self.last_migration {
            report.push_str(&format!("Last Migration: {:?}\n", last));
        }

        if !self.recent_failures.is_empty() {
            report.push_str("\nRecent Failures:\n");
            for failure in &self.recent_failures {
                report.push_str(&format!(
                    "  {} - {}: {}\n",
                    failure.version,
                    failure.name,
                    failure.error_message.as_deref().unwrap_or("Unknown error")
                ));
            }
        }

        report
    }
}
