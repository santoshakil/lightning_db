//! Command History Management for Lightning DB REPL
//!
//! Provides persistent command history with search, filtering,
//! and management capabilities for the interactive REPL.

use crate::{Result, Error};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write, BufWriter};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use tracing::{debug, warn};

/// Command history manager
pub struct HistoryManager {
    /// History entries
    entries: VecDeque<HistoryEntry>,
    /// Maximum number of entries to keep
    max_entries: usize,
    /// History file path
    history_file: Option<PathBuf>,
    /// Configuration
    config: HistoryConfig,
    /// Statistics
    stats: HistoryStats,
}

/// Configuration for history management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryConfig {
    /// Enable history persistence
    pub enable_persistence: bool,
    /// Enable history deduplication
    pub enable_deduplication: bool,
    /// Enable history search
    pub enable_search: bool,
    /// Minimum command length to save
    pub min_command_length: usize,
    /// Commands to exclude from history
    pub excluded_commands: Vec<String>,
    /// History file rotation size (in entries)
    pub rotation_size: usize,
    /// Enable timestamps
    pub enable_timestamps: bool,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            enable_persistence: true,
            enable_deduplication: true,
            enable_search: true,
            min_command_length: 1,
            excluded_commands: vec![
                "help".to_string(),
                "history".to_string(),
                "clear".to_string(),
            ],
            rotation_size: 10000,
            enable_timestamps: true,
        }
    }
}

/// History entry with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Command text
    pub command: String,
    /// Timestamp when command was executed
    pub timestamp: SystemTime,
    /// Execution duration (if available)
    pub duration: Option<Duration>,
    /// Success status
    pub success: Option<bool>,
    /// Session ID
    pub session_id: Option<String>,
    /// Entry ID
    pub id: u64,
}

/// History search result
#[derive(Debug, Clone)]
pub struct HistorySearchResult {
    /// Matching entries
    pub entries: Vec<HistoryEntry>,
    /// Total number of matches
    pub total_matches: usize,
    /// Search query
    pub query: String,
    /// Search execution time
    pub search_time: Duration,
}

/// History statistics
#[derive(Debug, Clone, Serialize)]
pub struct HistoryStats {
    /// Total entries added
    pub total_entries_added: u64,
    /// Current number of entries
    pub current_entries: usize,
    /// Most frequent commands
    pub most_frequent_commands: Vec<(String, u64)>,
    /// Average command length
    pub average_command_length: f64,
    /// History file size (if applicable)
    pub history_file_size: Option<u64>,
    /// Last save time
    pub last_save_time: Option<SystemTime>,
}

impl HistoryManager {
    /// Create a new history manager
    pub fn new(max_entries: usize, history_file: Option<String>) -> Result<Self> {
        let history_file = history_file.map(PathBuf::from);
        
        let mut manager = Self {
            entries: VecDeque::new(),
            max_entries,
            history_file,
            config: HistoryConfig::default(),
            stats: HistoryStats {
                total_entries_added: 0,
                current_entries: 0,
                most_frequent_commands: Vec::new(),
                average_command_length: 0.0,
                history_file_size: None,
                last_save_time: None,
            },
        };

        // Load existing history if available
        if manager.config.enable_persistence {
            if let Err(e) = manager.load_history() {
                warn!("Failed to load history: {}", e);
            }
        }

        Ok(manager)
    }

    /// Create history manager with custom configuration
    pub fn with_config(
        max_entries: usize,
        history_file: Option<String>,
        config: HistoryConfig,
    ) -> Result<Self> {
        let history_file = history_file.map(PathBuf::from);
        
        let mut manager = Self {
            entries: VecDeque::new(),
            max_entries,
            history_file,
            config,
            stats: HistoryStats {
                total_entries_added: 0,
                current_entries: 0,
                most_frequent_commands: Vec::new(),
                average_command_length: 0.0,
                history_file_size: None,
                last_save_time: None,
            },
        };

        // Load existing history if available
        if manager.config.enable_persistence {
            if let Err(e) = manager.load_history() {
                warn!("Failed to load history: {}", e);
            }
        }

        Ok(manager)
    }

    /// Add a new command to history
    pub fn add_entry(&mut self, command: String) -> Result<()> {
        self.add_detailed_entry(command, None, None, None)
    }

    /// Add a detailed entry to history
    pub fn add_detailed_entry(
        &mut self,
        command: String,
        duration: Option<Duration>,
        success: Option<bool>,
        session_id: Option<String>,
    ) -> Result<()> {
        // Filter out commands based on configuration
        if !self.should_save_command(&command) {
            return Ok(());
        }

        let entry = HistoryEntry {
            command: command.clone(),
            timestamp: SystemTime::now(),
            duration,
            success,
            session_id,
            id: self.stats.total_entries_added + 1,
        };

        // Check for deduplication
        if self.config.enable_deduplication {
            if let Some(last_entry) = self.entries.back() {
                if last_entry.command == command {
                    debug!("Skipping duplicate command: {}", command);
                    return Ok(());
                }
            }
        }

        // Add to entries
        self.entries.push_back(entry);
        self.stats.total_entries_added += 1;

        // Maintain size limit
        if self.entries.len() > self.max_entries {
            self.entries.pop_front();
        }

        // Update statistics
        self.update_stats();

        debug!("Added command to history: {}", command);
        Ok(())
    }

    /// Get recent entries
    pub fn get_recent_entries(&self, count: usize) -> Vec<&HistoryEntry> {
        self.entries
            .iter()
            .rev()
            .take(count)
            .collect()
    }

    /// Get all entries
    pub fn get_all_entries(&self) -> Vec<&HistoryEntry> {
        self.entries.iter().collect()
    }

    /// Search history entries
    pub fn search(&self, query: &str, limit: Option<usize>) -> HistorySearchResult {
        let start_time = std::time::Instant::now();
        
        if !self.config.enable_search {
            return HistorySearchResult {
                entries: Vec::new(),
                total_matches: 0,
                query: query.to_string(),
                search_time: start_time.elapsed(),
            };
        }

        let query_lower = query.to_lowercase();
        let mut matches = Vec::new();

        for entry in self.entries.iter().rev() {
            if entry.command.to_lowercase().contains(&query_lower) {
                matches.push(entry.clone());
                
                if let Some(limit) = limit {
                    if matches.len() >= limit {
                        break;
                    }
                }
            }
        }

        let total_matches = matches.len();
        
        HistorySearchResult {
            entries: matches,
            total_matches,
            query: query.to_string(),
            search_time: start_time.elapsed(),
        }
    }

    /// Search by pattern with more advanced matching
    pub fn search_pattern(&self, pattern: &str, limit: Option<usize>) -> HistorySearchResult {
        let start_time = std::time::Instant::now();
        
        let mut matches = Vec::new();
        
        // Simple pattern matching - could be extended with regex
        for entry in self.entries.iter().rev() {
            if self.matches_pattern(&entry.command, pattern) {
                matches.push(entry.clone());
                
                if let Some(limit) = limit {
                    if matches.len() >= limit {
                        break;
                    }
                }
            }
        }

        let total_matches = matches.len();
        
        HistorySearchResult {
            entries: matches,
            total_matches,
            query: pattern.to_string(),
            search_time: start_time.elapsed(),
        }
    }

    /// Get entries by time range
    pub fn get_entries_by_time_range(
        &self,
        start_time: SystemTime,
        end_time: SystemTime,
    ) -> Vec<&HistoryEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.timestamp >= start_time && entry.timestamp <= end_time)
            .collect()
    }

    /// Get entries by session
    pub fn get_entries_by_session(&self, session_id: &str) -> Vec<&HistoryEntry> {
        self.entries
            .iter()
            .filter(|entry| {
                entry.session_id
                    .as_ref()
                    .map(|id| id == session_id)
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Load history from file
    pub fn load_history(&mut self) -> Result<()> {
        if let Some(history_file) = &self.history_file {
            if !history_file.exists() {
                debug!("History file does not exist: {:?}", history_file);
                return Ok(());
            }

            let file = File::open(history_file)
                .map_err(|e| Error::Io(format!("Failed to open history file: {}", e)))?;
            let reader = BufReader::new(file);

            let mut loaded_count = 0;
            for line in reader.lines() {
                let line = line.map_err(|e| Error::Io(format!("Failed to read history line: {}", e)))?;
                
                if let Ok(entry) = self.parse_history_line(&line) {
                    self.entries.push_back(entry);
                    loaded_count += 1;
                    
                    // Respect size limit while loading
                    if self.entries.len() > self.max_entries {
                        self.entries.pop_front();
                    }
                }
            }

            debug!("Loaded {} history entries from {:?}", loaded_count, history_file);
            self.update_stats();
        }

        Ok(())
    }

    /// Save history to file
    pub fn save_history(&mut self) -> Result<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }

        if let Some(history_file) = &self.history_file {
            // Create parent directories if they don't exist
            if let Some(parent) = history_file.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| Error::Io(format!("Failed to create history directory: {}", e)))?;
            }

            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(history_file)
                .map_err(|e| Error::Io(format!("Failed to open history file for writing: {}", e)))?;

            let mut writer = BufWriter::new(file);

            for entry in &self.entries {
                let line = self.format_history_line(entry)?;
                writeln!(writer, "{}", line)
                    .map_err(|e| Error::Io(format!("Failed to write history entry: {}", e)))?;
            }

            writer.flush()
                .map_err(|e| Error::Io(format!("Failed to flush history file: {}", e)))?;

            self.stats.last_save_time = Some(SystemTime::now());
            debug!("Saved {} history entries to {:?}", self.entries.len(), history_file);
        }

        Ok(())
    }

    /// Clear all history
    pub fn clear_history(&mut self) -> Result<()> {
        self.entries.clear();
        self.update_stats();
        
        // Clear history file if it exists
        if self.config.enable_persistence {
            if let Some(history_file) = &self.history_file {
                if history_file.exists() {
                    std::fs::remove_file(history_file)
                        .map_err(|e| Error::Io(format!("Failed to remove history file: {}", e)))?;
                }
            }
        }

        debug!("Cleared all history entries");
        Ok(())
    }

    /// Remove entry by ID
    pub fn remove_entry(&mut self, entry_id: u64) -> Result<bool> {
        let initial_len = self.entries.len();
        self.entries.retain(|entry| entry.id != entry_id);
        
        let removed = self.entries.len() < initial_len;
        if removed {
            self.update_stats();
            debug!("Removed history entry with ID: {}", entry_id);
        }
        
        Ok(removed)
    }

    /// Get the last executed command
    pub fn get_last_command(&self) -> Option<&HistoryEntry> {
        self.entries.back()
    }

    /// Get the nth previous command (0 = last, 1 = second to last, etc.)
    pub fn get_previous_command(&self, n: usize) -> Option<&HistoryEntry> {
        if n >= self.entries.len() {
            return None;
        }
        
        self.entries.iter().rev().nth(n)
    }

    /// Get statistics
    pub fn get_stats(&self) -> &HistoryStats {
        &self.stats
    }

    /// Export history to different formats
    pub fn export_history(&self, format: HistoryExportFormat) -> Result<String> {
        match format {
            HistoryExportFormat::Json => {
                serde_json::to_string_pretty(&self.entries)
                    .map_err(|e| Error::Serialization(format!("Failed to serialize history: {}", e)))
            }
            HistoryExportFormat::Csv => {
                let mut output = String::from("id,command,timestamp,duration_ms,success,session_id\n");
                
                for entry in &self.entries {
                    let timestamp = entry.timestamp
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    
                    let duration_ms = entry.duration
                        .map(|d| d.as_millis().to_string())
                        .unwrap_or_default();
                    
                    let success = entry.success
                        .map(|s| s.to_string())
                        .unwrap_or_default();
                    
                    let session_id = entry.session_id
                        .as_deref()
                        .unwrap_or("");
                    
                    output.push_str(&format!(
                        "{},\"{}\",{},{},{},\"{}\"\n",
                        entry.id,
                        entry.command.replace('"', "\"\""), // Escape quotes
                        timestamp,
                        duration_ms,
                        success,
                        session_id
                    ));
                }
                
                Ok(output)
            }
            HistoryExportFormat::Plain => {
                Ok(self.entries
                    .iter()
                    .map(|entry| entry.command.clone())
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
        }
    }

    /// Import history from different formats
    pub fn import_history(&mut self, data: &str, format: HistoryExportFormat) -> Result<usize> {
        let entries = match format {
            HistoryExportFormat::Json => {
                serde_json::from_str::<Vec<HistoryEntry>>(data)
                    .map_err(|e| Error::Serialization(format!("Failed to deserialize history: {}", e)))?
            }
            HistoryExportFormat::Plain => {
                data.lines()
                    .enumerate()
                    .map(|(i, line)| HistoryEntry {
                        command: line.to_string(),
                        timestamp: SystemTime::now(),
                        duration: None,
                        success: None,
                        session_id: None,
                        id: self.stats.total_entries_added + i as u64 + 1,
                    })
                    .collect()
            }
            HistoryExportFormat::Csv => {
                return Err(Error::Generic("CSV import not yet implemented".to_string()));
            }
        };

        let imported_count = entries.len();
        
        for entry in entries {
            self.entries.push_back(entry);
            self.stats.total_entries_added += 1;
            
            // Maintain size limit
            if self.entries.len() > self.max_entries {
                self.entries.pop_front();
            }
        }

        self.update_stats();
        debug!("Imported {} history entries", imported_count);
        
        Ok(imported_count)
    }

    // Private helper methods

    /// Check if a command should be saved to history
    fn should_save_command(&self, command: &str) -> bool {
        // Check minimum length
        if command.len() < self.config.min_command_length {
            return false;
        }

        // Check excluded commands
        let command_lower = command.to_lowercase();
        for excluded in &self.config.excluded_commands {
            if command_lower.starts_with(&excluded.to_lowercase()) {
                return false;
            }
        }

        true
    }

    /// Update internal statistics
    fn update_stats(&mut self) {
        self.stats.current_entries = self.entries.len();
        
        // Calculate average command length
        if !self.entries.is_empty() {
            let total_length: usize = self.entries.iter().map(|e| e.command.len()).sum();
            self.stats.average_command_length = total_length as f64 / self.entries.len() as f64;
        }

        // Calculate most frequent commands
        let mut command_counts = std::collections::HashMap::new();
        for entry in &self.entries {
            let command = entry.command.split_whitespace().next().unwrap_or(&entry.command);
            *command_counts.entry(command.to_string()).or_insert(0) += 1;
        }

        let mut most_frequent: Vec<_> = command_counts.into_iter().collect();
        most_frequent.sort_by(|a, b| b.1.cmp(&a.1));
        self.stats.most_frequent_commands = most_frequent.into_iter().take(10).collect();

        // Update file size if file exists
        if let Some(history_file) = &self.history_file {
            if let Ok(metadata) = std::fs::metadata(history_file) {
                self.stats.history_file_size = Some(metadata.len());
            }
        }
    }

    /// Parse a history line from file
    fn parse_history_line(&self, line: &str) -> Result<HistoryEntry> {
        // Try JSON format first
        if line.starts_with('{') {
            return serde_json::from_str(line)
                .map_err(|e| Error::Serialization(format!("Failed to parse history entry: {}", e)));
        }

        // Fall back to simple format: timestamp:command
        if let Some(colon_pos) = line.find(':') {
            let timestamp_str = &line[..colon_pos];
            let command = &line[colon_pos + 1..];
            
            let timestamp_secs = timestamp_str.parse::<u64>()
                .map_err(|_| Error::Generic("Invalid timestamp in history".to_string()))?;
            
            let timestamp = UNIX_EPOCH + Duration::from_secs(timestamp_secs);
            
            Ok(HistoryEntry {
                command: command.to_string(),
                timestamp,
                duration: None,
                success: None,
                session_id: None,
                id: self.stats.total_entries_added + 1,
            })
        } else {
            // Just treat the whole line as a command
            Ok(HistoryEntry {
                command: line.to_string(),
                timestamp: SystemTime::now(),
                duration: None,
                success: None,
                session_id: None,
                id: self.stats.total_entries_added + 1,
            })
        }
    }

    /// Format a history entry for saving to file
    fn format_history_line(&self, entry: &HistoryEntry) -> Result<String> {
        if self.config.enable_timestamps {
            // Use JSON format for full metadata
            serde_json::to_string(entry)
                .map_err(|e| Error::Serialization(format!("Failed to serialize history entry: {}", e)))
        } else {
            // Simple format for minimal storage
            let timestamp_secs = entry.timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Ok(format!("{}:{}", timestamp_secs, entry.command))
        }
    }

    /// Check if command matches pattern
    fn matches_pattern(&self, command: &str, pattern: &str) -> bool {
        // Simple pattern matching - could be extended with proper glob/regex
        if pattern.contains('*') {
            // Basic wildcard support
            let pattern_parts: Vec<&str> = pattern.split('*').collect();
            if pattern_parts.len() == 2 {
                let prefix = pattern_parts[0];
                let suffix = pattern_parts[1];
                return command.starts_with(prefix) && command.ends_with(suffix);
            }
        }
        
        // Default to substring match
        command.to_lowercase().contains(&pattern.to_lowercase())
    }
}

/// History export formats
#[derive(Debug, Clone)]
pub enum HistoryExportFormat {
    Json,
    Csv,
    Plain,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_history_manager_creation() {
        let manager = HistoryManager::new(100, None).unwrap();
        assert_eq!(manager.entries.len(), 0);
        assert_eq!(manager.max_entries, 100);
    }

    #[test]
    fn test_add_entry() {
        let mut manager = HistoryManager::new(100, None).unwrap();
        
        manager.add_entry("GET key1".to_string()).unwrap();
        manager.add_entry("PUT key2 value2".to_string()).unwrap();
        
        assert_eq!(manager.entries.len(), 2);
        assert_eq!(manager.get_last_command().unwrap().command, "PUT key2 value2");
    }

    #[test]
    fn test_deduplication() {
        let mut config = HistoryConfig::default();
        config.enable_deduplication = true;
        
        let mut manager = HistoryManager::with_config(100, None, config).unwrap();
        
        manager.add_entry("GET key1".to_string()).unwrap();
        manager.add_entry("GET key1".to_string()).unwrap(); // Duplicate
        manager.add_entry("PUT key2 value2".to_string()).unwrap();
        
        assert_eq!(manager.entries.len(), 2);
    }

    #[test]
    fn test_search() {
        let mut manager = HistoryManager::new(100, None).unwrap();
        
        manager.add_entry("GET user:1".to_string()).unwrap();
        manager.add_entry("PUT user:2 data".to_string()).unwrap();
        manager.add_entry("DELETE config".to_string()).unwrap();
        
        let results = manager.search("user", None);
        assert_eq!(results.entries.len(), 2);
        
        let results = manager.search("DELETE", None);
        assert_eq!(results.entries.len(), 1);
    }

    #[test]
    fn test_max_entries_limit() {
        let mut manager = HistoryManager::new(2, None).unwrap();
        
        manager.add_entry("command1".to_string()).unwrap();
        manager.add_entry("command2".to_string()).unwrap();
        manager.add_entry("command3".to_string()).unwrap(); // Should evict command1
        
        assert_eq!(manager.entries.len(), 2);
        assert_eq!(manager.entries[0].command, "command2");
        assert_eq!(manager.entries[1].command, "command3");
    }

    #[test]
    fn test_export_plain_format() {
        let mut manager = HistoryManager::new(100, None).unwrap();
        
        manager.add_entry("GET key1".to_string()).unwrap();
        manager.add_entry("PUT key2 value2".to_string()).unwrap();
        
        let exported = manager.export_history(HistoryExportFormat::Plain).unwrap();
        assert!(exported.contains("GET key1"));
        assert!(exported.contains("PUT key2 value2"));
    }

    #[test]
    fn test_pattern_matching() {
        let manager = HistoryManager::new(100, None).unwrap();
        
        assert!(manager.matches_pattern("GET user:1", "GET*"));
        assert!(manager.matches_pattern("PUT user:2 data", "*user*"));
        assert!(!manager.matches_pattern("DELETE config", "GET*"));
    }

    #[test]
    fn test_time_range_search() {
        let mut manager = HistoryManager::new(100, None).unwrap();
        
        let start_time = SystemTime::now();
        
        manager.add_entry("command1".to_string()).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        
        let mid_time = SystemTime::now();
        
        manager.add_entry("command2".to_string()).unwrap();
        
        let _end_time = SystemTime::now();
        
        let results = manager.get_entries_by_time_range(start_time, mid_time);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].command, "command1");
    }

    #[test]
    fn test_file_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let history_file = temp_file.path().to_str().unwrap().to_string();
        
        // Create manager and add entries
        {
            let mut manager = HistoryManager::new(100, Some(history_file.clone())).unwrap();
            manager.add_entry("GET key1".to_string()).unwrap();
            manager.add_entry("PUT key2 value2".to_string()).unwrap();
            manager.save_history().unwrap();
        }
        
        // Load from file in new manager
        {
            let manager = HistoryManager::new(100, Some(history_file)).unwrap();
            assert_eq!(manager.entries.len(), 2);
            assert_eq!(manager.entries[0].command, "GET key1");
            assert_eq!(manager.entries[1].command, "PUT key2 value2");
        }
    }
}