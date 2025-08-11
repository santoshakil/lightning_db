//! Configuration Management System for Lightning DB
//!
//! This module provides a comprehensive configuration management system
//! supporting multiple environments, hot-reloading, validation, and templates.

use crate::compression::CompressionType;
use crate::{Error, LightningDbConfig, Result, WalSyncMode};
#[cfg(feature = "file-watching")]
use notify::{Event, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, RwLock};

/// Configuration source types
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigSource {
    /// Configuration from file (JSON, YAML, TOML)
    File(PathBuf),
    /// Configuration from environment variables
    Environment,
    /// Configuration from command line arguments
    CommandLine(Vec<String>),
    /// Configuration from remote source (URL)
    Remote(String),
    /// Inline configuration
    Inline(String),
}

/// Configuration format
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConfigFormat {
    Json,
    Yaml,
    Toml,
    HCL,
}

impl ConfigFormat {
    /// Detect format from file extension
    pub fn from_extension(path: &Path) -> Option<Self> {
        match path.extension()?.to_str()? {
            "json" => Some(ConfigFormat::Json),
            "yaml" | "yml" => Some(ConfigFormat::Yaml),
            "toml" => Some(ConfigFormat::Toml),
            "hcl" => Some(ConfigFormat::HCL),
            _ => None,
        }
    }
}

/// Environment-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentConfig {
    /// Environment name (development, staging, production)
    pub name: String,
    /// Base configuration
    pub base: LightningDbConfig,
    /// Environment-specific overrides
    pub overrides: HashMap<String, serde_json::Value>,
    /// Feature flags
    pub features: HashMap<String, bool>,
    /// Resource limits
    pub limits: ResourceLimits,
}

/// Resource limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum memory usage in bytes
    pub max_memory_bytes: Option<u64>,
    /// Maximum CPU cores
    pub max_cpu_cores: Option<usize>,
    /// Maximum disk usage in bytes
    pub max_disk_bytes: Option<u64>,
    /// Maximum connections
    pub max_connections: Option<usize>,
    /// Rate limits
    pub rate_limits: HashMap<String, RateLimit>,
}

/// Rate limit configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    /// Requests per second
    pub requests_per_second: f64,
    /// Burst size
    pub burst: usize,
}

/// Configuration template
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigTemplate {
    /// Template name
    pub name: String,
    /// Template description
    pub description: String,
    /// Base configuration
    pub config: LightningDbConfig,
    /// Variables that can be customized
    pub variables: HashMap<String, TemplateVariable>,
}

/// Template variable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateVariable {
    /// Variable description
    pub description: String,
    /// Variable type
    pub var_type: String,
    /// Default value
    pub default: serde_json::Value,
    /// Validation rules
    pub validation: Option<ValidationRule>,
}

/// Validation rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRule {
    /// Minimum value (for numbers)
    pub min: Option<f64>,
    /// Maximum value (for numbers)
    pub max: Option<f64>,
    /// Allowed values (for enums)
    pub allowed_values: Option<Vec<String>>,
    /// Regular expression (for strings)
    pub pattern: Option<String>,
}

/// Configuration manager
pub struct ConfigManager {
    /// Current configuration
    current_config: Arc<RwLock<LightningDbConfig>>,
    /// Environment configurations
    environments: HashMap<String, EnvironmentConfig>,
    /// Configuration templates
    templates: HashMap<String, ConfigTemplate>,
    /// Configuration sources
    sources: Vec<ConfigSource>,
    /// Hot-reload watcher
    #[cfg(feature = "file-watching")]
    watcher: Option<notify::RecommendedWatcher>,
    #[cfg(not(feature = "file-watching"))]
    watcher: Option<()>,
    /// Configuration change callbacks
    change_callbacks: Arc<RwLock<Vec<Box<dyn Fn(&LightningDbConfig) + Send + Sync>>>>,
}

impl ConfigManager {
    /// Create new configuration manager
    pub fn new() -> Self {
        Self {
            current_config: Arc::new(RwLock::new(LightningDbConfig::default())),
            environments: HashMap::new(),
            templates: Self::load_default_templates(),
            sources: Vec::new(),
            watcher: None,
            change_callbacks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Load configuration from source
    pub fn load_from_source(&mut self, source: ConfigSource) -> Result<()> {
        let config = match &source {
            ConfigSource::File(path) => self.load_from_file(path)?,
            ConfigSource::Environment => self.load_from_env()?,
            ConfigSource::CommandLine(args) => self.load_from_args(args)?,
            ConfigSource::Remote(url) => self.load_from_remote(url)?,
            ConfigSource::Inline(content) => self.parse_config(content, ConfigFormat::Json)?,
        };

        self.sources.push(source);
        self.update_config(config)?;
        Ok(())
    }

    /// Load configuration from file
    fn load_from_file(&self, path: &Path) -> Result<LightningDbConfig> {
        let content = fs::read_to_string(path).map_err(|e| Error::Io(e.to_string()))?;

        let format = ConfigFormat::from_extension(path)
            .ok_or_else(|| Error::Config("Unknown configuration format".into()))?;

        self.parse_config(&content, format)
    }

    /// Parse configuration from string
    fn parse_config(&self, content: &str, format: ConfigFormat) -> Result<LightningDbConfig> {
        match format {
            ConfigFormat::Json => serde_json::from_str(content)
                .map_err(|e| Error::Config(format!("JSON parse error: {}", e))),
            #[cfg(feature = "config-formats")]
            ConfigFormat::Yaml => serde_yaml::from_str(content)
                .map_err(|e| Error::Config(format!("YAML parse error: {}", e))),
            #[cfg(feature = "config-formats")]
            ConfigFormat::Toml => toml::from_str(content)
                .map_err(|e| Error::Config(format!("TOML parse error: {}", e))),
            #[cfg(not(feature = "config-formats"))]
            ConfigFormat::Yaml => Err(Error::Config("YAML support requires 'config-formats' feature".into())),
            #[cfg(not(feature = "config-formats"))]
            ConfigFormat::Toml => Err(Error::Config("TOML support requires 'config-formats' feature".into())),
            ConfigFormat::HCL => {
                // HCL parsing would go here
                Err(Error::Config("HCL format not yet implemented".into()))
            }
        }
    }

    /// Load configuration from environment variables
    fn load_from_env(&self) -> Result<LightningDbConfig> {
        let mut config = LightningDbConfig::default();

        // Cache size
        if let Ok(cache_size) = std::env::var("LIGHTNING_DB_CACHE_SIZE") {
            config.cache_size = cache_size
                .parse()
                .map_err(|_| Error::Config("Invalid cache size".into()))?;
        }

        // Compression
        if let Ok(compression) = std::env::var("LIGHTNING_DB_COMPRESSION") {
            config.compression_enabled = compression
                .parse()
                .map_err(|_| Error::Config("Invalid compression setting".into()))?;
        }

        // WAL sync mode
        if let Ok(wal_mode) = std::env::var("LIGHTNING_DB_WAL_SYNC_MODE") {
            config.wal_sync_mode = match wal_mode.as_str() {
                "sync" => WalSyncMode::Sync,
                "async" => WalSyncMode::Async,
                _ => return Err(Error::Config("Invalid WAL sync mode".into())),
            };
        }

        // Add more environment variable mappings as needed

        Ok(config)
    }

    /// Load configuration from command line arguments
    fn load_from_args(&self, args: &[String]) -> Result<LightningDbConfig> {
        let mut config = LightningDbConfig::default();

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--cache-size" => {
                    if i + 1 < args.len() {
                        config.cache_size = args[i + 1]
                            .parse()
                            .map_err(|_| Error::Config("Invalid cache size".into()))?;
                        i += 1;
                    }
                }
                "--compression" => {
                    if i + 1 < args.len() {
                        config.compression_enabled = args[i + 1]
                            .parse()
                            .map_err(|_| Error::Config("Invalid compression setting".into()))?;
                        i += 1;
                    }
                }
                // Add more argument parsing as needed
                _ => {}
            }
            i += 1;
        }

        Ok(config)
    }

    /// Load configuration from remote source
    fn load_from_remote(&self, url: &str) -> Result<LightningDbConfig> {
        // This would fetch configuration from a remote source
        // For now, returning an error
        Err(Error::Config(
            "Remote configuration not yet implemented".into(),
        ))
    }

    /// Update current configuration
    fn update_config(&self, config: LightningDbConfig) -> Result<()> {
        // Validate configuration
        self.validate_config(&config)?;

        // Update current config
        {
            let mut current = self.current_config.write().unwrap();
            *current = config.clone();
        }

        // Notify callbacks
        let callbacks = self.change_callbacks.read().unwrap();
        for callback in callbacks.iter() {
            callback(&config);
        }

        Ok(())
    }

    /// Validate configuration
    pub fn validate_config(&self, config: &LightningDbConfig) -> Result<()> {
        // Cache size validation
        if config.cache_size == 0 {
            return Err(Error::Config("Cache size must be greater than 0".into()));
        }

        if config.cache_size > 1024 * 1024 * 1024 * 1024 {
            // 1TB
            return Err(Error::Config("Cache size too large (max 1TB)".into()));
        }

        // Page size validation
        if config.page_size < 512 || config.page_size > 65536 {
            return Err(Error::Config(
                "Page size must be between 512 and 65536".into(),
            ));
        }

        if !config.page_size.is_power_of_two() {
            return Err(Error::Config("Page size must be a power of 2".into()));
        }

        // Transaction validation
        if config.max_active_transactions == 0 {
            return Err(Error::Config(
                "Must allow at least 1 active transaction".into(),
            ));
        }

        // Add more validation rules as needed

        Ok(())
    }

    /// Load configuration for specific environment
    pub fn load_environment(&mut self, env_name: &str) -> Result<()> {
        let env_config = self
            .environments
            .get(env_name)
            .ok_or_else(|| Error::Config(format!("Unknown environment: {}", env_name)))?
            .clone();

        // Apply base configuration
        let config = env_config.base;

        // Apply overrides
        // This would merge the overrides into the config

        self.update_config(config)?;
        Ok(())
    }

    /// Register configuration change callback
    pub fn on_change<F>(&self, callback: F)
    where
        F: Fn(&LightningDbConfig) + Send + Sync + 'static,
    {
        let mut callbacks = self.change_callbacks.write().unwrap();
        callbacks.push(Box::new(callback));
    }

    /// Enable hot-reloading for file configurations
    pub fn enable_hot_reload(&mut self) -> Result<()> {
        #[cfg(feature = "file-watching")]
        {
            let (tx, rx) = mpsc::channel();

            let mut watcher =
                notify::recommended_watcher(move |res: std::result::Result<Event, notify::Error>| {
                    if let Ok(event) = res {
                        let _ = tx.send(event);
                    }
                })
                .map_err(|e| Error::Config(format!("Failed to create watcher: {}", e)))?;

            // Watch all file sources
            for source in &self.sources {
                if let ConfigSource::File(path) = source {
                    watcher
                        .watch(path, RecursiveMode::NonRecursive)
                        .map_err(|e| Error::Config(format!("Failed to watch file: {}", e)))?;
                }
            }

            self.watcher = Some(watcher);

            // Spawn reload thread
            let sources = self.sources.clone();
            let config_manager = Arc::new(self);

            std::thread::spawn(move || {
                for event in rx {
                    if matches!(event.kind, notify::EventKind::Modify(_)) {
                        // Reload configuration
                        for path in event.paths {
                            for source in &sources {
                                if let ConfigSource::File(source_path) = source {
                                    if source_path == &path {
                                        // Reload this configuration
                                        // This would trigger a reload
                                    }
                                }
                            }
                        }
                    }
                }
            });

            Ok(())
        }
        #[cfg(not(feature = "file-watching"))]
        {
            Err(Error::Config("Hot-reload requires 'file-watching' feature".into()))
        }
    }

    /// Get current configuration
    pub fn get_config(&self) -> LightningDbConfig {
        self.current_config.read().unwrap().clone()
    }

    /// Export configuration to file
    pub fn export_config(&self, path: &Path, format: ConfigFormat) -> Result<()> {
        let config = self.get_config();

        let content = match format {
            ConfigFormat::Json => serde_json::to_string_pretty(&config)
                .map_err(|e| Error::Config(format!("JSON serialization error: {}", e)))?,
            #[cfg(feature = "config-formats")]
            ConfigFormat::Yaml => serde_yaml::to_string(&config)
                .map_err(|e| Error::Config(format!("YAML serialization error: {}", e)))?,
            #[cfg(feature = "config-formats")]
            ConfigFormat::Toml => toml::to_string_pretty(&config)
                .map_err(|e| Error::Config(format!("TOML serialization error: {}", e)))?,
            #[cfg(not(feature = "config-formats"))]
            ConfigFormat::Yaml => return Err(Error::Config("YAML export requires 'config-formats' feature".into())),
            #[cfg(not(feature = "config-formats"))]
            ConfigFormat::Toml => return Err(Error::Config("TOML export requires 'config-formats' feature".into())),
            ConfigFormat::HCL => {
                return Err(Error::Config("HCL format not yet implemented".into()));
            }
        };

        fs::write(path, content).map_err(|e| Error::Io(e.to_string()))?;

        Ok(())
    }

    /// Load default templates
    fn load_default_templates() -> HashMap<String, ConfigTemplate> {
        let mut templates = HashMap::new();

        // High-performance template
        templates.insert(
            "high_performance".to_string(),
            ConfigTemplate {
                name: "high_performance".to_string(),
                description: "Optimized for maximum performance".to_string(),
                config: LightningDbConfig {
                    cache_size: 1024 * 1024 * 1024 * 4, // 4GB
                    compression_enabled: false,
                    wal_sync_mode: WalSyncMode::Async,
                    prefetch_enabled: true,
                    prefetch_distance: 64,
                    write_batch_size: 10000,
                    enable_statistics: false,
                    ..Default::default()
                },
                variables: HashMap::new(),
            },
        );

        // Balanced template
        templates.insert(
            "balanced".to_string(),
            ConfigTemplate {
                name: "balanced".to_string(),
                description: "Balanced performance and resource usage".to_string(),
                config: LightningDbConfig {
                    cache_size: 1024 * 1024 * 1024, // 1GB
                    compression_enabled: true,
                    compression_type: 2, // Lz4
                    wal_sync_mode: WalSyncMode::Sync,
                    prefetch_enabled: true,
                    enable_statistics: true,
                    ..Default::default()
                },
                variables: HashMap::new(),
            },
        );

        // Low-resource template
        templates.insert(
            "low_resource".to_string(),
            ConfigTemplate {
                name: "low_resource".to_string(),
                description: "Optimized for minimal resource usage".to_string(),
                config: LightningDbConfig {
                    cache_size: 1024 * 1024 * 64, // 64MB
                    compression_enabled: true,
                    compression_type: 1, // Zstd
                    compression_level: Some(6),
                    wal_sync_mode: WalSyncMode::Sync,
                    write_batch_size: 100,
                    max_active_transactions: 10,
                    ..Default::default()
                },
                variables: HashMap::new(),
            },
        );

        templates
    }

    /// Apply template
    pub fn apply_template(
        &mut self,
        template_name: &str,
        variables: HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        let template = self
            .templates
            .get(template_name)
            .ok_or_else(|| Error::Config(format!("Unknown template: {}", template_name)))?
            .clone();

        let config = template.config;

        // Apply variables to template
        // This would substitute variables in the configuration

        self.update_config(config)?;
        Ok(())
    }

    /// Diff configurations
    pub fn diff_configs(config1: &LightningDbConfig, config2: &LightningDbConfig) -> ConfigDiff {
        let json1 = serde_json::to_value(config1).unwrap();
        let json2 = serde_json::to_value(config2).unwrap();

        ConfigDiff::from_json(&json1, &json2)
    }
}

/// Configuration difference
#[derive(Debug, Clone)]
pub struct ConfigDiff {
    /// Added fields
    pub added: HashMap<String, serde_json::Value>,
    /// Removed fields
    pub removed: HashMap<String, serde_json::Value>,
    /// Modified fields
    pub modified: HashMap<String, (serde_json::Value, serde_json::Value)>,
}

impl ConfigDiff {
    /// Create diff from JSON values
    fn from_json(val1: &serde_json::Value, val2: &serde_json::Value) -> Self {
        let mut diff = ConfigDiff {
            added: HashMap::new(),
            removed: HashMap::new(),
            modified: HashMap::new(),
        };

        // Compare JSON objects
        if let (Some(obj1), Some(obj2)) = (val1.as_object(), val2.as_object()) {
            // Check for removed/modified fields
            for (key, value1) in obj1 {
                if let Some(value2) = obj2.get(key) {
                    if value1 != value2 {
                        diff.modified
                            .insert(key.clone(), (value1.clone(), value2.clone()));
                    }
                } else {
                    diff.removed.insert(key.clone(), value1.clone());
                }
            }

            // Check for added fields
            for (key, value2) in obj2 {
                if !obj1.contains_key(key) {
                    diff.added.insert(key.clone(), value2.clone());
                }
            }
        }

        diff
    }

    /// Check if configurations are equal
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.modified.is_empty()
    }
}

/// Configuration builder for fluent API
pub struct ConfigBuilder {
    config: LightningDbConfig,
}

impl ConfigBuilder {
    /// Create new builder
    pub fn new() -> Self {
        Self {
            config: LightningDbConfig::default(),
        }
    }

    /// Set cache size
    pub fn cache_size(mut self, size: usize) -> Self {
        self.config.cache_size = size as u64;
        self
    }

    /// Enable compression
    pub fn compression(mut self, enabled: bool) -> Self {
        self.config.compression_enabled = enabled;
        self
    }

    /// Set compression type
    pub fn compression_type(mut self, comp_type: CompressionType) -> Self {
        self.config.compression_type = match comp_type {
            CompressionType::None => 0,
            #[cfg(feature = "zstd-compression")]
            CompressionType::Zstd => 1,
            CompressionType::Lz4 => 2,
            CompressionType::Snappy => 3,
        };
        self
    }

    /// Set WAL sync mode
    pub fn wal_sync_mode(mut self, mode: WalSyncMode) -> Self {
        self.config.wal_sync_mode = mode;
        self
    }

    /// Set maximum active transactions
    pub fn max_transactions(mut self, max: usize) -> Self {
        self.config.max_active_transactions = max;
        self
    }

    /// Enable prefetching
    pub fn prefetch(mut self, enabled: bool) -> Self {
        self.config.prefetch_enabled = enabled;
        self
    }

    /// Build configuration
    pub fn build(self) -> LightningDbConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = ConfigBuilder::new()
            .cache_size(1024 * 1024 * 1024)
            .compression(true)
            .compression_type(CompressionType::Zstd)
            .wal_sync_mode(WalSyncMode::Sync)
            .max_transactions(1000)
            .prefetch(true)
            .build();

        assert_eq!(config.cache_size, 1024 * 1024 * 1024);
        assert!(config.compression_enabled);
        assert_eq!(config.compression_type, 1); // Zstd
    }

    #[test]
    fn test_config_validation() {
        let manager = ConfigManager::new();

        // Valid config
        let valid_config = LightningDbConfig::default();
        assert!(manager.validate_config(&valid_config).is_ok());

        // Invalid cache size
        let mut invalid_config = LightningDbConfig::default();
        invalid_config.cache_size = 0;
        assert!(manager.validate_config(&invalid_config).is_err());
    }

    #[test]
    fn test_config_diff() {
        let config1 = ConfigBuilder::new()
            .cache_size(1024)
            .compression(true)
            .build();

        let config2 = ConfigBuilder::new()
            .cache_size(2048)
            .compression(true)
            .prefetch(true)
            .build();

        let diff = ConfigManager::diff_configs(&config1, &config2);
        assert!(!diff.modified.is_empty());
        assert!(!diff.added.is_empty());
    }
}
