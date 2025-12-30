use crate::{LightningDbConfig, WalSyncMode, MmapConfig, ConsistencyConfig, Result, Error};
use crate::features::encryption::EncryptionConfig;
use crate::utils::quotas::QuotaConfig;

/// Configuration validation limits
pub mod limits {
    /// Minimum page size (512 bytes)
    pub const MIN_PAGE_SIZE: u64 = 512;
    /// Maximum page size (1MB - must be power of 2)
    pub const MAX_PAGE_SIZE: u64 = 1024 * 1024;
    /// Maximum cache size (100GB)
    pub const MAX_CACHE_SIZE: u64 = 100 * 1024 * 1024 * 1024;
    /// Minimum active transactions
    pub const MIN_ACTIVE_TRANSACTIONS: usize = 1;
    /// Maximum active transactions
    pub const MAX_ACTIVE_TRANSACTIONS: usize = 100_000;
    /// Maximum write batch size
    pub const MAX_WRITE_BATCH_SIZE: usize = 100_000;
    /// Maximum prefetch workers
    pub const MAX_PREFETCH_WORKERS: usize = 64;
    /// Maximum prefetch distance
    pub const MAX_PREFETCH_DISTANCE: usize = 1024;
}

/// Production-ready configuration presets for different use cases
#[derive(Debug, Clone, Copy)]
pub enum ConfigPreset {
    /// High-performance configuration for read-heavy workloads
    ReadOptimized,
    /// Configuration optimized for write-heavy workloads
    WriteOptimized,
    /// Balanced configuration for mixed workloads
    Balanced,
    /// Memory-constrained environment (mobile, embedded)
    LowMemory,
    /// Configuration for development and testing
    Development,
    /// Maximum durability with fsync on every write
    MaxDurability,
}

impl ConfigPreset {
    /// Convert preset to actual configuration
    pub fn to_config(self) -> LightningDbConfig {
        match self {
            ConfigPreset::ReadOptimized => Self::read_optimized_config(),
            ConfigPreset::WriteOptimized => Self::write_optimized_config(),
            ConfigPreset::Balanced => Self::balanced_config(),
            ConfigPreset::LowMemory => Self::low_memory_config(),
            ConfigPreset::Development => Self::development_config(),
            ConfigPreset::MaxDurability => Self::max_durability_config(),
        }
    }

    fn read_optimized_config() -> LightningDbConfig {
        LightningDbConfig {
            page_size: 8192,
            cache_size: 256 * 1024 * 1024, // 256MB cache
            mmap_size: Some(512 * 1024 * 1024), // 512MB mmap
            compression_enabled: false, // No compression for faster reads
            compression_type: 0, // None
            compression_level: None,
            max_active_transactions: 1000,
            prefetch_enabled: true,
            prefetch_distance: 64,
            prefetch_workers: 4,
            use_optimized_transactions: true,
            use_optimized_page_manager: true,
            mmap_config: Some(MmapConfig {
                region_size: 512 * 1024 * 1024,
                populate_on_map: true,
                enable_huge_pages: false,
                enable_prefault: true,
                enable_async_msync: true,
                max_mapped_regions: 100,
                flush_interval: std::time::Duration::from_secs(1),
                lock_on_fault: false,
                use_direct_io: false,
                huge_page_size: 2 * 1024 * 1024,
                enable_transparent_huge_pages: false,
                numa_aware_allocation: false,
            }),
            consistency_config: ConsistencyConfig::default(),
            use_unified_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            write_batch_size: 100,
            encryption_config: EncryptionConfig::default(),
            quota_config: QuotaConfig::default(),
            enable_statistics: false,
        }
    }

    fn write_optimized_config() -> LightningDbConfig {
        LightningDbConfig {
            page_size: 16384, // Larger pages for batch writes
            cache_size: 128 * 1024 * 1024,
            mmap_size: None, // No mmap for write optimization
            compression_enabled: true,
            compression_type: 1, // ZSTD
            compression_level: Some(3),
            max_active_transactions: 100,
            prefetch_enabled: false,
            prefetch_distance: 16,
            prefetch_workers: 2,
            use_optimized_transactions: true,
            use_optimized_page_manager: true,
            mmap_config: None,
            consistency_config: ConsistencyConfig::default(),
            use_unified_wal: true,
            wal_sync_mode: WalSyncMode::Async, // Async writes
            write_batch_size: 1000,
            encryption_config: EncryptionConfig::default(),
            quota_config: QuotaConfig::default(),
            enable_statistics: false,
        }
    }

    fn balanced_config() -> LightningDbConfig {
        LightningDbConfig {
            page_size: 8192,
            cache_size: 128 * 1024 * 1024,
            mmap_size: Some(256 * 1024 * 1024),
            compression_enabled: true,
            compression_type: 2, // LZ4
            compression_level: None,
            max_active_transactions: 200,
            prefetch_enabled: true,
            prefetch_distance: 32,
            prefetch_workers: 3,
            use_optimized_transactions: true,
            use_optimized_page_manager: true,
            mmap_config: Some(MmapConfig {
                region_size: 256 * 1024 * 1024,
                populate_on_map: false,
                enable_huge_pages: false,
                enable_prefault: false,
                enable_async_msync: true,
                max_mapped_regions: 50,
                flush_interval: std::time::Duration::from_secs(2),
                lock_on_fault: false,
                use_direct_io: false,
                huge_page_size: 2 * 1024 * 1024,
                enable_transparent_huge_pages: false,
                numa_aware_allocation: false,
            }),
            consistency_config: ConsistencyConfig::default(),
            use_unified_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            write_batch_size: 500,
            encryption_config: EncryptionConfig::default(),
            quota_config: QuotaConfig::default(),
            enable_statistics: false,
        }
    }

    fn low_memory_config() -> LightningDbConfig {
        LightningDbConfig {
            page_size: 4096,
            cache_size: 4 * 1024 * 1024, // 4MB cache
            mmap_size: None,
            compression_enabled: true,
            compression_type: 2, // LZ4 for speed
            compression_level: None,
            max_active_transactions: 10,
            prefetch_enabled: false,
            prefetch_distance: 4,
            prefetch_workers: 1,
            use_optimized_transactions: false,
            use_optimized_page_manager: false,
            mmap_config: None,
            consistency_config: ConsistencyConfig::default(),
            use_unified_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            write_batch_size: 50,
            encryption_config: EncryptionConfig::default(),
            quota_config: QuotaConfig {
                enabled: true,
                max_database_size: Some(100 * 1024 * 1024), // 100MB limit
                max_transaction_size: Some(10 * 1024 * 1024),
                max_concurrent_transactions: Some(10),
                max_connections: Some(10),
            },
            enable_statistics: false,
        }
    }

    fn development_config() -> LightningDbConfig {
        LightningDbConfig {
            page_size: 4096,
            cache_size: 16 * 1024 * 1024,
            mmap_size: None,
            compression_enabled: false,
            compression_type: 0,
            compression_level: None,
            max_active_transactions: 50,
            prefetch_enabled: false,
            prefetch_distance: 8,
            prefetch_workers: 2,
            use_optimized_transactions: false,
            use_optimized_page_manager: false,
            mmap_config: None,
            consistency_config: ConsistencyConfig::default(),
            use_unified_wal: true,
            wal_sync_mode: WalSyncMode::Async,
            write_batch_size: 100,
            encryption_config: EncryptionConfig::default(),
            quota_config: QuotaConfig::default(),
            enable_statistics: true,
        }
    }

    fn max_durability_config() -> LightningDbConfig {
        LightningDbConfig {
            page_size: 4096,
            cache_size: 64 * 1024 * 1024,
            mmap_size: None,
            compression_enabled: true,
            compression_type: 1, // ZSTD
            compression_level: Some(6),
            max_active_transactions: 50,
            prefetch_enabled: false,
            prefetch_distance: 8,
            prefetch_workers: 1,
            use_optimized_transactions: true,
            use_optimized_page_manager: true,
            mmap_config: None,
            consistency_config: ConsistencyConfig {
                default_level: crate::utils::safety::consistency::ConsistencyLevel::Strong,
                consistency_timeout: std::time::Duration::from_secs(5),
                enable_read_repair: true,
                max_clock_skew: std::time::Duration::from_millis(100),
            },
            use_unified_wal: true,
            wal_sync_mode: WalSyncMode::Sync, // Fsync on every write
            write_batch_size: 10,
            encryption_config: EncryptionConfig::default(),
            quota_config: QuotaConfig::default(),
            enable_statistics: false,
        }
    }
}

/// Builder pattern for fine-tuning presets
pub struct ConfigBuilder {
    config: LightningDbConfig,
}

impl ConfigBuilder {
    /// Start with a preset configuration
    pub fn from_preset(preset: ConfigPreset) -> Self {
        Self {
            config: preset.to_config(),
        }
    }

    /// Start with default configuration
    pub fn new() -> Self {
        Self {
            config: LightningDbConfig::default(),
        }
    }

    pub fn page_size(mut self, size: u64) -> Self {
        self.config.page_size = size;
        self
    }

    pub fn cache_size(mut self, size: u64) -> Self {
        self.config.cache_size = size;
        self
    }

    pub fn compression_enabled(mut self, enabled: bool) -> Self {
        self.config.compression_enabled = enabled;
        self
    }

    pub fn max_active_transactions(mut self, max: usize) -> Self {
        self.config.max_active_transactions = max;
        self
    }

    pub fn wal_sync_mode(mut self, mode: WalSyncMode) -> Self {
        self.config.wal_sync_mode = mode;
        self
    }

    pub fn write_batch_size(mut self, size: usize) -> Self {
        self.config.write_batch_size = size;
        self
    }

    pub fn enable_statistics(mut self, enabled: bool) -> Self {
        self.config.enable_statistics = enabled;
        self
    }

    pub fn build(self) -> LightningDbConfig {
        self.config
    }

    /// Build configuration with validation
    /// Returns an error if any configuration values are invalid
    pub fn build_validated(self) -> Result<LightningDbConfig> {
        self.validate()?;
        Ok(self.config)
    }

    /// Validate configuration without building
    /// Returns a list of validation errors or Ok(()) if valid
    pub fn validate(&self) -> Result<()> {
        let config = &self.config;

        // Validate page_size
        if config.page_size < limits::MIN_PAGE_SIZE {
            return Err(Error::Config(format!(
                "page_size {} is below minimum {}",
                config.page_size, limits::MIN_PAGE_SIZE
            )));
        }
        if config.page_size > limits::MAX_PAGE_SIZE {
            return Err(Error::Config(format!(
                "page_size {} exceeds maximum {}",
                config.page_size, limits::MAX_PAGE_SIZE
            )));
        }
        // Page size must be a power of 2
        if !config.page_size.is_power_of_two() {
            return Err(Error::Config(format!(
                "page_size {} must be a power of 2",
                config.page_size
            )));
        }

        // Validate cache_size
        if config.cache_size > limits::MAX_CACHE_SIZE {
            return Err(Error::Config(format!(
                "cache_size {} exceeds maximum {} (100GB)",
                config.cache_size, limits::MAX_CACHE_SIZE
            )));
        }

        // Validate max_active_transactions
        if config.max_active_transactions < limits::MIN_ACTIVE_TRANSACTIONS {
            return Err(Error::Config(format!(
                "max_active_transactions {} is below minimum {}",
                config.max_active_transactions, limits::MIN_ACTIVE_TRANSACTIONS
            )));
        }
        if config.max_active_transactions > limits::MAX_ACTIVE_TRANSACTIONS {
            return Err(Error::Config(format!(
                "max_active_transactions {} exceeds maximum {}",
                config.max_active_transactions, limits::MAX_ACTIVE_TRANSACTIONS
            )));
        }

        // Validate write_batch_size (0 is allowed - means no batching)
        if config.write_batch_size > limits::MAX_WRITE_BATCH_SIZE {
            return Err(Error::Config(format!(
                "write_batch_size {} exceeds maximum {}",
                config.write_batch_size, limits::MAX_WRITE_BATCH_SIZE
            )));
        }

        // Validate prefetch settings if enabled
        if config.prefetch_enabled {
            if config.prefetch_workers == 0 {
                return Err(Error::Config(
                    "prefetch_workers must be at least 1 when prefetch is enabled".into()
                ));
            }
            if config.prefetch_workers > limits::MAX_PREFETCH_WORKERS {
                return Err(Error::Config(format!(
                    "prefetch_workers {} exceeds maximum {}",
                    config.prefetch_workers, limits::MAX_PREFETCH_WORKERS
                )));
            }
            if config.prefetch_distance == 0 {
                return Err(Error::Config(
                    "prefetch_distance must be at least 1 when prefetch is enabled".into()
                ));
            }
            if config.prefetch_distance > limits::MAX_PREFETCH_DISTANCE {
                return Err(Error::Config(format!(
                    "prefetch_distance {} exceeds maximum {}",
                    config.prefetch_distance, limits::MAX_PREFETCH_DISTANCE
                )));
            }
        }

        // Validate compression settings
        if config.compression_enabled {
            if let Some(level) = config.compression_level {
                // ZSTD levels are 1-22, LZ4 and Snappy don't use levels
                if config.compression_type == 1 && !(1..=22).contains(&level) {
                    return Err(Error::Config(format!(
                        "ZSTD compression_level {} must be between 1 and 22",
                        level
                    )));
                }
            }
        }

        // Validate mmap_config if present
        if let Some(ref mmap) = config.mmap_config {
            if mmap.region_size == 0 {
                return Err(Error::Config(
                    "mmap region_size cannot be 0".into()
                ));
            }
            if mmap.max_mapped_regions == 0 {
                return Err(Error::Config(
                    "mmap max_mapped_regions cannot be 0".into()
                ));
            }
        }

        Ok(())
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preset_configurations() {
        let read_config = ConfigPreset::ReadOptimized.to_config();
        assert_eq!(read_config.cache_size, 256 * 1024 * 1024);
        assert!(!read_config.compression_enabled);

        let write_config = ConfigPreset::WriteOptimized.to_config();
        assert_eq!(write_config.write_batch_size, 1000);
        assert!(write_config.compression_enabled);

        let low_mem_config = ConfigPreset::LowMemory.to_config();
        assert_eq!(low_mem_config.cache_size, 4 * 1024 * 1024);

        let dev_config = ConfigPreset::Development.to_config();
        assert!(dev_config.enable_statistics);
    }

    #[test]
    fn test_config_builder() {
        let config = ConfigBuilder::from_preset(ConfigPreset::Balanced)
            .cache_size(256 * 1024 * 1024)
            .compression_enabled(false)
            .max_active_transactions(500)
            .build();

        assert_eq!(config.cache_size, 256 * 1024 * 1024);
        assert!(!config.compression_enabled);
        assert_eq!(config.max_active_transactions, 500);
    }

    #[test]
    fn test_all_presets_valid() {
        let presets = vec![
            ConfigPreset::ReadOptimized,
            ConfigPreset::WriteOptimized,
            ConfigPreset::Balanced,
            ConfigPreset::LowMemory,
            ConfigPreset::Development,
            ConfigPreset::MaxDurability,
        ];

        for preset in presets {
            let config = preset.to_config();
            assert!(config.page_size > 0);
            assert!(config.cache_size > 0);
            assert!(config.max_active_transactions > 0);
        }
    }

    #[test]
    fn test_all_presets_pass_validation() {
        let presets = vec![
            ConfigPreset::ReadOptimized,
            ConfigPreset::WriteOptimized,
            ConfigPreset::Balanced,
            ConfigPreset::LowMemory,
            ConfigPreset::Development,
            ConfigPreset::MaxDurability,
        ];

        for preset in presets {
            let builder = ConfigBuilder::from_preset(preset);
            assert!(builder.validate().is_ok(), "Preset {:?} failed validation", preset);
        }
    }

    #[test]
    fn test_validation_page_size_too_small() {
        let builder = ConfigBuilder::new().page_size(256);
        let result = builder.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("page_size"));
    }

    #[test]
    fn test_validation_page_size_not_power_of_two() {
        let builder = ConfigBuilder::new().page_size(5000);
        let result = builder.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("power of 2"));
    }

    #[test]
    fn test_validation_max_transactions_zero() {
        let builder = ConfigBuilder::new().max_active_transactions(0);
        let result = builder.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_active_transactions"));
    }

    #[test]
    fn test_build_validated_success() {
        let config = ConfigBuilder::new().build_validated();
        assert!(config.is_ok());
    }

    #[test]
    fn test_build_validated_failure() {
        let config = ConfigBuilder::new().page_size(100).build_validated();
        assert!(config.is_err());
    }
}