use crate::{LightningDbConfig, WalSyncMode, MmapConfig, ConsistencyConfig};
use crate::features::encryption::EncryptionConfig;
use crate::utils::quotas::QuotaConfig;

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
}