# Lightning DB Configuration Guide

This guide covers all configuration options for Lightning DB, including presets for common use cases and detailed field documentation.

## Quick Start

```rust
use lightning_db::{Database, LightningDbConfig, ConfigPreset};

// Use a preset for common scenarios
let config = ConfigPreset::Balanced.to_config();
let db = Database::create("./my_db", config)?;

// Or customize the default configuration
let config = LightningDbConfig {
    cache_size: 128 * 1024 * 1024,  // 128MB
    compression_enabled: true,
    ..Default::default()
};
let db = Database::create("./my_db", config)?;
```

---

## Configuration Presets

Lightning DB provides production-ready presets for common use cases:

| Preset | Use Case | Cache | Compression | WAL Mode |
|--------|----------|-------|-------------|----------|
| `ReadOptimized` | Read-heavy workloads | 256MB | Disabled | Sync |
| `WriteOptimized` | Write-heavy workloads | 128MB | ZSTD | Async |
| `Balanced` | Mixed workloads | 128MB | LZ4 | Sync |
| `LowMemory` | Embedded/Mobile | 4MB | LZ4 | Sync |
| `Development` | Testing/Development | 16MB | Disabled | Async |
| `MaxDurability` | Critical data | 64MB | ZSTD | Full |

### Using Presets

```rust
use lightning_db::{ConfigPreset, Database};

// Read-optimized for analytics workloads
let config = ConfigPreset::ReadOptimized.to_config();

// Write-optimized for logging/time-series
let config = ConfigPreset::WriteOptimized.to_config();

// Balanced for general use
let config = ConfigPreset::Balanced.to_config();

// Low memory for embedded devices
let config = ConfigPreset::LowMemory.to_config();

// Development for testing
let config = ConfigPreset::Development.to_config();

// Maximum durability for financial data
let config = ConfigPreset::MaxDurability.to_config();
```

---

## LightningDbConfig Fields

### Storage Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `page_size` | `u64` | 4096 | Page size in bytes. Must be power of 2. Range: 512 - 1MB |
| `cache_size` | `u64` | 0 | Page cache size in bytes. 0 = disabled |
| `mmap_size` | `Option<u64>` | None | Memory-mapped file size. None = disabled |

```rust
LightningDbConfig {
    page_size: 8192,                    // 8KB pages
    cache_size: 256 * 1024 * 1024,      // 256MB cache
    mmap_size: Some(512 * 1024 * 1024), // 512MB mmap
    ..Default::default()
}
```

### Compression Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `compression_enabled` | `bool` | true | Enable data compression |
| `compression_type` | `i32` | 1 | Algorithm: 0=None, 1=ZSTD, 2=LZ4 |
| `compression_level` | `Option<i32>` | Some(3) | Compression level (ZSTD: 1-22) |

```rust
// High compression ratio
LightningDbConfig {
    compression_enabled: true,
    compression_type: 1,        // ZSTD
    compression_level: Some(9), // Higher compression
    ..Default::default()
}

// Fast compression
LightningDbConfig {
    compression_enabled: true,
    compression_type: 2,    // LZ4
    compression_level: None,
    ..Default::default()
}
```

### Transaction Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_active_transactions` | `usize` | 1000 | Max concurrent transactions. Range: 1 - 100,000 |
| `use_optimized_transactions` | `bool` | false | Use optimized transaction engine |

```rust
LightningDbConfig {
    max_active_transactions: 500,
    use_optimized_transactions: true,
    ..Default::default()
}
```

### WAL (Write-Ahead Log) Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `use_unified_wal` | `bool` | true | Use unified WAL system |
| `wal_sync_mode` | `WalSyncMode` | Async | Sync mode for durability |
| `write_batch_size` | `usize` | 1000 | Max operations per batch |

**WalSyncMode Values:**
- `Async` - Asynchronous writes (fastest, lowest durability)
- `Sync` - Synchronous writes (balanced)
- `Full` - Full fsync on every write (slowest, maximum durability)

```rust
use lightning_db::WalSyncMode;

// Maximum durability
LightningDbConfig {
    use_unified_wal: true,
    wal_sync_mode: WalSyncMode::Full,
    write_batch_size: 100,
    ..Default::default()
}

// Maximum performance
LightningDbConfig {
    use_unified_wal: true,
    wal_sync_mode: WalSyncMode::Async,
    write_batch_size: 5000,
    ..Default::default()
}
```

### Prefetch Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `prefetch_enabled` | `bool` | false | Enable prefetching |
| `prefetch_distance` | `usize` | 8 | Prefetch distance (pages ahead) |
| `prefetch_workers` | `usize` | 2 | Number of prefetch worker threads |

```rust
LightningDbConfig {
    prefetch_enabled: true,
    prefetch_distance: 64,
    prefetch_workers: 4,
    ..Default::default()
}
```

### Memory-Mapped I/O Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mmap_config` | `Option<MmapConfig>` | Some(default) | Memory mapping configuration |

```rust
use lightning_db::MmapConfig;

LightningDbConfig {
    mmap_config: Some(MmapConfig {
        region_size: 512 * 1024 * 1024,
        populate_on_map: true,
        enable_huge_pages: false,
        enable_prefault: true,
        enable_async_msync: true,
        max_mapped_regions: 100,
        flush_interval: std::time::Duration::from_secs(1),
        ..Default::default()
    }),
    ..Default::default()
}
```

### Encryption Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `encryption_config` | `EncryptionConfig` | disabled | Encryption settings |

```rust
use lightning_db::features::encryption::{EncryptionConfig, EncryptionAlgorithm};

LightningDbConfig {
    encryption_config: EncryptionConfig {
        enabled: true,
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        ..Default::default()
    },
    ..Default::default()
}
```

### Quota Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `quota_config` | `QuotaConfig` | disabled | Resource quota limits |

```rust
use lightning_db::utils::quotas::QuotaConfig;

LightningDbConfig {
    quota_config: QuotaConfig {
        enabled: true,
        max_database_size: Some(10 * 1024 * 1024 * 1024), // 10GB
        max_transaction_size: Some(100 * 1024 * 1024),     // 100MB
        max_concurrent_transactions: Some(100),
        max_connections: Some(50),
    },
    ..Default::default()
}
```

### Statistics Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enable_statistics` | `bool` | true | Enable performance statistics collection |

---

## Configuration Validation

Lightning DB validates configuration values within these limits:

| Parameter | Min | Max |
|-----------|-----|-----|
| `page_size` | 512 | 1MB |
| `cache_size` | 0 | 100GB |
| `max_active_transactions` | 1 | 100,000 |
| `write_batch_size` | 1 | 100,000 |
| `prefetch_workers` | 1 | 64 |
| `prefetch_distance` | 1 | 1024 |
| `compression_level` (ZSTD) | 1 | 22 |

Invalid configurations will result in an error at database creation.

---

## Builder Pattern

Use `ConfigBuilder` for fluent configuration:

```rust
use lightning_db::{ConfigBuilder, WalSyncMode};

let config = ConfigBuilder::new()
    .page_size(8192)
    .cache_size(256 * 1024 * 1024)
    .compression(true, 1, Some(3))  // enabled, ZSTD, level 3
    .wal_sync_mode(WalSyncMode::Sync)
    .max_transactions(500)
    .build()?;
```

---

## Performance Tuning

### For Read-Heavy Workloads

```rust
LightningDbConfig {
    cache_size: 512 * 1024 * 1024,  // Large cache
    prefetch_enabled: true,
    prefetch_distance: 64,
    compression_enabled: false,      // Avoid decompression overhead
    mmap_size: Some(1024 * 1024 * 1024),
    ..Default::default()
}
```

### For Write-Heavy Workloads

```rust
LightningDbConfig {
    wal_sync_mode: WalSyncMode::Async,
    write_batch_size: 5000,
    compression_enabled: true,
    compression_type: 1,  // ZSTD for better ratio
    prefetch_enabled: false,
    ..Default::default()
}
```

### For Low Latency

```rust
LightningDbConfig {
    cache_size: 256 * 1024 * 1024,
    compression_enabled: false,
    prefetch_enabled: true,
    wal_sync_mode: WalSyncMode::Async,
    ..Default::default()
}
```

### For Maximum Durability

```rust
LightningDbConfig {
    wal_sync_mode: WalSyncMode::Full,
    write_batch_size: 1,
    use_unified_wal: true,
    ..Default::default()
}
```

---

## Environment-Specific Recommendations

### Production

```rust
ConfigPreset::Balanced.to_config()
// Or customize:
LightningDbConfig {
    cache_size: 256 * 1024 * 1024,
    wal_sync_mode: WalSyncMode::Sync,
    enable_statistics: true,
    ..Default::default()
}
```

### Development/Testing

```rust
ConfigPreset::Development.to_config()
// Faster, less durable, better for iteration
```

### Embedded/IoT

```rust
ConfigPreset::LowMemory.to_config()
// Minimal memory footprint
```

### Cloud/Container

```rust
LightningDbConfig {
    cache_size: 64 * 1024 * 1024,  // Respect container limits
    mmap_size: None,                // Avoid mmap in containers
    quota_config: QuotaConfig {
        enabled: true,
        max_database_size: Some(10 * 1024 * 1024 * 1024),
        ..Default::default()
    },
    ..Default::default()
}
```
