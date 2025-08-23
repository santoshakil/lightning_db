# Lightning DB Module Architecture

## Module Overview

Lightning DB is organized into several key module categories, each with specific responsibilities and interdependencies.

## Core Modules (`src/core/`)

### 1. **error.rs**
- **Purpose**: Central error handling for entire system
- **Dependencies**: None (foundational)
- **Used by**: All modules
- **Key Types**: `Error`, `Result<T>`, `DatabaseError`

### 2. **storage/**
- **Purpose**: Low-level storage management
- **Dependencies**: `error`, `utils/serialization`
- **Used by**: `btree`, `lsm`, `wal`, `cache`
- **Components**:
  - `page_manager.rs`: Page-level storage operations
  - `file_manager.rs`: File I/O operations
  - `mmap.rs`: Memory-mapped file support

### 3. **btree/**
- **Purpose**: B+Tree implementation for ordered data
- **Dependencies**: `storage`, `cache`, `error`
- **Used by**: Main database API
- **Components**:
  - `node.rs`: B+Tree node structure
  - `iterator.rs`: Range scan support
  - `split_merge.rs`: Tree balancing operations

### 4. **lsm/**
- **Purpose**: LSM tree for write optimization
- **Dependencies**: `storage`, `cache`, `compaction`
- **Used by**: Main database API
- **Components**:
  - `memtable.rs`: In-memory write buffer
  - `sstable.rs`: Sorted string table files
  - `level_manager.rs`: Multi-level compaction

### 5. **transaction/**
- **Purpose**: ACID transaction support with MVCC
- **Dependencies**: `storage`, `wal`, `error`
- **Used by**: Main database API
- **Components**:
  - `mvcc.rs`: Multi-version concurrency control
  - `version_store.rs`: Version management
  - `unified_manager.rs`: Transaction coordination

### 6. **wal/**
- **Purpose**: Write-ahead logging for durability
- **Dependencies**: `storage`, `error`
- **Used by**: `transaction`, `recovery`
- **Components**:
  - `writer.rs`: WAL entry writing
  - `reader.rs`: WAL replay
  - `checkpoint.rs`: Checkpoint management

### 7. **recovery/**
- **Purpose**: Crash recovery and consistency
- **Dependencies**: `wal`, `storage`, `transaction`
- **Used by**: Database initialization
- **Components**:
  - `recovery_manager.rs`: Recovery coordination
  - `consistency_check.rs`: Data validation
  - `io_recovery.rs`: I/O error recovery

## Performance Modules (`src/performance/`)

### 1. **cache/**
- **Purpose**: Intelligent caching with ARC algorithm
- **Dependencies**: `core/storage`
- **Used by**: `btree`, `lsm`, main API
- **Components**:
  - `arc_cache.rs`: Adaptive Replacement Cache
  - `cache_manager.rs`: Cache coordination
  - `memory_pool.rs`: Memory management

### 2. **io_uring/** (Linux-specific)
- **Purpose**: High-performance async I/O
- **Dependencies**: `core/storage`
- **Used by**: Storage layer on Linux
- **Components**:
  - `direct_io.rs`: Direct I/O operations
  - `submission_queue.rs`: I/O request batching

### 3. **prefetch/**
- **Purpose**: Predictive data prefetching
- **Dependencies**: `cache`, `storage`
- **Used by**: Query execution
- **Components**:
  - `pattern_detector.rs`: Access pattern analysis
  - `prefetch_engine.rs`: Prefetch scheduling

## Feature Modules (`src/features/`)

### 1. **compaction/**
- **Purpose**: Background data compaction
- **Dependencies**: `lsm`, `storage`, `metrics`
- **Used by**: Database maintenance
- **Types**:
  - Online compaction
  - Offline compaction
  - Incremental compaction
  - LSM level compaction

### 2. **backup/**
- **Purpose**: Backup and restore functionality
- **Dependencies**: `storage`, `compression`
- **Used by**: Admin API
- **Components**:
  - `full_backup.rs`: Complete database backup
  - `incremental_backup.rs`: Delta backups
  - `restore.rs`: Backup restoration

### 3. **integrity/**
- **Purpose**: Data integrity verification
- **Dependencies**: `storage`, `transaction`
- **Used by**: Admin API, recovery
- **Components**:
  - `checksums.rs`: Data checksumming
  - `validator.rs`: Integrity validation
  - `repair.rs`: Corruption repair

### 4. **migration/**
- **Purpose**: Schema and data migration
- **Dependencies**: `storage`, `transaction`
- **Used by**: Database upgrades
- **Components**:
  - `schema_manager.rs`: Schema versioning
  - `migration_runner.rs`: Migration execution
  - `rollback.rs`: Migration rollback

### 5. **observability/**
- **Purpose**: Monitoring and telemetry
- **Dependencies**: External (OpenTelemetry, Prometheus)
- **Used by**: All modules
- **Components**:
  - `metrics.rs`: Performance metrics
  - `tracing.rs`: Distributed tracing
  - `health.rs`: Health checks

### 6. **logging/**
- **Purpose**: Structured logging
- **Dependencies**: External (tracing, log)
- **Used by**: All modules
- **Components**:
  - `logger.rs`: Log management
  - `telemetry.rs`: Telemetry integration

### 7. **compression/**
- **Purpose**: Data compression
- **Dependencies**: External (zstd, lz4)
- **Used by**: `storage`, `backup`
- **Algorithms**:
  - Zstd
  - LZ4
  - Snappy

### 8. **admin/**
- **Purpose**: Administrative operations
- **Dependencies**: Most core modules
- **Used by**: CLI tools, HTTP API
- **Operations**:
  - Statistics gathering
  - Maintenance tasks
  - Configuration management

### 9. **memory_monitoring/**
- **Purpose**: Memory usage tracking
- **Dependencies**: `utils/memory_tracker`
- **Used by**: Resource management
- **Features**:
  - Allocation tracking
  - Leak detection
  - Memory limits

### 10. **transactions/** (Extended)
- **Purpose**: Advanced transaction features
- **Dependencies**: `core/transaction`
- **Used by**: Main API
- **Features**:
  - Isolation levels
  - Deadlock detection
  - Lock management

## Utility Modules (`src/utils/`)

### 1. **serialization/**
- **Purpose**: Data serialization/deserialization
- **Dependencies**: External (serde, bincode)
- **Used by**: All data modules

### 2. **batching/**
- **Purpose**: Operation batching for efficiency
- **Dependencies**: None
- **Used by**: Write operations

### 3. **safety/**
- **Purpose**: Safety checks and validation
- **Dependencies**: None
- **Used by**: All modules

### 4. **resource_management/**
- **Purpose**: Resource lifecycle management
- **Dependencies**: System APIs
- **Used by**: All modules

### 5. **integrity/**
- **Purpose**: Data integrity utilities
- **Dependencies**: Checksumming libraries
- **Used by**: `features/integrity`

## Security Module (`src/security/`)

### 1. **encryption/**
- **Purpose**: Data encryption at rest
- **Dependencies**: Crypto libraries
- **Used by**: `storage`, `wal`

### 2. **authentication/**
- **Purpose**: User authentication
- **Dependencies**: Crypto libraries
- **Used by**: Network API

### 3. **audit/**
- **Purpose**: Security audit logging
- **Dependencies**: `logging`
- **Used by**: All security-sensitive operations

## Module Dependency Graph

```
┌─────────────────────────────────────────────────────┐
│                    Main API (lib.rs)                │
└────────────┬────────────────────────────────────────┘
             │
      ┌──────┴──────┬──────────┬──────────┬──────────┐
      │             │          │          │          │
  ┌───▼───┐   ┌────▼────┐ ┌───▼───┐ ┌───▼───┐ ┌────▼────┐
  │ BTree │   │   LSM   │ │  TXN  │ │ Cache │ │Features │
  └───┬───┘   └────┬────┘ └───┬───┘ └───┬───┘ └────┬────┘
      │            │          │          │          │
      └────────────┴──────────┴──────────┴──────────┤
                                                     │
                          ┌──────────────────────────▼─┐
                          │     Storage Layer          │
                          │  (Pages, Files, Memory)    │
                          └──────────────┬─────────────┘
                                        │
                          ┌─────────────▼──────────────┐
                          │      WAL & Recovery        │
                          └──────────────┬─────────────┘
                                        │
                          ┌─────────────▼──────────────┐
                          │    Utils & Security        │
                          └────────────────────────────┘
```

## Integration Points

### 1. **Database Initialization**
1. Open/create storage files
2. Initialize WAL
3. Run recovery if needed
4. Load cache
5. Start background tasks (compaction, monitoring)
6. Initialize observability

### 2. **Write Path**
1. Transaction begin (optional)
2. Write to WAL
3. Update memtable (LSM) or B+Tree
4. Update cache
5. Trigger compaction if needed
6. Transaction commit (optional)

### 3. **Read Path**
1. Check cache
2. Search memtable (LSM)
3. Search SSTable levels (LSM) or B+Tree
4. Update cache with result
5. Return data

### 4. **Compaction Flow**
1. Monitor data fragmentation
2. Schedule compaction task
3. Merge/reorganize data
4. Update indexes
5. Clean up old files
6. Update statistics

### 5. **Recovery Flow**
1. Check for clean shutdown
2. Replay WAL if needed
3. Validate data integrity
4. Rebuild indexes if corrupted
5. Start normal operations

## Module Communication Patterns

### Event-Driven
- Compaction triggers
- Cache eviction
- Resource limits

### Request-Response
- Storage operations
- Transaction operations
- Query execution

### Pub-Sub
- Metrics collection
- Health monitoring
- Configuration updates

### Shared State
- Cache (thread-safe)
- Transaction manager
- Resource pools

## Configuration Dependencies

Each module can be configured through:
1. `LightningDbConfig` - Main configuration
2. Module-specific configs (e.g., `CompactionConfig`, `CacheConfig`)
3. Runtime adjustments via admin API

## Testing Strategy

### Unit Tests
- Each module has isolated unit tests
- Mock dependencies where needed
- Focus on module-specific logic

### Integration Tests
- Test module interactions
- Verify data flow between modules
- Test failure scenarios

### System Tests
- End-to-end scenarios
- Performance benchmarks
- Stress testing

## Future Enhancements

### Planned Modules
1. **Replication** - Multi-node support
2. **Sharding** - Data partitioning
3. **SQL Engine** - SQL query support
4. **Stream Processing** - Real-time data streams
5. **Time-Series** - Optimized time-series storage

### Module Improvements
1. **Adaptive Indexing** - Self-tuning indexes
2. **ML-based Prefetching** - Predictive caching
3. **Zero-Copy I/O** - Further performance gains
4. **Hardware Acceleration** - GPU/FPGA support