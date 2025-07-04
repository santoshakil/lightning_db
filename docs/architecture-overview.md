# Lightning DB Architecture Overview

## Introduction

Lightning DB is a high-performance embedded key-value database designed for sub-microsecond latency and millions of operations per second. Built in Rust, it combines the best aspects of B+Trees and LSM trees in a hybrid architecture optimized for modern hardware.

## Design Philosophy

### Core Principles
1. **Performance First**: Optimized for speed without sacrificing correctness
2. **Memory Safety**: Leverage Rust's safety guarantees
3. **Hybrid Architecture**: Combine B+Tree and LSM advantages
4. **Minimal Footprint**: <5MB binary, configurable memory usage
5. **Zero-Copy Operations**: Minimize allocations in hot paths

### Target Workloads
- **High-frequency trading**: Ultra-low latency reads
- **Edge computing**: Small binary, low resource usage
- **Real-time analytics**: Fast mixed read/write workloads
- **IoT applications**: Resource-constrained environments

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│                  Lightning DB API                           │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │   CRUD   │ │   Batch  │ │   Scan   │ │   Index  │      │
│  │Operations│ │Operations│ │  Ranges  │ │ Queries  │      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘      │
├─────────────────────────────────────────────────────────────┤
│                   Transaction Layer                         │
│  ┌──────────────────┐ ┌─────────────────────────────────┐  │
│  │  MVCC Manager    │ │      Version Store              │  │
│  │  - Timestamps    │ │      - Snapshot Isolation       │  │
│  │  - Conflicts     │ │      - Version Chains           │  │
│  │  - Deadlocks     │ │      - Garbage Collection       │  │
│  └──────────────────┘ └─────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                     Storage Engine                          │
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐  │
│  │    B+Tree       │ │    LSM Tree     │ │    Cache     │  │
│  │  - Index data   │ │  - Write opt    │ │  - ARC algo  │  │
│  │  - Range scans  │ │  - Compaction   │ │  - Hot data  │  │
│  │  - Point lookups│ │  - Compression  │ │  - Prefetch  │  │
│  └─────────────────┘ └─────────────────┘ └──────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                    Persistence Layer                        │
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐  │
│  │      WAL        │ │  Page Manager   │ │   Backup     │  │
│  │  - Durability   │ │  - 4KB pages    │ │  - Export    │  │
│  │  - Recovery     │ │  - Memory maps  │ │  - Import    │  │
│  │  - Group commit │ │  - Allocations  │ │  - Verify    │  │
│  └─────────────────┘ └─────────────────┘ └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Storage Engine

#### B+Tree Component
- **Purpose**: Fast point lookups and range scans
- **Node Structure**: 4KB pages optimized for cache lines
- **Fanout**: Dynamic based on key/value sizes
- **Features**:
  - Copy-on-write for concurrency
  - Bulk loading optimization
  - Iterator support
  - Prefix compression

```rust
// B+Tree optimizations
struct BPlusTree {
    root_page_id: u32,
    height: u32,
    page_manager: Arc<PageManager>,
    node_cache: LRUCache<u32, Node>,
}

// SIMD-optimized key comparisons
fn find_key_simd(keys: &[u8], target: &[u8]) -> Option<usize> {
    // Use AVX2 for parallel key comparisons
}
```

#### LSM Tree Component
- **Purpose**: Optimized write performance
- **Levels**: L0 (memory) → L1 (disk) → L2+ (compacted)
- **Compaction**: Size-tiered and leveled strategies
- **Features**:
  - Delta compression between levels
  - Bloom filters for negative lookups
  - Background compaction threads
  - Write amplification optimization

```rust
struct LSMTree {
    memtable: SkipList<Vec<u8>, Vec<u8>>,
    levels: Vec<SSTable>,
    compaction_scheduler: CompactionScheduler,
    bloom_filters: BloomFilterCache,
}
```

### 2. Caching System

#### ARC (Adaptive Replacement Cache)
- **Algorithm**: Balances recency and frequency
- **Memory Management**: Configurable size limits
- **Hit Rates**: >95% for typical workloads
- **Features**:
  - Ghost lists for metadata tracking
  - Dynamic adaptation to workload
  - Lock-free operations on hot paths

```rust
struct ARCCache {
    t1: LRUList,      // Recent pages
    t2: LRUList,      // Frequent pages  
    b1: GhostList,    // Recently evicted from T1
    b2: GhostList,    // Recently evicted from T2
    target: usize,    // Adaptive target for T1 size
}
```

### 3. Transaction System

#### MVCC (Multi-Version Concurrency Control)
- **Isolation**: Snapshot isolation by default
- **Timestamps**: Logical clock for ordering
- **Conflict Detection**: Read/write set validation
- **Features**:
  - Optimistic concurrency control
  - Deadlock detection and prevention
  - Version garbage collection
  - Read-only transaction optimization

```rust
struct Transaction {
    id: u64,
    read_timestamp: u64,
    write_timestamp: Option<u64>,
    read_set: HashSet<(Vec<u8>, u64)>,  // (key, version)
    write_set: HashMap<Vec<u8>, WriteOp>,
    status: TransactionStatus,
}
```

#### Version Store
- **Structure**: Key → Version chain (linked list)
- **Garbage Collection**: Background cleanup of old versions
- **Memory Management**: LRU eviction of cold version chains
- **Optimization**: Single-version fast path for non-concurrent keys

### 4. Write-Ahead Logging (WAL)

#### Durability Guarantees
- **Modes**: Sync, periodic, async
- **Group Commit**: Batch multiple transactions
- **Recovery**: Replay operations after crash
- **Features**:
  - Checksum validation
  - Compression for reduced I/O
  - Parallel recovery
  - Incremental checkpointing

```rust
enum WALOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    TransactionBegin { tx_id: u64 },
    TransactionCommit { tx_id: u64 },
    TransactionAbort { tx_id: u64 },
    Checkpoint { lsn: u64 },
}
```

### 5. Page Management

#### Memory-Mapped I/O
- **Page Size**: 4KB (OS page size aligned)
- **Allocation**: Free page list management
- **Caching**: OS buffer cache + application cache
- **Features**:
  - Zero-copy reads when possible
  - Lazy loading of pages
  - Periodic sync for durability
  - Copy-on-write for concurrency

```rust
struct PageManager {
    file: File,
    mmap: Mmap,
    free_pages: BitSet,
    allocated_pages: u32,
    page_cache: HashMap<u32, Arc<Page>>,
}
```

## Performance Optimizations

### 1. Lock-Free Operations
- **Hot Paths**: Reads avoid locks using atomic operations
- **Structures**: Lock-free queues for background work
- **Memory Ordering**: Careful use of acquire/release semantics

### 2. SIMD Optimizations
- **Key Comparisons**: AVX2 for parallel string comparison
- **Checksums**: Hardware-accelerated CRC32
- **Compression**: Vectorized LZ4/Zstd

### 3. Zero-Copy Design
- **Small Keys**: Stack allocation for keys <32 bytes
- **Memory Layout**: Minimize pointer chasing
- **Serialization**: Direct memory mapping where possible

### 4. Thread-Local Optimization
- **Caches**: Per-thread allocation pools
- **Buffers**: Reusable memory for operations
- **Metrics**: Lock-free statistics collection

## Memory Management

### Memory Layout
```
┌─────────────────────────────────────────┐
│              User Data                   │
│  ┌─────────────┐ ┌─────────────────────┐ │
│  │   B+Tree    │ │     LSM Tree        │ │
│  │    Pages    │ │   MemTable + SSTables│ │
│  └─────────────┘ └─────────────────────┘ │
├─────────────────────────────────────────┤
│              System Data                 │
│  ┌─────────────┐ ┌─────────────────────┐ │
│  │   Caches    │ │   Version Store     │ │
│  │  (ARC LRU)  │ │   (TX Snapshots)    │ │
│  └─────────────┘ └─────────────────────┘ │
├─────────────────────────────────────────┤
│             Metadata                     │
│  ┌─────────────┐ ┌─────────────────────┐ │
│  │ Page Mgmt   │ │      WAL Log        │ │
│  │ Free Lists  │ │   Operation Buffer  │ │
│  └─────────────┘ └─────────────────────┘ │
└─────────────────────────────────────────┘
```

### Resource Limits
```rust
struct MemoryConfig {
    max_memory: usize,        // Hard limit (2GB default)
    cache_size: usize,        // Page cache size
    version_retention: Duration, // How long to keep old versions
    memtable_size: usize,     // LSM memory table size
    wal_buffer_size: usize,   // WAL batch buffer
}
```

## Concurrency Model

### Read Operations
1. **Lock-free path**: For cached data
2. **Shared locks**: For disk I/O
3. **MVCC snapshots**: For transaction isolation
4. **Prefetching**: Async read-ahead

### Write Operations
1. **WAL first**: Ensure durability
2. **Memory update**: Update in-memory structures
3. **Async compaction**: Background optimization
4. **Conflict detection**: Validate against concurrent transactions

### Background Tasks
- **Compaction**: LSM tree level merging
- **Version cleanup**: Old snapshot garbage collection  
- **Cache eviction**: Memory pressure management
- **Checkpointing**: WAL truncation and sync

## Secondary Indexes

### Index Types
- **B+Tree**: General purpose, range queries
- **Hash**: Exact matches, fastest lookups
- **Composite**: Multi-column indexes

### Index Management
```rust
struct IndexManager {
    indexes: HashMap<String, Box<dyn Index>>,
    query_planner: QueryPlanner,
    statistics: IndexStatistics,
}

trait Index {
    fn insert(&self, key: &IndexKey, primary_key: &[u8]) -> Result<()>;
    fn delete(&self, key: &IndexKey, primary_key: &[u8]) -> Result<()>;
    fn scan(&self, range: &IndexRange) -> Result<Vec<Vec<u8>>>;
}
```

## Query Processing

### Query Planner
- **Cost-based optimization**: Choose best index
- **Join algorithms**: Nested loop, hash join
- **Statistics**: Cardinality estimation
- **Caching**: Plan cache for repeated queries

### Execution Engine
- **Iterator model**: Pull-based execution
- **Vectorization**: Process multiple rows at once
- **Predicate pushdown**: Filter early in pipeline

## Storage Format

### Page Layout (4KB)
```
┌─────────────────────────────────────────┐
│           Page Header (64B)             │
│  magic(4) | type(1) | flags(1) | ...   │
├─────────────────────────────────────────┤
│               Slot Array                │
│  [offset1, len1] [offset2, len2] ...    │
├─────────────────────────────────────────┤
│              Free Space                 │
├─────────────────────────────────────────┤
│               Record Data               │
│  [key1, val1] [key2, val2] ...         │
└─────────────────────────────────────────┘
```

### Key-Value Encoding
- **Variable length**: Efficient space usage
- **Prefix compression**: Common prefixes shared
- **Type annotations**: Support different data types

## Monitoring and Observability

### Metrics Collection
```rust
struct DatabaseMetrics {
    // Performance
    read_ops_per_sec: Counter,
    write_ops_per_sec: Counter,
    read_latency_histogram: Histogram,
    write_latency_histogram: Histogram,
    
    // Resources
    memory_usage: Gauge,
    disk_usage: Gauge,
    cache_hit_rate: Gauge,
    
    // Health
    error_count: Counter,
    transaction_conflicts: Counter,
    compaction_time: Histogram,
}
```

### Health Checks
- **Memory pressure**: Alert before OOM
- **Disk space**: Warn at 80% full
- **Performance**: Alert on latency spikes
- **Errors**: Alert on error rate >0.1%

## Configuration

### Production Configuration
```rust
let config = LightningDbConfig {
    // Performance
    page_size: 4096,
    cache_size: 1_000_000_000,    // 1GB
    mmap_size: Some(2_000_000_000), // 2GB
    
    // Reliability  
    wal_sync_mode: WalSyncMode::Sync,
    enable_safety_guards: true,
    enable_paranoia_checks: true,
    
    // Optimization
    compression_enabled: true,
    compression_type: 1, // Zstd
    prefetch_enabled: false, // Disable for predictable latency
    
    // Limits
    max_active_transactions: 1000,
    write_batch_size: 1000,
};
```

## Future Architecture

### Planned Enhancements
1. **Distributed Mode**: Raft consensus for replication
2. **Column Families**: Logical data separation
3. **Time-Series Optimization**: Specialized storage for metrics
4. **SQL Layer**: Query interface on top of KV store
5. **GPU Acceleration**: CUDA kernels for data processing

### Scalability Roadmap
- **Sharding**: Horizontal partitioning
- **Read Replicas**: Scale read workloads
- **Cross-DC Replication**: Geographic distribution
- **Cloud Native**: Kubernetes operator

---

This architecture delivers world-class performance through careful optimization at every layer while maintaining the simplicity and reliability expected from an embedded database. The hybrid design combines the best aspects of different storage engines while Rust's safety guarantees prevent entire classes of bugs common in systems programming.

*Last Updated: Architecture Documentation v1.0*  
*Next Review: After major architecture changes*  
*Owner: Lightning DB Architecture Team*