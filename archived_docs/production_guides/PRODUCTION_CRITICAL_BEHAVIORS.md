# Lightning DB Production-Critical Behaviors

This document outlines critical behaviors and operational characteristics of Lightning DB that must be understood for production deployments.

## Table of Contents

1. [Data Durability Guarantees](#data-durability-guarantees)
2. [Transaction ACID Properties](#transaction-acid-properties)
3. [Crash Recovery Behavior](#crash-recovery-behavior)
4. [Memory Management](#memory-management)
5. [Concurrent Access Patterns](#concurrent-access-patterns)
6. [Write Performance Characteristics](#write-performance-characteristics)
7. [Read Performance Characteristics](#read-performance-characteristics)
8. [Failure Modes and Recovery](#failure-modes-and-recovery)
9. [Resource Limits](#resource-limits)
10. [Monitoring and Observability](#monitoring-and-observability)

## Data Durability Guarantees

### Write-Ahead Logging (WAL)

Lightning DB uses a Write-Ahead Log (WAL) to ensure data durability:

- **Default Mode**: Asynchronous WAL writes for maximum performance
- **Sync Mode**: Can be configured for synchronous WAL writes with `WalSyncMode::Sync`
- **Group Commit**: Batches multiple transactions for efficient I/O when sync mode is enabled

```rust
// Configure for maximum durability
let config = LightningDbConfig {
    use_improved_wal: true,
    wal_sync_mode: WalSyncMode::Sync,
    ..Default::default()
};
```

### Checkpointing

- Periodic checkpoints write dirty pages to disk
- Checkpoint interval is configurable
- During checkpoint, database remains fully operational
- Incomplete checkpoints are automatically recovered

### Page-Level Checksums

- All pages include CRC32 checksums
- Checksums are verified on read when `verify_checksums_on_read` is enabled
- Corrupted pages are detected and reported
- Failed checksum verification prevents data corruption propagation

## Transaction ACID Properties

### Atomicity

- Transactions are all-or-nothing operations
- Uses MVCC (Multi-Version Concurrency Control) for isolation
- Atomic reservation mechanism prevents partial commits
- Transaction rollback is instantaneous

### Consistency

- B+Tree structure maintains sorted order invariants
- Foreign key constraints are not enforced (application responsibility)
- Page splits and merges maintain tree balance
- Concurrent modifications use optimistic concurrency control

### Isolation

- **Read Committed** isolation level by default
- Snapshot isolation for read operations
- Writers don't block readers
- Readers don't block writers
- Write-write conflicts are detected and handled

### Durability

- Committed transactions are guaranteed durable after sync
- WAL ensures crash recovery
- Page writes use atomic operations
- fsync() called based on configured sync mode

## Crash Recovery Behavior

### Recovery Process

1. **WAL Replay**: Uncommitted transactions are rolled forward
2. **Page Verification**: All pages are checksum-verified
3. **Index Reconstruction**: B+Tree structure is validated
4. **Transaction Cleanup**: Incomplete transactions are rolled back

### Recovery Time

- Recovery time is proportional to:
  - Size of WAL since last checkpoint
  - Number of dirty pages
  - Number of incomplete transactions
- Typical recovery: <1 second for most workloads
- Large databases: Recovery may take several seconds

### Data Loss Scenarios

- **Clean shutdown**: No data loss
- **Power failure with sync WAL**: No committed data loss
- **Power failure with async WAL**: Possible loss of last few milliseconds
- **Storage failure**: Data loss depends on storage redundancy

## Memory Management

### Page Cache

- Adaptive Replacement Cache (ARC) algorithm
- Automatic hot/cold data segregation
- Configurable cache size via `cache_size`
- Zero-copy optimization for small keys

### Memory Pools

- Thread-local allocation pools reduce contention
- Configurable memory limits
- Automatic memory pressure handling
- Emergency memory reserves for critical operations

### Out-of-Memory Behavior

- Graceful degradation under memory pressure
- Non-essential caches are evicted first
- Write operations prioritized over reads
- OOM killer protection through memory reservations

## Concurrent Access Patterns

### Reader Scalability

- Multiple readers can access data simultaneously
- Lock-free data structures for hot paths
- Reader performance scales linearly with CPU cores
- No reader starvation under write load

### Writer Coordination

- Single writer per key at any moment
- Optimistic concurrency control for conflicts
- Automatic retry with exponential backoff
- Fair scheduling prevents writer starvation

### Lock Granularity

- Page-level locking for B+Tree operations
- Key-level locking for transactions
- No table-level locks
- Deadlock detection and resolution

## Write Performance Characteristics

### Write Amplification

- **B+Tree writes**: ~2-3x amplification
- **LSM writes**: ~10-15x amplification (when enabled)
- **WAL writes**: 1x (data written once)
- **Total**: 3-4x without LSM, 11-16x with LSM

### Write Patterns

- Sequential writes are optimized
- Random writes benefit from write buffering
- Batch writes significantly improve throughput
- Small writes are aggregated automatically

### Write Throttling

- Automatic throttling under memory pressure
- Configurable write rate limits
- Background compaction doesn't block writes
- Write stalls are monitored and reported

## Read Performance Characteristics

### Read Amplification

- **Point queries**: 1-3 page reads typically
- **Range scans**: Linear in result size
- **Cold reads**: May require disk I/O
- **Hot reads**: Served from memory

### Read Patterns

- Sequential reads benefit from prefetching
- Random reads optimized by caching
- Index-only scans avoid data page reads
- Bloom filters reduce unnecessary I/O (when enabled)

### Read Latency

- **Memory hits**: <1 microsecond
- **SSD reads**: 50-200 microseconds
- **HDD reads**: 5-10 milliseconds
- **Network reads**: Depends on network latency

## Failure Modes and Recovery

### Detectable Failures

1. **Checksum Mismatches**
   - Automatic page re-read attempted
   - Error reported to application
   - Page marked as corrupted

2. **Transaction Conflicts**
   - Automatic retry with backoff
   - Configurable retry limits
   - Clear error reporting

3. **Space Exhaustion**
   - Graceful error handling
   - No corruption on disk full
   - Recovery after space freed

4. **Memory Exhaustion**
   - Controlled degradation
   - Cache eviction
   - Operation failure with recovery

### Silent Failures

- **Bit rot**: Detected by checksums
- **Partial writes**: Detected by page structure validation
- **OS bugs**: May cause corruption (use ECC memory)
- **Hardware failures**: Use RAID/replication

## Resource Limits

### Hard Limits

- **Key size**: Maximum 1KB (configurable)
- **Value size**: Maximum 4GB per value
- **Database size**: Limited by filesystem (typically 16TB+)
- **Open files**: ~1000 file descriptors
- **Memory usage**: Configurable, defaults to available RAM

### Soft Limits

- **Concurrent transactions**: 10,000 (configurable)
- **Pages per operation**: Unlimited but affects latency
- **Keys per page**: ~50-100 depending on size
- **Tree depth**: Typically 3-5 levels for billions of keys

### Performance Cliffs

- Performance degrades gracefully until:
  - Memory pressure causes excessive paging
  - Transaction conflicts exceed retry limits
  - I/O bandwidth saturation
  - CPU saturation on single writer thread

## Monitoring and Observability

### Key Metrics

1. **Health Metrics**
   - `db.health.score`: Overall health (0.0-1.0)
   - `db.errors.total`: Error count by type
   - `db.recovery.duration`: Last recovery time

2. **Performance Metrics**
   - `db.operations.throughput`: Ops/sec by type
   - `db.operations.latency`: P50/P95/P99 latencies
   - `db.cache.hit_rate`: Cache effectiveness

3. **Resource Metrics**
   - `db.memory.usage`: Current memory usage
   - `db.disk.usage`: Storage consumption
   - `db.connections.active`: Active connections

### Health Checks

```rust
// Built-in health check endpoint
let health = db.health_check().await?;
if health.status != HealthStatus::Healthy {
    // Take corrective action
}
```

### Performance Profiling

- CPU profiling via `perf` integration
- Memory profiling with allocation tracking
- I/O profiling through system metrics
- Lock contention analysis

## Production Recommendations

### Configuration

1. **For Durability**
   ```rust
   wal_sync_mode: WalSyncMode::Sync,
   enable_checksums: true,
   verify_checksums_on_read: true,
   ```

2. **For Performance**
   ```rust
   cache_size: system_memory * 0.5,
   prefetch_enabled: true,
   compression_enabled: true,
   ```

3. **For Reliability**
   ```rust
   max_active_transactions: 1000,
   write_batch_size: 1000,
   consistency_config: ConsistencyConfig::strict(),
   ```

### Operational Procedures

1. **Backup Strategy**
   - Use snapshot backups during low activity
   - Continuous WAL archiving for PITR
   - Test restore procedures regularly

2. **Monitoring**
   - Set alerts on error rates
   - Monitor resource usage trends
   - Track performance regression

3. **Maintenance**
   - Regular integrity checks
   - Periodic compaction (if using LSM)
   - Update statistics for query optimizer

### Capacity Planning

- **Storage**: Plan for 3-4x data size for overhead
- **Memory**: Minimum 10% of dataset for good performance
- **CPU**: 4+ cores recommended for concurrent workloads
- **Network**: Consider replication bandwidth requirements

## Troubleshooting Guide

### Common Issues

1. **Slow Queries**
   - Check cache hit rate
   - Verify index usage
   - Monitor I/O patterns

2. **High Memory Usage**
   - Review cache configuration
   - Check for memory leaks
   - Monitor transaction duration

3. **Write Stalls**
   - Check disk space
   - Monitor compaction (if LSM enabled)
   - Verify WAL configuration

4. **Corruption Errors**
   - Run integrity check
   - Review hardware health
   - Check filesystem errors

### Emergency Procedures

1. **Database Won't Start**
   ```bash
   # Try recovery mode
   lightning_db recover --force /path/to/db
   
   # If recovery fails, restore from backup
   lightning_db restore --from-backup /backup/path
   ```

2. **Performance Emergency**
   ```rust
   // Disable non-critical features
   db.disable_compression();
   db.disable_prefetch();
   db.reduce_cache_size(current_size / 2);
   ```

3. **Data Corruption**
   ```rust
   // Run integrity check
   let report = db.verify_integrity().await?;
   
   // Attempt repair
   if !report.is_clean() {
       db.repair_integrity(&report).await?;
   }
   ```

## Version Compatibility

- **Forward compatible**: Newer versions can read older formats
- **Backward compatible**: Limited to same major version
- **Migration required**: Major version upgrades
- **Online migration**: Supported with dual-write mode

## Security Considerations

1. **Encryption**
   - At-rest encryption available
   - Key rotation supported
   - Hardware acceleration used when available

2. **Access Control**
   - File system permissions enforced
   - No built-in authentication
   - Application-level security required

3. **Audit Logging**
   - Optional operation logging
   - Configurable detail level
   - Integration with external audit systems

This document represents the critical operational knowledge required for running Lightning DB in production environments. Regular review and updates are recommended as the system evolves.