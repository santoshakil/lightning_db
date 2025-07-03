# Lightning DB Production Runbooks

## Table of Contents
1. [Emergency Procedures](#emergency-procedures)
2. [Performance Issues](#performance-issues)
3. [Data Integrity Issues](#data-integrity-issues)
4. [Resource Exhaustion](#resource-exhaustion)
5. [Backup and Recovery](#backup-and-recovery)
6. [Monitoring and Alerting](#monitoring-and-alerting)
7. [Maintenance Operations](#maintenance-operations)
8. [Troubleshooting Guide](#troubleshooting-guide)

---

## Emergency Procedures

### 🚨 Database Unresponsive

**Symptoms:**
- Application timeouts
- No response to queries
- High CPU/memory usage

**Immediate Actions:**
1. Check system resources:
   ```bash
   top -p $(pgrep -f lightning_db)
   iostat -x 1
   ```

2. Enable read-only mode:
   ```rust
   safety_guards.enable_read_only_mode();
   ```

3. Check circuit breaker state:
   ```rust
   if let Err(e) = safety_guards.circuit_breaker.check() {
       println!("Circuit breaker is OPEN: {}", e);
   }
   ```

4. If necessary, trigger emergency shutdown:
   ```rust
   safety_guards.emergency_shutdown();
   ```

**Recovery Steps:**
1. Identify root cause from logs
2. Fix underlying issue
3. Clear circuit breaker if needed
4. Disable read-only mode
5. Gradually ramp up traffic

### 🔥 Data Corruption Detected

**Symptoms:**
- Checksum mismatches
- Integrity check failures
- Inconsistent data

**Immediate Actions:**
1. Enable corruption guard:
   ```rust
   corruption_guard.report_corruption(location, details);
   ```

2. Run integrity check:
   ```bash
   ./target/release/examples/data_integrity_paranoia
   ```

3. Quarantine affected data:
   ```bash
   mkdir -p /var/lib/lightning_db/quarantine
   mv affected_files /var/lib/lightning_db/quarantine/
   ```

**Recovery Steps:**
1. Stop writes to affected regions
2. Run full integrity scan
3. Restore from backup if necessary
4. Rebuild corrupted indexes
5. Clear corruption flag after verification

---

## Performance Issues

### 🐌 Slow Query Performance

**Symptoms:**
- High query latency (p99 > 10ms)
- Degraded throughput
- Application timeouts

**Diagnostics:**
```rust
let snapshot = metrics.snapshot();
println!("Latency p99: {:?}", snapshot.performance.latency_p99);
println!("Cache hit rate: {:.1}%", snapshot.cache.hit_rate_percent);
```

**Actions:**
1. Check cache hit rate:
   - If < 70%: Increase cache size
   - Review access patterns

2. Check resource usage:
   ```rust
   let usage = resource_enforcer.get_usage();
   let percentages = usage.get_usage_percentages(&limits);
   ```

3. Enable prefetching if disabled:
   ```rust
   config.prefetch_enabled = true;
   ```

4. Check for lock contention:
   ```bash
   perf record -g -p $(pgrep -f lightning_db)
   perf report
   ```

### 📈 High Memory Usage

**Symptoms:**
- RSS > configured limits
- OOM killer activation risk
- System swapping

**Actions:**
1. Check current usage:
   ```rust
   let memory_mb = metrics.resources.memory_usage.load(Ordering::Relaxed) / 1024 / 1024;
   println!("Memory usage: {} MB", memory_mb);
   ```

2. Reduce cache size:
   ```rust
   config.cache_size = 32 * 1024 * 1024; // 32MB
   ```

3. Force cache eviction:
   ```rust
   // Trigger emergency cleanup
   resource_enforcer.emergency_cleanup();
   ```

4. Monitor for memory leaks:
   ```bash
   ./target/release/examples/production_stability_test
   ```

---

## Data Integrity Issues

### 🔍 Integrity Check Failures

**Procedure:**
1. Run comprehensive check:
   ```bash
   ./target/release/examples/chaos_engineering_suite
   ```

2. Identify affected pages:
   ```rust
   let report = verify_database_integrity(&db_path)?;
   for error in &report.errors {
       println!("Error: {} at {}", error.description, error.location);
   }
   ```

3. Attempt repair:
   ```rust
   let repair_report = verifier.repair(&integrity_report)?;
   println!("Repaired checksums: {}", repair_report.repaired_checksums);
   ```

4. If repair fails, restore from backup

### 🔐 Transaction Inconsistencies

**Symptoms:**
- MVCC violations
- Lost updates
- Phantom reads

**Actions:**
1. Check transaction state:
   ```rust
   println!("Active transactions: {}", metrics.transactions.active_transactions.load(Ordering::Relaxed));
   ```

2. Force transaction cleanup:
   ```rust
   transaction_manager.cleanup_abandoned_transactions();
   ```

3. Rebuild transaction log:
   ```rust
   wal.rebuild_from_pages(&page_manager)?;
   ```

---

## Resource Exhaustion

### 💾 Disk Space Full

**Symptoms:**
- Write failures
- WAL rotation failures
- Checkpoint failures

**Immediate Actions:**
1. Check disk usage:
   ```bash
   df -h /var/lib/lightning_db
   du -sh /var/lib/lightning_db/*
   ```

2. Rotate WAL manually:
   ```rust
   wal.force_rotation()?;
   ```

3. Run compaction:
   ```rust
   lsm_tree.force_major_compaction()?;
   ```

4. Delete old backups:
   ```bash
   find /backups -name "*.backup" -mtime +7 -delete
   ```

### 📁 File Descriptor Exhaustion

**Symptoms:**
- "Too many open files" errors
- Connection failures

**Actions:**
1. Check current usage:
   ```bash
   lsof -p $(pgrep -f lightning_db) | wc -l
   ```

2. Increase system limits:
   ```bash
   ulimit -n 65536
   echo "* soft nofile 65536" >> /etc/security/limits.conf
   ```

3. Close idle connections:
   ```rust
   connection_pool.close_idle_connections();
   ```

---

## Backup and Recovery

### 💾 Backup Procedures

**Regular Backup:**
```rust
// Acquire backup handle
let backup_handle = safety_guards.backup_guard.start_backup()?;

// Perform backup
let backup_path = format!("/backups/lightning_db_{}.backup", 
                         SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs());
database.backup_to(&backup_path)?;

// Handle automatically released
```

**Hot Backup (Online):**
1. Enable read-only mode temporarily
2. Create checkpoint
3. Copy database files
4. Resume normal operations

### 🔄 Recovery Procedures

**From Backup:**
```bash
# Stop application
systemctl stop myapp

# Move corrupted database
mv /var/lib/lightning_db /var/lib/lightning_db.corrupted

# Restore from backup
tar -xzf /backups/lightning_db_latest.tar.gz -C /var/lib/

# Verify integrity
./lightning_db_verify /var/lib/lightning_db

# Restart application
systemctl start myapp
```

**Point-in-Time Recovery:**
1. Restore base backup
2. Apply WAL logs up to target time
3. Verify consistency
4. Resume operations

---

## Monitoring and Alerting

### 📊 Key Metrics to Monitor

**Performance Metrics:**
- Latency percentiles (p50, p95, p99)
- Throughput (ops/sec)
- Cache hit rate
- Queue depth

**Resource Metrics:**
- Memory usage
- Disk usage
- File descriptors
- CPU usage

**Health Metrics:**
- Error rate
- Circuit breaker state
- WAL lag
- Replication lag

### 🚨 Alert Thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| Latency p99 | > 5ms | > 10ms |
| Cache Hit Rate | < 80% | < 60% |
| Memory Usage | > 80% | > 95% |
| Disk Usage | > 80% | > 95% |
| Error Rate | > 0.1% | > 1% |
| Circuit Breaker | Half-Open | Open |

**Prometheus Alerts:**
```yaml
groups:
  - name: lightning_db
    rules:
      - alert: HighLatency
        expr: lightning_db_latency_seconds{quantile="0.99"} > 0.01
        for: 5m
        annotations:
          summary: "High query latency detected"
          
      - alert: LowCacheHitRate
        expr: lightning_db_cache_hit_rate < 0.6
        for: 10m
        annotations:
          summary: "Cache hit rate below 60%"
          
      - alert: CircuitBreakerOpen
        expr: lightning_db_circuit_breaker_state == 2
        for: 1m
        annotations:
          summary: "Circuit breaker is OPEN"
```

---

## Maintenance Operations

### 🔧 Planned Maintenance

**Pre-Maintenance Checklist:**
1. Notify users
2. Enable maintenance mode
3. Stop background jobs
4. Create backup

**Procedure:**
```rust
// Enable maintenance mode
safety_guards.enable_maintenance_mode();

// Wait for active operations to complete
while metrics.performance.active_operations.load(Ordering::Relaxed) > 0 {
    thread::sleep(Duration::from_millis(100));
}

// Perform maintenance
// ... maintenance operations ...

// Disable maintenance mode
safety_guards.disable_maintenance_mode();
```

### 🔄 Rolling Upgrades

**Zero-Downtime Upgrade:**
1. Deploy new version to standby
2. Sync data to standby
3. Switch traffic to standby
4. Upgrade primary
5. Switch back if needed

**Version Compatibility Check:**
```rust
if !Database::check_version_compatibility(&db_path)? {
    eprintln!("Version incompatible - migration required");
    return Err(Error::InvalidDatabase);
}
```

---

## Troubleshooting Guide

### 🐛 Common Issues

**Issue: "Circuit breaker is OPEN"**
- **Cause**: Too many consecutive failures
- **Solution**: 
  1. Check logs for root cause
  2. Fix underlying issue
  3. Wait for timeout or manually reset

**Issue: "Resource limit exceeded"**
- **Cause**: Hitting configured limits
- **Solution**:
  1. Check current usage
  2. Increase limits if appropriate
  3. Optimize queries/operations

**Issue: "Checksum mismatch"**
- **Cause**: Data corruption or hardware issue
- **Solution**:
  1. Run integrity check
  2. Repair if possible
  3. Restore from backup if needed

**Issue: "Transaction timeout"**
- **Cause**: Long-running transactions
- **Solution**:
  1. Identify long transactions
  2. Break into smaller operations
  3. Increase timeout if necessary

### 🔍 Diagnostic Commands

**Database Status:**
```bash
# Check process
ps aux | grep lightning_db

# Check connections
netstat -an | grep ESTABLISHED | grep 8080

# Check disk I/O
iotop -p $(pgrep -f lightning_db)

# Check memory map
pmap -x $(pgrep -f lightning_db)
```

**Performance Analysis:**
```bash
# CPU profiling
perf record -F 99 -p $(pgrep -f lightning_db) -g -- sleep 30
perf report

# Trace system calls
strace -c -p $(pgrep -f lightning_db)

# Analyze lock contention
/usr/share/bcc/tools/offcputime -p $(pgrep -f lightning_db) 30
```

---

## Quick Reference

### 🚀 Emergency Commands

```rust
// Read-only mode
safety_guards.enable_read_only_mode();

// Maintenance mode
safety_guards.enable_maintenance_mode();

// Graceful shutdown
safety_guards.start_graceful_shutdown();

// Emergency shutdown
safety_guards.emergency_shutdown();

// Clear corruption
corruption_guard.clear_corruption_flag();

// Force checkpoint
database.checkpoint()?;

// Compact database
database.compact()?;
```

### 📞 Escalation Path

1. **Level 1**: Application team
   - Basic troubleshooting
   - Log analysis
   - Restart services

2. **Level 2**: Database team
   - Performance tuning
   - Integrity checks
   - Backup/restore

3. **Level 3**: Engineering team
   - Code-level debugging
   - Architecture changes
   - Hardware issues

---

## Appendix

### Configuration Reference

```toml
[lightning_db]
# Performance
cache_size = 67108864  # 64MB
prefetch_enabled = true
compression_enabled = true

# Safety
max_concurrent_operations = 1000
max_memory_bytes = 1073741824  # 1GB
max_write_throughput_bytes_per_sec = 104857600  # 100MB/s

# Monitoring
metrics_enabled = true
metrics_port = 9090
log_level = "info"
```

### Useful Scripts

**Health Check Script:**
```bash
#!/bin/bash
# lightning_db_health_check.sh

DB_PATH="/var/lib/lightning_db"
METRICS_URL="http://localhost:9090/metrics"

# Check if process is running
if ! pgrep -f lightning_db > /dev/null; then
    echo "CRITICAL: Lightning DB not running"
    exit 2
fi

# Check metrics endpoint
if ! curl -s $METRICS_URL > /dev/null; then
    echo "WARNING: Metrics endpoint not responding"
    exit 1
fi

# Check error rate
ERROR_RATE=$(curl -s $METRICS_URL | grep lightning_db_errors_total | awk '{print $2}')
if [ "$ERROR_RATE" -gt "100" ]; then
    echo "WARNING: High error rate: $ERROR_RATE"
    exit 1
fi

echo "OK: Lightning DB healthy"
exit 0
```

---

**Last Updated**: 2024-12-XX
**Version**: 1.0.0
**Maintainer**: Lightning DB Team