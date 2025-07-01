# Lightning DB Troubleshooting Guide

## Overview

This guide provides systematic approaches to diagnosing and resolving common issues with Lightning DB. It covers performance problems, data corruption, configuration issues, and operational challenges.

## Quick Diagnostic Commands

```bash
# System health check
lightning_db health-check --detailed

# Performance baseline test
lightning_db benchmark --quick --compare-baseline

# Integrity verification
lightning_db integrity-check --full

# Configuration validation
lightning_db config --validate

# System compatibility check
lightning_db system-check --verbose

# Log analysis
lightning_db logs --analyze --last-24h
```

## Common Issues and Solutions

### 1. Performance Issues

#### Symptom: Slow Read Performance

**Diagnosis:**
```bash
# Check cache hit rates
lightning_db metrics | grep cache_hit_rate

# Monitor I/O patterns
iotop -p $(pgrep lightning_db)

# Check memory usage
lightning_db memory-stats
```

**Common Causes & Solutions:**

1. **Low Cache Hit Rate**
   ```toml
   # Increase cache size in config
   [database]
   cache_size = "8GB"  # Increase based on available RAM
   ```

2. **Insufficient Prefetching**
   ```toml
   [performance]
   prefetch_enabled = true
   prefetch_distance = 128  # Increase for sequential workloads
   prefetch_workers = 8     # Match CPU cores
   ```

3. **Disk I/O Bottleneck**
   ```bash
   # Check disk performance
   iostat -x 1
   
   # Use faster storage (NVMe SSD)
   # Optimize file system (use ext4 with noatime)
   mount -o remount,noatime /data
   ```

#### Symptom: Slow Write Performance

**Diagnosis:**
```bash
# Check WAL sync behavior
lightning_db wal-stats

# Monitor write batch sizes
lightning_db metrics | grep batch_size

# Check transaction commit rates
lightning_db metrics | grep transaction_rate
```

**Solutions:**

1. **Optimize WAL Sync Mode**
   ```toml
   [database]
   wal_sync_mode = "periodic"
   wal_sync_interval = "100ms"  # Balance safety vs performance
   ```

2. **Increase Batch Sizes**
   ```toml
   [performance]
   write_batch_size = 10000  # Larger batches for bulk operations
   ```

3. **Use Faster Storage**
   ```bash
   # Check current disk performance
   fio --name=randwrite --ioengine=libaio --iodepth=1 --rw=randwrite \
       --bs=4k --size=4G --numjobs=1 --runtime=60 --group_reporting
   ```

#### Symptom: High Memory Usage

**Diagnosis:**
```bash
# Memory breakdown
lightning_db memory-stats --detailed

# Check for memory leaks
valgrind --tool=memcheck --leak-check=full lightning_db

# Monitor RSS over time
while true; do
    ps -o pid,rss,vsz,comm -p $(pgrep lightning_db)
    sleep 10
done
```

**Solutions:**

1. **Adjust Cache Size**
   ```toml
   [database]
   cache_size = "4GB"  # Reduce if system has limited RAM
   ```

2. **Enable Compression**
   ```toml
   [performance]
   compression_enabled = true
   compression_type = "lz4"  # Lower CPU overhead than zstd
   ```

3. **Tune Memory Allocator**
   ```bash
   # Use jemalloc for better memory management
   export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
   ```

### 2. Data Integrity Issues

#### Symptom: Data Corruption Detected

**Diagnosis:**
```bash
# Full integrity check
lightning_db integrity-check --full --verbose

# Check specific corruption type
lightning_db integrity-check --check-type=checksum
lightning_db integrity-check --check-type=structure
lightning_db integrity-check --check-type=consistency
```

**Recovery Steps:**

1. **Stop Database Immediately**
   ```bash
   systemctl stop lightning-db
   ```

2. **Backup Current State**
   ```bash
   cp -r /data/lightning_db /backup/corrupted-$(date +%s)
   ```

3. **Attempt Repair**
   ```bash
   # Try automatic repair
   lightning_db repair --auto-fix

   # Manual repair for specific issues
   lightning_db repair --fix-checksums
   lightning_db repair --rebuild-index
   ```

4. **Restore from Backup (if repair fails)**
   ```bash
   # Restore from latest clean backup
   lightning_db restore --backup-file /backup/latest.backup
   ```

#### Symptom: Index Corruption

**Diagnosis:**
```bash
# Check B+Tree structure
lightning_db btree-check --verbose

# Verify index consistency
lightning_db index-check --all-indexes
```

**Solutions:**

1. **Rebuild Index**
   ```bash
   lightning_db index-rebuild --index-name=primary
   lightning_db index-rebuild --all-indexes
   ```

2. **Recreate from WAL**
   ```bash
   # Replay WAL to rebuild state
   lightning_db wal-replay --from-checkpoint
   ```

### 3. Connection and Network Issues

#### Symptom: Connection Timeouts

**Diagnosis:**
```bash
# Test connectivity
telnet localhost 9090

# Check network latency
ping -c 10 database-server

# Monitor connection pool
netstat -an | grep :9090
```

**Solutions:**

1. **Increase Timeout Values**
   ```toml
   [server]
   connection_timeout = "60s"
   request_timeout = "120s"
   keepalive_timeout = "300s"
   ```

2. **Check Network Configuration**
   ```bash
   # Verify firewall rules
   iptables -L | grep 9090
   
   # Check TCP buffer sizes
   sysctl net.core.rmem_max
   sysctl net.core.wmem_max
   ```

3. **Optimize Connection Pool**
   ```toml
   [server]
   max_connections = 1000
   min_idle_connections = 10
   connection_pool_timeout = "30s"
   ```

#### Symptom: TLS Certificate Issues

**Diagnosis:**
```bash
# Test TLS connection
openssl s_client -connect localhost:9090

# Verify certificate
openssl x509 -in /etc/ssl/certs/lightning_db.crt -text -noout

# Check certificate chain
openssl verify -CAfile /etc/ssl/certs/ca.crt /etc/ssl/certs/lightning_db.crt
```

**Solutions:**

1. **Regenerate Certificates**
   ```bash
   # Generate new server certificate
   openssl req -new -key server.key -out server.csr
   openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -out server.crt
   ```

2. **Update Certificate Configuration**
   ```toml
   [security]
   tls_cert_file = "/etc/ssl/certs/lightning_db.crt"
   tls_key_file = "/etc/ssl/private/lightning_db.key"
   tls_ca_file = "/etc/ssl/certs/ca.crt"
   ```

### 4. Transaction Issues

#### Symptom: Transaction Deadlocks

**Diagnosis:**
```bash
# Monitor active transactions
lightning_db transaction-stats

# Check for long-running transactions
lightning_db active-transactions --sort-by-duration

# Analyze deadlock patterns
grep "deadlock" /var/log/lightning_db/app.log
```

**Solutions:**

1. **Reduce Transaction Scope**
   ```rust
   // Keep transactions short
   let tx_id = db.begin_transaction()?;
   // Minimal operations here
   db.commit_transaction(tx_id)?;
   ```

2. **Implement Retry Logic**
   ```rust
   use std::time::Duration;
   use std::thread::sleep;
   
   for attempt in 0..3 {
       match db.begin_transaction() {
           Ok(tx_id) => {
               // Transaction logic
               match db.commit_transaction(tx_id) {
                   Ok(_) => break,
                   Err(Error::DeadlockDetected) if attempt < 2 => {
                       sleep(Duration::from_millis(100 * (attempt + 1)));
                       continue;
                   }
                   Err(e) => return Err(e),
               }
           }
           Err(e) => return Err(e),
       }
   }
   ```

3. **Adjust Transaction Timeout**
   ```toml
   [transaction]
   timeout = "30s"
   deadlock_detection_interval = "1s"
   max_active_transactions = 1000
   ```

#### Symptom: Transaction Log Full

**Diagnosis:**
```bash
# Check WAL size
du -h /data/lightning_db/wal/

# Monitor WAL growth rate
watch -n 5 'du -h /data/lightning_db/wal/'

# Check checkpoint frequency
grep "checkpoint" /var/log/lightning_db/app.log | tail -10
```

**Solutions:**

1. **Force Checkpoint**
   ```bash
   lightning_db checkpoint --force
   ```

2. **Adjust Checkpoint Settings**
   ```toml
   [database]
   checkpoint_interval = "2m"  # More frequent checkpoints
   max_wal_size = "1GB"       # Smaller WAL files
   ```

3. **Clean Up Old WAL Files**
   ```bash
   lightning_db wal-cleanup --keep-days=7
   ```

### 5. Configuration Issues

#### Symptom: Invalid Configuration

**Diagnosis:**
```bash
# Validate configuration
lightning_db config --validate --file=/etc/lightning_db/config.toml

# Check configuration syntax
toml-check /etc/lightning_db/config.toml

# Compare with defaults
lightning_db config --show-defaults > default-config.toml
diff /etc/lightning_db/config.toml default-config.toml
```

**Solutions:**

1. **Generate Valid Configuration**
   ```bash
   # Create template configuration
   lightning_db config --generate-template > config-template.toml
   
   # Merge with existing config
   lightning_db config --merge config-template.toml /etc/lightning_db/config.toml
   ```

2. **Fix Common Configuration Errors**
   ```toml
   # Ensure correct data types
   [database]
   cache_size = "4GB"        # String with unit, not number
   max_connections = 1000    # Number, not string
   enabled = true            # Boolean, not string
   ```

#### Symptom: Permission Denied Errors

**Diagnosis:**
```bash
# Check file permissions
ls -la /data/lightning_db/
ls -la /etc/lightning_db/
ls -la /var/log/lightning_db/

# Check process user
ps aux | grep lightning_db

# Test file access
sudo -u lightning_db test -r /etc/lightning_db/config.toml
sudo -u lightning_db test -w /data/lightning_db/
```

**Solutions:**

1. **Fix File Permissions**
   ```bash
   # Set correct ownership
   chown -R lightning_db:lightning_db /data/lightning_db/
   chown -R lightning_db:lightning_db /var/log/lightning_db/
   
   # Set correct permissions
   chmod 700 /data/lightning_db/
   chmod 755 /var/log/lightning_db/
   chmod 640 /etc/lightning_db/config.toml
   ```

2. **Check SELinux/AppArmor**
   ```bash
   # SELinux
   setenforce 0  # Temporarily disable to test
   setsebool -P daemons_use_tcp_wrapper 1
   
   # AppArmor
   aa-status | grep lightning_db
   aa-disable /etc/apparmor.d/lightning_db
   ```

### 6. Resource Exhaustion

#### Symptom: Out of Disk Space

**Diagnosis:**
```bash
# Check disk usage
df -h /data
du -sh /data/lightning_db/*

# Check for large files
find /data/lightning_db -type f -size +1G -ls

# Monitor disk growth
iostat -x 1 10
```

**Solutions:**

1. **Clean Up Old Data**
   ```bash
   # Remove old backups
   find /backup -name "*.backup" -mtime +30 -delete
   
   # Compress old logs
   gzip /var/log/lightning_db/*.log.1
   
   # Clean up temporary files
   lightning_db cleanup --temp-files
   ```

2. **Compress Data**
   ```toml
   [performance]
   compression_enabled = true
   compression_type = "zstd"
   compression_level = 6
   ```

3. **Archive Old Data**
   ```bash
   # Archive data older than 1 year
   lightning_db archive --older-than=1y --output=/archive/
   ```

#### Symptom: Out of Memory

**Diagnosis:**
```bash
# Check memory usage
free -h
ps aux --sort=-%mem | head -10

# Check for memory leaks
pmap $(pgrep lightning_db)

# Monitor memory over time
sar -r 1 60
```

**Solutions:**

1. **Reduce Memory Usage**
   ```toml
   [database]
   cache_size = "2GB"  # Reduce cache size
   
   [performance]
   prefetch_workers = 2  # Reduce worker threads
   ```

2. **Add Swap Space**
   ```bash
   # Create swap file
   fallocate -l 4G /swapfile
   chmod 600 /swapfile
   mkswap /swapfile
   swapon /swapfile
   echo '/swapfile none swap sw 0 0' >> /etc/fstab
   ```

3. **Upgrade Hardware**
   ```bash
   # Check current RAM
   dmidecode -t memory | grep "Size:"
   
   # Plan RAM upgrade based on workload
   lightning_db memory-recommendations
   ```

## Advanced Diagnostics

### Performance Profiling

```bash
# CPU profiling
perf record -g lightning_db
perf report

# Memory profiling
valgrind --tool=massif lightning_db
ms_print massif.out.*

# I/O profiling
strace -e trace=file -p $(pgrep lightning_db)

# Lock contention analysis
perf record -e probe:pthread_mutex_lock lightning_db
```

### Network Analysis

```bash
# Packet capture
tcpdump -i any -w lightning_db.pcap port 9090

# Connection analysis
ss -tulpn | grep :9090

# Bandwidth monitoring
iftop -i eth0 -f "port 9090"

# DNS resolution issues
nslookup lightning-db.example.com
dig lightning-db.example.com
```

### Log Analysis

```bash
# Error pattern analysis
grep -E "(ERROR|FATAL)" /var/log/lightning_db/app.log | \
    awk '{print $1, $2, $5}' | sort | uniq -c | sort -nr

# Performance metrics from logs
grep "slow_operation" /var/log/lightning_db/app.log | \
    jq '.duration_ms' | sort -n | tail -20

# Connection pattern analysis
grep "connection" /var/log/lightning_db/app.log | \
    jq '.client_ip' | sort | uniq -c | sort -nr
```

## Emergency Procedures

### Database Recovery

```bash
#!/bin/bash
# emergency-recovery.sh

echo "=== Lightning DB Emergency Recovery ==="

# 1. Stop the database
systemctl stop lightning-db

# 2. Backup current state
BACKUP_DIR="/emergency-backup-$(date +%s)"
mkdir -p "$BACKUP_DIR"
cp -r /data/lightning_db "$BACKUP_DIR/"

# 3. Run integrity check
lightning_db integrity-check --full > "$BACKUP_DIR/integrity-report.txt"

# 4. Attempt automatic repair
if lightning_db repair --auto-fix; then
    echo "Automatic repair successful"
else
    echo "Automatic repair failed, attempting manual recovery"
    
    # 5. Try to restore from latest backup
    LATEST_BACKUP=$(ls -t /backup/*.backup | head -1)
    if [ -n "$LATEST_BACKUP" ]; then
        lightning_db restore --backup-file "$LATEST_BACKUP"
    else
        echo "No backup available, attempting WAL replay"
        lightning_db wal-replay --from-last-checkpoint
    fi
fi

# 6. Verify recovery
if lightning_db integrity-check --quick; then
    echo "Recovery successful, restarting database"
    systemctl start lightning-db
else
    echo "Recovery failed, manual intervention required"
    exit 1
fi
```

### Data Extraction

```bash
# Extract critical data before full recovery
lightning_db export --format=json --output=/emergency-export/ \
    --table=critical_data --where="created_at > '2023-11-01'"

# Export entire database
lightning_db dump --format=sql --output=full-dump.sql

# Extract specific keys
lightning_db get-range --start-key="user:" --end-key="user:~" \
    --output=/emergency-export/users.json
```

## Monitoring and Alerting

### Health Check Script

```bash
#!/bin/bash
# health-check.sh

# Check if service is running
if ! systemctl is-active --quiet lightning-db; then
    echo "CRITICAL: Lightning DB service is not running"
    exit 2
fi

# Check basic connectivity
if ! timeout 5 bash -c "echo > /dev/tcp/localhost/9090"; then
    echo "CRITICAL: Cannot connect to Lightning DB"
    exit 2
fi

# Check performance
RESPONSE_TIME=$(lightning_db benchmark --quick --json | jq '.avg_latency_ms')
if (( $(echo "$RESPONSE_TIME > 100" | bc -l) )); then
    echo "WARNING: High response time: ${RESPONSE_TIME}ms"
    exit 1
fi

# Check disk space
DISK_USAGE=$(df /data | tail -1 | awk '{print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -gt 90 ]; then
    echo "CRITICAL: Disk usage is ${DISK_USAGE}%"
    exit 2
fi

# Check memory usage
MEM_USAGE=$(ps -o %mem -p $(pgrep lightning_db) | tail -1 | cut -d. -f1)
if [ "$MEM_USAGE" -gt 80 ]; then
    echo "WARNING: Memory usage is ${MEM_USAGE}%"
    exit 1
fi

echo "OK: All health checks passed"
exit 0
```

### Log Rotation

```bash
# /etc/logrotate.d/lightning-db
/var/log/lightning_db/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 lightning_db lightning_db
    postrotate
        systemctl reload lightning-db
    endscript
}
```

## Getting Help

### Diagnostic Information Collection

```bash
#!/bin/bash
# collect-diagnostics.sh

DIAG_DIR="lightning-db-diagnostics-$(date +%s)"
mkdir -p "$DIAG_DIR"

# System information
uname -a > "$DIAG_DIR/system-info.txt"
free -h > "$DIAG_DIR/memory-info.txt"
df -h > "$DIAG_DIR/disk-info.txt"
lscpu > "$DIAG_DIR/cpu-info.txt"

# Lightning DB specific
lightning_db --version > "$DIAG_DIR/version.txt"
lightning_db health-check --detailed > "$DIAG_DIR/health-check.txt"
lightning_db config --show > "$DIAG_DIR/config.txt"
lightning_db metrics > "$DIAG_DIR/metrics.txt"

# Logs (last 1000 lines)
tail -1000 /var/log/lightning_db/app.log > "$DIAG_DIR/app.log"
tail -1000 /var/log/lightning_db/audit.log > "$DIAG_DIR/audit.log"

# System logs
journalctl -u lightning-db --lines=1000 > "$DIAG_DIR/systemd.log"

# Network information
netstat -tulpn | grep lightning > "$DIAG_DIR/network.txt"

# Create tarball
tar -czf "${DIAG_DIR}.tar.gz" "$DIAG_DIR"
echo "Diagnostics collected in ${DIAG_DIR}.tar.gz"
```

### Support Channels

- **GitHub Issues**: Report bugs and feature requests
- **Community Forum**: Ask questions and share solutions  
- **Enterprise Support**: 24/7 support with SLA guarantees
- **Emergency Hotline**: Critical production issues
- **Documentation**: Online documentation and guides
- **Training**: Professional training and certification programs

### Before Contacting Support

1. **Collect diagnostic information** using the script above
2. **Check recent changes** to configuration or environment
3. **Review logs** for error messages and patterns
4. **Try basic troubleshooting** steps from this guide
5. **Document the issue** with steps to reproduce
6. **Note the impact** and business criticality
7. **Gather environment details** (OS, hardware, network)

This information helps support teams resolve issues faster and more effectively.