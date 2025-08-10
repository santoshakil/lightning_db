# Lightning DB Production Guide

## Table of Contents
1. [Production Deployment](#production-deployment)
2. [Configuration](#configuration)
3. [Monitoring](#monitoring)
4. [Performance Tuning](#performance-tuning)
5. [Troubleshooting](#troubleshooting)
6. [Disaster Recovery](#disaster-recovery)
7. [Security](#security)
8. [Capacity Planning](#capacity-planning)

---

## Production Deployment

### Pre-Deployment Checklist
- [ ] Hardware requirements verified (min 4GB RAM, SSD storage)
- [ ] Operating system updated and patched
- [ ] Firewall rules configured
- [ ] Backup strategy in place
- [ ] Monitoring infrastructure ready
- [ ] Load testing completed
- [ ] Security audit performed
- [ ] Documentation reviewed

### Deployment Steps

1. **System Preparation**
```bash
# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install dependencies
sudo apt-get install -y build-essential pkg-config libssl-dev

# Set system limits
echo "* soft nofile 65536" >> /etc/security/limits.conf
echo "* hard nofile 65536" >> /etc/security/limits.conf
```

2. **Build and Install**
```bash
# Build with production optimizations
cargo build --release

# Run tests
cargo test --release

# Install
sudo cp target/release/lightning_db /usr/local/bin/
```

3. **Configuration**
```rust
// production_config.rs
let config = LightningDbConfig {
    compression_enabled: true,
    use_improved_wal: true,
    wal_sync_mode: WalSyncMode::Sync,
    write_batch_size: 1000,
    cache_size: 1024 * 1024 * 1024, // 1GB cache
    enable_statistics: true,
    max_active_transactions: 1000,
    ..Default::default()
};
```

### Docker Deployment
```yaml
version: '3.8'
services:
  lightning-db:
    build: .
    volumes:
      - ./data:/data
      - ./config:/config
    ports:
      - "8080:8080"
    environment:
      - RUST_LOG=info
      - DB_PATH=/data
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

---

## Configuration

### Core Settings
| Parameter | Default | Production | Description |
|-----------|---------|------------|-------------|
| `compression_enabled` | true | true | Enable compression |
| `cache_size` | 0 | 1GB+ | Memory cache size |
| `wal_sync_mode` | Async | Sync | WAL durability |
| `write_batch_size` | 1000 | 1000-5000 | Batch size |
| `max_active_transactions` | 1000 | 500-2000 | Transaction limit |

### Performance Tuning Parameters
```rust
// High throughput configuration
let high_throughput = LightningDbConfig {
    cache_size: 4 * 1024 * 1024 * 1024, // 4GB
    write_batch_size: 5000,
    wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
    ..Default::default()
};

// High durability configuration
let high_durability = LightningDbConfig {
    wal_sync_mode: WalSyncMode::Sync,
    write_batch_size: 100,
    compression_enabled: true,
    ..Default::default()
};
```

---

## Monitoring

### Key Metrics

#### Performance Metrics
- **Read Operations/sec**: Target > 100K
- **Write Operations/sec**: Target > 50K
- **Transaction Latency**: p99 < 10ms
- **Cache Hit Rate**: Target > 80%

#### Health Metrics
- **Memory Usage**: Monitor for leaks
- **Disk I/O**: Watch for saturation
- **SSTable Count**: Monitor compaction
- **Transaction Conflicts**: Should be < 20%

### Prometheus Integration
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'lightning-db'
    static_configs:
      - targets: ['localhost:9090']
    metrics_path: '/metrics'
```

### Grafana Dashboard
Key panels to include:
- Operations per second (read/write)
- Latency percentiles (p50, p95, p99)
- Cache hit rate
- Memory usage
- Disk usage
- Transaction success rate
- SSTable compaction rate

### Alerting Rules
```yaml
groups:
  - name: lightning_db
    rules:
      - alert: HighTransactionConflicts
        expr: transaction_conflicts_rate > 0.3
        for: 5m
        annotations:
          summary: "High transaction conflict rate"
          
      - alert: LowCacheHitRate
        expr: cache_hit_rate < 0.6
        for: 10m
        annotations:
          summary: "Cache hit rate below threshold"
          
      - alert: HighMemoryUsage
        expr: memory_usage_bytes > 8589934592
        for: 5m
        annotations:
          summary: "Memory usage above 8GB"
```

---

## Performance Tuning

### Hardware Optimization
- **CPU**: 4+ cores recommended
- **RAM**: 8GB minimum, 16GB+ for high load
- **Storage**: NVMe SSD strongly recommended
- **Network**: 10Gbps for distributed setups

### OS Tuning
```bash
# Kernel parameters
sysctl -w vm.swappiness=10
sysctl -w vm.dirty_ratio=15
sysctl -w vm.dirty_background_ratio=5

# I/O scheduler for SSD
echo noop > /sys/block/nvme0n1/queue/scheduler
```

### Application Tuning

#### Cache Optimization
```rust
// Calculate optimal cache size
let available_memory = get_available_memory();
let cache_size = (available_memory * 0.25) as u64; // Use 25% of RAM
```

#### Batch Operations
```rust
// Optimize batch writes
let mut batch = WriteBatch::new();
for (key, value) in items {
    batch.put(key, value);
    if batch.len() >= 1000 {
        db.write_batch(&batch)?;
        batch.clear();
    }
}
```

#### Connection Pooling
```rust
// Use connection pool for concurrent access
let pool = DatabasePool::new(db, 100); // 100 connections
```

---

## Troubleshooting

### Common Issues

#### High Memory Usage
**Symptoms**: OOM kills, slow performance
**Solutions**:
1. Reduce cache size
2. Enable compression
3. Increase compaction frequency
4. Check for memory leaks

#### Transaction Conflicts
**Symptoms**: Failed transactions, retries
**Solutions**:
1. Reduce transaction scope
2. Use shorter transactions
3. Implement retry logic
4. Consider optimistic locking

#### Slow Reads
**Symptoms**: High read latency
**Solutions**:
1. Increase cache size
2. Enable prefetching
3. Optimize SSTable compaction
4. Add indexes for common queries

#### Data Corruption
**Symptoms**: Checksum errors, crashes
**Solutions**:
1. Run integrity check
2. Restore from backup
3. Check disk health
4. Verify memory integrity

### Debug Commands
```bash
# Check database integrity
lightning_db check --path /data

# Dump statistics
lightning_db stats --path /data

# Compact database
lightning_db compact --path /data

# Export data
lightning_db export --path /data --output backup.db
```

---

## Disaster Recovery

### Backup Strategy

#### Online Backup
```rust
// Hot backup while running
db.create_backup("/backup/path")?;
```

#### Offline Backup
```bash
# Stop service
systemctl stop lightning-db

# Copy data files
rsync -av /data/ /backup/

# Start service
systemctl start lightning-db
```

### Recovery Procedures

#### From Backup
```bash
# Stop service
systemctl stop lightning-db

# Restore data
rsync -av /backup/ /data/

# Start service
systemctl start lightning-db
```

#### Point-in-Time Recovery
```rust
// Replay WAL to specific timestamp
db.recover_to_timestamp(timestamp)?;
```

### High Availability Setup
```yaml
# Master configuration
master:
  role: primary
  bind: 0.0.0.0:8080
  replication:
    enabled: true
    
# Replica configuration  
replica:
  role: secondary
  master: master.example.com:8080
  lag_threshold: 1000ms
```

---

## Security

### Authentication
```rust
// Enable authentication
let config = SecurityConfig {
    auth_enabled: true,
    tls_enabled: true,
    cert_path: "/certs/server.crt",
    key_path: "/certs/server.key",
};
```

### Encryption
```rust
// Enable encryption at rest
let config = EncryptionConfig {
    enabled: true,
    algorithm: EncryptionAlgorithm::AES256,
    key_rotation_days: 90,
};
```

### Access Control
```rust
// Role-based access
db.create_user("reader", Role::ReadOnly)?;
db.create_user("writer", Role::ReadWrite)?;
db.create_user("admin", Role::Admin)?;
```

### Security Checklist
- [ ] Authentication enabled
- [ ] TLS/SSL configured
- [ ] Encryption at rest enabled
- [ ] Regular security updates
- [ ] Audit logging enabled
- [ ] Network isolation configured
- [ ] Regular security scans
- [ ] Backup encryption enabled

---

## Capacity Planning

### Storage Requirements
```
Data Size = (Key Size + Value Size + Overhead) × Number of Records
Overhead ≈ 40 bytes per record

Example:
- 1M records
- 50 byte keys
- 200 byte values
- Storage = (50 + 200 + 40) × 1M = 290MB
```

### Memory Requirements
```
Memory = Cache Size + WAL Buffer + Transaction Buffer + OS Cache
Minimum = 4GB
Recommended = 8GB + (Data Size × 0.1)
```

### Performance Projections
| Records | Storage | Memory | Write ops/s | Read ops/s |
|---------|---------|--------|-------------|------------|
| 1M | 290MB | 4GB | 500K | 1M |
| 10M | 2.9GB | 8GB | 400K | 900K |
| 100M | 29GB | 16GB | 300K | 800K |
| 1B | 290GB | 32GB | 200K | 700K |

### Scaling Guidelines
1. **Vertical Scaling**: Add CPU/RAM up to 64GB
2. **Horizontal Scaling**: Shard by key range
3. **Read Scaling**: Add read replicas
4. **Write Scaling**: Partition by time/tenant

---

## Best Practices

### Development
1. Use transactions for consistency
2. Batch operations when possible
3. Handle conflicts with retry logic
4. Monitor performance metrics
5. Regular backups

### Operations
1. Regular health checks
2. Automated failover
3. Capacity monitoring
4. Performance baselines
5. Incident response plan

### Maintenance
1. Regular compaction
2. WAL cleanup
3. Statistics updates
4. Index optimization
5. Version upgrades

---

*Last Updated: 2025-08-09*
*Version: 1.0.0*