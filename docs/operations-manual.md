# Lightning DB Operations Manual

## Table of Contents

1. [Deployment Guide](#deployment-guide)
2. [Configuration Management](#configuration-management)
3. [Monitoring & Alerting](#monitoring--alerting)
4. [Performance Tuning](#performance-tuning)
5. [Backup & Recovery](#backup--recovery)
6. [Troubleshooting](#troubleshooting)
7. [Maintenance Procedures](#maintenance-procedures)
8. [Security Considerations](#security-considerations)
9. [Capacity Planning](#capacity-planning)
10. [Emergency Procedures](#emergency-procedures)

## Deployment Guide

### System Requirements

#### Minimum Requirements
- **CPU**: 2 cores, 2.4 GHz
- **Memory**: 4 GB RAM
- **Storage**: 10 GB available space
- **OS**: Linux (Ubuntu 20.04+), macOS (10.15+), Windows (Server 2019+)

#### Recommended Production Requirements
- **CPU**: 8+ cores, 3.0 GHz (Intel Skylake+ or AMD Zen2+)
- **Memory**: 32+ GB RAM
- **Storage**: NVMe SSD with 100+ GB space
- **Network**: 1 Gbps+ (for distributed setups)

#### Hardware Optimization
```bash
# Enable huge pages for better memory performance
echo 'vm.nr_hugepages = 1024' >> /etc/sysctl.conf
sysctl -p

# Optimize I/O scheduler for SSDs
echo mq-deadline > /sys/block/nvme0n1/queue/scheduler

# Disable swap to prevent performance degradation
swapoff -a
```

### Installation

#### Binary Installation
```bash
# Download latest release
wget https://github.com/org/lightning-db/releases/latest/lightning-db-linux-x86_64.tar.gz

# Extract
tar -xzf lightning-db-linux-x86_64.tar.gz

# Install
sudo cp lightning-db /usr/local/bin/
sudo cp lightning-cli /usr/local/bin/
sudo cp lightning-admin-server /usr/local/bin/

# Verify installation
lightning-cli --version
```

#### Building from Source
```bash
# Prerequisites
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Build
git clone https://github.com/org/lightning-db.git
cd lightning-db
cargo build --release

# Install
sudo cp target/release/lightning-cli /usr/local/bin/
sudo cp target/release/lightning-admin-server /usr/local/bin/
```

### Service Configuration

#### Systemd Service (Linux)
```ini
# /etc/systemd/system/lightning-db.service
[Unit]
Description=Lightning DB Service
After=network.target

[Service]
Type=simple
User=lightning
Group=lightning
ExecStart=/usr/local/bin/lightning-admin-server --config /etc/lightning-db/config.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=5
LimitNOFILE=65536

# Security settings
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/lightning-db /var/log/lightning-db

[Install]
WantedBy=multi-user.target
```

#### User and Directory Setup
```bash
# Create user
sudo useradd -r -s /bin/false lightning

# Create directories
sudo mkdir -p /var/lib/lightning-db
sudo mkdir -p /var/log/lightning-db
sudo mkdir -p /etc/lightning-db

# Set permissions
sudo chown -R lightning:lightning /var/lib/lightning-db
sudo chown -R lightning:lightning /var/log/lightning-db
sudo chmod 750 /var/lib/lightning-db
sudo chmod 750 /var/log/lightning-db
```

### Environment Configuration

#### Production Configuration File
```toml
# /etc/lightning-db/config.toml
[database]
path = "/var/lib/lightning-db/data"
page_size = 4096
cache_size_mb = 1024
max_memory_mb = 2048

[performance]
compression_enabled = true
compression_type = "zstd"
write_batch_size = 1000
prefetch_enabled = false

[durability]
wal_sync_mode = "sync"
enable_safety_guards = true
enable_paranoia_checks = true
checkpoint_interval_sec = 300

[logging]
level = "info"
format = "json"
file = "/var/log/lightning-db/lightning.log"
max_size_mb = 100
max_files = 10

[monitoring]
metrics_enabled = true
metrics_port = 9090
health_check_port = 8080
prometheus_export = true

[limits]
max_active_transactions = 1000
max_connections = 100
request_timeout_sec = 30
```

## Configuration Management

### Configuration Hierarchy
1. **Default values** (hardcoded)
2. **Configuration file** (`/etc/lightning-db/config.toml`)
3. **Environment variables** (`LIGHTNING_DB_*`)
4. **Command line arguments**

### Environment Variables
```bash
# Database settings
export LIGHTNING_DB_PATH="/var/lib/lightning-db/data"
export LIGHTNING_DB_CACHE_SIZE_MB="1024"
export LIGHTNING_DB_MAX_MEMORY_MB="2048"

# Performance settings
export LIGHTNING_DB_COMPRESSION_ENABLED="true"
export LIGHTNING_DB_WAL_SYNC_MODE="sync"

# Logging
export LIGHTNING_DB_LOG_LEVEL="info"
export LIGHTNING_DB_LOG_FILE="/var/log/lightning-db/lightning.log"

# Monitoring
export LIGHTNING_DB_METRICS_PORT="9090"
export LIGHTNING_DB_HEALTH_PORT="8080"
```

### Runtime Configuration Changes
```bash
# View current configuration
lightning-cli config show

# Update configuration (requires restart)
lightning-cli config set cache_size_mb 2048

# Hot-reload configuration (limited settings)
lightning-cli config reload
```

## Monitoring & Alerting

### Key Metrics to Monitor

#### Performance Metrics
```bash
# Check current performance
lightning-cli stats performance

# Expected ranges:
# Read latency: <100μs (P99)
# Write latency: <1ms (P99)  
# Read throughput: >100K ops/sec
# Write throughput: >50K ops/sec
```

#### Resource Metrics
```bash
# Memory usage
lightning-cli stats memory
# Alert if >80% of max_memory

# Disk usage
lightning-cli stats disk
# Alert if >85% full

# CPU usage
lightning-cli stats cpu
# Alert if >80% for >5 minutes
```

#### Health Metrics
```bash
# Error rates
lightning-cli stats errors
# Alert if error rate >0.1%

# Active connections
lightning-cli stats connections
# Alert if >90% of max_connections

# Transaction conflicts
lightning-cli stats transactions
# Alert if conflict rate >5%
```

### Prometheus Integration

#### Metrics Export
```bash
# Enable Prometheus metrics
curl http://localhost:9090/metrics

# Key metrics to monitor:
# lightning_db_operations_total{operation="read|write|delete"}
# lightning_db_operation_duration_seconds{operation="read|write|delete"}
# lightning_db_memory_usage_bytes
# lightning_db_disk_usage_bytes
# lightning_db_cache_hit_ratio
# lightning_db_active_transactions
# lightning_db_errors_total{type="io|corruption|transaction"}
```

#### Grafana Dashboard
```json
{
  "dashboard": {
    "title": "Lightning DB",
    "panels": [
      {
        "title": "Operations per Second",
        "targets": [
          {
            "expr": "rate(lightning_db_operations_total[5m])",
            "legendFormat": "{{operation}}"
          }
        ]
      },
      {
        "title": "Latency",
        "targets": [
          {
            "expr": "histogram_quantile(0.99, lightning_db_operation_duration_seconds)",
            "legendFormat": "P99 {{operation}}"
          }
        ]
      }
    ]
  }
}
```

### Alerting Rules

#### Prometheus Alerting Rules
```yaml
# alerts.yml
groups:
- name: lightning_db
  rules:
  - alert: HighMemoryUsage
    expr: lightning_db_memory_usage_bytes / lightning_db_memory_limit_bytes > 0.8
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Lightning DB memory usage is high"
      
  - alert: HighErrorRate
    expr: rate(lightning_db_errors_total[5m]) > 0.001
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "Lightning DB error rate is high"
      
  - alert: HighLatency
    expr: histogram_quantile(0.99, lightning_db_operation_duration_seconds) > 0.1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Lightning DB latency is high"
```

### Health Checks

#### HTTP Health Endpoint
```bash
# Basic health check
curl http://localhost:8080/health
# Returns: {"status": "healthy|warning|critical", "details": {...}}

# Detailed health check
curl http://localhost:8080/health/detailed
```

#### CLI Health Check
```bash
# Quick health check
lightning-cli health

# Comprehensive health check
lightning-cli health --detailed

# Health check with specific tests
lightning-cli health --tests memory,disk,performance
```

## Performance Tuning

### Workload-Specific Tuning

#### Read-Heavy Workloads
```toml
[performance]
cache_size_mb = 4096           # Large cache
compression_enabled = false    # Reduce CPU overhead
prefetch_enabled = true        # Anticipate reads
prefetch_distance = 16         # Aggressive prefetch

[database]
use_optimized_page_manager = true  # Better read performance
```

#### Write-Heavy Workloads
```toml
[performance]
write_batch_size = 5000        # Large batches
compression_enabled = true     # Reduce I/O
wal_sync_mode = "async"        # Faster writes (less durable)

[database]
lsm_memtable_size_mb = 64     # Larger write buffer
compaction_threads = 4         # Parallel compaction
```

#### Mixed Workloads
```toml
[performance]
cache_size_mb = 2048          # Balanced cache
write_batch_size = 1000       # Moderate batching
compression_enabled = true     # Good compression ratio
wal_sync_mode = "periodic"    # Balance durability/performance
periodic_sync_ms = 100        # 100ms sync interval
```

### Hardware Optimization

#### CPU Optimization
```bash
# Check CPU features
lightning-cli system cpu-features

# Enable SIMD optimizations (compile-time)
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Set CPU affinity for lightning-db process
taskset -c 0-7 lightning-admin-server

# Enable NUMA optimizations
numactl --cpunodebind=0 --membind=0 lightning-admin-server
```

#### Memory Optimization
```bash
# Configure huge pages
echo 2048 > /proc/sys/vm/nr_hugepages

# Optimize memory settings
echo 1 > /proc/sys/vm/swappiness
echo 1 > /proc/sys/vm/dirty_background_ratio
echo 5 > /proc/sys/vm/dirty_ratio
```

#### Storage Optimization
```bash
# Optimize mount options for performance
mount -o noatime,nodiratime /dev/nvme0n1p1 /var/lib/lightning-db

# Set I/O scheduler
echo mq-deadline > /sys/block/nvme0n1/queue/scheduler

# Increase I/O queue depth
echo 32 > /sys/block/nvme0n1/queue/nr_requests
```

### Cache Tuning

#### Cache Size Calculation
```bash
# Estimate working set size
lightning-cli analyze working-set

# Calculate optimal cache size (rule of thumb: 25% of working set)
working_set_gb=10
cache_size_mb=$((working_set_gb * 1024 / 4))
echo "Recommended cache size: ${cache_size_mb}MB"
```

#### Cache Hit Rate Monitoring
```bash
# Monitor cache performance
lightning-cli stats cache
# Target: >95% hit rate for read-heavy workloads
#         >80% hit rate for mixed workloads
```

## Backup & Recovery

### Backup Strategies

#### Full Backup
```bash
# Create full backup
lightning-cli backup create \
  --source /var/lib/lightning-db/data \
  --dest /backup/lightning-db/full-$(date +%Y%m%d) \
  --compress \
  --verify

# Automated daily backup
#!/bin/bash
# /usr/local/bin/backup-lightning-db.sh
DATE=$(date +%Y%m%d)
BACKUP_DIR="/backup/lightning-db"
SOURCE="/var/lib/lightning-db/data"

lightning-cli backup create \
  --source "$SOURCE" \
  --dest "$BACKUP_DIR/full-$DATE" \
  --compress \
  --verify

# Cleanup old backups (keep 30 days)
find "$BACKUP_DIR" -name "full-*" -mtime +30 -delete
```

#### Incremental Backup
```bash
# Create incremental backup
lightning-cli backup create \
  --source /var/lib/lightning-db/data \
  --dest /backup/lightning-db/inc-$(date +%Y%m%d-%H%M) \
  --incremental \
  --base-backup /backup/lightning-db/full-20240101
```

#### Point-in-Time Backup
```bash
# Create checkpoint before major operations
lightning-cli checkpoint create --name "before-migration"

# WAL-based point-in-time recovery setup
lightning-cli backup wal-archive \
  --source /var/lib/lightning-db/data/wal \
  --dest /backup/lightning-db/wal-archive \
  --continuous
```

### Restore Procedures

#### Full Restore
```bash
# Stop Lightning DB service
sudo systemctl stop lightning-db

# Restore from backup
lightning-cli restore \
  --source /backup/lightning-db/full-20240101 \
  --dest /var/lib/lightning-db/data \
  --verify

# Fix permissions
sudo chown -R lightning:lightning /var/lib/lightning-db

# Start service
sudo systemctl start lightning-db

# Verify restore
lightning-cli health --verify-data
```

#### Point-in-Time Recovery
```bash
# Restore base backup
lightning-cli restore \
  --source /backup/lightning-db/full-20240101 \
  --dest /var/lib/lightning-db/data

# Apply WAL files up to specific timestamp
lightning-cli restore wal-replay \
  --wal-archive /backup/lightning-db/wal-archive \
  --until "2024-01-02 14:30:00" \
  --target /var/lib/lightning-db/data
```

### Backup Verification

#### Integrity Verification
```bash
# Verify backup integrity
lightning-cli backup verify /backup/lightning-db/full-20240101

# Test restore (to temporary location)
lightning-cli restore \
  --source /backup/lightning-db/full-20240101 \
  --dest /tmp/restore-test \
  --verify-only

# Automated backup testing
#!/bin/bash
# Test latest backup
LATEST_BACKUP=$(ls -t /backup/lightning-db/full-* | head -1)
lightning-cli restore \
  --source "$LATEST_BACKUP" \
  --dest /tmp/backup-test \
  --verify-only \
  --cleanup
```

## Troubleshooting

### Common Issues

#### High Memory Usage
```bash
# Investigate memory usage
lightning-cli stats memory --detailed

# Check for memory leaks
lightning-cli debug memory-growth --duration 60

# Mitigation steps:
# 1. Reduce cache size
lightning-cli config set cache_size_mb 512

# 2. Increase version cleanup frequency
lightning-cli config set version_cleanup_interval_sec 30

# 3. Restart service if memory leak detected
sudo systemctl restart lightning-db
```

#### Performance Degradation
```bash
# Check performance metrics
lightning-cli stats performance --history 1h

# Analyze slow queries
lightning-cli debug slow-queries --threshold 100ms

# Common causes and solutions:
# 1. Cache thrashing
lightning-cli stats cache
# Solution: Increase cache size

# 2. Compaction overhead
lightning-cli stats lsm
# Solution: Adjust compaction settings

# 3. Lock contention
lightning-cli debug locks --show-waits
# Solution: Reduce transaction scope
```

#### Data Corruption
```bash
# Check for corruption
lightning-cli verify integrity

# Attempt automatic repair
lightning-cli repair --backup-first

# If repair fails, restore from backup
sudo systemctl stop lightning-db
lightning-cli restore \
  --source /backup/lightning-db/latest \
  --dest /var/lib/lightning-db/data
sudo systemctl start lightning-db
```

#### Connection Issues
```bash
# Check service status
sudo systemctl status lightning-db

# Check listening ports
sudo netstat -tlnp | grep lightning

# Check resource limits
lightning-cli stats limits

# Check logs for errors
sudo journalctl -u lightning-db -f
```

### Diagnostic Tools

#### Performance Profiling
```bash
# CPU profiling
lightning-cli profile cpu --duration 60s --output cpu-profile.svg

# Memory profiling
lightning-cli profile memory --duration 60s --output memory-profile.txt

# I/O profiling
lightning-cli profile io --duration 60s --output io-profile.json
```

#### Debug Information
```bash
# Generate debug report
lightning-cli debug report --output debug-report.zip

# Live debugging
lightning-cli debug live \
  --metrics \
  --locks \
  --transactions \
  --cache
```

### Log Analysis

#### Log Locations
```bash
# Application logs
/var/log/lightning-db/lightning.log

# System logs
sudo journalctl -u lightning-db

# Access logs (if admin server enabled)
/var/log/lightning-db/access.log
```

#### Log Analysis Commands
```bash
# Check for errors
grep ERROR /var/log/lightning-db/lightning.log | tail -20

# Monitor real-time logs
tail -f /var/log/lightning-db/lightning.log | grep -E "(ERROR|WARN)"

# Analyze performance logs
lightning-cli logs analyze \
  --file /var/log/lightning-db/lightning.log \
  --metric latency \
  --period 1h
```

## Maintenance Procedures

### Regular Maintenance

#### Daily Tasks
```bash
#!/bin/bash
# /usr/local/bin/lightning-db-daily.sh

# Check health
lightning-cli health

# Check disk space
df -h /var/lib/lightning-db

# Backup
lightning-cli backup create \
  --source /var/lib/lightning-db/data \
  --dest /backup/lightning-db/daily-$(date +%Y%m%d)

# Clean old logs
find /var/log/lightning-db -name "*.log" -mtime +7 -delete

# Update statistics
lightning-cli analyze update-stats
```

#### Weekly Tasks
```bash
#!/bin/bash
# /usr/local/bin/lightning-db-weekly.sh

# Integrity check
lightning-cli verify integrity --full

# Optimize indexes
lightning-cli optimize indexes

# Compact LSM tree
lightning-cli compact --force

# Update configuration if needed
lightning-cli config optimize --workload-analysis
```

#### Monthly Tasks
```bash
#!/bin/bash
# /usr/local/bin/lightning-db-monthly.sh

# Full backup verification
lightning-cli backup verify-all /backup/lightning-db

# Capacity planning report
lightning-cli analyze capacity-growth --period 30d

# Security audit
lightning-cli security audit

# Performance trend analysis
lightning-cli analyze performance-trends --period 30d
```

### Upgrade Procedures

#### Minor Version Upgrade
```bash
# 1. Backup current installation
lightning-cli backup create \
  --source /var/lib/lightning-db \
  --dest /backup/pre-upgrade-$(date +%Y%m%d)

# 2. Download new version
wget https://github.com/org/lightning-db/releases/latest/lightning-db-linux.tar.gz

# 3. Stop service
sudo systemctl stop lightning-db

# 4. Install new version
tar -xzf lightning-db-linux.tar.gz
sudo cp lightning-* /usr/local/bin/

# 5. Start service
sudo systemctl start lightning-db

# 6. Verify upgrade
lightning-cli version
lightning-cli health
```

#### Major Version Upgrade
```bash
# 1. Plan maintenance window
# 2. Full backup and verification
# 3. Test upgrade in staging environment
# 4. Follow migration guide for breaking changes
# 5. Monitor closely after upgrade
```

## Security Considerations

### Access Control

#### File System Permissions
```bash
# Database files
sudo chmod 750 /var/lib/lightning-db
sudo chown lightning:lightning /var/lib/lightning-db

# Configuration files
sudo chmod 640 /etc/lightning-db/config.toml
sudo chown root:lightning /etc/lightning-db/config.toml

# Log files
sudo chmod 640 /var/log/lightning-db/*.log
sudo chown lightning:lightning /var/log/lightning-db
```

#### Network Security
```bash
# Firewall rules (adjust as needed)
sudo ufw allow from 10.0.0.0/8 to any port 9090  # Metrics
sudo ufw allow from 10.0.0.0/8 to any port 8080  # Health check
sudo ufw deny 9090  # Block external access to metrics
sudo ufw deny 8080  # Block external access to health check
```

### Data Protection

#### Encryption at Rest
```toml
# config.toml
[encryption]
enabled = true
key_file = "/etc/lightning-db/encryption.key"
algorithm = "AES-256-GCM"
```

#### Backup Encryption
```bash
# Encrypted backup
lightning-cli backup create \
  --source /var/lib/lightning-db/data \
  --dest /backup/lightning-db/encrypted-backup \
  --encrypt \
  --key-file /etc/lightning-db/backup.key
```

### Audit Logging

#### Enable Audit Logging
```toml
# config.toml
[audit]
enabled = true
log_file = "/var/log/lightning-db/audit.log"
log_level = "info"
include_data = false  # Don't log sensitive data
```

#### Security Monitoring
```bash
# Monitor for suspicious activity
lightning-cli security monitor \
  --detect-anomalies \
  --alert-threshold high

# Generate security report
lightning-cli security report --period 30d
```

## Capacity Planning

### Growth Monitoring

#### Data Growth Analysis
```bash
# Analyze data growth trends
lightning-cli analyze data-growth --period 90d

# Project future storage needs
lightning-cli analyze project-growth --horizon 1y

# Calculate storage efficiency
lightning-cli analyze compression-ratio
```

#### Performance Scaling
```bash
# Identify performance bottlenecks
lightning-cli analyze bottlenecks

# Capacity recommendations
lightning-cli analyze recommend-capacity \
  --target-latency 1ms \
  --target-throughput 100000
```

### Resource Planning

#### Memory Requirements
```bash
# Calculate memory needs
working_set_gb=$(lightning-cli analyze working-set --unit gb)
cache_gb=$((working_set_gb / 4))
system_memory_gb=$((cache_gb * 2))

echo "Recommended system memory: ${system_memory_gb}GB"
```

#### Storage Requirements
```bash
# Calculate storage needs with growth
current_size_gb=$(lightning-cli stats disk --unit gb)
growth_rate_gb_month=$(lightning-cli analyze growth-rate --period 6m --unit gb-month)
months_ahead=12
projected_size_gb=$((current_size_gb + growth_rate_gb_month * months_ahead))
storage_with_overhead_gb=$((projected_size_gb * 2))  # 100% overhead

echo "Recommended storage: ${storage_with_overhead_gb}GB"
```

## Emergency Procedures

### Service Recovery

#### Emergency Restart
```bash
#!/bin/bash
# Emergency restart procedure

# 1. Check if process is hung
if ! lightning-cli health --timeout 5; then
    echo "Service unresponsive, forcing restart"
    
    # 2. Graceful stop with timeout
    sudo systemctl stop lightning-db
    sleep 10
    
    # 3. Force kill if needed
    pkill -f lightning
    
    # 4. Check for corruption
    lightning-cli verify integrity --quick
    
    # 5. Start service
    sudo systemctl start lightning-db
    
    # 6. Verify recovery
    lightning-cli health
fi
```

#### Disaster Recovery
```bash
#!/bin/bash
# Complete disaster recovery from backup

# 1. Assess damage
lightning-cli verify integrity

# 2. Stop service
sudo systemctl stop lightning-db

# 3. Backup corrupted data (for investigation)
sudo mv /var/lib/lightning-db/data /var/lib/lightning-db/data.corrupted

# 4. Restore from latest backup
LATEST_BACKUP=$(ls -t /backup/lightning-db/full-* | head -1)
lightning-cli restore \
  --source "$LATEST_BACKUP" \
  --dest /var/lib/lightning-db/data

# 5. Apply incremental backups if available
for inc_backup in $(ls -t /backup/lightning-db/inc-* | grep "$(date +%Y%m%d)"); do
    lightning-cli restore \
      --source "$inc_backup" \
      --dest /var/lib/lightning-db/data \
      --incremental
done

# 6. Fix permissions
sudo chown -R lightning:lightning /var/lib/lightning-db

# 7. Start service
sudo systemctl start lightning-db

# 8. Verify recovery
lightning-cli health --comprehensive
```

### Escalation Procedures

#### Contact Information
```bash
# Primary: Database Team
# Email: db-team@company.com
# Phone: +1-555-0123
# Slack: #database-ops

# Secondary: Infrastructure Team  
# Email: infra-team@company.com
# Phone: +1-555-0456
# Slack: #infrastructure

# Emergency: On-call Engineer
# PagerDuty: lightning-db-critical
```

#### Escalation Triggers
- Data corruption detected
- Service unavailable >5 minutes
- Memory usage >95% of limit
- Error rate >1% for >2 minutes
- Backup failures for >24 hours

---

This operations manual provides comprehensive guidance for deploying, monitoring, and maintaining Lightning DB in production environments. Regular review and updates ensure operational excellence and system reliability.

*Last Updated: Operations Manual v1.0*  
*Next Review: Quarterly or after major incidents*  
*Owner: Lightning DB Operations Team*