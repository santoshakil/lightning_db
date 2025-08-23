# Lightning DB Production Deployment Guide

## Table of Contents
1. [System Requirements](#system-requirements)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Performance Tuning](#performance-tuning)
5. [Security](#security)
6. [Monitoring](#monitoring)
7. [Backup & Recovery](#backup--recovery)
8. [High Availability](#high-availability)
9. [Troubleshooting](#troubleshooting)
10. [Best Practices](#best-practices)

## System Requirements

### Hardware Requirements

#### Minimum Requirements
- CPU: 2 cores (x86_64 or ARM64)
- RAM: 4GB
- Storage: 10GB SSD
- Network: 1Gbps (if using network features)

#### Recommended Requirements
- CPU: 8+ cores
- RAM: 32GB+
- Storage: NVMe SSD with 100GB+
- Network: 10Gbps

#### Production Requirements
- CPU: 16+ cores with AVX2 support
- RAM: 64GB+ ECC memory
- Storage: Enterprise NVMe SSD with power-loss protection
- Network: Redundant 10Gbps+ connections

### Operating System Support

#### Linux (Recommended)
```bash
# Ubuntu 22.04 LTS / Debian 12
apt-get update
apt-get install -y build-essential libc6-dev

# RHEL 9 / Rocky Linux 9
dnf groupinstall "Development Tools"
dnf install glibc-devel

# Enable io_uring for best performance (Linux 5.1+)
echo 'kernel.io_uring_disabled = 0' >> /etc/sysctl.conf
sysctl -p
```

#### macOS
```bash
# Requires macOS 12+ (Monterey)
xcode-select --install
```

#### Windows
```powershell
# Windows Server 2022 / Windows 11
# Install Visual Studio Build Tools
winget install Microsoft.VisualStudio.2022.BuildTools
```

### Dependencies
```toml
# Cargo.toml production dependencies
[dependencies]
lightning_db = { version = "0.1", features = ["production"] }
tokio = { version = "1.42", features = ["full"] }
tracing = "0.1"
tracing-subscriber = "0.3"
prometheus = "0.14"
```

## Installation

### From Source
```bash
# Clone repository
git clone https://github.com/your-org/lightning_db.git
cd lightning_db

# Build release version
cargo build --release --features production

# Install systemd service (Linux)
sudo cp target/release/lightning_db /usr/local/bin/
sudo cp scripts/lightning_db.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable lightning_db
```

### Docker Deployment
```dockerfile
# Dockerfile
FROM rust:1.75 as builder

WORKDIR /app
COPY . .
RUN cargo build --release --features production

FROM debian:12-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/lightning_db /usr/local/bin/

EXPOSE 8080 9090
CMD ["lightning_db", "server"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  lightning_db:
    image: lightning_db:latest
    volumes:
      - db_data:/var/lib/lightning_db
      - ./config:/etc/lightning_db
    ports:
      - "8080:8080"  # API
      - "9090:9090"  # Metrics
    environment:
      - LIGHTNING_DB_CONFIG=/etc/lightning_db/production.toml
    restart: unless-stopped
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
    deploy:
      resources:
        limits:
          cpus: '8'
          memory: 32G
        reservations:
          cpus: '4'
          memory: 16G

volumes:
  db_data:
    driver: local
```

### Kubernetes Deployment
```yaml
# lightning-db-deployment.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: lightning-db
spec:
  serviceName: lightning-db
  replicas: 3
  selector:
    matchLabels:
      app: lightning-db
  template:
    metadata:
      labels:
        app: lightning-db
    spec:
      containers:
      - name: lightning-db
        image: lightning_db:latest
        ports:
        - containerPort: 8080
        - containerPort: 9090
        volumeMounts:
        - name: data
          mountPath: /var/lib/lightning_db
        - name: config
          mountPath: /etc/lightning_db
        resources:
          requests:
            memory: "16Gi"
            cpu: "4"
          limits:
            memory: "32Gi"
            cpu: "8"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 100Gi
```

## Configuration

### Production Configuration File
```toml
# /etc/lightning_db/production.toml

[database]
path = "/var/lib/lightning_db/data"
max_db_size = 1099511627776  # 1TB
cache_size = 17179869184      # 16GB
wal_enabled = true
sync_on_commit = true
compression_enabled = true
compression_type = "zstd"
compression_level = 3

[performance]
num_threads = 16
io_threads = 8
max_connections = 10000
connection_timeout = 30
idle_timeout = 300
batch_size = 1000
prefetch_size = 4096

[cache]
type = "arc"
size = 17179869184  # 16GB
ttl = 3600          # 1 hour
eviction_policy = "lru"
warm_on_startup = true

[compaction]
auto_compaction = true
compaction_interval = 3600     # 1 hour
compaction_threshold = 0.3     # 30% fragmentation
max_concurrent_compactions = 2
compaction_priority = "low"

[wal]
enabled = true
sync_mode = "fsync"
max_size = 1073741824  # 1GB
rotation_size = 104857600  # 100MB
compression = true
buffer_size = 4194304  # 4MB

[backup]
enabled = true
schedule = "0 2 * * *"  # Daily at 2 AM
retention_days = 30
compression = true
encryption = true
backup_path = "/backup/lightning_db"

[security]
encryption_at_rest = true
encryption_algorithm = "aes-256-gcm"
key_rotation_interval = 2592000  # 30 days
tls_enabled = true
tls_cert = "/etc/lightning_db/cert.pem"
tls_key = "/etc/lightning_db/key.pem"
auth_enabled = true
auth_method = "jwt"

[monitoring]
metrics_enabled = true
metrics_port = 9090
tracing_enabled = true
tracing_endpoint = "http://jaeger:14268/api/traces"
logging_level = "info"
log_format = "json"
log_output = "/var/log/lightning_db/lightning.log"

[limits]
max_key_size = 4096
max_value_size = 104857600  # 100MB
max_batch_size = 10000
max_open_files = 10000
memory_limit = 34359738368  # 32GB

[network]
listen_address = "0.0.0.0:8080"
admin_address = "127.0.0.1:8081"
max_request_size = 10485760  # 10MB
keepalive_interval = 30
tcp_nodelay = true
```

### Environment Variables
```bash
# /etc/environment or .env file
export LIGHTNING_DB_CONFIG=/etc/lightning_db/production.toml
export LIGHTNING_DB_DATA=/var/lib/lightning_db
export LIGHTNING_DB_LOG_LEVEL=info
export LIGHTNING_DB_METRICS_ENABLED=true
export LIGHTNING_DB_ENCRYPTION_KEY_FILE=/etc/lightning_db/master.key
export RUST_BACKTRACE=1
export RUST_LOG=lightning_db=info
```

## Performance Tuning

### Linux Kernel Parameters
```bash
# /etc/sysctl.d/99-lightning-db.conf

# Memory
vm.swappiness = 1
vm.dirty_ratio = 15
vm.dirty_background_ratio = 5
vm.overcommit_memory = 1

# Network (if using network features)
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15

# File System
fs.file-max = 2097152
fs.nr_open = 2097152

# Apply settings
sysctl -p /etc/sysctl.d/99-lightning-db.conf
```

### Process Limits
```bash
# /etc/security/limits.d/lightning-db.conf
lightning_db soft nofile 65536
lightning_db hard nofile 65536
lightning_db soft nproc 32768
lightning_db hard nproc 32768
lightning_db soft memlock unlimited
lightning_db hard memlock unlimited
```

### CPU Affinity
```bash
# Pin database processes to specific CPUs
taskset -c 0-15 lightning_db server

# Or use systemd
# /etc/systemd/system/lightning_db.service
[Service]
CPUAffinity=0-15
```

### Memory Configuration
```bash
# Huge pages for better performance
echo 'vm.nr_hugepages = 8192' >> /etc/sysctl.conf
sysctl -p

# Disable NUMA balancing if multiple NUMA nodes
echo 0 > /proc/sys/kernel/numa_balancing
```

### Storage Optimization
```bash
# Mount options for data directory
mount -o noatime,nodiratime,nobarrier /dev/nvme0n1 /var/lib/lightning_db

# I/O scheduler for NVMe
echo 'none' > /sys/block/nvme0n1/queue/scheduler

# Read-ahead
blockdev --setra 256 /dev/nvme0n1
```

## Security

### Encryption Setup
```bash
# Generate master key
openssl rand -base64 32 > /etc/lightning_db/master.key
chmod 600 /etc/lightning_db/master.key
chown lightning_db:lightning_db /etc/lightning_db/master.key

# Generate TLS certificates
openssl req -x509 -nodes -days 365 -newkey rsa:4096 \
    -keyout /etc/lightning_db/key.pem \
    -out /etc/lightning_db/cert.pem \
    -subj "/C=US/ST=State/L=City/O=Org/CN=lightning-db.local"
```

### Firewall Rules
```bash
# iptables rules
iptables -A INPUT -p tcp --dport 8080 -j ACCEPT  # API
iptables -A INPUT -p tcp --dport 9090 -s 10.0.0.0/8 -j ACCEPT  # Metrics (internal only)
iptables -A INPUT -p tcp --dport 8081 -s 127.0.0.1 -j ACCEPT  # Admin (localhost only)

# UFW (Ubuntu)
ufw allow 8080/tcp
ufw allow from 10.0.0.0/8 to any port 9090
```

### SELinux Configuration
```bash
# RHEL/CentOS
semanage fcontext -a -t lightning_db_t "/var/lib/lightning_db(/.*)?"
restorecon -Rv /var/lib/lightning_db

# Create custom policy
cat > lightning_db.te << EOF
module lightning_db 1.0;
require {
    type lightning_db_t;
    class file { read write create unlink };
    class dir { read write add_name remove_name };
}
allow lightning_db_t self:file { read write create unlink };
allow lightning_db_t self:dir { read write add_name remove_name };
EOF

checkmodule -M -m -o lightning_db.mod lightning_db.te
semodule_package -o lightning_db.pp -m lightning_db.mod
semodule -i lightning_db.pp
```

## Monitoring

### Prometheus Configuration
```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'lightning_db'
    static_configs:
      - targets: ['localhost:9090']
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: 'lightning_db_.*'
        action: keep
```

### Grafana Dashboard
```json
{
  "dashboard": {
    "title": "Lightning DB Monitoring",
    "panels": [
      {
        "title": "Operations Per Second",
        "targets": [
          {
            "expr": "rate(lightning_db_operations_total[5m])"
          }
        ]
      },
      {
        "title": "Cache Hit Rate",
        "targets": [
          {
            "expr": "rate(lightning_db_cache_hits_total[5m]) / (rate(lightning_db_cache_hits_total[5m]) + rate(lightning_db_cache_misses_total[5m]))"
          }
        ]
      },
      {
        "title": "Latency Percentiles",
        "targets": [
          {
            "expr": "histogram_quantile(0.99, rate(lightning_db_operation_duration_seconds_bucket[5m]))"
          }
        ]
      }
    ]
  }
}
```

### Alerting Rules
```yaml
# alerts.yml
groups:
  - name: lightning_db
    rules:
      - alert: HighErrorRate
        expr: rate(lightning_db_errors_total[5m]) > 0.01
        for: 5m
        annotations:
          summary: "High error rate detected"
          
      - alert: LowCacheHitRate
        expr: lightning_db_cache_hit_rate < 0.8
        for: 10m
        annotations:
          summary: "Cache hit rate below 80%"
          
      - alert: HighMemoryUsage
        expr: lightning_db_memory_usage_bytes > 30000000000
        for: 5m
        annotations:
          summary: "Memory usage above 30GB"
          
      - alert: CompactionBacklog
        expr: lightning_db_compaction_pending > 10
        for: 30m
        annotations:
          summary: "Compaction backlog detected"
```

### Health Checks
```bash
# Basic health check
curl -f http://localhost:8080/health || exit 1

# Detailed health check
curl http://localhost:8080/health/detailed | jq '.'

# Readiness check
curl -f http://localhost:8080/ready || exit 1
```

## Backup & Recovery

### Backup Strategy
```bash
#!/bin/bash
# /usr/local/bin/lightning_db_backup.sh

BACKUP_DIR="/backup/lightning_db"
DATE=$(date +%Y%m%d_%H%M%S)
DB_PATH="/var/lib/lightning_db"

# Create backup directory
mkdir -p "$BACKUP_DIR/$DATE"

# Perform online backup
lightning_db backup \
    --source "$DB_PATH" \
    --destination "$BACKUP_DIR/$DATE" \
    --compression zstd \
    --encryption \
    --incremental

# Verify backup
lightning_db verify-backup \
    --backup-path "$BACKUP_DIR/$DATE"

# Upload to S3 (optional)
aws s3 sync "$BACKUP_DIR/$DATE" "s3://backups/lightning_db/$DATE" \
    --storage-class GLACIER

# Clean old backups
find "$BACKUP_DIR" -type d -mtime +30 -exec rm -rf {} \;

# Log backup completion
echo "$(date): Backup completed successfully" >> /var/log/lightning_db/backup.log
```

### Recovery Procedure
```bash
# Stop database
systemctl stop lightning_db

# Restore from backup
lightning_db restore \
    --backup-path "/backup/lightning_db/20240120_020000" \
    --destination "/var/lib/lightning_db" \
    --verify

# Start database
systemctl start lightning_db

# Verify data integrity
lightning_db verify-integrity \
    --data-path "/var/lib/lightning_db"
```

### Point-in-Time Recovery
```bash
# Enable PITR in configuration
[backup]
pitr_enabled = true
pitr_retention_days = 7
wal_archive_path = "/archive/lightning_db/wal"

# Recover to specific timestamp
lightning_db pitr \
    --backup-path "/backup/lightning_db/20240120_020000" \
    --wal-archive "/archive/lightning_db/wal" \
    --target-time "2024-01-20 15:30:00" \
    --destination "/var/lib/lightning_db"
```

## High Availability

### Master-Slave Replication
```toml
# Master configuration
[replication]
role = "master"
bind_address = "0.0.0.0:5432"
max_slaves = 5
wal_keep_segments = 100

# Slave configuration
[replication]
role = "slave"
master_address = "master.lightning-db.local:5432"
sync_interval = 1
read_only = true
```

### Load Balancing
```nginx
# nginx.conf
upstream lightning_db_cluster {
    least_conn;
    server db1.local:8080 weight=1;
    server db2.local:8080 weight=1;
    server db3.local:8080 weight=1;
    
    keepalive 32;
}

server {
    listen 80;
    
    location / {
        proxy_pass http://lightning_db_cluster;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }
}
```

### Failover Configuration
```bash
#!/bin/bash
# /usr/local/bin/lightning_db_failover.sh

MASTER="db1.local"
SLAVE="db2.local"

# Check master health
if ! curl -f "http://$MASTER:8080/health" > /dev/null 2>&1; then
    echo "Master is down, promoting slave..."
    
    # Promote slave to master
    ssh $SLAVE "lightning_db promote-to-master"
    
    # Update DNS or load balancer
    # ...
    
    # Notify administrators
    mail -s "Lightning DB Failover Completed" admin@example.com < /dev/null
fi
```

## Troubleshooting

### Common Issues

#### 1. High Memory Usage
```bash
# Check memory stats
lightning_db stats --memory

# Force cache eviction
lightning_db admin cache-clear

# Adjust cache size
lightning_db config set cache.size 8589934592
```

#### 2. Slow Performance
```bash
# Check slow queries
lightning_db slow-log --duration 100ms

# Analyze query patterns
lightning_db analyze --verbose

# Run compaction
lightning_db compact --force
```

#### 3. Corruption Issues
```bash
# Check integrity
lightning_db verify-integrity --deep

# Repair corruption
lightning_db repair --auto

# Rebuild indexes
lightning_db rebuild-index --all
```

### Debug Mode
```bash
# Enable debug logging
export RUST_LOG=lightning_db=debug
lightning_db server

# Enable tracing
export LIGHTNING_DB_TRACE=true
export LIGHTNING_DB_TRACE_FILE=/tmp/lightning_db.trace

# Profile performance
lightning_db profile --duration 60 --output flamegraph.svg
```

### Log Analysis
```bash
# Common log patterns
grep ERROR /var/log/lightning_db/lightning.log | tail -100
grep "slow query" /var/log/lightning_db/lightning.log | awk '{print $NF}' | sort | uniq -c
grep "compaction" /var/log/lightning_db/lightning.log | grep -v completed

# Log rotation
cat > /etc/logrotate.d/lightning_db << EOF
/var/log/lightning_db/*.log {
    daily
    rotate 7
    compress
    missingok
    notifempty
    postrotate
        systemctl reload lightning_db
    endscript
}
EOF
```

## Best Practices

### 1. Capacity Planning
- Monitor growth rate: 20% headroom minimum
- Plan for peak load: 3x average capacity
- Regular capacity reviews: Monthly

### 2. Maintenance Windows
- Schedule compaction during low traffic
- Stagger backup times across replicas
- Test updates in staging first

### 3. Security
- Regular security audits
- Key rotation every 30 days
- Principle of least privilege
- Network segmentation

### 4. Monitoring
- Set up alerts before issues occur
- Track trends, not just current values
- Correlate metrics with business events

### 5. Documentation
- Document all configuration changes
- Maintain runbooks for common operations
- Keep architecture diagrams updated

### 6. Testing
- Regular disaster recovery drills
- Load testing before major changes
- Automated integration tests

### 7. Updates
- Follow semantic versioning
- Read release notes carefully
- Test in non-production first
- Have rollback plan ready

## Production Checklist

- [ ] Hardware meets requirements
- [ ] OS kernel parameters tuned
- [ ] Process limits configured
- [ ] Storage optimized (mount options, scheduler)
- [ ] Configuration file reviewed and customized
- [ ] Encryption keys generated and secured
- [ ] TLS certificates installed
- [ ] Firewall rules configured
- [ ] Monitoring setup (Prometheus, Grafana)
- [ ] Alerting rules configured
- [ ] Backup strategy implemented
- [ ] Backup verification automated
- [ ] Recovery procedures documented and tested
- [ ] High availability configured (if needed)
- [ ] Load balancing setup (if needed)
- [ ] Log rotation configured
- [ ] Security audit completed
- [ ] Performance baseline established
- [ ] Documentation completed
- [ ] Team trained on operations
- [ ] Support contract in place (if applicable)

## Support

### Community Support
- GitHub Issues: https://github.com/your-org/lightning_db/issues
- Discord: https://discord.gg/lightning-db
- Stack Overflow: [lightning-db] tag

### Commercial Support
- Email: support@lightning-db.com
- Phone: +1-555-LIGHTNING
- SLA: 24/7 for critical issues

### Resources
- Documentation: https://docs.lightning-db.com
- API Reference: https://api.lightning-db.com
- Performance Tuning Guide: https://lightning-db.com/tuning
- Security Best Practices: https://lightning-db.com/security