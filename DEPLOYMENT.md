# Lightning DB Production Deployment Guide

## Overview

This guide covers deploying Lightning DB in production environments with focus on reliability, performance, security, and monitoring.

## Pre-Deployment Checklist

### System Requirements
- [ ] **CPU**: 4+ cores (8+ recommended)
- [ ] **RAM**: 8GB minimum (32GB+ recommended) 
- [ ] **Storage**: SSD with 100GB+ free space
- [ ] **Network**: Low-latency network (<1ms for clustering)
- [ ] **OS**: Linux kernel 5.4+ (Ubuntu 20.04+/RHEL 8+ recommended)

### Security Requirements
- [ ] Firewall configured with minimal required ports
- [ ] TLS certificates for encryption in transit
- [ ] File system encryption for data at rest
- [ ] User accounts with least-privilege access
- [ ] Log aggregation system configured

### Monitoring Requirements
- [ ] Prometheus/monitoring system available
- [ ] Log collection system (ELK, Fluentd, etc.)
- [ ] Alerting system configured
- [ ] Health check endpoints accessible

## Deployment Architectures

### 1. Single Node Deployment

**Use Case**: Development, testing, small applications
**Characteristics**: Simple setup, single point of failure

```yaml
# docker-compose.yml
version: '3.8'
services:
  lightning-db:
    image: lightningdb/lightning_db:latest
    container_name: lightning-db-single
    ports:
      - "9090:9090"
      - "9091:9091"  # Metrics endpoint
    volumes:
      - ./data:/data
      - ./config:/etc/lightning_db
      - ./logs:/var/log/lightning_db
    environment:
      - LIGHTNING_DB_CONFIG=/etc/lightning_db/production.toml
      - RUST_LOG=lightning_db=info
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "lightning_db", "health-check"]
      interval: 30s
      timeout: 10s
      retries: 3
    resource_limits:
      cpus: '4.0'
      memory: 8G
```

### 2. High Availability Deployment

**Use Case**: Production applications requiring high availability
**Characteristics**: Master-replica setup, automatic failover

```yaml
# docker-compose-ha.yml
version: '3.8'
services:
  lightning-db-primary:
    image: lightningdb/lightning_db:latest
    environment:
      - LIGHTNING_DB_MODE=primary
      - LIGHTNING_DB_REPLICATION_ENABLED=true
    volumes:
      - ./data/primary:/data
      - ./config/primary.toml:/etc/lightning_db/config.toml
    ports:
      - "9090:9090"
      - "9091:9091"
    
  lightning-db-replica-1:
    image: lightningdb/lightning_db:latest
    environment:
      - LIGHTNING_DB_MODE=replica
      - LIGHTNING_DB_PRIMARY_HOST=lightning-db-primary
    volumes:
      - ./data/replica1:/data
      - ./config/replica.toml:/etc/lightning_db/config.toml
    ports:
      - "9092:9090"
      - "9093:9091"
    depends_on:
      - lightning-db-primary
      
  lightning-db-replica-2:
    image: lightningdb/lightning_db:latest
    environment:
      - LIGHTNING_DB_MODE=replica
      - LIGHTNING_DB_PRIMARY_HOST=lightning-db-primary
    volumes:
      - ./data/replica2:/data
      - ./config/replica.toml:/etc/lightning_db/config.toml
    ports:
      - "9094:9090"
      - "9095:9091"
    depends_on:
      - lightning-db-primary

  haproxy:
    image: haproxy:latest
    volumes:
      - ./haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg
    ports:
      - "8080:8080"  # Load balancer endpoint
    depends_on:
      - lightning-db-primary
      - lightning-db-replica-1
      - lightning-db-replica-2
```

### 3. Kubernetes Deployment

```yaml
# lightning-db-k8s.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: lightning-db
  namespace: database
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
        image: lightningdb/lightning_db:1.0.0
        ports:
        - containerPort: 9090
          name: api
        - containerPort: 9091
          name: metrics
        env:
        - name: LIGHTNING_DB_CONFIG
          value: /etc/lightning_db/config.toml
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        volumeMounts:
        - name: data
          mountPath: /data
        - name: config
          mountPath: /etc/lightning_db
        resources:
          requests:
            cpu: 1000m
            memory: 4Gi
          limits:
            cpu: 4000m
            memory: 16Gi
        livenessProbe:
          httpGet:
            path: /health
            port: 9090
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 9090
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: config
        configMap:
          name: lightning-db-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 100Gi

---
apiVersion: v1
kind: Service
metadata:
  name: lightning-db
  namespace: database
spec:
  selector:
    app: lightning-db
  ports:
  - name: api
    port: 9090
    targetPort: 9090
  - name: metrics
    port: 9091
    targetPort: 9091
  clusterIP: None  # Headless service for StatefulSet

---
apiVersion: v1
kind: Service
metadata:
  name: lightning-db-lb
  namespace: database
spec:
  selector:
    app: lightning-db
  ports:
  - name: api
    port: 9090
    targetPort: 9090
  type: LoadBalancer
```

## Configuration Files

### Production Configuration

```toml
# production.toml
[server]
bind_address = "0.0.0.0:9090"
metrics_address = "0.0.0.0:9091"
max_connections = 1000
connection_timeout = "30s"
request_timeout = "60s"

[database]
data_directory = "/data"
page_size = 4096
cache_size = "4GB"
max_active_transactions = 10000
checkpoint_interval = "5m"
wal_sync_mode = "periodic"
wal_sync_interval = "1s"

[performance]
write_batch_size = 10000
prefetch_enabled = true
prefetch_distance = 64
prefetch_workers = 4
compression_enabled = true
compression_type = "zstd"
compression_level = 3

[security]
tls_enabled = true
tls_cert_file = "/etc/ssl/certs/lightning_db.crt"
tls_key_file = "/etc/ssl/private/lightning_db.key"
client_cert_required = false
max_request_size = "100MB"

[monitoring]
prometheus_enabled = true
opentelemetry_enabled = true
log_level = "info"
log_format = "json"
log_file = "/var/log/lightning_db/app.log"
log_rotation = "daily"
log_retention = "30d"

[replication]
enabled = false
role = "primary"  # primary or replica
primary_host = ""
replication_port = 9092
replication_timeout = "10s"
sync_mode = "async"  # sync or async

[backup]
enabled = true
backup_directory = "/backup"
backup_schedule = "0 2 * * *"  # Daily at 2 AM
backup_retention = "7d"
compression_enabled = true

[clustering]
enabled = false
cluster_size = 3
node_id = 1
discovery_method = "static"  # static, consul, etcd
static_nodes = []
```

### Load Balancer Configuration

```
# haproxy.cfg
global
    daemon
    maxconn 4096

defaults
    mode http
    timeout connect 5000ms
    timeout client 50000ms
    timeout server 50000ms
    option httplog

frontend lightning_db_frontend
    bind *:8080
    default_backend lightning_db_backend

backend lightning_db_backend
    balance roundrobin
    option httpchk GET /health
    server primary lightning-db-primary:9090 check
    server replica1 lightning-db-replica-1:9090 check backup
    server replica2 lightning-db-replica-2:9090 check backup

frontend lightning_db_metrics
    bind *:8081
    default_backend lightning_db_metrics_backend

backend lightning_db_metrics_backend
    balance roundrobin
    server primary lightning-db-primary:9091 check
    server replica1 lightning-db-replica-1:9091 check
    server replica2 lightning-db-replica-2:9091 check
```

## Security Hardening

### 1. File System Security

```bash
# Create dedicated user
sudo useradd -r -s /bin/false lightning_db
sudo mkdir -p /data /var/log/lightning_db
sudo chown lightning_db:lightning_db /data /var/log/lightning_db
sudo chmod 700 /data
sudo chmod 755 /var/log/lightning_db

# Set up file permissions
sudo chmod 600 /etc/lightning_db/config.toml
sudo chmod 600 /etc/ssl/private/lightning_db.key
sudo chmod 644 /etc/ssl/certs/lightning_db.crt
```

### 2. Network Security

```bash
# Firewall rules (UFW example)
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow from 10.0.0.0/8 to any port 9090  # API access from internal network
sudo ufw allow from 10.0.0.0/8 to any port 9091  # Metrics access
sudo ufw allow ssh
sudo ufw enable

# Or iptables rules
sudo iptables -A INPUT -p tcp --dport 9090 -s 10.0.0.0/8 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9091 -s 10.0.0.0/8 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 22 -j ACCEPT
sudo iptables -A INPUT -j DROP
```

### 3. TLS Configuration

```bash
# Generate TLS certificates
openssl req -x509 -newkey rsa:4096 -keyout lightning_db.key -out lightning_db.crt -days 365 -nodes

# Or use Let's Encrypt
sudo certbot certonly --standalone -d lightning-db.yourdomain.com
```

## Monitoring and Observability

### 1. Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'lightning-db'
    static_configs:
      - targets: ['lightning-db:9091']
    scrape_interval: 5s
    metrics_path: /metrics
    
  - job_name: 'lightning-db-cluster'
    static_configs:
      - targets: 
        - 'lightning-db-primary:9091'
        - 'lightning-db-replica-1:9091'  
        - 'lightning-db-replica-2:9091'
```

### 2. Grafana Dashboard

Import the Lightning DB dashboard from `monitoring/grafana-dashboard.json` or create custom dashboards monitoring:

- **Performance Metrics**: Operations/sec, latency percentiles, throughput
- **Resource Usage**: CPU, memory, disk I/O, network
- **Database Metrics**: Cache hit rate, transaction counts, WAL size
- **Error Rates**: Failed operations, timeout rates, corruption events

### 3. Alerting Rules

```yaml
# alerting-rules.yml
groups:
- name: lightning-db
  rules:
  - alert: LightningDBDown
    expr: up{job="lightning-db"} == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Lightning DB instance is down"
      
  - alert: LightningDBHighLatency
    expr: lightning_db_operation_duration_seconds{quantile="0.95"} > 0.1
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "Lightning DB high latency detected"
      
  - alert: LightningDBHighErrorRate
    expr: rate(lightning_db_operations_total{status="error"}[5m]) > 0.01
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "Lightning DB high error rate detected"
```

## Backup and Recovery

### 1. Automated Backup

```bash
#!/bin/bash
# backup.sh
BACKUP_DIR="/backup"
DB_DATA_DIR="/data"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="lightning_db_backup_${TIMESTAMP}.tar.gz"

# Create backup
lightning_db backup --data-dir "$DB_DATA_DIR" --output "$BACKUP_DIR/$BACKUP_FILE"

# Compress and encrypt (optional)
gpg --symmetric --cipher-algo AES256 "$BACKUP_DIR/$BACKUP_FILE"

# Upload to cloud storage (optional)
aws s3 cp "$BACKUP_DIR/$BACKUP_FILE.gpg" s3://your-backup-bucket/

# Cleanup old backups (keep 7 days)
find "$BACKUP_DIR" -name "lightning_db_backup_*.tar.gz*" -mtime +7 -delete
```

### 2. Recovery Procedures

```bash
# Point-in-time recovery
lightning_db restore --backup-file /backup/lightning_db_backup_20231201_020000.tar.gz --target-time "2023-12-01 10:30:00"

# Full recovery
lightning_db restore --backup-file /backup/lightning_db_backup_20231201_020000.tar.gz --data-dir /data
```

## Performance Tuning

### 1. System-Level Tuning

```bash
# Kernel parameters for optimal performance
echo 'vm.swappiness = 1' >> /etc/sysctl.conf
echo 'vm.dirty_ratio = 15' >> /etc/sysctl.conf
echo 'vm.dirty_background_ratio = 5' >> /etc/sysctl.conf
echo 'net.core.rmem_max = 16777216' >> /etc/sysctl.conf
echo 'net.core.wmem_max = 16777216' >> /etc/sysctl.conf
sysctl -p

# File system optimizations (ext4)
mount -o remount,noatime,barrier=0 /data

# CPU governor for performance
echo performance > /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

### 2. Application-Level Tuning

```toml
# High-performance configuration
[database]
cache_size = "50% of available RAM"
write_batch_size = 50000
checkpoint_interval = "2m"
wal_sync_mode = "periodic"
wal_sync_interval = "100ms"

[performance]
prefetch_enabled = true
prefetch_distance = 128
prefetch_workers = 8
compression_enabled = true
compression_type = "lz4"  # Faster than zstd
```

## Deployment Automation

### 1. Ansible Playbook

```yaml
# deploy-lightning-db.yml
---
- hosts: lightning_db_servers
  become: yes
  vars:
    lightning_db_version: "1.0.0"
    lightning_db_user: "lightning_db"
    
  tasks:
    - name: Create lightning_db user
      user:
        name: "{{ lightning_db_user }}"
        system: yes
        shell: /bin/false
        
    - name: Create data directories
      file:
        path: "{{ item }}"
        state: directory
        owner: "{{ lightning_db_user }}"
        group: "{{ lightning_db_user }}"
        mode: '0700'
      loop:
        - /data
        - /var/log/lightning_db
        - /etc/lightning_db
        
    - name: Download Lightning DB
      get_url:
        url: "https://releases.lightning-db.com/v{{ lightning_db_version }}/lightning_db-linux-x86_64.tar.gz"
        dest: "/tmp/lightning_db.tar.gz"
        
    - name: Extract Lightning DB
      unarchive:
        src: "/tmp/lightning_db.tar.gz"
        dest: "/usr/local"
        remote_src: yes
        
    - name: Install configuration
      template:
        src: production.toml.j2
        dest: /etc/lightning_db/config.toml
        owner: "{{ lightning_db_user }}"
        mode: '0600'
        
    - name: Install systemd service
      template:
        src: lightning-db.service.j2
        dest: /etc/systemd/system/lightning-db.service
      notify: restart lightning-db
        
    - name: Start and enable Lightning DB
      systemd:
        name: lightning-db
        state: started
        enabled: yes
        daemon_reload: yes
        
  handlers:
    - name: restart lightning-db
      systemd:
        name: lightning-db
        state: restarted
```

### 2. Terraform Infrastructure

```hcl
# infrastructure.tf
resource "aws_instance" "lightning_db" {
  count           = 3
  ami             = "ami-0c55b159cbfafe1d0"  # Ubuntu 20.04
  instance_type   = "r5.2xlarge"
  key_name        = var.key_name
  security_groups = [aws_security_group.lightning_db.name]
  
  ebs_block_device {
    device_name = "/dev/sdf"
    volume_type = "gp3"
    volume_size = 1000
    iops        = 3000
    encrypted   = true
  }
  
  tags = {
    Name = "lightning-db-${count.index + 1}"
    Role = count.index == 0 ? "primary" : "replica"
  }
  
  user_data = templatefile("install-lightning-db.sh", {
    role = count.index == 0 ? "primary" : "replica"
  })
}

resource "aws_security_group" "lightning_db" {
  name_prefix = "lightning-db-"
  
  ingress {
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
  
  ingress {
    from_port   = 9091
    to_port     = 9091
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
```

## Troubleshooting

### Common Deployment Issues

1. **Permission Errors**: Check file ownership and permissions
2. **Network Connectivity**: Verify firewall rules and DNS resolution
3. **Resource Constraints**: Monitor CPU, memory, and disk usage
4. **Configuration Errors**: Validate configuration file syntax

### Health Checks

```bash
# Basic health check
curl -f http://localhost:9090/health || exit 1

# Detailed system check
lightning_db system-check --verbose

# Performance baseline check
lightning_db benchmark --quick --threshold-file performance-baseline.json
```

## Next Steps

After successful deployment:

1. **Configure Monitoring**: Set up Prometheus, Grafana, and alerting
2. **Set up Backups**: Implement automated backup procedures  
3. **Security Review**: Conduct security assessment and penetration testing
4. **Performance Testing**: Run load tests to establish baselines
5. **Disaster Recovery**: Test failover and recovery procedures
6. **Documentation**: Update runbooks and operational procedures

## Support

- **Enterprise Support**: Available 24/7 with SLA guarantees
- **Community Support**: GitHub issues and discussions
- **Professional Services**: Migration assistance and custom implementations