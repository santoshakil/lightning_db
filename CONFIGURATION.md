# Lightning DB Configuration Reference

## Overview

Lightning DB uses TOML configuration files for comprehensive system configuration. This document provides a complete reference for all configuration options, recommended settings for different environments, and optimization guidelines.

## Configuration File Locations

```bash
# System-wide configuration (highest priority)
/etc/lightning_db/config.toml

# User-specific configuration
~/.config/lightning_db/config.toml

# Application-specific configuration
./lightning_db.toml

# Environment variable override
export LIGHTNING_DB_CONFIG="/path/to/custom/config.toml"
```

## Complete Configuration Reference

### Server Configuration

```toml
[server]
# Network binding configuration
bind_address = "127.0.0.1:9090"        # Primary API endpoint
metrics_bind_address = "127.0.0.1:9091" # Metrics endpoint (optional)
admin_bind_address = "127.0.0.1:9092"   # Admin interface (optional)

# Connection management
max_connections = 1000                   # Maximum concurrent connections
min_idle_connections = 10               # Minimum idle connections in pool
connection_timeout = "30s"              # Connection establishment timeout
request_timeout = "60s"                 # Maximum request processing time
keepalive_timeout = "300s"              # TCP keepalive timeout
idle_timeout = "600s"                   # Connection idle timeout

# Request handling
max_request_size = "100MB"              # Maximum request payload size
max_response_size = "1GB"               # Maximum response size
request_queue_size = 10000              # Request queue depth
worker_threads = 0                      # 0 = auto-detect CPU cores

# HTTP/API settings
cors_enabled = false                    # Enable CORS for web clients
cors_origins = ["*"]                    # Allowed CORS origins
compression_enabled = true              # Enable response compression
compression_threshold = 1024            # Minimum size for compression
```

### Database Configuration

```toml
[database]
# Core database settings
data_directory = "/data/lightning_db"   # Primary data storage location
temp_directory = "/tmp/lightning_db"    # Temporary files location
page_size = 4096                        # Database page size (bytes)
max_file_size = "2GB"                   # Maximum single file size

# Memory management
cache_size = "1GB"                      # Total cache size
cache_policy = "arc"                    # Cache policy: lru, arc, clock
dirty_page_threshold = 0.7              # Dirty page flush threshold (0.0-1.0)
memory_map_threshold = "10MB"           # Minimum file size for mmap

# Transaction settings
max_active_transactions = 1000          # Maximum concurrent transactions
transaction_timeout = "300s"            # Transaction timeout
isolation_level = "snapshot"            # snapshot, read_committed, serializable
deadlock_detection_interval = "1s"     # Deadlock detection frequency

# Write-Ahead Log (WAL) settings
wal_enabled = true                      # Enable WAL for durability
wal_directory = "/data/lightning_db/wal" # WAL storage location
wal_sync_mode = "periodic"              # sync, periodic, async
wal_sync_interval = "1s"                # Sync interval for periodic mode
max_wal_size = "500MB"                  # Maximum WAL file size
wal_compression = true                  # Compress WAL entries

# Checkpoint settings
checkpoint_interval = "5m"              # Automatic checkpoint frequency
checkpoint_threshold = "100MB"          # WAL size threshold for checkpoint
checkpoint_timeout = "60s"              # Maximum checkpoint duration
force_checkpoint_on_shutdown = true     # Force checkpoint during shutdown
```

### Performance Configuration

```toml
[performance]
# Write optimization
write_batch_size = 1000                 # Operations per write batch
write_buffer_size = "64MB"              # Write buffer size
write_threads = 2                       # Dedicated write threads
sync_writes = false                     # Synchronous write mode

# Read optimization
prefetch_enabled = true                 # Enable read prefetching
prefetch_distance = 64                  # Pages to prefetch ahead
prefetch_workers = 4                    # Prefetch worker threads
read_ahead_size = "1MB"                 # Read-ahead buffer size

# Compression settings
compression_enabled = true              # Enable data compression
compression_type = "zstd"               # zstd, lz4, snappy, none
compression_level = 3                   # Compression level (1-22 for zstd)
compression_threshold = 512             # Minimum size for compression
dictionary_enabled = true               # Use compression dictionaries

# Indexing optimization
index_cache_size = "256MB"              # Index-specific cache
btree_node_size = 4096                  # B+Tree node size
btree_fill_factor = 0.9                 # B+Tree fill factor (0.5-1.0)
index_compression = true                # Compress index pages

# Background processing
background_threads = 2                  # Background maintenance threads
compaction_enabled = true               # Enable background compaction
compaction_interval = "1h"              # Compaction frequency
compaction_threshold = 0.3              # Fragmentation threshold for compaction
```

### Storage Configuration

```toml
[storage]
# File system settings
file_sync_mode = "datasync"             # fsync, fdatasync, datasync
direct_io = false                       # Use direct I/O (Linux only)
file_permissions = 0o600                # File permissions (octal)
directory_permissions = 0o700           # Directory permissions (octal)

# Storage engine settings
storage_engine = "file"                 # file, memory, cloud
storage_format = "custom"               # custom, sqlite, leveldb
endianness = "little"                   # little, big, native

# File allocation
preallocate_files = true                # Preallocate file space
allocation_chunk_size = "10MB"          # File growth chunk size
sparse_files = false                    # Use sparse files

# Backup integration
backup_enabled = false                  # Enable integrated backup
backup_directory = "/backup/lightning_db" # Backup storage location
backup_schedule = "0 2 * * *"           # Cron-style backup schedule
backup_retention = "30d"                # Backup retention period
backup_compression = true               # Compress backup files
```

### Security Configuration

```toml
[security]
# TLS/SSL settings
tls_enabled = false                     # Enable TLS encryption
tls_min_version = "1.2"                 # Minimum TLS version (1.2, 1.3)
tls_cert_file = "/etc/ssl/certs/lightning_db.crt" # Server certificate
tls_key_file = "/etc/ssl/private/lightning_db.key" # Private key
tls_ca_file = "/etc/ssl/certs/ca.crt"   # Certificate Authority
tls_crl_file = ""                       # Certificate Revocation List
tls_cipher_suites = []                  # Allowed cipher suites (empty = default)
tls_client_auth = "none"                # none, optional, require

# Authentication
auth_enabled = false                    # Enable authentication
auth_method = "api_key"                 # api_key, jwt, oauth2, certificate
auth_config_file = "/etc/lightning_db/auth.toml" # Auth configuration
session_timeout = "24h"                # Session timeout
max_login_attempts = 5                  # Maximum failed login attempts
lockout_duration = "15m"               # Account lockout duration

# Authorization
rbac_enabled = false                    # Role-based access control
permission_model = "allow_all"          # allow_all, deny_all, rbac
admin_users = []                        # List of admin users
readonly_users = []                     # List of read-only users

# Data protection
encryption_at_rest = false              # Encrypt stored data
encryption_algorithm = "AES256-GCM"     # Encryption algorithm
key_file = "/etc/lightning_db/master.key" # Master encryption key
key_rotation_interval = "90d"           # Key rotation frequency

# Audit logging
audit_enabled = false                   # Enable audit logging
audit_file = "/var/log/lightning_db/audit.log" # Audit log file
audit_level = "info"                    # trace, debug, info, warn, error
audit_operations = ["write", "delete", "admin"] # Operations to audit
audit_retention = "1y"                  # Audit log retention
```

### Monitoring Configuration

```toml
[monitoring]
# General monitoring
enabled = true                          # Enable monitoring
update_interval = "10s"                 # Metrics update frequency
retention_period = "24h"                # Metrics retention in memory

# Prometheus integration
prometheus_enabled = false              # Enable Prometheus metrics
prometheus_endpoint = "/metrics"        # Metrics endpoint path
prometheus_namespace = "lightning_db"   # Metrics namespace
prometheus_buckets = [0.001, 0.01, 0.1, 1.0, 10.0] # Histogram buckets

# OpenTelemetry integration
opentelemetry_enabled = false           # Enable OpenTelemetry
otel_service_name = "lightning_db"      # Service name
otel_service_version = "1.0.0"          # Service version
otel_endpoint = "http://localhost:4317" # OTLP endpoint
otel_headers = {}                       # Additional headers

# Health checks
health_check_enabled = true             # Enable health checks
health_check_interval = "30s"           # Health check frequency
health_check_timeout = "5s"             # Health check timeout
health_check_endpoints = ["/health", "/ready"] # Health endpoints

# Alerting
alerting_enabled = false                # Enable built-in alerting
alert_webhook_url = ""                  # Webhook for alerts
alert_thresholds = { cpu = 80, memory = 90, disk = 95 } # Alert thresholds
```

### Logging Configuration

```toml
[logging]
# General logging settings
level = "info"                          # trace, debug, info, warn, error
format = "text"                         # text, json
timestamp_format = "rfc3339"            # rfc3339, unix, custom

# Output configuration
output = "file"                         # stdout, stderr, file, syslog
file_path = "/var/log/lightning_db/app.log" # Log file path
max_file_size = "100MB"                 # Maximum log file size
max_backup_files = 10                   # Number of backup files to keep
compress_backups = true                 # Compress rotated log files

# Log rotation
rotation = "daily"                      # daily, weekly, size
rotation_time = "00:00"                 # Rotation time (for daily/weekly)

# Component-specific logging
component_levels = {
    "database" = "info",
    "network" = "warn", 
    "security" = "debug",
    "performance" = "info"
}

# Structured logging
include_caller = false                  # Include caller information
include_stacktrace = false              # Include stack trace for errors
correlation_id_header = "X-Correlation-ID" # HTTP correlation ID header
```

### Clustering Configuration

```toml
[clustering]
# Cluster settings
enabled = false                         # Enable clustering
cluster_name = "lightning_db_cluster"   # Cluster identifier
node_id = 1                             # Unique node identifier
node_name = "node1"                     # Human-readable node name

# Discovery settings
discovery_method = "static"             # static, consul, etcd, dns
static_peers = [                        # Static peer list
    "node2:9090",
    "node3:9090"
]
discovery_interval = "30s"              # Peer discovery frequency

# Replication settings
replication_enabled = false             # Enable data replication
replication_factor = 3                  # Number of replicas
consistency_level = "quorum"            # one, quorum, all
replication_timeout = "5s"              # Replication timeout

# Leader election
leader_election = true                  # Enable leader election
election_timeout = "5s"                 # Election timeout
heartbeat_interval = "1s"               # Leader heartbeat interval
```

### Advanced Configuration

```toml
[experimental]
# Experimental features (use with caution)
async_io = false                        # Use async I/O (io_uring on Linux)
lock_free_structures = false            # Use lock-free data structures
numa_aware = false                      # NUMA-aware memory allocation
huge_pages = false                      # Use huge pages for memory

# Advanced tuning
memory_allocator = "system"             # system, jemalloc, tcmalloc
cpu_affinity = []                       # CPU core affinity
scheduler_policy = "normal"             # normal, fifo, batch

# Development settings
debug_mode = false                      # Enable debug features
profile_enabled = false                 # Enable performance profiling
trace_enabled = false                   # Enable detailed tracing
assertions_enabled = false              # Enable runtime assertions
```

## Environment-Specific Configurations

### Development Environment

```toml
# development.toml
[server]
bind_address = "127.0.0.1:9090"
max_connections = 100

[database]
data_directory = "./data"
cache_size = "256MB"
wal_sync_mode = "async"

[performance]
compression_enabled = false
prefetch_enabled = false

[logging]
level = "debug"
output = "stdout"
format = "text"

[monitoring]
prometheus_enabled = true

[experimental]
debug_mode = true
assertions_enabled = true
```

### Testing Environment

```toml
# testing.toml
[server]
bind_address = "127.0.0.1:19090"
max_connections = 50
request_timeout = "10s"

[database]
data_directory = "/tmp/lightning_db_test"
cache_size = "128MB"
wal_sync_mode = "sync"
checkpoint_interval = "30s"

[performance]
write_batch_size = 100
compression_enabled = true
compression_type = "lz4"

[logging]
level = "info"
output = "file"
file_path = "/tmp/lightning_db_test.log"

[monitoring]
enabled = true
update_interval = "5s"
```

### Production Environment

```toml
# production.toml
[server]
bind_address = "0.0.0.0:9090"
metrics_bind_address = "0.0.0.0:9091"
max_connections = 2000
connection_timeout = "30s"
request_timeout = "120s"
worker_threads = 16

[database]
data_directory = "/data/lightning_db"
cache_size = "8GB"
max_active_transactions = 5000
wal_sync_mode = "periodic"
wal_sync_interval = "1s"
checkpoint_interval = "5m"

[performance]
write_batch_size = 10000
compression_enabled = true
compression_type = "zstd"
compression_level = 6
prefetch_enabled = true
prefetch_distance = 128
prefetch_workers = 8
background_threads = 4

[storage]
preallocate_files = true
file_sync_mode = "fdatasync"

[security]
tls_enabled = true
tls_min_version = "1.3"
auth_enabled = true
encryption_at_rest = true
audit_enabled = true

[monitoring]
prometheus_enabled = true
opentelemetry_enabled = true
health_check_enabled = true
alerting_enabled = true

[logging]
level = "info"
format = "json"
output = "file"
file_path = "/var/log/lightning_db/app.log"
rotation = "daily"
max_backup_files = 30
```

### High-Performance Environment

```toml
# high-performance.toml
[server]
worker_threads = 32
max_connections = 10000

[database]
cache_size = "32GB"
page_size = 8192
max_active_transactions = 20000

[performance]
write_batch_size = 50000
write_buffer_size = "256MB"
write_threads = 8
compression_type = "lz4"
compression_level = 1
prefetch_distance = 256
prefetch_workers = 16
background_threads = 8

[storage]
direct_io = true
preallocate_files = true
allocation_chunk_size = "100MB"

[experimental]
async_io = true
lock_free_structures = true
numa_aware = true
huge_pages = true
memory_allocator = "jemalloc"
```

## Configuration Validation

### Validation Tool

```bash
# Validate configuration file
lightning_db config --validate --file=config.toml

# Show effective configuration
lightning_db config --show-effective

# Check for deprecated options
lightning_db config --check-deprecated

# Generate configuration template
lightning_db config --generate-template > template.toml
```

### Common Validation Errors

```toml
# ERROR: Invalid data type
cache_size = 1000000000  # Should be "1GB" not a number

# ERROR: Invalid enum value
compression_type = "invalid"  # Should be zstd, lz4, snappy, or none

# ERROR: Invalid time format
checkpoint_interval = "5 minutes"  # Should be "5m"

# ERROR: Invalid size format
max_file_size = "2 GB"  # Should be "2GB" (no space)

# ERROR: Path doesn't exist
data_directory = "/nonexistent/path"

# ERROR: Invalid permissions
file_permissions = 777  # Should be 0o777 (octal) or "0o777"
```

## Performance Tuning Guidelines

### Memory Configuration

```toml
# For systems with 8GB RAM
[database]
cache_size = "4GB"       # 50% of RAM for cache
dirty_page_threshold = 0.7

[performance]
index_cache_size = "1GB" # 12.5% of RAM for indexes
write_buffer_size = "256MB"

# For systems with 64GB RAM
[database]
cache_size = "32GB"
dirty_page_threshold = 0.8

[performance]
index_cache_size = "8GB"
write_buffer_size = "1GB"
```

### Storage Configuration

```toml
# For SSD storage
[storage]
file_sync_mode = "fdatasync"
direct_io = false
allocation_chunk_size = "10MB"

[performance]
write_batch_size = 10000
compression_type = "lz4"  # Fast compression

# For NVMe storage
[storage]
file_sync_mode = "datasync"
direct_io = true
allocation_chunk_size = "50MB"

[performance]
write_batch_size = 50000
compression_type = "zstd"
compression_level = 3

[experimental]
async_io = true
```

### Workload-Specific Tuning

```toml
# Read-heavy workload
[performance]
prefetch_enabled = true
prefetch_distance = 128
compression_type = "zstd"  # Better compression ratio
index_cache_size = "2GB"   # Larger index cache

[database]
cache_size = "8GB"         # Larger data cache

# Write-heavy workload
[performance]
write_batch_size = 20000
write_buffer_size = "512MB"
write_threads = 4
compression_type = "lz4"   # Faster compression
background_threads = 6

[database]
wal_sync_interval = "100ms" # More frequent WAL sync
checkpoint_interval = "2m"  # More frequent checkpoints

# Mixed workload
[performance]
write_batch_size = 5000
compression_type = "zstd"
compression_level = 3
prefetch_distance = 64

[database]
cache_size = "6GB"
checkpoint_interval = "5m"
```

## Configuration Management

### Configuration Inheritance

```toml
# base.toml
[database]
data_directory = "/data/lightning_db"
cache_size = "4GB"

[logging]
level = "info"
format = "json"

# production.toml (inherits from base.toml)
inherit = "base.toml"

[server]
bind_address = "0.0.0.0:9090"

[security]
tls_enabled = true
```

### Environment Variables

```bash
# Override configuration with environment variables
export LIGHTNING_DB_SERVER_BIND_ADDRESS="0.0.0.0:9090"
export LIGHTNING_DB_DATABASE_CACHE_SIZE="8GB"
export LIGHTNING_DB_LOGGING_LEVEL="debug"

# Configuration file from environment
export LIGHTNING_DB_CONFIG="/etc/lightning_db/production.toml"
```

### Configuration Hot Reload

```bash
# Reload configuration without restart (subset of options)
lightning_db config --reload

# Check which options support hot reload
lightning_db config --hot-reload-options
```

## Troubleshooting Configuration

### Common Issues

1. **Configuration Not Found**
   ```bash
   # Check configuration search paths
   lightning_db config --show-paths
   
   # Verify file exists and is readable
   test -r /etc/lightning_db/config.toml && echo "OK" || echo "FAIL"
   ```

2. **Permission Errors**
   ```bash
   # Check file permissions
   ls -la /etc/lightning_db/config.toml
   
   # Fix permissions
   sudo chown lightning_db:lightning_db /etc/lightning_db/config.toml
   sudo chmod 640 /etc/lightning_db/config.toml
   ```

3. **Invalid Values**
   ```bash
   # Validate configuration
   lightning_db config --validate --verbose
   
   # Check specific sections
   lightning_db config --validate --section=database
   ```

### Configuration Debugging

```bash
# Show effective configuration
lightning_db config --show-effective --format=json

# Show configuration sources
lightning_db config --show-sources

# Show default values
lightning_db config --show-defaults

# Compare configurations
lightning_db config --diff base.toml production.toml
```

## Best Practices

### Security

1. **File Permissions**: Restrict configuration file access
2. **Sensitive Data**: Use environment variables for secrets
3. **Encryption**: Enable TLS and encryption at rest in production
4. **Authentication**: Always enable authentication in production

### Performance

1. **Memory**: Allocate 50-70% of RAM to cache
2. **Compression**: Use LZ4 for speed, Zstd for space
3. **Batch Size**: Tune based on workload characteristics
4. **Background Threads**: Match to CPU cores

### Reliability

1. **WAL**: Use periodic sync for balance of safety and performance
2. **Checkpoints**: Frequent checkpoints for faster recovery
3. **Backups**: Enable automatic backups
4. **Monitoring**: Enable comprehensive monitoring

### Maintainability

1. **Documentation**: Comment complex configuration choices
2. **Version Control**: Track configuration changes
3. **Validation**: Always validate before deployment
4. **Testing**: Test configuration changes in staging environment