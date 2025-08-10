# Lightning DB Production Troubleshooting Guide

## Overview

This guide provides systematic approaches to diagnose and resolve common issues with Lightning DB in production. It includes diagnostic procedures, resolution steps, and preventive measures.

**Emergency Contacts**:
- **On-Call DBA**: _______________
- **Engineering Lead**: _______________
- **24/7 Support**: _______________
- **Escalation**: _______________

---

## Table of Contents

1. [Quick Diagnostics](#quick-diagnostics)
2. [Performance Issues](#performance-issues)
3. [Connection Problems](#connection-problems)
4. [Data Integrity Issues](#data-integrity-issues)
5. [Replication Problems](#replication-problems)
6. [Memory Issues](#memory-issues)
7. [Storage Problems](#storage-problems)
8. [Transaction Issues](#transaction-issues)
9. [Diagnostic Tools](#diagnostic-tools)
10. [Root Cause Analysis](#root-cause-analysis)

---

## Quick Diagnostics

### Initial Health Check (< 2 minutes)

```bash
#!/bin/bash
# quick_health_check.sh

echo "=== Lightning DB Quick Health Check ==="
echo "Time: $(date)"

# 1. Check if database is running
if systemctl is-active --quiet lightning-db; then
    echo "✓ Database is running"
else
    echo "✗ DATABASE IS DOWN!"
    exit 1
fi

# 2. Check connectivity
if lightning_db ping --timeout=5; then
    echo "✓ Database is responding"
else
    echo "✗ Database not responding to ping"
fi

# 3. Check basic metrics
lightning_db status --format=json | jq '{
    uptime: .uptime,
    connections: .connections.active,
    memory_usage: .memory.used_percent,
    disk_usage: .disk.used_percent,
    replication_lag: .replication.lag_seconds,
    error_rate: .errors.rate_per_minute
}'

# 4. Check recent errors
echo -e "\n=== Recent Errors ==="
tail -20 /var/log/lightning_db/error.log | grep -E "ERROR|FATAL|PANIC"

# 5. Check system resources
echo -e "\n=== System Resources ==="
df -h /var/lib/lightning_db
free -h
top -bn1 | head -5
```

### Severity Classification

| Level | Symptoms | Response Time | Action |
|-------|----------|---------------|--------|
| **P1 - Critical** | Database down, data loss risk | < 15 min | Page on-call, all hands |
| **P2 - High** | Performance degraded >50%, partial outage | < 30 min | Page on-call |
| **P3 - Medium** | Performance degraded <50%, minor issues | < 2 hours | Notify team |
| **P4 - Low** | Cosmetic issues, warnings | < 24 hours | Create ticket |

---

## Performance Issues

### Symptom: Slow Queries

#### 1. Identify Slow Queries

```sql
-- Find currently running slow queries
SELECT 
    query_id,
    username,
    query_text,
    duration_ms,
    state,
    wait_event
FROM lightning_db_stat_activity
WHERE duration_ms > 1000
ORDER BY duration_ms DESC;

-- Historical slow queries
SELECT 
    query_fingerprint,
    COUNT(*) as execution_count,
    AVG(duration_ms) as avg_duration,
    MAX(duration_ms) as max_duration,
    SUM(duration_ms) as total_duration
FROM query_log
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY query_fingerprint
HAVING AVG(duration_ms) > 1000
ORDER BY total_duration DESC
LIMIT 20;
```

#### 2. Analyze Query Plans

```bash
# Get query execution plan
lightning_db explain --analyze "YOUR_SLOW_QUERY"

# Check for common issues:
# - Missing indexes (Sequential Scan on large tables)
# - Poor join order
# - Incorrect statistics
```

#### 3. Quick Fixes

```sql
-- Update table statistics
ANALYZE TABLE affected_table;

-- Kill long-running queries
SELECT lightning_db_terminate_query(query_id)
FROM lightning_db_stat_activity
WHERE duration_ms > 300000;  -- 5 minutes

-- Clear query cache if stale
CALL lightning_db_clear_query_cache();
```

### Symptom: High CPU Usage

#### 1. Identify CPU Consumers

```bash
# Database processes consuming CPU
top -p $(pgrep -d',' lightning_db) -b -n 1

# Query-level CPU usage
lightning_db top --sort=cpu --limit=10

# System call analysis
strace -c -p $(pgrep lightning_db | head -1) -f -T 10
```

#### 2. Common Causes and Solutions

```bash
# Runaway queries
lightning_db query list --min-cpu=50 --kill

# Inefficient sorting/grouping
# Add appropriate indexes
lightning_db suggest-indexes --workload=recent

# Lock contention
lightning_db locks --show-blocking

# Background maintenance
lightning_db maintenance status
lightning_db maintenance pause  # If during peak hours
```

### Symptom: High Latency Spikes

#### 1. Latency Analysis

```python
#!/usr/bin/env python3
# latency_analyzer.py

import subprocess
import json
import statistics

def analyze_latency():
    # Collect latency samples
    cmd = "lightning_db benchmark --duration=60 --read-only"
    result = subprocess.run(cmd.split(), capture_output=True, text=True)
    
    latencies = json.loads(result.stdout)['latencies']
    
    print(f"Latency Analysis:")
    print(f"  Min: {min(latencies):.2f}ms")
    print(f"  P50: {statistics.median(latencies):.2f}ms")
    print(f"  P95: {sorted(latencies)[int(len(latencies)*0.95)]:.2f}ms")
    print(f"  P99: {sorted(latencies)[int(len(latencies)*0.99)]:.2f}ms")
    print(f"  Max: {max(latencies):.2f}ms")
    
    # Identify outliers
    mean = statistics.mean(latencies)
    stdev = statistics.stdev(latencies)
    outliers = [l for l in latencies if l > mean + 3*stdev]
    
    if outliers:
        print(f"\nOutliers detected: {len(outliers)} requests > {mean + 3*stdev:.2f}ms")

if __name__ == "__main__":
    analyze_latency()
```

#### 2. Common Fixes

```bash
# Cache warming after restart
lightning_db cache warm --tables=frequently_accessed

# Disable synchronous commits temporarily
lightning_db config set wal_sync_mode=async

# Increase connection pool
lightning_db config set max_connections=500

# Enable query result caching
lightning_db config set query_cache_size=1GB
```

---

## Connection Problems

### Symptom: "Connection Refused"

#### 1. Basic Checks

```bash
# Check if Lightning DB is listening
netstat -tlnp | grep 5432
lsof -i :5432

# Check firewall rules
iptables -L -n | grep 5432
firewall-cmd --list-all

# Test connectivity
telnet localhost 5432
nc -zv localhost 5432
```

#### 2. Configuration Issues

```bash
# Check listen address
grep "listen_addresses" /etc/lightning_db/lightning_db.conf

# Check authentication
grep -E "host|local" /etc/lightning_db/auth.conf

# Verify SSL certificates
openssl s_client -connect localhost:5432 -ssl3
```

### Symptom: "Too Many Connections"

#### 1. Connection Analysis

```sql
-- Current connections by user
SELECT 
    username,
    application_name,
    COUNT(*) as connection_count,
    COUNT(*) FILTER (WHERE state = 'active') as active,
    COUNT(*) FILTER (WHERE state = 'idle') as idle,
    MAX(duration_ms) as longest_connection_ms
FROM lightning_db_stat_activity
GROUP BY username, application_name
ORDER BY connection_count DESC;

-- Connection pool efficiency
SELECT 
    AVG(wait_time_ms) as avg_wait_time,
    MAX(wait_time_ms) as max_wait_time,
    COUNT(*) FILTER (WHERE wait_time_ms > 1000) as slow_acquisitions
FROM connection_pool_stats
WHERE timestamp > NOW() - INTERVAL '10 minutes';
```

#### 2. Emergency Actions

```bash
# Increase connection limit (temporary)
lightning_db config set max_connections=1000 --reload

# Kill idle connections
lightning_db connections terminate --state=idle --duration=">1h"

# Identify connection leaks
for app in $(lightning_db connections list --format=json | jq -r '.[] | .application_name' | sort -u); do
    echo "App: $app"
    lightning_db connections list --app="$app" --format=summary
done
```

---

## Data Integrity Issues

### Symptom: Checksum Failures

#### 1. Identify Corrupted Pages

```bash
# Run integrity check
lightning_db verify --full --parallel=4 --output=corruption_report.json

# Parse results
cat corruption_report.json | jq '.errors[] | select(.type == "checksum_mismatch")'
```

#### 2. Recovery Procedures

```bash
# Attempt automatic repair
lightning_db repair --corruption-report=corruption_report.json --backup-first

# If repair fails, restore specific pages from backup
lightning_db page-restore \
    --backup=/backups/latest \
    --pages="1234,5678" \
    --verify

# Last resort: Full recovery
systemctl stop lightning-db
lightning_db recover --data=/var/lib/lightning_db --wal=/var/lib/lightning_db/wal
```

### Symptom: Constraint Violations

```sql
-- Find orphaned records
SELECT t1.*
FROM child_table t1
LEFT JOIN parent_table t2 ON t1.parent_id = t2.id
WHERE t2.id IS NULL;

-- Find duplicates
SELECT column1, column2, COUNT(*)
FROM table_name
GROUP BY column1, column2
HAVING COUNT(*) > 1;

-- Validate all constraints
CALL lightning_db_validate_constraints();
```

---

## Replication Problems

### Symptom: Replication Lag

#### 1. Measure Lag

```bash
# Check replication status
lightning_db replication status --detailed

# Monitor lag trend
while true; do
    lag=$(lightning_db replication lag --format=seconds)
    echo "$(date +%H:%M:%S) - Lag: ${lag}s"
    sleep 5
done
```

#### 2. Common Causes

```sql
-- Long-running transactions on primary
SELECT 
    pid,
    username,
    query_start,
    state,
    query
FROM lightning_db_stat_activity
WHERE timestamp < NOW() - INTERVAL '10 minutes'
ORDER BY timestamp;

-- Slow replica queries blocking replication
SELECT 
    pid,
    username,
    query
FROM lightning_db_stat_activity
WHERE state = 'active' 
AND query_start < NOW() - INTERVAL '1 minute';
```

#### 3. Fixes

```bash
# Increase WAL sender processes
lightning_db config set max_wal_senders=10 --reload

# Increase replication bandwidth
lightning_db config set wal_sender_bandwidth=100MB --reload

# Skip large transactions (careful!)
lightning_db replication skip-transaction --xid=12345

# Rebuild replica if too far behind
lightning_db replica rebuild --source=primary --parallel=8
```

### Symptom: Replication Broken

```bash
# Check replication slots
lightning_db replication slots list

# Recreate slot if missing
lightning_db replication slot create --name=replica1

# Reset replication
lightning_db replication reset --replica=replica1 --confirm
```

---

## Memory Issues

### Symptom: Out of Memory Errors

#### 1. Memory Analysis

```bash
# Database memory usage
lightning_db memory status --detailed

# System memory
free -h
vmstat 1 10

# Memory by process
ps aux | grep lightning_db | awk '{sum+=$6} END {print "Total RSS: " sum/1024 " MB"}'

# Shared memory
ipcs -m
```

#### 2. Identify Memory Consumers

```sql
-- Cache usage by table
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as size,
    cache_hit_ratio
FROM lightning_db_stat_user_tables
ORDER BY pg_relation_size(schemaname||'.'||tablename) DESC
LIMIT 20;

-- Memory by query
SELECT 
    query_fingerprint,
    AVG(memory_used_mb) as avg_memory,
    MAX(memory_used_mb) as max_memory,
    COUNT(*) as executions
FROM query_stats
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY query_fingerprint
ORDER BY max_memory DESC
LIMIT 10;
```

#### 3. Emergency Actions

```bash
# Clear caches
lightning_db cache clear --type=query
lightning_db cache clear --type=table --partial

# Reduce memory settings
lightning_db config set work_mem=64MB --reload
lightning_db config set shared_buffers=25% --reload

# Kill memory-intensive queries
lightning_db query terminate --memory=">1GB"
```

---

## Storage Problems

### Symptom: Disk Full

#### 1. Space Analysis

```bash
# Database space usage
lightning_db space report --detailed

# Find large tables
lightning_db tables list --sort=size --limit=20

# Find bloated tables
lightning_db bloat report --threshold=50

# Temporary files
find /var/lib/lightning_db/temp -type f -size +100M -exec ls -lh {} \;
```

#### 2. Emergency Cleanup

```bash
# Clear old WAL files
lightning_db wal cleanup --keep=2h

# Remove old backups
find /backup -name "*.backup" -mtime +7 -delete

# Vacuum tables
lightning_db vacuum --full --parallel=4 --tables=large_table

# Drop unnecessary indexes
lightning_db indexes unused --drop --older-than=30d
```

### Symptom: I/O Errors

```bash
# Check disk health
smartctl -a /dev/sda
dmesg | grep -i "error\|fail"

# I/O statistics
iostat -x 1 10
iotop -o

# File system checks
fsck -n /var/lib/lightning_db
```

---

## Transaction Issues

### Symptom: Deadlocks

#### 1. Identify Deadlocks

```sql
-- Recent deadlocks
SELECT * FROM lightning_db_stat_deadlocks
ORDER BY deadlock_time DESC
LIMIT 10;

-- Current lock waits
SELECT 
    blocked.pid as blocked_pid,
    blocked.username as blocked_user,
    blocking.pid as blocking_pid,
    blocking.username as blocking_user,
    blocked.query as blocked_query,
    blocking.query as blocking_query
FROM lightning_db_locks blocked
JOIN lightning_db_locks blocking 
    ON blocked.locktype = blocking.locktype
    AND blocked.pid != blocking.pid
WHERE NOT blocked.granted;
```

#### 2. Resolution

```bash
# Kill specific transaction
lightning_db transaction kill --pid=12345

# Adjust deadlock timeout
lightning_db config set deadlock_timeout=5s --reload

# Change isolation level
lightning_db config set default_isolation_level=read_committed --reload
```

### Symptom: Long-Running Transactions

```sql
-- Find long transactions
SELECT 
    pid,
    username,
    transaction_start,
    NOW() - transaction_start as duration,
    state,
    query
FROM lightning_db_stat_activity
WHERE transaction_start < NOW() - INTERVAL '10 minutes'
ORDER BY transaction_start;

-- Transaction age distribution
SELECT 
    CASE 
        WHEN age < INTERVAL '1 minute' THEN '< 1min'
        WHEN age < INTERVAL '5 minutes' THEN '1-5min'
        WHEN age < INTERVAL '30 minutes' THEN '5-30min'
        ELSE '> 30min'
    END as age_bucket,
    COUNT(*) as count
FROM (
    SELECT NOW() - transaction_start as age
    FROM lightning_db_stat_activity
    WHERE transaction_start IS NOT NULL
) t
GROUP BY age_bucket;
```

---

## Diagnostic Tools

### Built-in Diagnostics

```bash
# Comprehensive diagnostics
lightning_db diagnose --full --output=diagnostics.tar.gz

# Performance snapshot
lightning_db perf snapshot --duration=300 --output=perf_report.html

# Query profiling
lightning_db profile --query="SELECT ..." --detailed

# Lock analysis
lightning_db locks analyze --visualize --output=locks.svg
```

### Custom Diagnostic Scripts

```python
#!/usr/bin/env python3
# db_diagnostics.py

import subprocess
import json
import datetime

class LightningDBDiagnostics:
    def __init__(self, connection_string):
        self.conn_str = connection_string
    
    def collect_metrics(self):
        """Collect comprehensive metrics"""
        metrics = {
            'timestamp': datetime.datetime.now().isoformat(),
            'health': self.check_health(),
            'performance': self.check_performance(),
            'resources': self.check_resources(),
            'errors': self.check_errors()
        }
        return metrics
    
    def check_health(self):
        """Basic health checks"""
        cmd = f"lightning_db --connection='{self.conn_str}' ping"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        return {
            'responsive': result.returncode == 0,
            'response_time_ms': self.measure_response_time()
        }
    
    def check_performance(self):
        """Performance metrics"""
        return {
            'qps': self.get_qps(),
            'latency_p95': self.get_latency_p95(),
            'cache_hit_rate': self.get_cache_hit_rate()
        }
    
    def generate_report(self, metrics):
        """Generate diagnostic report"""
        report = f"""
Lightning DB Diagnostic Report
Generated: {metrics['timestamp']}

Health Status: {'OK' if metrics['health']['responsive'] else 'CRITICAL'}
Response Time: {metrics['health']['response_time_ms']}ms

Performance:
- QPS: {metrics['performance']['qps']}
- P95 Latency: {metrics['performance']['latency_p95']}ms
- Cache Hit Rate: {metrics['performance']['cache_hit_rate']:.2%}

Recommendations:
{self.generate_recommendations(metrics)}
        """
        return report
```

### Trace Analysis

```bash
# Enable tracing for specific query
lightning_db trace enable --query-pattern="SELECT.*FROM orders" --level=debug

# Analyze trace
lightning_db trace analyze --input=trace.log --output=trace_analysis.html

# Performance trace
perf record -g -p $(pgrep lightning_db) sleep 30
perf report
```

---

## Root Cause Analysis

### RCA Template

```markdown
## Incident Post-Mortem

**Incident ID**: INC-2024-001
**Date**: 2024-01-15
**Duration**: 45 minutes
**Impact**: 20% of queries failed

### Timeline
- 14:00 - Alert triggered for high error rate
- 14:05 - On-call engineer acknowledged
- 14:10 - Identified connection pool exhaustion
- 14:20 - Increased connection limit
- 14:30 - Added additional read replicas
- 14:45 - Service fully recovered

### Root Cause
Sudden spike in traffic due to marketing campaign exceeded connection pool capacity.

### Contributing Factors
1. Connection pool sized for normal traffic
2. No auto-scaling enabled
3. Monitoring threshold too high

### Action Items
- [ ] Implement connection pool auto-scaling
- [ ] Lower alerting threshold to 70%
- [ ] Create runbook for connection issues
- [ ] Add circuit breaker for traffic spikes
```

### Common Root Causes

| Symptom | Common Root Causes | Prevention |
|---------|-------------------|------------|
| Slow queries | Missing indexes, outdated statistics | Regular index analysis |
| High CPU | Inefficient queries, lock contention | Query optimization |
| Memory issues | Memory leaks, large result sets | Memory limits |
| Connection issues | Pool exhaustion, network issues | Connection pooling |
| Replication lag | Long transactions, network bandwidth | Transaction limits |
| Disk full | Log retention, temporary files | Automated cleanup |

---

## Troubleshooting Flowcharts

### Performance Issue Flowchart

```
Start
  │
  ▼
Is DB responding? ──No──→ Check process/connectivity
  │Yes
  ▼
Check CPU usage ──>70%──→ Identify heavy queries → Kill/Optimize
  │<70%
  ▼
Check Memory ──>85%──→ Clear caches/Increase memory
  │<85%
  ▼
Check I/O wait ──High──→ Check disk/Optimize I/O
  │Low
  ▼
Check locks ──Found──→ Resolve blocking queries
  │None
  ▼
Check network ──Issues──→ Fix network problems
  │OK
  ▼
Collect diagnostics → Escalate
```

---

## Emergency Procedures

### Database Hang

```bash
#!/bin/bash
# emergency_unhang.sh

echo "EMERGENCY: Database hang recovery"

# 1. Generate stack traces
kill -USR1 $(pgrep lightning_db)
sleep 2

# 2. Collect diagnostics
lightning_db diagnose --emergency --output=/tmp/hang_diag.tar.gz

# 3. Try graceful restart
systemctl reload lightning-db

# 4. If still hung, force restart
if ! lightning_db ping --timeout=10; then
    echo "Forcing restart..."
    systemctl stop lightning-db
    sleep 5
    pkill -9 lightning_db
    systemctl start lightning-db
fi
```

### Data Recovery

```bash
#!/bin/bash
# emergency_recovery.sh

# 1. Stop database
systemctl stop lightning-db

# 2. Backup current state
tar -czf /backup/emergency/corrupted_$(date +%Y%m%d_%H%M%S).tar.gz /var/lib/lightning_db

# 3. Run recovery
lightning_db recover \
    --data=/var/lib/lightning_db \
    --wal=/var/lib/lightning_db/wal \
    --aggressive \
    --output-report=/tmp/recovery_report.txt

# 4. Verify and start
if lightning_db verify --quick; then
    systemctl start lightning-db
else
    echo "Recovery failed - restore from backup required"
fi
```

---

## Prevention Best Practices

1. **Monitoring**
   - Set up comprehensive monitoring
   - Create meaningful alerts
   - Regular health checks

2. **Maintenance**
   - Regular vacuuming
   - Statistics updates
   - Index maintenance

3. **Capacity Planning**
   - Monitor growth trends
   - Plan for peaks
   - Regular load testing

4. **Documentation**
   - Keep runbooks updated
   - Document all changes
   - Regular drills

5. **Automation**
   - Automated diagnostics
   - Self-healing scripts
   - Automated failover

---

**Remember**: When troubleshooting production issues, always:
1. Preserve evidence (logs, metrics)
2. Communicate status regularly
3. Document actions taken
4. Follow up with RCA
5. Implement preventive measures