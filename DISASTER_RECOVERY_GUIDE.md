# Lightning DB Disaster Recovery Guide

## Overview

This guide provides comprehensive procedures for disaster recovery (DR) scenarios with Lightning DB. It covers prevention, detection, response, and recovery procedures for various failure scenarios.

**Critical Contact Information**:
- **24/7 Support**: _______________
- **Escalation Manager**: _______________
- **Database Team Lead**: _______________
- **Last Updated**: _______________

---

## Table of Contents

1. [DR Strategy Overview](#dr-strategy-overview)
2. [Backup and Recovery Procedures](#backup-and-recovery-procedures)
3. [Failure Scenarios and Recovery](#failure-scenarios-and-recovery)
4. [Data Corruption Recovery](#data-corruption-recovery)
5. [Point-in-Time Recovery](#point-in-time-recovery)
6. [Distributed System Recovery](#distributed-system-recovery)
7. [Emergency Procedures](#emergency-procedures)
8. [Recovery Testing](#recovery-testing)
9. [Post-Recovery Validation](#post-recovery-validation)

---

## DR Strategy Overview

### Recovery Objectives

| Metric | Target | Critical | Standard |
|--------|--------|----------|----------|
| **RTO** (Recovery Time Objective) | Time to restore service | < 15 min | < 1 hour |
| **RPO** (Recovery Point Objective) | Maximum data loss | < 1 min | < 15 min |
| **MTTR** (Mean Time to Repair) | Average recovery time | < 30 min | < 2 hours |

### DR Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Primary   │────▶│   Standby   │────▶│   Archive   │
│  Database   │     │  Database   │     │   Storage   │
└─────────────┘     └─────────────┘     └─────────────┘
       │                    │                    │
       ▼                    ▼                    ▼
  WAL Shipping      Async Replication      Cold Backup
```

---

## Backup and Recovery Procedures

### Backup Strategy

#### 1. Continuous WAL Archiving

```bash
# Enable WAL archiving
cat >> lightning_db.conf << EOF
wal_archive_enabled = true
wal_archive_command = 'cp %p /backup/wal/%f'
wal_archive_timeout = 60  # Archive every 60 seconds
EOF

# Monitor WAL archiving
lightning_db wal-status
```

#### 2. Scheduled Full Backups

```bash
#!/bin/bash
# Daily backup script

BACKUP_DIR="/backup/full/$(date +%Y%m%d)"
DB_PATH="/var/lib/lightning_db/data"

# Create hot backup
lightning_db backup create \
  --source=$DB_PATH \
  --dest=$BACKUP_DIR \
  --compression=zstd \
  --parallel=4 \
  --verify

# Verify backup integrity
lightning_db backup verify $BACKUP_DIR

# Upload to remote storage
aws s3 sync $BACKUP_DIR s3://backup-bucket/lightning-db/
```

#### 3. Incremental Backups

```bash
# Create incremental backup based on last full
lightning_db backup create-incremental \
  --base=/backup/full/20240115 \
  --dest=/backup/incr/$(date +%Y%m%d_%H%M) \
  --wal-dir=/backup/wal/
```

### Recovery Procedures

#### Standard Recovery

```bash
# 1. Stop the database
systemctl stop lightning-db

# 2. Backup corrupted data (just in case)
cp -r /var/lib/lightning_db/data /tmp/corrupted_backup

# 3. Restore from backup
lightning_db restore \
  --backup=/backup/full/20240115 \
  --dest=/var/lib/lightning_db/data \
  --verify-checksums

# 4. Apply WAL logs
lightning_db wal-replay \
  --data=/var/lib/lightning_db/data \
  --wal-dir=/backup/wal/ \
  --stop-at="2024-01-15 14:30:00"

# 5. Verify integrity
lightning_db verify --full /var/lib/lightning_db/data

# 6. Start database
systemctl start lightning-db
```

---

## Failure Scenarios and Recovery

### Scenario 1: Complete Server Failure

**Symptoms**: Server unresponsive, hardware failure, total loss

**Recovery Steps**:

```bash
# On replacement server

# 1. Install Lightning DB
curl -sSL https://install.lightning-db.com | sh

# 2. Restore configuration
scp backup-server:/backup/config/lightning_db.conf /etc/

# 3. Restore latest backup
lightning_db restore \
  --backup=s3://backup-bucket/lightning-db/latest \
  --dest=/var/lib/lightning_db/data

# 4. Apply recent WAL files
lightning_db wal-replay \
  --wal-source=s3://backup-bucket/wal/ \
  --after-timestamp="$(date -d '1 hour ago' --iso-8601)"

# 5. Update DNS/Load balancer
# Point applications to new server

# 6. Verify operations
lightning_db health-check
```

**Expected Recovery Time**: 30-45 minutes

### Scenario 2: Disk Failure

**Symptoms**: I/O errors, degraded RAID, disk full

**Recovery Steps**:

```bash
# 1. Identify failed disk
dmesg | grep -i error
smartctl -a /dev/sdb

# 2. If RAID degraded but operational
# Add hot spare
mdadm --add /dev/md0 /dev/sdc

# 3. If data loss occurred
# Switch to standby
lightning_db failover --to-standby

# 4. Rebuild primary from standby
lightning_db rebuild-primary \
  --source=standby.example.com \
  --parallel=8
```

### Scenario 3: Accidental Data Deletion

**Symptoms**: Critical data missing, application errors

**Recovery Steps**:

```bash
# 1. Identify deletion time
# Check application logs
grep -i "delete" /var/log/app.log | tail -100

# 2. Create point-in-time recovery
lightning_db pitr \
  --backup=/backup/full/latest \
  --wal-dir=/backup/wal/ \
  --target-time="2024-01-15 13:45:00" \
  --dest=/tmp/recovery

# 3. Extract deleted data
lightning_db export \
  --source=/tmp/recovery \
  --query="SELECT * FROM critical_table" \
  --output=/tmp/deleted_data.sql

# 4. Restore deleted data to production
lightning_db import \
  --dest=/var/lib/lightning_db/data \
  --input=/tmp/deleted_data.sql \
  --conflict=ignore
```

### Scenario 4: Ransomware Attack

**Symptoms**: Encrypted files, ransom notes, unusual file extensions

**Recovery Steps**:

```bash
# 1. IMMEDIATELY isolate affected systems
iptables -A INPUT -j DROP
iptables -A OUTPUT -j DROP

# 2. Assess damage
find /var/lib/lightning_db -name "*.encrypted" -o -name "*.locked"

# 3. Check backup integrity (on isolated system)
lightning_db backup verify s3://backup-bucket/lightning-db/

# 4. Perform clean recovery
# Boot from clean media
# Reinstall OS
# Restore from known-good backup pre-dating attack

# 5. Restore database
lightning_db restore \
  --backup=s3://backup-bucket/lightning-db/20240110 \
  --verify-signatures \
  --dest=/var/lib/lightning_db/data

# 6. Apply WAL up to attack time
lightning_db wal-replay \
  --stop-before="2024-01-15 09:00:00"
```

---

## Data Corruption Recovery

### Detecting Corruption

```bash
# Run comprehensive integrity check
lightning_db verify \
  --mode=full \
  --check-checksums \
  --check-structure \
  --check-consistency \
  --output=corruption_report.json

# Analyze report
cat corruption_report.json | jq '.errors[] | select(.severity == "critical")'
```

### Recovery Strategies

#### Level 1: Page-Level Corruption

```bash
# Attempt automatic repair
lightning_db repair \
  --corruption-report=corruption_report.json \
  --mode=aggressive \
  --backup-first

# If repair fails, restore specific pages
lightning_db page-restore \
  --backup=/backup/full/latest \
  --pages="1234,5678,9012" \
  --verify
```

#### Level 2: Index Corruption

```bash
# Rebuild corrupted indexes
lightning_db rebuild-index \
  --index=btree_main \
  --parallel=4 \
  --verify

# Verify index integrity
lightning_db verify-index --all
```

#### Level 3: Transaction Log Corruption

```bash
# Truncate corrupted WAL
lightning_db wal-truncate \
  --after-lsn=0x1234567890 \
  --backup-corrupted

# Reset to last checkpoint
lightning_db reset-to-checkpoint \
  --checkpoint=latest \
  --force
```

---

## Point-in-Time Recovery (PITR)

### Setup PITR

```bash
# Configure continuous archiving
cat >> lightning_db.conf << EOF
# PITR Configuration
pitr_enabled = true
wal_level = archive
archive_mode = on
archive_retention = 7d
checkpoint_interval = 15m
EOF
```

### Perform PITR

```bash
#!/bin/bash
# PITR to specific timestamp

TARGET_TIME="2024-01-15 14:30:00"
RECOVERY_DIR="/tmp/pitr_recovery"

# 1. Find base backup before target time
BASE_BACKUP=$(lightning_db backup list \
  --before="$TARGET_TIME" \
  --limit=1 \
  --format=json | jq -r '.backups[0].path')

echo "Using base backup: $BASE_BACKUP"

# 2. Restore base backup
lightning_db restore \
  --backup=$BASE_BACKUP \
  --dest=$RECOVERY_DIR \
  --no-start

# 3. Create recovery configuration
cat > $RECOVERY_DIR/recovery.conf << EOF
recovery_target_time = '$TARGET_TIME'
recovery_target_action = promote
restore_command = 'cp /backup/wal/%f %p'
recovery_target_timeline = latest
EOF

# 4. Start recovery
lightning_db recover \
  --data=$RECOVERY_DIR \
  --config=$RECOVERY_DIR/recovery.conf

# 5. Verify recovery point
lightning_db verify-pitr \
  --data=$RECOVERY_DIR \
  --expected-time="$TARGET_TIME"
```

---

## Distributed System Recovery

### Multi-Node Failure Recovery

```bash
# Scenario: Multiple shard failures in distributed setup

# 1. Assess cluster state
lightning_db cluster status --detailed

# 2. Identify failed shards
FAILED_SHARDS=$(lightning_db cluster list-failed-shards)

# 3. For each failed shard
for shard in $FAILED_SHARDS; do
  echo "Recovering shard: $shard"
  
  # Try to recover from replica
  if lightning_db shard has-replica $shard; then
    lightning_db shard promote-replica $shard
  else
    # Restore from backup
    lightning_db shard restore \
      --shard=$shard \
      --backup=s3://backup/$shard/latest \
      --rebalance
  fi
done

# 4. Verify cluster health
lightning_db cluster verify --wait-healthy
```

### Split-Brain Recovery

```bash
# Detect split-brain
lightning_db cluster detect-split-brain

# If split-brain detected:
# 1. Identify authoritative partition
AUTH_PARTITION=$(lightning_db cluster find-authoritative)

# 2. Fence non-authoritative nodes
lightning_db cluster fence --except=$AUTH_PARTITION

# 3. Rebuild from authoritative
lightning_db cluster rebuild \
  --from=$AUTH_PARTITION \
  --force-reconcile
```

---

## Emergency Procedures

### Emergency Shutdown

```bash
#!/bin/bash
# emergency_shutdown.sh

echo "EMERGENCY SHUTDOWN INITIATED"

# 1. Stop accepting new connections
lightning_db connection-freeze

# 2. Wait for active transactions
timeout 30 lightning_db wait-for-transactions

# 3. Force checkpoint
lightning_db checkpoint --force

# 4. Sync to disk
sync && sync

# 5. Shutdown
lightning_db shutdown --immediate

# 6. Backup current state
tar -czf /backup/emergency/db_$(date +%Y%m%d_%H%M%S).tar.gz \
  /var/lib/lightning_db/data
```

### Data Salvage

When standard recovery fails:

```bash
#!/bin/bash
# salvage_data.sh

# 1. Use low-level tools to extract data
lightning_db salvage \
  --source=/corrupted/data \
  --dest=/salvaged/data \
  --ignore-errors \
  --best-effort

# 2. Export readable data
lightning_db export-raw \
  --source=/salvaged/data \
  --format=csv \
  --output=/recovery/salvaged_data/

# 3. Rebuild from salvaged data
lightning_db import-bulk \
  --source=/recovery/salvaged_data/ \
  --dest=/new/database \
  --create-schema
```

---

## Recovery Testing

### Monthly DR Drill

```bash
#!/bin/bash
# dr_drill.sh

echo "Starting DR Drill - $(date)"

# 1. Create test database
TEST_DB="/tmp/dr_test_$(date +%Y%m%d)"

# 2. Restore from backup
time lightning_db restore \
  --backup=/backup/full/latest \
  --dest=$TEST_DB

# 3. Apply WAL
time lightning_db wal-replay \
  --data=$TEST_DB \
  --wal-dir=/backup/wal/

# 4. Verify data integrity
lightning_db verify --full $TEST_DB

# 5. Run application tests
./run_app_tests.sh --db=$TEST_DB

# 6. Measure recovery metrics
echo "RTO: $(get_recovery_time)"
echo "RPO: $(get_data_loss_window)"

# 7. Cleanup
rm -rf $TEST_DB
```

### Automated Recovery Testing

```yaml
# .github/workflows/dr-test.yml
name: DR Testing

on:
  schedule:
    - cron: '0 2 * * 0'  # Weekly

jobs:
  disaster-recovery-test:
    runs-on: ubuntu-latest
    steps:
      - name: Simulate failures
        run: |
          ./tests/simulate_disk_failure.sh
          ./tests/simulate_corruption.sh
          ./tests/simulate_network_partition.sh
      
      - name: Test recovery procedures
        run: |
          ./tests/test_backup_restore.sh
          ./tests/test_pitr.sh
          ./tests/test_failover.sh
      
      - name: Validate recovery
        run: |
          ./tests/validate_data_integrity.sh
          ./tests/validate_performance.sh
```

---

## Post-Recovery Validation

### Validation Checklist

After any recovery procedure:

#### 1. Data Integrity
```bash
# Full integrity check
lightning_db verify \
  --mode=full \
  --report=post_recovery_report.json

# Compare with pre-failure state
lightning_db compare \
  --baseline=/backup/metadata/baseline.json \
  --current=post_recovery_report.json
```

#### 2. Application Testing
```bash
# Run application test suite
./run_integration_tests.sh

# Verify critical business functions
./verify_critical_paths.sh
```

#### 3. Performance Validation
```bash
# Run performance benchmarks
lightning_db benchmark \
  --workload=production \
  --duration=3600 \
  --compare-baseline
```

#### 4. Monitoring Verification
```bash
# Ensure all monitoring is functional
curl http://localhost:9090/metrics
check_prometheus_alerts.sh
verify_grafana_dashboards.sh
```

### Recovery Report Template

```markdown
## Recovery Report

**Incident ID**: INC-2024-001
**Date**: 2024-01-15
**Duration**: 45 minutes
**Data Loss**: None

### Timeline
- 14:00 - Failure detected
- 14:05 - Recovery initiated
- 14:30 - Data restored
- 14:45 - Service operational

### Actions Taken
1. Identified root cause as disk failure
2. Failed over to standby
3. Restored primary from backup
4. Validated data integrity

### Lessons Learned
- Need faster detection mechanism
- Backup verification should run daily
- Documentation was accurate

### Action Items
- [ ] Implement improved monitoring
- [ ] Automate failover process
- [ ] Update runbooks
```

---

## Appendix: Quick Reference

### Critical Commands

```bash
# Emergency stop
lightning_db shutdown --immediate

# Check status
lightning_db status --detailed

# Quick backup
lightning_db backup create --quick --compress

# Verify integrity
lightning_db verify --check-only

# Start in recovery mode
lightning_db start --recovery-mode

# Export critical data
lightning_db export --tables=critical --format=sql
```

### Recovery Decision Tree

```
Is database running?
├─ Yes: Check for corruption
│  ├─ Corrupted: Run repair tools
│  └─ Clean: Monitor closely
└─ No: Check for data files
   ├─ Files exist: Attempt normal start
   │  ├─ Fails: Check logs, try recovery mode
   │  └─ Success: Verify integrity
   └─ Files missing: Restore from backup
      ├─ Recent backup: Restore + WAL replay
      └─ Old backup: Restore + investigate data loss
```

---

**Remember**: In a disaster scenario, stay calm, follow procedures, and document everything. When in doubt, preserve data first, then focus on recovery.