# Lightning DB - Troubleshooting Guide v1.0

## Overview

This comprehensive troubleshooting guide covers all aspects of Lightning DB v1.0, including the new security and reliability features. It provides systematic approaches to diagnose and resolve issues with detailed error handling guidance.

**Troubleshooting Approach**: Systematic diagnosis → Root cause analysis → Solution with verification  
**Error Handling**: All errors include actionable guidance and recovery procedures  
**Support Levels**: Self-service → Community → Enterprise support escalation  

---

## Quick Diagnostic Tools

### Instant Health Check

```bash
#!/bin/bash
# Quick diagnostic script for immediate issues

echo "Lightning DB Quick Diagnostic v1.0"
echo "=================================="

# 1. Service status
echo "[1/6] Service Status:"
if systemctl is-active --quiet lightning-db 2>/dev/null; then
    echo "✅ Lightning DB service is running"
else
    echo "❌ Lightning DB service is NOT running"
    echo "   Try: systemctl start lightning-db"
fi

# 2. Basic connectivity
echo "[2/6] Connectivity:"
if lightning-cli ping --timeout 2s 2>/dev/null; then
    echo "✅ Database responding to commands"
else
    echo "❌ Database not responding"
    echo "   Check: lightning-cli status"
fi

# 3. Disk space
echo "[3/6] Disk Space:"
DISK_USAGE=$(df -h . | tail -1 | awk '{print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -lt 90 ]; then
    echo "✅ Disk space OK (${DISK_USAGE}% used)"
else
    echo "⚠️ Disk space critical (${DISK_USAGE}% used)"
    echo "   Free space immediately"
fi

# 4. Memory usage
echo "[4/6] Memory Usage:"
MEMORY_USAGE=$(free | grep Mem | awk '{printf "%.0f", $3/$2 * 100.0}')
if [ "$MEMORY_USAGE" -lt 90 ]; then
    echo "✅ Memory usage OK (${MEMORY_USAGE}%)"
else
    echo "⚠️ High memory usage (${MEMORY_USAGE}%)"
fi

# 5. Recent errors
echo "[5/6] Recent Errors:"
ERROR_COUNT=$(journalctl -u lightning-db --since "1 hour ago" | grep -c ERROR || echo 0)
if [ "$ERROR_COUNT" -eq 0 ]; then
    echo "✅ No recent errors"
else
    echo "⚠️ $ERROR_COUNT errors in the last hour"
    echo "   Review: journalctl -u lightning-db --since '1 hour ago' | grep ERROR"
fi

# 6. Data integrity
echo "[6/6] Data Integrity:"
if lightning-cli validate --quick 2>/dev/null | grep -q "PASSED"; then
    echo "✅ Data integrity OK"
else
    echo "❌ Data integrity issues detected"
    echo "   Run: lightning-cli validate --comprehensive"
fi

echo ""
echo "Quick diagnostic completed"
```

### Error Code Reference

Lightning DB v1.0 uses structured error codes for systematic troubleshooting:

| Error Code | Category | Severity | Description |
|------------|----------|----------|-------------|
| -1 to -10 | Core Operations | High | Basic database operations |
| -11 to -20 | Transaction | Medium | Transaction-related errors |
| -21 to -30 | Storage | High | Storage and I/O errors |
| -31 to -40 | Security | Critical | Security-related errors |
| -41 to -50 | Network | Medium | Network and connectivity |
| -51 to -60 | Configuration | Low | Configuration issues |
| -61 to -70 | Performance | Medium | Performance-related warnings |
| -71 to -83 | Recovery | High | Recovery and reliability errors |

---

## Common Issues & Solutions

### 1. Database Won't Start

#### Issue: Service fails to start after upgrade to v1.0

**Symptoms**:
```bash
$ systemctl start lightning-db
Job for lightning-db.service failed because the control process exited with error code.
```

**Diagnosis**:
```bash
# Check detailed error message
journalctl -u lightning-db --no-pager

# Check configuration validity
lightning-cli validate-config --config /etc/lightning-db/lightning_db.toml
```

**Common Causes & Solutions**:

##### A) Configuration Compatibility Issue
```bash
# Error: "Unknown configuration field 'old_setting'"
# Solution: Migrate configuration
lightning-migrate config --input /etc/lightning-db/lightning_db.toml --fix-deprecated

# Backup old config
cp /etc/lightning-db/lightning_db.toml /etc/lightning-db/lightning_db.toml.v0.backup

# Apply fixed config  
lightning-migrate config --input /etc/lightning-db/lightning_db.toml --output /tmp/fixed_config.toml
sudo cp /tmp/fixed_config.toml /etc/lightning-db/lightning_db.toml
```

##### B) Permission Issues
```bash
# Error: "Permission denied accessing database directory"
# Solution: Fix permissions
sudo chown -R lightning-db:lightning-db /var/lib/lightning-db
sudo chmod -R 755 /var/lib/lightning-db
```

##### C) Port Already in Use
```bash
# Error: "Address already in use (port 8080)"
# Check what's using the port
sudo netstat -tlnp | grep :8080

# Solution: Either stop conflicting service or change port
# Change port in configuration:
# [monitoring]
# health_check_port = 8081
```

##### D) Insufficient Resources
```bash
# Error: "Cannot allocate memory" or "Too many open files"
# Solution: Increase system limits
echo "lightning-db soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "lightning-db hard nofile 65536" | sudo tee -a /etc/security/limits.conf

# For memory:
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### 2. Performance Issues

#### Issue: Slow query performance after v1.0 upgrade

**Symptoms**:
```bash
# Queries that used to take 1ms now take 10ms
# High CPU usage during normal operations
```

**Diagnosis**:
```bash
# Check performance metrics
lightning-cli metrics --performance

# Enable performance profiling
lightning-cli profile start --duration 60s
# ... run problematic queries ...
lightning-cli profile stop --output /tmp/profile.json

# Check for resource contention
lightning-cli diagnostic --resource-contention
```

**Common Causes & Solutions**:

##### A) Integrity Validation Overhead
```bash
# Problem: Paranoid validation level causing slowdown
# Check current level
lightning-cli config get security.integrity_validation_level

# Temporary fix: Reduce validation level
lightning-cli config set security.integrity_validation_level comprehensive

# Permanent fix: Update configuration file
[security]
integrity_validation_level = "comprehensive"  # Instead of "paranoid"
```

##### B) Monitoring Overhead
```bash
# Problem: Extensive monitoring causing performance impact
# Check monitoring settings
lightning-cli config get monitoring

# Solution: Optimize monitoring frequency
[monitoring]
metrics_collection_interval = "10s"  # Instead of "1s"
health_check_interval = "30s"        # Instead of "5s"
```

##### C) Deadlock Prevention False Positives
```bash
# Problem: Aggressive deadlock prevention causing unnecessary waits
# Check deadlock statistics
lightning-cli statistics deadlock-prevention

# Solution: Tune deadlock detection
[reliability]
deadlock_detection_sensitivity = "medium"  # Instead of "high"
```

### 3. Security-Related Issues

#### Issue: Security violation alerts after upgrade

**Symptoms**:
```bash
# Logs show: "SecurityViolation: Unauthorized access attempt"
# Application receives security-related errors
```

**Diagnosis**:
```bash
# Check security events
lightning-cli security events --recent

# Review security configuration
lightning-cli config get security

# Check audit log
lightning-cli audit-log --since "1 hour ago"
```

**Solutions**:

##### A) Authentication Configuration
```bash
# Problem: New security features require explicit authentication
# Check if authentication is enabled
lightning-cli config get security.authentication_enabled

# Solution: Configure authentication or disable for development
[security]
authentication_enabled = false  # For development only
# OR properly configure authentication for production
```

##### B) Data Validation Strictness
```bash
# Problem: Stricter input validation rejecting previously accepted data
# Solution: Update application to handle new validation rules
# Check validation errors in detail:
lightning-cli validate --verbose --show-rejected
```

### 4. Recovery & Reliability Issues

#### Issue: Automatic recovery failing

**Symptoms**:
```bash
# Error: "RecoveryFailure: Automatic recovery stage failed"
# Database stuck in recovery mode
```

**Diagnosis**:
```bash
# Check recovery status
lightning-cli recovery status

# Review recovery logs
lightning-cli recovery logs --detailed

# Check recovery stage progress
lightning-cli recovery progress
```

**Solutions**:

##### A) Resource Constraints During Recovery
```bash
# Problem: Recovery requires more resources than available
# Error: "InsufficientResources: memory required: 2GB, available: 1GB"

# Solution: Increase available resources
# 1. Free memory temporarily
sudo systemctl stop non-essential-service

# 2. Or reduce recovery memory requirements
lightning-cli config set recovery.memory_limit "1GB"

# 3. Restart recovery
lightning-cli recovery restart
```

##### B) Corrupted Recovery State
```bash
# Problem: Recovery state itself is corrupted
# Solution: Reset recovery and start clean
lightning-cli recovery reset --force
lightning-cli recovery start --from-wal
```

##### C) Manual Recovery Required
```bash
# Problem: Automatic recovery cannot proceed
# Solution: Switch to manual recovery mode
lightning-cli recovery manual-mode

# Follow manual recovery steps
lightning-cli recovery manual-steps
```

### 5. Data Corruption Issues

#### Issue: Data integrity validation failures

**Symptoms**:
```bash
# Error: "DataCorruption: Checksum mismatch in page 12345"
# Queries returning unexpected results
```

**Diagnosis**:
```bash
# Comprehensive integrity check
lightning-cli validate --comprehensive --report /tmp/integrity_report.json

# Check corruption scope
lightning-cli corruption scan --detailed

# Analyze corruption patterns
lightning-cli corruption analyze --report /tmp/corruption_analysis.json
```

**Solutions**:

##### A) Minor Corruption (Repairable)
```bash
# Check if corruption is repairable
lightning-cli corruption repair --dry-run

# If repairable, execute repair
lightning-cli corruption repair --confirm

# Verify repair success
lightning-cli validate --comprehensive
```

##### B) Major Corruption (Backup Required)
```bash
# Stop database immediately
systemctl stop lightning-db

# Restore from backup
LATEST_BACKUP=$(ls -t /backup/lightning_db_* | head -1)
/backup/restore-from-backup.sh "$LATEST_BACKUP"

# Verify restored data
systemctl start lightning-db
lightning-cli validate --comprehensive
```

##### C) Systematic Corruption (Investigation Required)
```bash
# Corruption affecting multiple components
# 1. Create forensic backup
cp -r /var/lib/lightning-db /forensic/lightning_db_$(date +%Y%m%d_%H%M%S)

# 2. Analyze corruption patterns  
lightning-cli forensic analyze --input /forensic/lightning_db_*

# 3. Check for hardware issues
sudo smartctl -a /dev/sda
sudo memtest86+ # or equivalent memory test

# 4. Contact support with forensic data
```

---

## Advanced Troubleshooting

### Memory-Related Issues

#### Issue: Out of Memory (OOM) errors

**Diagnosis**:
```bash
# Check memory usage patterns
lightning-cli memory usage --history

# Check for memory leaks
lightning-cli memory leaks --scan

# Monitor memory over time
lightning-cli monitor memory --duration 300s --interval 5s
```

**Solutions**:

##### Memory Configuration Optimization
```toml
[database]
# Reduce cache size if memory is limited
cache_size = "512MB"  # Instead of "2GB"

# Enable memory management features
[performance]
memory_pressure_handling = true
garbage_collection_enabled = true
memory_limit_enforcement = true

[reliability]
# Enable memory monitoring
memory_monitoring_enabled = true
memory_alert_threshold = 80  # percent
```

### Network-Related Issues

#### Issue: Connection timeouts or network errors

**Diagnosis**:
```bash
# Test network connectivity
lightning-cli network test --target-host database-host

# Check network statistics
lightning-cli network statistics

# Monitor connection pool
lightning-cli connections status
```

**Solutions**:

```toml
[network]
# Increase timeouts for slow networks
connection_timeout = "30s"  # Instead of "5s"
read_timeout = "60s"        # Instead of "10s"

# Configure connection pool
max_connections = 100
connection_pool_size = 20
keep_alive_interval = "30s"

# Enable connection recovery
auto_reconnect = true
reconnect_backoff = "exponential"
```

### Concurrency Issues

#### Issue: Deadlock detection false positives

**Diagnosis**:
```bash
# Check deadlock statistics
lightning-cli deadlock statistics

# Monitor lock contention
lightning-cli locks monitor --duration 60s

# Analyze lock patterns
lightning-cli locks analyze --report /tmp/lock_analysis.json
```

**Solutions**:

```toml
[reliability]
# Tune deadlock detection
deadlock_detection_enabled = true
deadlock_detection_sensitivity = "medium"  # high|medium|low
deadlock_timeout = "30s"

# Configure lock behavior
lock_timeout = "10s"
lock_retry_count = 3
lock_retry_backoff = "exponential"
```

---

## Error Code Deep Dive

### Critical Error Codes (-71 to -83)

These are the new recovery and reliability error codes in v1.0:

#### -70: RecoveryImpossible
```bash
# Problem: Recovery cannot proceed due to fundamental issues
# Example: "Recovery impossible: WAL files missing, no backup available"

# Investigation:
lightning-cli recovery diagnose --error-code -70

# Solutions:
# 1. Check for backup files
ls -la /backup/lightning_db_*

# 2. Check for partial recovery options
lightning-cli recovery partial --assess

# 3. Last resort: Initialize new database (DATA LOSS)
lightning-cli recovery initialize-new --confirm-data-loss
```

#### -71: WalCorrupted
```bash
# Problem: Write-Ahead Log is corrupted
# Investigation:
lightning-cli wal diagnose --all-segments

# Solutions:
# 1. Attempt WAL repair
lightning-cli wal repair --segment-id <ID>

# 2. Rollback to last known good state
lightning-cli recovery rollback --to-last-checkpoint

# 3. Restore from backup
lightning-cli recovery restore --from-backup
```

#### -72: PartialRecoveryFailure
```bash
# Problem: Recovery partially completed but failed at specific stage
# Investigation:
lightning-cli recovery status --detailed

# Shows: "Completed: [Stage1, Stage2], Failed: Stage3, Rollback: Available"

# Solutions:
# 1. Retry failed stage
lightning-cli recovery retry --stage Stage3

# 2. Rollback completed stages
lightning-cli recovery rollback --stages "Stage1,Stage2"

# 3. Skip failed stage (if safe)
lightning-cli recovery skip --stage Stage3 --force
```

### Security Error Codes (-31 to -40)

#### -31: SecurityViolation
```bash
# Investigation:
lightning-cli security investigate --error-code -31

# Common causes:
# 1. Invalid authentication tokens
# 2. Unauthorized access attempts  
# 3. Malformed security headers

# Solutions:
# 1. Update authentication configuration
# 2. Review access control policies
# 3. Check client security implementations
```

#### -32: IntegrityValidationFailed
```bash
# Problem: Data integrity check failed
# Investigation:
lightning-cli integrity check --failed-items-only

# Solutions:
# 1. Automatic repair (if safe)
lightning-cli integrity repair --auto

# 2. Manual verification and repair
lightning-cli integrity verify --manual-mode

# 3. Restore from backup if corruption is extensive
```

---

## Log Analysis

### Log Levels and Their Meanings

```bash
# Configure logging detail level based on troubleshooting needs
[monitoring]
log_level = "debug"  # trace|debug|info|warn|error

# Different components can have different log levels
[monitoring.components]
database_core = "info"
security = "debug"      # More detail for security issues
reliability = "warn"    # Only warnings and errors
performance = "info"
```

### Critical Log Patterns

#### Security Events
```bash
# Look for these patterns in logs:
grep "SECURITY" /var/log/lightning-db/database.log

# Example patterns:
# "SECURITY: Authentication failure from IP 192.168.1.100"  
# "SECURITY: Integrity violation detected in page 12345"
# "SECURITY: Unauthorized access attempt to admin endpoint"
```

#### Reliability Events
```bash
# Reliability-related log patterns:
grep -E "(RECOVERY|RELIABILITY|DEADLOCK|CORRUPTION)" /var/log/lightning-db/database.log

# Example patterns:
# "RECOVERY: Stage 3 of 12 completed (IndexReconstruction)"
# "RELIABILITY: Circuit breaker opened for service 'storage'"  
# "DEADLOCK: Prevention system activated (resource: page_locks)"
# "CORRUPTION: Data integrity violation in component 'btree'"
```

#### Performance Events
```bash
# Performance-related patterns:
grep -E "(PERF|SLOW|TIMEOUT)" /var/log/lightning-db/database.log

# Example patterns:
# "PERF: Query exceeded threshold: 1.2s (limit: 1.0s)"
# "SLOW: Operation 'batch_insert' took 5.3s"
# "TIMEOUT: Health check timeout after 30s"
```

---

## Monitoring & Alerting

### Setting Up Proactive Monitoring

```yaml
# prometheus-alerts.yml - Lightning DB v1.0 alerts
groups:
  - name: lightning_db_v1_alerts
    rules:
      # Critical alerts
      - alert: LightningDB_Down
        expr: lightning_db_up == 0
        for: 30s
        annotations:
          summary: "Lightning DB is down"
          description: "Database has been down for more than 30 seconds"
          
      - alert: LightningDB_DataCorruption
        expr: lightning_db_corruption_events_total > 0
        for: 0s
        annotations:
          summary: "Data corruption detected"
          description: "Immediate investigation required"
          
      # New v1.0 specific alerts
      - alert: LightningDB_SecurityViolation
        expr: increase(lightning_db_security_violations_total[5m]) > 0
        annotations:
          summary: "Security violation detected"
          
      - alert: LightningDB_RecoveryFailure
        expr: lightning_db_recovery_failures_total > 0
        annotations:
          summary: "Recovery operation failed"
          
      - alert: LightningDB_CircuitBreakerOpen
        expr: lightning_db_circuit_breaker_state == 1
        for: 1m
        annotations:
          summary: "Circuit breaker is open"
```

### Health Check Automation

```bash
#!/bin/bash
# /usr/local/bin/lightning-db-health-check.sh
# Automated health monitoring script

HEALTH_URL="http://localhost:8080/health"
ALERT_EMAIL="admin@company.com"

# Get health status
HEALTH_RESPONSE=$(curl -s "$HEALTH_URL" || echo '{"status": "unreachable"}')
HEALTH_STATUS=$(echo "$HEALTH_RESPONSE" | jq -r '.status')

case "$HEALTH_STATUS" in
    "healthy")
        echo "$(date): Lightning DB is healthy"
        ;;
    "degraded")
        echo "$(date): Lightning DB is degraded - $(echo "$HEALTH_RESPONSE" | jq -r '.details')"
        # Send warning notification
        ;;
    "unhealthy"|"unreachable")
        echo "$(date): Lightning DB is unhealthy or unreachable"
        # Send critical alert
        echo "Lightning DB health check failed at $(date)" | \
            mail -s "CRITICAL: Lightning DB Down" "$ALERT_EMAIL"
        # Attempt automatic recovery
        systemctl restart lightning-db
        ;;
esac
```

---

## Performance Debugging

### Identifying Performance Bottlenecks

```bash
# 1. Enable detailed performance metrics
lightning-cli config set monitoring.detailed_performance_metrics true

# 2. Start performance profiling
lightning-cli profile start --all-components

# 3. Run workload that exhibits performance issues
# ... your application workload ...

# 4. Stop profiling and analyze
lightning-cli profile stop --analyze --output /tmp/perf_analysis.json

# 5. Generate performance report
lightning-cli profile report --format html --output /tmp/perf_report.html
```

### Performance Tuning Recommendations

```toml
# lightning_db_performance.toml - Optimized for performance
[database]
# Larger cache for performance (adjust based on available RAM)
cache_size = "4GB"
# Optimize for write performance
write_batch_size = 5000
# Reduce validation overhead for performance-critical deployments
[security]
integrity_validation_level = "standard"  # Instead of "paranoid"

[performance]
# Enable performance optimizations
simd_optimizations = true
cache_prefetch = true  
batch_size_optimization = "large"

# Tune for specific workloads
[workload_optimization]
read_heavy = false      # Set true for read-heavy workloads
write_heavy = true      # Set true for write-heavy workloads
mixed_workload = false  # Set true for balanced workloads
```

---

## Emergency Procedures

### Database Emergency Shutdown

```bash
#!/bin/bash
# Emergency shutdown procedure for Lightning DB

echo "EMERGENCY: Initiating Lightning DB emergency shutdown"

# 1. Immediate stop (preserves data integrity)
lightning-cli shutdown emergency --wait-for-completion

# 2. If that fails, force stop service
systemctl stop lightning-db --force

# 3. If service won't stop, kill processes
pkill -TERM lightning-db
sleep 10
pkill -KILL lightning-db

# 4. Protect data directory
chmod 000 /var/lib/lightning-db
echo "Data directory protected from further access"

# 5. Create incident report
cat > "/tmp/lightning_db_emergency_$(date +%Y%m%d_%H%M%S).txt" << EOF
Lightning DB Emergency Shutdown Report
=====================================
Date: $(date)
Reason: Manual emergency shutdown
System Status: Database stopped, data protected
Next Steps: Investigation required before restart
EOF

echo "Emergency shutdown completed. Investigation required before restart."
```

### Data Recovery Emergency

```bash
#!/bin/bash
# Emergency data recovery procedure

echo "EMERGENCY: Lightning DB data recovery procedure"

# 1. Assess damage
lightning-cli recovery assess --emergency-mode

# 2. Find latest good backup
BACKUP_DIR=$(ls -dt /backup/lightning_db_* | head -1)
echo "Latest backup: $BACKUP_DIR"

# 3. Estimate data loss
lightning-cli recovery estimate-loss --since-backup "$BACKUP_DIR"

# 4. Decision point
echo "Data loss estimation completed. Review /tmp/recovery_assessment.txt"
echo "Continue with recovery? (yes/no)"
read -r CONTINUE

if [ "$CONTINUE" = "yes" ]; then
    # 5. Execute recovery
    lightning-cli recovery execute --from-backup "$BACKUP_DIR" --emergency-mode
    
    # 6. Validate recovery
    lightning-cli validate --comprehensive --emergency-mode
    
    echo "Emergency recovery completed. Full validation recommended."
else
    echo "Recovery cancelled. System remains in safe mode."
fi
```

---

## Getting Help

### Self-Service Resources

1. **Built-in Help**:
   ```bash
   lightning-cli help troubleshooting
   lightning-cli diagnostic --guided
   lightning-cli recovery --help-wizard
   ```

2. **Documentation**:
   - `/docs/troubleshooting/` - This guide
   - `/docs/error-codes/` - Complete error code reference
   - `/docs/monitoring/` - Monitoring and alerting setup
   - `/docs/recovery/` - Recovery procedures

3. **Diagnostic Tools**:
   ```bash
   # Generate comprehensive diagnostic report
   lightning-cli diagnostic complete --output /tmp/diagnostic_report.zip
   
   # This includes:
   # - System information
   # - Configuration analysis
   # - Recent logs
   # - Performance metrics
   # - Error analysis
   # - Recovery status
   ```

### Community Support

- **GitHub Issues**: For bug reports and feature requests
- **GitHub Discussions**: For troubleshooting help
- **Community Forum**: For general questions and best practices
- **Stack Overflow**: Tag questions with `lightning-db`

### Enterprise Support

For enterprise customers, priority support is available:

- **Severity 1 (Critical)**: Response within 2 hours
- **Severity 2 (High)**: Response within 8 hours  
- **Severity 3 (Medium)**: Response within 24 hours
- **Severity 4 (Low)**: Response within 5 business days

Contact: `enterprise-support@lightning-db.com`

---

## Conclusion

Lightning DB v1.0's enhanced error handling and reliability features make troubleshooting more systematic and effective. The comprehensive error codes, detailed logging, and built-in diagnostic tools provide the information needed to quickly identify and resolve issues.

**Remember the troubleshooting hierarchy**:
1. **Quick diagnostics** - Use built-in tools for immediate assessment
2. **Error code analysis** - Use structured error codes for systematic diagnosis  
3. **Log analysis** - Review logs for patterns and context
4. **Recovery procedures** - Follow established procedures for data protection
5. **Escalation** - Use community or enterprise support when needed

The investment in comprehensive troubleshooting capabilities in v1.0 pays dividends in reduced downtime and faster issue resolution.

---

**Troubleshooting Guide Version**: 1.0  
**Last Updated**: 2025-01-10  
**Compatibility**: Lightning DB v1.0+  
**Next Update**: Based on community feedback and new issue patterns