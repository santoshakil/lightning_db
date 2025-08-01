# Lightning DB Operations Runbook

## Overview

This runbook provides step-by-step procedures for routine operational tasks required to maintain Lightning DB in production. Each procedure includes verification steps, rollback procedures, and escalation criteria.

**Last Updated**: _______________  
**Review Schedule**: Monthly  
**Runbook Owner**: _______________  
**Approval**: _______________

---

## Table of Contents

1. [Daily Operations](#daily-operations)
2. [Weekly Operations](#weekly-operations)
3. [Monthly Operations](#monthly-operations)
4. [On-Demand Operations](#on-demand-operations)
5. [Emergency Procedures](#emergency-procedures)
6. [Automation Scripts](#automation-scripts)
7. [Operational Metrics](#operational-metrics)
8. [Handover Procedures](#handover-procedures)

---

## Daily Operations

### Morning Health Check (8:00 AM)

**Time Required**: 15-30 minutes  
**Priority**: P1 - Critical  
**Owner**: On-duty DBA

#### Procedure

```bash
#!/bin/bash
# daily_health_check.sh

echo "=== Lightning DB Daily Health Check ==="
echo "Date: $(date)"
echo "Operator: $USER"

# 1. Service Status
echo -e "\n[1/10] Checking service status..."
for instance in primary replica-1 replica-2; do
    echo -n "  $instance: "
    if ssh $instance 'systemctl is-active lightning-db' &>/dev/null; then
        echo "✓ Running"
    else
        echo "✗ DOWN - ESCALATE IMMEDIATELY"
        exit 1
    fi
done

# 2. Replication Status
echo -e "\n[2/10] Checking replication..."
lightning_db replication status --format=json | jq -r '.replicas[] | "\(.name): lag=\(.lag_seconds)s status=\(.status)"'

# 3. Backup Verification
echo -e "\n[3/10] Verifying last night's backup..."
last_backup=$(ls -t /backup/daily/ | head -1)
if [[ -z "$last_backup" ]]; then
    echo "✗ No backup found - INVESTIGATE"
else
    echo "✓ Latest backup: $last_backup"
    lightning_db backup verify "/backup/daily/$last_backup" --quick
fi

# 4. Disk Space Check
echo -e "\n[4/10] Checking disk space..."
df -h | grep -E "lightning|backup" | awk '$5+0 > 80 {print "⚠ WARNING: " $0}'

# 5. Error Log Review
echo -e "\n[5/10] Checking error logs..."
error_count=$(grep -c "ERROR\|FATAL\|PANIC" /var/log/lightning_db/error.log 2>/dev/null || echo 0)
if [[ $error_count -gt 0 ]]; then
    echo "⚠ Found $error_count errors in logs"
    tail -5 /var/log/lightning_db/error.log
else
    echo "✓ No errors in logs"
fi

# 6. Performance Metrics
echo -e "\n[6/10] Performance snapshot..."
lightning_db metrics snapshot --period=24h --format=summary

# 7. Connection Pool Status
echo -e "\n[7/10] Connection pool status..."
lightning_db connections summary

# 8. Cache Effectiveness
echo -e "\n[8/10] Cache statistics..."
lightning_db cache stats --format=summary

# 9. Query Performance
echo -e "\n[9/10] Slow query summary..."
lightning_db query stats --slow --limit=5 --period=24h

# 10. System Resources
echo -e "\n[10/10] System resource check..."
for instance in primary replica-1 replica-2; do
    echo "Instance: $instance"
    ssh $instance 'free -h | grep Mem; uptime'
done

echo -e "\n=== Health Check Complete ==="
```

#### Verification
- All checks should show green (✓)
- No critical errors in logs
- Replication lag < 10 seconds
- Disk usage < 80%

#### Escalation
- Any service down: Page on-call immediately
- Replication lag > 60s: Alert team lead
- Backup failure: Create P1 incident

### Transaction Log Maintenance (10:00 AM)

**Time Required**: 5-10 minutes  
**Priority**: P2 - High  
**Owner**: On-duty DBA

```bash
#!/bin/bash
# transaction_log_maintenance.sh

echo "Starting transaction log maintenance..."

# 1. Check WAL directory size
wal_size=$(du -sh /var/lib/lightning_db/wal | cut -f1)
echo "Current WAL size: $wal_size"

# 2. Archive old WAL files
lightning_db wal archive --older-than=24h --compress

# 3. Clean archived WALs older than retention period
find /archive/wal -name "*.wal.gz" -mtime +7 -delete

# 4. Verify WAL continuity
lightning_db wal verify --check-sequence

echo "Transaction log maintenance complete"
```

### Performance Baseline Update (2:00 PM)

**Time Required**: 10 minutes  
**Priority**: P3 - Medium  
**Owner**: Performance Engineer

```sql
-- Update performance baseline
INSERT INTO performance_baseline (
    timestamp,
    metric_name,
    metric_value,
    percentile
)
SELECT 
    NOW(),
    'daily_avg_qps',
    AVG(queries_per_second),
    NULL
FROM metrics_1min
WHERE timestamp > NOW() - INTERVAL '24 hours'
UNION ALL
SELECT 
    NOW(),
    'daily_p95_latency',
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms),
    95
FROM query_log
WHERE timestamp > NOW() - INTERVAL '24 hours';

-- Alert on degradation
SELECT 
    metric_name,
    today.metric_value as today_value,
    baseline.metric_value as baseline_value,
    ((today.metric_value - baseline.metric_value) / baseline.metric_value * 100) as pct_change
FROM performance_baseline today
JOIN performance_baseline baseline 
    ON today.metric_name = baseline.metric_name
    AND baseline.timestamp = NOW() - INTERVAL '7 days'
WHERE today.timestamp::date = CURRENT_DATE
AND ABS((today.metric_value - baseline.metric_value) / baseline.metric_value) > 0.20;
```

### End-of-Day Report (5:00 PM)

**Time Required**: 15 minutes  
**Priority**: P3 - Medium  
**Owner**: On-duty DBA

```python
#!/usr/bin/env python3
# generate_daily_report.py

import datetime
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def generate_daily_report():
    report_date = datetime.date.today()
    
    report = {
        'date': str(report_date),
        'health_status': check_overall_health(),
        'key_metrics': collect_key_metrics(),
        'incidents': get_incidents_today(),
        'maintenance_performed': get_maintenance_tasks(),
        'upcoming_tasks': get_upcoming_tasks()
    }
    
    html_report = format_html_report(report)
    send_report(html_report, f"Lightning DB Daily Report - {report_date}")

def check_overall_health():
    # Implementation
    return "GREEN"  # or "YELLOW", "RED"

def format_html_report(report):
    html = f"""
    <html>
    <body>
    <h2>Lightning DB Daily Operations Report</h2>
    <p>Date: {report['date']}</p>
    <p>Overall Health: <b style="color: {'green' if report['health_status'] == 'GREEN' else 'red'}">{report['health_status']}</b></p>
    
    <h3>Key Metrics</h3>
    <table border="1">
    <tr><th>Metric</th><th>Value</th><th>Trend</th></tr>
    {"".join(f"<tr><td>{k}</td><td>{v['value']}</td><td>{v['trend']}</td></tr>" for k,v in report['key_metrics'].items())}
    </table>
    
    <h3>Incidents Today</h3>
    {"<p>No incidents</p>" if not report['incidents'] else "<ul>" + "".join(f"<li>{inc}</li>" for inc in report['incidents']) + "</ul>"}
    
    <h3>Maintenance Performed</h3>
    <ul>
    {"".join(f"<li>{task}</li>" for task in report['maintenance_performed'])}
    </ul>
    
    <h3>Upcoming Tasks</h3>
    <ul>
    {"".join(f"<li>{task}</li>" for task in report['upcoming_tasks'])}
    </ul>
    </body>
    </html>
    """
    return html

if __name__ == "__main__":
    generate_daily_report()
```

---

## Weekly Operations

### Monday: Comprehensive Health Assessment

**Time Required**: 2 hours  
**Priority**: P2 - High  
**Owner**: Senior DBA

#### Full System Audit

```bash
#!/bin/bash
# weekly_system_audit.sh

echo "=== Weekly System Audit Starting ==="
audit_date=$(date +%Y%m%d)
audit_dir="/var/log/lightning_db/audits/$audit_date"
mkdir -p "$audit_dir"

# 1. Configuration Audit
echo "[1/8] Configuration audit..."
lightning_db config diff --baseline=/etc/lightning_db/baseline.conf > "$audit_dir/config_drift.txt"

# 2. Security Audit
echo "[2/8] Security audit..."
lightning_db security audit --full > "$audit_dir/security_audit.txt"

# 3. Performance Audit
echo "[3/8] Performance audit..."
lightning_db performance analyze --week > "$audit_dir/performance_audit.txt"

# 4. Index Analysis
echo "[4/8] Index analysis..."
lightning_db indexes analyze --unused --duplicates --missing > "$audit_dir/index_audit.txt"

# 5. Table Bloat Analysis
echo "[5/8] Bloat analysis..."
lightning_db bloat report --threshold=20 > "$audit_dir/bloat_report.txt"

# 6. Connection Analysis
echo "[6/8] Connection analysis..."
lightning_db connections analyze --week > "$audit_dir/connection_patterns.txt"

# 7. Query Pattern Analysis
echo "[7/8] Query patterns..."
lightning_db query patterns --week > "$audit_dir/query_patterns.txt"

# 8. Growth Projection
echo "[8/8] Growth analysis..."
lightning_db capacity project --months=3 > "$audit_dir/growth_projection.txt"

# Generate summary
cat > "$audit_dir/summary.txt" << EOF
Weekly Audit Summary - $audit_date

Critical Findings:
$(grep -h "CRITICAL\|ERROR" $audit_dir/*.txt | head -10)

Recommendations:
$(grep -h "RECOMMEND" $audit_dir/*.txt | head -10)

Action Items:
$(grep -h "ACTION:" $audit_dir/*.txt)
EOF

echo "Audit complete. Results in: $audit_dir"
```

### Tuesday: Performance Tuning

**Time Required**: 1-3 hours  
**Priority**: P2 - High  
**Owner**: Performance Engineer

```bash
#!/bin/bash
# weekly_performance_tuning.sh

echo "=== Weekly Performance Tuning ==="

# 1. Analyze slow queries from past week
echo "[1/5] Analyzing slow queries..."
lightning_db query analyze --slow --week --output=slow_queries.json

# 2. Update table statistics
echo "[2/5] Updating statistics..."
lightning_db stats update --all --sample-rate=default

# 3. Identify missing indexes
echo "[3/5] Checking for missing indexes..."
lightning_db indexes suggest --workload=recent --benefit-threshold=1000

# 4. Cache optimization
echo "[4/5] Optimizing cache..."
lightning_db cache optimize --auto-tune

# 5. Connection pool tuning
echo "[5/5] Tuning connection pools..."
lightning_db connections tune --based-on=peak-usage

# Apply recommended changes (with approval)
echo -e "\nProposed changes:"
cat tuning_recommendations.txt

read -p "Apply changes? (y/n) " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
    lightning_db config apply tuning_recommendations.txt --backup-current
fi
```

### Wednesday: Backup Validation

**Time Required**: 2-4 hours  
**Priority**: P1 - Critical  
**Owner**: Backup Administrator

```bash
#!/bin/bash
# weekly_backup_validation.sh

echo "=== Weekly Backup Validation ==="

# 1. Select random backup for full restore test
backup_to_test=$(ls -t /backup/daily/ | head -7 | sort -R | head -1)
echo "Testing backup: $backup_to_test"

# 2. Restore to test instance
test_instance="backup-validation-01"
echo "Restoring to test instance: $test_instance"

ssh $test_instance << 'EOF'
    # Stop test instance
    systemctl stop lightning-db
    
    # Clear data directory
    rm -rf /var/lib/lightning_db/data/*
    
    # Restore backup
    lightning_db restore \
        --backup=/backup/daily/$backup_to_test \
        --data=/var/lib/lightning_db/data \
        --verify
    
    # Start instance
    systemctl start lightning-db
    
    # Wait for startup
    sleep 30
EOF

# 3. Validate restored data
echo "Validating restored data..."
ssh $test_instance << 'EOF'
    # Check row counts
    lightning_db validate --row-counts --compare-with=production
    
    # Verify critical tables
    lightning_db query "SELECT COUNT(*) FROM critical_table_1"
    lightning_db query "SELECT MAX(created_at) FROM audit_log"
    
    # Run integrity checks
    lightning_db verify --full
EOF

# 4. Backup retention cleanup
echo "Cleaning old backups..."
find /backup/daily -mtime +30 -delete
find /backup/weekly -mtime +90 -delete
find /backup/monthly -mtime +365 -delete

echo "Backup validation complete"
```

### Thursday: Security Review

**Time Required**: 1-2 hours  
**Priority**: P2 - High  
**Owner**: Security Admin

```bash
#!/bin/bash
# weekly_security_review.sh

echo "=== Weekly Security Review ==="

# 1. User access review
echo "[1/6] Reviewing user access..."
lightning_db users audit --inactive --over-privileged

# 2. Failed login analysis
echo "[2/6] Analyzing failed logins..."
grep "authentication failed" /var/log/lightning_db/auth.log | \
    awk '{print $NF}' | sort | uniq -c | sort -rn | head -20

# 3. Privilege audit
echo "[3/6] Auditing privileges..."
lightning_db privileges audit --excessive --unused

# 4. SSL certificate check
echo "[4/6] Checking SSL certificates..."
for cert in /etc/lightning_db/certs/*.crt; do
    echo "Certificate: $cert"
    openssl x509 -in "$cert" -noout -dates
done

# 5. Firewall rules review
echo "[5/6] Reviewing firewall rules..."
iptables -L -n | grep -E "5432|lightning"

# 6. Security patch check
echo "[6/6] Checking for security updates..."
lightning_db version check-updates --security-only
```

### Friday: Maintenance Window Preparation

**Time Required**: 1 hour  
**Priority**: P2 - High  
**Owner**: Operations Lead

```bash
#!/bin/bash
# prepare_maintenance_window.sh

echo "=== Maintenance Window Preparation ==="

# 1. Review scheduled maintenance
echo "[1/5] Scheduled maintenance for this weekend:"
lightning_db maintenance list --upcoming

# 2. Pre-maintenance backup
echo "[2/5] Creating pre-maintenance backup..."
lightning_db backup create --name="pre_maintenance_$(date +%Y%m%d)" --compress

# 3. Generate rollback plan
echo "[3/5] Generating rollback plan..."
cat > /tmp/rollback_plan.txt << EOF
Rollback Plan - $(date)

1. Stop Lightning DB
   systemctl stop lightning-db

2. Restore from backup
   lightning_db restore --backup=pre_maintenance_$(date +%Y%m%d)

3. Restore configuration
   cp /etc/lightning_db/backup/*.conf /etc/lightning_db/

4. Start services
   systemctl start lightning-db

5. Verify functionality
   lightning_db health-check --full
EOF

# 4. Notify stakeholders
echo "[4/5] Sending maintenance notifications..."
python3 send_maintenance_notification.py

# 5. Update change calendar
echo "[5/5] Updating change calendar..."
echo "Please update the change calendar with maintenance details"
```

---

## Monthly Operations

### First Monday: Capacity Planning Review

**Time Required**: 4 hours  
**Priority**: P2 - High  
**Owner**: Capacity Planning Team

```python
#!/usr/bin/env python3
# monthly_capacity_review.py

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

class CapacityReview:
    def __init__(self):
        self.report_date = datetime.now()
        
    def generate_growth_analysis(self):
        """Analyze growth trends"""
        # Collect metrics for past 6 months
        metrics = self.collect_historical_metrics()
        
        # Calculate growth rates
        growth_rates = {
            'data_size': self.calculate_growth_rate(metrics['data_size']),
            'qps': self.calculate_growth_rate(metrics['qps']),
            'connections': self.calculate_growth_rate(metrics['connections'])
        }
        
        # Project future needs
        projections = self.project_capacity_needs(growth_rates)
        
        # Generate recommendations
        recommendations = self.generate_recommendations(projections)
        
        return {
            'growth_rates': growth_rates,
            'projections': projections,
            'recommendations': recommendations
        }
    
    def generate_report(self):
        """Generate comprehensive capacity report"""
        report = f"""
# Monthly Capacity Planning Report
Date: {self.report_date.strftime('%Y-%m-%d')}

## Executive Summary
{self.generate_executive_summary()}

## Growth Analysis
{self.format_growth_analysis()}

## Resource Utilization
{self.format_resource_utilization()}

## Projections (6 months)
{self.format_projections()}

## Recommendations
{self.format_recommendations()}

## Budget Impact
{self.calculate_budget_impact()}
        """
        
        with open(f'capacity_report_{self.report_date.strftime("%Y%m")}.md', 'w') as f:
            f.write(report)

if __name__ == "__main__":
    review = CapacityReview()
    review.generate_report()
```

### Second Tuesday: Disaster Recovery Test

**Time Required**: 4-8 hours  
**Priority**: P1 - Critical  
**Owner**: DR Team

```bash
#!/bin/bash
# monthly_dr_test.sh

echo "=== Monthly Disaster Recovery Test ==="
test_id="DR_TEST_$(date +%Y%m%d_%H%M%S)"
log_file="/var/log/lightning_db/dr_tests/$test_id.log"

# Start logging
exec 1> >(tee -a "$log_file")
exec 2>&1

echo "Test ID: $test_id"
echo "Start Time: $(date)"

# 1. Failover Test
echo -e "\n[1/5] Testing automatic failover..."
start_time=$(date +%s)

# Simulate primary failure
ssh dr-test-primary 'sudo systemctl stop lightning-db'

# Wait for automatic failover
sleep 60

# Check if replica promoted
if ssh dr-test-replica 'lightning_db role' | grep -q "primary"; then
    echo "✓ Automatic failover successful"
    failover_time=$(($(date +%s) - start_time))
    echo "  Failover time: ${failover_time}s"
else
    echo "✗ Automatic failover FAILED"
    exit 1
fi

# 2. Backup Restore Test
echo -e "\n[2/5] Testing backup restore..."
restore_start=$(date +%s)

ssh dr-test-restore << 'EOF'
    # Clear existing data
    rm -rf /var/lib/lightning_db/data/*
    
    # Restore latest backup
    latest_backup=$(ls -t /backup/daily/ | head -1)
    lightning_db restore \
        --backup="/backup/daily/$latest_backup" \
        --parallel=8 \
        --verify
EOF

restore_time=$(($(date +%s) - restore_start))
echo "  Restore time: ${restore_time}s"

# 3. Data Validation
echo -e "\n[3/5] Validating restored data..."
lightning_db validate \
    --source=dr-test-restore \
    --target=production \
    --sample-rate=0.01

# 4. Performance Validation
echo -e "\n[4/5] Performance validation..."
lightning_db benchmark \
    --host=dr-test-restore \
    --duration=300 \
    --workload=production-replay

# 5. Rollback Test
echo -e "\n[5/5] Testing rollback procedures..."
# Restore original configuration
ssh dr-test-primary 'sudo systemctl start lightning-db'
sleep 30
lightning_db failover --to=dr-test-primary --force

# Generate Report
cat > "dr_test_report_$test_id.txt" << EOF
Disaster Recovery Test Report
Test ID: $test_id
Date: $(date)

Results:
- Automatic Failover: PASSED (${failover_time}s)
- Backup Restore: PASSED (${restore_time}s)
- Data Validation: PASSED
- Performance Test: PASSED
- Rollback: PASSED

RTO Achieved: $(( (failover_time + restore_time) / 60 )) minutes
RPO Achieved: < 1 minute

Recommendations:
- Continue current DR procedures
- Schedule next test: $(date -d '+1 month' +%Y-%m-%d)
EOF

echo -e "\n=== DR Test Complete ==="
```

### Third Wednesday: License and Compliance Audit

**Time Required**: 2 hours  
**Priority**: P2 - High  
**Owner**: Compliance Officer

```python
#!/usr/bin/env python3
# monthly_compliance_audit.py

import json
import subprocess
from datetime import datetime

class ComplianceAudit:
    def __init__(self):
        self.audit_date = datetime.now()
        self.compliance_items = {
            'gdpr': self.check_gdpr_compliance,
            'pci_dss': self.check_pci_compliance,
            'sox': self.check_sox_compliance,
            'licensing': self.check_licensing
        }
    
    def run_audit(self):
        """Run all compliance checks"""
        results = {}
        
        for compliance_type, check_function in self.compliance_items.items():
            print(f"Checking {compliance_type} compliance...")
            results[compliance_type] = check_function()
        
        self.generate_compliance_report(results)
    
    def check_gdpr_compliance(self):
        """Check GDPR compliance"""
        checks = {
            'data_retention': self.check_data_retention_policy(),
            'encryption_at_rest': self.verify_encryption(),
            'audit_logging': self.check_audit_logs(),
            'data_anonymization': self.check_anonymization_procedures(),
            'right_to_be_forgotten': self.verify_deletion_procedures()
        }
        return checks
    
    def check_data_retention_policy(self):
        """Verify data retention policies are enforced"""
        # Check for data older than retention period
        cmd = """
        lightning_db query "
            SELECT COUNT(*) 
            FROM audit_logs 
            WHERE created_at < NOW() - INTERVAL '2 years'
        "
        """
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        old_records = int(result.stdout.strip())
        
        return {
            'status': 'PASS' if old_records == 0 else 'FAIL',
            'details': f"Found {old_records} records older than retention period"
        }
    
    def generate_compliance_report(self, results):
        """Generate compliance report"""
        report = {
            'audit_date': self.audit_date.isoformat(),
            'overall_status': 'COMPLIANT',
            'details': results,
            'action_items': []
        }
        
        # Check for any failures
        for compliance_type, checks in results.items():
            for check_name, check_result in checks.items():
                if check_result.get('status') == 'FAIL':
                    report['overall_status'] = 'NON_COMPLIANT'
                    report['action_items'].append({
                        'type': compliance_type,
                        'check': check_name,
                        'action': f"Address: {check_result.get('details', 'Unknown issue')}"
                    })
        
        # Save report
        with open(f'compliance_audit_{self.audit_date.strftime("%Y%m")}.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\nCompliance Status: {report['overall_status']}")
        if report['action_items']:
            print("\nAction Items:")
            for item in report['action_items']:
                print(f"- {item['type']}/{item['check']}: {item['action']}")

if __name__ == "__main__":
    audit = ComplianceAudit()
    audit.run_audit()
```

### Last Friday: Monthly Maintenance

**Time Required**: 4-6 hours (maintenance window)  
**Priority**: P1 - Critical  
**Owner**: Operations Team

```bash
#!/bin/bash
# monthly_maintenance.sh

echo "=== Monthly Maintenance Window ==="
maintenance_id="MAINT_$(date +%Y%m%d)"

# Pre-maintenance checklist
echo "Pre-maintenance checklist:"
echo "[ ] Maintenance notification sent (24h advance)"
echo "[ ] Backup completed and verified"
echo "[ ] Rollback plan documented"
echo "[ ] Team members on standby"
read -p "Continue with maintenance? (yes/no) " confirm
[[ "$confirm" != "yes" ]] && exit 1

# 1. Enable maintenance mode
echo "[1/8] Enabling maintenance mode..."
lightning_db maintenance-mode enable --message="Monthly maintenance in progress"

# 2. Full backup
echo "[2/8] Creating full backup..."
lightning_db backup create --full --name="$maintenance_id" --verify

# 3. Major version updates
echo "[3/8] Applying updates..."
lightning_db update apply --major --backup-first

# 4. Full vacuum
echo "[4/8] Running full vacuum..."
lightning_db vacuum --full --all-tables --parallel=4

# 5. Index rebuild
echo "[5/8] Rebuilding indexes..."
lightning_db indexes rebuild --all --parallel=4

# 6. Statistics update
echo "[6/8] Updating all statistics..."
lightning_db stats update --all --full

# 7. Configuration optimization
echo "[7/8] Optimizing configuration..."
lightning_db config optimize --apply --backup-current

# 8. Disable maintenance mode
echo "[8/8] Disabling maintenance mode..."
lightning_db maintenance-mode disable

# Post-maintenance validation
echo -e "\nPost-maintenance validation:"
lightning_db health-check --full
lightning_db performance benchmark --quick

echo "Monthly maintenance complete!"
```

---

## On-Demand Operations

### Emergency Space Recovery

**Trigger**: Disk usage > 90%  
**Time Required**: 30-60 minutes  
**Priority**: P1 - Critical

```bash
#!/bin/bash
# emergency_space_recovery.sh

echo "=== EMERGENCY: Space Recovery Procedure ==="
echo "Current disk usage:"
df -h /var/lib/lightning_db

# 1. Clear temporary files
echo "[1/6] Clearing temporary files..."
find /var/lib/lightning_db/temp -type f -delete
find /tmp -name "lightning_*" -mtime +1 -delete

# 2. Truncate old WAL files
echo "[2/6] Cleaning WAL files..."
lightning_db wal cleanup --aggressive --keep-minimum

# 3. Remove old backups
echo "[3/6] Removing old backups..."
find /backup -name "*.backup" -mtime +3 -delete

# 4. Vacuum large tables
echo "[4/6] Vacuuming large tables..."
lightning_db vacuum --tables="$(lightning_db tables list --top=5 --by=size)"

# 5. Archive old data
echo "[5/6] Archiving old data..."
lightning_db archive --older-than="1 year" --tables=audit_logs,old_transactions

# 6. Compress remaining files
echo "[6/6] Compressing logs..."
find /var/log/lightning_db -name "*.log" -mtime +1 -exec gzip {} \;

echo "Space recovery complete:"
df -h /var/lib/lightning_db
```

### Performance Emergency Response

**Trigger**: Response time > 5s or error rate > 5%  
**Time Required**: 15-30 minutes  
**Priority**: P1 - Critical

```bash
#!/bin/bash
# performance_emergency.sh

echo "=== EMERGENCY: Performance Response ==="

# 1. Identify bottleneck
echo "[1/5] Identifying bottleneck..."
lightning_db diagnose --quick

# 2. Kill slow queries
echo "[2/5] Terminating slow queries..."
lightning_db query terminate --duration=">30s" --exclude-maintenance

# 3. Clear caches if memory pressure
mem_usage=$(free | grep Mem | awk '{print ($3/$2) * 100}')
if (( $(echo "$mem_usage > 90" | bc -l) )); then
    echo "[3/5] High memory usage detected, clearing caches..."
    lightning_db cache clear --partial --type=query
fi

# 4. Disable non-essential features
echo "[4/5] Disabling non-essential features..."
lightning_db config set enable_analytics=false --reload
lightning_db config set enable_debug_logging=false --reload

# 5. Add emergency capacity
echo "[5/5] Adding emergency read replica..."
lightning_db replica add --emergency --instance-type=c5.2xlarge

echo "Emergency response complete. Monitoring recovery..."
```

### Security Incident Response

**Trigger**: Security breach detected  
**Time Required**: Immediate  
**Priority**: P0 - Emergency

```bash
#!/bin/bash
# security_incident_response.sh

echo "=== SECURITY INCIDENT RESPONSE ==="
incident_id="SEC_$(date +%Y%m%d_%H%M%S)"

# 1. Isolate system
echo "[CRITICAL] Isolating database..."
lightning_db network isolate --except-admin

# 2. Preserve evidence
echo "Preserving evidence..."
mkdir -p "/security/incidents/$incident_id"
cp -r /var/log/lightning_db "/security/incidents/$incident_id/"
lightning_db connections snapshot > "/security/incidents/$incident_id/connections.json"
lightning_db audit export --all > "/security/incidents/$incident_id/audit.json"

# 3. Terminate suspicious connections
echo "Terminating suspicious connections..."
lightning_db connections terminate --suspicious --force

# 4. Rotate credentials
echo "Rotating all credentials..."
lightning_db security rotate-all-passwords
lightning_db security rotate-ssl-certs

# 5. Enable enhanced logging
echo "Enabling forensic logging..."
lightning_db config set audit_level=forensic --reload

echo "Initial response complete. Incident ID: $incident_id"
```

---

## Automation Scripts

### Automated Health Monitor

```python
#!/usr/bin/env python3
# automated_health_monitor.py

import time
import subprocess
import json
from datetime import datetime
import requests

class HealthMonitor:
    def __init__(self, config_file='monitor_config.json'):
        with open(config_file) as f:
            self.config = json.load(f)
        self.alert_webhook = self.config['alert_webhook']
        self.check_interval = self.config['check_interval']
        
    def run_continuous_monitoring(self):
        """Run continuous health monitoring"""
        while True:
            try:
                health_status = self.check_health()
                
                if health_status['overall'] != 'healthy':
                    self.send_alert(health_status)
                
                # Log metrics
                self.log_metrics(health_status)
                
            except Exception as e:
                self.send_alert({
                    'error': str(e),
                    'severity': 'critical',
                    'message': 'Health monitor failed'
                })
            
            time.sleep(self.check_interval)
    
    def check_health(self):
        """Perform health checks"""
        checks = {
            'database_up': self.check_database_up(),
            'replication_healthy': self.check_replication(),
            'disk_space_ok': self.check_disk_space(),
            'performance_normal': self.check_performance(),
            'no_errors': self.check_error_logs()
        }
        
        overall = 'healthy' if all(checks.values()) else 'unhealthy'
        
        return {
            'timestamp': datetime.now().isoformat(),
            'overall': overall,
            'checks': checks
        }
    
    def check_database_up(self):
        """Check if database is responding"""
        result = subprocess.run(
            ['lightning_db', 'ping', '--timeout=5'],
            capture_output=True
        )
        return result.returncode == 0
    
    def send_alert(self, health_status):
        """Send alert via webhook"""
        requests.post(self.alert_webhook, json={
            'text': f"Lightning DB Health Alert: {health_status['overall']}",
            'details': health_status
        })

if __name__ == "__main__":
    monitor = HealthMonitor()
    monitor.run_continuous_monitoring()
```

### Automated Maintenance Scheduler

```python
#!/usr/bin/env python3
# maintenance_scheduler.py

import schedule
import time
import subprocess
from datetime import datetime
import logging

class MaintenanceScheduler:
    def __init__(self):
        self.setup_logging()
        self.setup_schedule()
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='/var/log/lightning_db/maintenance_scheduler.log'
        )
    
    def setup_schedule(self):
        """Setup maintenance schedule"""
        # Daily tasks
        schedule.every().day.at("03:00").do(self.run_daily_maintenance)
        schedule.every().day.at("08:00").do(self.run_health_check)
        schedule.every().day.at("14:00").do(self.update_performance_baseline)
        
        # Weekly tasks
        schedule.every().monday.at("10:00").do(self.run_weekly_audit)
        schedule.every().wednesday.at("02:00").do(self.validate_backups)
        schedule.every().friday.at("16:00").do(self.prepare_maintenance_window)
        
        # Monthly tasks
        schedule.every().month.do(self.run_capacity_planning)
        schedule.every().month.do(self.run_dr_test)
    
    def run_daily_maintenance(self):
        """Run daily maintenance tasks"""
        logging.info("Starting daily maintenance")
        try:
            subprocess.run(['/opt/lightning_db/scripts/daily_maintenance.sh'], check=True)
            logging.info("Daily maintenance completed successfully")
        except subprocess.CalledProcessError as e:
            logging.error(f"Daily maintenance failed: {e}")
            self.send_alert("Daily maintenance failed", severity='high')
    
    def run(self):
        """Run the scheduler"""
        logging.info("Maintenance scheduler started")
        while True:
            schedule.run_pending()
            time.sleep(60)

if __name__ == "__main__":
    scheduler = MaintenanceScheduler()
    scheduler.run()
```

---

## Operational Metrics

### Key Performance Indicators (KPIs)

| KPI | Target | Measurement | Alert Threshold |
|-----|--------|-------------|-----------------|
| Uptime | 99.99% | Monthly | < 99.9% |
| Response Time (P95) | < 100ms | Real-time | > 200ms |
| Error Rate | < 0.1% | Real-time | > 1% |
| Backup Success Rate | 100% | Daily | Any failure |
| Recovery Time | < 15 min | Quarterly test | > 30 min |
| Replication Lag | < 5s | Real-time | > 60s |

### Operational Dashboard

```json
{
  "dashboard": {
    "title": "Lightning DB Operations Dashboard",
    "refresh": "30s",
    "panels": [
      {
        "title": "System Health",
        "type": "stat",
        "targets": [{
          "expr": "lightning_db_health_score"
        }]
      },
      {
        "title": "Daily Tasks Completion",
        "type": "gauge",
        "targets": [{
          "expr": "lightning_db_ops_tasks_completed / lightning_db_ops_tasks_total * 100"
        }]
      },
      {
        "title": "Maintenance Window Success Rate",
        "type": "graph",
        "targets": [{
          "expr": "rate(lightning_db_maintenance_success[30d])"
        }]
      },
      {
        "title": "Operational Alerts",
        "type": "table",
        "targets": [{
          "expr": "ALERTS{job='lightning_db'}"
        }]
      }
    ]
  }
}
```

---

## Handover Procedures

### Shift Handover Checklist

```markdown
## Shift Handover - Date: _______

### Outgoing Operator: _______
### Incoming Operator: _______

#### System Status
- [ ] Overall health: Green / Yellow / Red
- [ ] Active alerts: _______________
- [ ] Ongoing issues: _______________

#### Completed Tasks
- [ ] Morning health check: Complete / Pending
- [ ] Backup verification: Complete / Pending
- [ ] Performance baseline: Complete / Pending

#### In-Progress Items
- [ ] Current maintenance: _______________
- [ ] Open tickets: _______________
- [ ] Scheduled tasks: _______________

#### Special Instructions
_________________________________
_________________________________

#### Handover Confirmed
- Outgoing: _______ (signature)
- Incoming: _______ (signature)
- Time: _______
```

### On-Call Handover

```bash
#!/bin/bash
# oncall_handover.sh

echo "=== On-Call Handover Procedure ==="

# Generate handover report
cat > handover_$(date +%Y%m%d).txt << EOF
On-Call Handover Report
Date: $(date)
Outgoing: $1
Incoming: $2

System Status:
$(lightning_db status --summary)

Active Issues:
$(lightning_db issues list --active)

Recent Incidents:
$(lightning_db incidents list --days=7)

Upcoming Maintenance:
$(lightning_db maintenance list --upcoming --days=7)

Important Notes:
- Check backup job at 3 AM (sometimes delays on Sundays)
- Monitor replication lag during peak hours (2-4 PM)
- Weekly maintenance window: Saturday 2-6 AM

Emergency Contacts:
- Escalation: +1-555-ESCALATE
- Vendor Support: +1-555-VENDOR
- Management: +1-555-MANAGER
EOF

echo "Handover report generated: handover_$(date +%Y%m%d).txt"
```

---

## Operations Calendar

### Standard Schedule

| Day | Time | Task | Owner | Duration |
|-----|------|------|-------|----------|
| **Daily** | 08:00 | Health Check | On-duty DBA | 30 min |
| **Daily** | 10:00 | WAL Maintenance | On-duty DBA | 10 min |
| **Daily** | 14:00 | Performance Baseline | Perf Engineer | 10 min |
| **Daily** | 17:00 | Daily Report | On-duty DBA | 15 min |
| **Monday** | 10:00 | Weekly Audit | Senior DBA | 2 hours |
| **Tuesday** | 14:00 | Performance Tuning | Perf Engineer | 2 hours |
| **Wednesday** | 03:00 | Backup Validation | Backup Admin | 3 hours |
| **Thursday** | 10:00 | Security Review | Security Admin | 2 hours |
| **Friday** | 15:00 | Maintenance Prep | Ops Lead | 1 hour |
| **Monthly-1st** | 09:00 | Capacity Review | Planning Team | 4 hours |
| **Monthly-2nd** | 10:00 | DR Test | DR Team | 6 hours |
| **Monthly-3rd** | 14:00 | Compliance Audit | Compliance | 2 hours |
| **Monthly-Last** | 02:00 | Major Maintenance | Ops Team | 4 hours |

---

**Remember**: Consistency in operations is key to reliability. Follow procedures exactly, document any deviations, and continuously improve based on lessons learned.