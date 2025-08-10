# Lightning DB Monitoring and Alerting Setup Guide

## Overview

This guide provides comprehensive instructions for setting up monitoring, alerting, and observability for Lightning DB in production environments. Proper monitoring is essential for maintaining performance, detecting issues early, and ensuring SLA compliance.

**Stack Components**:
- **Metrics**: Prometheus + Grafana
- **Logs**: Loki + Promtail
- **Traces**: OpenTelemetry + Jaeger
- **Alerts**: AlertManager + PagerDuty/Slack
- **Dashboards**: Grafana

---

## Table of Contents

1. [Metrics Collection](#metrics-collection)
2. [Log Aggregation](#log-aggregation)
3. [Distributed Tracing](#distributed-tracing)
4. [Alerting Rules](#alerting-rules)
5. [Dashboards](#dashboards)
6. [SLI/SLO Configuration](#slislo-configuration)
7. [Troubleshooting with Metrics](#troubleshooting-with-metrics)
8. [Automation and Integration](#automation-and-integration)

---

## Metrics Collection

### 1. Enable Lightning DB Metrics

```rust
// Configure metrics in Lightning DB
let metrics_config = MetricsConfig {
    enabled: true,
    endpoint: "0.0.0.0:9090",
    
    // Metric categories
    enable_system_metrics: true,
    enable_query_metrics: true,
    enable_transaction_metrics: true,
    enable_cache_metrics: true,
    enable_io_metrics: true,
    enable_replication_metrics: true,
    
    // Sampling
    sample_rate: 1.0,  // 100% for critical metrics
    histogram_buckets: vec![
        0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0
    ],
    
    // Labels
    default_labels: HashMap::from([
        ("environment", "production"),
        ("region", "us-east-1"),
        ("cluster", "primary"),
    ]),
};
```

### 2. Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    monitor: 'lightning-db-monitor'
    environment: 'production'

# Alerting
alerting:
  alertmanagers:
    - static_configs:
        - targets:
            - alertmanager:9093

# Load rules
rule_files:
  - "lightning_db_rules.yml"
  - "lightning_db_recording_rules.yml"

# Scrape configurations
scrape_configs:
  # Lightning DB metrics
  - job_name: 'lightning_db'
    static_configs:
      - targets: 
        - 'db-primary:9090'
        - 'db-replica-1:9090'
        - 'db-replica-2:9090'
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '([^:]+):.*'
      
  # Node exporter for system metrics
  - job_name: 'node_exporter'
    static_configs:
      - targets:
        - 'db-primary:9100'
        - 'db-replica-1:9100'
        - 'db-replica-2:9100'
```

### 3. Key Metrics to Collect

```yaml
# Essential Lightning DB metrics
metrics:
  # Performance metrics
  - lightning_db_query_duration_seconds
  - lightning_db_transaction_duration_seconds
  - lightning_db_operations_total
  - lightning_db_operations_errors_total
  
  # Resource metrics
  - lightning_db_memory_usage_bytes
  - lightning_db_cache_hit_ratio
  - lightning_db_disk_usage_bytes
  - lightning_db_connections_active
  
  # Replication metrics
  - lightning_db_replication_lag_seconds
  - lightning_db_wal_lag_bytes
  
  # Business metrics
  - lightning_db_table_size_bytes
  - lightning_db_table_rows_total
```

---

## Log Aggregation

### 1. Configure Structured Logging

```toml
# lightning_db_logging.toml
[logging]
format = "json"
level = "info"
output = "/var/log/lightning_db/lightning_db.log"

[fields]
# Always include these fields
timestamp = true
level = true
module = true
correlation_id = true
user_id = true
query_id = true
duration_ms = true

[filters]
# Sensitive data masking
mask_credit_cards = true
mask_ssn = true
mask_passwords = true
```

### 2. Promtail Configuration

```yaml
# promtail.yml
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://loki:3100/loki/api/v1/push

scrape_configs:
  - job_name: lightning_db
    static_configs:
      - targets:
          - localhost
        labels:
          job: lightning_db
          environment: production
          __path__: /var/log/lightning_db/*.log
    
    pipeline_stages:
      # Parse JSON logs
      - json:
          expressions:
            timestamp: timestamp
            level: level
            module: module
            message: message
            query_id: query_id
            duration_ms: duration_ms
            error: error
      
      # Extract additional fields
      - regex:
          expression: 'query_type=(?P<query_type>\w+)'
      
      # Set timestamp
      - timestamp:
          source: timestamp
          format: RFC3339
      
      # Add labels
      - labels:
          level:
          module:
          query_type:
      
      # Filter sensitive data
      - replace:
          expression: '(\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4})'
          replace: 'XXXX-XXXX-XXXX-${1}'
```

### 3. Loki Configuration

```yaml
# loki.yml
auth_enabled: false

server:
  http_listen_port: 3100

ingester:
  lifecycler:
    address: 127.0.0.1
    ring:
      kvstore:
        store: inmemory
      replication_factor: 1
  chunk_idle_period: 5m
  chunk_retain_period: 30s

schema_config:
  configs:
    - from: 2024-01-01
      store: boltdb-shipper
      object_store: filesystem
      schema: v11
      index:
        prefix: index_
        period: 24h

storage_config:
  boltdb_shipper:
    active_index_directory: /loki/boltdb-shipper-active
    cache_location: /loki/boltdb-shipper-cache
    shared_store: filesystem
  filesystem:
    directory: /loki/chunks

limits_config:
  enforce_metric_name: false
  reject_old_samples: true
  reject_old_samples_max_age: 168h
  ingestion_rate_mb: 16
  ingestion_burst_size_mb: 32

chunk_store_config:
  max_look_back_period: 0s

table_manager:
  retention_deletes_enabled: true
  retention_period: 720h  # 30 days
```

---

## Distributed Tracing

### 1. OpenTelemetry Configuration

```rust
// Enable tracing in Lightning DB
use opentelemetry::{global, sdk::export::trace::stdout};

let tracing_config = TracingConfig {
    enabled: true,
    service_name: "lightning-db",
    
    // Sampling
    sample_rate: 0.1,  // 10% in production
    
    // Exporters
    exporters: vec![
        ExporterConfig::Jaeger {
            endpoint: "http://jaeger:14268/api/traces",
        },
        ExporterConfig::OTLP {
            endpoint: "http://otel-collector:4317",
        },
    ],
    
    // Span processors
    batch_size: 512,
    batch_timeout: Duration::from_secs(5),
};
```

### 2. Instrument Critical Paths

```rust
// Example instrumentation
use opentelemetry::{trace::{Tracer, SpanKind}, KeyValue};

impl Database {
    #[instrument(skip(self), fields(query_id = %query_id))]
    pub async fn execute_query(&self, query: Query) -> Result<QueryResult> {
        let tracer = global::tracer("lightning-db");
        let mut span = tracer
            .span_builder("execute_query")
            .with_kind(SpanKind::Server)
            .with_attributes(vec![
                KeyValue::new("query.type", query.query_type()),
                KeyValue::new("query.complexity", query.complexity()),
            ])
            .start(&tracer);
        
        // Parse query
        let parse_span = tracer.span_builder("parse_query").start(&tracer);
        let parsed = self.parse_query(query)?;
        parse_span.end();
        
        // Plan execution
        let plan_span = tracer.span_builder("plan_query").start(&tracer);
        let plan = self.create_plan(parsed)?;
        plan_span.set_attribute(KeyValue::new("plan.cost", plan.cost()));
        plan_span.end();
        
        // Execute
        let exec_span = tracer.span_builder("execute_plan").start(&tracer);
        let result = self.execute_plan(plan).await?;
        exec_span.set_attribute(KeyValue::new("rows.returned", result.row_count()));
        exec_span.end();
        
        span.set_status(opentelemetry::trace::Status::Ok);
        Ok(result)
    }
}
```

---

## Alerting Rules

### 1. Critical Alerts

```yaml
# lightning_db_alerts.yml
groups:
  - name: lightning_db_critical
    interval: 30s
    rules:
      # Database down
      - alert: LightningDBDown
        expr: up{job="lightning_db"} == 0
        for: 1m
        labels:
          severity: critical
          team: database
        annotations:
          summary: "Lightning DB instance {{ $labels.instance }} is down"
          description: "Database has been down for more than 1 minute"
          runbook_url: "https://wiki/lightning-db-down"
      
      # High error rate
      - alert: HighErrorRate
        expr: |
          rate(lightning_db_operations_errors_total[5m]) 
          / rate(lightning_db_operations_total[5m]) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High error rate: {{ $value | humanizePercentage }}"
          description: "Error rate is above 5% for 5 minutes"
      
      # Replication lag
      - alert: ReplicationLagCritical
        expr: lightning_db_replication_lag_seconds > 60
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Replication lag is {{ $value }}s"
          description: "Replication is more than 60 seconds behind"
      
      # Disk space critical
      - alert: DiskSpaceCritical
        expr: |
          (node_filesystem_avail_bytes{mountpoint="/var/lib/lightning_db"} 
          / node_filesystem_size_bytes{mountpoint="/var/lib/lightning_db"}) < 0.1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Less than 10% disk space remaining"
          description: "Only {{ $value | humanizePercentage }} disk space left"
```

### 2. Warning Alerts

```yaml
  - name: lightning_db_warnings
    interval: 1m
    rules:
      # High latency
      - alert: HighQueryLatency
        expr: |
          histogram_quantile(0.95, 
            rate(lightning_db_query_duration_seconds_bucket[5m])
          ) > 1.0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "P95 query latency is {{ $value }}s"
          description: "95th percentile latency above 1 second"
      
      # Cache hit rate low
      - alert: LowCacheHitRate
        expr: lightning_db_cache_hit_ratio < 0.8
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Cache hit rate is {{ $value | humanizePercentage }}"
          description: "Cache hit rate below 80% for 15 minutes"
      
      # Connection pool exhaustion
      - alert: ConnectionPoolNearLimit
        expr: |
          lightning_db_connections_active 
          / lightning_db_connections_max > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Connection pool at {{ $value | humanizePercentage }} capacity"
```

### 3. AlertManager Configuration

```yaml
# alertmanager.yml
global:
  resolve_timeout: 5m
  
  # Slack configuration
  slack_api_url: 'YOUR_SLACK_WEBHOOK_URL'
  
  # PagerDuty configuration
  pagerduty_url: 'https://events.pagerduty.com/v2/enqueue'

# Routing tree
route:
  group_by: ['alertname', 'cluster', 'service']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'default'
  
  routes:
    # Critical alerts -> PagerDuty
    - match:
        severity: critical
      receiver: pagerduty
      continue: true
    
    # Database team alerts
    - match:
        team: database
      receiver: database-team
    
    # Performance alerts
    - match_re:
        alertname: '.*(Latency|Performance|Slow).*'
      receiver: performance-team

receivers:
  - name: 'default'
    slack_configs:
      - channel: '#alerts'
        title: 'Lightning DB Alert'
        text: '{{ range .Alerts }}{{ .Annotations.summary }}{{ end }}'
  
  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: 'YOUR_PAGERDUTY_SERVICE_KEY'
        description: '{{ .GroupLabels.alertname }}'
  
  - name: 'database-team'
    slack_configs:
      - channel: '#database-team'
        send_resolved: true
```

---

## Dashboards

### 1. Main Operations Dashboard

```json
{
  "dashboard": {
    "title": "Lightning DB Operations",
    "panels": [
      {
        "title": "Query Rate",
        "targets": [{
          "expr": "rate(lightning_db_operations_total[5m])"
        }]
      },
      {
        "title": "Error Rate",
        "targets": [{
          "expr": "rate(lightning_db_operations_errors_total[5m]) / rate(lightning_db_operations_total[5m])"
        }]
      },
      {
        "title": "Query Latency (P50, P95, P99)",
        "targets": [
          {
            "expr": "histogram_quantile(0.5, rate(lightning_db_query_duration_seconds_bucket[5m]))",
            "legendFormat": "P50"
          },
          {
            "expr": "histogram_quantile(0.95, rate(lightning_db_query_duration_seconds_bucket[5m]))",
            "legendFormat": "P95"
          },
          {
            "expr": "histogram_quantile(0.99, rate(lightning_db_query_duration_seconds_bucket[5m]))",
            "legendFormat": "P99"
          }
        ]
      },
      {
        "title": "Active Connections",
        "targets": [{
          "expr": "lightning_db_connections_active"
        }]
      }
    ]
  }
}
```

### 2. Resource Utilization Dashboard

```json
{
  "dashboard": {
    "title": "Lightning DB Resources",
    "panels": [
      {
        "title": "Memory Usage",
        "targets": [{
          "expr": "lightning_db_memory_usage_bytes / (1024*1024*1024)"
        }]
      },
      {
        "title": "Cache Hit Rate",
        "targets": [{
          "expr": "lightning_db_cache_hit_ratio"
        }]
      },
      {
        "title": "Disk I/O",
        "targets": [
          {
            "expr": "rate(node_disk_read_bytes_total[5m])",
            "legendFormat": "Read"
          },
          {
            "expr": "rate(node_disk_written_bytes_total[5m])",
            "legendFormat": "Write"
          }
        ]
      },
      {
        "title": "CPU Usage",
        "targets": [{
          "expr": "100 - (avg(rate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)"
        }]
      }
    ]
  }
}
```

### 3. Replication Dashboard

```json
{
  "dashboard": {
    "title": "Lightning DB Replication",
    "panels": [
      {
        "title": "Replication Lag",
        "targets": [{
          "expr": "lightning_db_replication_lag_seconds"
        }]
      },
      {
        "title": "WAL Generation Rate",
        "targets": [{
          "expr": "rate(lightning_db_wal_bytes_written[5m])"
        }]
      },
      {
        "title": "Replica Status",
        "targets": [{
          "expr": "lightning_db_replica_status"
        }]
      }
    ]
  }
}
```

---

## SLI/SLO Configuration

### 1. Define SLIs (Service Level Indicators)

```yaml
# sli_definitions.yml
slis:
  # Availability SLI
  - name: availability
    description: "Percentage of successful health checks"
    query: |
      avg_over_time(
        up{job="lightning_db"}[5m]
      )
  
  # Latency SLI
  - name: latency
    description: "95th percentile query latency"
    query: |
      histogram_quantile(0.95,
        rate(lightning_db_query_duration_seconds_bucket[5m])
      )
  
  # Error rate SLI
  - name: error_rate
    description: "Percentage of failed operations"
    query: |
      1 - (
        rate(lightning_db_operations_errors_total[5m]) /
        rate(lightning_db_operations_total[5m])
      )
  
  # Freshness SLI (for replicas)
  - name: freshness
    description: "Replication lag in seconds"
    query: |
      max(lightning_db_replication_lag_seconds)
```

### 2. Define SLOs (Service Level Objectives)

```yaml
# slo_definitions.yml
slos:
  - name: "Lightning DB Availability"
    sli: availability
    objective: 0.999  # 99.9%
    window: 30d
    
  - name: "Query Latency"
    sli: latency
    objective:
      - target: 0.95  # 95% of queries
        threshold: 0.1  # under 100ms
      - target: 0.99  # 99% of queries
        threshold: 1.0  # under 1 second
    window: 7d
    
  - name: "Error Rate"
    sli: error_rate
    objective: 0.99  # 99% success rate
    window: 7d
    
  - name: "Replication Freshness"
    sli: freshness
    objective:
      threshold: 10  # max 10 seconds lag
    window: 1d
```

### 3. Error Budget Alerts

```yaml
# error_budget_alerts.yml
groups:
  - name: error_budgets
    interval: 5m
    rules:
      - alert: AvailabilityBudgetBurn
        expr: |
          (
            1 - avg_over_time(up{job="lightning_db"}[1h])
          ) > (1 - 0.999) * 14.4
        labels:
          severity: warning
          slo: availability
        annotations:
          summary: "Burning through availability error budget"
          description: "At current rate, monthly error budget will be exhausted in {{ $value }} hours"
```

---

## Troubleshooting with Metrics

### Common Issues and Queries

#### 1. Slow Queries

```promql
# Find slowest query types
topk(10,
  avg by (query_type) (
    rate(lightning_db_query_duration_seconds_sum[5m]) /
    rate(lightning_db_query_duration_seconds_count[5m])
  )
)

# Queries taking > 5 seconds
histogram_quantile(0.99,
  rate(lightning_db_query_duration_seconds_bucket{le="+Inf"}[5m])
) > 5
```

#### 2. Memory Issues

```promql
# Memory growth rate
deriv(lightning_db_memory_usage_bytes[1h])

# Cache eviction rate
rate(lightning_db_cache_evictions_total[5m])

# Memory pressure indicators
lightning_db_memory_usage_bytes / node_memory_MemTotal_bytes
```

#### 3. Connection Problems

```promql
# Connection churn
rate(lightning_db_connections_created_total[5m])

# Long-running connections
histogram_quantile(0.95,
  lightning_db_connection_duration_seconds_bucket
)

# Connection errors
rate(lightning_db_connection_errors_total[5m])
```

---

## Automation and Integration

### 1. Auto-Remediation

```python
#!/usr/bin/env python3
# auto_remediation.py

import requests
import subprocess
import time

class LightningDBAutoHealer:
    def __init__(self, prometheus_url, db_host):
        self.prometheus_url = prometheus_url
        self.db_host = db_host
    
    def check_health(self):
        """Check database health metrics"""
        query = 'up{job="lightning_db",instance="' + self.db_host + '"}'
        response = requests.get(f"{self.prometheus_url}/api/v1/query", 
                              params={'query': query})
        
        data = response.json()
        if data['data']['result']:
            return float(data['data']['result'][0]['value'][1])
        return 0
    
    def check_replication_lag(self):
        """Check replication lag"""
        query = f'lightning_db_replication_lag_seconds{{instance="{self.db_host}"}}'
        response = requests.get(f"{self.prometheus_url}/api/v1/query",
                              params={'query': query})
        
        data = response.json()
        if data['data']['result']:
            return float(data['data']['result'][0]['value'][1])
        return 0
    
    def restart_service(self):
        """Restart Lightning DB service"""
        subprocess.run(['systemctl', 'restart', 'lightning-db'])
        time.sleep(30)  # Wait for startup
    
    def clear_cache(self):
        """Clear cache if memory pressure"""
        subprocess.run(['lightning_db', 'cache', 'clear', '--partial'])
    
    def force_checkpoint(self):
        """Force checkpoint to reduce WAL size"""
        subprocess.run(['lightning_db', 'checkpoint', '--force'])
    
    def run(self):
        """Main auto-healing loop"""
        while True:
            # Check if database is up
            if self.check_health() == 0:
                print("Database is down, attempting restart...")
                self.restart_service()
            
            # Check replication lag
            lag = self.check_replication_lag()
            if lag > 300:  # 5 minutes
                print(f"High replication lag ({lag}s), forcing checkpoint...")
                self.force_checkpoint()
            
            time.sleep(60)  # Check every minute

if __name__ == "__main__":
    healer = LightningDBAutoHealer(
        prometheus_url="http://prometheus:9090",
        db_host="db-primary:9090"
    )
    healer.run()
```

### 2. Slack Integration

```python
#!/usr/bin/env python3
# slack_alerts.py

import json
import requests
from prometheus_client.parser import text_string_to_metric_families

class SlackAlerter:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
    
    def send_performance_summary(self, metrics):
        """Send daily performance summary to Slack"""
        message = {
            "text": "Lightning DB Daily Performance Summary",
            "attachments": [{
                "color": "good" if metrics['error_rate'] < 0.01 else "danger",
                "fields": [
                    {
                        "title": "Availability",
                        "value": f"{metrics['availability']:.2%}",
                        "short": True
                    },
                    {
                        "title": "Error Rate", 
                        "value": f"{metrics['error_rate']:.2%}",
                        "short": True
                    },
                    {
                        "title": "P95 Latency",
                        "value": f"{metrics['p95_latency']:.0f}ms",
                        "short": True
                    },
                    {
                        "title": "Queries/sec",
                        "value": f"{metrics['qps']:.0f}",
                        "short": True
                    }
                ]
            }]
        }
        
        requests.post(self.webhook_url, json=message)
```

---

## Monitoring Checklist

### Initial Setup
- [ ] Prometheus configured and scraping all instances
- [ ] Grafana dashboards imported
- [ ] AlertManager configured with routing rules
- [ ] Log aggregation working with Loki
- [ ] Distributed tracing enabled
- [ ] All critical alerts defined
- [ ] SLOs configured and tracked
- [ ] Runbooks linked to all alerts

### Daily Checks
- [ ] Review overnight alerts
- [ ] Check error budget consumption
- [ ] Verify backup completion
- [ ] Review slow query log
- [ ] Check replication status

### Weekly Reviews
- [ ] Analyze performance trends
- [ ] Review capacity projections
- [ ] Update alert thresholds if needed
- [ ] Review false positive alerts
- [ ] Update documentation

### Monthly Tasks
- [ ] SLO review meeting
- [ ] Capacity planning review
- [ ] Alert fatigue analysis
- [ ] Dashboard optimization
- [ ] Monitoring cost review

---

**Remember**: Good monitoring is not about collecting all possible metrics, but about collecting the right metrics that help you understand and improve your system's behavior. Focus on actionable insights rather than data collection.