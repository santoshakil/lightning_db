# Lightning DB Capacity Planning Guide

## Overview

This guide provides comprehensive methodologies for capacity planning with Lightning DB, helping you predict resource requirements, plan for growth, and optimize resource utilization while maintaining performance SLAs.

**Planning Horizon**: 6-24 months  
**Review Frequency**: Monthly  
**Last Updated**: _______________  
**Next Review**: _______________

---

## Table of Contents

1. [Capacity Planning Framework](#capacity-planning-framework)
2. [Workload Analysis](#workload-analysis)
3. [Resource Modeling](#resource-modeling)
4. [Growth Projection](#growth-projection)
5. [Scaling Strategies](#scaling-strategies)
6. [Cost Optimization](#cost-optimization)
7. [Capacity Monitoring](#capacity-monitoring)
8. [Planning Tools and Calculators](#planning-tools-and-calculators)

---

## Capacity Planning Framework

### Planning Methodology

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Measure      │────▶│    Model        │────▶│    Monitor      │
│ Current Usage   │     │ Future Needs    │     │   Capacity      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         ▲                                               │
         └───────────────────────────────────────────────┘
                         Continuous Loop
```

### Key Metrics for Capacity Planning

| Resource | Primary Metrics | Secondary Metrics | Threshold |
|----------|----------------|-------------------|-----------|
| **CPU** | Utilization %, Query Rate | Wait Time, Context Switches | 70% sustained |
| **Memory** | Used GB, Cache Hit Rate | Page Faults, Swapping | 85% used |
| **Storage** | Used TB, Growth Rate | IOPS, Latency | 80% capacity |
| **Network** | Bandwidth Mbps, Packet Rate | Latency, Packet Loss | 60% bandwidth |
| **Connections** | Active Count, Connection Rate | Wait Time, Pool Usage | 80% of max |

---

## Workload Analysis

### 1. Baseline Establishment

```sql
-- Create workload baseline table
CREATE TABLE capacity_baseline (
    timestamp TIMESTAMP,
    metric_name TEXT,
    metric_value DOUBLE,
    percentile INT,
    PRIMARY KEY (timestamp, metric_name, percentile)
);

-- Collect baseline data (run hourly for 30 days)
INSERT INTO capacity_baseline
SELECT 
    NOW() as timestamp,
    'queries_per_second' as metric_name,
    COUNT(*) / 3600.0 as metric_value,
    NULL as percentile
FROM query_log
WHERE timestamp > NOW() - INTERVAL '1 hour';

-- Percentile analysis
INSERT INTO capacity_baseline
SELECT 
    NOW() as timestamp,
    'query_latency_ms' as metric_name,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ms) as metric_value,
    50 as percentile
FROM query_log
WHERE timestamp > NOW() - INTERVAL '1 hour'
UNION ALL
SELECT 
    NOW() as timestamp,
    'query_latency_ms' as metric_name,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as metric_value,
    95 as percentile
FROM query_log
WHERE timestamp > NOW() - INTERVAL '1 hour';
```

### 2. Workload Characterization

```python
#!/usr/bin/env python3
# workload_analyzer.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class WorkloadAnalyzer:
    def __init__(self, db_connection):
        self.db = db_connection
    
    def analyze_patterns(self, days=30):
        """Analyze workload patterns over time"""
        
        # Daily patterns
        daily_pattern = self.db.query("""
            SELECT 
                EXTRACT(hour FROM timestamp) as hour,
                AVG(queries_per_second) as avg_qps,
                MAX(queries_per_second) as max_qps,
                STDDEV(queries_per_second) as stddev_qps
            FROM capacity_baseline
            WHERE timestamp > NOW() - INTERVAL '{} days'
            GROUP BY hour
            ORDER BY hour
        """.format(days))
        
        # Weekly patterns
        weekly_pattern = self.db.query("""
            SELECT 
                EXTRACT(dow FROM timestamp) as day_of_week,
                AVG(queries_per_second) as avg_qps,
                MAX(queries_per_second) as max_qps
            FROM capacity_baseline
            WHERE timestamp > NOW() - INTERVAL '{} days'
            GROUP BY day_of_week
            ORDER BY day_of_week
        """.format(days))
        
        # Peak identification
        peaks = self.identify_peaks(daily_pattern)
        
        return {
            'daily_pattern': daily_pattern,
            'weekly_pattern': weekly_pattern,
            'peak_hours': peaks,
            'peak_multiplier': self.calculate_peak_multiplier(daily_pattern)
        }
    
    def identify_peaks(self, daily_pattern):
        """Identify peak usage hours"""
        threshold = daily_pattern['avg_qps'].mean() * 1.5
        peak_hours = daily_pattern[daily_pattern['avg_qps'] > threshold]['hour'].tolist()
        return peak_hours
    
    def calculate_peak_multiplier(self, daily_pattern):
        """Calculate peak to average ratio"""
        return daily_pattern['max_qps'].max() / daily_pattern['avg_qps'].mean()
```

### 3. Query Pattern Analysis

```sql
-- Analyze query types and resource usage
WITH query_analysis AS (
    SELECT 
        query_type,
        COUNT(*) as query_count,
        AVG(duration_ms) as avg_duration,
        AVG(rows_examined) as avg_rows_examined,
        AVG(memory_used_mb) as avg_memory_mb,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration
    FROM query_log
    WHERE timestamp > NOW() - INTERVAL '7 days'
    GROUP BY query_type
)
SELECT 
    query_type,
    query_count,
    query_count * 100.0 / SUM(query_count) OVER () as percentage,
    avg_duration,
    p95_duration,
    avg_rows_examined,
    avg_memory_mb,
    -- Resource impact score
    (query_count * avg_duration * avg_memory_mb) / 1000000.0 as impact_score
FROM query_analysis
ORDER BY impact_score DESC;
```

---

## Resource Modeling

### 1. CPU Capacity Model

```python
class CPUCapacityModel:
    def __init__(self, current_cores, current_qps, current_cpu_usage):
        self.cores = current_cores
        self.qps = current_qps
        self.cpu_usage = current_cpu_usage
        self.cpu_per_query = current_cpu_usage / current_qps
    
    def predict_cpu_needs(self, target_qps, target_latency_multiplier=1.0):
        """Predict CPU needs for target QPS"""
        # Basic linear model with safety factor
        base_cpu = target_qps * self.cpu_per_query
        
        # Adjust for latency requirements (lower latency = more CPU)
        latency_adjusted = base_cpu / target_latency_multiplier
        
        # Add overhead for background tasks (20%)
        total_cpu = latency_adjusted * 1.2
        
        # Calculate cores needed at 70% target utilization
        cores_needed = total_cpu / 0.7
        
        return {
            'cores_needed': int(np.ceil(cores_needed)),
            'predicted_cpu_usage': total_cpu,
            'headroom': (cores_needed - total_cpu) / cores_needed
        }
    
    def recommend_instance_type(self, cloud_provider='aws'):
        """Recommend cloud instance type"""
        instance_types = {
            'aws': [
                {'type': 'c5.large', 'cores': 2, 'cost': 0.085},
                {'type': 'c5.xlarge', 'cores': 4, 'cost': 0.17},
                {'type': 'c5.2xlarge', 'cores': 8, 'cost': 0.34},
                {'type': 'c5.4xlarge', 'cores': 16, 'cost': 0.68},
                {'type': 'c5.9xlarge', 'cores': 36, 'cost': 1.53},
            ]
        }
        
        # Find best fit
        recommendations = []
        for instance in instance_types[cloud_provider]:
            if instance['cores'] >= self.cores:
                efficiency = self.cores / instance['cores']
                recommendations.append({
                    'instance_type': instance['type'],
                    'cores': instance['cores'],
                    'cost_per_hour': instance['cost'],
                    'efficiency': efficiency,
                    'monthly_cost': instance['cost'] * 24 * 30
                })
        
        return sorted(recommendations, key=lambda x: x['efficiency'], reverse=True)
```

### 2. Memory Capacity Model

```python
class MemoryCapacityModel:
    def __init__(self, data_size_gb, working_set_ratio=0.2):
        self.data_size = data_size_gb
        self.working_set_ratio = working_set_ratio
        self.working_set_size = data_size_gb * working_set_ratio
    
    def calculate_memory_needs(self, target_cache_hit_rate=0.95):
        """Calculate memory needs for target cache hit rate"""
        # Cache size calculation based on working set theory
        if target_cache_hit_rate >= 0.99:
            cache_multiplier = 1.0  # Need full working set
        elif target_cache_hit_rate >= 0.95:
            cache_multiplier = 0.8
        elif target_cache_hit_rate >= 0.90:
            cache_multiplier = 0.6
        else:
            cache_multiplier = 0.4
        
        cache_size = self.working_set_size * cache_multiplier
        
        # Add system overhead
        system_overhead = 2  # GB for OS and Lightning DB processes
        connection_memory = 0.01  # GB per connection
        buffer_memory = self.data_size * 0.01  # 1% for buffers
        
        total_memory = cache_size + system_overhead + buffer_memory
        
        return {
            'cache_size_gb': cache_size,
            'total_memory_gb': total_memory,
            'working_set_coverage': cache_multiplier,
            'breakdown': {
                'cache': cache_size,
                'system': system_overhead,
                'buffers': buffer_memory
            }
        }
    
    def project_memory_growth(self, data_growth_rate, months=12):
        """Project memory needs over time"""
        projections = []
        for month in range(1, months + 1):
            future_data_size = self.data_size * (1 + data_growth_rate) ** month
            future_model = MemoryCapacityModel(future_data_size, self.working_set_ratio)
            memory_needs = future_model.calculate_memory_needs()
            
            projections.append({
                'month': month,
                'data_size_gb': future_data_size,
                'memory_needed_gb': memory_needs['total_memory_gb'],
                'cache_size_gb': memory_needs['cache_size_gb']
            })
        
        return projections
```

### 3. Storage Capacity Model

```python
class StorageCapacityModel:
    def __init__(self, current_size_gb, growth_rate_monthly):
        self.current_size = current_size_gb
        self.growth_rate = growth_rate_monthly
        
    def project_storage_needs(self, months=24, compression_ratio=0.3):
        """Project storage needs with compression"""
        projections = []
        
        for month in range(1, months + 1):
            # Raw data growth
            raw_size = self.current_size * (1 + self.growth_rate) ** month
            
            # Apply compression
            compressed_size = raw_size * (1 - compression_ratio)
            
            # Add overhead for indexes (20%), WAL (10%), backups (100%)
            index_size = compressed_size * 0.2
            wal_size = compressed_size * 0.1
            backup_size = compressed_size * 1.0
            
            total_size = compressed_size + index_size + wal_size + backup_size
            
            projections.append({
                'month': month,
                'raw_data_gb': raw_size,
                'compressed_data_gb': compressed_size,
                'total_storage_gb': total_size,
                'breakdown': {
                    'data': compressed_size,
                    'indexes': index_size,
                    'wal': wal_size,
                    'backups': backup_size
                }
            })
        
        return projections
    
    def calculate_iops_needs(self, qps, read_write_ratio=0.8):
        """Calculate IOPS requirements"""
        # Assume average query touches 10 pages
        pages_per_query = 10
        page_size_kb = 4
        
        # Read IOPS
        read_qps = qps * read_write_ratio
        read_iops = read_qps * pages_per_query
        
        # Write IOPS (includes WAL writes)
        write_qps = qps * (1 - read_write_ratio)
        write_iops = write_qps * pages_per_query * 2  # 2x for WAL
        
        total_iops = read_iops + write_iops
        
        # Add 50% headroom
        required_iops = total_iops * 1.5
        
        return {
            'read_iops': read_iops,
            'write_iops': write_iops,
            'total_iops': total_iops,
            'required_iops': required_iops,
            'storage_type': 'SSD' if required_iops > 3000 else 'HDD'
        }
```

---

## Growth Projection

### 1. Business Metrics Correlation

```python
def correlate_business_metrics(business_data, database_metrics):
    """Correlate business growth with database growth"""
    
    import scipy.stats as stats
    
    correlations = {}
    
    # Common business metrics to correlate
    business_metrics = ['daily_active_users', 'transactions', 'revenue', 'api_calls']
    db_metrics = ['data_size_gb', 'qps', 'connection_count']
    
    for biz_metric in business_metrics:
        if biz_metric in business_data.columns:
            for db_metric in db_metrics:
                if db_metric in database_metrics.columns:
                    correlation, p_value = stats.pearsonr(
                        business_data[biz_metric],
                        database_metrics[db_metric]
                    )
                    
                    if p_value < 0.05:  # Statistically significant
                        correlations[f"{biz_metric}_to_{db_metric}"] = {
                            'correlation': correlation,
                            'p_value': p_value,
                            'model': np.polyfit(
                                business_data[biz_metric],
                                database_metrics[db_metric],
                                1
                            )
                        }
    
    return correlations
```

### 2. Growth Scenarios

```python
class GrowthScenarioPlanner:
    def __init__(self, current_metrics):
        self.current = current_metrics
        
    def create_scenarios(self):
        """Create multiple growth scenarios"""
        
        scenarios = {
            'conservative': {
                'name': 'Conservative Growth',
                'user_growth': 0.05,  # 5% monthly
                'data_growth': 0.08,  # 8% monthly
                'query_growth': 0.05,  # 5% monthly
                'probability': 0.6
            },
            'expected': {
                'name': 'Expected Growth',
                'user_growth': 0.10,  # 10% monthly
                'data_growth': 0.15,  # 15% monthly
                'query_growth': 0.12,  # 12% monthly
                'probability': 0.3
            },
            'aggressive': {
                'name': 'Aggressive Growth',
                'user_growth': 0.20,  # 20% monthly
                'data_growth': 0.25,  # 25% monthly
                'query_growth': 0.20,  # 20% monthly
                'probability': 0.1
            }
        }
        
        return self.project_scenarios(scenarios, months=24)
    
    def project_scenarios(self, scenarios, months):
        """Project resource needs for each scenario"""
        
        projections = {}
        
        for scenario_name, scenario in scenarios.items():
            monthly_projections = []
            
            for month in range(1, months + 1):
                # Calculate growth multipliers
                user_multiplier = (1 + scenario['user_growth']) ** month
                data_multiplier = (1 + scenario['data_growth']) ** month
                query_multiplier = (1 + scenario['query_growth']) ** month
                
                # Project metrics
                projected = {
                    'month': month,
                    'users': self.current['users'] * user_multiplier,
                    'data_size_gb': self.current['data_size_gb'] * data_multiplier,
                    'qps': self.current['qps'] * query_multiplier,
                    'required_memory_gb': self.calculate_memory(
                        self.current['data_size_gb'] * data_multiplier
                    ),
                    'required_cpu_cores': self.calculate_cpu(
                        self.current['qps'] * query_multiplier
                    ),
                    'required_storage_tb': self.calculate_storage(
                        self.current['data_size_gb'] * data_multiplier
                    ) / 1024
                }
                
                monthly_projections.append(projected)
            
            projections[scenario_name] = {
                'scenario': scenario,
                'projections': monthly_projections
            }
        
        return projections
    
    def calculate_memory(self, data_size_gb):
        """Simple memory calculation"""
        return data_size_gb * 0.25 + 8  # 25% of data + 8GB base
    
    def calculate_cpu(self, qps):
        """Simple CPU calculation"""
        return max(4, qps / 1000)  # 1 core per 1000 QPS, minimum 4
    
    def calculate_storage(self, data_size_gb):
        """Simple storage calculation"""
        return data_size_gb * 2.5  # 2.5x for indexes, WAL, backups
```

---

## Scaling Strategies

### 1. Vertical Scaling Plan

```yaml
# vertical_scaling_thresholds.yml
scaling_triggers:
  cpu:
    scale_up:
      threshold: 80  # CPU usage %
      duration: 15   # minutes
      action: "Increase instance size"
    scale_down:
      threshold: 30
      duration: 60
      action: "Decrease instance size"
  
  memory:
    scale_up:
      threshold: 85  # Memory usage %
      duration: 30
      action: "Add memory or increase instance"
    scale_down:
      threshold: 40
      duration: 120
      
  storage:
    scale_up:
      threshold: 75  # Storage usage %
      duration: 0    # Immediate
      action: "Expand volume"

scaling_plan:
  - current: "c5.2xlarge (8 cores, 16GB)"
    next_up: "c5.4xlarge (16 cores, 32GB)"
    next_down: "c5.xlarge (4 cores, 8GB)"
    
  - current: "c5.4xlarge (16 cores, 32GB)"
    next_up: "c5.9xlarge (36 cores, 72GB)"
    next_down: "c5.2xlarge (8 cores, 16GB)"
```

### 2. Horizontal Scaling Strategy

```python
class HorizontalScalingPlanner:
    def __init__(self, shard_size_limit_gb=1000):
        self.shard_size_limit = shard_size_limit_gb
        
    def plan_sharding(self, current_size_gb, growth_projections):
        """Plan sharding strategy based on growth"""
        
        sharding_plan = []
        
        for projection in growth_projections:
            month = projection['month']
            projected_size = projection['data_size_gb']
            
            # Calculate number of shards needed
            shards_needed = int(np.ceil(projected_size / self.shard_size_limit))
            
            # Determine if re-sharding is needed
            if month > 1:
                previous_shards = sharding_plan[-1]['shards_needed']
                if shards_needed > previous_shards:
                    action = f"Re-shard from {previous_shards} to {shards_needed} shards"
                else:
                    action = "No change needed"
            else:
                action = f"Initial setup with {shards_needed} shards"
            
            sharding_plan.append({
                'month': month,
                'data_size_gb': projected_size,
                'shards_needed': shards_needed,
                'avg_shard_size_gb': projected_size / shards_needed,
                'action': action
            })
        
        return sharding_plan
    
    def calculate_replication_needs(self, read_qps, write_qps, replica_capacity=5000):
        """Calculate read replica needs"""
        
        # Reserve primary for writes
        read_replicas_needed = int(np.ceil(read_qps / replica_capacity))
        
        # Add one for high availability
        total_replicas = read_replicas_needed + 1
        
        return {
            'read_replicas': total_replicas,
            'total_instances': total_replicas + 1,  # +1 for primary
            'read_capacity': total_replicas * replica_capacity,
            'utilization': read_qps / (total_replicas * replica_capacity)
        }
```

---

## Cost Optimization

### 1. Cost Modeling

```python
class CostOptimizer:
    def __init__(self, cloud_pricing):
        self.pricing = cloud_pricing
        
    def calculate_total_cost(self, resources):
        """Calculate total monthly cost"""
        
        costs = {
            'compute': self.calculate_compute_cost(resources['instances']),
            'storage': self.calculate_storage_cost(resources['storage_gb']),
            'network': self.calculate_network_cost(resources['network_gb']),
            'backup': self.calculate_backup_cost(resources['backup_gb'])
        }
        
        costs['total'] = sum(costs.values())
        
        return costs
    
    def optimize_instance_mix(self, workload_profile):
        """Optimize instance types for cost"""
        
        if workload_profile['pattern'] == 'steady':
            # Use reserved instances
            return {
                'strategy': 'reserved_instances',
                'savings': '40-60%',
                'recommendation': 'Purchase 3-year reserved instances'
            }
        elif workload_profile['pattern'] == 'spiky':
            # Use auto-scaling with spot instances
            return {
                'strategy': 'auto_scaling_spot',
                'savings': '60-80%',
                'recommendation': 'Use spot instances for read replicas'
            }
        else:
            # Mixed approach
            return {
                'strategy': 'mixed',
                'savings': '30-50%',
                'recommendation': 'Reserved for baseline, on-demand for peaks'
            }
    
    def storage_tiering_analysis(self, data_access_patterns):
        """Analyze data for storage tiering"""
        
        tiers = []
        
        # Hot data (accessed daily)
        hot_data = data_access_patterns[data_access_patterns['access_frequency'] >= 1]
        tiers.append({
            'tier': 'hot',
            'storage_type': 'SSD',
            'size_gb': hot_data['size_gb'].sum(),
            'cost_per_gb': 0.10
        })
        
        # Warm data (accessed weekly)
        warm_data = data_access_patterns[
            (data_access_patterns['access_frequency'] < 1) & 
            (data_access_patterns['access_frequency'] >= 0.14)
        ]
        tiers.append({
            'tier': 'warm',
            'storage_type': 'HDD',
            'size_gb': warm_data['size_gb'].sum(),
            'cost_per_gb': 0.045
        })
        
        # Cold data (accessed monthly or less)
        cold_data = data_access_patterns[data_access_patterns['access_frequency'] < 0.14]
        tiers.append({
            'tier': 'cold',
            'storage_type': 'Object Storage',
            'size_gb': cold_data['size_gb'].sum(),
            'cost_per_gb': 0.023
        })
        
        return tiers
```

### 2. Resource Optimization

```python
def optimize_resources(current_usage, peak_usage, cost_constraints):
    """Optimize resource allocation for cost and performance"""
    
    optimization_plan = {
        'immediate_actions': [],
        'short_term': [],  # 1-3 months
        'long_term': []    # 3-12 months
    }
    
    # Immediate optimizations
    if current_usage['cache_hit_rate'] < 0.8:
        optimization_plan['immediate_actions'].append({
            'action': 'Increase cache size',
            'impact': 'Reduce I/O costs by 30%',
            'cost': 'Additional memory: $200/month'
        })
    
    if current_usage['cpu_usage'] < 0.3:
        optimization_plan['immediate_actions'].append({
            'action': 'Downsize instance',
            'impact': 'Save 40% on compute costs',
            'cost': 'Savings: $500/month'
        })
    
    # Short-term optimizations
    if peak_usage['duration'] < 4:  # Peak lasts less than 4 hours
        optimization_plan['short_term'].append({
            'action': 'Implement auto-scaling',
            'impact': 'Handle peaks without overprovisioning',
            'cost': 'Savings: $1000/month'
        })
    
    # Long-term optimizations
    optimization_plan['long_term'].append({
        'action': 'Implement data archiving',
        'impact': 'Reduce storage costs by 50%',
        'cost': 'Savings: $2000/month after 6 months'
    })
    
    return optimization_plan
```

---

## Capacity Monitoring

### 1. Capacity Dashboards

```json
{
  "dashboard": {
    "title": "Lightning DB Capacity Planning",
    "panels": [
      {
        "title": "Storage Growth Trend",
        "type": "graph",
        "targets": [{
          "expr": "predict_linear(lightning_db_data_size_bytes[30d], 86400 * 30)"
        }]
      },
      {
        "title": "Days Until Storage Full",
        "type": "stat",
        "targets": [{
          "expr": "(lightning_db_storage_capacity_bytes - lightning_db_data_size_bytes) / (rate(lightning_db_data_size_bytes[7d]) * 86400)"
        }]
      },
      {
        "title": "Memory Pressure Score",
        "type": "gauge",
        "targets": [{
          "expr": "(1 - lightning_db_cache_hit_ratio) * lightning_db_memory_usage_percent"
        }]
      },
      {
        "title": "Connection Pool Utilization",
        "type": "graph",
        "targets": [{
          "expr": "lightning_db_connections_active / lightning_db_connections_max * 100"
        }]
      }
    ]
  }
}
```

### 2. Alerting for Capacity

```yaml
# capacity_alerts.yml
groups:
  - name: capacity_planning
    interval: 5m
    rules:
      # Storage capacity
      - alert: StorageCapacityWarning
        expr: |
          predict_linear(lightning_db_data_size_bytes[7d], 86400 * 30) 
          > lightning_db_storage_capacity_bytes * 0.8
        labels:
          severity: warning
          team: capacity-planning
        annotations:
          summary: "Storage will reach 80% in 30 days"
          description: "Current growth rate will fill storage in {{ $value / 86400 }} days"
      
      # Memory capacity
      - alert: MemoryCapacityWarning
        expr: |
          lightning_db_memory_usage_bytes / lightning_db_memory_total_bytes > 0.85
          AND
          rate(lightning_db_cache_evictions_total[5m]) > 100
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "Memory pressure detected"
          description: "High memory usage with increased cache evictions"
      
      # Connection capacity
      - alert: ConnectionCapacityWarning
        expr: |
          predict_linear(lightning_db_connections_active[1h], 3600) 
          > lightning_db_connections_max * 0.9
        labels:
          severity: warning
        annotations:
          summary: "Connection pool will be exhausted in 1 hour"
```

---

## Planning Tools and Calculators

### 1. Interactive Capacity Calculator

```html
<!DOCTYPE html>
<html>
<head>
    <title>Lightning DB Capacity Calculator</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <h1>Lightning DB Capacity Calculator</h1>
    
    <div id="inputs">
        <h2>Current Metrics</h2>
        <label>Data Size (GB): <input type="number" id="dataSize" value="100"></label><br>
        <label>Monthly Growth %: <input type="number" id="growthRate" value="10"></label><br>
        <label>Queries/Second: <input type="number" id="qps" value="1000"></label><br>
        <label>Users: <input type="number" id="users" value="10000"></label><br>
        
        <h2>Planning Horizon</h2>
        <label>Months: <input type="number" id="months" value="12"></label><br>
        
        <button onclick="calculate()">Calculate</button>
    </div>
    
    <div id="results">
        <canvas id="growthChart"></canvas>
        <div id="recommendations"></div>
    </div>
    
    <script>
    function calculate() {
        const dataSize = parseFloat(document.getElementById('dataSize').value);
        const growthRate = parseFloat(document.getElementById('growthRate').value) / 100;
        const qps = parseFloat(document.getElementById('qps').value);
        const users = parseFloat(document.getElementById('users').value);
        const months = parseInt(document.getElementById('months').value);
        
        // Calculate projections
        const projections = [];
        for (let i = 0; i <= months; i++) {
            const multiplier = Math.pow(1 + growthRate, i);
            projections.push({
                month: i,
                dataSize: dataSize * multiplier,
                storage: dataSize * multiplier * 2.5, // Include overhead
                memory: dataSize * multiplier * 0.25 + 8,
                cpu: Math.max(4, (qps * multiplier) / 1000),
                users: users * multiplier
            });
        }
        
        // Display chart
        displayChart(projections);
        
        // Generate recommendations
        generateRecommendations(projections[months]);
    }
    
    function displayChart(projections) {
        const ctx = document.getElementById('growthChart').getContext('2d');
        new Chart(ctx, {
            type: 'line',
            data: {
                labels: projections.map(p => `Month ${p.month}`),
                datasets: [
                    {
                        label: 'Storage (GB)',
                        data: projections.map(p => p.storage),
                        borderColor: 'blue'
                    },
                    {
                        label: 'Memory (GB)',
                        data: projections.map(p => p.memory),
                        borderColor: 'green'
                    },
                    {
                        label: 'CPU Cores',
                        data: projections.map(p => p.cpu),
                        borderColor: 'red'
                    }
                ]
            }
        });
    }
    
    function generateRecommendations(finalMetrics) {
        const recommendations = document.getElementById('recommendations');
        recommendations.innerHTML = `
            <h2>Recommendations</h2>
            <ul>
                <li>Storage: Provision ${Math.ceil(finalMetrics.storage / 1024)} TB</li>
                <li>Memory: Minimum ${Math.ceil(finalMetrics.memory)} GB RAM</li>
                <li>CPU: ${Math.ceil(finalMetrics.cpu)} cores required</li>
                <li>Consider sharding if data exceeds 1TB</li>
                <li>Implement archiving for data older than 1 year</li>
            </ul>
        `;
    }
    </script>
</body>
</html>
```

### 2. Capacity Planning Spreadsheet Template

```python
# generate_capacity_template.py
import pandas as pd
from openpyxl import Workbook
from openpyxl.chart import LineChart, Reference

def create_capacity_planning_template():
    """Create Excel template for capacity planning"""
    
    wb = Workbook()
    
    # Current State sheet
    ws1 = wb.active
    ws1.title = "Current State"
    ws1.append(["Metric", "Value", "Unit"])
    ws1.append(["Data Size", 100, "GB"])
    ws1.append(["QPS", 1000, "queries/sec"])
    ws1.append(["Users", 10000, "count"])
    ws1.append(["CPU Cores", 8, "cores"])
    ws1.append(["Memory", 32, "GB"])
    
    # Growth Projections sheet
    ws2 = wb.create_sheet("Projections")
    ws2.append(["Month", "Data Size (GB)", "QPS", "Users", "CPU Needed", "Memory Needed"])
    
    # Formulas for projections
    for month in range(1, 25):
        ws2.append([
            month,
            f"='Current State'!B2*(1.1^{month})",  # 10% monthly growth
            f"='Current State'!B3*(1.1^{month})",
            f"='Current State'!B4*(1.1^{month})",
            f"=MAX(4, C{month+1}/1000)",
            f"=B{month+1}*0.25+8"
        ])
    
    # Add chart
    chart = LineChart()
    chart.title = "Capacity Growth Projection"
    chart.y_axis.title = "Resources"
    chart.x_axis.title = "Months"
    
    data = Reference(ws2, min_col=2, min_row=1, max_row=25, max_col=6)
    chart.add_data(data, titles_from_data=True)
    ws2.add_chart(chart, "H2")
    
    # Recommendations sheet
    ws3 = wb.create_sheet("Recommendations")
    ws3.append(["Timeline", "Action", "Reason", "Cost Impact"])
    ws3.append(["Month 3", "Upgrade to 64GB RAM", "Memory usage > 85%", "$200/month"])
    ws3.append(["Month 6", "Add read replica", "QPS exceeds 5000", "$500/month"])
    ws3.append(["Month 9", "Implement sharding", "Data size > 1TB", "$2000 setup"])
    
    wb.save("capacity_planning_template.xlsx")

if __name__ == "__main__":
    create_capacity_planning_template()
```

---

## Capacity Planning Checklist

### Monthly Review
- [ ] Analyze growth trends from last month
- [ ] Update growth rate projections
- [ ] Review resource utilization
- [ ] Check against capacity thresholds
- [ ] Update cost projections

### Quarterly Planning
- [ ] Review business growth projections
- [ ] Update capacity models
- [ ] Plan hardware/instance upgrades
- [ ] Review and optimize costs
- [ ] Update disaster recovery capacity

### Annual Planning
- [ ] Long-term architecture review
- [ ] Evaluate new technologies
- [ ] Plan major scaling initiatives
- [ ] Budget allocation for growth
- [ ] Update capacity planning tools

---

**Remember**: Capacity planning is not just about having enough resources, but about having the right resources at the right time while optimizing costs. Regular review and adjustment of your capacity plans ensures smooth growth and optimal performance.