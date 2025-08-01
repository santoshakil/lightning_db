#!/bin/bash

# Lightning DB Monitoring Setup Script
#
# This script sets up comprehensive monitoring for Lightning DB
# Components: Prometheus, Grafana, Alertmanager, Loki

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MONITORING_DIR="${MONITORING_DIR:-/opt/lightning_db/monitoring}"
INSTALL_METHOD="${INSTALL_METHOD:-docker}"

# Versions
PROMETHEUS_VERSION="2.45.0"
GRAFANA_VERSION="10.0.0"
ALERTMANAGER_VERSION="0.26.0"
LOKI_VERSION="2.9.0"

# Ports
PROMETHEUS_PORT="9090"
GRAFANA_PORT="3000"
ALERTMANAGER_PORT="9093"
LOKI_PORT="3100"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_usage() {
    cat << EOF
Lightning DB Monitoring Setup

Usage: $0 [OPTIONS]

Options:
    -m, --method METHOD     Installation method (docker|native|k8s) [default: docker]
    -d, --dir DIR          Monitoring directory [default: /opt/lightning_db/monitoring]
    -p, --prometheus PORT  Prometheus port [default: 9090]
    -g, --grafana PORT     Grafana port [default: 3000]
    -a, --alertmanager PORT Alertmanager port [default: 9093]
    -l, --loki PORT        Loki port [default: 3100]
    -h, --help             Show this help message

Examples:
    # Docker setup (recommended)
    $0 --method docker

    # Native installation
    $0 --method native --dir /var/lib/monitoring

    # Kubernetes setup
    $0 --method k8s
EOF
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--method)
            INSTALL_METHOD="$2"
            shift 2
            ;;
        -d|--dir)
            MONITORING_DIR="$2"
            shift 2
            ;;
        -p|--prometheus)
            PROMETHEUS_PORT="$2"
            shift 2
            ;;
        -g|--grafana)
            GRAFANA_PORT="$2"
            shift 2
            ;;
        -a|--alertmanager)
            ALERTMANAGER_PORT="$2"
            shift 2
            ;;
        -l|--loki)
            LOKI_PORT="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Create directories
create_directories() {
    log_info "Creating monitoring directories..."
    
    sudo mkdir -p "$MONITORING_DIR"/{prometheus,grafana,alertmanager,loki}/{config,data}
    sudo chmod -R 755 "$MONITORING_DIR"
    
    log_success "Directories created"
}

# Generate Prometheus configuration
generate_prometheus_config() {
    log_info "Generating Prometheus configuration..."
    
    cat > "$MONITORING_DIR/prometheus/config/prometheus.yml" << EOF
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    monitor: 'lightning-db-monitor'

alerting:
  alertmanagers:
    - static_configs:
        - targets: ['localhost:$ALERTMANAGER_PORT']

rule_files:
  - /etc/prometheus/alerts/*.yml

scrape_configs:
  # Lightning DB metrics
  - job_name: 'lightning_db'
    static_configs:
      - targets: ['host.docker.internal:8080']
        labels:
          instance: 'primary'
    metrics_path: '/metrics'
    scrape_interval: 10s

  # Node exporter for system metrics
  - job_name: 'node'
    static_configs:
      - targets: ['localhost:9100']

  # Prometheus self-monitoring
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  # Grafana metrics
  - job_name: 'grafana'
    static_configs:
      - targets: ['localhost:3000']

  # Loki metrics
  - job_name: 'loki'
    static_configs:
      - targets: ['localhost:3100']
EOF

    # Generate alert rules
    mkdir -p "$MONITORING_DIR/prometheus/config/alerts"
    cat > "$MONITORING_DIR/prometheus/config/alerts/lightning_db.yml" << 'EOF'
groups:
  - name: lightning_db_alerts
    interval: 30s
    rules:
      # High latency alert
      - alert: HighLatency
        expr: lightning_db_operation_duration_seconds{quantile="0.99"} > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High operation latency detected"
          description: "P99 latency is {{ $value }}s for {{ $labels.operation }}"

      # Low cache hit rate
      - alert: LowCacheHitRate
        expr: lightning_db_cache_hit_rate < 0.8
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Cache hit rate below 80%"
          description: "Current cache hit rate: {{ $value | humanizePercentage }}"

      # High memory usage
      - alert: HighMemoryUsage
        expr: lightning_db_memory_usage_bytes / lightning_db_memory_limit_bytes > 0.9
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Memory usage above 90%"
          description: "Memory usage is at {{ $value | humanizePercentage }} of limit"

      # Database down
      - alert: DatabaseDown
        expr: up{job="lightning_db"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Lightning DB is down"
          description: "Lightning DB instance {{ $labels.instance }} has been down for more than 1 minute"

      # High error rate
      - alert: HighErrorRate
        expr: rate(lightning_db_errors_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value }} errors/sec"

      # Disk space low
      - alert: DiskSpaceLow
        expr: lightning_db_disk_free_bytes / lightning_db_disk_total_bytes < 0.1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Disk space below 10%"
          description: "Only {{ $value | humanizePercentage }} disk space remaining"
EOF

    log_success "Prometheus configuration generated"
}

# Generate Grafana configuration
generate_grafana_config() {
    log_info "Generating Grafana configuration..."
    
    # Grafana provisioning
    mkdir -p "$MONITORING_DIR/grafana/config/provisioning"/{dashboards,datasources}
    
    # Datasource configuration
    cat > "$MONITORING_DIR/grafana/config/provisioning/datasources/prometheus.yml" << EOF
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:$PROMETHEUS_PORT
    isDefault: true
    editable: true
    jsonData:
      timeInterval: "10s"

  - name: Loki
    type: loki
    access: proxy
    url: http://loki:$LOKI_PORT
    editable: true
EOF

    # Dashboard provisioning
    cat > "$MONITORING_DIR/grafana/config/provisioning/dashboards/lightning_db.yml" << EOF
apiVersion: 1

providers:
  - name: 'Lightning DB'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards
EOF

    # Generate main dashboard
    cat > "$MONITORING_DIR/grafana/config/provisioning/dashboards/lightning_db_dashboard.json" << 'EOF'
{
  "annotations": {
    "list": [
      {
        "builtIn": 1,
        "datasource": "-- Grafana --",
        "enable": true,
        "hide": true,
        "iconColor": "rgba(0, 211, 255, 1)",
        "name": "Annotations & Alerts",
        "type": "dashboard"
      }
    ]
  },
  "editable": true,
  "gnetId": null,
  "graphTooltip": 0,
  "id": null,
  "links": [],
  "panels": [
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 10,
            "gradientMode": "none",
            "hideFrom": {
              "tooltip": false,
              "viz": false,
              "legend": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "never",
            "spanNulls": true,
            "stacking": {
              "group": "A",
              "mode": "none"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "red",
                "value": 80
              }
            ]
          },
          "unit": "ops"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 0
      },
      "id": 1,
      "options": {
        "tooltip": {
          "mode": "single"
        }
      },
      "pluginVersion": "8.0.0",
      "targets": [
        {
          "expr": "rate(lightning_db_operations_total[1m])",
          "refId": "A",
          "legendFormat": "{{operation}}"
        }
      ],
      "title": "Operations Per Second",
      "type": "timeseries"
    },
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "thresholds"
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "yellow",
                "value": 0.001
              },
              {
                "color": "red",
                "value": 0.01
              }
            ]
          },
          "unit": "s"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 0
      },
      "id": 2,
      "options": {
        "orientation": "auto",
        "reduceOptions": {
          "values": false,
          "calcs": [
            "lastNotNull"
          ],
          "fields": ""
        },
        "showThresholdLabels": false,
        "showThresholdMarkers": true
      },
      "pluginVersion": "8.0.0",
      "targets": [
        {
          "expr": "lightning_db_operation_duration_seconds{quantile=\"0.99\"}",
          "refId": "A"
        }
      ],
      "title": "P99 Latency",
      "type": "gauge"
    }
  ],
  "refresh": "10s",
  "schemaVersion": 27,
  "style": "dark",
  "tags": ["lightning-db"],
  "templating": {
    "list": []
  },
  "time": {
    "from": "now-1h",
    "to": "now"
  },
  "timepicker": {},
  "timezone": "",
  "title": "Lightning DB Overview",
  "uid": "lightning-db-overview",
  "version": 0
}
EOF

    log_success "Grafana configuration generated"
}

# Generate Alertmanager configuration
generate_alertmanager_config() {
    log_info "Generating Alertmanager configuration..."
    
    cat > "$MONITORING_DIR/alertmanager/config/alertmanager.yml" << EOF
global:
  resolve_timeout: 5m

route:
  group_by: ['alertname', 'cluster', 'service']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'default'
  routes:
    - match:
        severity: critical
      receiver: critical

receivers:
  - name: 'default'
    webhook_configs:
      - url: 'http://localhost:5001/webhook'
        send_resolved: true

  - name: 'critical'
    webhook_configs:
      - url: 'http://localhost:5001/critical'
        send_resolved: true
    # Add email/slack/pagerduty configuration here

inhibit_rules:
  - source_match:
      severity: 'critical'
    target_match:
      severity: 'warning'
    equal: ['alertname', 'dev', 'instance']
EOF

    log_success "Alertmanager configuration generated"
}

# Generate Loki configuration
generate_loki_config() {
    log_info "Generating Loki configuration..."
    
    cat > "$MONITORING_DIR/loki/config/loki.yml" << EOF
auth_enabled: false

server:
  http_listen_port: $LOKI_PORT
  grpc_listen_port: 9096

common:
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory

schema_config:
  configs:
    - from: 2020-10-24
      store: boltdb-shipper
      object_store: filesystem
      schema: v11
      index:
        prefix: index_
        period: 24h

ruler:
  alertmanager_url: http://localhost:$ALERTMANAGER_PORT

analytics:
  reporting_enabled: false
EOF

    log_success "Loki configuration generated"
}

# Docker setup
setup_docker() {
    log_info "Setting up monitoring with Docker..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker not found. Please install Docker first."
        exit 1
    fi
    
    # Generate docker-compose.yml
    cat > "$MONITORING_DIR/docker-compose.yml" << EOF
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:v${PROMETHEUS_VERSION}
    container_name: lightning_prometheus
    restart: unless-stopped
    ports:
      - "${PROMETHEUS_PORT}:9090"
    volumes:
      - ./prometheus/config:/etc/prometheus
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/usr/share/prometheus/console_libraries'
      - '--web.console.templates=/usr/share/prometheus/consoles'
      - '--web.enable-lifecycle'
    extra_hosts:
      - "host.docker.internal:host-gateway"

  grafana:
    image: grafana/grafana:${GRAFANA_VERSION}
    container_name: lightning_grafana
    restart: unless-stopped
    ports:
      - "${GRAFANA_PORT}:3000"
    volumes:
      - ./grafana/config/provisioning:/etc/grafana/provisioning
      - grafana_data:/var/lib/grafana
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=lightning
      - GF_USERS_ALLOW_SIGN_UP=false
      - GF_INSTALL_PLUGINS=grafana-clock-panel,grafana-simple-json-datasource

  alertmanager:
    image: prom/alertmanager:v${ALERTMANAGER_VERSION}
    container_name: lightning_alertmanager
    restart: unless-stopped
    ports:
      - "${ALERTMANAGER_PORT}:9093"
    volumes:
      - ./alertmanager/config:/etc/alertmanager
      - alertmanager_data:/alertmanager
    command:
      - '--config.file=/etc/alertmanager/alertmanager.yml'
      - '--storage.path=/alertmanager'

  loki:
    image: grafana/loki:${LOKI_VERSION}
    container_name: lightning_loki
    restart: unless-stopped
    ports:
      - "${LOKI_PORT}:3100"
    volumes:
      - ./loki/config:/etc/loki
      - loki_data:/loki
    command: -config.file=/etc/loki/loki.yml

  node-exporter:
    image: prom/node-exporter:latest
    container_name: lightning_node_exporter
    restart: unless-stopped
    ports:
      - "9100:9100"
    pid: host
    command:
      - '--path.rootfs=/host'
      - '--path.procfs=/host/proc'
      - '--path.sysfs=/host/sys'
      - '--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)'
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro

volumes:
  prometheus_data:
  grafana_data:
  alertmanager_data:
  loki_data:
EOF

    # Start services
    cd "$MONITORING_DIR"
    docker-compose up -d
    
    log_success "Docker monitoring stack deployed"
}

# Native installation
setup_native() {
    log_info "Setting up native monitoring installation..."
    
    # This would involve downloading and installing each component
    # For brevity, showing the structure
    log_warning "Native installation requires manual setup of each component"
    log_info "Configuration files have been generated in: $MONITORING_DIR"
    
    cat << EOF

To complete native installation:

1. Download and install Prometheus:
   wget https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}/prometheus-${PROMETHEUS_VERSION}.linux-amd64.tar.gz
   
2. Download and install Grafana:
   wget https://dl.grafana.com/oss/release/grafana-${GRAFANA_VERSION}.linux-amd64.tar.gz
   
3. Download and install Alertmanager:
   wget https://github.com/prometheus/alertmanager/releases/download/v${ALERTMANAGER_VERSION}/alertmanager-${ALERTMANAGER_VERSION}.linux-amd64.tar.gz
   
4. Download and install Loki:
   wget https://github.com/grafana/loki/releases/download/v${LOKI_VERSION}/loki-linux-amd64.zip

5. Use the generated configuration files in $MONITORING_DIR

EOF
}

# Kubernetes setup
setup_kubernetes() {
    log_info "Setting up monitoring in Kubernetes..."
    
    # Generate Kubernetes manifests
    cat > "$MONITORING_DIR/k8s-monitoring.yaml" << EOF
# This would contain full Kubernetes manifests
# For production, consider using Prometheus Operator instead
---
apiVersion: v1
kind: Namespace
metadata:
  name: lightning-monitoring
---
# Add full manifests here...
EOF

    log_info "Kubernetes manifests generated in: $MONITORING_DIR/k8s-monitoring.yaml"
    log_info "For production, consider using Prometheus Operator:"
    echo "  https://github.com/prometheus-operator/prometheus-operator"
}

# Post-setup tasks
post_setup_tasks() {
    log_info "Running post-setup tasks..."
    
    # Wait for services to start
    sleep 10
    
    # Test connectivity
    if [[ "$INSTALL_METHOD" == "docker" ]]; then
        log_info "Testing service connectivity..."
        
        # Test Prometheus
        if curl -s "http://localhost:$PROMETHEUS_PORT/-/healthy" > /dev/null; then
            log_success "Prometheus is running"
        else
            log_warning "Prometheus health check failed"
        fi
        
        # Test Grafana
        if curl -s "http://localhost:$GRAFANA_PORT/api/health" > /dev/null; then
            log_success "Grafana is running"
        else
            log_warning "Grafana health check failed"
        fi
    fi
    
    log_success "Post-setup tasks completed"
}

# Main function
main() {
    echo -e "${BLUE}Lightning DB Monitoring Setup${NC}"
    echo "============================="
    
    # Create directories
    create_directories
    
    # Generate configurations
    generate_prometheus_config
    generate_grafana_config
    generate_alertmanager_config
    generate_loki_config
    
    # Setup based on method
    case $INSTALL_METHOD in
        docker)
            setup_docker
            ;;
        native)
            setup_native
            ;;
        k8s|kubernetes)
            setup_kubernetes
            ;;
        *)
            log_error "Invalid installation method: $INSTALL_METHOD"
            exit 1
            ;;
    esac
    
    # Post-setup
    post_setup_tasks
    
    # Print summary
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}     Monitoring Setup Complete!                     ${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
    echo ""
    echo "Access URLs:"
    echo "  Prometheus: http://localhost:$PROMETHEUS_PORT"
    echo "  Grafana: http://localhost:$GRAFANA_PORT (admin/lightning)"
    echo "  Alertmanager: http://localhost:$ALERTMANAGER_PORT"
    echo "  Loki: http://localhost:$LOKI_PORT"
    echo ""
    echo "Next Steps:"
    echo "1. Configure Lightning DB to expose metrics on port 8080"
    echo "2. Import additional dashboards from Grafana.com"
    echo "3. Configure alert notifications in Alertmanager"
    echo "4. Set up log shipping to Loki"
    echo ""
    echo "Documentation: $PROJECT_ROOT/MONITORING_AND_ALERTING_GUIDE.md"
}

# Run main
main "$@"