#!/bin/bash

# Lightning DB Production Deployment Script
#
# This script automates the deployment of Lightning DB to production environments
# Supports: Linux, macOS, Docker, Kubernetes

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_VERSION="${DEPLOY_VERSION:-latest}"
DEPLOY_ENV="${DEPLOY_ENV:-production}"
DEPLOY_TARGET="${DEPLOY_TARGET:-local}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Functions
print_banner() {
    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════════════╗"
    echo "║       Lightning DB Production Deployment       ║"
    echo "║                Version: $DEPLOY_VERSION              ║"
    echo "╚════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
    -t, --target TARGET     Deployment target (local|docker|k8s|systemd) [default: local]
    -e, --env ENV          Environment (production|staging|test) [default: production]
    -v, --version VERSION  Version to deploy [default: latest]
    -c, --config FILE      Configuration file path
    -d, --data-dir DIR     Data directory path [default: /var/lib/lightning_db]
    -p, --port PORT        Service port [default: 8080]
    -h, --help             Show this help message

Examples:
    # Deploy locally as systemd service
    $0 --target systemd --data-dir /data/lightning_db

    # Deploy to Docker
    $0 --target docker --version 1.0.0

    # Deploy to Kubernetes
    $0 --target k8s --config production.yaml
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
DEPLOY_CONFIG=""
DATA_DIR="/var/lib/lightning_db"
SERVICE_PORT="8080"

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--target)
            DEPLOY_TARGET="$2"
            shift 2
            ;;
        -e|--env)
            DEPLOY_ENV="$2"
            shift 2
            ;;
        -v|--version)
            DEPLOY_VERSION="$2"
            shift 2
            ;;
        -c|--config)
            DEPLOY_CONFIG="$2"
            shift 2
            ;;
        -d|--data-dir)
            DATA_DIR="$2"
            shift 2
            ;;
        -p|--port)
            SERVICE_PORT="$2"
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

print_banner

# Validate deployment target
case $DEPLOY_TARGET in
    local|docker|k8s|systemd)
        ;;
    *)
        log_error "Invalid deployment target: $DEPLOY_TARGET"
        print_usage
        exit 1
        ;;
esac

# Pre-deployment checks
pre_deployment_checks() {
    log_info "Running pre-deployment checks..."

    # Check if Lightning DB binary exists
    if [[ ! -f "$PROJECT_ROOT/target/release/lightning_db" ]]; then
        log_error "Lightning DB binary not found. Please build first with: cargo build --release"
        exit 1
    fi

    # Run validation tests
    if [[ -f "$PROJECT_ROOT/scripts/run_production_validation.sh" ]]; then
        log_info "Running production validation..."
        if ! "$PROJECT_ROOT/scripts/run_production_validation.sh" > /tmp/lightning_validation.log 2>&1; then
            log_error "Production validation failed. Check /tmp/lightning_validation.log"
            exit 1
        fi
        log_success "Production validation passed"
    fi

    # Check system requirements
    AVAILABLE_MEM=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}' || echo "0")
    if [[ $AVAILABLE_MEM -lt 1024 ]]; then
        log_warning "Low memory detected: ${AVAILABLE_MEM}MB. Recommended: 1024MB+"
    fi

    log_success "Pre-deployment checks completed"
}

# Deploy locally
deploy_local() {
    log_info "Deploying Lightning DB locally to $DATA_DIR..."

    # Create data directory
    sudo mkdir -p "$DATA_DIR"
    sudo chown "$(whoami)" "$DATA_DIR"

    # Copy binary
    cp "$PROJECT_ROOT/target/release/lightning_db" /usr/local/bin/
    chmod +x /usr/local/bin/lightning_db

    # Create configuration
    cat > "$DATA_DIR/lightning_db.conf" << EOF
# Lightning DB Configuration
data_path = "$DATA_DIR/data"
log_path = "$DATA_DIR/logs"
port = $SERVICE_PORT
environment = "$DEPLOY_ENV"

[cache]
size = 1073741824  # 1GB

[wal]
sync_mode = "sync"

[monitoring]
enabled = true
prometheus_port = 9090
EOF

    log_success "Local deployment completed"
    echo "Start with: lightning_db --config $DATA_DIR/lightning_db.conf"
}

# Deploy as systemd service
deploy_systemd() {
    log_info "Deploying Lightning DB as systemd service..."

    # Deploy locally first
    deploy_local

    # Create systemd service file
    sudo tee /etc/systemd/system/lightning_db.service > /dev/null << EOF
[Unit]
Description=Lightning DB High-Performance Database
After=network.target
Documentation=https://github.com/example/lightning_db

[Service]
Type=notify
ExecStart=/usr/local/bin/lightning_db --config $DATA_DIR/lightning_db.conf
ExecReload=/bin/kill -HUP \$MAINPID
Restart=on-failure
RestartSec=5
User=lightning_db
Group=lightning_db

# Security
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR

# Resource limits
LimitNOFILE=65535
LimitMEMLOCK=infinity

# Performance
CPUAccounting=true
MemoryAccounting=true
IOAccounting=true

[Install]
WantedBy=multi-user.target
EOF

    # Create user
    if ! id lightning_db &>/dev/null; then
        sudo useradd --system --home-dir "$DATA_DIR" --shell /bin/false lightning_db
    fi
    sudo chown -R lightning_db:lightning_db "$DATA_DIR"

    # Enable and start service
    sudo systemctl daemon-reload
    sudo systemctl enable lightning_db.service
    sudo systemctl start lightning_db.service

    log_success "Systemd service deployed"
    echo "Service status: systemctl status lightning_db"
    echo "View logs: journalctl -u lightning_db -f"
}

# Deploy to Docker
deploy_docker() {
    log_info "Building and deploying Lightning DB Docker container..."

    # Build Docker image
    cd "$PROJECT_ROOT"
    docker build -t "lightning_db:$DEPLOY_VERSION" .

    # Create Docker volume
    docker volume create lightning_db_data

    # Run container
    docker run -d \
        --name lightning_db \
        --restart unless-stopped \
        -p "$SERVICE_PORT:8080" \
        -p "9090:9090" \
        -v lightning_db_data:/data \
        -v "$DATA_DIR/config:/config:ro" \
        --memory="2g" \
        --cpus="2.0" \
        "lightning_db:$DEPLOY_VERSION"

    log_success "Docker deployment completed"
    echo "Container status: docker ps -a | grep lightning_db"
    echo "View logs: docker logs -f lightning_db"
}

# Deploy to Kubernetes
deploy_k8s() {
    log_info "Deploying Lightning DB to Kubernetes..."

    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Please install kubectl first."
        exit 1
    fi

    # Create namespace
    kubectl create namespace lightning-db --dry-run=client -o yaml | kubectl apply -f -

    # Generate Kubernetes manifests
    cat > /tmp/lightning-db-k8s.yaml << EOF
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: lightning-db-config
  namespace: lightning-db
data:
  lightning_db.conf: |
    data_path = "/data"
    log_path = "/logs"
    port = 8080
    environment = "$DEPLOY_ENV"
    
    [cache]
    size = 2147483648  # 2GB
    
    [monitoring]
    enabled = true
    prometheus_port = 9090

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: lightning-db-data
  namespace: lightning-db
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
  storageClassName: fast-ssd

---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: lightning-db
  namespace: lightning-db
spec:
  serviceName: lightning-db
  replicas: 1
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
        image: lightning_db:$DEPLOY_VERSION
        ports:
        - containerPort: 8080
          name: api
        - containerPort: 9090
          name: metrics
        volumeMounts:
        - name: data
          mountPath: /data
        - name: config
          mountPath: /config
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
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
      volumes:
      - name: config
        configMap:
          name: lightning-db-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi
      storageClassName: fast-ssd

---
apiVersion: v1
kind: Service
metadata:
  name: lightning-db
  namespace: lightning-db
spec:
  selector:
    app: lightning-db
  ports:
  - name: api
    port: 8080
    targetPort: 8080
  - name: metrics
    port: 9090
    targetPort: 9090
  type: ClusterIP

---
apiVersion: v1
kind: Service
metadata:
  name: lightning-db-external
  namespace: lightning-db
spec:
  selector:
    app: lightning-db
  ports:
  - name: api
    port: 8080
    targetPort: 8080
  type: LoadBalancer
EOF

    # Apply Kubernetes manifests
    kubectl apply -f /tmp/lightning-db-k8s.yaml

    log_success "Kubernetes deployment completed"
    echo "Check deployment: kubectl get all -n lightning-db"
    echo "View logs: kubectl logs -n lightning-db -l app=lightning-db -f"
}

# Post-deployment tasks
post_deployment_tasks() {
    log_info "Running post-deployment tasks..."

    # Set up monitoring
    case $DEPLOY_TARGET in
        systemd|local)
            cat > /tmp/prometheus-lightning-db.yml << EOF
  - job_name: 'lightning_db'
    static_configs:
    - targets: ['localhost:9090']
      labels:
        environment: '$DEPLOY_ENV'
EOF
            log_info "Add the above to your Prometheus configuration"
            ;;
        docker)
            log_info "Prometheus endpoint available at: http://localhost:9090/metrics"
            ;;
        k8s)
            # Create ServiceMonitor for Prometheus Operator
            cat > /tmp/lightning-db-servicemonitor.yaml << EOF
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: lightning-db
  namespace: lightning-db
spec:
  selector:
    matchLabels:
      app: lightning-db
  endpoints:
  - port: metrics
    interval: 30s
EOF
            kubectl apply -f /tmp/lightning-db-servicemonitor.yaml 2>/dev/null || \
                log_warning "ServiceMonitor not created. Prometheus Operator may not be installed."
            ;;
    esac

    # Verify deployment
    log_info "Verifying deployment..."
    sleep 5

    case $DEPLOY_TARGET in
        systemd)
            if systemctl is-active --quiet lightning_db; then
                log_success "Service is running"
            else
                log_error "Service failed to start"
                systemctl status lightning_db
                exit 1
            fi
            ;;
        docker)
            if docker ps | grep -q lightning_db; then
                log_success "Container is running"
            else
                log_error "Container failed to start"
                docker logs lightning_db
                exit 1
            fi
            ;;
        k8s)
            kubectl wait --for=condition=ready pod -n lightning-db -l app=lightning-db --timeout=60s
            log_success "Pods are ready"
            ;;
    esac

    log_success "Post-deployment tasks completed"
}

# Main deployment flow
main() {
    # Run pre-deployment checks
    pre_deployment_checks

    # Deploy based on target
    case $DEPLOY_TARGET in
        local)
            deploy_local
            ;;
        systemd)
            deploy_systemd
            ;;
        docker)
            deploy_docker
            ;;
        k8s)
            deploy_k8s
            ;;
    esac

    # Run post-deployment tasks
    post_deployment_tasks

    # Print summary
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}     Lightning DB Deployment Successful!           ${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
    echo ""
    echo "Deployment Summary:"
    echo "  Target: $DEPLOY_TARGET"
    echo "  Environment: $DEPLOY_ENV"
    echo "  Version: $DEPLOY_VERSION"
    echo "  Data Directory: $DATA_DIR"
    echo "  Service Port: $SERVICE_PORT"
    echo ""
    echo "Next Steps:"
    echo "1. Monitor the service health"
    echo "2. Configure backup procedures"
    echo "3. Set up alerting rules"
    echo "4. Test with your application"
    echo ""
    echo "Documentation: $PROJECT_ROOT/DOCUMENTATION_INDEX.md"
}

# Run main function
main "$@"