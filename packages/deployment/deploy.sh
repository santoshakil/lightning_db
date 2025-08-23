#!/bin/bash
set -euo pipefail

# Lightning DB Deployment Script
# Usage: ./deploy.sh <environment> [options]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/tmp/lightning-db-deploy.log"

# Default values
ENVIRONMENT=""
NAMESPACE="lightning-db"
DRY_RUN=false
SKIP_BACKUP=false
FORCE_DEPLOY=false
IMAGE_TAG=""
TIMEOUT=600

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case "$level" in
        "INFO")
            echo -e "${GREEN}[INFO]${NC} $message" | tee -a "$LOG_FILE"
            ;;
        "WARN")
            echo -e "${YELLOW}[WARN]${NC} $message" | tee -a "$LOG_FILE"
            ;;
        "ERROR")
            echo -e "${RED}[ERROR]${NC} $message" | tee -a "$LOG_FILE"
            ;;
        "DEBUG")
            echo -e "${BLUE}[DEBUG]${NC} $message" | tee -a "$LOG_FILE"
            ;;
    esac
    echo "$timestamp [$level] $message" >> "$LOG_FILE"
}

# Error handling
error_exit() {
    log "ERROR" "$1"
    exit 1
}

# Usage information
usage() {
    cat << EOF
Usage: $0 <environment> [options]

Environments:
  dev         Deploy to development environment
  staging     Deploy to staging environment
  prod        Deploy to production environment

Options:
  --namespace=NAME      Kubernetes namespace (default: lightning-db)
  --image-tag=TAG       Docker image tag to deploy
  --dry-run            Show what would be deployed without applying
  --skip-backup        Skip pre-deployment backup (not recommended for prod)
  --force              Force deployment even with warnings
  --timeout=SECONDS    Deployment timeout in seconds (default: 600)
  --help              Show this help message

Examples:
  $0 staging --image-tag=v1.2.3
  $0 prod --dry-run
  $0 dev --namespace=lightning-db-dev
EOF
}

# Parse command line arguments
parse_args() {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi
    
    ENVIRONMENT="$1"
    shift
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --namespace=*)
                NAMESPACE="${1#*=}"
                shift
                ;;
            --image-tag=*)
                IMAGE_TAG="${1#*=}"
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --skip-backup)
                SKIP_BACKUP=true
                shift
                ;;
            --force)
                FORCE_DEPLOY=true
                shift
                ;;
            --timeout=*)
                TIMEOUT="${1#*=}"
                shift
                ;;
            --help)
                usage
                exit 0
                ;;
            *)
                error_exit "Unknown option: $1"
                ;;
        esac
    done
}

# Validate environment
validate_environment() {
    case "$ENVIRONMENT" in
        "dev"|"staging"|"prod")
            log "INFO" "Deploying to $ENVIRONMENT environment"
            ;;
        *)
            error_exit "Invalid environment: $ENVIRONMENT. Must be dev, staging, or prod"
            ;;
    esac
}

# Check prerequisites
check_prerequisites() {
    local deps=("kubectl" "docker" "helm")
    
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            error_exit "Required dependency '$dep' not found"
        fi
    done
    
    # Check kubectl context
    local current_context
    current_context=$(kubectl config current-context 2>/dev/null || echo "none")
    log "INFO" "Current kubectl context: $current_context"
    
    # Verify cluster connectivity
    if ! kubectl cluster-info &> /dev/null; then
        error_exit "Cannot connect to Kubernetes cluster"
    fi
    
    log "INFO" "Prerequisites check passed"
}

# Create namespace if it doesn't exist
create_namespace() {
    if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
        log "INFO" "Creating namespace: $NAMESPACE"
        if [[ "$DRY_RUN" == "false" ]]; then
            kubectl create namespace "$NAMESPACE"
        fi
    else
        log "INFO" "Namespace $NAMESPACE already exists"
    fi
}

# Apply configurations
apply_configs() {
    local config_dir="$SCRIPT_DIR/configs/$ENVIRONMENT"
    
    if [[ ! -d "$config_dir" ]]; then
        error_exit "Configuration directory not found: $config_dir"
    fi
    
    log "INFO" "Applying configurations for $ENVIRONMENT"
    
    # Create configmap from environment-specific config
    local config_file="$config_dir/lightning_db.toml"
    if [[ -f "$config_file" ]]; then
        if [[ "$DRY_RUN" == "false" ]]; then
            kubectl create configmap lightning-db-config \
                --from-file="$config_file" \
                --namespace="$NAMESPACE" \
                --dry-run=client -o yaml | kubectl apply -f -
        else
            log "INFO" "[DRY RUN] Would create configmap from $config_file"
        fi
    fi
}

# Backup current deployment
backup_deployment() {
    if [[ "$SKIP_BACKUP" == "true" ]]; then
        log "WARN" "Skipping backup as requested"
        return
    fi
    
    if [[ "$ENVIRONMENT" == "prod" ]]; then
        log "INFO" "Creating backup before production deployment"
        
        if [[ "$DRY_RUN" == "false" ]]; then
            # Trigger backup via API if pods are running
            local pods
            pods=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=lightning-db --no-headers 2>/dev/null | wc -l)
            
            if [[ "$pods" -gt 0 ]]; then
                log "INFO" "Triggering database backup"
                kubectl exec -n "$NAMESPACE" deployment/lightning-db -- /app/scripts/backup.sh full || {
                    log "WARN" "Backup failed, but continuing deployment"
                }
            else
                log "INFO" "No running pods found, skipping backup"
            fi
        else
            log "INFO" "[DRY RUN] Would create backup"
        fi
    fi
}

# Build and push image if needed
build_image() {
    if [[ -n "$IMAGE_TAG" ]]; then
        log "INFO" "Using specified image tag: $IMAGE_TAG"
        return
    fi
    
    # Generate image tag from git commit
    if command -v git &> /dev/null && git rev-parse --git-dir > /dev/null 2>&1; then
        local commit_hash
        commit_hash=$(git rev-parse --short HEAD)
        IMAGE_TAG="$commit_hash"
        log "INFO" "Generated image tag from git: $IMAGE_TAG"
    else
        IMAGE_TAG="latest"
        log "WARN" "Using 'latest' tag - not recommended for production"
    fi
    
    if [[ "$ENVIRONMENT" == "prod" && "$IMAGE_TAG" == "latest" && "$FORCE_DEPLOY" == "false" ]]; then
        error_exit "Cannot deploy 'latest' tag to production without --force flag"
    fi
}

# Apply Kubernetes manifests
apply_manifests() {
    local k8s_dir="$SCRIPT_DIR/kubernetes"
    
    log "INFO" "Applying Kubernetes manifests"
    
    # Apply in specific order
    local manifest_order=(
        "namespace.yaml"
        "configmap.yaml"
        "secrets.yaml"
        "rbac.yaml"
        "pvc.yaml"
        "deployment.yaml"
        "service.yaml"
    )
    
    for manifest in "${manifest_order[@]}"; do
        local manifest_file="$k8s_dir/$manifest"
        
        if [[ -f "$manifest_file" ]]; then
            log "INFO" "Applying $manifest"
            
            if [[ "$DRY_RUN" == "false" ]]; then
                # Replace placeholders in manifests
                sed -e "s/{{NAMESPACE}}/$NAMESPACE/g" \
                    -e "s/{{IMAGE_TAG}}/$IMAGE_TAG/g" \
                    -e "s/{{ENVIRONMENT}}/$ENVIRONMENT/g" \
                    "$manifest_file" | kubectl apply -f - --namespace="$NAMESPACE"
            else
                log "INFO" "[DRY RUN] Would apply $manifest"
            fi
        else
            log "WARN" "Manifest file not found: $manifest_file"
        fi
    done
}

# Update deployment image
update_deployment_image() {
    if [[ -n "$IMAGE_TAG" ]]; then
        log "INFO" "Updating deployment image to: ghcr.io/lightning-db/lightning-db:$IMAGE_TAG"
        
        if [[ "$DRY_RUN" == "false" ]]; then
            kubectl set image deployment/lightning-db \
                lightning-db="ghcr.io/lightning-db/lightning-db:$IMAGE_TAG" \
                --namespace="$NAMESPACE"
        else
            log "INFO" "[DRY RUN] Would update image to $IMAGE_TAG"
        fi
    fi
}

# Wait for deployment to complete
wait_for_deployment() {
    log "INFO" "Waiting for deployment to complete (timeout: ${TIMEOUT}s)"
    
    if [[ "$DRY_RUN" == "false" ]]; then
        if ! kubectl rollout status deployment/lightning-db \
            --namespace="$NAMESPACE" \
            --timeout="${TIMEOUT}s"; then
            error_exit "Deployment failed or timed out"
        fi
        
        # Wait for pods to be ready
        if ! kubectl wait --for=condition=ready pod \
            -l app.kubernetes.io/name=lightning-db \
            --namespace="$NAMESPACE" \
            --timeout="${TIMEOUT}s"; then
            error_exit "Pods failed to become ready"
        fi
        
        log "INFO" "Deployment completed successfully"
    else
        log "INFO" "[DRY RUN] Would wait for deployment completion"
    fi
}

# Verify deployment
verify_deployment() {
    log "INFO" "Verifying deployment"
    
    if [[ "$DRY_RUN" == "false" ]]; then
        # Check pod status
        local ready_pods
        ready_pods=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=lightning-db --no-headers | grep "Running" | wc -l)
        
        if [[ "$ready_pods" -eq 0 ]]; then
            error_exit "No running pods found after deployment"
        fi
        
        log "INFO" "Found $ready_pods running pods"
        
        # Health check
        local health_check_passed=false
        for i in {1..10}; do
            if kubectl exec -n "$NAMESPACE" deployment/lightning-db -- curl -f http://localhost:8080/health &> /dev/null; then
                health_check_passed=true
                break
            fi
            log "INFO" "Health check attempt $i/10..."
            sleep 5
        done
        
        if [[ "$health_check_passed" == "true" ]]; then
            log "INFO" "Health check passed"
        else
            error_exit "Health check failed after 10 attempts"
        fi
        
        # Verify metrics endpoint
        if kubectl exec -n "$NAMESPACE" deployment/lightning-db -- curl -f http://localhost:9090/metrics &> /dev/null; then
            log "INFO" "Metrics endpoint verified"
        else
            log "WARN" "Metrics endpoint not responding"
        fi
        
    else
        log "INFO" "[DRY RUN] Would verify deployment"
    fi
}

# Rollback deployment on failure
rollback_deployment() {
    log "ERROR" "Deployment failed, initiating rollback"
    
    if [[ "$DRY_RUN" == "false" ]]; then
        kubectl rollout undo deployment/lightning-db --namespace="$NAMESPACE"
        kubectl rollout status deployment/lightning-db --namespace="$NAMESPACE" --timeout=300s
        log "INFO" "Rollback completed"
    else
        log "INFO" "[DRY RUN] Would rollback deployment"
    fi
}

# Generate deployment report
generate_report() {
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    cat << EOF | tee -a "$LOG_FILE"

=====================================
Lightning DB Deployment Report
=====================================
Environment: $ENVIRONMENT
Namespace: $NAMESPACE
Image Tag: $IMAGE_TAG
Start Time: $(date -d "@$start_time" '+%Y-%m-%d %H:%M:%S')
End Time: $(date -d "@$end_time" '+%Y-%m-%d %H:%M:%S')
Duration: ${duration}s
Status: SUCCESS
Dry Run: $DRY_RUN

Next Steps:
1. Monitor application metrics
2. Check application logs
3. Verify functionality
4. Update monitoring dashboards
=====================================
EOF
}

# Cleanup function
cleanup() {
    log "INFO" "Cleaning up temporary files"
    # Add cleanup logic here if needed
}

# Main execution
main() {
    local start_time=$(date +%s)
    
    log "INFO" "Starting Lightning DB deployment"
    
    # Set up error handling
    trap 'error_exit "Deployment interrupted"' INT TERM
    trap cleanup EXIT
    
    # Parse and validate arguments
    parse_args "$@"
    validate_environment
    
    # Pre-deployment checks
    check_prerequisites
    
    # Deployment process
    create_namespace
    apply_configs
    backup_deployment
    build_image
    apply_manifests
    update_deployment_image
    
    # Wait and verify
    if ! wait_for_deployment; then
        rollback_deployment
        error_exit "Deployment failed and rollback completed"
    fi
    
    verify_deployment
    
    # Generate report
    generate_report
    
    log "INFO" "Deployment completed successfully!"
    
    # Show next steps
    if [[ "$DRY_RUN" == "false" ]]; then
        echo ""
        echo "Access your deployment:"
        echo "  kubectl get services -n $NAMESPACE"
        echo "  kubectl logs -f deployment/lightning-db -n $NAMESPACE"
        echo ""
        echo "Monitor your deployment:"
        echo "  kubectl top pods -n $NAMESPACE"
        echo "  kubectl describe deployment lightning-db -n $NAMESPACE"
    fi
}

# Run main function with all arguments
main "$@"