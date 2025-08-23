#!/bin/bash
set -euo pipefail

# Lightning DB Restore Script
# Usage: ./restore.sh <backup_file> [target_data_dir] [--point-in-time=timestamp]

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/lightning-db-restore.log"

# Default values
BACKUP_FILE="${1:-}"
TARGET_DATA_DIR="${2:-/app/data}"
ENCRYPTION_KEY_FILE="${ENCRYPTION_KEY_FILE:-/app/secrets/backup-encryption-key}"
POINT_IN_TIME=""

# Parse command line arguments
for arg in "$@"; do
    case $arg in
        --point-in-time=*)
            POINT_IN_TIME="${arg#*=}"
            shift
            ;;
    esac
done

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$$] $*" | tee -a "$LOG_FILE"
}

# Error handling
error_exit() {
    log "ERROR: $1"
    exit 1
}

# Usage information
usage() {
    cat << EOF
Usage: $0 <backup_file> [target_data_dir] [options]

Arguments:
  backup_file       Path to backup file or S3/GCS URL
  target_data_dir   Target directory for restored data (default: /app/data)

Options:
  --point-in-time=TIMESTAMP   Restore to specific point in time (YYYY-MM-DD HH:MM:SS)

Examples:
  $0 /backups/lightning_db_full_server1_20231201_120000.tar.gz
  $0 s3://my-bucket/backups/lightning_db_full_server1_20231201_120000.tar.gz.enc
  $0 /backups/backup.tar.gz /app/data --point-in-time="2023-12-01 10:30:00"
EOF
}

# Check dependencies
check_dependencies() {
    local deps=("tar" "gzip" "openssl")
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            error_exit "Required dependency '$dep' not found"
        fi
    done
    
    # Check cloud tools if needed
    if [[ "$BACKUP_FILE" == s3://* ]] && ! command -v aws &> /dev/null; then
        error_exit "AWS CLI required for S3 backups but not found"
    fi
    
    if [[ "$BACKUP_FILE" == gs://* ]] && ! command -v gsutil &> /dev/null; then
        error_exit "Google Cloud SDK required for GCS backups but not found"
    fi
}

# Validate inputs
validate_inputs() {
    if [[ -z "$BACKUP_FILE" ]]; then
        usage
        error_exit "Backup file must be specified"
    fi
    
    if [[ ! -d "$(dirname "$TARGET_DATA_DIR")" ]]; then
        error_exit "Parent directory of target data directory does not exist: $(dirname "$TARGET_DATA_DIR")"
    fi
}

# Stop Lightning DB service
stop_database() {
    log "Stopping Lightning DB service..."
    
    # Try different methods to stop the service
    if command -v systemctl &> /dev/null; then
        systemctl stop lightning-db || log "systemctl stop failed, continuing..."
    fi
    
    # Try stopping via admin API
    if command -v curl &> /dev/null; then
        curl -s -X POST "http://localhost:8080/admin/shutdown" || log "API shutdown failed, continuing..."
    fi
    
    # Wait for process to stop
    sleep 5
    
    # Force kill if still running
    pkill -f lightning-db || log "No lightning-db processes found"
    
    log "Database service stopped"
}

# Start Lightning DB service
start_database() {
    log "Starting Lightning DB service..."
    
    if command -v systemctl &> /dev/null; then
        systemctl start lightning-db || error_exit "Failed to start Lightning DB service"
    else
        log "Manual start required - systemctl not available"
    fi
    
    # Wait for service to be ready
    local max_attempts=30
    local attempt=0
    
    while [[ $attempt -lt $max_attempts ]]; do
        if curl -s -f "http://localhost:8080/health" &> /dev/null; then
            log "Database service is ready"
            return 0
        fi
        
        log "Waiting for database service to be ready... (attempt $((attempt + 1))/$max_attempts)"
        sleep 2
        ((attempt++))
    done
    
    error_exit "Database service failed to start within expected time"
}

# Download backup from cloud storage
download_backup() {
    local backup_url="$1"
    local local_path="$2"
    
    log "Downloading backup from cloud storage: $backup_url"
    
    case "$backup_url" in
        s3://*)
            aws s3 cp "$backup_url" "$local_path" \
                || error_exit "Failed to download from S3"
            ;;
        gs://*)
            gsutil cp "$backup_url" "$local_path" \
                || error_exit "Failed to download from GCS"
            ;;
        *)
            error_exit "Unsupported backup URL format: $backup_url"
            ;;
    esac
    
    log "Download completed: $local_path"
}

# Decrypt backup if encrypted
decrypt_backup() {
    local encrypted_file="$1"
    local decrypted_file="$2"
    
    if [[ ! -f "$ENCRYPTION_KEY_FILE" ]]; then
        error_exit "Encryption key file not found: $ENCRYPTION_KEY_FILE"
    fi
    
    log "Decrypting backup..."
    openssl enc -aes-256-cbc -d -pbkdf2 \
        -in "$encrypted_file" \
        -out "$decrypted_file" \
        -pass "file:$ENCRYPTION_KEY_FILE" \
        || error_exit "Failed to decrypt backup"
    
    log "Backup decrypted successfully"
}

# Verify backup integrity
verify_backup() {
    local backup_file="$1"
    
    log "Verifying backup integrity: $backup_file"
    
    if ! tar -tzf "$backup_file" > /dev/null 2>&1; then
        error_exit "Backup file is corrupted or invalid"
    fi
    
    log "Backup verification successful"
}

# Create backup of current data
backup_current_data() {
    local data_dir="$1"
    local backup_dir="$2"
    
    if [[ ! -d "$data_dir" ]]; then
        log "No existing data directory to backup"
        return
    fi
    
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local backup_name="pre_restore_backup_${timestamp}.tar.gz"
    local backup_path="${backup_dir}/${backup_name}"
    
    log "Creating backup of current data..."
    
    mkdir -p "$backup_dir"
    tar -czf "$backup_path" -C "$(dirname "$data_dir")" "$(basename "$data_dir")" \
        || error_exit "Failed to backup current data"
    
    log "Current data backed up to: $backup_path"
}

# Extract backup to target directory
extract_backup() {
    local backup_file="$1"
    local target_dir="$2"
    
    log "Extracting backup to: $target_dir"
    
    # Remove existing data directory
    if [[ -d "$target_dir" ]]; then
        rm -rf "$target_dir"
    fi
    
    # Create parent directory
    mkdir -p "$(dirname "$target_dir")"
    
    # Extract backup
    tar -xzf "$backup_file" -C "$(dirname "$target_dir")" \
        || error_exit "Failed to extract backup"
    
    # Set proper permissions
    chown -R 1000:1000 "$target_dir" 2>/dev/null || log "Could not set ownership (non-root execution?)"
    chmod -R 755 "$target_dir"
    
    log "Backup extracted successfully"
}

# Apply point-in-time recovery
apply_point_in_time_recovery() {
    local target_dir="$1"
    local pit_timestamp="$2"
    
    log "Applying point-in-time recovery to: $pit_timestamp"
    
    # Convert timestamp to epoch
    local target_epoch
    target_epoch=$(date -d "$pit_timestamp" +%s) || error_exit "Invalid timestamp format"
    
    # Find and apply WAL files up to the target time
    local wal_dir="${target_dir}/wal"
    if [[ -d "$wal_dir" ]]; then
        find "$wal_dir" -name "*.wal" -type f | while read -r wal_file; do
            local wal_epoch
            wal_epoch=$(stat -c %Y "$wal_file")
            
            if [[ $wal_epoch -gt $target_epoch ]]; then
                log "Removing WAL file created after target time: $wal_file"
                rm "$wal_file"
            fi
        done
    fi
    
    log "Point-in-time recovery applied"
}

# Validate restored data
validate_restored_data() {
    local data_dir="$1"
    
    log "Validating restored data..."
    
    # Check if data directory exists and has content
    if [[ ! -d "$data_dir" ]]; then
        error_exit "Data directory not found after restore: $data_dir"
    fi
    
    if [[ -z "$(ls -A "$data_dir" 2>/dev/null)" ]]; then
        error_exit "Data directory is empty after restore"
    fi
    
    # Check for critical files
    local critical_files=("metadata" "index")
    for file in "${critical_files[@]}"; do
        if [[ ! -e "$data_dir/$file" ]]; then
            log "WARNING: Critical file not found: $file"
        fi
    done
    
    log "Data validation completed"
}

# Generate restore report
generate_report() {
    local backup_file="$1"
    local target_dir="$2"
    local start_time="$3"
    local end_time="$4"
    local pit_timestamp="$5"
    
    local duration=$((end_time - start_time))
    
    cat << EOF
Lightning DB Restore Report
==========================
Backup File: $backup_file
Target Directory: $target_dir
Point-in-Time: ${pit_timestamp:-"N/A"}
Start Time: $(date -d "@$start_time" '+%Y-%m-%d %H:%M:%S')
End Time: $(date -d "@$end_time" '+%Y-%m-%d %H:%M:%S')
Duration: ${duration}s
Status: SUCCESS

Next Steps:
1. Verify application connectivity
2. Run data consistency checks
3. Update monitoring systems
4. Notify stakeholders of restore completion
EOF
}

# Main execution
main() {
    local start_time=$(date +%s)
    
    log "Starting Lightning DB restore process"
    log "Backup file: $BACKUP_FILE"
    log "Target directory: $TARGET_DATA_DIR"
    log "Point-in-time: ${POINT_IN_TIME:-"N/A"}"
    
    # Validate inputs
    validate_inputs
    
    # Check dependencies
    check_dependencies
    
    # Determine local backup file path
    local local_backup_file="$BACKUP_FILE"
    
    # Download from cloud if necessary
    if [[ "$BACKUP_FILE" =~ ^(s3|gs):// ]]; then
        local temp_dir=$(mktemp -d)
        local_backup_file="${temp_dir}/$(basename "$BACKUP_FILE")"
        download_backup "$BACKUP_FILE" "$local_backup_file"
    fi
    
    # Decrypt if encrypted
    if [[ "$local_backup_file" == *.enc ]]; then
        local decrypted_file="${local_backup_file%.enc}"
        decrypt_backup "$local_backup_file" "$decrypted_file"
        local_backup_file="$decrypted_file"
    fi
    
    # Verify backup integrity
    verify_backup "$local_backup_file"
    
    # Stop database service
    stop_database
    
    # Backup current data
    backup_current_data "$TARGET_DATA_DIR" "/tmp/lightning-db-backups"
    
    # Extract backup
    extract_backup "$local_backup_file" "$TARGET_DATA_DIR"
    
    # Apply point-in-time recovery if requested
    if [[ -n "$POINT_IN_TIME" ]]; then
        apply_point_in_time_recovery "$TARGET_DATA_DIR" "$POINT_IN_TIME"
    fi
    
    # Validate restored data
    validate_restored_data "$TARGET_DATA_DIR"
    
    # Start database service
    start_database
    
    # Cleanup temporary files
    if [[ "$BACKUP_FILE" =~ ^(s3|gs):// ]]; then
        rm -rf "$(dirname "$local_backup_file")"
    fi
    
    local end_time=$(date +%s)
    
    # Generate report
    generate_report "$BACKUP_FILE" "$TARGET_DATA_DIR" "$start_time" "$end_time" "$POINT_IN_TIME"
    
    log "Restore process completed successfully"
}

# Show usage if no arguments provided
if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

# Run main function
main "$@"