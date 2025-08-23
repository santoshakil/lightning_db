#!/bin/bash
set -euo pipefail

# Lightning DB Backup Script
# Usage: ./backup.sh [full|incremental] [target_location]

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/backup.conf"
LOG_FILE="/var/log/lightning-db-backup.log"

# Default values
BACKUP_TYPE="${1:-incremental}"
TARGET_LOCATION="${2:-/backups/lightning-db}"
DATA_DIR="${LIGHTNING_DB_DATA_DIR:-/app/data}"
ENCRYPTION_KEY_FILE="${ENCRYPTION_KEY_FILE:-/app/secrets/backup-encryption-key}"
RETENTION_DAYS="${RETENTION_DAYS:-30}"
COMPRESSION_LEVEL="${COMPRESSION_LEVEL:-6}"

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$$] $*" | tee -a "$LOG_FILE"
}

# Error handling
error_exit() {
    log "ERROR: $1"
    exit 1
}

# Check dependencies
check_dependencies() {
    local deps=("tar" "gzip" "openssl" "aws" "kubectl")
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            error_exit "Required dependency '$dep' not found"
        fi
    done
}

# Create backup directory
setup_backup_dir() {
    local backup_dir="$1"
    mkdir -p "$backup_dir" || error_exit "Failed to create backup directory: $backup_dir"
    
    # Set proper permissions
    chmod 750 "$backup_dir"
    log "Backup directory ready: $backup_dir"
}

# Generate backup filename
generate_backup_filename() {
    local backup_type="$1"
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local hostname=$(hostname)
    echo "lightning_db_${backup_type}_${hostname}_${timestamp}"
}

# Create database checkpoint
create_checkpoint() {
    log "Creating database checkpoint..."
    
    # Send checkpoint command to Lightning DB admin API
    if command -v curl &> /dev/null; then
        curl -s -X POST "http://localhost:8080/admin/checkpoint" || {
            log "WARNING: Failed to create checkpoint via API, continuing anyway"
        }
    fi
    
    # Wait for any pending writes to complete
    sleep 5
}

# Perform full backup
full_backup() {
    local backup_dir="$1"
    local backup_name="$2"
    
    log "Starting full backup: $backup_name"
    
    create_checkpoint
    
    # Create full backup archive
    local archive_path="${backup_dir}/${backup_name}.tar.gz"
    
    tar -czf "$archive_path" \
        -C "$(dirname "$DATA_DIR")" \
        "$(basename "$DATA_DIR")" \
        --exclude="*.tmp" \
        --exclude="*.lock" \
        || error_exit "Failed to create backup archive"
    
    # Encrypt backup if encryption key is available
    if [[ -f "$ENCRYPTION_KEY_FILE" ]]; then
        log "Encrypting backup..."
        openssl enc -aes-256-cbc -salt -pbkdf2 \
            -in "$archive_path" \
            -out "${archive_path}.enc" \
            -pass "file:$ENCRYPTION_KEY_FILE" \
            || error_exit "Failed to encrypt backup"
        
        # Remove unencrypted backup
        rm "$archive_path"
        archive_path="${archive_path}.enc"
    fi
    
    log "Full backup completed: $archive_path"
    echo "$archive_path"
}

# Perform incremental backup
incremental_backup() {
    local backup_dir="$1"
    local backup_name="$2"
    
    log "Starting incremental backup: $backup_name"
    
    # Find last full backup
    local last_full_backup
    last_full_backup=$(find "$backup_dir" -name "*_full_*" -type f | sort -r | head -1)
    
    if [[ -z "$last_full_backup" ]]; then
        log "No full backup found, performing full backup instead"
        full_backup "$backup_dir" "$backup_name"
        return
    fi
    
    log "Using reference backup: $last_full_backup"
    
    create_checkpoint
    
    # Create incremental backup using rsync-style approach
    local archive_path="${backup_dir}/${backup_name}.tar.gz"
    local temp_dir=$(mktemp -d)
    
    # Find files newer than last backup
    local reference_time
    reference_time=$(stat -c %Y "$last_full_backup")
    
    find "$DATA_DIR" -type f -newer "$last_full_backup" \
        -not -name "*.tmp" -not -name "*.lock" \
        -exec cp --parents {} "$temp_dir" \;
    
    if [[ -n "$(ls -A "$temp_dir" 2>/dev/null)" ]]; then
        tar -czf "$archive_path" -C "$temp_dir" . \
            || error_exit "Failed to create incremental backup"
        
        # Encrypt if needed
        if [[ -f "$ENCRYPTION_KEY_FILE" ]]; then
            log "Encrypting incremental backup..."
            openssl enc -aes-256-cbc -salt -pbkdf2 \
                -in "$archive_path" \
                -out "${archive_path}.enc" \
                -pass "file:$ENCRYPTION_KEY_FILE" \
                || error_exit "Failed to encrypt backup"
            
            rm "$archive_path"
            archive_path="${archive_path}.enc"
        fi
        
        log "Incremental backup completed: $archive_path"
    else
        log "No changes since last backup, skipping"
        rm -f "$archive_path"
        archive_path=""
    fi
    
    # Cleanup temp directory
    rm -rf "$temp_dir"
    
    echo "$archive_path"
}

# Upload backup to cloud storage
upload_backup() {
    local backup_file="$1"
    local cloud_target="$2"
    
    if [[ -z "$backup_file" || ! -f "$backup_file" ]]; then
        log "No backup file to upload"
        return
    fi
    
    log "Uploading backup to cloud storage: $cloud_target"
    
    case "$cloud_target" in
        s3://*)
            aws s3 cp "$backup_file" "$cloud_target/" \
                --storage-class STANDARD_IA \
                || error_exit "Failed to upload to S3"
            ;;
        gs://*)
            gsutil cp "$backup_file" "$cloud_target/" \
                || error_exit "Failed to upload to GCS"
            ;;
        *)
            log "WARNING: Unknown cloud target format: $cloud_target"
            ;;
    esac
    
    log "Upload completed successfully"
}

# Cleanup old backups
cleanup_old_backups() {
    local backup_dir="$1"
    local retention_days="$2"
    
    log "Cleaning up backups older than $retention_days days"
    
    find "$backup_dir" -name "lightning_db_*" -type f -mtime "+$retention_days" -delete
    
    # Cleanup cloud storage as well
    if [[ -n "${CLOUD_BACKUP_TARGET:-}" ]]; then
        case "$CLOUD_BACKUP_TARGET" in
            s3://*)
                aws s3 ls "$CLOUD_BACKUP_TARGET/" --recursive | \
                awk -v date="$(date -d "$retention_days days ago" '+%Y-%m-%d')" '$1 < date {print $4}' | \
                xargs -I {} aws s3 rm "s3://$CLOUD_BACKUP_TARGET/{}"
                ;;
        esac
    fi
    
    log "Cleanup completed"
}

# Verify backup integrity
verify_backup() {
    local backup_file="$1"
    
    log "Verifying backup integrity: $backup_file"
    
    if [[ "$backup_file" == *.enc ]]; then
        # Verify encrypted backup
        openssl enc -aes-256-cbc -d -pbkdf2 \
            -in "$backup_file" \
            -pass "file:$ENCRYPTION_KEY_FILE" | \
            tar -tzf - > /dev/null \
            || error_exit "Backup verification failed"
    else
        # Verify unencrypted backup
        tar -tzf "$backup_file" > /dev/null \
            || error_exit "Backup verification failed"
    fi
    
    log "Backup verification successful"
}

# Generate backup report
generate_report() {
    local backup_file="$1"
    local backup_type="$2"
    local start_time="$3"
    local end_time="$4"
    
    local duration=$((end_time - start_time))
    local size=""
    
    if [[ -f "$backup_file" ]]; then
        size=$(du -h "$backup_file" | cut -f1)
    fi
    
    cat << EOF
Lightning DB Backup Report
=========================
Backup Type: $backup_type
Start Time: $(date -d "@$start_time" '+%Y-%m-%d %H:%M:%S')
End Time: $(date -d "@$end_time" '+%Y-%m-%d %H:%M:%S')
Duration: ${duration}s
Backup File: $backup_file
File Size: $size
Status: SUCCESS
EOF
}

# Main execution
main() {
    local start_time=$(date +%s)
    
    log "Starting Lightning DB backup process"
    log "Backup type: $BACKUP_TYPE"
    log "Target location: $TARGET_LOCATION"
    
    # Check dependencies
    check_dependencies
    
    # Setup backup directory
    setup_backup_dir "$TARGET_LOCATION"
    
    # Generate backup filename
    local backup_name
    backup_name=$(generate_backup_filename "$BACKUP_TYPE")
    
    # Perform backup based on type
    local backup_file=""
    case "$BACKUP_TYPE" in
        "full")
            backup_file=$(full_backup "$TARGET_LOCATION" "$backup_name")
            ;;
        "incremental")
            backup_file=$(incremental_backup "$TARGET_LOCATION" "$backup_name")
            ;;
        *)
            error_exit "Unknown backup type: $BACKUP_TYPE"
            ;;
    esac
    
    # Verify backup if created
    if [[ -n "$backup_file" && -f "$backup_file" ]]; then
        verify_backup "$backup_file"
        
        # Upload to cloud storage if configured
        if [[ -n "${CLOUD_BACKUP_TARGET:-}" ]]; then
            upload_backup "$backup_file" "$CLOUD_BACKUP_TARGET"
        fi
    fi
    
    # Cleanup old backups
    cleanup_old_backups "$TARGET_LOCATION" "$RETENTION_DAYS"
    
    local end_time=$(date +%s)
    
    # Generate report
    generate_report "$backup_file" "$BACKUP_TYPE" "$start_time" "$end_time"
    
    log "Backup process completed successfully"
}

# Load configuration if available
if [[ -f "$CONFIG_FILE" ]]; then
    source "$CONFIG_FILE"
fi

# Run main function
main "$@"