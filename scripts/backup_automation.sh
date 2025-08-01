#!/bin/bash

# Lightning DB Backup Automation Script
#
# This script automates backup operations for Lightning DB
# Supports: Full backups, Incremental backups, Remote storage, Retention policies

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default configuration
DB_PATH="${DB_PATH:-/var/lib/lightning_db/data}"
BACKUP_DIR="${BACKUP_DIR:-/var/backups/lightning_db}"
BACKUP_TYPE="${BACKUP_TYPE:-full}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
REMOTE_STORAGE="${REMOTE_STORAGE:-}"
NOTIFICATION_EMAIL="${NOTIFICATION_EMAIL:-}"
LOG_FILE="${LOG_FILE:-/var/log/lightning_db_backup.log}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Functions
print_usage() {
    cat << EOF
Lightning DB Backup Automation

Usage: $0 [OPTIONS]

Options:
    -d, --db-path PATH       Database path [default: /var/lib/lightning_db/data]
    -b, --backup-dir DIR     Backup directory [default: /var/backups/lightning_db]
    -t, --type TYPE          Backup type (full|incremental|differential) [default: full]
    -r, --retention DAYS     Retention period in days [default: 7]
    -s, --remote STORAGE     Remote storage (s3://bucket/path, gs://bucket/path, etc.)
    -e, --email EMAIL        Email for notifications
    -c, --cron               Install as cron job
    -h, --help               Show this help message

Examples:
    # Full backup
    $0 --type full

    # Incremental backup with S3 upload
    $0 --type incremental --remote s3://my-bucket/lightning-backups

    # Install daily backup cron job
    $0 --cron --type full --retention 30

EOF
}

log_info() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $1"
    echo -e "${BLUE}${msg}${NC}"
    echo "$msg" >> "$LOG_FILE"
}

log_success() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: $1"
    echo -e "${GREEN}${msg}${NC}"
    echo "$msg" >> "$LOG_FILE"
}

log_error() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1"
    echo -e "${RED}${msg}${NC}" >&2
    echo "$msg" >> "$LOG_FILE"
}

log_warning() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $1"
    echo -e "${YELLOW}${msg}${NC}"
    echo "$msg" >> "$LOG_FILE"
}

# Parse arguments
INSTALL_CRON=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--db-path)
            DB_PATH="$2"
            shift 2
            ;;
        -b|--backup-dir)
            BACKUP_DIR="$2"
            shift 2
            ;;
        -t|--type)
            BACKUP_TYPE="$2"
            shift 2
            ;;
        -r|--retention)
            RETENTION_DAYS="$2"
            shift 2
            ;;
        -s|--remote)
            REMOTE_STORAGE="$2"
            shift 2
            ;;
        -e|--email)
            NOTIFICATION_EMAIL="$2"
            shift 2
            ;;
        -c|--cron)
            INSTALL_CRON=true
            shift
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

# Create required directories
mkdir -p "$BACKUP_DIR"
mkdir -p "$(dirname "$LOG_FILE")"
touch "$LOG_FILE"

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check if database exists
    if [[ ! -d "$DB_PATH" ]]; then
        log_error "Database path not found: $DB_PATH"
        exit 1
    fi

    # Check Lightning DB tools
    if ! command -v lightning_db &> /dev/null; then
        log_error "lightning_db command not found. Please ensure Lightning DB is installed."
        exit 1
    fi

    # Check remote storage tools if needed
    if [[ -n "$REMOTE_STORAGE" ]]; then
        if [[ "$REMOTE_STORAGE" == s3://* ]] && ! command -v aws &> /dev/null; then
            log_error "AWS CLI not found. Please install aws-cli for S3 backups."
            exit 1
        elif [[ "$REMOTE_STORAGE" == gs://* ]] && ! command -v gsutil &> /dev/null; then
            log_error "gsutil not found. Please install Google Cloud SDK for GCS backups."
            exit 1
        fi
    fi

    log_success "Prerequisites check passed"
}

# Get last backup info
get_last_backup_info() {
    local last_backup_file="$BACKUP_DIR/.last_backup"
    if [[ -f "$last_backup_file" ]]; then
        cat "$last_backup_file"
    else
        echo "none"
    fi
}

# Save backup info
save_backup_info() {
    local backup_name="$1"
    local backup_type="$2"
    echo "$backup_name|$backup_type|$TIMESTAMP" > "$BACKUP_DIR/.last_backup"
}

# Perform full backup
perform_full_backup() {
    local backup_name="lightning_db_full_${TIMESTAMP}"
    local backup_path="$BACKUP_DIR/$backup_name"
    
    log_info "Starting full backup to $backup_path..."
    
    # Create backup using Lightning DB backup tool
    if lightning_db backup \
        --source "$DB_PATH" \
        --destination "$backup_path" \
        --type full \
        --compress \
        --verify 2>&1 | tee -a "$LOG_FILE"; then
        
        # Calculate backup size
        local backup_size=$(du -sh "$backup_path" | cut -f1)
        log_success "Full backup completed: $backup_name (Size: $backup_size)"
        
        # Save backup metadata
        cat > "$backup_path.meta" << EOF
{
    "timestamp": "$TIMESTAMP",
    "type": "full",
    "size": "$backup_size",
    "db_path": "$DB_PATH",
    "version": "$(lightning_db --version)"
}
EOF
        
        save_backup_info "$backup_name" "full"
        echo "$backup_path"
    else
        log_error "Full backup failed"
        return 1
    fi
}

# Perform incremental backup
perform_incremental_backup() {
    local last_backup=$(get_last_backup_info | cut -d'|' -f1)
    if [[ "$last_backup" == "none" ]]; then
        log_warning "No previous backup found. Performing full backup instead."
        perform_full_backup
        return
    fi
    
    local backup_name="lightning_db_incr_${TIMESTAMP}"
    local backup_path="$BACKUP_DIR/$backup_name"
    local base_backup="$BACKUP_DIR/$last_backup"
    
    log_info "Starting incremental backup based on $last_backup..."
    
    if lightning_db backup \
        --source "$DB_PATH" \
        --destination "$backup_path" \
        --type incremental \
        --base "$base_backup" \
        --compress \
        --verify 2>&1 | tee -a "$LOG_FILE"; then
        
        local backup_size=$(du -sh "$backup_path" | cut -f1)
        log_success "Incremental backup completed: $backup_name (Size: $backup_size)"
        
        save_backup_info "$backup_name" "incremental"
        echo "$backup_path"
    else
        log_error "Incremental backup failed"
        return 1
    fi
}

# Upload to remote storage
upload_to_remote() {
    local backup_path="$1"
    
    if [[ -z "$REMOTE_STORAGE" ]]; then
        return 0
    fi
    
    log_info "Uploading backup to remote storage: $REMOTE_STORAGE"
    
    local backup_file=$(basename "$backup_path")
    local remote_path="$REMOTE_STORAGE/$backup_file"
    
    if [[ "$REMOTE_STORAGE" == s3://* ]]; then
        if aws s3 cp "$backup_path" "$remote_path" \
            --storage-class GLACIER_IR \
            --metadata "timestamp=$TIMESTAMP,type=$BACKUP_TYPE"; then
            log_success "Uploaded to S3: $remote_path"
            
            # Also upload metadata
            aws s3 cp "$backup_path.meta" "$remote_path.meta"
        else
            log_error "Failed to upload to S3"
            return 1
        fi
        
    elif [[ "$REMOTE_STORAGE" == gs://* ]]; then
        if gsutil -m cp "$backup_path" "$remote_path"; then
            log_success "Uploaded to GCS: $remote_path"
            gsutil cp "$backup_path.meta" "$remote_path.meta"
        else
            log_error "Failed to upload to GCS"
            return 1
        fi
        
    elif [[ "$REMOTE_STORAGE" == rsync://* ]]; then
        local rsync_dest="${REMOTE_STORAGE#rsync://}"
        if rsync -avz --progress "$backup_path" "$backup_path.meta" "$rsync_dest/"; then
            log_success "Uploaded via rsync to: $rsync_dest"
        else
            log_error "Failed to upload via rsync"
            return 1
        fi
    fi
}

# Clean old backups
cleanup_old_backups() {
    log_info "Cleaning up backups older than $RETENTION_DAYS days..."
    
    # Local cleanup
    find "$BACKUP_DIR" -name "lightning_db_*" -type f -mtime +$RETENTION_DAYS -delete
    find "$BACKUP_DIR" -name "*.meta" -type f -mtime +$RETENTION_DAYS -delete
    
    # Remote cleanup for S3
    if [[ "$REMOTE_STORAGE" == s3://* ]]; then
        local bucket_path="${REMOTE_STORAGE#s3://}"
        aws s3 ls "$REMOTE_STORAGE/" | while read -r line; do
            local file_date=$(echo "$line" | awk '{print $1}')
            local file_name=$(echo "$line" | awk '{print $4}')
            
            if [[ -n "$file_name" ]]; then
                local file_age=$(( ($(date +%s) - $(date -d "$file_date" +%s)) / 86400 ))
                if [[ $file_age -gt $RETENTION_DAYS ]]; then
                    aws s3 rm "$REMOTE_STORAGE/$file_name"
                    log_info "Deleted old backup: $file_name"
                fi
            fi
        done
    fi
    
    log_success "Cleanup completed"
}

# Send notification
send_notification() {
    local status="$1"
    local message="$2"
    
    if [[ -z "$NOTIFICATION_EMAIL" ]]; then
        return 0
    fi
    
    local subject="Lightning DB Backup $status - $(hostname)"
    
    if command -v mail &> /dev/null; then
        echo "$message" | mail -s "$subject" "$NOTIFICATION_EMAIL"
    elif command -v sendmail &> /dev/null; then
        {
            echo "To: $NOTIFICATION_EMAIL"
            echo "Subject: $subject"
            echo ""
            echo "$message"
        } | sendmail "$NOTIFICATION_EMAIL"
    else
        log_warning "No mail command found. Cannot send notification."
    fi
}

# Install cron job
install_cron_job() {
    log_info "Installing cron job for automated backups..."
    
    local cron_schedule="0 2 * * *"  # Daily at 2 AM
    local cron_command="$0 --db-path '$DB_PATH' --backup-dir '$BACKUP_DIR' --type '$BACKUP_TYPE' --retention '$RETENTION_DAYS'"
    
    if [[ -n "$REMOTE_STORAGE" ]]; then
        cron_command="$cron_command --remote '$REMOTE_STORAGE'"
    fi
    
    if [[ -n "$NOTIFICATION_EMAIL" ]]; then
        cron_command="$cron_command --email '$NOTIFICATION_EMAIL'"
    fi
    
    # Add to crontab
    (crontab -l 2>/dev/null | grep -v "lightning_db.*backup"; echo "$cron_schedule $cron_command >> $LOG_FILE 2>&1") | crontab -
    
    log_success "Cron job installed. Backups will run daily at 2 AM."
    echo "View cron jobs: crontab -l"
    echo "Edit schedule: crontab -e"
}

# Main backup function
perform_backup() {
    local backup_start=$(date +%s)
    local backup_path=""
    local backup_status="SUCCESS"
    local error_message=""
    
    # Start backup based on type
    case $BACKUP_TYPE in
        full)
            backup_path=$(perform_full_backup) || backup_status="FAILED"
            ;;
        incremental)
            backup_path=$(perform_incremental_backup) || backup_status="FAILED"
            ;;
        differential)
            log_error "Differential backup not yet implemented"
            backup_status="FAILED"
            ;;
        *)
            log_error "Invalid backup type: $BACKUP_TYPE"
            backup_status="FAILED"
            ;;
    esac
    
    # Upload to remote if successful
    if [[ "$backup_status" == "SUCCESS" ]] && [[ -n "$backup_path" ]]; then
        if ! upload_to_remote "$backup_path"; then
            backup_status="PARTIAL"
            error_message="Backup successful but remote upload failed"
        fi
    fi
    
    # Cleanup old backups
    cleanup_old_backups
    
    # Calculate duration
    local backup_end=$(date +%s)
    local duration=$((backup_end - backup_start))
    
    # Generate report
    local report="Lightning DB Backup Report
============================
Date: $(date)
Type: $BACKUP_TYPE
Status: $backup_status
Duration: ${duration}s
Database: $DB_PATH
Backup: ${backup_path:-N/A}
Remote: ${REMOTE_STORAGE:-None}
${error_message:+Error: $error_message}

Disk Usage:
$(df -h "$BACKUP_DIR" | tail -1)

Recent Backups:
$(ls -lh "$BACKUP_DIR"/lightning_db_* 2>/dev/null | tail -5 || echo "No backups found")
"
    
    # Log report
    echo "$report" | tee -a "$LOG_FILE"
    
    # Send notification
    send_notification "$backup_status" "$report"
    
    # Exit with appropriate code
    [[ "$backup_status" == "SUCCESS" ]] && exit 0 || exit 1
}

# Main execution
main() {
    echo -e "${BLUE}Lightning DB Backup Automation${NC}"
    echo "==============================="
    
    # Check prerequisites
    check_prerequisites
    
    # Install cron if requested
    if [[ "$INSTALL_CRON" == true ]]; then
        install_cron_job
        exit 0
    fi
    
    # Perform backup
    perform_backup
}

# Run main function
main "$@"