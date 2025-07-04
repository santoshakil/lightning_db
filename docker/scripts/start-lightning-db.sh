#!/bin/bash
set -euo pipefail

# Lightning DB Production Startup Script
echo "🚀 Starting Lightning DB in production mode..."

# Configuration
DB_PATH="${LIGHTNING_DB_PATH:-/data/db}"
CACHE_SIZE="${LIGHTNING_DB_CACHE_SIZE:-256MB}"
PORT="${LIGHTNING_DB_PORT:-8080}"
LOG_LEVEL="${LIGHTNING_DB_LOG_LEVEL:-info}"
BACKUP_DIR="${LIGHTNING_DB_BACKUP_DIR:-/data/backups}"

# Ensure data directories exist
mkdir -p "$DB_PATH" "$BACKUP_DIR" /data/logs

# Convert cache size to bytes
case "$CACHE_SIZE" in
    *MB) CACHE_BYTES=$((${CACHE_SIZE%MB} * 1024 * 1024)) ;;
    *GB) CACHE_BYTES=$((${CACHE_SIZE%GB} * 1024 * 1024 * 1024)) ;;
    *) CACHE_BYTES="$CACHE_SIZE" ;;
esac

echo "📊 Configuration:"
echo "  Database Path: $DB_PATH"
echo "  Cache Size: $CACHE_SIZE ($CACHE_BYTES bytes)"
echo "  Port: $PORT"
echo "  Log Level: $LOG_LEVEL"
echo "  Backup Directory: $BACKUP_DIR"

# Health check function
health_check() {
    if ! curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1; then
        echo "❌ Health check failed"
        return 1
    fi
    return 0
}

# Graceful shutdown handler
shutdown() {
    echo "🛑 Received shutdown signal, gracefully stopping Lightning DB..."
    
    if [ -n "${ADMIN_PID:-}" ]; then
        echo "📊 Stopping admin server (PID: $ADMIN_PID)..."
        kill -TERM "$ADMIN_PID" 2>/dev/null || true
        wait "$ADMIN_PID" 2>/dev/null || true
    fi
    
    echo "✅ Lightning DB stopped gracefully"
    exit 0
}

# Set up signal handlers
trap shutdown SIGTERM SIGINT

# Pre-startup checks
echo "🔍 Running pre-startup checks..."

# Check disk space
AVAILABLE_SPACE=$(df "$DB_PATH" | awk 'NR==2 {print $4}')
MIN_SPACE=1048576  # 1GB in KB
if [ "$AVAILABLE_SPACE" -lt "$MIN_SPACE" ]; then
    echo "❌ Insufficient disk space: ${AVAILABLE_SPACE}KB available, ${MIN_SPACE}KB required"
    exit 1
fi
echo "✅ Disk space check passed: ${AVAILABLE_SPACE}KB available"

# Check memory
AVAILABLE_MEM=$(free | awk 'NR==2{print $7}')
MIN_MEM=524288  # 512MB in KB
if [ "$AVAILABLE_MEM" -lt "$MIN_MEM" ]; then
    echo "⚠️  Warning: Low available memory: ${AVAILABLE_MEM}KB available, ${MIN_MEM}KB recommended"
fi
echo "✅ Memory check completed: ${AVAILABLE_MEM}KB available"

# Create database if it doesn't exist
if [ ! -d "$DB_PATH/btree.db" ]; then
    echo "🆕 Creating new database at $DB_PATH..."
    lightning-cli create "$DB_PATH" --cache-size "${CACHE_SIZE%MB}"
    echo "✅ Database created successfully"
else
    echo "📂 Using existing database at $DB_PATH"
    
    # Run integrity check on existing database
    echo "🔍 Running integrity check..."
    if lightning-cli check "$DB_PATH" --verbose; then
        echo "✅ Database integrity check passed"
    else
        echo "⚠️  Database integrity check found issues, but continuing..."
    fi
fi

# Start admin server
echo "🖥️  Starting Lightning DB admin server..."
lightning-admin-server "$DB_PATH" --port "$PORT" --create &
ADMIN_PID=$!

# Wait for server to start
echo "⏳ Waiting for server to start..."
for i in {1..30}; do
    if health_check; then
        echo "✅ Lightning DB admin server started successfully (PID: $ADMIN_PID)"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Failed to start Lightning DB admin server"
        kill "$ADMIN_PID" 2>/dev/null || true
        exit 1
    fi
    sleep 1
done

# Display startup information
echo ""
echo "🎉 Lightning DB is ready!"
echo "📊 Admin API: http://localhost:$PORT"
echo "🏥 Health endpoint: http://localhost:$PORT/health"
echo "📈 Status endpoint: http://localhost:$PORT/status"
echo ""

# Start monitoring loop
echo "📊 Starting health monitoring..."
while true; do
    sleep 30
    
    if ! health_check; then
        echo "❌ Health check failed at $(date)"
        # In production, you might want to send alerts here
    else
        echo "💚 Health check passed at $(date)"
    fi
    
    # Check if admin server is still running
    if ! kill -0 "$ADMIN_PID" 2>/dev/null; then
        echo "❌ Admin server process died, exiting..."
        exit 1
    fi
done