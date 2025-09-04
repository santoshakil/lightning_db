#!/usr/bin/env bash
set -euo pipefail

DATE_DIR="snapshots/$(date +%Y%m%d_%H%M%S)"
DB_PATH="$DATE_DIR/db"
BACKUP_PATH="$DATE_DIR/backup"
mkdir -p "$DATE_DIR"

run_with_timeout() {
  local timeout_s=${1:-120}
  shift
  ( "$@" & pid=$!; (
      sleep "$timeout_s" && echo "Timeout ${timeout_s}s reached; killing $pid" >&2 && kill -TERM $pid 2>/dev/null
    ) & watcher=$!; 
    wait $pid; status=$?; kill -TERM $watcher 2>/dev/null; wait $watcher 2>/dev/null; exit $status )
}

echo "Creating DB at $DB_PATH"
run_with_timeout 60 cargo run --quiet --features cli --bin lightning-cli -- create "$DB_PATH" --cache-size 32 --compression || true

echo "Seeding keys..."
for i in $(seq -w 00001 01000); do
  run_with_timeout 5 cargo run --quiet --features cli --bin lightning-cli -- put "$DB_PATH" "seed_$i" "value_$i" >/dev/null || true
done

echo "Running backup to $BACKUP_PATH"
run_with_timeout 120 cargo run --quiet --features cli --bin lightning-cli -- backup "$DB_PATH" "$BACKUP_PATH" --compress zstd || true

echo "Snapshot created: $DATE_DIR"
