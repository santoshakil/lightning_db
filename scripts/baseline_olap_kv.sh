#!/usr/bin/env bash
set -euo pipefail

DATE_DIR="benchmark_results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$DATE_DIR"
OUT_TXT="$DATE_DIR/olap_kv_scan.txt"
JSON="$DATE_DIR/olap_kv_scan.json"

run_with_timeout() {
  local timeout_s=${1:-120}
  shift
  ( "$@" & pid=$!; (
      sleep "$timeout_s" && echo "Timeout ${timeout_s}s reached; killing $pid" >&2 && kill -TERM $pid 2>/dev/null
    ) & watcher=$!; 
    wait $pid; status=$?; kill -TERM $watcher 2>/dev/null; wait $watcher 2>/dev/null; exit $status )
}

echo "Running OLAP KV baseline (examples/olap_kv_scan)..."
if OLAP_ROWS=200000 OLAP_VALUE_BYTES=128 run_with_timeout 240 cargo run --quiet --example olap_kv_scan | tee "$OUT_TXT"; then
  awk '
    BEGIN{print "{"}
    /rows:/ {print "  \"rows\": " $2 ","}
    /value_bytes:/ {print "  \"value_bytes\": " $2 ","}
    /load_ms:/ {print "  \"load_ms\": " $2 ","}
    /scan_ms:/ {print "  \"scan_ms\": " $2 ","}
    /scanned_rows:/ {print "  \"scanned_rows\": " $2 ","}
    /scanned_bytes:/ {print "  \"scanned_bytes\": " $2 ","}
    /mb_per_s:/ {print "  \"mb_per_s\": " $2}
    END{print "}"}
  ' "$OUT_TXT" > "$JSON" || true
  echo "Artifacts saved to $DATE_DIR"
else
  echo "OLAP KV baseline run failed" >&2
  exit 1
fi

