#!/usr/bin/env bash
set -euo pipefail

DATE_DIR="benchmark_results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$DATE_DIR"
OUT_TXT="$DATE_DIR/olap_scan.txt"
JSON="$DATE_DIR/olap_scan.json"

run_with_timeout() {
  local timeout_s=${1:-120}
  shift
  ( "$@" & pid=$!; (
      sleep "$timeout_s" && echo "Timeout ${timeout_s}s reached; killing $pid" >&2 && kill -TERM $pid 2>/dev/null
    ) & watcher=$!; 
    wait $pid; status=$?; kill -TERM $watcher 2>/dev/null; wait $watcher 2>/dev/null; exit $status )
}

echo "Running OLAP baseline (examples/olap_scan)..."
if OLAP_ROWS=300000 run_with_timeout 180 cargo run --quiet --example olap_scan | tee "$OUT_TXT"; then
  awk '
    BEGIN{print "{"}
    /rows:/ {print "  \"rows\": " $2 ","}
    /load_ms:/ {print "  \"load_ms\": " $2 ","}
    /scan_ms:/ {print "  \"scan_ms\": " $2 ","}
    /agg_ms:/ {print "  \"agg_ms\": " $2 ","}
    /sum_qty/ {print "  \"sum_qty_over_3\": " $2}
    END{print "}"}
  ' "$OUT_TXT" > "$JSON" || true
  echo "Artifacts saved to $DATE_DIR"
else
  echo "OLAP baseline run failed" >&2
  exit 1
fi

