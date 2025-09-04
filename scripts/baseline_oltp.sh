#!/usr/bin/env bash
set -euo pipefail

DATE_DIR="benchmark_results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$DATE_DIR"

echo "Running OLTP baseline (examples/oltp_basic_benchmark)..."
OUT_TXT="$DATE_DIR/oltp_basic_benchmark.txt"
JSON="$DATE_DIR/oltp_basic_benchmark.json"

# Faster dev settings
export RUST_LOG=warn

run_with_timeout() {
  local timeout_s=${1:-120}
  shift
  ( "$@" & pid=$!; (
      sleep "$timeout_s" && echo "Timeout ${timeout_s}s reached; killing $pid" >&2 && kill -TERM $pid 2>/dev/null
    ) & watcher=$!; 
    wait $pid; status=$?; kill -TERM $watcher 2>/dev/null; wait $watcher 2>/dev/null; exit $status )
}

if run_with_timeout 180 cargo run --quiet --features integration_tests --example oltp_basic_benchmark | tee "$OUT_TXT"; then
  echo "Parsing output into JSON..."
  # Simple parser for the printed table
  awk '
    BEGIN{print "{\n  \"operations\": ["}
    /^\| [^|]+\|/ {
      # Split by pipe
      nfields=split($0, a, /\|/);
      # a[2]= op, a[3]= throughput, a[4]= p50, a[5]= p95, a[6]= p99 columns with padding
      op=a[2]; thr=a[3]; p50=a[4]; p95=a[5]; p99=a[6];
      # trim spaces
      sub(/^ +/,"",op); sub(/ +$/,"",op);
      sub(/^ +/,"",thr); sub(/ +$/,"",thr);
      sub(/^ +/,"",p50); sub(/ +$/,"",p50);
      sub(/^ +/,"",p95); sub(/ +$/,"",p95);
      sub(/^ +/,"",p99); sub(/ +$/,"",p99);
      if(op=="Operation") next;
      if(n++) print ",";
      printf "    {\"name\": \"%s\", \"throughput_ops\": %s, \"p50_us\": %s, \"p95_us\": %s, \"p99_us\": %s}", op, thr, p50, p95, p99;
    }
    END{print "\n  ]\n}"}
  ' "$OUT_TXT" > "$JSON" || true
  echo "Artifacts saved to $DATE_DIR"
else
  echo "OLTP baseline run failed" >&2
  exit 1
fi
