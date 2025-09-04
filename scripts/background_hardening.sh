#!/usr/bin/env bash
set -euo pipefail

# Continuous hardening loop: runs checks repeatedly and logs output.
# Usage: nohup scripts/background_hardening.sh > logs/hardening_stdout.log 2>&1 &

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

START_TS=$(date +%s)
MAX_SECONDS=$((5 * 60 * 60)) # 5 hours

echo "[hardening] start=$(date -Is) root=$ROOT_DIR pid=$$" | tee -a "$LOG_DIR/hardening.meta.log"

run_step() {
  local name="$1"; shift
  echo "[hardening] step=$name start=$(date -Is)" | tee -a "$LOG_DIR/hardening.steps.log"
  ( time "$@" ) \
    >  "$LOG_DIR/${name}.out.log" \
    2> "$LOG_DIR/${name}.err.log" || true
  echo "[hardening] step=$name end=$(date -Is)" | tee -a "$LOG_DIR/hardening.steps.log"
}

iteration=0
while :; do
  now=$(date +%s)
  elapsed=$(( now - START_TS ))
  if (( elapsed > MAX_SECONDS )); then
    echo "[hardening] reached time budget; exiting." | tee -a "$LOG_DIR/hardening.meta.log"
    break
  fi

  iteration=$((iteration+1))
  echo "[hardening] iteration=$iteration at=$(date -Is)" | tee -a "$LOG_DIR/hardening.meta.log"

  # Fast checks first
  run_step "cargo_check_all"          bash -lc 'cargo check --all-targets --all-features'
  run_step "cargo_clippy_all"         bash -lc 'cargo clippy --all-targets --all-features || true'

  # Unit + fast integration tests; avoid benches by default
  run_step "cargo_test"               bash -lc 'RUST_TEST_THREADS=1 cargo test --all-features -- --nocapture || true'

  # Optional docs build for public API sanity
  run_step "cargo_doc"                bash -lc 'cargo doc --no-deps --all-features || true'

  # Snapshot status
  git status --porcelain=v1 > "$LOG_DIR/git_status_${iteration}.txt" 2>/dev/null || true

  # Small idle to avoid hammering the system
  sleep 10
done

echo "[hardening] done at $(date -Is)" | tee -a "$LOG_DIR/hardening.meta.log"

