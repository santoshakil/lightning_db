#!/bin/bash

echo "🧪 Lightning DB Comprehensive Test Suite"
echo "========================================"
echo "Started at: $(date)"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counters
total_tests=0
passed_tests=0
failed_tests=0
timeout_tests=0

# Function to run a test with timeout
run_test() {
    local test_name=$1
    local test_cmd=$2
    local timeout_duration=$3
    
    echo -n "Running $test_name... "
    total_tests=$((total_tests + 1))
    
    # Run test with timeout
    if timeout $timeout_duration bash -c "$test_cmd" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ PASS${NC}"
        passed_tests=$((passed_tests + 1))
    else
        exit_code=$?
        if [ $exit_code -eq 124 ]; then
            echo -e "${YELLOW}⏱️  TIMEOUT${NC}"
            timeout_tests=$((timeout_tests + 1))
        else
            echo -e "${RED}❌ FAIL${NC}"
            failed_tests=$((failed_tests + 1))
        fi
    fi
}

echo "=== Unit Tests ==="
run_test "Memory Pool Test" "cargo test --release --lib test_bounded_memory_pool -- --nocapture" "30s"
run_test "B+Tree Split Test" "cargo test --release --lib test_internal_node_split -- --nocapture" "30s"
run_test "Compression Tests" "cargo test --release --lib compression::tests -- --nocapture" "30s"

echo ""
echo "=== Integration Tests ==="
run_test "Simple B+Tree Test" "cargo test --release --test test_simple_btree" "60s"
run_test "Backup/Restore Test" "cargo test --release --test test_backup_restore" "60s"

echo ""
echo "=== Key Examples ==="
run_test "Basic Usage" "cargo run --release --example basic_usage" "30s"
run_test "Simple Test" "cargo run --release --example simple_test" "30s"
run_test "Quick Summary" "cargo run --release --example quick_summary" "30s"
run_test "Concurrent Test" "cargo run --release --example concurrent_test" "30s"
run_test "Final Benchmark" "cargo run --release --example final_benchmark" "90s"
run_test "Final Test Check" "cargo run --release --example final_test_check" "30s"
run_test "Feature Summary" "cargo run --release --example feature_summary" "30s"
run_test "Memory Test" "cargo run --release --example memory_test" "30s"
run_test "Stress Test" "cargo run --release --example stress_test" "60s"
run_test "Chaos Test" "cargo run --release --example chaos_test" "60s"

echo ""
echo "=== Test Summary ==="
echo "Total tests run: $total_tests"
echo -e "${GREEN}Passed: $passed_tests${NC}"
echo -e "${RED}Failed: $failed_tests${NC}"
echo -e "${YELLOW}Timeout: $timeout_tests${NC}"
echo ""

if [ $failed_tests -eq 0 ] && [ $timeout_tests -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}⚠️  Some tests failed or timed out${NC}"
    exit 1
fi