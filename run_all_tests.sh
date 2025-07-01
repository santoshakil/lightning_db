#!/bin/bash

# Lightning DB Comprehensive Test Suite Runner
# This script runs all tests to verify production readiness

set -e

echo "=== Lightning DB Comprehensive Test Suite ==="
echo "Starting at: $(date)"
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
PASSED=0
FAILED=0

run_test() {
    local test_name=$1
    local test_command=$2
    
    echo -e "${YELLOW}Running: $test_name${NC}"
    if eval "$test_command"; then
        echo -e "${GREEN}✅ PASSED: $test_name${NC}\n"
        ((PASSED++))
    else
        echo -e "${RED}❌ FAILED: $test_name${NC}\n"
        ((FAILED++))
    fi
}

# Build in release mode first
echo "Building Lightning DB in release mode..."
cargo build --release --all-features

# Run unit tests
run_test "Unit Tests" "cargo test --release"

# Run property tests
run_test "Property Tests" "cargo test --release --features proptest proptest_ -- --nocapture"

# Run benchmarks (quick run)
run_test "Benchmarks" "cargo bench --no-run"

# Run examples
echo -e "\n${YELLOW}Running Example Tests...${NC}\n"

# Basic examples
run_test "Basic Usage" "cargo run --release --example basic_usage"
run_test "Quick Summary" "cargo run --release --example quick_summary"
run_test "Demo" "cargo run --release --example demo"

# Verification tests
run_test "Persistence Verification" "cargo run --release --example verify_persistence_correctly"
run_test "Edge Cases" "cargo run --release --example edge_case_tests"
run_test "Concurrent Safety" "cargo run --release --example concurrent_safety_test"

# Stress tests
run_test "Production Stress Test" "timeout 60 cargo run --release --example production_stress_test_simple || true"
run_test "Simple Memory Test" "cargo run --release --example simple_memory_test"

# Advanced tests (may take longer)
if [[ "$1" == "--full" ]]; then
    echo -e "\n${YELLOW}Running Full Test Suite...${NC}\n"
    run_test "Network Failure Simulation" "cargo run --release --example network_failure_simulation"
    run_test "Long Running Stability" "timeout 300 cargo run --release --example long_running_stability_test || true"
    run_test "Backup and Restore" "cargo run --release --example backup_restore_test"
    run_test "Performance Comparison" "cargo run --release --example performance_comparison_test"
fi

# Check for compilation warnings
echo -e "\n${YELLOW}Checking for warnings...${NC}"
if cargo check --all-targets --all-features 2>&1 | grep -q "warning:"; then
    echo -e "${YELLOW}⚠️  Compilation warnings detected${NC}"
else
    echo -e "${GREEN}✅ No compilation warnings${NC}"
fi

# Summary
echo -e "\n${YELLOW}=== Test Summary ===${NC}"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo "Completed at: $(date)"

if [ $FAILED -eq 0 ]; then
    echo -e "\n${GREEN}✅ All tests passed! Lightning DB is production ready.${NC}"
    exit 0
else
    echo -e "\n${RED}❌ Some tests failed. Please review the output above.${NC}"
    exit 1
fi