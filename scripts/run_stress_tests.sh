#!/bin/bash

# Lightning DB Stress Testing Script
# Usage: ./scripts/run_stress_tests.sh [test_type] [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
TEST_TYPE="quick"
OUTPUT_DIR="./stress_test_results"
TIMEOUT="600" # 10 minutes default
PARALLEL="false"
VERBOSE="false"

# Help function
show_help() {
    cat << EOF
Lightning DB Stress Testing Script

Usage: $0 [OPTIONS] [TEST_TYPE]

TEST_TYPES:
    quick           Run quick stress tests (default, ~5-10 minutes)
    full            Run full stress test suite (~30-60 minutes)
    endurance       Run endurance tests (~1-24 hours)
    compatibility   Run compatibility tests only
    stress-limits   Run stress limits tests only
    chaos           Run chaos engineering tests only
    recovery        Run recovery tests only
    custom          Run specific test by name

OPTIONS:
    -h, --help      Show this help message
    -o, --output    Output directory (default: ./stress_test_results)
    -t, --timeout   Test timeout in seconds (default: 600)
    -p, --parallel  Run tests in parallel where possible
    -v, --verbose   Enable verbose output
    -f, --format    Output format: console, json, html, all (default: all)
    --duration      Duration for endurance tests in hours (default: 1)
    --cache-size    Database cache size in MB (default: 100)
    --no-cleanup    Don't cleanup test databases after completion

EXAMPLES:
    $0 quick                    # Run quick stress tests
    $0 full -o ./results        # Run full suite, save to ./results
    $0 endurance --duration 4   # Run 4-hour endurance test
    $0 custom test_max_concurrent_connections  # Run specific test
    $0 compatibility -v         # Run compatibility tests with verbose output

ENVIRONMENT VARIABLES:
    LIGHTNING_CACHE_SIZE        Database cache size in bytes
    LIGHTNING_TEST_THREADS      Number of test threads
    LIGHTNING_OUTPUT_FORMAT     Output format (console/json/html/all)
    LIGHTNING_TIMEOUT           Test timeout in seconds
    RUST_LOG                    Rust logging level (debug, info, warn, error)

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -t|--timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -p|--parallel)
            PARALLEL="true"
            shift
            ;;
        -v|--verbose)
            VERBOSE="true"
            shift
            ;;
        -f|--format)
            export LIGHTNING_OUTPUT_FORMAT="$2"
            shift 2
            ;;
        --duration)
            ENDURANCE_DURATION="$2"
            shift 2
            ;;
        --cache-size)
            export LIGHTNING_CACHE_SIZE=$((${2} * 1024 * 1024))
            shift 2
            ;;
        --no-cleanup)
            NO_CLEANUP="true"
            shift
            ;;
        quick|full|endurance|compatibility|stress-limits|chaos|recovery|custom)
            TEST_TYPE="$1"
            shift
            ;;
        *)
            if [[ "$TEST_TYPE" == "custom" ]]; then
                CUSTOM_TEST="$1"
            else
                echo -e "${RED}Unknown option: $1${NC}"
                show_help
                exit 1
            fi
            shift
            ;;
    esac
done

# Set up environment
setup_environment() {
    echo -e "${BLUE}Setting up test environment...${NC}"
    
    # Create output directory
    mkdir -p "$OUTPUT_DIR"
    
    # Set default environment variables if not already set
    export LIGHTNING_OUTPUT_FORMAT="${LIGHTNING_OUTPUT_FORMAT:-all}"
    export LIGHTNING_TIMEOUT="${LIGHTNING_TIMEOUT:-$TIMEOUT}"
    export LIGHTNING_CACHE_SIZE="${LIGHTNING_CACHE_SIZE:-104857600}" # 100MB default
    export LIGHTNING_TEST_THREADS="${LIGHTNING_TEST_THREADS:-$(nproc 2>/dev/null || echo 4)}"
    
    if [[ "$VERBOSE" == "true" ]]; then
        export RUST_LOG="${RUST_LOG:-debug}"
    else
        export RUST_LOG="${RUST_LOG:-info}"
    fi
    
    # Set endurance duration
    if [[ -n "$ENDURANCE_DURATION" ]]; then
        export LIGHTNING_STRESS_DURATION=$((${ENDURANCE_DURATION} * 3600))
    fi
    
    echo "Environment configured:"
    echo "  Output format: $LIGHTNING_OUTPUT_FORMAT"
    echo "  Cache size: $((LIGHTNING_CACHE_SIZE / 1024 / 1024))MB"
    echo "  Test threads: $LIGHTNING_TEST_THREADS"
    echo "  Timeout: ${LIGHTNING_TIMEOUT}s"
    echo "  Log level: $RUST_LOG"
}

# System information
show_system_info() {
    echo -e "${BLUE}System Information:${NC}"
    echo "  OS: $(uname -s) $(uname -r)"
    echo "  Architecture: $(uname -m)"
    echo "  CPU cores: $(nproc 2>/dev/null || echo 'unknown')"
    
    # Memory info
    if command -v free >/dev/null 2>&1; then
        echo "  Memory: $(free -h | awk '/^Mem:/ {print $2}') total"
    elif [[ "$(uname -s)" == "Darwin" ]]; then
        echo "  Memory: $(($(sysctl -n hw.memsize) / 1024 / 1024 / 1024))GB total"
    fi
    
    # Disk space
    echo "  Disk space: $(df -h . | awk 'NR==2 {print $4}') available"
    
    # Rust version
    if command -v rustc >/dev/null 2>&1; then
        echo "  Rust version: $(rustc --version)"
    fi
    
    echo ""
}

# Run specific test suite
run_test_suite() {
    local test_name="$1"
    local test_args="$2"
    
    echo -e "${YELLOW}Running $test_name...${NC}"
    
    local start_time=$(date +%s)
    
    if [[ "$VERBOSE" == "true" ]]; then
        timeout "$TIMEOUT" cargo test "$test_args" -- --ignored --nocapture
    else
        timeout "$TIMEOUT" cargo test "$test_args" -- --ignored
    fi
    
    local exit_code=$?
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [[ $exit_code -eq 0 ]]; then
        echo -e "${GREEN}✓ $test_name completed successfully in ${duration}s${NC}"
    elif [[ $exit_code -eq 124 ]]; then
        echo -e "${RED}✗ $test_name timed out after ${TIMEOUT}s${NC}"
    else
        echo -e "${RED}✗ $test_name failed with exit code $exit_code after ${duration}s${NC}"
    fi
    
    return $exit_code
}

# Run tests based on type
run_tests() {
    case "$TEST_TYPE" in
        quick)
            echo -e "${GREEN}Running Quick Stress Tests...${NC}"
            run_test_suite "Quick Stress Test" "test_quick_stress"
            ;;
        full)
            echo -e "${GREEN}Running Full Stress Test Suite...${NC}"
            run_test_suite "Full Stress Test Suite" "test_full_stress_suite"
            ;;
        endurance)
            echo -e "${GREEN}Running Endurance Tests (${ENDURANCE_DURATION:-1} hours)...${NC}"
            echo -e "${YELLOW}Warning: This test will run for a long time!${NC}"
            run_test_suite "24-Hour Endurance Test" "test_24_hour_endurance"
            run_test_suite "Memory Leak Detection" "test_memory_leak_detection"
            run_test_suite "Performance Regression" "test_performance_regression"
            ;;
        compatibility)
            echo -e "${GREEN}Running Compatibility Tests...${NC}"
            run_test_suite "Compatibility Suite" "test_compatibility_suite"
            run_test_suite "Platform Basic Compatibility" "test_platform_basic_compatibility"
            ;;
        stress-limits)
            echo -e "${GREEN}Running Stress Limits Tests...${NC}"
            run_test_suite "Max Concurrent Connections" "test_max_concurrent_connections"
            run_test_suite "Max Transaction Rate" "test_max_transaction_rate"
            run_test_suite "Max Database Size" "test_max_database_size"
            run_test_suite "Max Key Value Sizes" "test_max_key_value_sizes"
            run_test_suite "Resource Exhaustion" "test_resource_exhaustion"
            ;;
        chaos)
            echo -e "${GREEN}Running Chaos Engineering Tests...${NC}"
            run_test_suite "Chaos Test Suite" "test_chaos_suite"
            run_test_suite "Process Kill Resilience" "test_process_kill_resilience"
            run_test_suite "Corruption Resilience" "test_corruption_resilience"
            ;;
        recovery)
            echo -e "${GREEN}Running Recovery Tests...${NC}"
            run_test_suite "Recovery Test Suite" "test_recovery_suite"
            run_test_suite "Crash Recovery Performance" "test_crash_recovery_performance"
            ;;
        custom)
            if [[ -n "$CUSTOM_TEST" ]]; then
                echo -e "${GREEN}Running Custom Test: $CUSTOM_TEST...${NC}"
                run_test_suite "Custom Test" "$CUSTOM_TEST"
            else
                echo -e "${RED}Error: Custom test name required${NC}"
                exit 1
            fi
            ;;
        *)
            echo -e "${RED}Error: Unknown test type: $TEST_TYPE${NC}"
            show_help
            exit 1
            ;;
    esac
}

# Cleanup function
cleanup() {
    if [[ "$NO_CLEANUP" != "true" ]]; then
        echo -e "${BLUE}Cleaning up test artifacts...${NC}"
        # Cleanup is handled by tempfile in the tests
    fi
    
    # Move generated reports to output directory
    if [[ -d "$OUTPUT_DIR" ]]; then
        mv stress_test_results_*.json "$OUTPUT_DIR/" 2>/dev/null || true
        mv stress_test_results_*.html "$OUTPUT_DIR/" 2>/dev/null || true
        echo -e "${GREEN}Results saved to: $OUTPUT_DIR${NC}"
    fi
}

# Resource monitoring
start_monitoring() {
    if command -v iostat >/dev/null 2>&1 && [[ "$VERBOSE" == "true" ]]; then
        echo -e "${BLUE}Starting resource monitoring...${NC}"
        iostat -x 1 > "$OUTPUT_DIR/iostat.log" 2>&1 &
        IOSTAT_PID=$!
        
        vmstat 1 > "$OUTPUT_DIR/vmstat.log" 2>&1 &
        VMSTAT_PID=$!
    fi
}

stop_monitoring() {
    if [[ -n "$IOSTAT_PID" ]]; then
        kill $IOSTAT_PID 2>/dev/null || true
    fi
    if [[ -n "$VMSTAT_PID" ]]; then
        kill $VMSTAT_PID 2>/dev/null || true
    fi
}

# Pre-flight checks
preflight_checks() {
    echo -e "${BLUE}Running pre-flight checks...${NC}"
    
    # Check if we're in the right directory
    if [[ ! -f "Cargo.toml" ]]; then
        echo -e "${RED}Error: Must be run from Lightning DB root directory${NC}"
        exit 1
    fi
    
    # Check if cargo is available
    if ! command -v cargo >/dev/null 2>&1; then
        echo -e "${RED}Error: cargo not found in PATH${NC}"
        exit 1
    fi
    
    # Check disk space (need at least 1GB for tests)
    available_space=$(df . | awk 'NR==2 {gsub(/[^0-9]/, "", $4); print $4}')
    if [[ $available_space -lt 1048576 ]]; then
        echo -e "${YELLOW}Warning: Less than 1GB disk space available${NC}"
    fi
    
    # Check if running as root (not recommended)
    if [[ $EUID -eq 0 ]]; then
        echo -e "${YELLOW}Warning: Running as root is not recommended${NC}"
    fi
    
    echo -e "${GREEN}✓ Pre-flight checks passed${NC}"
}

# Main execution
main() {
    echo -e "${GREEN}Lightning DB Stress Testing Suite${NC}"
    echo "========================================"
    
    preflight_checks
    setup_environment
    show_system_info
    
    # Set up cleanup trap
    trap cleanup EXIT
    trap stop_monitoring EXIT
    
    start_monitoring
    
    local start_time=$(date +%s)
    
    # Build the project first
    echo -e "${BLUE}Building Lightning DB...${NC}"
    if ! cargo build --release; then
        echo -e "${RED}Error: Failed to build Lightning DB${NC}"
        exit 1
    fi
    
    # Run the tests
    run_tests
    local test_exit_code=$?
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    echo ""
    echo "========================================"
    if [[ $test_exit_code -eq 0 ]]; then
        echo -e "${GREEN}✓ All tests completed successfully!${NC}"
    else
        echo -e "${RED}✗ Some tests failed${NC}"
    fi
    echo "Total duration: ${total_duration}s ($((total_duration / 60))m $((total_duration % 60))s)"
    echo "Results saved to: $OUTPUT_DIR"
    echo "========================================"
    
    exit $test_exit_code
}

# Run main function
main "$@"