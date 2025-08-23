#!/bin/bash

# Lightning DB Integration Test Runner
# Comprehensive test execution with reporting and CI integration

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="${PROJECT_ROOT}/target/integration-test-results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${RESULTS_DIR}/test_run_${TIMESTAMP}.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test suite configurations
declare -A TEST_SUITES=(
    ["smoke"]="Fast smoke tests for CI"
    ["integration"]="Full integration test suite"
    ["performance"]="Performance and load tests"
    ["security"]="Security integration tests"
    ["chaos"]="Chaos engineering and stress tests"
    ["all"]="All integration tests"
)

declare -A SUITE_TESTS=(
    ["smoke"]="end_to_end_tests::test_complete_database_lifecycle concurrency_tests::test_concurrent_read_write_workload recovery_integration_tests::test_crash_recovery_with_wal_replay"
    ["integration"]="end_to_end_tests concurrency_tests recovery_integration_tests system_integration_tests"
    ["performance"]="performance_integration_tests"
    ["security"]="security_integration_tests"
    ["chaos"]="chaos_tests"
    ["all"]="end_to_end_tests concurrency_tests recovery_integration_tests performance_integration_tests ha_tests security_integration_tests system_integration_tests chaos_tests"
)

# Default configuration
SUITE="smoke"
PARALLEL="auto"
COVERAGE="false"
VERBOSE="false"
DRY_RUN="false"
JUNIT_OUTPUT="false"
HTML_REPORT="false"
TIMEOUT="3600"
RETRY_COUNT="1"

# Function definitions
print_help() {
    cat << EOF
Lightning DB Integration Test Runner

Usage: $0 [OPTIONS]

Options:
    -s, --suite SUITE       Test suite to run: ${!TEST_SUITES[@]}
                           Default: smoke
    -p, --parallel COUNT    Parallel test execution (auto|1-16)
                           Default: auto
    -c, --coverage          Generate code coverage report
    -v, --verbose           Verbose output
    -d, --dry-run           Show what would be executed without running
    -j, --junit            Generate JUnit XML output
    -r, --html-report      Generate HTML test report
    -t, --timeout SECONDS  Test timeout in seconds (default: 3600)
    -R, --retry COUNT      Number of retries for failed tests (default: 1)
    -h, --help             Show this help message

Test Suites:
EOF
    for suite in "${!TEST_SUITES[@]}"; do
        printf "    %-12s %s\n" "$suite" "${TEST_SUITES[$suite]}"
    done
    echo ""
    echo "Examples:"
    echo "    $0 --suite smoke --coverage"
    echo "    $0 --suite all --parallel 4 --html-report"
    echo "    $0 --suite chaos --timeout 7200"
}

log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case "$level" in
        INFO)  echo -e "${BLUE}[INFO]${NC}  $message" | tee -a "$LOG_FILE" ;;
        WARN)  echo -e "${YELLOW}[WARN]${NC}  $message" | tee -a "$LOG_FILE" ;;
        ERROR) echo -e "${RED}[ERROR]${NC} $message" | tee -a "$LOG_FILE" ;;
        SUCCESS) echo -e "${GREEN}[SUCCESS]${NC} $message" | tee -a "$LOG_FILE" ;;
        *) echo "$message" | tee -a "$LOG_FILE" ;;
    esac
}

setup_environment() {
    log INFO "Setting up test environment..."
    
    # Create results directory
    mkdir -p "$RESULTS_DIR"
    
    # Initialize log file
    echo "Lightning DB Integration Test Run - $(date)" > "$LOG_FILE"
    echo "Suite: $SUITE" >> "$LOG_FILE"
    echo "Parallel: $PARALLEL" >> "$LOG_FILE"
    echo "Coverage: $COVERAGE" >> "$LOG_FILE"
    echo "========================================" >> "$LOG_FILE"
    
    # Check dependencies
    if ! command -v cargo &> /dev/null; then
        log ERROR "cargo not found. Please install Rust."
        exit 1
    fi
    
    # Check project structure
    if [[ ! -f "$PROJECT_ROOT/Cargo.toml" ]]; then
        log ERROR "Not in a Rust project directory."
        exit 1
    fi
    
    # Set parallel execution
    if [[ "$PARALLEL" == "auto" ]]; then
        PARALLEL=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "4")
        log INFO "Auto-detected $PARALLEL CPU cores"
    fi
    
    # Environment variables
    export RUST_BACKTRACE=1
    export RUST_LOG=info
    
    if [[ "$COVERAGE" == "true" ]]; then
        export RUSTFLAGS="-C instrument-coverage"
        export LLVM_PROFILE_FILE="$RESULTS_DIR/coverage-%p-%m.profraw"
    fi
    
    log INFO "Environment setup complete"
}

build_project() {
    log INFO "Building project..."
    
    cd "$PROJECT_ROOT"
    
    if [[ "$COVERAGE" == "true" ]]; then
        cargo build --release --features testing 2>&1 | tee -a "$LOG_FILE"
    else
        cargo build --release 2>&1 | tee -a "$LOG_FILE"
    fi
    
    if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
        log ERROR "Build failed"
        exit 1
    fi
    
    log SUCCESS "Build completed"
}

run_test_suite() {
    local suite="$1"
    local test_list="${SUITE_TESTS[$suite]}"
    
    log INFO "Running test suite: $suite"
    log INFO "Tests: $test_list"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log INFO "DRY RUN: Would execute tests: $test_list"
        return 0
    fi
    
    local start_time=$(date +%s)
    local test_results=()
    local failed_tests=()
    
    cd "$PROJECT_ROOT"
    
    # Create test-specific output directory
    local suite_results_dir="$RESULTS_DIR/suite_${suite}_${TIMESTAMP}"
    mkdir -p "$suite_results_dir"
    
    # Run tests based on parallelization setting
    if [[ "$PARALLEL" -gt 1 ]] && [[ "$suite" != "chaos" ]]; then
        log INFO "Running tests in parallel (max $PARALLEL concurrent)"
        run_tests_parallel "$test_list" "$suite_results_dir"
    else
        log INFO "Running tests sequentially"
        run_tests_sequential "$test_list" "$suite_results_dir"
    fi
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Generate results summary
    generate_test_summary "$suite" "$duration" "$suite_results_dir"
}

run_tests_sequential() {
    local test_list="$1"
    local output_dir="$2"
    
    local passed=0
    local failed=0
    local total=0
    
    for test in $test_list; do
        total=$((total + 1))
        log INFO "Running test: $test"
        
        local test_start=$(date +%s)
        local test_output_file="$output_dir/${test//::/_}.log"
        
        if run_single_test "$test" "$test_output_file"; then
            passed=$((passed + 1))
            log SUCCESS "✓ $test"
        else
            failed=$((failed + 1))
            log ERROR "✗ $test"
        fi
        
        local test_duration=$(($(date +%s) - test_start))
        echo "Test: $test, Duration: ${test_duration}s, Status: $([[ $? -eq 0 ]] && echo "PASSED" || echo "FAILED")" >> "$output_dir/test_summary.txt"
    done
    
    log INFO "Sequential execution complete: $passed passed, $failed failed out of $total total"
}

run_tests_parallel() {
    local test_list="$1"
    local output_dir="$2"
    
    local pids=()
    local test_names=()
    
    # Start tests in parallel
    for test in $test_list; do
        if [[ ${#pids[@]} -ge $PARALLEL ]]; then
            # Wait for one to complete
            wait_for_any_job pids test_names "$output_dir"
        fi
        
        log INFO "Starting test: $test"
        local test_output_file="$output_dir/${test//::/_}.log"
        
        (
            run_single_test "$test" "$test_output_file"
            echo $? > "$test_output_file.exit_code"
        ) &
        
        pids+=($!)
        test_names+=("$test")
    done
    
    # Wait for remaining jobs
    while [[ ${#pids[@]} -gt 0 ]]; do
        wait_for_any_job pids test_names "$output_dir"
    done
    
    log INFO "Parallel execution complete"
}

wait_for_any_job() {
    local -n pids_ref=$1
    local -n names_ref=$2
    local output_dir="$3"
    
    local completed_idx=-1
    
    # Check which job completed
    for i in "${!pids_ref[@]}"; do
        if ! kill -0 "${pids_ref[$i]}" 2>/dev/null; then
            completed_idx=$i
            break
        fi
    done
    
    if [[ $completed_idx -eq -1 ]]; then
        # Wait for any job to complete
        wait -n
        for i in "${!pids_ref[@]}"; do
            if ! kill -0 "${pids_ref[$i]}" 2>/dev/null; then
                completed_idx=$i
                break
            fi
        done
    fi
    
    if [[ $completed_idx -ne -1 ]]; then
        local test_name="${names_ref[$completed_idx]}"
        local test_output_file="$output_dir/${test_name//::/_}.log"
        
        if [[ -f "$test_output_file.exit_code" ]]; then
            local exit_code=$(cat "$test_output_file.exit_code")
            if [[ $exit_code -eq 0 ]]; then
                log SUCCESS "✓ $test_name"
            else
                log ERROR "✗ $test_name"
            fi
        fi
        
        # Remove completed job from arrays
        unset pids_ref[$completed_idx]
        unset names_ref[$completed_idx]
        pids_ref=("${pids_ref[@]}")
        names_ref=("${names_ref[@]}")
    fi
}

run_single_test() {
    local test_name="$1"
    local output_file="$2"
    local retry_count=0
    
    while [[ $retry_count -le $RETRY_COUNT ]]; do
        if [[ $retry_count -gt 0 ]]; then
            log WARN "Retrying $test_name (attempt $((retry_count + 1)))"
        fi
        
        # Run the test with timeout
        if timeout "$TIMEOUT" cargo test "$test_name" \
            --test integration \
            --release \
            -- \
            --nocapture \
            --test-threads=1 \
            > "$output_file" 2>&1; then
            return 0
        else
            local exit_code=$?
            if [[ $exit_code -eq 124 ]]; then
                log ERROR "Test $test_name timed out after ${TIMEOUT}s"
                echo "TIMEOUT after ${TIMEOUT}s" >> "$output_file"
            else
                log ERROR "Test $test_name failed with exit code $exit_code"
            fi
            
            retry_count=$((retry_count + 1))
            if [[ $retry_count -le $RETRY_COUNT ]]; then
                sleep 2
            fi
        fi
    done
    
    return 1
}

generate_coverage_report() {
    if [[ "$COVERAGE" != "true" ]]; then
        return 0
    fi
    
    log INFO "Generating coverage report..."
    
    cd "$PROJECT_ROOT"
    
    # Check for coverage data files
    local coverage_files=$(find "$RESULTS_DIR" -name "*.profraw" 2>/dev/null || true)
    if [[ -z "$coverage_files" ]]; then
        log WARN "No coverage data found"
        return 0
    fi
    
    # Generate coverage report using llvm-cov
    if command -v llvm-cov &> /dev/null; then
        local binary_path="target/release/deps/integration-"
        local binary=$(find target/release/deps -name "integration-*" -type f -executable | head -1)
        
        if [[ -n "$binary" ]]; then
            llvm-cov show "$binary" \
                --instr-profile="$RESULTS_DIR/coverage.profdata" \
                --format=html \
                --output-dir="$RESULTS_DIR/coverage_html" \
                --show-line-counts-or-regions \
                --show-instantiations \
                2>&1 | tee -a "$LOG_FILE"
            
            log SUCCESS "Coverage report generated at $RESULTS_DIR/coverage_html/index.html"
        else
            log WARN "Could not find test binary for coverage report"
        fi
    else
        log WARN "llvm-cov not found, skipping coverage report"
    fi
}

generate_test_summary() {
    local suite="$1"
    local duration="$2"
    local results_dir="$3"
    
    log INFO "Generating test summary for suite: $suite"
    
    local summary_file="$RESULTS_DIR/summary_${suite}_${TIMESTAMP}.txt"
    local passed=0
    local failed=0
    local total=0
    
    {
        echo "Lightning DB Integration Test Summary"
        echo "===================================="
        echo "Suite: $suite"
        echo "Timestamp: $(date)"
        echo "Duration: ${duration}s"
        echo ""
        echo "Test Results:"
        echo "-------------"
    } > "$summary_file"
    
    # Count test results
    for log_file in "$results_dir"/*.log; do
        if [[ -f "$log_file" ]]; then
            local test_name=$(basename "$log_file" .log)
            local exit_code_file="${log_file}.exit_code"
            
            total=$((total + 1))
            
            if [[ -f "$exit_code_file" ]] && [[ $(cat "$exit_code_file") -eq 0 ]]; then
                passed=$((passed + 1))
                echo "✓ PASS: $test_name" >> "$summary_file"
            else
                failed=$((failed + 1))
                echo "✗ FAIL: $test_name" >> "$summary_file"
            fi
        fi
    done
    
    {
        echo ""
        echo "Summary:"
        echo "--------"
        echo "Total:  $total"
        echo "Passed: $passed"
        echo "Failed: $failed"
        echo "Success Rate: $(( total > 0 ? (passed * 100) / total : 0 ))%"
    } >> "$summary_file"
    
    # Display summary
    cat "$summary_file"
    
    # Generate JUnit XML if requested
    if [[ "$JUNIT_OUTPUT" == "true" ]]; then
        generate_junit_xml "$suite" "$results_dir" "$total" "$passed" "$failed" "$duration"
    fi
    
    # Generate HTML report if requested
    if [[ "$HTML_REPORT" == "true" ]]; then
        generate_html_report "$suite" "$results_dir" "$total" "$passed" "$failed" "$duration"
    fi
    
    # Return appropriate exit code
    if [[ $failed -gt 0 ]]; then
        return 1
    else
        return 0
    fi
}

generate_junit_xml() {
    local suite="$1"
    local results_dir="$2"
    local total="$3"
    local passed="$4"
    local failed="$5"
    local duration="$6"
    
    local junit_file="$RESULTS_DIR/junit_${suite}_${TIMESTAMP}.xml"
    
    {
        echo '<?xml version="1.0" encoding="UTF-8"?>'
        echo "<testsuite name=\"lightning_db_${suite}\" tests=\"$total\" failures=\"$failed\" errors=\"0\" skipped=\"0\" time=\"$duration\">"
        
        for log_file in "$results_dir"/*.log; do
            if [[ -f "$log_file" ]]; then
                local test_name=$(basename "$log_file" .log)
                local exit_code_file="${log_file}.exit_code"
                
                echo "  <testcase name=\"$test_name\" time=\"0\">"
                
                if [[ -f "$exit_code_file" ]] && [[ $(cat "$exit_code_file") -ne 0 ]]; then
                    echo "    <failure message=\"Test failed\">"
                    # Include last few lines of test output
                    tail -20 "$log_file" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g'
                    echo "    </failure>"
                fi
                
                echo "  </testcase>"
            fi
        done
        
        echo "</testsuite>"
    } > "$junit_file"
    
    log SUCCESS "JUnit XML report generated: $junit_file"
}

generate_html_report() {
    local suite="$1"
    local results_dir="$2"
    local total="$3"
    local passed="$4"
    local failed="$5"
    local duration="$6"
    
    local html_file="$RESULTS_DIR/report_${suite}_${TIMESTAMP}.html"
    
    {
        cat << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Lightning DB Integration Test Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .summary { background: #f0f0f0; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        .passed { color: green; font-weight: bold; }
        .failed { color: red; font-weight: bold; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .test-output { font-family: monospace; font-size: 12px; background: #f8f8f8; padding: 10px; border-radius: 3px; max-height: 200px; overflow-y: auto; }
    </style>
</head>
<body>
    <h1>Lightning DB Integration Test Report</h1>
    
    <div class="summary">
        <h2>Test Suite: ${suite}</h2>
        <p><strong>Timestamp:</strong> $(date)</p>
        <p><strong>Duration:</strong> ${duration}s</p>
        <p><strong>Total Tests:</strong> $total</p>
        <p><strong>Passed:</strong> <span class="passed">$passed</span></p>
        <p><strong>Failed:</strong> <span class="failed">$failed</span></p>
        <p><strong>Success Rate:</strong> $(( total > 0 ? (passed * 100) / total : 0 ))%</p>
    </div>
    
    <h2>Test Results</h2>
    <table>
        <thead>
            <tr>
                <th>Test Name</th>
                <th>Status</th>
                <th>Output</th>
            </tr>
        </thead>
        <tbody>
EOF
        
        for log_file in "$results_dir"/*.log; do
            if [[ -f "$log_file" ]]; then
                local test_name=$(basename "$log_file" .log)
                local exit_code_file="${log_file}.exit_code"
                
                if [[ -f "$exit_code_file" ]] && [[ $(cat "$exit_code_file") -eq 0 ]]; then
                    local status="<span class=\"passed\">PASSED</span>"
                else
                    local status="<span class=\"failed\">FAILED</span>"
                fi
                
                echo "            <tr>"
                echo "                <td>$test_name</td>"
                echo "                <td>$status</td>"
                echo "                <td><details><summary>Show Output</summary><div class=\"test-output\">"
                # Escape HTML and include test output
                cat "$log_file" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g'
                echo "                </div></details></td>"
                echo "            </tr>"
            fi
        done
        
        cat << 'EOF'
        </tbody>
    </table>
</body>
</html>
EOF
    } > "$html_file"
    
    log SUCCESS "HTML report generated: $html_file"
}

cleanup() {
    log INFO "Cleaning up..."
    
    # Kill any remaining background processes
    jobs -p | xargs -r kill 2>/dev/null || true
    
    # Clean up temporary files
    if [[ "$COVERAGE" == "true" ]]; then
        find "$RESULTS_DIR" -name "*.profraw" -delete 2>/dev/null || true
    fi
    
    log INFO "Cleanup complete"
}

main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--suite)
                SUITE="$2"
                shift 2
                ;;
            -p|--parallel)
                PARALLEL="$2"
                shift 2
                ;;
            -c|--coverage)
                COVERAGE="true"
                shift
                ;;
            -v|--verbose)
                VERBOSE="true"
                shift
                ;;
            -d|--dry-run)
                DRY_RUN="true"
                shift
                ;;
            -j|--junit)
                JUNIT_OUTPUT="true"
                shift
                ;;
            -r|--html-report)
                HTML_REPORT="true"
                shift
                ;;
            -t|--timeout)
                TIMEOUT="$2"
                shift 2
                ;;
            -R|--retry)
                RETRY_COUNT="$2"
                shift 2
                ;;
            -h|--help)
                print_help
                exit 0
                ;;
            *)
                log ERROR "Unknown option: $1"
                print_help
                exit 1
                ;;
        esac
    done
    
    # Validate suite
    if [[ ! ${TEST_SUITES[$SUITE]+_} ]]; then
        log ERROR "Invalid test suite: $SUITE"
        log INFO "Available suites: ${!TEST_SUITES[@]}"
        exit 1
    fi
    
    # Set up trap for cleanup
    trap cleanup EXIT
    
    # Main execution flow
    setup_environment
    build_project
    
    local start_time=$(date +%s)
    
    if run_test_suite "$SUITE"; then
        local end_time=$(date +%s)
        local total_duration=$((end_time - start_time))
        
        generate_coverage_report
        
        log SUCCESS "Integration test suite '$SUITE' completed successfully in ${total_duration}s"
        log INFO "Results available in: $RESULTS_DIR"
        
        exit 0
    else
        local end_time=$(date +%s)
        local total_duration=$((end_time - start_time))
        
        log ERROR "Integration test suite '$SUITE' failed after ${total_duration}s"
        log INFO "Check logs in: $RESULTS_DIR"
        
        exit 1
    fi
}

# Execute main function
main "$@"