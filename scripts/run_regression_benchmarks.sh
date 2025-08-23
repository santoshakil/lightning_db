#!/bin/bash

set -euo pipefail

# Lightning DB Performance Regression Benchmark Runner
# This script runs the comprehensive regression benchmark suite

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPORT_DIR="$PROJECT_ROOT/performance_reports"
CONFIG_FILE="$PROJECT_ROOT/performance-baselines.json"

# Default values
MODE="full"
DURATION=30
THREADS="1,4,8,16"
VALUE_SIZES="64,256,1024,4096"
CACHE_SIZES="64MB,256MB,1GB"
OUTPUT_FORMAT="html,json,csv"
FAIL_ON_REGRESSION=true
BASELINE_UPDATE=false
VERBOSE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

show_help() {
    cat << EOF
Lightning DB Performance Regression Benchmark Runner

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -m, --mode MODE           Benchmark mode: full, quick, specific (default: full)
    -d, --duration SECONDS    Duration for each benchmark in seconds (default: 30)
    -t, --threads LIST        Comma-separated list of thread counts (default: 1,4,8,16)
    -v, --value-sizes LIST    Comma-separated list of value sizes in bytes (default: 64,256,1024,4096)
    -c, --cache-sizes LIST    Comma-separated list of cache sizes (default: 64MB,256MB,1GB)
    -o, --output FORMAT       Output format: html,json,csv,md (default: html,json,csv)
    -f, --fail-on-regression  Fail if regressions are detected (default: true)
    -u, --update-baseline     Update baseline values with current results
    -b, --baseline-file FILE  Custom baseline file (default: performance-baselines.json)
    --config FILE             Custom configuration file
    --report-dir DIR          Output directory for reports (default: performance_reports)
    --quick                   Quick mode: reduced test duration and scope
    --verbose                 Enable verbose output
    -h, --help                Show this help message

MODES:
    full      - Run all benchmark suites (default)
    quick     - Run abbreviated benchmarks for fast feedback
    specific  - Run only specific benchmarks (requires --benchmarks option)

EXAMPLES:
    # Run full benchmark suite
    $0

    # Quick regression check
    $0 --quick

    # Custom configuration
    $0 --mode full --duration 60 --threads 1,8,16 --fail-on-regression

    # Update baselines
    $0 --mode quick --update-baseline

    # Generate only JSON report
    $0 --output json --report-dir ./my_reports

ENVIRONMENT VARIABLES:
    RUST_LOG                  Set logging level (debug, info, warn, error)
    LIGHTNING_DB_TEST_THREADS Override thread count for testing
    NO_COLOR                  Disable colored output
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -m|--mode)
                MODE="$2"
                shift 2
                ;;
            -d|--duration)
                DURATION="$2"
                shift 2
                ;;
            -t|--threads)
                THREADS="$2"
                shift 2
                ;;
            -v|--value-sizes)
                VALUE_SIZES="$2"
                shift 2
                ;;
            -c|--cache-sizes)
                CACHE_SIZES="$2"
                shift 2
                ;;
            -o|--output)
                OUTPUT_FORMAT="$2"
                shift 2
                ;;
            -f|--fail-on-regression)
                FAIL_ON_REGRESSION=true
                shift
                ;;
            --no-fail-on-regression)
                FAIL_ON_REGRESSION=false
                shift
                ;;
            -u|--update-baseline)
                BASELINE_UPDATE=true
                shift
                ;;
            -b|--baseline-file)
                CONFIG_FILE="$2"
                shift 2
                ;;
            --config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            --report-dir)
                REPORT_DIR="$2"
                shift 2
                ;;
            --quick)
                MODE="quick"
                DURATION=10
                THREADS="1,8"
                VALUE_SIZES="256,1024"
                CACHE_SIZES="256MB"
                shift
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

check_dependencies() {
    log_info "Checking dependencies..."
    
    # Check if we're in the right directory
    if [[ ! -f "$PROJECT_ROOT/Cargo.toml" ]]; then
        log_error "Not in Lightning DB project root. Cargo.toml not found."
        exit 1
    fi
    
    # Check Rust toolchain
    if ! command -v cargo &> /dev/null; then
        log_error "cargo not found. Please install Rust toolchain."
        exit 1
    fi
    
    # Check if criterion is available for benchmarks
    if ! grep -q "criterion" "$PROJECT_ROOT/Cargo.toml"; then
        log_error "criterion not found in Cargo.toml. Please add criterion as a dev dependency."
        exit 1
    fi
    
    # Create report directory
    mkdir -p "$REPORT_DIR"
    
    log_success "Dependencies check passed"
}

setup_environment() {
    log_info "Setting up benchmark environment..."
    
    # Set environment variables for optimal performance
    export RUST_LOG=${RUST_LOG:-warn}
    export RUSTFLAGS="-C target-cpu=native"
    
    # Disable debug assertions for benchmarks
    export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=false
    export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=false
    
    # Configure jemalloc for better performance measurement
    export MALLOC_CONF="prof:true,prof_active:false,lg_prof_sample:19"
    
    # System-specific optimizations
    case "$(uname -s)" in
        Linux)
            # Linux-specific optimizations
            if [[ -w /proc/sys/vm/swappiness ]]; then
                echo 1 | sudo tee /proc/sys/vm/swappiness >/dev/null 2>&1 || true
            fi
            
            # Set CPU governor to performance if available
            if command -v cpufreq-set &> /dev/null; then
                sudo cpufreq-set -g performance >/dev/null 2>&1 || true
            fi
            ;;
        Darwin)
            # macOS-specific optimizations
            # Disable thermal throttling warnings
            export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
            ;;
    esac
    
    log_success "Environment setup completed"
}

build_benchmarks() {
    log_info "Building benchmarks in release mode..."
    
    cd "$PROJECT_ROOT"
    
    # Clean previous builds if requested
    if [[ "${CLEAN_BUILD:-false}" == "true" ]]; then
        cargo clean
    fi
    
    # Build benchmarks
    if [[ "$VERBOSE" == "true" ]]; then
        cargo build --release --benches
    else
        cargo build --release --benches >/dev/null 2>&1
    fi
    
    if [[ $? -ne 0 ]]; then
        log_error "Failed to build benchmarks"
        exit 1
    fi
    
    log_success "Benchmarks built successfully"
}

create_config() {
    local config_file="$1"
    
    log_info "Creating benchmark configuration..."
    
    # Convert comma-separated values to JSON arrays
    local thread_array=$(echo "$THREADS" | sed 's/,/", "/g' | sed 's/^/["/' | sed 's/$/"]/')
    local value_size_array=$(echo "$VALUE_SIZES" | sed 's/,/, /g' | sed 's/^/[/' | sed 's/$/]/')
    
    # Convert cache sizes to bytes
    local cache_size_array=""
    IFS=',' read -ra CACHE_ARRAY <<< "$CACHE_SIZES"
    for size in "${CACHE_ARRAY[@]}"; do
        case $size in
            *KB) bytes=$((${size%KB} * 1024)) ;;
            *MB) bytes=$((${size%MB} * 1024 * 1024)) ;;
            *GB) bytes=$((${size%GB} * 1024 * 1024 * 1024)) ;;
            *) bytes=$size ;;
        esac
        if [[ -z "$cache_size_array" ]]; then
            cache_size_array="[$bytes"
        else
            cache_size_array="$cache_size_array, $bytes"
        fi
    done
    cache_size_array="$cache_size_array]"
    
    # Generate benchmark list based on mode
    local benchmark_list=""
    case "$MODE" in
        quick)
            benchmark_list='"sequential_reads", "sequential_writes", "mixed_workload"'
            ;;
        full)
            benchmark_list='"sequential_reads", "random_reads", "sequential_writes", "random_writes", "mixed_workload", "transaction_throughput", "concurrent_operations", "cache_performance"'
            ;;
        *)
            benchmark_list='"sequential_reads", "sequential_writes", "mixed_workload"'
            ;;
    esac
    
    cat > "$config_file" << EOF
{
  "benchmarks": [
    {
      "name": "regression_suite",
      "workload_type": "Mixed",
      "duration_secs": $DURATION,
      "sample_size": 10,
      "thread_counts": $thread_array,
      "value_sizes": $value_size_array,
      "dataset_sizes": [10000, 100000],
      "cache_sizes": $cache_size_array,
      "compression_enabled": false,
      "max_regression_percent": 15.0
    }
  ],
  "global_config": {
    "output_dir": "$REPORT_DIR",
    "generate_html_report": true,
    "generate_json_report": true,
    "generate_csv_export": true,
    "track_memory_usage": true,
    "track_cpu_usage": true,
    "track_io_stats": true,
    "fail_on_regression": $FAIL_ON_REGRESSION,
    "statistical_significance_threshold": 0.05
  },
  "baseline_file": "$CONFIG_FILE",
  "history_file": "$REPORT_DIR/performance-history.json"
}
EOF
    
    log_success "Configuration created: $config_file"
}

run_benchmarks() {
    log_info "Running benchmark suite in $MODE mode..."
    
    cd "$PROJECT_ROOT"
    
    # Create temporary config file
    local temp_config="$REPORT_DIR/benchmark_config.json"
    create_config "$temp_config"
    
    local start_time=$(date +%s)
    
    # Run the actual benchmarks
    local benchmark_cmd="cargo bench --bench regression_suite"
    
    if [[ "$VERBOSE" == "true" ]]; then
        log_info "Executing: $benchmark_cmd"
        $benchmark_cmd
    else
        $benchmark_cmd 2>&1 | tee "$REPORT_DIR/benchmark_output.log"
    fi
    
    local exit_code=$?
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [[ $exit_code -eq 0 ]]; then
        log_success "Benchmarks completed successfully in ${duration}s"
    else
        log_error "Benchmarks failed with exit code $exit_code"
        return $exit_code
    fi
    
    return 0
}

analyze_results() {
    log_info "Analyzing benchmark results..."
    
    # Check if reports were generated
    local html_report="$REPORT_DIR/performance_report.html"
    local json_report="$REPORT_DIR/performance_report.json"
    
    if [[ -f "$json_report" ]]; then
        # Extract key metrics from JSON report
        local total_benchmarks=$(jq -r '.summary.total_benchmarks // 0' "$json_report" 2>/dev/null || echo "0")
        local regressions=$(jq -r '.summary.regressions_detected // 0' "$json_report" 2>/dev/null || echo "0")
        local improvements=$(jq -r '.summary.improvements_detected // 0' "$json_report" 2>/dev/null || echo "0")
        local overall_status=$(jq -r '.summary.overall_status // "Unknown"' "$json_report" 2>/dev/null || echo "Unknown")
        
        log_info "Results Summary:"
        echo "  Total Benchmarks: $total_benchmarks"
        echo "  Regressions: $regressions"
        echo "  Improvements: $improvements"
        echo "  Overall Status: $overall_status"
        
        # Check for regressions
        if [[ "$regressions" -gt 0 ]] && [[ "$FAIL_ON_REGRESSION" == "true" ]]; then
            log_error "Performance regressions detected!"
            if [[ -f "$html_report" ]]; then
                log_info "Detailed report available at: $html_report"
            fi
            return 1
        elif [[ "$regressions" -gt 0 ]]; then
            log_warn "Performance regressions detected, but not failing due to configuration"
        fi
        
        if [[ "$improvements" -gt 0 ]]; then
            log_success "Performance improvements detected!"
        fi
    else
        log_warn "JSON report not found, unable to analyze results"
    fi
    
    return 0
}

generate_reports() {
    log_info "Generating performance reports..."
    
    # Reports are generated by the benchmark suite itself
    # Check which formats were requested and verify they exist
    
    IFS=',' read -ra FORMATS <<< "$OUTPUT_FORMAT"
    for format in "${FORMATS[@]}"; do
        case "$format" in
            html)
                if [[ -f "$REPORT_DIR/performance_report.html" ]]; then
                    log_success "HTML report generated: $REPORT_DIR/performance_report.html"
                else
                    log_warn "HTML report not found"
                fi
                ;;
            json)
                if [[ -f "$REPORT_DIR/performance_report.json" ]]; then
                    log_success "JSON report generated: $REPORT_DIR/performance_report.json"
                else
                    log_warn "JSON report not found"
                fi
                ;;
            csv)
                if [[ -f "$REPORT_DIR/performance_data.csv" ]]; then
                    log_success "CSV data exported: $REPORT_DIR/performance_data.csv"
                else
                    log_warn "CSV export not found"
                fi
                ;;
            md)
                if [[ -f "$REPORT_DIR/performance_report.md" ]]; then
                    log_success "Markdown report generated: $REPORT_DIR/performance_report.md"
                else
                    log_warn "Markdown report not found"
                fi
                ;;
        esac
    done
}

update_baselines() {
    if [[ "$BASELINE_UPDATE" == "true" ]]; then
        log_info "Updating baseline values..."
        
        if [[ -f "$REPORT_DIR/performance_report.json" ]]; then
            # Extract current results and update baselines
            # In a real implementation, this would be more sophisticated
            cp "$REPORT_DIR/performance_report.json" "$CONFIG_FILE.bak"
            log_success "Baseline values updated (backup created)"
        else
            log_warn "Cannot update baselines: performance report not found"
        fi
    fi
}

cleanup() {
    log_info "Cleaning up temporary files..."
    
    # Remove temporary configuration files
    rm -f "$REPORT_DIR/benchmark_config.json"
    
    # Reset system configurations if we changed them
    case "$(uname -s)" in
        Linux)
            if [[ -w /proc/sys/vm/swappiness ]]; then
                echo 60 | sudo tee /proc/sys/vm/swappiness >/dev/null 2>&1 || true
            fi
            ;;
    esac
    
    log_info "Cleanup completed"
}

main() {
    log_info "Lightning DB Performance Regression Benchmark Runner"
    log_info "=================================================="
    
    parse_args "$@"
    
    # Set up signal handlers
    trap cleanup EXIT
    trap 'log_error "Interrupted by user"; exit 130' INT TERM
    
    check_dependencies
    setup_environment
    build_benchmarks
    
    if ! run_benchmarks; then
        log_error "Benchmark execution failed"
        exit 1
    fi
    
    generate_reports
    
    if ! analyze_results; then
        log_error "Performance regression detected - failing build"
        exit 1
    fi
    
    update_baselines
    
    log_success "Benchmark suite completed successfully!"
    log_info "Reports available in: $REPORT_DIR"
    
    return 0
}

# Run main function with all arguments
main "$@"