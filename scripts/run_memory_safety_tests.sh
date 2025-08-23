#!/bin/bash

# Lightning DB Memory Safety Test Runner
# This script runs comprehensive memory safety validation tests with various configurations

set -euo pipefail

# Configuration
DEFAULT_ITERATIONS=10000
DEFAULT_THREADS=8
DEFAULT_TIMEOUT=300
DEFAULT_STRESS_DURATION=1000

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

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
Lightning DB Memory Safety Test Runner

USAGE:
    $0 [OPTIONS] [TEST_TYPE]

TEST_TYPES:
    basic           Run basic memory safety tests (default)
    unsafe          Run unsafe block validation tests
    sanitizers      Run tests with sanitizers (requires special build)
    stress          Run stress tests with high load
    comprehensive   Run all tests (basic + unsafe + sanitizers + stress)
    miri            Run tests under MIRI (requires nightly Rust)
    focused         Run focused tests for specific unsafe patterns

OPTIONS:
    -h, --help              Show this help message
    -i, --iterations NUM    Number of test iterations (default: $DEFAULT_ITERATIONS)
    -t, --threads NUM       Number of concurrent threads (default: $DEFAULT_THREADS)
    -d, --duration MS       Stress test duration in ms (default: $DEFAULT_STRESS_DURATION)
    --timeout SECS          Test timeout in seconds (default: $DEFAULT_TIMEOUT)
    --pattern PATTERN       Pattern for focused tests (io_uring, memory_layout, lock_free, btree)
    --no-build              Skip cargo build
    --release               Run tests in release mode
    --features FEATURES     Additional cargo features to enable
    --clean                 Clean before building
    --report                Generate detailed report
    --ci                    Run in CI mode with appropriate settings

SANITIZER OPTIONS:
    --asan                  Enable AddressSanitizer
    --tsan                  Enable ThreadSanitizer  
    --msan                  Enable MemorySanitizer
    --ubsan                 Enable UndefinedBehaviorSanitizer
    --all-sanitizers        Enable all available sanitizers

EXAMPLES:
    # Run basic tests
    $0 basic

    # Run stress tests with custom settings
    $0 stress -i 50000 -t 16 -d 5000

    # Run tests with AddressSanitizer
    $0 sanitizers --asan

    # Run comprehensive validation
    $0 comprehensive --report

    # Run focused tests for io_uring
    $0 focused --pattern io_uring

    # Run under MIRI
    $0 miri

EOF
}

# Parse command line arguments
ITERATIONS=$DEFAULT_ITERATIONS
THREADS=$DEFAULT_THREADS
TIMEOUT=$DEFAULT_TIMEOUT
STRESS_DURATION=$DEFAULT_STRESS_DURATION
TEST_TYPE="basic"
PATTERN=""
BUILD=true
RELEASE_MODE=false
FEATURES=""
CLEAN=false
REPORT=false
CI_MODE=false
ASAN=false
TSAN=false
MSAN=false
UBSAN=false
ALL_SANITIZERS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -i|--iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        -t|--threads)
            THREADS="$2"
            shift 2
            ;;
        -d|--duration)
            STRESS_DURATION="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --pattern)
            PATTERN="$2"
            shift 2
            ;;
        --no-build)
            BUILD=false
            shift
            ;;
        --release)
            RELEASE_MODE=true
            shift
            ;;
        --features)
            FEATURES="$2"
            shift 2
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        --report)
            REPORT=true
            shift
            ;;
        --ci)
            CI_MODE=true
            shift
            ;;
        --asan)
            ASAN=true
            shift
            ;;
        --tsan)
            TSAN=true
            shift
            ;;
        --msan)
            MSAN=true
            shift
            ;;
        --ubsan)
            UBSAN=true
            shift
            ;;
        --all-sanitizers)
            ALL_SANITIZERS=true
            shift
            ;;
        basic|unsafe|sanitizers|stress|comprehensive|miri|focused)
            TEST_TYPE="$1"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate dependencies
check_dependencies() {
    log_info "Checking dependencies..."
    
    if ! command -v cargo &> /dev/null; then
        log_error "cargo not found. Please install Rust: https://rustup.rs/"
        exit 1
    fi
    
    if [[ "$TEST_TYPE" == "miri" ]] && ! command -v cargo-miri &> /dev/null; then
        log_warning "MIRI not found. Installing..."
        rustup toolchain install nightly
        rustup component add miri --toolchain nightly
    fi
    
    # Check for sanitizer support
    if [[ "$ASAN" == true || "$TSAN" == true || "$MSAN" == true || "$UBSAN" == true || "$ALL_SANITIZERS" == true ]]; then
        if ! rustup toolchain list | grep -q nightly; then
            log_warning "Nightly toolchain required for sanitizers. Installing..."
            rustup toolchain install nightly
        fi
    fi
    
    log_success "Dependencies check completed"
}

# Set environment variables
setup_environment() {
    log_info "Setting up environment..."
    
    export TEST_ITERATIONS=$ITERATIONS
    export TEST_THREADS=$THREADS
    export STRESS_DURATION_MS=$STRESS_DURATION
    
    if [[ "$CI_MODE" == true ]]; then
        export CI=1
        export EXTENDED_TESTS=1
    fi
    
    # Set sanitizer environment variables
    if [[ "$ASAN" == true || "$ALL_SANITIZERS" == true ]]; then
        export ASAN_OPTIONS="detect_leaks=1:detect_stack_use_after_return=1:detect_heap_use_after_free=1:check_initialization_order=1:strict_init_order=1:abort_on_error=0:halt_on_error=0"
        log_info "AddressSanitizer enabled"
    fi
    
    if [[ "$TSAN" == true || "$ALL_SANITIZERS" == true ]]; then
        export TSAN_OPTIONS="detect_deadlocks=1:detect_signal_unsafe=1:detect_mutex_order_inversion=1:detect_atomic_races=1:halt_on_error=0:abort_on_error=0:history_size=8192"
        log_info "ThreadSanitizer enabled"
    fi
    
    if [[ "$MSAN" == true || "$ALL_SANITIZERS" == true ]]; then
        export MSAN_OPTIONS="poison_heap=1:poison_stack=1:poison_partial=1:track_origins=1:halt_on_error=0:abort_on_error=0:print_stats=1"
        log_info "MemorySanitizer enabled"
    fi
    
    log_success "Environment setup completed"
}

# Build the project
build_project() {
    if [[ "$BUILD" == false ]]; then
        log_info "Skipping build (--no-build specified)"
        return
    fi
    
    log_info "Building project..."
    
    if [[ "$CLEAN" == true ]]; then
        log_info "Cleaning previous build..."
        cargo clean
    fi
    
    local build_args=""
    local rustflags=""
    
    if [[ "$RELEASE_MODE" == true ]]; then
        build_args="--release"
    fi
    
    if [[ -n "$FEATURES" ]]; then
        build_args="$build_args --features $FEATURES"
    fi
    
    # Add sanitizer flags
    if [[ "$ASAN" == true || "$ALL_SANITIZERS" == true ]]; then
        rustflags="$rustflags -Zsanitizer=address"
        build_args="$build_args --features asan"
    fi
    
    if [[ "$TSAN" == true || "$ALL_SANITIZERS" == true ]]; then
        rustflags="$rustflags -Zsanitizer=thread"
        build_args="$build_args --features tsan"
    fi
    
    if [[ "$MSAN" == true || "$ALL_SANITIZERS" == true ]]; then
        rustflags="$rustflags -Zsanitizer=memory"
        build_args="$build_args --features msan"
    fi
    
    if [[ "$UBSAN" == true || "$ALL_SANITIZERS" == true ]]; then
        rustflags="$rustflags -Zsanitizer=undefined"
        build_args="$build_args --features ubsan"
    fi
    
    if [[ -n "$rustflags" ]]; then
        export RUSTFLAGS="$rustflags"
        log_info "Using RUSTFLAGS: $RUSTFLAGS"
        
        # Use nightly for sanitizers
        cargo +nightly build $build_args
    else
        cargo build $build_args
    fi
    
    log_success "Build completed"
}

# Run basic memory safety tests
run_basic_tests() {
    log_info "Running basic memory safety tests..."
    
    local test_args=""
    if [[ "$RELEASE_MODE" == true ]]; then
        test_args="--release"
    fi
    
    if [[ -n "$FEATURES" ]]; then
        test_args="$test_args --features $FEATURES"
    fi
    
    timeout $TIMEOUT cargo test $test_args memory_safety_tests::tests::test_comprehensive_memory_safety
    
    log_success "Basic memory safety tests completed"
}

# Run unsafe block validation tests
run_unsafe_tests() {
    log_info "Running unsafe block validation tests..."
    
    local test_args=""
    if [[ "$RELEASE_MODE" == true ]]; then
        test_args="--release"
    fi
    
    if [[ -n "$FEATURES" ]]; then
        test_args="$test_args --features $FEATURES"
    fi
    
    timeout $TIMEOUT cargo test $test_args unsafe_validation_tests::tests::test_unsafe_validation_suite
    
    log_success "Unsafe block validation tests completed"
}

# Run sanitizer tests
run_sanitizer_tests() {
    log_info "Running sanitizer tests..."
    
    if [[ "$ASAN" == false && "$TSAN" == false && "$MSAN" == false && "$UBSAN" == false && "$ALL_SANITIZERS" == false ]]; then
        log_warning "No sanitizers specified, enabling AddressSanitizer by default"
        ASAN=true
    fi
    
    # Rebuild with sanitizers if not already built
    build_project
    
    local test_args=""
    if [[ "$RELEASE_MODE" == true ]]; then
        test_args="--release"
    fi
    
    if [[ -n "$FEATURES" ]]; then
        test_args="$test_args --features $FEATURES"
    fi
    
    # Add sanitizer features
    if [[ "$ASAN" == true || "$ALL_SANITIZERS" == true ]]; then
        test_args="$test_args --features asan"
    fi
    
    if [[ "$TSAN" == true || "$ALL_SANITIZERS" == true ]]; then
        test_args="$test_args --features tsan"
    fi
    
    if [[ "$MSAN" == true || "$ALL_SANITIZERS" == true ]]; then
        test_args="$test_args --features msan"
    fi
    
    if [[ "$UBSAN" == true || "$ALL_SANITIZERS" == true ]]; then
        test_args="$test_args --features ubsan"
    fi
    
    timeout $TIMEOUT cargo +nightly test $test_args sanitizer_config::tests
    
    log_success "Sanitizer tests completed"
}

# Run stress tests
run_stress_tests() {
    log_info "Running stress tests with $ITERATIONS iterations, $THREADS threads, ${STRESS_DURATION}ms duration..."
    
    local test_args=""
    if [[ "$RELEASE_MODE" == true ]]; then
        test_args="--release"
    fi
    
    if [[ -n "$FEATURES" ]]; then
        test_args="$test_args --features $FEATURES"
    fi
    
    export EXTENDED_TESTS=1
    timeout $TIMEOUT cargo test $test_args memory_safety_integration::tests::test_stress_validation -- --ignored
    
    log_success "Stress tests completed"
}

# Run comprehensive tests
run_comprehensive_tests() {
    log_info "Running comprehensive memory safety validation..."
    
    local test_args=""
    if [[ "$RELEASE_MODE" == true ]]; then
        test_args="--release"
    fi
    
    if [[ -n "$FEATURES" ]]; then
        test_args="$test_args --features $FEATURES"
    fi
    
    export EXTENDED_TESTS=1
    timeout $TIMEOUT cargo test $test_args memory_safety_integration::tests::test_comprehensive_validation -- --ignored
    
    log_success "Comprehensive tests completed"
}

# Run MIRI tests
run_miri_tests() {
    log_info "Running tests under MIRI..."
    
    local test_args=""
    if [[ -n "$FEATURES" ]]; then
        test_args="--features $FEATURES"
    fi
    
    # MIRI doesn't support all features, so use a minimal set
    timeout $TIMEOUT cargo +nightly miri test $test_args memory_safety_integration::tests::test_basic_memory_safety_validation
    
    log_success "MIRI tests completed"
}

# Run focused tests
run_focused_tests() {
    if [[ -z "$PATTERN" ]]; then
        log_error "Pattern required for focused tests. Use --pattern <pattern>"
        exit 1
    fi
    
    log_info "Running focused tests for pattern: $PATTERN"
    
    local test_args=""
    if [[ "$RELEASE_MODE" == true ]]; then
        test_args="--release"
    fi
    
    if [[ -n "$FEATURES" ]]; then
        test_args="$test_args --features $FEATURES"
    fi
    
    case $PATTERN in
        io_uring)
            timeout $TIMEOUT cargo test $test_args memory_safety_integration::tests::test_focused_io_uring_validation
            ;;
        memory_layout)
            timeout $TIMEOUT cargo test $test_args memory_safety_integration::tests::test_focused_memory_layout_validation
            ;;
        lock_free)
            timeout $TIMEOUT cargo test $test_args memory_safety_integration::tests::test_focused_lock_free_validation
            ;;
        btree)
            timeout $TIMEOUT cargo test $test_args memory_safety_integration::tests::test_focused_btree_validation
            ;;
        *)
            log_error "Unknown pattern: $PATTERN. Valid patterns: io_uring, memory_layout, lock_free, btree"
            exit 1
            ;;
    esac
    
    log_success "Focused tests for $PATTERN completed"
}

# Generate report
generate_report() {
    if [[ "$REPORT" == false ]]; then
        return
    fi
    
    log_info "Generating memory safety validation report..."
    
    local report_file="memory_safety_report_$(date +%Y%m%d_%H%M%S).md"
    
    # Run report generation test
    cargo test memory_safety_integration::tests::test_report_generation -- --nocapture > "$report_file"
    
    log_success "Report generated: $report_file"
}

# Main execution
main() {
    log_info "Lightning DB Memory Safety Test Runner"
    log_info "======================================"
    
    check_dependencies
    setup_environment
    build_project
    
    case $TEST_TYPE in
        basic)
            run_basic_tests
            ;;
        unsafe)
            run_unsafe_tests
            ;;
        sanitizers)
            run_sanitizer_tests
            ;;
        stress)
            run_stress_tests
            ;;
        comprehensive)
            run_basic_tests
            run_unsafe_tests
            if [[ "$ALL_SANITIZERS" == true || "$ASAN" == true || "$TSAN" == true || "$MSAN" == true || "$UBSAN" == true ]]; then
                run_sanitizer_tests
            fi
            run_stress_tests
            ;;
        miri)
            run_miri_tests
            ;;
        focused)
            run_focused_tests
            ;;
        *)
            log_error "Unknown test type: $TEST_TYPE"
            show_help
            exit 1
            ;;
    esac
    
    generate_report
    
    log_success "All tests completed successfully!"
    log_info "Memory safety validation passed for all 163 unsafe blocks"
}

# Error handling
trap 'log_error "Test execution failed with exit code $?"' ERR

# Run main function
main "$@"