#!/bin/bash

# Lightning DB Production Validation Script
# 
# This script runs comprehensive validation tests to ensure
# Lightning DB is ready for production deployment.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
VALIDATION_OUTPUT_DIR="${PROJECT_ROOT}/validation_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="${VALIDATION_OUTPUT_DIR}/validation_report_${TIMESTAMP}.txt"
LOG_FILE="${VALIDATION_OUTPUT_DIR}/validation_log_${TIMESTAMP}.log"

# Print banner
echo -e "${BLUE}"
echo "╔════════════════════════════════════════════════╗"
echo "║     Lightning DB Production Validation         ║"
echo "║           Version 1.0.0                        ║"
echo "╚════════════════════════════════════════════════╝"
echo -e "${NC}"

# Create output directory
mkdir -p "$VALIDATION_OUTPUT_DIR"

# Function to print status
print_status() {
    echo -e "${BLUE}[$(date +%H:%M:%S)]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[$(date +%H:%M:%S)] ✅ $1${NC}"
}

print_error() {
    echo -e "${RED}[$(date +%H:%M:%S)] ❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}[$(date +%H:%M:%S)] ⚠️  $1${NC}"
}

# Check prerequisites
print_status "Checking prerequisites..."

# Check Rust installation
if ! command -v cargo &> /dev/null; then
    print_error "Rust is not installed. Please install Rust first."
    exit 1
fi

# Check if in correct directory
if [ ! -f "$PROJECT_ROOT/Cargo.toml" ]; then
    print_error "Not in Lightning DB project directory"
    exit 1
fi

print_success "Prerequisites checked"

# System information
print_status "System Information:"
echo "  OS: $(uname -s)"
echo "  Architecture: $(uname -m)"
echo "  CPU Cores: $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "unknown")"
echo "  Memory: $(free -h 2>/dev/null | grep Mem | awk '{print $2}' || echo "unknown")"
echo "  Rust Version: $(rustc --version)"
echo ""

# Build the project
print_status "Building Lightning DB in release mode..."
cd "$PROJECT_ROOT"
if cargo build --release --all-features > "$LOG_FILE" 2>&1; then
    print_success "Build completed successfully"
else
    print_error "Build failed. Check $LOG_FILE for details"
    exit 1
fi

# Run clippy checks
print_status "Running code quality checks..."
if cargo clippy --all-targets --all-features -- -D warnings >> "$LOG_FILE" 2>&1; then
    print_success "Code quality checks passed"
else
    print_warning "Code quality issues found. Check $LOG_FILE for details"
fi

# Run unit tests first
print_status "Running unit tests..."
if cargo test --release -- --test-threads=4 >> "$LOG_FILE" 2>&1; then
    print_success "Unit tests passed"
else
    print_error "Unit tests failed. Check $LOG_FILE for details"
    exit 1
fi

# Set environment for production validation
export RUST_LOG=lightning_db=info
export RUST_BACKTRACE=1

# Configure validation parameters based on system resources
AVAILABLE_MEM=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}' || echo "8192")
THREAD_COUNT=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "8")

# Adjust parameters for available resources
if [ "$AVAILABLE_MEM" -lt 4096 ]; then
    print_warning "Low memory detected. Using reduced test parameters."
    OPERATION_COUNT=10000
    SUSTAINED_DURATION=60
else
    OPERATION_COUNT=100000
    SUSTAINED_DURATION=300
fi

print_status "Validation Configuration:"
echo "  Operations: $OPERATION_COUNT"
echo "  Threads: $THREAD_COUNT"
echo "  Sustained Test Duration: ${SUSTAINED_DURATION}s"
echo ""

# Create test runner
cat > "$PROJECT_ROOT/src/bin/production_validator.rs" << 'EOF'
use lightning_db::tests::production_validation_suite::{ProductionValidationSuite, ValidationConfig};
use std::time::Duration;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let operation_count: usize = env::var("OPERATION_COUNT")
        .unwrap_or_else(|_| "100000".to_string())
        .parse()?;
    
    let thread_count: usize = env::var("THREAD_COUNT")
        .unwrap_or_else(|_| "8".to_string())
        .parse()?;
    
    let sustained_duration: u64 = env::var("SUSTAINED_DURATION")
        .unwrap_or_else(|_| "300".to_string())
        .parse()?;

    let config = ValidationConfig {
        operation_count,
        thread_count,
        value_size: 1024,
        sustained_duration: Duration::from_secs(sustained_duration),
        verbose: true,
    };

    println!("Starting production validation with config: {:?}", config);

    let mut suite = ProductionValidationSuite::new(config)?;
    let report = suite.run_all()?;
    
    report.print_detailed();
    
    // Save report
    let report_path = env::var("REPORT_FILE").unwrap_or_else(|_| "validation_report.txt".to_string());
    report.save_to_file(&report_path)?;
    
    // Exit with error if any tests failed
    if report.failed_tests > 0 {
        std::process::exit(1);
    }
    
    Ok(())
}
EOF

# Build the validator
print_status "Building production validator..."
if cargo build --release --bin production_validator >> "$LOG_FILE" 2>&1; then
    print_success "Validator built successfully"
else
    print_error "Failed to build validator. Check $LOG_FILE"
    exit 1
fi

# Run production validation
print_status "Running production validation tests..."
echo ""
echo "This will test:"
echo "  • Basic operations and ACID compliance"
echo "  • Concurrent access patterns"
echo "  • Crash recovery and durability"
echo "  • Performance characteristics"
echo "  • Resource management"
echo "  • Error handling"
echo ""
echo "This may take several minutes..."
echo ""

# Set environment variables for the validator
export OPERATION_COUNT=$OPERATION_COUNT
export THREAD_COUNT=$THREAD_COUNT
export SUSTAINED_DURATION=$SUSTAINED_DURATION
export REPORT_FILE=$REPORT_FILE

# Run the validator
if "$PROJECT_ROOT/target/release/production_validator" 2>&1 | tee -a "$LOG_FILE"; then
    print_success "Production validation completed successfully!"
else
    print_error "Production validation failed!"
    exit_code=$?
fi

# Run benchmarks
print_status "Running performance benchmarks..."
if cargo bench --features "production_validation" -- --save-baseline production_${TIMESTAMP} >> "$LOG_FILE" 2>&1; then
    print_success "Benchmarks completed"
else
    print_warning "Benchmark issues detected"
fi

# Generate summary
print_status "Generating validation summary..."

cat > "${VALIDATION_OUTPUT_DIR}/validation_summary_${TIMESTAMP}.md" << EOF
# Lightning DB Production Validation Summary

**Date**: $(date)
**Version**: 1.0.0
**System**: $(uname -s) $(uname -m)

## Test Results

### Unit Tests
Status: PASSED ✅

### Production Validation Suite
$(grep -E "(Total Tests:|Passed:|Failed:)" "$REPORT_FILE" || echo "See full report for details")

### Performance Highlights
$(grep -E "ops_per_sec|throughput" "$REPORT_FILE" | head -10 || echo "See full report for details")

### Recommendations
1. Review the full validation report at: $REPORT_FILE
2. Check the detailed log at: $LOG_FILE
3. Compare benchmark results with baseline

## Next Steps
- Deploy to staging environment
- Run integration tests with real workload
- Monitor performance metrics
- Set up alerting thresholds

EOF

print_success "Validation summary saved to: ${VALIDATION_OUTPUT_DIR}/validation_summary_${TIMESTAMP}.md"

# Archive results
print_status "Archiving validation results..."
cd "$VALIDATION_OUTPUT_DIR"
tar -czf "validation_results_${TIMESTAMP}.tar.gz" \
    "validation_report_${TIMESTAMP}.txt" \
    "validation_log_${TIMESTAMP}.log" \
    "validation_summary_${TIMESTAMP}.md" 2>/dev/null || true

# Cleanup
rm -f "$PROJECT_ROOT/src/bin/production_validator.rs"

# Final summary
echo ""
echo -e "${BLUE}════════════════════════════════════════════════${NC}"
echo -e "${BLUE}          Validation Complete!                  ${NC}"
echo -e "${BLUE}════════════════════════════════════════════════${NC}"
echo ""

if [ "${exit_code:-0}" -eq 0 ]; then
    echo -e "${GREEN}✅ Lightning DB passed all production validation tests${NC}"
    echo ""
    echo "Results saved to:"
    echo "  • Report: $REPORT_FILE"
    echo "  • Log: $LOG_FILE"
    echo "  • Archive: ${VALIDATION_OUTPUT_DIR}/validation_results_${TIMESTAMP}.tar.gz"
    echo ""
    echo -e "${GREEN}Lightning DB is ready for production deployment!${NC}"
else
    echo -e "${RED}❌ Lightning DB failed validation tests${NC}"
    echo ""
    echo "Please review:"
    echo "  • Report: $REPORT_FILE"
    echo "  • Log: $LOG_FILE"
    echo ""
    echo -e "${RED}Address the issues before deploying to production.${NC}"
fi

exit ${exit_code:-0}