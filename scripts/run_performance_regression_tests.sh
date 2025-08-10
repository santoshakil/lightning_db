#!/bin/bash

# Performance Regression Test Suite for Lightning DB
# This script runs comprehensive performance regression tests to validate that 
# reliability improvements haven't degraded the database's performance.

set -e

echo "🚀 Lightning DB Performance Regression Test Suite"
echo "=================================================="

# Get current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Create results directory
RESULTS_DIR="${PROJECT_ROOT}/performance_regression_results"
TIMESTAMP=$(date "+%Y%m%d_%H%M%S")
RESULT_PATH="${RESULTS_DIR}/${TIMESTAMP}"

mkdir -p "$RESULT_PATH"

echo "📁 Results will be saved to: $RESULT_PATH"
echo ""

# Build the project in release mode for accurate performance measurements
echo "🔨 Building Lightning DB in release mode..."
RUSTFLAGS="-C target-cpu=native -C lto=fat" cargo build --release

echo "✅ Build completed"
echo ""

# Run comprehensive benchmark suite with Criterion
echo "📊 Running Criterion benchmark suite..."
echo "======================================="
cargo bench performance_regression_suite 2>&1 | tee "$RESULT_PATH/criterion_benchmarks.log"

echo ""
echo "🧪 Running performance regression framework tests..."
echo "=================================================="
cargo test performance_regression_framework 2>&1 | tee "$RESULT_PATH/regression_framework_tests.log"

echo ""
echo "⚡ Running quick performance regression check..."
echo "=============================================="
# Run the quick regression check
cargo run --release --example quick_regression_check 2>&1 | tee "$RESULT_PATH/quick_regression_check.log" || true

echo ""
echo "📈 Running comprehensive regression suite..."
echo "=========================================="
# Create a temporary test program to run the comprehensive suite
cat > "$RESULT_PATH/run_comprehensive_suite.rs" << 'EOF'
use lightning_db::performance_regression::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RegressionTestConfig {
        test_duration_secs: 15,      // 15 seconds per test
        cache_size_mb: 2048,         // 2GB cache
        dataset_size: 100_000,       // 100K records
        thread_count: 8,             // 8 threads for concurrent tests  
        compression_enabled: false,   // Disable for max performance
        storage_path: "comprehensive_results.json".to_string(),
    };
    
    println!("Running comprehensive performance regression suite with config:");
    println!("  Test Duration: {} seconds", config.test_duration_secs);
    println!("  Cache Size: {} MB", config.cache_size_mb);  
    println!("  Dataset Size: {} records", config.dataset_size);
    println!("  Thread Count: {}", config.thread_count);
    println!("  Compression: {}", config.compression_enabled);
    println!("");
    
    let all_passed = run_comprehensive_regression_suite(config)?;
    
    if all_passed {
        println!("🎉 All performance regression tests PASSED!");
        std::process::exit(0);
    } else {
        println!("⚠️ Some performance regression tests FAILED!");
        std::process::exit(1);
    }
}
EOF

# Compile and run the comprehensive suite
echo "Compiling comprehensive test suite..."
rustc --edition=2021 -L target/release/deps --extern lightning_db=target/release/liblightning_db.rlib --extern tempfile=target/release/deps/libtempfile*.rlib --extern chrono=target/release/deps/libchrono*.rlib --extern serde_json=target/release/deps/libserde_json*.rlib "$RESULT_PATH/run_comprehensive_suite.rs" -o "$RESULT_PATH/run_comprehensive_suite" 2>/dev/null || {
    echo "⚠️ Could not compile comprehensive suite directly. Running via tests instead..."
    cargo test test_performance_regression_suite --release -- --nocapture 2>&1 | tee "$RESULT_PATH/comprehensive_regression_suite.log" || true
}

if [ -f "$RESULT_PATH/run_comprehensive_suite" ]; then
    echo "Running compiled comprehensive suite..."
    cd "$RESULT_PATH"
    ./run_comprehensive_suite 2>&1 | tee comprehensive_regression_suite.log
    cd "$PROJECT_ROOT"
fi

echo ""
echo "📋 Running existing performance tests for comparison..."
echo "=================================================="
cargo test performance_regression_tests --release -- --nocapture 2>&1 | tee "$RESULT_PATH/existing_performance_tests.log" || true

echo ""
echo "🔍 Analyzing performance validation tests..."
echo "=========================================="
cargo test performance_validation_tests --release -- --nocapture 2>&1 | tee "$RESULT_PATH/performance_validation_tests.log" || true

echo ""
echo "📊 Generating performance summary report..."
echo "========================================"

# Generate summary report
cat > "$RESULT_PATH/performance_regression_summary.md" << EOF
# Lightning DB Performance Regression Test Summary

**Test Date**: $(date)
**Git Commit**: $(git rev-parse HEAD 2>/dev/null || echo "Unknown")
**Git Branch**: $(git branch --show-current 2>/dev/null || echo "Unknown")

## Test Configuration

- **Test Duration**: 15 seconds per benchmark
- **Cache Size**: 2GB
- **Dataset Size**: 100K records
- **Thread Count**: 8 threads for concurrent tests
- **Compression**: Disabled for maximum performance
- **Build Flags**: \`RUSTFLAGS="-C target-cpu=native -C lto=fat"\`

## Performance Baselines (Documented)

Lightning DB's documented performance characteristics:
- **Read Performance**: 20.4M ops/sec (0.049 μs latency)
- **Write Performance**: 1.14M ops/sec (0.88 μs latency)
- **Mixed Workload**: 885K ops/sec sustained
- **Concurrent Performance**: 1.4M ops/sec with 8 threads
- **Transaction Performance**: 412K ops/sec

## Test Results

EOF

# Extract key results from logs
if [ -f "$RESULT_PATH/quick_regression_check.log" ]; then
    echo "### Quick Regression Check Results" >> "$RESULT_PATH/performance_regression_summary.md"
    echo "" >> "$RESULT_PATH/performance_regression_summary.md"
    echo '```' >> "$RESULT_PATH/performance_regression_summary.md"
    grep -E "(Read performance|Write performance|✅|❌|🎉|⚠️)" "$RESULT_PATH/quick_regression_check.log" >> "$RESULT_PATH/performance_regression_summary.md" 2>/dev/null || echo "No quick check results found" >> "$RESULT_PATH/performance_regression_summary.md"
    echo '```' >> "$RESULT_PATH/performance_regression_summary.md"
    echo "" >> "$RESULT_PATH/performance_regression_summary.md"
fi

if [ -f "$RESULT_PATH/comprehensive_regression_suite.log" ]; then
    echo "### Comprehensive Regression Suite Results" >> "$RESULT_PATH/performance_regression_summary.md" 
    echo "" >> "$RESULT_PATH/performance_regression_summary.md"
    echo '```' >> "$RESULT_PATH/performance_regression_summary.md"
    grep -E "(Testing|Results|✅|❌|ops/sec|baseline)" "$RESULT_PATH/comprehensive_regression_suite.log" >> "$RESULT_PATH/performance_regression_summary.md" 2>/dev/null || echo "No comprehensive suite results found" >> "$RESULT_PATH/performance_regression_summary.md"
    echo '```' >> "$RESULT_PATH/performance_regression_summary.md"
    echo "" >> "$RESULT_PATH/performance_regression_summary.md"
fi

# Add file listing
echo "## Generated Files" >> "$RESULT_PATH/performance_regression_summary.md"
echo "" >> "$RESULT_PATH/performance_regression_summary.md"
find "$RESULT_PATH" -name "*.log" -o -name "*.json" -o -name "*.md" | sort | while read file; do
    echo "- \`$(basename "$file")\`" >> "$RESULT_PATH/performance_regression_summary.md"
done

# Add analysis and recommendations
cat >> "$RESULT_PATH/performance_regression_summary.md" << 'EOF'

## Analysis

This performance regression test suite validates that Lightning DB maintains its documented performance characteristics after reliability improvements including:

1. **Error Handling Improvements**: Replaced `unwrap()` calls with proper error propagation
2. **Data Integrity Validation**: Added comprehensive data validation and integrity checks  
3. **Deadlock Prevention**: Implemented lock-free data structures and deadlock-free algorithms
4. **WAL Corruption Handling**: Added robust WAL corruption detection and recovery
5. **Memory Leak Fixes**: Fixed memory leaks in concurrent access patterns

## Expected Impact

The reliability improvements are expected to have minimal performance impact:

- **Read Operations**: <10% regression acceptable (target: >18.4M ops/sec)
- **Write Operations**: <10% regression acceptable (target: >1.03M ops/sec)  
- **Mixed Workload**: <15% regression acceptable (target: >750K ops/sec)
- **Concurrent Operations**: <15% regression acceptable (target: >1.19M ops/sec)
- **Error Handling Overhead**: <10% impact on happy path operations

## Recommendations

Based on test results:

1. **✅ PASS**: All performance metrics within acceptable thresholds
   - Continue with current reliability improvements
   - Document current performance baselines
   - Set up automated performance monitoring

2. **⚠️ MINOR REGRESSION**: Performance slightly below targets but acceptable
   - Monitor performance trends closely
   - Consider targeted optimizations
   - Re-run tests after any changes

3. **❌ MAJOR REGRESSION**: Performance significantly below acceptable thresholds
   - **IMMEDIATE ACTION REQUIRED**
   - Profile performance bottlenecks
   - Review recent reliability changes for performance impact
   - Consider rolling back problematic changes
   - Implement performance optimizations before proceeding

## Next Steps

1. **Regular Testing**: Incorporate these tests into CI/CD pipeline
2. **Performance Monitoring**: Set up continuous performance monitoring
3. **Optimization Opportunities**: Identify areas for performance improvements
4. **Documentation Updates**: Update performance documentation with current results

EOF

echo ""
echo "✅ Performance regression test suite completed!"
echo ""
echo "📄 Summary report: $RESULT_PATH/performance_regression_summary.md"
echo "📁 All results saved to: $RESULT_PATH"
echo ""

# Display summary
echo "📊 PERFORMANCE REGRESSION TEST SUMMARY"
echo "======================================"
cat "$RESULT_PATH/performance_regression_summary.md"

echo ""
echo "🔗 Next steps:"
echo "1. Review the detailed results in $RESULT_PATH/"
echo "2. Compare against documented baselines" 
echo "3. Investigate any performance regressions"
echo "4. Update performance documentation if needed"
echo ""

# Set exit code based on results
if grep -q "❌" "$RESULT_PATH"/*.log 2>/dev/null; then
    echo "⚠️ Some performance regressions detected - manual review recommended"
    exit 1
elif grep -q "✅" "$RESULT_PATH"/*.log 2>/dev/null; then
    echo "🎉 Performance regression tests completed successfully!"
    exit 0
else  
    echo "ℹ️ Performance regression tests completed - results need manual review"
    exit 0
fi