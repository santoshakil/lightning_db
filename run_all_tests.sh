#!/bin/bash

echo "🧪 Lightning DB Test Suite"
echo "=========================="
echo ""

# Test pre-compiled examples
echo "Running available examples..."
echo ""

# Function to test an example if it exists
test_example() {
    local name="$1"
    local binary="./target/release/examples/$2"
    
    if [ -f "$binary" ]; then
        echo "▶️  Testing $name..."
        if $binary > /tmp/test_output.txt 2>&1; then
            if grep -q "❌\|FAIL" /tmp/test_output.txt; then
                echo "  ⚠️  Partial pass (some subtests failed)"
                grep -E "✅|❌|PASS|FAIL" /tmp/test_output.txt | tail -5
            else
                echo "  ✅ PASS"
            fi
        else
            echo "  ❌ FAIL"
        fi
        echo ""
    fi
}

# Core functionality tests
test_example "Quick Summary" "quick_summary"
test_example "Final Test Check" "final_test_check"
test_example "Concurrent Test" "concurrent_test"
test_example "Simple Test" "simple_test"
test_example "Minimal Test" "minimal_test"
test_example "Basic Usage" "basic_usage"
test_example "Memory Test" "memory_test"
test_example "Feature Test" "feature_test"
test_example "Debug Hanging" "debug_hanging"
test_example "Test Quick Cleanup" "test_quick_cleanup"

echo "=========================="
echo "✅ Test run complete"
echo ""
echo "To run unit tests: cargo test --release --lib"
echo "To run all tests: cargo test --release"