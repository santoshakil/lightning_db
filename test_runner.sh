#!/bin/bash

echo "🧪 Lightning DB Test Runner"
echo "=========================="

# Key examples to test
examples=(
    "quick_summary"
    "concurrent_test" 
    "simple_test"
    "final_benchmark"
    "feature_summary"
)

failed=0
passed=0

for example in "${examples[@]}"; do
    echo -n "Testing $example... "
    
    # Run with 60 second timeout
    if timeout 60s cargo run --release --example $example > /dev/null 2>&1; then
        echo "✅ PASS"
        ((passed++))
    else
        echo "❌ FAIL"
        ((failed++))
    fi
done

echo ""
echo "📊 Results: $passed passed, $failed failed"

if [ $failed -eq 0 ]; then
    echo "🎉 All tests passed!"
    exit 0
else
    echo "⚠️  Some tests failed"
    exit 1
fi