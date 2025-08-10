#!/bin/bash

# Quick concurrent test with fewer operations
CLI_PATH="./target/release/lightning-cli"
DB_PATH="quick_test_db/test.db"

echo "🧪 Quick Concurrent Test (5 workers, 10 ops each)"
echo "================================================"

# Clean up and create database
rm -rf quick_test_db
mkdir -p quick_test_db
$CLI_PATH create $DB_PATH

# Simple worker function
worker() {
    local id=$1
    echo "Worker $id starting..."
    for i in {1..10}; do
        $CLI_PATH put $DB_PATH "w${id}_k${i}" "value_${id}_${i}" 2>&1
    done
    echo "Worker $id done"
}

# Start time
START=$(date +%s)

# Launch 5 workers
for i in {1..5}; do
    worker $i &
done

# Wait for completion
wait

# End time
END=$(date +%s)
DURATION=$((END - START))

echo ""
echo "Test completed in ${DURATION} seconds"
echo ""

# Check results
echo "Database stats:"
$CLI_PATH stats $DB_PATH

echo ""
echo "Total keys:"
$CLI_PATH scan $DB_PATH | grep -c "Key:"

echo ""
echo "Database integrity:"
$CLI_PATH check $DB_PATH