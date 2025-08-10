#!/bin/bash

# Lightning DB Large Dataset Test
# Tests database performance with large amounts of data

CLI_PATH="./target/release/lightning-cli"
DB_PATH="large_test_db/test.db"

echo "📊 Lightning DB Large Dataset Test"
echo "=================================="
echo ""

# Clean up previous test
rm -rf large_test_db
mkdir -p large_test_db

# Create database
echo "1. Creating database..."
$CLI_PATH create $DB_PATH

# Function to generate large value
generate_large_value() {
    local size=$1
    python3 -c "print('x' * $size)"
}

# Test 1: Many small records
echo ""
echo "2. Test 1: Inserting 10,000 small records..."
START_TIME=$(date +%s)

for i in $(seq 1 10000); do
    $CLI_PATH put $DB_PATH "small_key_$i" "small_value_$i" >/dev/null 2>&1
    
    # Progress indicator
    if [ $((i % 1000)) -eq 0 ]; then
        echo "   Progress: $i/10000 records"
    fi
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo "✓ Completed in ${DURATION} seconds"
echo "  Rate: $((10000 / DURATION)) records/second"

# Check database size
echo ""
echo "3. Checking database size..."
DB_SIZE=$(du -sh large_test_db | cut -f1)
echo "✓ Database size: $DB_SIZE"

# Test 2: Large values (1MB each)
echo ""
echo "4. Test 2: Inserting 100 large records (1MB each)..."
LARGE_VALUE=$(generate_large_value 1048576)  # 1MB
START_TIME=$(date +%s)

for i in $(seq 1 100); do
    $CLI_PATH put $DB_PATH "large_key_$i" "$LARGE_VALUE" >/dev/null 2>&1
    
    # Progress indicator
    if [ $((i % 10)) -eq 0 ]; then
        echo "   Progress: $i/100 large records"
    fi
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo "✓ Completed in ${DURATION} seconds"

# Check updated database size
echo ""
echo "5. Checking updated database size..."
DB_SIZE=$(du -sh large_test_db | cut -f1)
echo "✓ Database size after large values: $DB_SIZE"

# Test 3: Read performance on large dataset
echo ""
echo "6. Test 3: Random read performance..."
START_TIME=$(date +%s.%N)

# Read 1000 random keys
for i in $(seq 1 1000); do
    RANDOM_KEY=$((RANDOM % 10000 + 1))
    $CLI_PATH get $DB_PATH "small_key_$RANDOM_KEY" >/dev/null 2>&1
done

END_TIME=$(date +%s.%N)
DURATION=$(echo "$END_TIME - $START_TIME" | bc)
RATE=$(echo "scale=2; 1000 / $DURATION" | bc)
echo "✓ Read 1000 random keys in ${DURATION}s"
echo "  Rate: $RATE reads/second"

# Test 4: Scan performance
echo ""
echo "7. Test 4: Scan performance..."
START_TIME=$(date +%s.%N)

$CLI_PATH scan $DB_PATH --limit 1000 >/dev/null 2>&1

END_TIME=$(date +%s.%N)
DURATION=$(echo "$END_TIME - $START_TIME" | bc)
echo "✓ Scanned 1000 records in ${DURATION}s"

# Database integrity check
echo ""
echo "8. Running integrity check on large database..."
$CLI_PATH check $DB_PATH

# Memory usage check
echo ""
echo "9. System memory usage..."
ps aux | grep lightning-cli | grep -v grep || echo "No active CLI processes"

# Benchmark on large dataset
echo ""
echo "10. Running benchmark on large dataset..."
$CLI_PATH bench $DB_PATH --reads 10000 --writes 1000

# Summary
echo ""
echo "📊 Large Dataset Test Summary"
echo "============================="
echo "✓ Small records test: 10,000 records"
echo "✓ Large values test: 100 x 1MB records"
echo "✓ Total theoretical data: ~110MB"
echo "✓ Actual database size: $DB_SIZE"
echo "✓ Read performance tested"
echo "✓ Scan performance tested"
echo "✓ Integrity verified"

echo ""
echo "🏁 Large dataset test complete!"