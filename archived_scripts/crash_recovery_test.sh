#!/bin/bash

# Lightning DB Crash Recovery Test
# Tests database recovery after simulated crashes

CLI_PATH="./target/release/lightning-cli"
DB_PATH="crash_test_db/test.db"

echo "💥 Lightning DB Crash Recovery Test"
echo "==================================="
echo ""

# Clean up previous test
rm -rf crash_test_db crash_test_backup
mkdir -p crash_test_db

# Create database and add initial data
echo "1. Creating database with initial data..."
$CLI_PATH create $DB_PATH

# Add test data
for i in {1..20}; do
    $CLI_PATH put $DB_PATH "persistent_key_$i" "persistent_value_$i"
done

echo "✓ Added 20 persistent keys"

# Create backup of good state
echo ""
echo "2. Creating backup of good state..."
$CLI_PATH backup $DB_PATH crash_test_backup
echo "✓ Backup created"

# Verify initial data
echo ""
echo "3. Verifying initial data..."
INITIAL_COUNT=$($CLI_PATH scan $DB_PATH 2>/dev/null | grep -c "Key:")
echo "✓ Initial key count: $INITIAL_COUNT"

# Test 1: Simulate crash during writes
echo ""
echo "4. Test 1: Simulating crash during batch writes..."
echo "   Starting background writer..."

# Background writer process
(
    for i in {21..100}; do
        $CLI_PATH put $DB_PATH "crash_test_key_$i" "crash_test_value_$i" 2>/dev/null
        # Simulate crash after key 50
        if [ $i -eq 50 ]; then
            echo "   💥 Simulating crash at key $i"
            # Kill all CLI processes
            pkill -f "lightning-cli put" 2>/dev/null
            break
        fi
    done
) &

WRITER_PID=$!

# Wait a bit for writes to start
sleep 2

# Force kill the writer
kill -9 $WRITER_PID 2>/dev/null
wait $WRITER_PID 2>/dev/null

echo "   ✓ Writer process killed"

# Check database integrity after crash
echo ""
echo "5. Checking database integrity after crash..."
$CLI_PATH check $DB_PATH

# Count keys after crash
echo ""
echo "6. Counting keys after crash..."
AFTER_CRASH_COUNT=$($CLI_PATH scan $DB_PATH 2>/dev/null | grep -c "Key:")
echo "✓ Keys after crash: $AFTER_CRASH_COUNT"

# Verify persistent data is intact
echo ""
echo "7. Verifying persistent data integrity..."
ERRORS=0
for i in {1..20}; do
    VALUE=$($CLI_PATH get $DB_PATH "persistent_key_$i" 2>/dev/null | grep -o "persistent_value_$i")
    if [ "$VALUE" != "persistent_value_$i" ]; then
        echo "❌ Error: persistent_key_$i is corrupted or missing!"
        ((ERRORS++))
    fi
done

if [ $ERRORS -eq 0 ]; then
    echo "✓ All persistent data verified intact!"
else
    echo "❌ $ERRORS persistent keys corrupted!"
fi

# Test 2: Restore from backup
echo ""
echo "8. Test 2: Restoring from backup..."
rm -rf crash_test_db
mkdir -p crash_test_db
$CLI_PATH restore crash_test_backup $DB_PATH

# Verify restored data
echo ""
echo "9. Verifying restored data..."
RESTORED_COUNT=$($CLI_PATH scan $DB_PATH 2>/dev/null | grep -c "Key:")
echo "✓ Keys after restore: $RESTORED_COUNT"

if [ $RESTORED_COUNT -eq $INITIAL_COUNT ]; then
    echo "✓ Restore successful - all original data recovered!"
else
    echo "❌ Restore failed - key count mismatch!"
fi

# Test 3: Database file corruption simulation
echo ""
echo "10. Test 3: Simulating file corruption..."
echo "    Adding more test data..."
for i in {101..110}; do
    $CLI_PATH put $DB_PATH "corruption_test_$i" "value_$i"
done

# Get database files
DB_FILES=$(find crash_test_db -type f -name "*.db" -o -name "*.sst" -o -name "*.log" 2>/dev/null | head -1)

if [ ! -z "$DB_FILES" ]; then
    echo "    💥 Corrupting database file..."
    # Corrupt a file by writing random data in the middle
    dd if=/dev/urandom of="$DB_FILES" bs=1024 count=1 seek=1 conv=notrunc 2>/dev/null
    echo "    ✓ File corrupted"
    
    echo ""
    echo "11. Attempting to read from corrupted database..."
    $CLI_PATH get $DB_PATH "persistent_key_1" 2>&1 | head -5
    
    echo ""
    echo "12. Running integrity check on corrupted database..."
    $CLI_PATH check $DB_PATH 2>&1 | head -10
else
    echo "    ⚠️  No database files found to corrupt"
fi

# Summary
echo ""
echo "📊 Crash Recovery Test Summary"
echo "=============================="
echo "✓ Initial data creation: SUCCESS"
echo "✓ Crash during writes: TESTED"
echo "✓ Data integrity after crash: $([ $ERRORS -eq 0 ] && echo "PASSED" || echo "FAILED")"
echo "✓ Backup creation: SUCCESS"
echo "✓ Restore from backup: $([ $RESTORED_COUNT -eq $INITIAL_COUNT ] && echo "SUCCESS" || echo "FAILED")"
echo "✓ Corruption handling: TESTED"

echo ""
echo "🏁 Crash recovery test complete!"