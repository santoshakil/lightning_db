#!/bin/bash

# Lightning DB Concurrent Stress Test
# This script tests concurrent operations under heavy load

CLI_PATH="./target/release/lightning-cli"
DB_PATH="stress_test_db/test.db"
LOG_FILE="stress_test_results.log"
CONCURRENT_WORKERS=10
OPERATIONS_PER_WORKER=1000

echo "🚀 Lightning DB Concurrent Stress Test" | tee $LOG_FILE
echo "=====================================" | tee -a $LOG_FILE
echo "Workers: $CONCURRENT_WORKERS" | tee -a $LOG_FILE
echo "Operations per worker: $OPERATIONS_PER_WORKER" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

# Clean up previous test
rm -rf stress_test_db
mkdir -p stress_test_db

# Create database
echo "Creating test database..." | tee -a $LOG_FILE
$CLI_PATH create $DB_PATH

# Function to run worker operations
run_worker() {
    local worker_id=$1
    local start_time=$(date +%s.%N)
    
    echo "[Worker $worker_id] Starting..." >> $LOG_FILE
    
    for i in $(seq 1 $OPERATIONS_PER_WORKER); do
        # Mix of operations
        case $((i % 4)) in
            0) # PUT operation
                $CLI_PATH put $DB_PATH "worker${worker_id}_key${i}" "value_${worker_id}_${i}_$(date +%s%N)" 2>&1 | grep -q "error" && echo "[Worker $worker_id] PUT error at $i" >> $LOG_FILE
                ;;
            1) # GET operation
                $CLI_PATH get $DB_PATH "worker${worker_id}_key$((i-1))" > /dev/null 2>&1
                ;;
            2) # UPDATE operation
                $CLI_PATH put $DB_PATH "worker${worker_id}_key$((i-2))" "updated_${worker_id}_${i}_$(date +%s%N)" 2>&1 | grep -q "error" && echo "[Worker $worker_id] UPDATE error at $i" >> $LOG_FILE
                ;;
            3) # DELETE operation (every 10th key)
                if [ $((i % 10)) -eq 0 ]; then
                    $CLI_PATH delete $DB_PATH "worker${worker_id}_key$((i-3))" 2>&1 | grep -q "error" && echo "[Worker $worker_id] DELETE error at $i" >> $LOG_FILE
                fi
                ;;
        esac
        
        # Progress indicator every 100 operations
        if [ $((i % 100)) -eq 0 ]; then
            echo "[Worker $worker_id] Progress: $i/$OPERATIONS_PER_WORKER" >> $LOG_FILE
        fi
    done
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    echo "[Worker $worker_id] Completed in ${duration}s" >> $LOG_FILE
}

# Start time
TEST_START=$(date +%s.%N)

# Launch concurrent workers
echo "Launching $CONCURRENT_WORKERS concurrent workers..." | tee -a $LOG_FILE
for worker in $(seq 1 $CONCURRENT_WORKERS); do
    run_worker $worker &
done

# Wait for all workers to complete
echo "Waiting for workers to complete..." | tee -a $LOG_FILE
wait

# End time
TEST_END=$(date +%s.%N)
TEST_DURATION=$(echo "$TEST_END - $TEST_START" | bc)

echo "" | tee -a $LOG_FILE
echo "✅ All workers completed!" | tee -a $LOG_FILE
echo "Total test duration: ${TEST_DURATION}s" | tee -a $LOG_FILE

# Verify database integrity
echo "" | tee -a $LOG_FILE
echo "Verifying database integrity..." | tee -a $LOG_FILE
$CLI_PATH check $DB_PATH | tee -a $LOG_FILE

# Get final statistics
echo "" | tee -a $LOG_FILE
echo "Final database statistics:" | tee -a $LOG_FILE
$CLI_PATH stats $DB_PATH | tee -a $LOG_FILE

# Count total keys
echo "" | tee -a $LOG_FILE
echo "Counting total keys in database..." | tee -a $LOG_FILE
KEY_COUNT=$($CLI_PATH scan $DB_PATH 2>/dev/null | grep -c "Key:")
echo "Total keys: $KEY_COUNT" | tee -a $LOG_FILE

# Calculate operations per second
TOTAL_OPS=$((CONCURRENT_WORKERS * OPERATIONS_PER_WORKER))
OPS_PER_SEC=$(echo "scale=2; $TOTAL_OPS / $TEST_DURATION" | bc)
echo "Total operations: $TOTAL_OPS" | tee -a $LOG_FILE
echo "Operations per second: $OPS_PER_SEC" | tee -a $LOG_FILE

# Check for errors in log
echo "" | tee -a $LOG_FILE
ERROR_COUNT=$(grep -c "error" $LOG_FILE)
if [ $ERROR_COUNT -eq 0 ]; then
    echo "✅ No errors detected during stress test!" | tee -a $LOG_FILE
else
    echo "⚠️  $ERROR_COUNT errors detected during stress test" | tee -a $LOG_FILE
fi

echo "" | tee -a $LOG_FILE
echo "🏁 Stress test complete. Results saved to $LOG_FILE" | tee -a $LOG_FILE