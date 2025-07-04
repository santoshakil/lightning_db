#!/bin/bash
# Lightning DB - unwrap() Removal Progress Tracker

echo "Lightning DB - unwrap() Removal Progress Tracker"
echo "================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to count unwraps in a directory
count_unwraps() {
    local dir=$1
    local count=$(rg -c "\.unwrap\(\)" "$dir" 2>/dev/null | awk -F: '{sum += $2} END {print sum}')
    echo ${count:-0}
}

# Total count
echo "Total unwrap() calls in codebase:"
total=$(rg "\.unwrap\(\)" --type rust . 2>/dev/null | grep -c "unwrap()")
echo -e "${RED}$total${NC} unwrap() calls found"
echo ""

# Priority 1: Critical paths
echo "Priority 1 - Critical Safety:"
echo "-----------------------------"
core_count=$(count_unwraps "src/lib.rs")
echo -e "Core API (src/lib.rs): ${RED}$core_count${NC}"

tx_count=$(count_unwraps "src/transaction.rs")
echo -e "Transaction (src/transaction.rs): ${RED}$tx_count${NC}"

mvcc_count=$(count_unwraps "src/transaction/mvcc.rs")
echo -e "MVCC (src/transaction/mvcc.rs): ${RED}$mvcc_count${NC}"
echo ""

# Priority 2: Storage
echo "Priority 2 - Storage Layer:"
echo "---------------------------"
storage_count=$(count_unwraps "src/storage/")
echo -e "Storage module total: ${YELLOW}$storage_count${NC}"

wal_count=$(count_unwraps "src/wal_improved.rs")
echo -e "WAL (src/wal_improved.rs): ${YELLOW}$wal_count${NC}"
echo ""

# Priority 3: Tools
echo "Priority 3 - Admin Tools:"
echo "-------------------------"
cli_count=$(count_unwraps "src/bin/")
echo -e "Binary tools total: ${YELLOW}$cli_count${NC}"
echo ""

# Priority 4: Monitoring
echo "Priority 4 - Monitoring:"
echo "------------------------"
metrics_count=$(count_unwraps "src/metrics.rs")
echo -e "Metrics (src/metrics.rs): ${GREEN}$metrics_count${NC}"

obs_count=$(count_unwraps "src/observability.rs")
echo -e "Observability (src/observability.rs): ${GREEN}$obs_count${NC}"
echo ""

# Examples and tests (lower priority)
echo "Priority 5 - Examples & Tests:"
echo "------------------------------"
examples_count=$(count_unwraps "examples/")
echo -e "Examples total: ${GREEN}$examples_count${NC}"

tests_count=$(count_unwraps "tests/")
echo -e "Tests total: ${GREEN}$tests_count${NC}"
echo ""

# Production code only
prod_count=$(rg -c "\.unwrap\(\)" --type rust src/ 2>/dev/null | awk -F: '{sum += $2} END {print sum}')
echo "======================================"
echo -e "Production code (src/) total: ${RED}$prod_count${NC}"
echo "======================================"
echo ""

# Most problematic files
echo "Top 10 files with most unwrap() calls:"
echo "--------------------------------------"
rg -c "\.unwrap\(\)" --type rust . 2>/dev/null | sort -t: -k2 -nr | head -10 | while read line; do
    file=$(echo $line | cut -d: -f1)
    count=$(echo $line | cut -d: -f2)
    if [[ $file == src/* ]]; then
        echo -e "${RED}$count${NC} - $file"
    elif [[ $file == examples/* ]] || [[ $file == tests/* ]]; then
        echo -e "${GREEN}$count${NC} - $file"
    else
        echo -e "${YELLOW}$count${NC} - $file"
    fi
done
echo ""

# Check for patterns that need fixing
echo "Common patterns found:"
echo "---------------------"
lock_unwraps=$(rg "\.(?:read|write)\(\)\.unwrap\(\)" --type rust src/ -c 2>/dev/null | awk -F: '{sum += $2} END {print sum}')
echo "Lock unwraps: ${lock_unwraps:-0}"

arc_unwraps=$(rg "Arc::try_unwrap.*\.unwrap\(\)" --type rust src/ -c 2>/dev/null | awk -F: '{sum += $2} END {print sum}')
echo "Arc unwraps: ${arc_unwraps:-0}"

get_unwraps=$(rg "\.get\(.*\)\.unwrap\(\)" --type rust src/ -c 2>/dev/null | awk -F: '{sum += $2} END {print sum}')
echo "HashMap/BTreeMap get unwraps: ${get_unwraps:-0}"

path_unwraps=$(rg "\.to_str\(\)\.unwrap\(\)" --type rust src/ -c 2>/dev/null | awk -F: '{sum += $2} END {print sum}')
echo "Path conversion unwraps: ${path_unwraps:-0}"

channel_unwraps=$(rg "\.(?:send|recv)\(.*\)\.unwrap\(\)" --type rust src/ -c 2>/dev/null | awk -F: '{sum += $2} END {print sum}')
echo "Channel unwraps: ${channel_unwraps:-0}"
echo ""

# Progress calculation
initial_count=1733
remaining=$prod_count
fixed=$((initial_count - remaining))
if [ $initial_count -gt 0 ]; then
    progress=$((fixed * 100 / initial_count))
else
    progress=0
fi

echo "======================================"
echo "PROGRESS SUMMARY:"
echo "======================================"
echo "Initial unwrap() count: $initial_count"
echo "Currently remaining: $remaining"
echo "Fixed so far: $fixed"
echo -e "Progress: ${GREEN}${progress}%${NC}"
echo "======================================"

# Save progress to file
date=$(date +%Y-%m-%d_%H-%M-%S)
echo "$date,$total,$prod_count,$fixed,$progress" >> unwrap_removal_progress.csv

echo ""
echo "Progress saved to unwrap_removal_progress.csv"