#!/bin/bash
# Simple unwrap counter

echo "Counting unwrap() calls in Lightning DB..."
echo ""

# Count in src directory
echo "=== Production Code (src/) ==="
find src -name "*.rs" -type f -exec grep -H "\.unwrap()" {} \; | wc -l

# Count by major module
echo ""
echo "=== By Module ==="
echo -n "src/lib.rs: "
grep "\.unwrap()" src/lib.rs 2>/dev/null | wc -l

echo -n "src/transaction.rs: "
grep "\.unwrap()" src/transaction.rs 2>/dev/null | wc -l

echo -n "src/transaction/ (all): "
find src/transaction -name "*.rs" -exec grep "\.unwrap()" {} \; 2>/dev/null | wc -l

echo -n "src/storage/ (all): "
find src/storage -name "*.rs" -exec grep "\.unwrap()" {} \; 2>/dev/null | wc -l

echo -n "src/bin/ (all): "
find src/bin -name "*.rs" -exec grep "\.unwrap()" {} \; 2>/dev/null | wc -l

echo ""
echo "=== Examples ==="
find examples -name "*.rs" -exec grep "\.unwrap()" {} \; 2>/dev/null | wc -l

echo ""
echo "=== Tests ==="
find tests -name "*.rs" -exec grep "\.unwrap()" {} \; 2>/dev/null | wc -l

echo ""
echo "=== TOTAL ==="
find . -name "*.rs" -type f -exec grep "\.unwrap()" {} \; 2>/dev/null | wc -l