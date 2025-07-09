#!/bin/bash
set -e

# CI Validation Script for Lightning DB
# This script validates CI configurations and runs basic checks

echo "🔍 Lightning DB CI Validation Script"
echo "===================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Functions
print_test() {
    echo -e "${BLUE}[TEST]${NC} $1"
    ((TESTS_RUN++))
}

print_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((TESTS_PASSED++))
}

print_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((TESTS_FAILED++))
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

# Check if running in CI or locally
if [ -n "$CI" ]; then
    print_info "Running in CI environment"
else
    print_info "Running locally"
fi

# Test 1: Validate YAML files
print_test "Validating GitHub Actions YAML files..."
if command -v yamllint &> /dev/null; then
    if yamllint .github/workflows/*.yml; then
        print_pass "YAML files are valid"
    else
        print_fail "YAML validation failed"
    fi
else
    print_info "yamllint not installed, skipping YAML validation"
fi

# Test 2: Check Rust toolchain
print_test "Checking Rust toolchain..."
if command -v cargo &> /dev/null; then
    RUST_VERSION=$(rustc --version)
    print_pass "Rust installed: $RUST_VERSION"
else
    print_fail "Rust not installed"
    exit 1
fi

# Test 3: Check formatting
print_test "Checking code formatting..."
if cargo fmt --all -- --check; then
    print_pass "Code formatting is correct"
else
    print_fail "Code formatting issues found. Run 'cargo fmt' to fix."
fi

# Test 4: Run clippy
print_test "Running clippy..."
if cargo clippy --all-targets --all-features -- -D warnings 2>/dev/null; then
    print_pass "Clippy passed with no warnings"
else
    print_fail "Clippy found issues"
fi

# Test 5: Check compilation
print_test "Checking compilation..."
if cargo check --all-targets --all-features; then
    print_pass "Code compiles successfully"
else
    print_fail "Compilation failed"
fi

# Test 6: Run tests
print_test "Running tests..."
if cargo test --all-features -- --test-threads=4; then
    print_pass "All tests passed"
else
    print_fail "Some tests failed"
fi

# Test 7: Check examples
print_test "Building examples..."
if cargo build --examples --all-features; then
    print_pass "All examples build successfully"
else
    print_fail "Example build failed"
fi

# Test 8: Check benchmarks compile
print_test "Checking benchmarks..."
if cargo bench --no-run --all-features; then
    print_pass "Benchmarks compile successfully"
else
    print_fail "Benchmark compilation failed"
fi

# Test 9: Check for security vulnerabilities
print_test "Checking for security vulnerabilities..."
if command -v cargo-audit &> /dev/null; then
    if cargo audit; then
        print_pass "No security vulnerabilities found"
    else
        print_fail "Security vulnerabilities detected"
    fi
else
    print_info "cargo-audit not installed, installing..."
    cargo install cargo-audit
    if cargo audit; then
        print_pass "No security vulnerabilities found"
    else
        print_fail "Security vulnerabilities detected"
    fi
fi

# Test 10: Check documentation
print_test "Building documentation..."
if cargo doc --no-deps --all-features; then
    print_pass "Documentation builds successfully"
else
    print_fail "Documentation build failed"
fi

# Test 11: Check for large files
print_test "Checking for large files..."
LARGE_FILES=$(find . -type f -size +1M -not -path "./target/*" -not -path "./.git/*" -not -path "./build/*" 2>/dev/null)
if [ -z "$LARGE_FILES" ]; then
    print_pass "No large files found"
else
    print_fail "Large files found:"
    echo "$LARGE_FILES"
fi

# Test 12: Validate Cargo.toml
print_test "Validating Cargo.toml..."
if cargo verify-project; then
    print_pass "Cargo.toml is valid"
else
    print_fail "Cargo.toml validation failed"
fi

# Test 13: Check for TODO/FIXME comments
print_test "Checking for TODO/FIXME comments..."
TODO_COUNT=$(grep -r "TODO\|FIXME" --include="*.rs" src/ 2>/dev/null | wc -l || echo "0")
if [ "$TODO_COUNT" -eq 0 ]; then
    print_pass "No TODO/FIXME comments found"
else
    print_info "Found $TODO_COUNT TODO/FIXME comments"
fi

# Test 14: License check
print_test "Checking license files..."
if [ -f "LICENSE" ] || [ -f "LICENSE.md" ] || [ -f "LICENSE.txt" ]; then
    print_pass "License file found"
else
    print_fail "No license file found"
fi

# Test 15: Platform-specific build test
print_test "Testing current platform build..."
CURRENT_TARGET=$(rustc -vV | sed -n 's|host: ||p')
if cargo build --release --target "$CURRENT_TARGET"; then
    print_pass "Platform-specific build successful for $CURRENT_TARGET"
    
    # Check binary size
    BINARY_PATH="target/$CURRENT_TARGET/release/lightning_db"
    if [ -f "$BINARY_PATH" ]; then
        BINARY_SIZE=$(ls -lh "$BINARY_PATH" | awk '{print $5}')
        print_info "Binary size: $BINARY_SIZE"
    fi
else
    print_fail "Platform-specific build failed"
fi

# Summary
echo ""
echo "===================================="
echo -e "${BLUE}Summary:${NC}"
echo -e "  Tests run: $TESTS_RUN"
echo -e "  ${GREEN}Passed: $TESTS_PASSED${NC}"
echo -e "  ${RED}Failed: $TESTS_FAILED${NC}"
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All CI validations passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some CI validations failed.${NC}"
    exit 1
fi