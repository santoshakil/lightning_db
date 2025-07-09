#!/bin/bash
set -e

echo "🧪 Testing CI locally..."

# Check formatting
echo "📝 Checking formatting..."
cargo fmt -- --check
if [ $? -eq 0 ]; then
    echo "✅ Formatting check passed"
else
    echo "❌ Formatting issues found. Run 'cargo fmt' to fix."
    exit 1
fi

# Check clippy
echo "🔍 Running clippy..."
cargo clippy --all-targets --all-features -- -D warnings
if [ $? -eq 0 ]; then
    echo "✅ Clippy check passed"
else
    echo "❌ Clippy warnings found"
    exit 1
fi

# Build FFI library
echo "🔨 Building FFI library..."
cd lightning_db_ffi
cargo build --release
cd ..

# Check FFI library exists
if [ -f "lightning_db_ffi/target/release/liblightning_db_ffi.so" ] || \
   [ -f "lightning_db_ffi/target/release/liblightning_db_ffi.dylib" ] || \
   [ -f "lightning_db_ffi/target/release/lightning_db_ffi.dll" ]; then
    echo "✅ FFI library built successfully"
else
    echo "❌ FFI library not found"
    exit 1
fi

# Test copying libraries (simulate CI)
echo "📦 Testing library setup..."
mkdir -p packages/lightning_db_dart/linux
mkdir -p packages/lightning_db_dart/macos
mkdir -p packages/lightning_db_dart/windows

# Copy based on platform
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    cp lightning_db_ffi/target/release/liblightning_db_ffi.so packages/lightning_db_dart/linux/
elif [[ "$OSTYPE" == "darwin"* ]]; then
    cp lightning_db_ffi/target/release/liblightning_db_ffi.dylib packages/lightning_db_dart/macos/
fi

echo "✅ Library setup successful"

# Run tests
echo "🧪 Running tests..."
cargo test --all-features

echo "✅ All CI checks passed locally!"