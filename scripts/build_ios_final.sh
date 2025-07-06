#!/bin/bash

# Lightning DB - iOS Build Script (Final)
# Builds iOS static libraries without zstd

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
BUILD_DIR="$PROJECT_ROOT/build/releases/ios"

echo "Building Lightning DB for iOS (no zstd)..."
echo "Project root: $PROJECT_ROOT"
echo "Build output: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"

# Add iOS targets
rustup target add aarch64-apple-ios x86_64-apple-ios aarch64-apple-ios-sim

# Build for iOS device (arm64)
echo ""
echo "Building for iOS device (arm64)..."
cargo build --release \
    --target aarch64-apple-ios \
    -p lightning_db_ffi \
    --no-default-features \
    --features ios

# Build for iOS simulator (x86_64)
echo ""
echo "Building for iOS simulator (x86_64)..."
cargo build --release \
    --target x86_64-apple-ios \
    -p lightning_db_ffi \
    --no-default-features \
    --features ios

# Build for iOS simulator (arm64)
echo ""
echo "Building for iOS simulator (arm64)..."
cargo build --release \
    --target aarch64-apple-ios-sim \
    -p lightning_db_ffi \
    --no-default-features \
    --features ios

# Create directories
mkdir -p "$BUILD_DIR/device"
mkdir -p "$BUILD_DIR/simulator"

# Copy device library
cp "$PROJECT_ROOT/target/aarch64-apple-ios/release/liblightning_db_ffi.a" "$BUILD_DIR/device/"

# Create fat library for simulators
echo ""
echo "Creating universal simulator library..."
lipo -create \
    "$PROJECT_ROOT/target/x86_64-apple-ios/release/liblightning_db_ffi.a" \
    "$PROJECT_ROOT/target/aarch64-apple-ios-sim/release/liblightning_db_ffi.a" \
    -output "$BUILD_DIR/simulator/liblightning_db_ffi.a"

# Create XCFramework
echo ""
echo "Creating XCFramework..."
XCFRAMEWORK_PATH="$BUILD_DIR/LightningDB.xcframework"
rm -rf "$XCFRAMEWORK_PATH"

xcodebuild -create-xcframework \
    -library "$BUILD_DIR/device/liblightning_db_ffi.a" \
    -library "$BUILD_DIR/simulator/liblightning_db_ffi.a" \
    -output "$XCFRAMEWORK_PATH"

# Copy header file
cp "$PROJECT_ROOT/packages/lightning_db_dart/src/lightning_db_ffi.h" "$BUILD_DIR/"

# Create module map
cat > "$BUILD_DIR/module.modulemap" << EOF
module LightningDB {
    header "lightning_db_ffi.h"
    export *
}
EOF

# Create release package
cd "$BUILD_DIR/.."
zip -r "lightning_db-ios-xcframework.zip" "ios"

echo ""
echo "✅ iOS build complete!"
echo "XCFramework: $XCFRAMEWORK_PATH"
echo "Release package: $BUILD_DIR/../lightning_db-ios-xcframework.zip"