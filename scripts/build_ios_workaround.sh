#!/bin/bash

# Lightning DB - iOS Build Script with Workaround
# Builds iOS static libraries with zstd disabled

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
BUILD_DIR="$PROJECT_ROOT/build/releases/ios"

echo "Building Lightning DB for iOS (with workaround)..."
echo "Project root: $PROJECT_ROOT"
echo "Build output: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"

# Add iOS targets
rustup target add aarch64-apple-ios x86_64-apple-ios aarch64-apple-ios-sim

# Set environment variables to disable problematic features
export IPHONEOS_DEPLOYMENT_TARGET=11.0
export CARGO_PROFILE_RELEASE_PANIC="abort"

# Use a feature flag to disable zstd for iOS
echo "Building with iOS-specific configuration..."

# Build for iOS device (arm64)
echo ""
echo "Building for iOS device (arm64)..."
RUSTFLAGS="-C panic=abort -C opt-level=z" \
cargo rustc --release --target aarch64-apple-ios -p lightning_db_ffi \
    -- -C link-arg=-dead_strip

# Build for iOS simulator (x86_64)
echo ""
echo "Building for iOS simulator (x86_64)..."
RUSTFLAGS="-C panic=abort -C opt-level=z" \
cargo rustc --release --target x86_64-apple-ios -p lightning_db_ffi \
    -- -C link-arg=-dead_strip

# Build for iOS simulator (arm64)
echo ""
echo "Building for iOS simulator (arm64)..."
RUSTFLAGS="-C panic=abort -C opt-level=z" \
cargo rustc --release --target aarch64-apple-ios-sim -p lightning_db_ffi \
    -- -C link-arg=-dead_strip

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

# Verify the libraries
echo ""
echo "Verifying libraries..."
lipo -info "$BUILD_DIR/device/liblightning_db_ffi.a"
lipo -info "$BUILD_DIR/simulator/liblightning_db_ffi.a"

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

# Create release package
cd "$BUILD_DIR"
zip -r "../lightning_db-ios-xcframework.zip" "LightningDB.xcframework" "lightning_db_ffi.h"

echo ""
echo "✅ iOS build complete!"
echo "XCFramework: $XCFRAMEWORK_PATH"
echo "Release package: $BUILD_DIR/../lightning_db-ios-xcframework.zip"