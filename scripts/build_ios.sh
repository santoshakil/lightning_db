#!/bin/bash

# Lightning DB - iOS Build Script
# Builds iOS static libraries and XCFramework

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
BUILD_DIR="$PROJECT_ROOT/build/releases/ios"

echo "Building Lightning DB for iOS..."
echo "Project root: $PROJECT_ROOT"
echo "Build output: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"

# Set iOS SDK path
export SDKROOT=$(xcrun --sdk iphoneos --show-sdk-path)

# Add iOS targets
rustup target add aarch64-apple-ios x86_64-apple-ios aarch64-apple-ios-sim

# Build for iOS device (arm64)
echo ""
echo "Building for iOS device (arm64)..."
export IPHONEOS_DEPLOYMENT_TARGET=11.0
cargo build --release --target aarch64-apple-ios -p lightning_db_ffi

# Build for iOS simulator (x86_64)
echo ""
echo "Building for iOS simulator (x86_64)..."
export SDKROOT=$(xcrun --sdk iphonesimulator --show-sdk-path)
cargo build --release --target x86_64-apple-ios -p lightning_db_ffi

# Build for iOS simulator (arm64)
echo ""
echo "Building for iOS simulator (arm64)..."
cargo build --release --target aarch64-apple-ios-sim -p lightning_db_ffi

# Create directories for libraries
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

# Create the module map
MODULE_MAP_PATH="$BUILD_DIR/module.modulemap"
cat > "$MODULE_MAP_PATH" << EOF
module LightningDB {
    header "lightning_db_ffi.h"
    export *
}
EOF

# Copy the header file
cp "$PROJECT_ROOT/packages/lightning_db_dart/src/lightning_db_ffi.h" "$BUILD_DIR/"

# Package for release
cd "$BUILD_DIR"
zip -r "../lightning_db-ios-xcframework.zip" "LightningDB.xcframework" "lightning_db_ffi.h" "module.modulemap"

echo ""
echo "✅ iOS build complete!"
echo "XCFramework created at: $XCFRAMEWORK_PATH"
echo "Release package: $BUILD_DIR/../lightning_db-ios-xcframework.zip"