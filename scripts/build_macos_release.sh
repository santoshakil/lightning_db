#!/bin/bash

# Lightning DB - macOS Release Build Script
# Builds macOS universal binary and iOS xcframework

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
BUILD_DIR="$PROJECT_ROOT/build/releases"

echo "Building Lightning DB for macOS and iOS..."
echo "Project root: $PROJECT_ROOT"
echo "Build output: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"

# Build for macOS architectures
echo ""
echo "Building macOS libraries..."

# Build arm64
echo "Building for aarch64-apple-darwin..."
cargo build --release --target aarch64-apple-darwin -p lightning_db_ffi

# Build x86_64
echo "Building for x86_64-apple-darwin..."
cargo build --release --target x86_64-apple-darwin -p lightning_db_ffi

# Create universal binary
echo "Creating macOS universal binary..."
mkdir -p "$BUILD_DIR/macos"
lipo -create \
    "$PROJECT_ROOT/target/aarch64-apple-darwin/release/liblightning_db_ffi.dylib" \
    "$PROJECT_ROOT/target/x86_64-apple-darwin/release/liblightning_db_ffi.dylib" \
    -output "$BUILD_DIR/macos/liblightning_db.dylib"
echo "✓ Created universal binary"

# Build for iOS
echo ""
echo "Building iOS libraries..."

# Add iOS targets
rustup target add aarch64-apple-ios x86_64-apple-ios aarch64-apple-ios-sim

# Build iOS device (arm64)
echo "Building for aarch64-apple-ios..."
cargo build --release --target aarch64-apple-ios -p lightning_db_ffi

# Build iOS simulator (x86_64)
echo "Building for x86_64-apple-ios..."
cargo build --release --target x86_64-apple-ios -p lightning_db_ffi

# Build iOS simulator (arm64)
echo "Building for aarch64-apple-ios-sim..."
cargo build --release --target aarch64-apple-ios-sim -p lightning_db_ffi

# Create XCFramework
echo "Creating iOS XCFramework..."
XCFRAMEWORK_DIR="$BUILD_DIR/ios/LightningDB.xcframework"
rm -rf "$XCFRAMEWORK_DIR"

# Create fat library for simulators
mkdir -p "$BUILD_DIR/ios/sim"
lipo -create \
    "$PROJECT_ROOT/target/x86_64-apple-ios/release/liblightning_db_ffi.a" \
    "$PROJECT_ROOT/target/aarch64-apple-ios-sim/release/liblightning_db_ffi.a" \
    -output "$BUILD_DIR/ios/sim/liblightning_db.a"

xcodebuild -create-xcframework \
    -library "$PROJECT_ROOT/target/aarch64-apple-ios/release/liblightning_db_ffi.a" \
    -library "$BUILD_DIR/ios/sim/liblightning_db.a" \
    -output "$XCFRAMEWORK_DIR"

echo "✓ Created XCFramework"

# Create release packages
echo ""
echo "Creating release packages..."

cd "$BUILD_DIR"

# Package macOS
zip -r "lightning_db-macos.zip" "macos"
echo "✓ Created lightning_db-macos.zip"

# Package iOS
cd ios
zip -r "../lightning_db-ios-xcframework.zip" "LightningDB.xcframework"
cd ..
echo "✓ Created lightning_db-ios-xcframework.zip"

echo ""
echo "✅ Build complete!"
echo "Release packages created in: $BUILD_DIR"
echo ""
echo "Files ready for release:"
ls -la "$BUILD_DIR"/*.zip