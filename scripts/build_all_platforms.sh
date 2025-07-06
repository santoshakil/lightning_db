#!/bin/bash

# Lightning DB - Build script for all platforms
# This script builds the native library for all supported platforms

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
BUILD_DIR="$PROJECT_ROOT/build/releases"

echo "Building Lightning DB for all platforms..."
echo "Project root: $PROJECT_ROOT"
echo "Build output: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"

# Function to build for a specific target
build_target() {
    local TARGET=$1
    local OUTPUT_NAME=$2
    local OUTPUT_DIR="$BUILD_DIR/$TARGET"
    
    echo ""
    echo "Building for $TARGET..."
    
    mkdir -p "$OUTPUT_DIR"
    
    # Add target if not already installed
    rustup target add "$TARGET" 2>/dev/null || true
    
    # Build
    cargo build --release --target "$TARGET"
    
    # Copy output
    if [[ "$TARGET" == *"windows"* ]]; then
        cp "$PROJECT_ROOT/target/$TARGET/release/lightning_db.dll" "$OUTPUT_DIR/$OUTPUT_NAME"
    elif [[ "$TARGET" == *"darwin"* ]]; then
        cp "$PROJECT_ROOT/target/$TARGET/release/liblightning_db.dylib" "$OUTPUT_DIR/$OUTPUT_NAME"
    else
        cp "$PROJECT_ROOT/target/$TARGET/release/liblightning_db.so" "$OUTPUT_DIR/$OUTPUT_NAME"
    fi
    
    echo "✓ Built $TARGET"
}

# Build for all targets
echo ""
echo "Building native libraries..."

# macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    build_target "aarch64-apple-darwin" "liblightning_db.dylib"
    build_target "x86_64-apple-darwin" "liblightning_db.dylib"
    
    # Create universal binary
    echo "Creating macOS universal binary..."
    mkdir -p "$BUILD_DIR/macos-universal"
    lipo -create \
        "$BUILD_DIR/aarch64-apple-darwin/liblightning_db.dylib" \
        "$BUILD_DIR/x86_64-apple-darwin/liblightning_db.dylib" \
        -output "$BUILD_DIR/macos-universal/liblightning_db.dylib"
    echo "✓ Created universal binary"
fi

# Linux
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    build_target "x86_64-unknown-linux-gnu" "liblightning_db.so"
fi

# Android (requires Android NDK)
if command -v cargo-ndk &> /dev/null; then
    echo ""
    echo "Building for Android..."
    
    # Install cargo-ndk if not present
    cargo install cargo-ndk
    
    # Build for each Android architecture
    cargo ndk -t armeabi-v7a -t arm64-v8a -t x86 -t x86_64 \
        -o "$BUILD_DIR/android" \
        build --release
    
    echo "✓ Built Android libraries"
else
    echo "⚠️  Skipping Android build (cargo-ndk not found)"
fi

# iOS (requires macOS)
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo ""
    echo "Building for iOS..."
    
    # Build for iOS targets
    build_target "aarch64-apple-ios" "liblightning_db.a"
    build_target "x86_64-apple-ios" "liblightning_db.a"
    
    # Create XCFramework
    echo "Creating iOS XCFramework..."
    XCFRAMEWORK_DIR="$BUILD_DIR/ios/LightningDB.xcframework"
    rm -rf "$XCFRAMEWORK_DIR"
    
    xcodebuild -create-xcframework \
        -library "$BUILD_DIR/aarch64-apple-ios/liblightning_db.a" \
        -library "$BUILD_DIR/x86_64-apple-ios/liblightning_db.a" \
        -output "$XCFRAMEWORK_DIR"
    
    echo "✓ Created XCFramework"
fi

# Windows (requires Windows or cross-compilation setup)
if command -v x86_64-pc-windows-gnu-gcc &> /dev/null; then
    build_target "x86_64-pc-windows-gnu" "lightning_db.dll"
else
    echo "⚠️  Skipping Windows build (cross-compilation tools not found)"
fi

# Create release packages
echo ""
echo "Creating release packages..."

cd "$BUILD_DIR"

# Package macOS
if [[ -d "macos-universal" ]]; then
    zip -r "lightning_db-macos.zip" "macos-universal"
    echo "✓ Created lightning_db-macos.zip"
fi

# Package Linux
if [[ -d "x86_64-unknown-linux-gnu" ]]; then
    zip -r "lightning_db-linux-x64.zip" "x86_64-unknown-linux-gnu"
    echo "✓ Created lightning_db-linux-x64.zip"
fi

# Package Android
if [[ -d "android" ]]; then
    cd android
    for ARCH in arm64-v8a armeabi-v7a x86 x86_64; do
        if [[ -d "$ARCH" ]]; then
            zip -r "../lightning_db-android-$ARCH.zip" "$ARCH"
            echo "✓ Created lightning_db-android-$ARCH.zip"
        fi
    done
    cd ..
fi

# Package iOS
if [[ -d "ios/LightningDB.xcframework" ]]; then
    cd ios
    zip -r "../lightning_db-ios-xcframework.zip" "LightningDB.xcframework"
    echo "✓ Created lightning_db-ios-xcframework.zip"
    cd ..
fi

# Package Windows
if [[ -d "x86_64-pc-windows-gnu" ]]; then
    zip -r "lightning_db-windows-x64.zip" "x86_64-pc-windows-gnu"
    echo "✓ Created lightning_db-windows-x64.zip"
fi

echo ""
echo "✅ Build complete!"
echo "Release packages created in: $BUILD_DIR"
echo ""
echo "To publish a release:"
echo "1. Create a GitHub release with tag 'v0.0.1'"
echo "2. Upload the .zip files from $BUILD_DIR"
echo "3. Update the download URLs in packages/lightning_db_dart/bin/install.dart"