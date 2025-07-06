#!/bin/bash

# Lightning DB - Android Build Script
# Builds Android libraries for all architectures

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
BUILD_DIR="$PROJECT_ROOT/build/releases/android"

echo "Building Lightning DB for Android..."
echo "Project root: $PROJECT_ROOT"
echo "Build output: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"

# Check if Android NDK is set up
if [ -z "$ANDROID_NDK_HOME" ]; then
    echo "Warning: ANDROID_NDK_HOME not set, trying default locations..."
    if [ -d "$HOME/Library/Android/sdk/ndk/27.0.12077973" ]; then
        export ANDROID_NDK_HOME="$HOME/Library/Android/sdk/ndk/27.0.12077973"
    elif [ -d "$HOME/Library/Android/sdk/ndk/26.3.11579264" ]; then
        export ANDROID_NDK_HOME="$HOME/Library/Android/sdk/ndk/26.3.11579264"
    elif [ -d "$HOME/Android/Sdk/ndk/25.2.9519653" ]; then
        export ANDROID_NDK_HOME="$HOME/Android/Sdk/ndk/25.2.9519653"
    else
        echo "Error: Android NDK not found. Please install Android NDK and set ANDROID_NDK_HOME"
        exit 1
    fi
fi

echo "Using Android NDK: $ANDROID_NDK_HOME"

# Install cargo-ndk if not present
if ! command -v cargo-ndk &> /dev/null; then
    echo "Installing cargo-ndk..."
    cargo install cargo-ndk
fi

# Add Android targets
echo "Adding Android targets..."
rustup target add \
    aarch64-linux-android \
    armv7-linux-androideabi \
    i686-linux-android \
    x86_64-linux-android

# Build for each Android architecture
echo ""
echo "Building for Android architectures..."

# Use cargo-ndk to build for all Android architectures
cargo ndk \
    -t arm64-v8a \
    -t armeabi-v7a \
    -t x86 \
    -t x86_64 \
    -o "$BUILD_DIR" \
    build --release \
    -p lightning_db_ffi \
    --no-default-features

# Verify output
echo ""
echo "Verifying build output..."
for arch in arm64-v8a armeabi-v7a x86 x86_64; do
    if [ -f "$BUILD_DIR/$arch/liblightning_db_ffi.so" ]; then
        echo "✓ Built for $arch"
        ls -lh "$BUILD_DIR/$arch/liblightning_db_ffi.so"
    else
        echo "✗ Failed to build for $arch"
    fi
done

# Create AAR structure for easier Android integration
AAR_DIR="$BUILD_DIR/lightning_db.aar.structure"
mkdir -p "$AAR_DIR/jni"

# Copy libraries to AAR structure
cp -r "$BUILD_DIR/arm64-v8a" "$AAR_DIR/jni/" || true
cp -r "$BUILD_DIR/armeabi-v7a" "$AAR_DIR/jni/" || true
cp -r "$BUILD_DIR/x86" "$AAR_DIR/jni/" || true
cp -r "$BUILD_DIR/x86_64" "$AAR_DIR/jni/" || true

# Create release package
cd "$BUILD_DIR"
zip -r "../lightning_db-android.zip" */liblightning_db_ffi.so

echo ""
echo "✅ Android build complete!"
echo "Libraries built in: $BUILD_DIR"
echo "Release package: $BUILD_DIR/../lightning_db-android.zip"
echo ""
echo "Architecture directories:"
ls -la "$BUILD_DIR/"