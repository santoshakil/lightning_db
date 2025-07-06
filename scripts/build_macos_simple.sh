#!/bin/bash

# Lightning DB - Simple macOS Release Build

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
BUILD_DIR="$PROJECT_ROOT/build/releases"

echo "Building Lightning DB for macOS..."
echo "Project root: $PROJECT_ROOT"
echo "Build output: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"

# Build for current architecture
echo ""
echo "Building for current architecture..."
cargo build --release -p lightning_db_ffi

# Copy the library
mkdir -p "$BUILD_DIR/macos"
cp "$PROJECT_ROOT/target/release/liblightning_db_ffi.dylib" "$BUILD_DIR/macos/liblightning_db.dylib"

# Create release package
cd "$BUILD_DIR"
zip -r "lightning_db-macos.zip" "macos"
echo "✓ Created lightning_db-macos.zip"

echo ""
echo "✅ Build complete!"
echo "Release package created: $BUILD_DIR/lightning_db-macos.zip"