#!/bin/bash
# Simple Rust build script for Cargokit

set -e

if [[ -z "$1" || -z "$2" ]]; then
    echo "Usage: $0 <path-to-rust-project> <lib-name>"
    exit 1
fi

RUST_PROJECT_DIR="$1"
LIB_NAME="$2"

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Get the root of the Flutter plugin
PLUGIN_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Determine target architecture
if [[ "$ARCHS" == "x86_64" ]]; then
    RUST_TARGET="x86_64-apple-darwin"
elif [[ "$ARCHS" == "arm64" ]]; then
    RUST_TARGET="aarch64-apple-darwin"
else
    # Default to current architecture
    RUST_TARGET="aarch64-apple-darwin"
fi

# Determine build type
if [[ "$CONFIGURATION" == "Release" ]]; then
    BUILD_FLAG="--release"
    TARGET_DIR="release"
else
    BUILD_FLAG=""
    TARGET_DIR="debug"
fi

echo "Building Rust library for $RUST_TARGET ($CONFIGURATION)"
echo "Rust project: $RUST_PROJECT_DIR"
echo "Library name: $LIB_NAME"

# Build the Rust library
cd "$RUST_PROJECT_DIR"

# Check if we're in a workspace
if [[ -f "../Cargo.toml" ]] && grep -q "\[workspace\]" "../Cargo.toml" 2>/dev/null; then
    echo "Detected workspace, building from parent directory"
    cd ..
    cargo build $BUILD_FLAG -p $LIB_NAME
else
    # For macOS, we can build without specifying target
    cargo build $BUILD_FLAG
fi

# Try to find the built library - first check workspace output
WORKSPACE_ROOT="$(pwd)"
RUST_OUTPUT="$WORKSPACE_ROOT/target/$TARGET_DIR/lib${LIB_NAME}.a"

if [[ ! -f "$RUST_OUTPUT" ]]; then
    # If not found, check with target triple
    RUST_OUTPUT="$WORKSPACE_ROOT/target/$RUST_TARGET/$TARGET_DIR/lib${LIB_NAME}.a"
fi

if [[ ! -f "$RUST_OUTPUT" ]]; then
    # Try original project directory
    cd "$RUST_PROJECT_DIR"
    RUST_OUTPUT="target/$TARGET_DIR/lib${LIB_NAME}.a"
fi

if [[ ! -f "$RUST_OUTPUT" ]]; then
    # If not found, check with target triple in project dir
    RUST_OUTPUT="target/$RUST_TARGET/$TARGET_DIR/lib${LIB_NAME}.a"
fi

if [[ -f "$RUST_OUTPUT" ]]; then
    cp "$RUST_OUTPUT" "${BUILT_PRODUCTS_DIR}/lib${LIB_NAME}.a"
    echo "Copied $RUST_OUTPUT to ${BUILT_PRODUCTS_DIR}/lib${LIB_NAME}.a"
else
    echo "Error: Expected Rust library not found"
    echo "Searched locations:"
    echo "  - target/$TARGET_DIR/lib${LIB_NAME}.a"
    echo "  - target/$RUST_TARGET/$TARGET_DIR/lib${LIB_NAME}.a"
    exit 1
fi