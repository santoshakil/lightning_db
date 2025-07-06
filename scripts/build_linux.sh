#!/bin/bash

# Lightning DB - Linux Build Script
# Builds Linux libraries for x86_64

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
BUILD_DIR="$PROJECT_ROOT/build/releases/linux"

echo "Building Lightning DB for Linux..."
echo "Project root: $PROJECT_ROOT"
echo "Build output: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"

# Check if we're on Linux or cross-compiling
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "Building on Linux..."
    
    # Build for native architecture
    cargo build --release -p lightning_db_ffi
    
    # Copy the library
    cp "$PROJECT_ROOT/target/release/liblightning_db_ffi.so" "$BUILD_DIR/"
    
else
    echo "Cross-compiling for Linux..."
    
    # Add Linux target
    rustup target add x86_64-unknown-linux-gnu
    
    # Install cross-compilation tools if available
    if command -v cross &> /dev/null; then
        echo "Using 'cross' for cross-compilation..."
        cross build --release --target x86_64-unknown-linux-gnu -p lightning_db_ffi
    else
        echo "Warning: 'cross' not found. Attempting direct cross-compilation..."
        echo "This may fail without proper cross-compilation setup."
        
        # Set up cross-compilation environment for macOS to Linux
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # On macOS, we need to set up the linker
            export CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc
            export CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++
            export AR_x86_64_unknown_linux_gnu=x86_64-linux-gnu-ar
            
            # Check if cross-compilation tools are installed
            if ! command -v x86_64-linux-gnu-gcc &> /dev/null; then
                echo "Error: Linux cross-compilation tools not found."
                echo "Install with: brew install FiloSottile/musl-cross/musl-cross"
                echo "Or use Docker/cross for cross-compilation."
                exit 1
            fi
        fi
        
        cargo build --release --target x86_64-unknown-linux-gnu -p lightning_db_ffi
    fi
    
    # Copy the library
    cp "$PROJECT_ROOT/target/x86_64-unknown-linux-gnu/release/liblightning_db_ffi.so" "$BUILD_DIR/"
fi

# Verify the library
echo ""
echo "Verifying library..."
if [ -f "$BUILD_DIR/liblightning_db_ffi.so" ]; then
    echo "✓ Library built successfully"
    ls -lh "$BUILD_DIR/liblightning_db_ffi.so"
    
    # Check library dependencies
    if command -v ldd &> /dev/null; then
        echo ""
        echo "Library dependencies:"
        ldd "$BUILD_DIR/liblightning_db_ffi.so" || true
    fi
else
    echo "✗ Failed to build library"
    exit 1
fi

# Create release package
cd "$BUILD_DIR"
zip -r "../lightning_db-linux-x64.zip" "liblightning_db_ffi.so"

echo ""
echo "✅ Linux build complete!"
echo "Library: $BUILD_DIR/liblightning_db_ffi.so"
echo "Release package: $BUILD_DIR/../lightning_db-linux-x64.zip"