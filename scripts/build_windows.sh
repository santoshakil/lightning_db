#!/bin/bash

# Lightning DB - Windows Build Script
# Cross-compiles Windows DLL from macOS/Linux

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
BUILD_DIR="$PROJECT_ROOT/build/releases/windows"

echo "Building Lightning DB for Windows..."
echo "Project root: $PROJECT_ROOT"
echo "Build output: $BUILD_DIR"

# Create build directory
mkdir -p "$BUILD_DIR"

# Add Windows target
echo "Adding Windows target..."
rustup target add x86_64-pc-windows-gnu

# Check for cross-compilation tools
if command -v cross &> /dev/null; then
    echo "Using 'cross' for cross-compilation..."
    cross build --release --target x86_64-pc-windows-gnu -p lightning_db_ffi
    
elif command -v x86_64-w64-mingw32-gcc &> /dev/null; then
    echo "Using MinGW for cross-compilation..."
    
    # Set up MinGW environment
    export CC_x86_64_pc_windows_gnu=x86_64-w64-mingw32-gcc
    export CXX_x86_64_pc_windows_gnu=x86_64-w64-mingw32-g++
    export AR_x86_64_pc_windows_gnu=x86_64-w64-mingw32-ar
    
    cargo build --release --target x86_64-pc-windows-gnu -p lightning_db_ffi
    
else
    echo "Warning: No Windows cross-compilation tools found."
    echo "Install MinGW-w64 or use 'cross' for cross-compilation."
    echo ""
    echo "On macOS: brew install mingw-w64"
    echo "Or: cargo install cross"
    echo ""
    echo "Attempting build anyway..."
    
    cargo build --release --target x86_64-pc-windows-gnu -p lightning_db_ffi
fi

# Copy the DLL
if [ -f "$PROJECT_ROOT/target/x86_64-pc-windows-gnu/release/lightning_db_ffi.dll" ]; then
    cp "$PROJECT_ROOT/target/x86_64-pc-windows-gnu/release/lightning_db_ffi.dll" "$BUILD_DIR/lightning_db.dll"
    echo "✓ DLL built successfully"
    ls -lh "$BUILD_DIR/lightning_db.dll"
else
    echo "✗ Failed to build DLL"
    exit 1
fi

# Also copy the .lib file if it exists (for linking)
if [ -f "$PROJECT_ROOT/target/x86_64-pc-windows-gnu/release/lightning_db_ffi.dll.lib" ]; then
    cp "$PROJECT_ROOT/target/x86_64-pc-windows-gnu/release/lightning_db_ffi.dll.lib" "$BUILD_DIR/lightning_db.lib"
    echo "✓ Import library copied"
fi

# Create release package
cd "$BUILD_DIR"
zip -r "../lightning_db-windows-x64.zip" "lightning_db.dll" $([ -f "lightning_db.lib" ] && echo "lightning_db.lib")

echo ""
echo "✅ Windows build complete!"
echo "DLL: $BUILD_DIR/lightning_db.dll"
echo "Release package: $BUILD_DIR/../lightning_db-windows-x64.zip"