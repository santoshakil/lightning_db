#!/bin/bash

# Lightning DB - Master Build Script
# Builds for all available platforms

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
BUILD_DIR="$PROJECT_ROOT/build/releases"

echo "==========================================="
echo "Lightning DB - Multi-Platform Build"
echo "==========================================="
echo ""

# Track what was built
BUILT_PLATFORMS=()
FAILED_PLATFORMS=()

# Function to try building a platform
try_build() {
    local PLATFORM=$1
    local SCRIPT=$2
    
    echo "Building for $PLATFORM..."
    if [ -x "$SCRIPT" ]; then
        if $SCRIPT; then
            BUILT_PLATFORMS+=("$PLATFORM")
            echo "✅ $PLATFORM build successful"
        else
            FAILED_PLATFORMS+=("$PLATFORM")
            echo "❌ $PLATFORM build failed"
        fi
    else
        echo "⚠️  $PLATFORM build script not found or not executable"
        FAILED_PLATFORMS+=("$PLATFORM")
    fi
    echo ""
}

# Build for each platform
try_build "macOS" "$SCRIPT_DIR/build_macos_simple.sh"
try_build "iOS" "$SCRIPT_DIR/build_ios_final.sh"
try_build "Android" "$SCRIPT_DIR/build_android.sh"
try_build "Linux" "$SCRIPT_DIR/build_linux.sh"
try_build "Windows" "$SCRIPT_DIR/build_windows.sh"

# Summary
echo "==========================================="
echo "Build Summary"
echo "==========================================="
echo ""

if [ ${#BUILT_PLATFORMS[@]} -gt 0 ]; then
    echo "✅ Successfully built for:"
    for platform in "${BUILT_PLATFORMS[@]}"; do
        echo "   - $platform"
    done
    echo ""
fi

if [ ${#FAILED_PLATFORMS[@]} -gt 0 ]; then
    echo "❌ Failed to build for:"
    for platform in "${FAILED_PLATFORMS[@]}"; do
        echo "   - $platform"
    done
    echo ""
    echo "To enable cross-compilation:"
    echo "  - Linux: Install 'cross' (cargo install cross) or musl-cross"
    echo "  - Windows: Install mingw-w64 (brew install mingw-w64) or 'cross'"
    echo ""
fi

# List all release packages
echo "Release packages created:"
ls -la "$BUILD_DIR"/*.zip 2>/dev/null || echo "No release packages found"

echo ""
echo "Build complete!"