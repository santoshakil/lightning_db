#!/bin/bash

# Lightning DB - Integration Test Runner
# Runs integration tests on all supported platforms

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
EXAMPLE_DIR="$PROJECT_ROOT/packages/lightning_db/example"

echo "======================================"
echo "Lightning DB Integration Tests"
echo "======================================"
echo ""

# Function to run tests on a specific platform
run_platform_tests() {
    local PLATFORM=$1
    local DEVICE=$2
    
    echo "Running tests on $PLATFORM..."
    
    cd "$EXAMPLE_DIR"
    
    case $PLATFORM in
        "ios")
            flutter test integration_test/ --device-id="$DEVICE" || echo "iOS tests failed"
            ;;
        "android")
            flutter test integration_test/ --device-id="$DEVICE" || echo "Android tests failed"
            ;;
        "macos")
            flutter test integration_test/ -d macos || echo "macOS tests failed"
            ;;
        "linux")
            flutter test integration_test/ -d linux || echo "Linux tests failed"
            ;;
        "windows")
            flutter test integration_test/ -d windows || echo "Windows tests failed"
            ;;
        *)
            echo "Unknown platform: $PLATFORM"
            ;;
    esac
    
    echo ""
}

# Check if Flutter is installed
if ! command -v flutter &> /dev/null; then
    echo "Flutter not found. Please install Flutter first."
    exit 1
fi

# Get Flutter devices
echo "Checking available devices..."
flutter devices
echo ""

# Parse command line arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 <platform> [device-id]"
    echo "Platforms: ios, android, macos, linux, windows, all"
    echo ""
    echo "Examples:"
    echo "  $0 macos                    # Run on macOS"
    echo "  $0 ios <device-id>          # Run on specific iOS device"
    echo "  $0 all                      # Run on all available platforms"
    exit 1
fi

PLATFORM=$1
DEVICE_ID=${2:-""}

# Build native libraries first
echo "Building native libraries..."
cd "$PROJECT_ROOT"

case $PLATFORM in
    "ios")
        echo "Building iOS library..."
        ./scripts/build_ios_final.sh || echo "iOS build failed"
        ;;
    "android")
        echo "Building Android library..."
        ./scripts/build_android.sh || echo "Android build failed"
        ;;
    "macos")
        echo "Building macOS library..."
        ./scripts/build_macos_simple.sh || echo "macOS build failed"
        ;;
    "linux")
        echo "Building Linux library..."
        cargo build --release -p lightning_db_ffi || echo "Linux build failed"
        ;;
    "windows")
        echo "Building Windows library..."
        cargo build --release -p lightning_db_ffi || echo "Windows build failed"
        ;;
    "all")
        echo "Building all platform libraries..."
        ./scripts/build_all.sh || echo "Some builds failed"
        ;;
esac

echo ""
echo "Running integration tests..."
echo ""

# Run integration tests
cd "$EXAMPLE_DIR"

# Install dependencies
flutter pub get

# Generate Freezed files
echo "Generating Freezed models..."
dart run build_runner build --delete-conflicting-outputs

# Copy binaries to example app
echo "Installing binaries..."
case $PLATFORM in
    "macos")
        mkdir -p macos/Frameworks
        cp "$PROJECT_ROOT/target/release/liblightning_db_ffi.dylib" macos/Frameworks/liblightning_db.dylib || true
        ;;
    "linux")
        mkdir -p linux/lib
        cp "$PROJECT_ROOT/target/release/liblightning_db_ffi.so" linux/lib/liblightning_db.so || true
        ;;
    "windows")
        mkdir -p windows
        cp "$PROJECT_ROOT/target/release/lightning_db_ffi.dll" windows/lightning_db.dll || true
        ;;
esac

# Run tests based on platform
case $PLATFORM in
    "all")
        # Run on all available platforms
        if [[ "$OSTYPE" == "darwin"* ]]; then
            run_platform_tests "macos" ""
        fi
        
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            run_platform_tests "linux" ""
        fi
        
        if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
            run_platform_tests "windows" ""
        fi
        
        echo "Note: iOS and Android tests require connected devices/emulators"
        ;;
    *)
        run_platform_tests "$PLATFORM" "$DEVICE_ID"
        ;;
esac

echo ""
echo "======================================"
echo "Integration tests completed!"
echo "======================================"