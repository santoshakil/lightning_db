#!/bin/bash
set -e

# Build script for Lightning DB - all platforms
# This script builds Lightning DB for multiple platforms using cross-compilation

echo "🚀 Lightning DB Multi-Platform Build Script"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[BUILD]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Check if cargo is installed
if ! command -v cargo &> /dev/null; then
    print_error "Cargo is not installed. Please install Rust first."
    exit 1
fi

# Install cross if not already installed
if ! command -v cross &> /dev/null; then
    print_status "Installing cross for cross-compilation..."
    cargo install cross --git https://github.com/cross-rs/cross
fi

# Create build directory
BUILD_DIR="build/releases"
mkdir -p "$BUILD_DIR"

# Array of targets to build
TARGETS=(
    # Linux targets
    "x86_64-unknown-linux-gnu"
    "x86_64-unknown-linux-musl"
    "aarch64-unknown-linux-gnu"
    "aarch64-unknown-linux-musl"
    "armv7-unknown-linux-gnueabihf"
    "i686-unknown-linux-gnu"
    
    # Windows targets (if on Windows or using cross)
    "x86_64-pc-windows-gnu"
    
    # macOS targets (only if on macOS)
    # "x86_64-apple-darwin"
    # "aarch64-apple-darwin"
    
    # FreeBSD
    "x86_64-unknown-freebsd"
    
    # WebAssembly
    "wasm32-unknown-unknown"
)

# Function to build for a target
build_target() {
    local target=$1
    print_status "Building for $target..."
    
    # Determine if we need to use cross
    local use_cross=true
    if [[ "$target" == "x86_64-unknown-linux-gnu" && "$OSTYPE" == "linux-gnu"* ]]; then
        use_cross=false
    elif [[ "$target" == "x86_64-apple-darwin" && "$OSTYPE" == "darwin"* ]]; then
        use_cross=false
    elif [[ "$target" == "wasm32-unknown-unknown" ]]; then
        use_cross=false
    fi
    
    # Build command
    if [ "$use_cross" = true ]; then
        if [ "$target" = "wasm32-unknown-unknown" ]; then
            cargo build --release --target "$target" --no-default-features || {
                print_warning "Failed to build for $target"
                return 1
            }
        else
            cross build --release --target "$target" || {
                print_warning "Failed to build for $target"
                return 1
            }
        fi
    else
        if [ "$target" = "wasm32-unknown-unknown" ]; then
            cargo build --release --target "$target" --no-default-features || {
                print_warning "Failed to build for $target"
                return 1
            }
        else
            cargo build --release --target "$target" || {
                print_warning "Failed to build for $target"
                return 1
            }
        fi
    fi
    
    # Package the binary
    local binary_name="lightning_db"
    local binary_ext=""
    
    if [[ "$target" == *"windows"* ]]; then
        binary_ext=".exe"
    fi
    
    local binary_path="target/$target/release/$binary_name$binary_ext"
    
    if [ -f "$binary_path" ]; then
        # Strip binary if not Windows
        if [[ "$target" != *"windows"* ]] && command -v strip &> /dev/null; then
            strip "$binary_path" || true
        fi
        
        # Create archive
        local archive_name="lightning_db-${target}"
        if [[ "$target" == *"windows"* ]]; then
            if command -v zip &> /dev/null; then
                (cd "target/$target/release" && zip -q "$BUILD_DIR/../../../$BUILD_DIR/${archive_name}.zip" "$binary_name$binary_ext")
            else
                cp "$binary_path" "$BUILD_DIR/${archive_name}${binary_ext}"
            fi
        else
            tar -czf "$BUILD_DIR/${archive_name}.tar.gz" -C "target/$target/release" "$binary_name"
        fi
        
        print_status "✓ Built and packaged $target"
    else
        print_warning "Binary not found for $target"
    fi
}

# Main build loop
print_status "Starting multi-platform build..."
echo ""

# Add targets based on current platform
if [[ "$OSTYPE" == "darwin"* ]]; then
    print_status "Detected macOS, adding Apple targets..."
    TARGETS+=("x86_64-apple-darwin" "aarch64-apple-darwin")
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    print_status "Detected Windows, adding Windows targets..."
    TARGETS+=("x86_64-pc-windows-msvc" "i686-pc-windows-msvc")
fi

# Install required targets
print_status "Installing Rust targets..."
for target in "${TARGETS[@]}"; do
    rustup target add "$target" 2>/dev/null || true
done

# Special handling for WASM
if [[ " ${TARGETS[@]} " =~ " wasm32-unknown-unknown " ]]; then
    print_status "Installing wasm-pack for WebAssembly builds..."
    if ! command -v wasm-pack &> /dev/null; then
        curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
    fi
fi

# Check for Android NDK and build Android targets if available
if command -v cargo-ndk &> /dev/null || command -v ndk-build &> /dev/null; then
    print_status "Android NDK detected, adding Android targets..."
    
    # Install cargo-ndk if not present
    if ! command -v cargo-ndk &> /dev/null; then
        print_status "Installing cargo-ndk..."
        cargo install cargo-ndk
    fi
    
    # Android targets
    ANDROID_TARGETS=(
        "aarch64-linux-android"
        "armv7-linux-androideabi"
        "i686-linux-android"
        "x86_64-linux-android"
    )
    
    # Add Android targets
    for target in "${ANDROID_TARGETS[@]}"; do
        rustup target add "$target" 2>/dev/null || true
    done
    
    # Build Android targets using cargo-ndk
    for target in "${ANDROID_TARGETS[@]}"; do
        print_status "Building Android target: $target"
        if cargo ndk -t "$target" build --release; then
            # Package Android library
            local lib_name="liblightning_db.so"
            local lib_path="target/$target/release/liblightning_db.so"
            if [ -f "$lib_path" ]; then
                tar -czf "$BUILD_DIR/lightning_db-android-$target.tar.gz" -C "target/$target/release" "$lib_name"
                print_status "✓ Built and packaged Android $target"
            fi
        else
            print_warning "Failed to build Android target: $target"
        fi
    done
else
    print_warning "Android NDK not found, skipping Android builds"
fi

# Build each target
SUCCESSFUL_BUILDS=0
FAILED_BUILDS=0

for target in "${TARGETS[@]}"; do
    if build_target "$target"; then
        ((SUCCESSFUL_BUILDS++))
    else
        ((FAILED_BUILDS++))
    fi
    echo ""
done

# Summary
echo "=========================================="
print_status "Build Summary:"
echo -e "  ${GREEN}Successful:${NC} $SUCCESSFUL_BUILDS"
echo -e "  ${RED}Failed:${NC} $FAILED_BUILDS"
echo ""
print_status "Build artifacts are in: $BUILD_DIR"
echo ""

# List built artifacts
if [ -d "$BUILD_DIR" ] && [ "$(ls -A $BUILD_DIR)" ]; then
    print_status "Built artifacts:"
    ls -lh "$BUILD_DIR"
fi

# Create universal macOS binary if on macOS and both architectures built
if [[ "$OSTYPE" == "darwin"* ]]; then
    if [ -f "target/x86_64-apple-darwin/release/lightning_db" ] && [ -f "target/aarch64-apple-darwin/release/lightning_db" ]; then
        print_status "Creating universal macOS binary..."
        lipo -create \
            "target/x86_64-apple-darwin/release/lightning_db" \
            "target/aarch64-apple-darwin/release/lightning_db" \
            -output "$BUILD_DIR/lightning_db-macos-universal"
        
        strip "$BUILD_DIR/lightning_db-macos-universal"
        tar -czf "$BUILD_DIR/lightning_db-macos-universal.tar.gz" -C "$BUILD_DIR" "lightning_db-macos-universal"
        rm "$BUILD_DIR/lightning_db-macos-universal"
        
        print_status "✓ Created universal macOS binary"
    fi
fi

echo ""
print_status "Build complete!"

exit 0