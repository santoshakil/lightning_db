#!/bin/bash

# Lightning DB Package Distribution Script
# Automates building and distributing packages for all supported platforms

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PACKAGE_NAME="lightning_db"
VERSION=$(grep '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
BUILD_DIR="build"
DIST_DIR="dist"

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to clean build directories
clean_build() {
    log_info "Cleaning build directories..."
    rm -rf "$BUILD_DIR" "$DIST_DIR"
    mkdir -p "$BUILD_DIR" "$DIST_DIR"
    log_success "Build directories cleaned"
}

# Function to verify prerequisites
verify_prerequisites() {
    log_info "Verifying prerequisites..."
    
    # Check Rust toolchain
    if ! command -v cargo &> /dev/null; then
        log_error "Rust/Cargo not found. Please install Rust first."
        exit 1
    fi
    
    # Check for required targets
    local targets=(
        "x86_64-unknown-linux-gnu"
        "x86_64-unknown-linux-musl"
        "aarch64-unknown-linux-gnu"
        "x86_64-apple-darwin"
        "aarch64-apple-darwin"
        "x86_64-pc-windows-msvc"
        "wasm32-unknown-unknown"
    )
    
    for target in "${targets[@]}"; do
        if ! rustup target list --installed | grep -q "$target"; then
            log_warning "Installing missing target: $target"
            rustup target add "$target" || log_warning "Failed to install $target"
        fi
    done
    
    log_success "Prerequisites verified"
}

# Function to build native library
build_native() {
    log_info "Building native library for all platforms..."
    
    # Linux x86_64
    log_info "Building for Linux x86_64..."
    cargo build --release --target x86_64-unknown-linux-gnu
    
    # Linux x86_64 (musl)
    log_info "Building for Linux x86_64 (musl)..."
    CC=musl-gcc cargo build --release --target x86_64-unknown-linux-musl || log_warning "musl build failed"
    
    # Linux ARM64
    log_info "Building for Linux ARM64..."
    cargo build --release --target aarch64-unknown-linux-gnu || log_warning "ARM64 build failed"
    
    # macOS (Intel and Apple Silicon)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        log_info "Building for macOS Intel..."
        cargo build --release --target x86_64-apple-darwin
        
        log_info "Building for macOS Apple Silicon..."
        cargo build --release --target aarch64-apple-darwin
    else
        log_warning "Skipping macOS builds (not on macOS)"
    fi
    
    # Windows
    log_info "Building for Windows..."
    cargo build --release --target x86_64-pc-windows-msvc || log_warning "Windows build failed"
    
    log_success "Native library builds completed"
}

# Function to build WebAssembly
build_wasm() {
    log_info "Building WebAssembly package..."
    
    # Check for wasm-pack
    if ! command -v wasm-pack &> /dev/null; then
        log_warning "wasm-pack not found. Installing..."
        curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
    fi
    
    # Build WASM package
    wasm-pack build --target web --out-dir pkg/web --release
    wasm-pack build --target nodejs --out-dir pkg/nodejs --release
    wasm-pack build --target bundler --out-dir pkg/bundler --release
    
    # Package for distribution
    mkdir -p "$DIST_DIR/wasm"
    cp -r pkg/* "$DIST_DIR/wasm/"
    
    log_success "WebAssembly package built"
}

# Function to build FFI libraries
build_ffi() {
    log_info "Building FFI libraries..."
    
    # Create FFI directory structure
    mkdir -p "$BUILD_DIR/ffi/"{linux,macos,windows,ios,android}
    
    # Linux FFI
    if [[ -f "target/x86_64-unknown-linux-gnu/release/liblightning_db_ffi.so" ]]; then
        cp target/x86_64-unknown-linux-gnu/release/liblightning_db_ffi.so "$BUILD_DIR/ffi/linux/"
    fi
    
    # macOS FFI
    if [[ -f "target/x86_64-apple-darwin/release/liblightning_db_ffi.dylib" ]] && [[ -f "target/aarch64-apple-darwin/release/liblightning_db_ffi.dylib" ]]; then
        # Create universal binary
        lipo -create \
            target/x86_64-apple-darwin/release/liblightning_db_ffi.dylib \
            target/aarch64-apple-darwin/release/liblightning_db_ffi.dylib \
            -output "$BUILD_DIR/ffi/macos/liblightning_db.dylib" || log_warning "Failed to create universal macOS binary"
    fi
    
    # Windows FFI
    if [[ -f "target/x86_64-pc-windows-msvc/release/lightning_db_ffi.dll" ]]; then
        cp target/x86_64-pc-windows-msvc/release/lightning_db_ffi.dll "$BUILD_DIR/ffi/windows/lightning_db.dll"
    fi
    
    log_success "FFI libraries built"
}

# Function to create distribution packages
create_packages() {
    log_info "Creating distribution packages..."
    
    # Native Rust library (source distribution)
    log_info "Packaging Rust crate..."
    cargo package --no-verify
    mv target/package/${PACKAGE_NAME}-${VERSION}.crate "$DIST_DIR/"
    
    # Binary distributions
    create_binary_packages
    
    # Language-specific packages
    create_language_packages
    
    log_success "All packages created"
}

# Function to create binary packages
create_binary_packages() {
    log_info "Creating binary packages..."
    
    # Linux binaries
    if [[ -f "target/x86_64-unknown-linux-gnu/release/lightning_db" ]]; then
        mkdir -p "$BUILD_DIR/linux-x86_64"
        cp target/x86_64-unknown-linux-gnu/release/lightning_db "$BUILD_DIR/linux-x86_64/"
        cp README.md LICENSE* "$BUILD_DIR/linux-x86_64/" 2>/dev/null || true
        
        cd "$BUILD_DIR"
        tar czf "../$DIST_DIR/lightning_db-${VERSION}-linux-x86_64.tar.gz" linux-x86_64
        cd ..
    fi
    
    # macOS binaries
    if [[ -f "target/x86_64-apple-darwin/release/lightning_db" ]] && [[ -f "target/aarch64-apple-darwin/release/lightning_db" ]]; then
        mkdir -p "$BUILD_DIR/macos-universal"
        
        # Create universal binary
        lipo -create \
            target/x86_64-apple-darwin/release/lightning_db \
            target/aarch64-apple-darwin/release/lightning_db \
            -output "$BUILD_DIR/macos-universal/lightning_db" || log_warning "Failed to create universal macOS binary"
        
        cp README.md LICENSE* "$BUILD_DIR/macos-universal/" 2>/dev/null || true
        
        cd "$BUILD_DIR"
        tar czf "../$DIST_DIR/lightning_db-${VERSION}-macos-universal.tar.gz" macos-universal
        cd ..
    fi
    
    # Windows binaries
    if [[ -f "target/x86_64-pc-windows-msvc/release/lightning_db.exe" ]]; then
        mkdir -p "$BUILD_DIR/windows-x86_64"
        cp target/x86_64-pc-windows-msvc/release/lightning_db.exe "$BUILD_DIR/windows-x86_64/"
        cp README.md LICENSE* "$BUILD_DIR/windows-x86_64/" 2>/dev/null || true
        
        cd "$BUILD_DIR"
        zip -r "../$DIST_DIR/lightning_db-${VERSION}-windows-x86_64.zip" windows-x86_64
        cd ..
    fi
    
    log_success "Binary packages created"
}

# Function to create language-specific packages
create_language_packages() {
    log_info "Creating language-specific packages..."
    
    # Android AAR
    create_android_package
    
    # iOS XCFramework
    create_ios_package
    
    # Node.js package
    create_nodejs_package
    
    # Python wheel
    create_python_package
    
    log_success "Language-specific packages created"
}

# Function to create Android package
create_android_package() {
    log_info "Creating Android AAR package..."
    
    # Check for Android NDK and cargo-ndk
    if command -v cargo-ndk &> /dev/null && [[ -n "$ANDROID_NDK_ROOT" ]]; then
        mkdir -p "$BUILD_DIR/android"
        
        # Build for Android targets
        cargo ndk \
            -t arm64-v8a \
            -t armeabi-v7a \
            -t x86 \
            -t x86_64 \
            -o "$BUILD_DIR/android" \
            build --release -p lightning_db_ffi --no-default-features || log_warning "Android build failed"
        
        # Package AAR
        cd "$BUILD_DIR"
        zip -r "../$DIST_DIR/lightning_db-${VERSION}-android.zip" android
        cd ..
    else
        log_warning "Skipping Android package (cargo-ndk or Android NDK not available)"
    fi
}

# Function to create iOS package
create_ios_package() {
    log_info "Creating iOS XCFramework..."
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # Build for iOS targets
        cargo build --release --target aarch64-apple-ios -p lightning_db_ffi --no-default-features --features ios || log_warning "iOS device build failed"
        cargo build --release --target x86_64-apple-ios -p lightning_db_ffi --no-default-features --features ios || log_warning "iOS simulator build failed"
        cargo build --release --target aarch64-apple-ios-sim -p lightning_db_ffi --no-default-features --features ios || log_warning "iOS sim ARM64 build failed"
        
        # Create XCFramework structure
        mkdir -p "$BUILD_DIR/ios/device" "$BUILD_DIR/ios/simulator"
        
        # Copy device library
        if [[ -f "target/aarch64-apple-ios/release/liblightning_db_ffi.a" ]]; then
            cp target/aarch64-apple-ios/release/liblightning_db_ffi.a "$BUILD_DIR/ios/device/"
        fi
        
        # Create fat library for simulators
        if [[ -f "target/x86_64-apple-ios/release/liblightning_db_ffi.a" ]] && [[ -f "target/aarch64-apple-ios-sim/release/liblightning_db_ffi.a" ]]; then
            lipo -create \
                target/x86_64-apple-ios/release/liblightning_db_ffi.a \
                target/aarch64-apple-ios-sim/release/liblightning_db_ffi.a \
                -output "$BUILD_DIR/ios/simulator/liblightning_db_ffi.a" || log_warning "Failed to create iOS simulator fat library"
        fi
        
        # Create XCFramework
        if command -v xcodebuild &> /dev/null; then
            xcodebuild -create-xcframework \
                -library "$BUILD_DIR/ios/device/liblightning_db_ffi.a" \
                -library "$BUILD_DIR/ios/simulator/liblightning_db_ffi.a" \
                -output "$BUILD_DIR/ios/LightningDB.xcframework" || log_warning "XCFramework creation failed"
            
            # Package for distribution
            cd "$BUILD_DIR"
            zip -r "../$DIST_DIR/lightning_db-${VERSION}-ios-xcframework.zip" ios
            cd ..
        else
            log_warning "xcodebuild not found, skipping XCFramework creation"
        fi
    else
        log_warning "Skipping iOS package (not on macOS)"
    fi
}

# Function to create Node.js package
create_nodejs_package() {
    log_info "Creating Node.js package..."
    
    # Check if Node.js bindings exist
    if [[ -d "packages/nodejs" ]]; then
        cd packages/nodejs
        npm pack
        mv *.tgz "../../$DIST_DIR/"
        cd ../..
        log_success "Node.js package created"
    else
        log_warning "Node.js package directory not found"
    fi
}

# Function to create Python package
create_python_package() {
    log_info "Creating Python wheel..."
    
    # Check if Python bindings exist
    if [[ -f "setup.py" ]] || [[ -f "pyproject.toml" ]]; then
        if command -v python3 &> /dev/null; then
            python3 -m pip install --user build
            python3 -m build --wheel --outdir "$DIST_DIR"
            log_success "Python wheel created"
        else
            log_warning "Python3 not found, skipping Python package"
        fi
    else
        log_warning "Python package configuration not found"
    fi
}

# Function to generate checksums
generate_checksums() {
    log_info "Generating checksums..."
    
    cd "$DIST_DIR"
    
    # Generate SHA256 checksums
    if command -v sha256sum &> /dev/null; then
        sha256sum * > SHA256SUMS
    elif command -v shasum &> /dev/null; then
        shasum -a 256 * > SHA256SUMS
    else
        log_warning "No SHA256 utility found"
    fi
    
    # Generate MD5 checksums
    if command -v md5sum &> /dev/null; then
        md5sum * > MD5SUMS
    elif command -v md5 &> /dev/null; then
        md5 * > MD5SUMS
    else
        log_warning "No MD5 utility found"
    fi
    
    cd ..
    log_success "Checksums generated"
}

# Function to create release notes
create_release_notes() {
    log_info "Creating release notes..."
    
    cat > "$DIST_DIR/RELEASE_NOTES.md" << EOF
# Lightning DB ${VERSION} Release

## Package Contents

This release includes the following packages:

### Rust Crate
- \`lightning_db-${VERSION}.crate\` - Source distribution for Rust projects

### Binary Distributions
- \`lightning_db-${VERSION}-linux-x86_64.tar.gz\` - Linux x86_64 binary
- \`lightning_db-${VERSION}-macos-universal.tar.gz\` - macOS Universal binary
- \`lightning_db-${VERSION}-windows-x86_64.zip\` - Windows x86_64 binary

### Platform-Specific Libraries
- \`lightning_db-${VERSION}-android.zip\` - Android AAR and native libraries
- \`lightning_db-${VERSION}-ios-xcframework.zip\` - iOS XCFramework

### Language Bindings
- WebAssembly packages in \`dist/wasm/\`
- Node.js package: \`*.tgz\` files
- Python wheel: \`*.whl\` files

## Installation

### Rust
Add to your \`Cargo.toml\`:
\`\`\`toml
[dependencies]
lightning_db = "${VERSION}"
\`\`\`

### Node.js
\`\`\`bash
npm install lightning_db@${VERSION}
\`\`\`

### Python
\`\`\`bash
pip install lightning_db==${VERSION}
\`\`\`

## Verification

Use the provided checksums to verify package integrity:
- \`SHA256SUMS\` - SHA256 checksums
- \`MD5SUMS\` - MD5 checksums

## Documentation

- [API Documentation](https://docs.rs/lightning_db/${VERSION})
- [Platform Integration Guides](https://github.com/santoshakil/lightning_db/tree/main/docs/platforms)
- [Examples](https://github.com/santoshakil/lightning_db/tree/main/examples)

## Support

- [GitHub Issues](https://github.com/santoshakil/lightning_db/issues)
- [Repository](https://github.com/santoshakil/lightning_db)
EOF
    
    log_success "Release notes created"
}

# Function to display usage
usage() {
    echo "Lightning DB Package Distribution Script"
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -c, --clean             Clean build directories only"
    echo "  --skip-native           Skip native library builds"
    echo "  --skip-wasm             Skip WebAssembly build"
    echo "  --skip-ffi              Skip FFI library builds"
    echo "  --skip-packages         Skip package creation"
    echo "  --skip-checksums        Skip checksum generation"
    echo
    echo "Examples:"
    echo "  $0                      Build and package everything"
    echo "  $0 --skip-wasm          Build everything except WebAssembly"
    echo "  $0 --clean              Clean build directories"
}

# Main function
main() {
    local clean_only=false
    local skip_native=false
    local skip_wasm=false
    local skip_ffi=false
    local skip_packages=false
    local skip_checksums=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                usage
                exit 0
                ;;
            -c|--clean)
                clean_only=true
                shift
                ;;
            --skip-native)
                skip_native=true
                shift
                ;;
            --skip-wasm)
                skip_wasm=true
                shift
                ;;
            --skip-ffi)
                skip_ffi=true
                shift
                ;;
            --skip-packages)
                skip_packages=true
                shift
                ;;
            --skip-checksums)
                skip_checksums=true
                shift
                ;;
            -*)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                log_error "Unexpected argument: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    log_info "Lightning DB Package Distribution v${VERSION}"
    echo
    
    # Clean build directories
    clean_build
    
    if [[ "$clean_only" == true ]]; then
        log_success "Clean completed"
        exit 0
    fi
    
    # Verify prerequisites
    verify_prerequisites
    
    # Build everything
    if [[ "$skip_native" != true ]]; then
        build_native
    fi
    
    if [[ "$skip_wasm" != true ]]; then
        build_wasm
    fi
    
    if [[ "$skip_ffi" != true ]]; then
        build_ffi
    fi
    
    # Create packages
    if [[ "$skip_packages" != true ]]; then
        create_packages
    fi
    
    # Generate checksums
    if [[ "$skip_checksums" != true ]]; then
        generate_checksums
    fi
    
    # Create release notes
    create_release_notes
    
    # Summary
    echo
    log_success "Package distribution completed successfully!"
    log_info "Packages available in: $DIST_DIR/"
    log_info "Total packages created: $(ls -1 "$DIST_DIR" | wc -l)"
    
    echo
    log_info "Next steps:"
    log_info "1. Test packages on target platforms"
    log_info "2. Upload to package registries"
    log_info "3. Create GitHub release with packages"
    log_info "4. Update documentation with installation instructions"
}

# Run main function with all arguments
main "$@"