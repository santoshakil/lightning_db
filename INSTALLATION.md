# Lightning DB Installation Guide

## System Requirements

### Minimum Requirements
- **RAM**: 1GB available memory
- **Storage**: 100MB free disk space
- **OS**: Linux (Ubuntu 18.04+), macOS (10.14+), Windows (10+)
- **Rust**: 1.70.0 or later (for building from source)

### Recommended Requirements
- **RAM**: 8GB+ available memory
- **Storage**: 10GB+ free disk space (SSD recommended)
- **CPU**: Multi-core processor (4+ cores recommended)
- **OS**: Linux (Ubuntu 20.04+), macOS (11.0+)

## Installation Methods

### 1. Pre-compiled Binaries (Recommended)

#### Linux (x86_64)
```bash
# Download latest release
curl -L https://github.com/your-org/lightning_db/releases/latest/download/lightning_db-linux-x86_64.tar.gz -o lightning_db.tar.gz

# Extract
tar -xzf lightning_db.tar.gz

# Install system-wide
sudo cp lightning_db/bin/* /usr/local/bin/
sudo cp lightning_db/lib/* /usr/local/lib/
sudo cp lightning_db/include/* /usr/local/include/

# Update library cache
sudo ldconfig
```

#### macOS
```bash
# Using Homebrew (recommended)
brew install lightning_db

# Or download manually
curl -L https://github.com/your-org/lightning_db/releases/latest/download/lightning_db-macos-x86_64.tar.gz -o lightning_db.tar.gz
tar -xzf lightning_db.tar.gz
sudo cp lightning_db/bin/* /usr/local/bin/
sudo cp lightning_db/lib/* /usr/local/lib/
sudo cp lightning_db/include/* /usr/local/include/
```

#### Windows
```powershell
# Download and extract to C:\Program Files\LightningDB
# Add C:\Program Files\LightningDB\bin to PATH environment variable
```

### 2. Building from Source

#### Prerequisites
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Install system dependencies
# Ubuntu/Debian:
sudo apt update
sudo apt install build-essential pkg-config libssl-dev

# CentOS/RHEL:
sudo yum groupinstall "Development Tools"
sudo yum install openssl-devel

# macOS:
xcode-select --install
```

#### Build Instructions
```bash
# Clone repository
git clone https://github.com/your-org/lightning_db.git
cd lightning_db

# Build release version
cargo build --release

# Run tests to verify build
cargo test --release

# Install locally
cargo install --path . --locked
```

#### Build with Features
```bash
# Build with all features
cargo build --release --all-features

# Build with specific features
cargo build --release --features "prometheus,opentelemetry,compression"

# Available features:
# - prometheus      : Prometheus metrics integration
# - opentelemetry   : OpenTelemetry integration  
# - compression     : Advanced compression support
# - async           : Async API support
# - monitoring      : Production monitoring hooks
```

### 3. Container Installation

#### Docker
```bash
# Pull official image
docker pull lightningdb/lightning_db:latest

# Run with persistent storage
docker run -d \
  --name lightning_db \
  -v /host/data:/data \
  -p 9090:9090 \
  lightningdb/lightning_db:latest

# Run with custom configuration
docker run -d \
  --name lightning_db \
  -v /host/data:/data \
  -v /host/config.toml:/etc/lightning_db/config.toml \
  -p 9090:9090 \
  lightningdb/lightning_db:latest
```

#### Kubernetes
```yaml
# lightning-db-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lightning-db
spec:
  replicas: 1
  selector:
    matchLabels:
      app: lightning-db
  template:
    metadata:
      labels:
        app: lightning-db
    spec:
      containers:
      - name: lightning-db
        image: lightningdb/lightning_db:latest
        ports:
        - containerPort: 9090
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "8Gi"  
            cpu: "4"
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: lightning-db-pvc
```

### 4. Language-Specific Installation

#### Rust
```toml
# Cargo.toml
[dependencies]
lightning_db = "1.0.0"
```

#### C/C++
```bash
# Install development headers
sudo apt install lightning-db-dev  # Ubuntu/Debian
sudo yum install lightning-db-devel # CentOS/RHEL

# Compile your application
gcc -o myapp myapp.c -llightning_db
```

#### Python (Coming Soon)
```bash
pip install lightning-db-py
```

## Post-Installation Verification

### 1. Command Line Verification
```bash
# Check installation
lightning_db --version
lightning_db --help

# Run basic functionality test
lightning_db test --quick

# Check library linking (Linux/macOS)
ldd $(which lightning_db)  # Linux
otool -L $(which lightning_db)  # macOS
```

### 2. API Verification
```rust
use lightning_db::{Database, LightningDbConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test basic functionality
    let db = Database::open("/tmp/test_db", LightningDbConfig::default())?;
    
    db.put(b"hello", b"world")?;
    let value = db.get(b"hello")?.unwrap();
    assert_eq!(value, b"world");
    
    println!("Lightning DB installation verified successfully!");
    Ok(())
}
```

### 3. Performance Verification
```bash
# Run built-in benchmarks
lightning_db benchmark --quick

# Expected output should show:
# - Read performance: >10K ops/sec
# - Write performance: >5K ops/sec
# - No critical errors
```

## Configuration

### 1. Default Configuration
Lightning DB will work out-of-the-box with sensible defaults, but you can customize behavior:

```bash
# Create config directory
mkdir -p ~/.config/lightning_db

# Generate default config
lightning_db config --generate > ~/.config/lightning_db/config.toml
```

### 2. System-wide Configuration
```bash
# System config location
sudo mkdir -p /etc/lightning_db
sudo lightning_db config --generate > /etc/lightning_db/config.toml
```

## Troubleshooting Installation

### Common Issues

#### 1. Permission Denied
```bash
# Fix: Install with proper permissions
sudo chown -R $(whoami) /usr/local/bin/lightning_db
sudo chmod +x /usr/local/bin/lightning_db
```

#### 2. Library Not Found
```bash
# Linux: Update library path
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
echo 'export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH' >> ~/.bashrc

# macOS: Update library path  
export DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH
echo 'export DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH' >> ~/.zshrc
```

#### 3. Build Failures
```bash
# Update Rust toolchain
rustup update stable

# Clean and rebuild
cargo clean
cargo build --release

# Check for missing system dependencies
# Ubuntu/Debian:
sudo apt install build-essential pkg-config libssl-dev

# CentOS/RHEL:
sudo yum groupinstall "Development Tools"
```

#### 4. Performance Issues
```bash
# Verify system resources
free -h              # Check available memory
df -h               # Check disk space
lscpu               # Check CPU information

# Run system compatibility check
lightning_db system-check
```

### Getting Help

- **Documentation**: See README.md and API.md
- **Issues**: Report bugs at https://github.com/your-org/lightning_db/issues  
- **Discussions**: Community forum at https://github.com/your-org/lightning_db/discussions
- **Support**: Enterprise support available

## Next Steps

After successful installation:

1. **Read the [Deployment Guide](DEPLOYMENT.md)** for production setup
2. **Review [Configuration Reference](CONFIGURATION.md)** for optimization
3. **Check [Security Guide](SECURITY.md)** for security hardening
4. **See [API Documentation](API.md)** for development
5. **Run performance benchmarks** to establish baselines

## Uninstallation

### Remove Binaries
```bash
# System installation
sudo rm -f /usr/local/bin/lightning_db*
sudo rm -f /usr/local/lib/liblightning_db*
sudo rm -f /usr/local/include/lightning_db.h

# Homebrew (macOS)
brew uninstall lightning_db

# Cargo installation
cargo uninstall lightning_db
```

### Remove Data and Configuration
```bash
# Remove user configuration
rm -rf ~/.config/lightning_db

# Remove system configuration  
sudo rm -rf /etc/lightning_db

# Remove data (WARNING: This deletes all databases)
# Only run if you're sure you want to delete all data
# rm -rf ~/.local/share/lightning_db
```