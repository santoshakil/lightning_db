# Lightning DB Documentation

Welcome to the comprehensive documentation for Lightning DB, a high-performance embedded key-value database written in Rust.

## 📚 Documentation Overview

This documentation provides everything you need to understand, deploy, operate, and contribute to Lightning DB.

### 🚀 Getting Started

**New to Lightning DB?** Start here:

1. **[Production Readiness Guide](production-readiness.md)** - Current status, limitations, and deployment guidelines
2. **[Architecture Overview](architecture-overview.md)** - High-level system design and components
3. **[API Reference](api-reference.md)** - Complete API documentation with examples

### 🏗️ For Operators

**Deploying and managing Lightning DB in production:**

4. **[Operations Manual](operations-manual.md)** - Deployment, monitoring, backup, and troubleshooting
5. **[Performance Tuning Guide](#)** - Optimization for different workloads *(coming soon)*
6. **[Security Guidelines](#)** - Security best practices *(coming soon)*

### 👩‍💻 For Developers

**Contributing to Lightning DB development:**

7. **[Development Guide](development-guide.md)** - Setup, coding standards, and contribution workflow
8. **[Internals Documentation](#)** - Deep dive into implementation details *(coming soon)*
9. **[Testing Guidelines](#)** - Testing strategies and frameworks *(coming soon)*

## 🎯 Lightning DB at a Glance

### What is Lightning DB?

Lightning DB is a high-performance embedded key-value database designed for:
- **Ultra-low latency**: Sub-microsecond read operations
- **High throughput**: Millions of operations per second
- **Small footprint**: <5MB binary, configurable memory usage
- **Reliability**: ACID transactions with durability guarantees
- **Modern architecture**: Hybrid B+Tree/LSM design optimized for current hardware

### Key Features

#### ⚡ Performance
- **14M+ reads/sec** at 0.07μs latency
- **350K+ writes/sec** at 2.81μs latency
- SIMD-optimized operations
- Lock-free hot paths
- Adaptive caching (ARC algorithm)

#### 🛡️ Reliability
- ACID transactions with MVCC
- Write-ahead logging (WAL)
- Crash recovery
- Data integrity verification
- Comprehensive error handling

#### 🔧 Flexibility
- Multiple consistency levels
- Configurable compression (Zstd, LZ4)
- Secondary indexes
- Range queries and iterators
- Batch operations

#### 🏗️ Architecture
- Hybrid B+Tree/LSM storage engine
- Memory-mapped I/O with zero-copy optimization
- Background compaction
- Pluggable compression algorithms
- Prometheus metrics integration

### Current Status: ⚠️ STAGING ONLY

**Important**: Lightning DB achieves exceptional performance but requires critical fixes before production deployment:

- ✅ **Performance**: Exceeds all targets by 3-14x
- ✅ **Architecture**: Modern, well-designed hybrid approach
- ✅ **Memory Safety**: Rust prevents entire classes of bugs
- ❌ **Critical Issues**: Memory leak, integrity bypass, panic risks

**See [Production Readiness Guide](production-readiness.md) for detailed status and timeline.**

## 📖 Quick Reference

### Basic Usage

```rust
use lightning_db::{Database, LightningDbConfig};

// Create database
let db = Database::create("./mydb", LightningDbConfig::default())?;

// Basic operations
db.put(b"key", b"value")?;
let value = db.get(b"key")?;
db.delete(b"key")?;

// Transactions
let tx_id = db.begin_transaction()?;
db.put_tx(tx_id, b"tx_key", b"tx_value")?;
db.commit_transaction(tx_id)?;

// Range queries
let iter = db.scan_prefix(b"user:")?;
for result in iter {
    let (key, value) = result?;
    println!("{:?} = {:?}", key, value);
}
```

### CLI Usage

```bash
# Create database
lightning-cli create ./mydb --cache-size 100

# Basic operations
lightning-cli put ./mydb key value
lightning-cli get ./mydb key
lightning-cli delete ./mydb key

# Database operations
lightning-cli stats ./mydb
lightning-cli backup ./mydb /backup/location
lightning-cli verify-integrity ./mydb
```

### Configuration Example

```rust
let config = LightningDbConfig {
    // Performance
    cache_size: 256 * 1024 * 1024,  // 256MB cache
    compression_enabled: true,
    
    // Reliability
    wal_sync_mode: WalSyncMode::Sync,
    enable_safety_guards: true,
    
    // Limits
    max_active_transactions: 1000,
    write_batch_size: 1000,
    
    ..Default::default()
};
```

## 🗺️ Documentation Roadmap

### Current Documentation (✅ Complete)
- **Production Readiness Guide**: Deployment status and guidelines
- **Architecture Overview**: System design and components
- **API Reference**: Complete API with examples
- **Operations Manual**: Production deployment and management
- **Development Guide**: Contributing and development workflow

### Planned Documentation (🚧 Coming Soon)
- **Performance Tuning Guide**: Workload-specific optimization
- **Security Guidelines**: Security best practices and compliance
- **Internals Documentation**: Deep implementation details
- **Testing Guidelines**: Testing strategies and frameworks
- **Migration Guide**: Upgrading and data migration procedures

## 🎯 Use Cases

### Ideal For
- **High-frequency trading**: Ultra-low latency requirements
- **Edge computing**: Small binary, minimal resource usage
- **Real-time analytics**: Fast mixed read/write workloads
- **IoT applications**: Resource-constrained environments
- **Caching layers**: High-performance data caching

### Consider Alternatives For
- **Large-scale distributed systems**: Single-node design
- **SQL workloads**: Key-value interface only
- **Append-only logs**: Not optimized for log-structured data
- **Complex analytics**: Limited query capabilities

## 🚨 Important Notes

### Database Development Principles

This is a **DATABASE** - every decision must prioritize:

1. **Data Integrity**: Never lose or corrupt data
2. **Reliability**: Consistent behavior under all conditions
3. **Durability**: Survive crashes and recover fully
4. **Performance**: Meet SLAs consistently
5. **Operability**: Easy to monitor and troubleshoot

### Known Limitations

Before using Lightning DB, understand these current limitations:

- **Memory Leak**: 63.84 MB growth per 30 seconds
- **Integrity Bypass**: Corruption detection vulnerability
- **Panic Risk**: 125+ unwrap() calls in codebase
- **Stability Issues**: 28.6% chaos test failure rate

**Timeline to fix**: 4-6 weeks of focused development

## 🤝 Community

### Getting Help
- **Issues**: [GitHub Issues](https://github.com/org/lightning-db/issues)
- **Discussions**: [GitHub Discussions](https://github.com/org/lightning-db/discussions)
- **Discord**: [Lightning DB Community](https://discord.gg/lightning-db)

### Contributing
We welcome contributions! See the [Development Guide](development-guide.md) for:
- Setting up development environment
- Coding standards and guidelines
- Testing requirements
- Pull request process

### Reporting Issues
When reporting issues, include:
- Lightning DB version
- Operating system and hardware
- Minimal reproduction case
- Configuration used
- Error messages and logs

## 📝 License

Lightning DB is licensed under [Apache License 2.0](../LICENSE).

## 📞 Support

For production support inquiries, contact the Lightning DB team:
- **Email**: support@lightning-db.com
- **Enterprise Support**: Available for production deployments

---

**Ready to get started?** 

- **Explore Lightning DB**: Read the [Architecture Overview](architecture-overview.md)
- **Try the API**: Check out the [API Reference](api-reference.md) with examples
- **Deploy safely**: Follow the [Production Readiness Guide](production-readiness.md)
- **Contribute**: Set up your environment with the [Development Guide](development-guide.md)

*Lightning DB: Redefining database performance, one microsecond at a time.* ⚡

---

*Documentation Version: 1.0*  
*Last Updated: December 2024*  
*Maintained by: Lightning DB Documentation Team*