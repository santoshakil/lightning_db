# Lightning DB Examples

This directory contains comprehensive examples demonstrating Lightning DB features and usage patterns.

## Directory Structure

### `basic/`
Getting started with Lightning DB
- `basic_usage.rs` - Simple CRUD operations
- `demo.rs` - Quick demonstration of core features
- `best_practices.rs` - Recommended patterns and practices

### `advanced/`
Advanced features and capabilities
- `encryption_example.rs` - Database encryption
- `all_features.rs` - Comprehensive feature showcase

### `benchmarks/`
Performance testing and benchmarking
- `benchmark.rs` - Performance benchmarks
- `stress_test_suite.rs` - Stress testing scenarios

### `production/`
Production deployment examples
- `config_management_demo.rs` - Configuration management
- `health_check_server.rs` - Health monitoring server
- `distributed_tracing_example.rs` - OpenTelemetry integration

### `diagnostics/`
Debugging and diagnostic tools
- `transaction_debug_detailed.rs` - Transaction debugging
- `transaction_loss_diagnostic.rs` - Transaction loss analysis
- `transaction_manager_comparison.rs` - Transaction manager comparison
- `version_store_debug.rs` - Version store debugging
- `race_condition_diagnostic.rs` - Race condition detection

### `migration_examples/`
Database migration patterns
- Progressive migration examples (e01-e10)
- Zero-downtime migration strategies
- Schema evolution patterns

### Platform-Specific

#### `android/`
Android integration examples

#### `ios/`
iOS Swift integration with Package.swift

#### `wasm/`
WebAssembly browser examples

#### `desktop/`
Desktop application integration

## Running Examples

### Basic Example
```bash
cargo run --example basic_usage
```

### Benchmark
```bash
cargo run --release --example benchmark
```

### Production Health Check
```bash
cargo run --release --example health_check_server
```

## Quick Start

1. Start with `basic/basic_usage.rs` for fundamental operations
2. Review `basic/best_practices.rs` for recommended patterns
3. Explore `advanced/` for specific features you need
4. Use `production/` examples for deployment guidance
5. Reference `diagnostics/` for troubleshooting

For more detailed documentation, see the [main documentation](../docs/).