# Known Issues and Limitations

## Test Execution
- **Library tests may hang**: Run with `--test-threads=1` to avoid deadlocks
- **Some tests are disabled**: Tests marked with `#[ignore]` have known timeout issues

## Storage Limitations
- **Key Size**: Maximum 4096 bytes
- **Value Size**: Recommended <100MB for optimal performance
- **Cache Size**: Default configuration may need adjustment for production

## Performance Considerations
- **First-run initialization**: Database creation can be slow on initial setup
- **Lock contention**: High-concurrency workloads may experience reduced throughput
- **Large transactions**: Operations with >10,000 items may cause memory pressure

## Platform Support
- **Primary**: Linux and macOS fully tested
- **Secondary**: Windows support is experimental

## Workarounds

### Single-threaded test execution
```bash
cargo test -- --test-threads=1
```

### Memory-constrained environments
```rust
let config = LightningDbConfig {
    cache_size: 4 * 1024 * 1024, // 4MB
    ..Default::default()
};
```

### Batch operations for better performance
```rust
let mut batch = WriteBatch::new();
for (key, value) in items {
    batch.put(key, value)?;
}
db.write_batch(&batch)?;
```