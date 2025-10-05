# Known Issues and Limitations

## Test Execution
- **Long-running tests**: Some IO and snapshot tests may take 60+ seconds to complete
- **Recommended single-threaded**: Run with `--test-threads=1` for most reliable results
- **Integration tests**: Full test suite may timeout in CI environments with strict limits

## Storage Limitations
- **Key Size**: Maximum 64KB for inline optimization (per code)
- **Value Size**: Recommended <100MB for optimal performance
- **Database Size**: Tested up to 1TB, performance may degrade beyond that

## Performance Considerations
- **First-run initialization**: Database creation can be slow on initial setup
- **Lock contention**: High-concurrency workloads may experience reduced throughput
- **Large transactions**: Operations with >10,000 items may cause memory pressure
- **MemTable rotation**: Limited to max_immutable tables before backpressure

## Code Quality
- **Unwrap/Expect usage**: 804 instances across codebase (many in acceptable contexts)
- **Panic calls**: 8 instances (mostly test code or intentional error handling)
- **No TODO/FIXME**: All tracked items have been resolved or removed

## Platform Support
- **Primary**: Linux and macOS fully tested
- **Secondary**: Windows support is experimental
- **Jemalloc**: Not available on MSVC targets

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