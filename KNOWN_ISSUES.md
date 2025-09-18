# Known Issues and Limitations

## Current Issues

### Test Infrastructure
- **Hanging Tests**: Some tests may hang when run in multi-threaded mode due to lock contention. Always run tests with `--test-threads=1`
- **Encryption Tests**: Page encryption tests are currently disabled (`#[ignore]`) due to timeout issues in key manager initialization

### Performance Limitations
- **Database Initialization**: Database creation and opening can be slow on first run due to extensive initialization
- **Memory Usage**: Cache sizing may not accurately reflect actual memory usage in all scenarios
- **Lock Contention**: High contention workloads may experience degraded performance due to lock granularity

### Platform-Specific Issues
- **macOS**: File locking behavior may differ from Linux, potentially affecting concurrent access patterns
- **Windows**: Not thoroughly tested, may have path handling issues

## Architectural Limitations

### Storage Engine
- **Key Size**: Keys are limited to 64KB for inline optimization
- **Value Size**: Individual values should not exceed 100MB for optimal performance
- **Database Size**: While theoretically unlimited, databases over 1TB may experience degraded performance

### Transactions
- **Active Transactions**: Limited to configured maximum (default 1000) concurrent transactions
- **Transaction Size**: Large transactions (>10000 operations) may cause memory pressure
- **Isolation Levels**: Serializable isolation may significantly impact performance under high contention

### Backup and Recovery
- **Incremental Backups**: Require significant temporary disk space
- **Recovery Time**: Proportional to WAL size, can be slow for databases with large write volumes
- **Point-in-Time Recovery**: Limited to WAL retention period

## Workarounds

### For Hanging Tests
```bash
# Always run tests single-threaded
cargo test --release -- --test-threads=1
```

### For Memory Issues
```rust
// Configure smaller cache sizes for memory-constrained environments
let config = LightningDbConfig {
    cache_size: 4 * 1024 * 1024, // 4MB instead of default
    ..Default::default()
};
```

### For Lock Contention
```rust
// Use batch operations to reduce lock acquisition overhead
let mut batch = WriteBatch::new();
for (key, value) in items {
    batch.put(key, value)?;
}
db.write_batch(&batch)?;
```

## Future Improvements

1. **Lock-Free Data Structures**: Implement lock-free B+Tree for better concurrent performance
2. **Async I/O**: Add async support for better resource utilization
3. **Compression Improvements**: Add Zstandard support for better compression ratios
4. **Memory Management**: Implement more accurate memory accounting
5. **Test Stability**: Fix test infrastructure to support parallel execution

## Reporting Issues

Please report issues to the project repository with:
- Operating system and version
- Rust compiler version
- Minimal reproducible example
- Error messages and stack traces
- Test output with `--nocapture` flag