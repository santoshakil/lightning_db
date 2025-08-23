# Lightning DB Integration Test Suite

A comprehensive integration test suite for Lightning DB that validates the entire system working together under realistic conditions.

## Overview

This integration test suite provides extensive coverage of Lightning DB's functionality through realistic scenarios that test the complete system rather than individual components. The tests are designed to validate production readiness, performance characteristics, and system reliability under various conditions.

## Test Categories

### 🚀 End-to-End Tests (`end_to_end_tests.rs`)
- **Complete Database Lifecycle**: Create, insert, query, update, delete, drop operations
- **Transaction Workflows**: ACID compliance, commit/rollback scenarios
- **Multi-table Operations**: Complex queries and join simulations
- **Bulk Operations**: Large-scale data loading and batch processing
- **Database Reopen**: Persistence verification across restarts

### ⚡ Concurrency Tests (`concurrency_tests.rs`)
- **Multi-threaded Workloads**: Read-heavy, write-heavy, and mixed patterns
- **Transaction Isolation**: MVCC behavior and isolation level testing
- **Deadlock Detection**: Deadlock scenarios and resolution mechanisms
- **Connection Pooling**: Simulated connection pool behavior
- **High Contention**: Hot key scenarios and lock contention

### 🔄 Recovery Integration Tests (`recovery_integration_tests.rs`)
- **Crash Recovery**: WAL replay and data consistency after crashes
- **Transaction Recovery**: Uncommitted transaction rollback
- **Backup and Restore**: Full and incremental backup workflows
- **Point-in-time Recovery**: Consistent state restoration
- **Corruption Recovery**: Data integrity validation and repair

### 📊 Performance Integration Tests (`performance_integration_tests.rs`)
- **Mixed OLTP/OLAP Workloads**: Real-world query patterns
- **Cache Effectiveness**: Memory utilization and cache hit rates
- **I/O Subsystem Stress**: Disk throughput and latency testing
- **Memory Management**: Memory pressure and leak detection
- **Query Optimization**: Performance validation of different query types

### 🔧 High Availability Tests (`ha_tests.rs`)
- **Replication Setup**: Multi-node cluster configuration
- **Failover/Failback**: Primary node failure and recovery scenarios
- **Split-brain Prevention**: Network partition handling
- **Load Balancing**: Traffic distribution validation
- **Rolling Upgrades**: Zero-downtime upgrade procedures

### 🔒 Security Integration Tests (`security_integration_tests.rs`)
- **Authentication Flows**: User login, session management, timeouts
- **Authorization**: Role-based access control and permissions
- **Encryption**: Data encryption at rest and in transit
- **Audit Logging**: Security event tracking and compliance
- **Attack Prevention**: SQL injection, input validation

### 🖥️ System Integration Tests (`system_integration_tests.rs`)
- **Operating System Interaction**: File handles, memory management
- **Network Stack Integration**: Connection handling, timeouts
- **Monitoring Integration**: Metrics collection and alerting
- **Backup System Integration**: External backup tool compatibility
- **Container/Kubernetes**: Deployment and scaling scenarios

### 💥 Chaos and Stress Tests (`chaos_tests.rs`)
- **Resource Exhaustion**: Memory, disk, file handle limits
- **Random Failure Injection**: Simulated component failures
- **Long-running Stability**: Extended operation validation
- **Memory Leak Detection**: Long-term memory usage monitoring
- **Performance Degradation**: System behavior under stress

## Test Orchestration

The `test_orchestrator.rs` module provides a comprehensive test execution framework with:

- **Test Suite Management**: Organized test execution with dependencies
- **Parallel Execution**: Configurable parallel test execution
- **Reporting**: JUnit XML, HTML, and JSON report generation
- **CI/CD Integration**: GitHub Actions, GitLab CI, Jenkins support
- **Coverage Analysis**: Code coverage reporting and analysis
- **Metrics Collection**: Performance and resource usage tracking

## Quick Start

### Running Integration Tests

1. **Smoke Tests** (Fast, suitable for CI):
   ```bash
   ./scripts/run_integration_tests.sh --suite smoke
   ```

2. **Full Integration Suite**:
   ```bash
   ./scripts/run_integration_tests.sh --suite integration --coverage --html-report
   ```

3. **Performance Tests**:
   ```bash
   ./scripts/run_integration_tests.sh --suite performance --timeout 3600
   ```

4. **Security Tests**:
   ```bash
   ./scripts/run_integration_tests.sh --suite security --junit
   ```

5. **Chaos Engineering Tests**:
   ```bash
   ./scripts/run_integration_tests.sh --suite chaos --timeout 7200 --retry 0
   ```

### Using Cargo Directly

Run specific test modules:
```bash
# End-to-end tests
cargo test --test integration end_to_end_tests -- --nocapture

# Concurrency tests
cargo test --test integration concurrency_tests -- --nocapture

# Performance tests (longer running)
cargo test --test integration performance_integration_tests -- --nocapture --test-threads=1
```

## Configuration

### Test Suites

| Suite | Description | Duration | Parallelizable |
|-------|-------------|----------|----------------|
| `smoke` | Fast smoke tests for CI | ~5 min | Yes |
| `integration` | Core integration tests | ~20 min | Yes |
| `performance` | Performance validation | ~30 min | Limited |
| `security` | Security feature tests | ~15 min | Yes |
| `chaos` | Stress and chaos tests | ~60 min | No |
| `all` | Complete test suite | ~2 hours | Mixed |

### Environment Variables

- `RUST_LOG`: Set logging level (`debug`, `info`, `warn`, `error`)
- `RUST_BACKTRACE`: Enable stack traces (`1` or `full`)
- `LIGHTNING_DB_TEST_TIMEOUT`: Override default test timeouts
- `LIGHTNING_DB_TEST_PARALLEL`: Override parallel execution count

### Test Data

Tests use temporary directories and databases that are automatically cleaned up. No persistent test data is created outside of the test execution.

## CI/CD Integration

### GitHub Actions

The `.github/workflows/integration-tests.yml` file provides comprehensive CI integration:

- **Pull Request Testing**: Smoke tests on every PR
- **Nightly Testing**: Full integration suite every night
- **Manual Triggers**: On-demand test execution with parameter selection
- **Cross-platform Testing**: Linux, macOS, and Windows validation

### Local Development

For development workflows:

```bash
# Quick validation before committing
./scripts/run_integration_tests.sh --suite smoke --parallel auto

# Full validation before merging
./scripts/run_integration_tests.sh --suite integration --coverage

# Performance regression testing
./scripts/run_integration_tests.sh --suite performance --html-report
```

## Test Writing Guidelines

### Test Structure

1. **Setup**: Use `TestEnvironment` for consistent test setup
2. **Execution**: Follow arrange-act-assert pattern
3. **Cleanup**: Automatic cleanup via RAII and Drop traits
4. **Assertions**: Comprehensive validation with meaningful error messages

### Example Test

```rust
#[test]
fn test_database_feature() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Arrange
    setup_test_data(&db).unwrap();
    
    // Act
    let result = db.some_operation();
    
    // Assert
    assert!(result.is_ok(), "Operation should succeed");
    assert_eq!(result.unwrap(), expected_value);
    
    // Verify side effects
    let verification = db.verify_state();
    assert!(verification, "Database state should be consistent");
}
```

### Best Practices

1. **Isolation**: Each test should be independent and not affect others
2. **Determinism**: Tests should produce consistent results
3. **Meaningful Names**: Test names should describe the scenario being tested
4. **Documentation**: Complex tests should include comments explaining the scenario
5. **Error Handling**: Proper error handling and informative failure messages
6. **Performance**: Reasonable test execution times while maintaining coverage

## Performance Characteristics

### Expected Test Durations

- **Smoke Tests**: 2-5 minutes
- **Integration Tests**: 15-25 minutes
- **Performance Tests**: 20-40 minutes
- **Security Tests**: 10-20 minutes
- **Chaos Tests**: 30-90 minutes

### Resource Requirements

- **Memory**: 2-8 GB RAM depending on test suite
- **Disk**: 1-5 GB temporary space
- **CPU**: Multi-core recommended for parallel execution
- **Network**: Minimal (only for system integration tests)

## Troubleshooting

### Common Issues

1. **Test Timeouts**: Increase timeout values or check system performance
2. **Resource Exhaustion**: Monitor memory and file handle usage
3. **Flaky Tests**: Check for race conditions and timing dependencies
4. **CI Failures**: Review CI logs and artifact outputs

### Debugging

1. **Verbose Output**: Use `--nocapture` flag with cargo test
2. **Logging**: Set `RUST_LOG=debug` for detailed logging
3. **Test Reports**: Check HTML reports for detailed test information
4. **Metrics**: Review performance metrics in test output

## Contributing

### Adding New Tests

1. Choose the appropriate test module
2. Follow existing test patterns
3. Update test suites in `test_orchestrator.rs`
4. Add documentation for new test scenarios
5. Update CI configuration if needed

### Test Categories

When adding tests, consider:
- **Functionality**: What database feature is being tested?
- **Load**: Light, medium, or heavy resource usage?
- **Duration**: Short (<1min), medium (1-5min), or long (>5min)?
- **Dependencies**: External systems or specific hardware?

## Monitoring and Metrics

### Test Metrics

- **Execution Time**: Per-test and suite-level timing
- **Resource Usage**: Memory, CPU, disk I/O
- **Error Rates**: Test failure patterns and trends
- **Coverage**: Code coverage from integration tests

### Performance Tracking

- **Regression Detection**: Performance trend analysis
- **Benchmark Results**: Throughput and latency measurements
- **Resource Efficiency**: Memory and CPU utilization
- **Scalability**: Multi-core and multi-node performance

## Support

For questions or issues with the integration test suite:

1. Check this documentation
2. Review existing test patterns
3. Check CI logs and artifacts
4. Create an issue with test logs and environment details

---

**Note**: This integration test suite is designed to complement unit tests, not replace them. Integration tests focus on system-level validation while unit tests provide detailed component validation.