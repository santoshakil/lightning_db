# Lightning DB CLI Reference

The Lightning DB command-line interface provides comprehensive tools for database management, data operations, performance benchmarking, and maintenance tasks.

## Installation

```bash
# Build the CLI with release optimizations
cargo build --release --features cli

# The binary will be at:
./target/release/lightning-cli
```

## Global Options

All commands support these global options:

| Option | Short | Description |
|--------|-------|-------------|
| `--format <format>` | `-o` | Output format: `text` (default) or `json` |
| `--quiet` | `-q` | Suppress informational output (errors still shown) |
| `--help` | `-h` | Print help information |
| `--version` | `-V` | Print version information |

### JSON Output Mode

Use `-o json` or `--format json` to get machine-readable JSON output for all commands. This is ideal for:
- Scripting and automation
- Integration with monitoring systems
- Programmatic parsing of results

**Example:**
```bash
# Get key value as JSON
lightning-cli -o json get my_database key1

# Output:
# {
#   "key": "key1",
#   "value": "value1",
#   "size": 6,
#   "status": "success"
# }
```

### Quiet Mode

Use `-q` to suppress informational output. Only errors are shown. Useful for scripts that only care about exit codes.

**Example:**
```bash
# Silent put operation
lightning-cli -q put my_database key1 value1
echo "Exit code: $?"
```

## Command Overview

| Command | Description |
|---------|-------------|
| `create` | Create a new database |
| `get` | Retrieve a value by key |
| `put` | Store a key-value pair |
| `delete` | Delete a key |
| `scan` | Scan keys in a range |
| `backup` | Create a database backup |
| `restore` | Restore from backup |
| `stats` | Show database statistics |
| `health` | Check database health |
| `compact` | Trigger database compaction |
| `bench` | Run performance benchmark |
| `check` | Check database integrity |
| `tx-test` | Test transaction ACID properties |
| `index-test` | Test index operations |
| `index-list` | List all indexes in database |

---

## Commands

### create

Create a new database at the specified path.

```
Usage: lightning-cli create [OPTIONS] <path>

Arguments:
  <path>  Database path

Options:
      --cache-size <cache-size>  Cache size in MB (1-102400) [default: 100]
      --compression              Enable compression
```

**Examples:**

```bash
# Create a basic database
lightning-cli create my_database

# Create with 256MB cache and compression enabled
lightning-cli create my_database --cache-size 256 --compression
```

---

### get

Retrieve a value by its key.

```
Usage: lightning-cli get [OPTIONS] <path> <key>

Arguments:
  <path>  Database path
  <key>   Key to retrieve

Options:
      --format <format>  Output format [default: text]
                         Possible values: text, hex, json
```

**Examples:**

```bash
# Get a value as text
lightning-cli get my_database user:1001

# Get as hexadecimal (useful for binary data)
lightning-cli get my_database binary_key --format hex

# Get as JSON (wrapped with metadata)
lightning-cli get my_database config --format json
```

**Output:**
- Success: Prints the value to stdout
- Not found: Prints "Key not found" to stderr, exits with code 3 (NOT_FOUND)

**JSON output example:**
```bash
lightning-cli -o json get my_database user:1001

# Success response:
# {
#   "key": "user:1001",
#   "value": "John Doe",
#   "size": 8,
#   "status": "success"
# }

# Not found response (still outputs JSON before error):
# {
#   "key": "nonexistent",
#   "error": "key_not_found",
#   "status": "error"
# }
```

---

### put

Store a key-value pair in the database.

```
Usage: lightning-cli put <path> <key> <value>

Arguments:
  <path>   Database path
  <key>    Key to store
  <value>  Value to store
```

**Examples:**

```bash
# Store a simple value
lightning-cli put my_database user:1001 "John Doe"

# Store JSON data
lightning-cli put my_database config '{"debug": true, "port": 8080}'
```

**Constraints:**
- Key size: 1 byte to 64KB
- Value size: Up to 100MB (recommended <1MB for optimal performance)

---

### delete

Delete a key from the database.

```
Usage: lightning-cli delete <path> <key>

Arguments:
  <path>  Database path
  <key>   Key to delete
```

**Examples:**

```bash
# Delete a key
lightning-cli delete my_database user:1001
```

---

### scan

Scan keys in a range, with optional filtering and limits.

```
Usage: lightning-cli scan [OPTIONS] <path>

Arguments:
  <path>  Database path

Options:
      --start <start>  Start key (inclusive) [default: ""]
      --end <end>      End key (exclusive)
      --limit <limit>  Maximum results (1-1000000) [default: 100]
      --reverse        Scan in reverse order
```

**Examples:**

```bash
# Scan all keys (first 100)
lightning-cli scan my_database

# Scan keys in range
lightning-cli scan my_database --start "user:1000" --end "user:2000"

# Scan with limit
lightning-cli scan my_database --limit 50

# Reverse scan (newest first)
lightning-cli scan my_database --start "log:" --reverse --limit 10
```

**Output format:**
```
key1 = value1
key2 = value2
...
```

---

### backup

Create a database backup.

```
Usage: lightning-cli backup [OPTIONS] <path> <output>

Arguments:
  <path>    Database path
  <output>  Backup output path

Options:
      --incremental          Create incremental backup
      --compress <compress>  Compression type [default: none]
                             Possible values: none, zstd
                             Note: zstd requires zstd-compression feature
```

**Examples:**

```bash
# Create a full backup (uncompressed by default)
lightning-cli backup my_database ./backups/backup_2024

# Create backup with LZ4 compression
lightning-cli backup my_database ./backups/backup_lz4 --compress lz4

# Create incremental backup (faster, smaller)
lightning-cli backup my_database ./backups/backup_incr --incremental
```

---

### restore

Restore a database from a backup.

```
Usage: lightning-cli restore [OPTIONS] <backup> <output>

Arguments:
  <backup>  Backup path
  <output>  Restore destination

Options:
      --verify  Verify backup integrity before restore
```

**Examples:**

```bash
# Restore from backup
lightning-cli restore ./backups/backup_2024 ./restored_db

# Restore with verification (recommended for critical data)
lightning-cli restore ./backups/backup_2024 ./restored_db --verify
```

---

### stats

Display database statistics.

```
Usage: lightning-cli stats [OPTIONS] <path>

Arguments:
  <path>  Database path

Options:
      --detailed  Show detailed statistics
```

**Examples:**

```bash
# Show basic stats
lightning-cli stats my_database

# Show detailed statistics
lightning-cli stats my_database --detailed
```

**Sample output:**
```
Database Statistics:
  Page count: 1,234
  Tree height: 4
  Cache hit rate: 94.5%
  Memory usage: 128 MB
  Disk usage: 512 MB
```

---

### health

Check database health status.

```
Usage: lightning-cli health [OPTIONS] <path>

Arguments:
  <path>  Database path

Options:
      --verify  Perform integrity verification
```

**Examples:**

```bash
# Quick health check
lightning-cli health my_database

# Health check with integrity verification
lightning-cli health my_database --verify
```

**Exit codes:**
- 0: Healthy
- 1: Issues detected

---

### compact

Trigger database compaction to reclaim space and optimize performance.

```
Usage: lightning-cli compact [OPTIONS] <path>

Arguments:
  <path>  Database path

Options:
      --force  Force compaction even if not needed
```

**Examples:**

```bash
# Compact if needed
lightning-cli compact my_database

# Force compaction
lightning-cli compact my_database --force
```

---

### bench

Run performance benchmarks on the database.

```
Usage: lightning-cli bench [OPTIONS] <path>

Arguments:
  <path>  Database path

Options:
      --ops <operations>         Number of operations (1-10000000) [default: 10000]
      --threads <threads>        Number of threads (1-256) [default: 1]
      --value-size <value-size>  Value size in bytes (1-10485760) [default: 100]
```

**Examples:**

```bash
# Quick benchmark (10K ops)
lightning-cli bench my_database

# Full benchmark (1M ops, 8 threads)
lightning-cli bench my_database --ops 1000000 --threads 8

# Large value benchmark
lightning-cli bench my_database --ops 50000 --value-size 10240
```

**Sample output:**
```
Benchmark Results:
  Operations: 1,000,000
  Threads: 8
  Duration: 2.34s
  Throughput: 427,350 ops/sec
  Latency (p50): 0.12ms
  Latency (p99): 0.89ms
```

---

### check

Check database integrity with configurable verification depth.

```
Usage: lightning-cli check [OPTIONS] <path>

Arguments:
  <path>  Database path

Options:
      --checksums <SAMPLE_SIZE>  Verify checksums (sample size) [default: 100]
      --verbose                  Verbose output
```

**Examples:**

```bash
# Basic integrity check
lightning-cli check my_database

# Thorough check with verbose output
lightning-cli check my_database --checksums 1000 --verbose

# Full checksum verification
lightning-cli check my_database --checksums 0 --verbose
```

**Note:** Setting `--checksums 0` verifies all pages (thorough but slower).

---

### tx-test

Test transaction ACID properties (Atomicity, Consistency, Isolation, Durability).

```
Usage: lightning-cli tx-test [OPTIONS] <path>

Arguments:
  <path>  Database path

Options:
      --isolation <isolation>  Isolation level [default: snapshot]
                               Possible values: read-committed, repeatable-read, snapshot, serializable
      --ops <operations>       Number of operations per transaction (1-10000) [default: 100]
      --verbose                Show detailed test output
```

**Examples:**

```bash
# Run transaction tests with default settings
lightning-cli tx-test my_database

# Test with specific isolation level
lightning-cli tx-test my_database --isolation serializable

# Verbose output with more operations
lightning-cli tx-test my_database --ops 1000 --verbose
```

**Tests performed:**
1. Basic transaction commit
2. Transaction rollback
3. Read isolation (uncommitted writes not visible)
4. Write durability (committed writes persist)
5. Transaction atomicity (all-or-nothing)

---

### index-test

Test index operations and performance.

```
Usage: lightning-cli index-test [OPTIONS] <path>

Arguments:
  <path>  Database path

Options:
      --ops <operations>  Number of operations (1-50, limited by page size) [default: 50]
      --verbose           Show detailed test output
```

**Examples:**

```bash
# Run index tests
lightning-cli index-test my_database

# Detailed test with more operations
lightning-cli index-test my_database --ops 500 --verbose
```

**Tests performed:**
1. Index creation and deletion
2. Index lookup performance
3. Indexed updates
4. Index listing

---

### index-list

List all indexes in the database.

```
Usage: lightning-cli index-list <path>

Arguments:
  <path>  Database path
```

**Examples:**

```bash
# List all indexes
lightning-cli index-list my_database
```

**Sample output:**
```
Indexes (3):
  - user_email_idx
  - order_date_idx
  - product_category_idx
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Usage error (invalid arguments) |
| 3 | Database not found |
| 4 | Database already exists |
| 5 | Validation error |
| 6 | Permission denied |
| 7 | IO error |
| 8 | Database operation error |
| 9 | Transaction error |
| 10 | Integrity check failed |
| 11 | Test failure |
| 99 | Internal error |

---

## Environment Variables

Currently, Lightning DB CLI does not use environment variables. All configuration is passed via command-line arguments.

---

## Tips and Best Practices

1. **Use compression** for production databases to save disk space:
   ```bash
   lightning-cli create my_db --compression
   ```

2. **Set appropriate cache size** based on available memory:
   ```bash
   lightning-cli create my_db --cache-size 512  # 512MB cache
   ```

3. **Run regular health checks** in production:
   ```bash
   lightning-cli health my_db --verify
   ```

4. **Benchmark before deploying** to understand performance characteristics:
   ```bash
   lightning-cli bench my_db --ops 100000 --threads 4
   ```

5. **Create backups regularly** and verify them:
   ```bash
   lightning-cli backup my_db ./backup --compress lz4
   lightning-cli restore ./backup ./test_restore --verify
   ```

---

## Troubleshooting

### "Database not found"
Ensure the path points to a valid Lightning DB directory created with `create`.

### "Permission denied"
Check file permissions on the database directory.

### "Database is locked"
Another process may have the database open. Lightning DB uses file locking to prevent corruption.

### Slow operations
- Run `compact` to optimize the database
- Increase cache size with `--cache-size`
- Check `stats` for cache hit rate
