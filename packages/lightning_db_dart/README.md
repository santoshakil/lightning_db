# Lightning DB Dart FFI Bindings

Core FFI bindings for Lightning DB - a high-performance embedded database.

## Overview

This package provides low-level Dart FFI bindings to the Lightning DB native library. For most Flutter applications, you should use the higher-level `lightning_db` package instead, which provides a more convenient API with Freezed support.

## Installation

```yaml
dependencies:
  lightning_db_dart: ^0.1.0
```

## Basic Usage

```dart
import 'package:lightning_db_dart/lightning_db_dart.dart';

// Initialize the library
LightningDbInit.init();

// Create or open a database
final db = LightningDb.create('path/to/database.db');

// Basic operations
db.put('key', 'value');
final value = db.get('key');
db.delete('key');

// Binary data
final bytes = Uint8List.fromList([1, 2, 3, 4, 5]);
db.putBytes('binary_key', bytes);
final retrieved = db.getBytes('binary_key');

// JSON data
db.putJson('json_key', {'name': 'John', 'age': 30});
final json = db.getJson('json_key');

// Check existence
if (db.contains('key')) {
  print('Key exists');
}

// Close the database
db.close();
```

## Advanced Features

### Configuration

```dart
final config = DatabaseConfig(
  pageSize: 4096,              // Page size in bytes
  cacheSize: 50 * 1024 * 1024, // 50MB cache
  syncMode: SyncMode.normal,   // Sync mode
  journalMode: JournalMode.wal,// Write-ahead logging
  enableCompression: true,     // Enable compression
  compressionType: CompressionType.lz4, // Compression algorithm
);

final db = LightningDb.create('database.db', config: config);
```

### Transactions

```dart
// Manual transaction management
final tx = db.beginTransaction();
try {
  tx.put('key1', 'value1');
  tx.put('key2', 'value2');
  tx.commit();
} catch (e) {
  tx.rollback();
  rethrow;
}

// Transaction helper
db.runInTransaction(() {
  db.put('key1', 'value1');
  db.put('key2', 'value2');
  // Automatically commits or rolls back
});
```

### Batch Operations

```dart
final batch = db.batch();
batch.put('key1', 'value1');
batch.put('key2', 'value2');
batch.delete('key3');
batch.commit();
```

### Iteration

```dart
// Iterate over all keys
for (final key in db.keys()) {
  print('Key: $key');
}

// Iterate with prefix
for (final key in db.keys(prefix: 'user:')) {
  final value = db.get(key);
  print('$key = $value');
}

// Iterate with callback
db.iterate((key, value) {
  print('$key = $value');
  return true; // Continue iteration
});
```

### Statistics and Maintenance

```dart
// Get database statistics
final stats = db.getStatistics();
print('Total keys: ${stats.keyCount}');
print('Disk usage: ${stats.diskSize} bytes');
print('Memory usage: ${stats.memoryUsage} bytes');
print('Cache hit rate: ${stats.cacheHitRate}%');

// Compact the database
db.compact();

// Create a backup
db.backup('path/to/backup.db');

// Verify integrity
final isValid = db.verifyIntegrity();
```

## Platform Support

| Platform | Binary Name | Architecture |
|----------|------------|--------------|
| macOS    | liblightning_db.dylib | x64, arm64 |
| Linux    | liblightning_db.so | x64 |
| Windows  | lightning_db.dll | x64 |
| iOS      | liblightning_db.a | arm64, x64 sim |
| Android  | liblightning_db.so | arm64, arm, x86, x64 |

## Binary Installation

The package includes a binary installer that can download pre-built binaries:

```bash
dart run lightning_db_dart:install --target-os-type macos
```

Options:
- `--target-os-type`: Target OS (android, ios, macos, windows, linux)

## Error Handling

```dart
try {
  db.put('key', 'value');
} on LightningDbException catch (e) {
  print('Database error: ${e.message}');
  print('Error code: ${e.code}');
  
  // Handle specific errors
  if (e.code == 'DISK_FULL') {
    // Handle disk full error
  }
}
```

## Thread Safety

Lightning DB is thread-safe for concurrent reads and writes. The Dart bindings handle synchronization automatically:

- Multiple isolates can read simultaneously
- Writes are serialized internally
- Transactions provide isolation

## Memory Management

The FFI bindings handle memory management automatically:
- Strings are converted between Dart and C representations
- Binary data uses efficient zero-copy transfers when possible
- Native resources are freed when Dart objects are garbage collected

## Performance Tips

1. **Use binary format for large data**: More efficient than JSON
2. **Enable compression**: Reduces disk I/O for text data
3. **Batch operations**: Group multiple operations together
4. **Configure cache size**: Based on available memory
5. **Use appropriate page size**: 4KB for SSDs, 8KB for HDDs

## Building from Source

If you need to build the native library:

```bash
# Clone the repository
git clone https://github.com/yourusername/lightning_db.git
cd lightning_db

# Build the FFI library
cargo build --release -p lightning_db_ffi

# Copy to Dart package
cp target/release/liblightning_db_ffi.* packages/lightning_db_dart/
```

## API Reference

### LightningDb Class

#### Static Methods
- `LightningDb.create(String path, [DatabaseConfig? config])` - Create new database
- `LightningDb.open(String path, [DatabaseConfig? config])` - Open existing database

#### Instance Methods
- `void put(String key, String value)` - Store string value
- `void putBytes(String key, Uint8List value)` - Store binary data
- `void putJson(String key, Map<String, dynamic> value)` - Store JSON
- `String? get(String key)` - Get string value
- `Uint8List? getBytes(String key)` - Get binary data
- `Map<String, dynamic>? getJson(String key)` - Get JSON
- `void delete(String key)` - Delete key
- `bool contains(String key)` - Check if key exists
- `void close()` - Close database
- `Transaction beginTransaction()` - Start transaction
- `WriteBatch batch()` - Create write batch
- `DatabaseStatistics getStatistics()` - Get stats
- `void compact()` - Compact database
- `void backup(String path)` - Create backup
- `bool verifyIntegrity()` - Check integrity

#### Properties
- `String path` - Database file path
- `bool isOpen` - Whether database is open

### DatabaseConfig Class

- `int pageSize` - Page size in bytes (default: 4096)
- `int cacheSize` - Cache size in bytes (default: 50MB)
- `SyncMode syncMode` - Synchronization mode
- `JournalMode journalMode` - Journal mode
- `bool enableCompression` - Enable compression (default: false)
- `CompressionType compressionType` - Compression algorithm
- `bool enableMmap` - Memory-mapped I/O (default: false)
- `bool readOnly` - Open in read-only mode (default: false)

### Enums

#### SyncMode
- `SyncMode.off` - No syncing (fastest, least safe)
- `SyncMode.normal` - Sync at critical moments
- `SyncMode.full` - Sync after every write (slowest, safest)

#### JournalMode
- `JournalMode.delete` - Delete journal after commit
- `JournalMode.truncate` - Truncate journal
- `JournalMode.persist` - Keep journal file
- `JournalMode.wal` - Write-ahead logging

#### CompressionType
- `CompressionType.none` - No compression
- `CompressionType.lz4` - LZ4 (fast)
- `CompressionType.zstd` - Zstandard (balanced)
- `CompressionType.snappy` - Snappy (fast)

## License

MIT License - see LICENSE file for details.