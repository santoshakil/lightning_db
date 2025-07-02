# Lightning DB for Dart

High-performance embedded database for Dart and Flutter applications.

## Features

- **Blazing Fast**: 1.4M writes/sec, 14M reads/sec
- **ACID Transactions**: Full transaction support with isolation
- **Zero Dependencies**: Pure Dart API with FFI
- **Cross Platform**: Works on iOS, Android, macOS, Linux, and Windows
- **Memory Efficient**: Configurable memory usage from 10MB+
- **Type Safe**: Strong typing with null safety

## Installation

Add to your `pubspec.yaml`:

```yaml
dependencies:
  lightning_db: ^0.1.0
```

## Usage

### Basic Operations

```dart
import 'dart:convert';
import 'package:lightning_db/lightning_db.dart';

// Create or open a database
final db = LightningDB.create('./my_database');

// Store data
db.put(utf8.encode('key'), utf8.encode('value'));

// Retrieve data
final value = db.get(utf8.encode('key'));
if (value != null) {
  print('Value: ${utf8.decode(value)}');
}

// Delete data
db.delete(utf8.encode('key'));

// Always close when done
db.close();
```

### Configuration

```dart
final db = LightningDB.create(
  './my_database',
  config: LightningDBConfig(
    pageSize: 4096,              // Page size in bytes
    cacheSize: 64 * 1024 * 1024, // 64MB cache
  ),
);
```

### Transactions

```dart
// Begin a transaction
final txId = db.beginTransaction();

try {
  // Perform multiple operations atomically
  db.putInTransaction(txId, key1, value1);
  db.putInTransaction(txId, key2, value2);
  db.putInTransaction(txId, key3, value3);
  
  // Commit all changes
  db.commitTransaction(txId);
} catch (e) {
  // Rollback on error
  db.abortTransaction(txId);
  rethrow;
}
```

### Binary Data

Lightning DB works with raw bytes, making it perfect for storing any type of data:

```dart
// Store binary data
final binaryKey = Uint8List.fromList([0, 1, 2, 3]);
final binaryValue = Uint8List.fromList([255, 254, 253, 252]);
db.put(binaryKey, binaryValue);

// Store JSON
final json = jsonEncode({'name': 'John', 'age': 30});
db.put(utf8.encode('user:1'), utf8.encode(json));

// Store msgpack, protobuf, or any binary format
db.put(keyBytes, valueBytes);
```

### Error Handling

```dart
try {
  db.put(key, value);
} on LightningDBException catch (e) {
  print('Database error: $e');
} catch (e) {
  print('Unexpected error: $e');
}
```

## Platform Setup

### iOS

No additional setup required. The native library is included in the package.

### Android

Add to your `android/app/build.gradle`:

```gradle
android {
    // ...
    sourceSets {
        main {
            jniLibs.srcDirs += ['../../lightning_db/android']
        }
    }
}
```

### Desktop (Windows, macOS, Linux)

The native library must be available in your system's library path:

- **macOS**: `liblightning_db_ffi.dylib`
- **Linux**: `liblightning_db_ffi.so`
- **Windows**: `lightning_db_ffi.dll`

For development, you can specify the library path:

```dart
final db = LightningDB.create(
  './my_database',
  libraryPath: '/path/to/liblightning_db_ffi.dylib',
);
```

## Performance

Lightning DB is designed for extreme performance:

- **Writes**: Up to 1.4M operations/second
- **Reads**: Up to 14M operations/second (from cache)
- **Binary size**: <5MB
- **Memory usage**: Configurable from 10MB

## API Reference

### LightningDB

The main database class.

#### Factory Constructors

- `LightningDB.create(String path, {LightningDBConfig? config, String? libraryPath})` - Create a new database
- `LightningDB.open(String path, {LightningDBConfig? config, String? libraryPath})` - Open an existing database

#### Methods

- `void put(Uint8List key, Uint8List value)` - Store a key-value pair
- `Uint8List? get(Uint8List key)` - Retrieve a value by key
- `void delete(Uint8List key)` - Delete a key-value pair
- `int beginTransaction()` - Start a new transaction
- `void putInTransaction(int txId, Uint8List key, Uint8List value)` - Put within a transaction
- `void commitTransaction(int txId)` - Commit a transaction
- `void abortTransaction(int txId)` - Abort a transaction
- `void checkpoint()` - Force a checkpoint (flush to disk)
- `void close()` - Close the database

### LightningDBConfig

Configuration options for the database.

- `int? pageSize` - Page size in bytes (default: 4096)
- `int? cacheSize` - Cache size in bytes (default: 64MB)

### Exceptions

- `LightningDBException` - Base exception for all database errors
- `NullPointerException` - Null pointer encountered
- `InvalidUtf8Exception` - Invalid UTF-8 string
- `IoException` - I/O error occurred
- `CorruptedDataException` - Data corruption detected
- `InvalidArgumentException` - Invalid argument provided
- `OutOfMemoryException` - Out of memory
- `DatabaseLockedException` - Database is locked
- `TransactionConflictException` - Transaction conflict

## Thread Safety

Lightning DB is thread-safe. You can use the same database instance from multiple isolates.

## License

MIT OR Apache-2.0