# Lightning DB Dart SDK

High-performance embedded database for Dart and Flutter applications.

## Features

- **High Performance**: 20M+ read ops/sec, 1M+ write ops/sec
- **ACID Transactions**: Full transaction support with commit/rollback
- **Range Scans**: Efficient key range iteration
- **Consistency Levels**: Choose between eventual and strong consistency
- **Compression**: Support for Zstd, LZ4, and Snappy compression
- **Cross-Platform**: Works on iOS, Android, macOS, Linux, and Windows

## Installation

Add to your `pubspec.yaml`:

```yaml
dependencies:
  lightning_db_dart:
    path: ../path/to/lightning_db_dart
```

## Usage

### Basic Operations

```dart
import 'dart:typed_data';
import 'package:lightning_db_dart/lightning_db_dart.dart';

// Create or open a database
final db = await LightningDb.create('./my_database');

// Put a key-value pair
final key = Uint8List.fromList('hello'.codeUnits);
final value = Uint8List.fromList('world'.codeUnits);
await db.put(key, value);

// Get a value
final retrieved = await db.get(key);
if (retrieved != null) {
  print(String.fromCharCodes(retrieved)); // "world"
}

// Delete a key
final deleted = await db.delete(key);

// Close the database
await db.close();
```

### Transactions

```dart
// Begin a transaction
final tx = await db.beginTransaction();

try {
  // Perform multiple operations atomically
  await tx.put(key1, value1);
  await tx.put(key2, value2);
  await tx.delete(key3);
  
  // Commit the transaction
  await tx.commit();
} catch (e) {
  // Rollback on error
  await tx.rollback();
}
```

### Range Scans

```dart
// Scan all keys
final iter = await db.scan();

// Scan a specific range
final iter = await db.scan(
  start: Uint8List.fromList('user:'.codeUnits),
  end: Uint8List.fromList('user:~'.codeUnits),
);

// Iterate over results
await for (final (key, value) in iter.toStream()) {
  print('Key: ${String.fromCharCodes(key)}');
  print('Value: ${String.fromCharCodes(value)}');
}
```

### Advanced Configuration

```dart
// Create database with custom configuration
final db = await LightningDb.createWithConfig(
  path: './my_database',
  cacheSize: 128 * 1024 * 1024, // 128MB cache
  compressionType: CompressionType.lz4,
  walSyncMode: WalSyncMode.periodic,
);
```

### Consistency Levels

```dart
// Write with strong consistency
await db.putWithConsistency(key, value, ConsistencyLevel.strong);

// Read with eventual consistency
final value = await db.getWithConsistency(key, ConsistencyLevel.eventual);
```

## Platform Setup

### iOS

1. Add to your `ios/Podfile`:
   ```ruby
   pod 'lightning_db_dart', :path => '../path/to/lightning_db_dart'
   ```

2. Run `pod install`

### Android

1. Ensure your `android/app/build.gradle` has:
   ```gradle
   android {
     ndkVersion "21.0.6113669"
   }
   ```

### macOS

1. Add to your `macos/Podfile`:
   ```ruby
   pod 'lightning_db_dart', :path => '../path/to/lightning_db_dart'
   ```

2. Run `pod install`

### Linux/Windows

The CMake configuration is automatically handled by Flutter.

## Performance Tips

1. **Batch Operations**: Use transactions for multiple operations
2. **Key Design**: Use sortable keys for efficient range scans
3. **Compression**: Enable compression for large values
4. **Consistency**: Use eventual consistency for better performance when possible

## Example

See the [example](example/main.dart) directory for a complete example.

## API Reference

### LightningDb

- `create(String path)` - Create a new database
- `open(String path)` - Open an existing database
- `createWithConfig(...)` - Create with custom configuration
- `put(Uint8List key, Uint8List value)` - Store a key-value pair
- `get(Uint8List key)` - Retrieve a value
- `delete(Uint8List key)` - Delete a key
- `beginTransaction()` - Start a new transaction
- `scan({start, end})` - Create an iterator for range scans
- `sync()` - Force sync to disk
- `checkpoint()` - Create a checkpoint
- `close()` - Close the database

### Transaction

- `put(Uint8List key, Uint8List value)` - Put within transaction
- `get(Uint8List key)` - Get within transaction
- `delete(Uint8List key)` - Delete within transaction
- `commit()` - Commit the transaction
- `rollback()` - Rollback the transaction

### DbIterator

- `next()` - Get next key-value pair
- `toStream()` - Convert to a Dart Stream
- `close()` - Close the iterator

## License

MIT OR Apache-2.0