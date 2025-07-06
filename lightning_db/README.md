# Lightning DB

A high-performance embedded database for Dart and Flutter applications. Zero configuration required - just add to your pubspec.yaml and start using!

## Features

- 🚀 Blazing fast performance (20M+ reads/sec, 1M+ writes/sec)
- 📱 Cross-platform support (iOS, Android, macOS, Windows, Linux)
- 🔐 ACID transactions with MVCC
- 💾 Efficient memory usage with configurable caching
- 🗜️ Built-in compression (Zstd, LZ4, Snappy)
- 🔍 Range queries and iterators
- 🛡️ Crash recovery and data integrity
- 📦 Zero configuration - just works!

## Installation

Add this to your `pubspec.yaml`:

```yaml
dependencies:
  lightning_db: ^0.1.0
```

Then run:

```bash
flutter pub get
```

That's it! The native libraries will be automatically built for your platform.

## Usage

### Basic Operations

```dart
import 'package:lightning_db/lightning_db.dart';
import 'dart:typed_data';

// Create or open a database
final db = await LightningDb.create('./my_database');

// Store data
final key = Uint8List.fromList('user:123'.codeUnits);
final value = Uint8List.fromList('{"name": "Alice", "age": 30}'.codeUnits);
await db.put(key, value);

// Retrieve data
final retrieved = await db.get(key);
if (retrieved != null) {
  final json = String.fromCharCodes(retrieved);
  print('User data: $json');
}

// Delete data
await db.delete(key);

// Close the database
await db.close();
```

### Transactions

```dart
// Begin a transaction
final tx = await db.beginTransaction();

try {
  // Multiple operations in a single transaction
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

### Range Queries

```dart
// Scan a range of keys
final iterator = await db.scan(
  start: Uint8List.fromList('user:'.codeUnits),
  end: Uint8List.fromList('user:~'.codeUnits),
);

// Iterate through results
await for (final (key, value) in iterator.toStream()) {
  final keyStr = String.fromCharCodes(key);
  final valueStr = String.fromCharCodes(value);
  print('$keyStr: $valueStr');
}
```

### Custom Configuration

```dart
final db = await LightningDb.createWithConfig(
  path: './my_database',
  cacheSize: 128 * 1024 * 1024, // 128MB cache
  compressionType: CompressionType.zstd,
  walSyncMode: WalSyncMode.periodic,
);
```

## Example App

Check out the [example](example/) directory for a complete Flutter app demonstrating Lightning DB usage.

## Performance

Lightning DB is designed for extreme performance:

- **Read Performance**: 20.4M ops/sec (0.049 μs latency)
- **Write Performance**: 1.14M ops/sec (0.88 μs latency)
- **Concurrent Operations**: 1.4M ops/sec with 8 threads
- **Large Dataset**: 237 MB/s write throughput

## Platform Support

| Platform | Status | Architecture |
|----------|--------|--------------|
| Android  | ✅ | arm64-v8a, armeabi-v7a, x86_64, x86 |
| iOS      | ✅ | arm64, x86_64 (simulator) |
| macOS    | ✅ | arm64, x86_64 |
| Linux    | ✅ | x86_64, aarch64 |
| Windows  | ✅ | x86_64 |

## How It Works

Lightning DB uses a hybrid storage architecture combining:
- B+Tree for efficient indexing
- LSM tree for write optimization
- MVCC for concurrent transactions
- Adaptive compression for storage efficiency

The plugin automatically builds the Rust core using [cargokit](https://github.com/irondash/cargokit) during the Flutter build process. No manual setup required!

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

