# Lightning DB Flutter Plugin

A high-performance embedded database for Flutter applications with full support for Freezed immutable data models.

## Features

- 🚀 High performance (20M+ reads/sec, 1M+ writes/sec)
- 🔄 Full ACID compliance with MVCC transactions
- 📦 Zero-configuration setup
- 🎯 Type-safe collections with Freezed support
- 🔍 Reactive queries with RxDart integration
- 📱 Cross-platform support (iOS, Android, macOS, Windows, Linux)
- 🗜️ Built-in compression (Zstd/LZ4/Snappy)
- 💾 Memory-efficient with configurable cache

## Installation

Add to your `pubspec.yaml`:

```yaml
dependencies:
  lightning_db: ^0.0.1
```

## Usage

### Basic Setup

```dart
import 'package:lightning_db/lightning_db.dart';

// Initialize database
final db = await LightningDb.create('myapp.db');

// Create a collection
final users = FreezedCollection<User>(
  db,
  'users',
  FreezedAdapter<User>(
    fromJson: User.fromJson,
    toJson: (user) => user.toJson(),
  ),
);

// Add data
await users.add(User(id: '1', name: 'John Doe'));

// Query data
final user = await users.get('1');
final allUsers = await users.getAll();

// Watch for changes
users.watch().listen((users) {
  print('Users updated: ${users.length}');
});
```

### With Freezed Models

```dart
@freezed
sealed class User with _$User {
  const factory User({
    required String id,
    required String name,
    required String email,
  }) = _User;

  factory User.fromJson(Map<String, dynamic> json) => _$UserFromJson(json);
}
```

### Transactions

```dart
await db.transaction((tx) async {
  await tx.put('users', 'user1', user1.toJson());
  await tx.put('users', 'user2', user2.toJson());
  // All operations succeed or fail together
});
```

### Configuration

```dart
final db = await LightningDb.create(
  'myapp.db',
  DatabaseConfig(
    pageSize: 4096,
    cacheSize: 50 * 1024 * 1024, // 50MB
    syncMode: SyncMode.normal,
    compression: CompressionType.zstd,
  ),
);
```

## Platform Support

| Platform | Status |
|----------|--------|
| Android  | ✅     |
| iOS      | ✅     |
| macOS    | ✅     |
| Windows  | ✅     |
| Linux    | ✅     |
| Web      | ❌     |

## Example

See the [example](example) directory for a complete sample application.

## License

Apache License 2.0