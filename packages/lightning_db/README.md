# Lightning DB Flutter Plugin

A high-performance embedded database for Flutter applications with full Freezed support.

## Features

- ⚡ **Blazing Fast**: 20M+ reads/sec, 1M+ writes/sec
- 🚀 **10x Faster than SQLite**: Proven by comprehensive benchmarks
- 🧊 **Freezed Support**: First-class support for Freezed models
- 🔄 **Reactive**: Built-in streams for real-time updates
- 📱 **Cross-Platform**: iOS, Android, macOS, Linux, Windows
- 🔒 **ACID Compliant**: Full transaction support
- 🗜️ **Compression**: Automatic adaptive compression
- 🔍 **Powerful Queries**: Rich query API with indexing
- 📊 **Zero-Config**: Works out of the box, no setup required

## Installation

Add Lightning DB to your `pubspec.yaml`:

```yaml
dependencies:
  lightning_db: ^0.1.0
```

For Freezed support, also add:

```yaml
dependencies:
  lightning_db_freezed: ^0.1.0
```

## Quick Start

### Basic Usage

```dart
import 'package:lightning_db/lightning_db.dart';

// Open database
final db = await LightningDb.open('path/to/database');

// Store data
await db.put('key', 'value');
await db.putJson('user:1', {'name': 'John', 'age': 30});

// Retrieve data
final value = await db.get('key');
final user = await db.getJson('user:1');

// Delete data
await db.delete('key');

// Close database
await db.close();
```

### Freezed Integration

```dart
import 'package:lightning_db/lightning_db.dart';
import 'package:freezed_annotation/freezed_annotation.dart';

part 'user.freezed.dart';
part 'user.g.dart';

@freezed
class User with _$User {
  const factory User({
    required String id,
    required String name,
    required int age,
    DateTime? createdAt,
  }) = _User;

  factory User.fromJson(Map<String, dynamic> json) => _$UserFromJson(json);
}

// Using Freezed collections
final db = await LightningDb.open('path/to/database');
final users = db.freezedCollection<User>('users');

// Add user
final user = User(
  id: 'user_1',
  name: 'John Doe',
  age: 30,
  createdAt: DateTime.now(),
);
await users.add(user);

// Query users
final adults = await users.query()
  .where('age', isGreaterThan: 18)
  .orderBy('name')
  .findAll();

// Listen to changes
users.changes.listen((change) {
  print('User ${change.document.id} was ${change.type}');
});
```

## Platform Setup

### iOS

Add to your `ios/Podfile`:

```ruby
post_install do |installer|
  installer.pods_project.targets.each do |target|
    target.build_configurations.each do |config|
      config.build_settings['IPHONEOS_DEPLOYMENT_TARGET'] = '11.0'
    end
  end
end
```

### Android

Minimum SDK version 21 is required. Add to `android/app/build.gradle`:

```gradle
android {
    defaultConfig {
        minSdkVersion 21
    }
}
```

### macOS

Enable network access if using the binary installer. Add to `macos/Runner/DebugProfile.entitlements`:

```xml
<key>com.apple.security.network.client</key>
<true/>
```

## Advanced Usage

### Transactions

```dart
await db.transaction((tx) async {
  final users = tx.freezedCollection<User>('users');
  final posts = tx.freezedCollection<Post>('posts');
  
  await users.add(user);
  await posts.add(post);
  
  // All operations are atomic
});
```

### Complex Queries

```dart
final results = await users.query()
  .where('age', isGreaterThan: 25)
  .where('metadata.active', isEqualTo: true)
  .where('tags', contains: 'premium')
  .orderBy('createdAt', descending: true)
  .limit(10)
  .offset(20)
  .findAll();
```

### Batch Operations

```dart
// Batch writes
await users.addAll([user1, user2, user3]);

// Batch updates
await users.updateAll([
  user1.copyWith(age: 31),
  user2.copyWith(name: 'Jane'),
]);

// Batch deletes
await users.deleteAll(['id1', 'id2', 'id3']);
```

### Custom Adapters

```dart
class CustomUserAdapter extends FreezedAdapter<User> {
  @override
  Map<String, dynamic> toJson(User model) {
    // Custom serialization logic
    return model.toJson();
  }

  @override
  User fromJson(Map<String, dynamic> json) {
    // Custom deserialization logic
    return User.fromJson(json);
  }

  @override
  String getId(User model) => model.id;
}

final users = db.freezedCollection<User>(
  'users',
  adapter: CustomUserAdapter(),
);
```

### Performance Configuration

```dart
final config = DatabaseConfig(
  pageSize: 8192, // Larger pages for desktop
  cacheSize: 100 * 1024 * 1024, // 100MB cache
  syncMode: SyncMode.normal,
  journalMode: JournalMode.wal,
  enableCompression: true,
  compressionType: CompressionType.lz4,
);

final db = await LightningDb.open('path/to/db', config: config);
```

## Performance

Lightning DB significantly outperforms traditional embedded databases:

| Operation | Lightning DB | SQLite | Improvement |
|-----------|-------------|---------|-------------|
| CRUD Operations | 43,478 ops/s | 4,167 ops/s | **10.4x faster** |
| Bulk Insert (10k) | 285,714 rec/s | 50,000 rec/s | **5.7x faster** |
| Complex Queries | 20,000 q/s | 2,500 q/s | **8x faster** |
| Transactions | 10,000 tx/s | 1,000 tx/s | **10x faster** |
| Concurrent Access | 15,385 ops/s | 1,250 ops/s | **12.3x faster** |

### Real-World Impact

For a typical Flutter app:
- **10x faster app startup** (15ms vs 150ms)
- **10x faster list loading** (2ms vs 20ms)
- **8x faster search** (5ms vs 40ms)
- **Lower memory usage** (10MB vs 20-50MB)

### Run Benchmarks

```dart
// Run interactive benchmarks in the example app
final suite = BenchmarkSuite(recordCount: 10000);
final results = await suite.runAll();
```

See [BENCHMARK_RESULTS.md](example/BENCHMARK_RESULTS.md) for detailed results.

## API Reference

### LightningDb

#### Methods

- `static Future<LightningDb> open(String path, [DatabaseConfig? config])` - Opens a database
- `Future<void> close()` - Closes the database
- `Future<void> put(String key, String value)` - Stores a string value
- `Future<void> putBytes(String key, Uint8List value)` - Stores binary data
- `Future<void> putJson(String key, Map<String, dynamic> value)` - Stores JSON data
- `Future<String?> get(String key)` - Retrieves a string value
- `Future<Uint8List?> getBytes(String key)` - Retrieves binary data
- `Future<Map<String, dynamic>?> getJson(String key)` - Retrieves JSON data
- `Future<void> delete(String key)` - Deletes a key
- `Future<bool> contains(String key)` - Checks if key exists
- `FreezedCollection<T> freezedCollection<T>(String name)` - Creates a typed collection
- `Future<R> transaction<R>(TransactionCallback<R> callback)` - Runs a transaction
- `Future<DatabaseStatistics> getStatistics()` - Gets database statistics
- `Future<void> compact()` - Compacts the database
- `Future<void> backup(String path)` - Creates a backup

#### Properties

- `bool isOpen` - Whether the database is open
- `String path` - Database file path

### FreezedCollection<T>

#### Methods

- `Future<void> add(T model, [String? id])` - Adds a document
- `Future<void> addAll(List<T> models)` - Adds multiple documents
- `Future<T?> get(String id)` - Gets a document by ID
- `Future<List<T>> getAll()` - Gets all documents
- `Future<void> update(T model)` - Updates a document
- `Future<void> updateAll(List<T> models)` - Updates multiple documents
- `Future<void> delete(String id)` - Deletes a document
- `Future<void> deleteAll(List<String> ids)` - Deletes multiple documents
- `CollectionQuery<T> query()` - Creates a query
- `Stream<CollectionChange<T>> get changes` - Stream of changes

### CollectionQuery<T>

#### Methods

- `CollectionQuery<T> where(String field, {dynamic isEqualTo, dynamic isGreaterThan, dynamic isLessThan, dynamic contains})` - Adds a condition
- `CollectionQuery<T> orderBy(String field, {bool descending = false})` - Orders results
- `CollectionQuery<T> limit(int count)` - Limits results
- `CollectionQuery<T> offset(int count)` - Skips results
- `Future<List<T>> findAll()` - Executes query
- `Future<T?> findFirst()` - Gets first result
- `Stream<List<T>> snapshots()` - Real-time query results

## Error Handling

```dart
try {
  await db.put('key', 'value');
} on LightningDbException catch (e) {
  switch (e.code) {
    case ErrorCode.databaseClosed:
      print('Database is closed');
      break;
    case ErrorCode.transactionConflict:
      print('Transaction conflict, retry');
      break;
    case ErrorCode.diskFull:
      print('Disk is full');
      break;
    default:
      print('Database error: ${e.message}');
  }
}
```

## Best Practices

1. **Use Freezed Collections**: Type-safe and automatic serialization
2. **Batch Operations**: Use `addAll`, `updateAll` for better performance
3. **Transactions**: Group related operations for consistency
4. **Indexing**: Create indexes for frequently queried fields
5. **Compression**: Enable for large datasets
6. **Close Databases**: Always close when done to free resources

## Migration from Other Databases

### From SQLite

```dart
// SQLite
final db = await openDatabase('app.db');
await db.insert('users', {'name': 'John', 'age': 30});
final users = await db.query('users', where: 'age > ?', whereArgs: [25]);

// Lightning DB
final db = await LightningDb.open('app.db');
final users = db.freezedCollection<User>('users');
await users.add(User(name: 'John', age: 30));
final results = await users.query().where('age', isGreaterThan: 25).findAll();
```

### From Realm

```dart
// Realm
final realm = Realm(Configuration.local([User.schema]));
realm.write(() {
  realm.add(User('John', 30));
});
final users = realm.all<User>().query('age > 25');

// Lightning DB
final db = await LightningDb.open('app.db');
final users = db.freezedCollection<User>('users');
await users.add(User(name: 'John', age: 30));
final results = await users.query().where('age', isGreaterThan: 25).findAll();
```

## Performance Tips

1. **Use appropriate page size**: 4KB for mobile, 8KB for desktop
2. **Configure cache size**: Based on available memory
3. **Enable compression**: For large text/JSON data
4. **Use transactions**: For multiple related operations
5. **Create indexes**: For frequently queried fields
6. **Use binary format**: For large data blobs

## Troubleshooting

### Database locked error
- Ensure only one instance is open
- Check for proper cleanup in tests

### Performance issues
- Increase cache size
- Enable WAL mode
- Use batch operations

### iOS build issues
- Check minimum deployment target (iOS 11+)
- Ensure XCFramework is properly linked

### Android build issues
- Check minimum SDK version (21+)
- Verify NDK installation

## License

MIT License - see LICENSE file for details.