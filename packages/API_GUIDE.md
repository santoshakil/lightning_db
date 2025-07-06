# Lightning DB Flutter SDK - API Guide

Complete API reference and usage guide for Lightning DB Flutter packages.

## Table of Contents

1. [Package Overview](#package-overview)
2. [Installation](#installation)
3. [Core API Reference](#core-api-reference)
4. [Freezed Integration API](#freezed-integration-api)
5. [Flutter Plugin API](#flutter-plugin-api)
6. [Examples](#examples)
7. [Best Practices](#best-practices)
8. [Migration Guide](#migration-guide)

## Package Overview

Lightning DB Flutter SDK consists of three packages:

### 1. `lightning_db_dart`
Low-level FFI bindings to the native Lightning DB library.
- Direct database operations
- Manual memory management
- Platform-specific binary loading

### 2. `lightning_db_freezed`
High-level type-safe collections with Freezed support.
- Automatic serialization/deserialization
- Type-safe queries
- Reactive streams

### 3. `lightning_db`
Flutter plugin combining both packages with platform integration.
- Zero-configuration setup
- Platform-specific optimizations
- Ready-to-use Flutter widgets

## Installation

### Basic Installation

```yaml
dependencies:
  lightning_db: ^0.1.0
```

### With Freezed Support

```yaml
dependencies:
  lightning_db: ^0.1.0
  lightning_db_freezed: ^0.1.0
  freezed_annotation: ^2.4.0

dev_dependencies:
  build_runner: ^2.4.0
  freezed: ^2.4.0
  json_serializable: ^6.7.0
```

### Manual Binary Installation

```bash
# Download binaries for specific platform
dart run lightning_db_dart:install --target-os-type macos
```

## Core API Reference

### LightningDb Class

Main database interface for low-level operations.

#### Creation and Opening

```dart
// Create new database
final db = LightningDb.create('path/to/database.db');

// Open existing database
final db = LightningDb.open('path/to/database.db');

// With configuration
final config = DatabaseConfig(
  pageSize: 4096,
  cacheSize: 100 * 1024 * 1024, // 100MB
  syncMode: SyncMode.normal,
  journalMode: JournalMode.wal,
  enableCompression: true,
  compressionType: CompressionType.lz4,
);
final db = LightningDb.create('database.db', config: config);
```

#### Basic Operations

```dart
// String operations
db.put('key', 'value');
String? value = db.get('key');
db.delete('key');
bool exists = db.contains('key');

// Binary operations
db.putBytes('binary_key', Uint8List.fromList([1, 2, 3]));
Uint8List? bytes = db.getBytes('binary_key');

// JSON operations
db.putJson('json_key', {'name': 'John', 'age': 30});
Map<String, dynamic>? json = db.getJson('json_key');
```

#### Iteration

```dart
// Iterate all keys
for (final key in db.keys()) {
  print(key);
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
}, prefix: 'user:');
```

#### Transactions

```dart
// Manual transaction
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
});
```

#### Batch Operations

```dart
final batch = db.batch();
batch.put('key1', 'value1');
batch.put('key2', 'value2');
batch.delete('key3');
batch.commit();
```

#### Maintenance

```dart
// Get statistics
final stats = db.getStatistics();
print('Keys: ${stats.keyCount}');
print('Disk size: ${stats.diskSize}');
print('Cache hit rate: ${stats.cacheHitRate}%');

// Compact database
db.compact();

// Create backup
db.backup('path/to/backup.db');

// Verify integrity
bool isValid = db.verifyIntegrity();

// Close database
db.close();
```

### DatabaseConfig

```dart
class DatabaseConfig {
  final int pageSize;           // Page size in bytes (default: 4096)
  final int cacheSize;          // Cache size in bytes (default: 50MB)
  final SyncMode syncMode;      // Sync mode (default: normal)
  final JournalMode journalMode; // Journal mode (default: delete)
  final bool enableCompression;  // Enable compression (default: false)
  final CompressionType compressionType; // Compression type
  final bool enableMmap;        // Memory-mapped I/O (default: false)
  final bool readOnly;          // Read-only mode (default: false)
  final bool directIo;          // Direct I/O (Linux only)
}
```

### Enumerations

```dart
enum SyncMode {
  off,    // No syncing (fastest, least safe)
  normal, // Sync at critical moments
  full,   // Sync after every write (safest)
}

enum JournalMode {
  delete,   // Delete journal after commit
  truncate, // Truncate journal to zero
  persist,  // Keep journal file
  wal,      // Write-ahead logging
}

enum CompressionType {
  none,    // No compression
  lz4,     // LZ4 (fastest)
  zstd,    // Zstandard (balanced)
  snappy,  // Snappy (fast)
}
```

## Freezed Integration API

### FreezedCollection<T>

Type-safe collection for Freezed models.

#### Creation

```dart
// Basic creation
final users = FreezedCollection<User>(
  db,
  'users',
  FreezedAdapter<User>(
    fromJson: User.fromJson,
    toJson: (user) => user.toJson(),
  ),
);

// With custom ID field
final products = FreezedCollection<Product>(
  db,
  'products',
  FreezedAdapter<Product>(
    fromJson: Product.fromJson,
    toJson: (product) => product.toJson(),
    getId: (product) => product.sku,
  ),
);

// Extension method (Flutter plugin)
final users = db.freezedCollection<User>('users');
```

#### CRUD Operations

```dart
// Create
await users.add(user);
await users.addAll([user1, user2, user3]);

// Read
User? user = await users.get('id');
List<User> allUsers = await users.getAll();
List<User> someUsers = await users.getMany(['id1', 'id2']);

// Update
await users.update(user);
await users.updateAll([user1, user2]);
await users.upsert(user); // Insert or update

// Delete
await users.delete('id');
await users.deleteAll(['id1', 'id2']);
await users.clear(); // Delete all
```

#### Queries

```dart
// Basic query
final results = await users.query()
  .where('age', isGreaterThan: 25)
  .findAll();

// Complex query
final results = await users.query()
  .where('age', isGreaterThan: 25)
  .where('age', isLessThan: 65)
  .where('metadata.active', isEqualTo: true)
  .where('email', contains: '@company.com')
  .where('tags', containsAny: ['premium', 'vip'])
  .orderBy('createdAt', descending: true)
  .limit(20)
  .offset(40)
  .findAll();

// Query operators
.where('field', isEqualTo: value)
.where('field', isNotEqualTo: value)
.where('field', isGreaterThan: value)
.where('field', isGreaterThanOrEqualTo: value)
.where('field', isLessThan: value)
.where('field', isLessThanOrEqualTo: value)
.where('field', isNull: true)
.where('field', isNull: false)
.where('field', contains: substring)
.where('field', startsWith: prefix)
.where('field', endsWith: suffix)
.where('field', matches: regexPattern)
.where('array', containsAny: ['value1', 'value2'])
.where('array', containsAll: ['value1', 'value2'])
```

#### Reactive Streams

```dart
// Document changes
final subscription = users.changes.listen((change) {
  switch (change.type) {
    case ChangeType.added:
      print('Added: ${change.document.id}');
      break;
    case ChangeType.modified:
      print('Modified: ${change.document.id}');
      break;
    case ChangeType.removed:
      print('Removed: ${change.document.id}');
      break;
  }
});

// Query snapshots
users.query()
  .where('age', isGreaterThan: 25)
  .snapshots()
  .listen((users) {
    print('Active users: ${users.length}');
  });

// Watch specific document
users.watch('user_id').listen((user) {
  if (user != null) {
    print('User updated: ${user.name}');
  } else {
    print('User deleted');
  }
});

// Watch all documents
users.watchAll().listen((users) {
  print('Total users: ${users.length}');
});
```

#### Indexes

```dart
// Create index
await users.createIndex('age_index', ['age']);
await users.createIndex('email_unique', ['email'], unique: true);
await users.createIndex('composite', ['age', 'metadata.active']);

// Drop index
await users.dropIndex('age_index');

// List indexes
List<IndexInfo> indexes = await users.listIndexes();
```

### FreezedAdapter<T>

```dart
class FreezedAdapter<T> {
  final T Function(Map<String, dynamic>) fromJson;
  final Map<String, dynamic> Function(T) toJson;
  final String Function(T)? getId;
  
  // Custom adapter example
  class TimestampAdapter extends FreezedAdapter<User> {
    @override
    Map<String, dynamic> toJson(User model) {
      final json = model.toJson();
      json['_lastModified'] = DateTime.now().toIso8601String();
      return json;
    }
    
    @override
    User fromJson(Map<String, dynamic> json) {
      json.remove('_lastModified');
      return User.fromJson(json);
    }
  }
}
```

### CollectionQuery<T>

```dart
class CollectionQuery<T> {
  // Conditions
  CollectionQuery<T> where(String field, {
    dynamic isEqualTo,
    dynamic isNotEqualTo,
    dynamic isGreaterThan,
    dynamic isGreaterThanOrEqualTo,
    dynamic isLessThan,
    dynamic isLessThanOrEqualTo,
    bool? isNull,
    dynamic contains,
    dynamic containsAny,
    dynamic containsAll,
    String? startsWith,
    String? endsWith,
    String? matches,
  });
  
  // Ordering
  CollectionQuery<T> orderBy(String field, {bool descending = false});
  
  // Pagination
  CollectionQuery<T> limit(int count);
  CollectionQuery<T> offset(int count);
  
  // Projection
  CollectionQuery<T> select(List<String> fields);
  
  // Execution
  Future<List<T>> findAll();
  Future<T?> findFirst();
  Future<int> count();
  Stream<List<T>> snapshots();
}
```

## Flutter Plugin API

### Extension Methods

```dart
extension LightningDbExtensions on LightningDb {
  // Create typed collection
  FreezedCollection<T> freezedCollection<T>(String name, {
    FreezedAdapter<T>? adapter,
    String Function(T)? getId,
  });
  
  // Run in transaction
  Future<R> transaction<R>(TransactionCallback<R> callback);
  
  // Async operations
  Future<void> putAsync(String key, String value);
  Future<String?> getAsync(String key);
  Future<void> deleteAsync(String key);
}
```

### Platform-Specific Methods

```dart
// Get native library info
final info = db.getNativeLibraryInfo();
print('Platform: ${info.platform}');
print('Architecture: ${info.architecture}');
print('Features: ${info.features}');

// Platform-specific configuration
if (Platform.isIOS) {
  // iOS without zstd
  config.compressionType = CompressionType.lz4;
} else if (Platform.isAndroid) {
  // Android with larger cache
  config.cacheSize = 100 * 1024 * 1024;
}
```

## Examples

### Basic CRUD Example

```dart
import 'package:lightning_db/lightning_db.dart';

void main() async {
  // Open database
  final db = await LightningDb.open('myapp.db');
  
  // Basic operations
  await db.put('user:1', 'John Doe');
  await db.put('user:2', 'Jane Smith');
  
  // Read data
  final user1 = await db.get('user:1');
  print('User 1: $user1');
  
  // Iterate users
  for (final key in db.keys(prefix: 'user:')) {
    final value = await db.get(key);
    print('$key: $value');
  }
  
  // Close database
  await db.close();
}
```

### Freezed Model Example

```dart
import 'package:lightning_db/lightning_db.dart';
import 'package:freezed_annotation/freezed_annotation.dart';

part 'main.freezed.dart';
part 'main.g.dart';

@freezed
class Task with _$Task {
  const factory Task({
    required String id,
    required String title,
    required bool completed,
    DateTime? dueDate,
  }) = _Task;
  
  factory Task.fromJson(Map<String, dynamic> json) => _$TaskFromJson(json);
}

void main() async {
  final db = await LightningDb.open('tasks.db');
  final tasks = db.freezedCollection<Task>('tasks');
  
  // Add task
  await tasks.add(Task(
    id: '1',
    title: 'Complete Lightning DB integration',
    completed: false,
    dueDate: DateTime.now().add(Duration(days: 7)),
  ));
  
  // Query incomplete tasks
  final incompleteTasks = await tasks.query()
    .where('completed', isEqualTo: false)
    .where('dueDate', isGreaterThan: DateTime.now())
    .orderBy('dueDate')
    .findAll();
  
  // Listen to changes
  tasks.changes.listen((change) {
    print('Task ${change.document.id} was ${change.type}');
  });
  
  await db.close();
}
```

### Transaction Example

```dart
class BankAccount {
  final String id;
  final String owner;
  final double balance;
  
  // ... Freezed implementation
}

Future<void> transferMoney(
  LightningDb db,
  String fromId,
  String toId,
  double amount,
) async {
  await db.transaction((tx) async {
    final accounts = tx.freezedCollection<BankAccount>('accounts');
    
    // Get accounts
    final from = await accounts.get(fromId);
    final to = await accounts.get(toId);
    
    if (from == null || to == null) {
      throw Exception('Account not found');
    }
    
    if (from.balance < amount) {
      throw Exception('Insufficient funds');
    }
    
    // Update balances
    await accounts.update(from.copyWith(balance: from.balance - amount));
    await accounts.update(to.copyWith(balance: to.balance + amount));
    
    // Log transaction
    await tx.putJson('transactions:${DateTime.now().millisecondsSinceEpoch}', {
      'from': fromId,
      'to': toId,
      'amount': amount,
      'timestamp': DateTime.now().toIso8601String(),
    });
  });
}
```

## Best Practices

### 1. Use Appropriate Data Types

```dart
// ❌ Bad: Storing everything as JSON
await db.putJson('user:1', user.toJson());

// ✅ Good: Use typed collections
final users = db.freezedCollection<User>('users');
await users.add(user);
```

### 2. Batch Operations

```dart
// ❌ Bad: Individual operations
for (final user in userList) {
  await users.add(user);
}

// ✅ Good: Batch operations
await users.addAll(userList);
```

### 3. Proper Error Handling

```dart
// ❌ Bad: No error handling
final user = await users.get('id');

// ✅ Good: Handle errors
try {
  final user = await users.get('id');
  if (user == null) {
    print('User not found');
  }
} on LightningDbException catch (e) {
  print('Database error: ${e.message}');
}
```

### 4. Resource Management

```dart
// ❌ Bad: Not closing database
final db = await LightningDb.open('app.db');
// ... use database

// ✅ Good: Always close when done
final db = await LightningDb.open('app.db');
try {
  // ... use database
} finally {
  await db.close();
}
```

### 5. Index Usage

```dart
// ❌ Bad: No indexes for frequently queried fields
final results = await users.query()
  .where('email', isEqualTo: 'john@example.com')
  .findAll();

// ✅ Good: Create indexes for query performance
await users.createIndex('email_index', ['email'], unique: true);
final results = await users.query()
  .where('email', isEqualTo: 'john@example.com')
  .findAll();
```

## Migration Guide

### From SQLite

```dart
// SQLite
final db = await openDatabase('app.db');
await db.execute('''
  CREATE TABLE users (
    id TEXT PRIMARY KEY,
    name TEXT,
    age INTEGER
  )
''');
await db.insert('users', {'id': '1', 'name': 'John', 'age': 30});
final users = await db.query('users', where: 'age > ?', whereArgs: [25]);

// Lightning DB
final db = await LightningDb.open('app.db');
final users = db.freezedCollection<User>('users');
await users.add(User(id: '1', name: 'John', age: 30));
final results = await users.query().where('age', isGreaterThan: 25).findAll();
```

### From Hive

```dart
// Hive
await Hive.initFlutter();
await Hive.openBox<User>('users');
final box = Hive.box<User>('users');
await box.put('1', user);
final user = box.get('1');

// Lightning DB
final db = await LightningDb.open('app.db');
final users = db.freezedCollection<User>('users');
await users.add(user);
final retrieved = await users.get('1');
```

### From Realm

```dart
// Realm
final config = Configuration.local([User.schema]);
final realm = Realm(config);
realm.write(() {
  realm.add(User()..id = '1'..name = 'John');
});
final users = realm.all<User>();

// Lightning DB
final db = await LightningDb.open('app.db');
final users = db.freezedCollection<User>('users');
await users.add(User(id: '1', name: 'John'));
final allUsers = await users.getAll();
```

## Performance Tuning

### Configuration Tips

```dart
// Mobile configuration
final mobileConfig = DatabaseConfig(
  pageSize: 4096,              // 4KB pages
  cacheSize: 50 * 1024 * 1024, // 50MB cache
  syncMode: SyncMode.normal,
  compressionType: CompressionType.lz4,
);

// Desktop configuration
final desktopConfig = DatabaseConfig(
  pageSize: 8192,               // 8KB pages
  cacheSize: 500 * 1024 * 1024, // 500MB cache
  syncMode: SyncMode.normal,
  journalMode: JournalMode.wal,
  enableMmap: true,
);

// High-write configuration
final writeOptimized = DatabaseConfig(
  syncMode: SyncMode.off,       // Faster writes, less safe
  journalMode: JournalMode.wal,
  cacheSize: 200 * 1024 * 1024,
);
```

### Query Optimization

```dart
// Create composite indexes for complex queries
await users.createIndex('age_active', ['age', 'metadata.active']);

// Use projections to reduce data transfer
final names = await users.query()
  .select(['id', 'name'])
  .findAll();

// Limit results for pagination
final page1 = await users.query()
  .orderBy('createdAt', descending: true)
  .limit(20)
  .findAll();
```

## Troubleshooting

### Common Issues

1. **Database Locked**
   ```dart
   // Ensure single instance
   class DatabaseService {
     static LightningDb? _instance;
     
     static Future<LightningDb> getInstance() async {
       _instance ??= await LightningDb.open('app.db');
       return _instance!;
     }
   }
   ```

2. **Performance Issues**
   - Check cache size configuration
   - Create indexes for queried fields
   - Use batch operations
   - Enable compression for large text data

3. **Platform-Specific Issues**
   - iOS: Check minimum deployment target (11+)
   - Android: Verify minimum SDK (21+)
   - Desktop: Ensure binary permissions

## Additional Resources

- [GitHub Repository](https://github.com/yourusername/lightning_db)
- [API Documentation](https://pub.dev/documentation/lightning_db/latest/)
- [Example App](https://github.com/yourusername/lightning_db/tree/master/packages/lightning_db/example)
- [Performance Benchmarks](https://github.com/yourusername/lightning_db/wiki/Benchmarks)

## License

MIT License - see LICENSE file for details.