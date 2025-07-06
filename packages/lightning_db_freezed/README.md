# Lightning DB Freezed Integration

Type-safe collections for Lightning DB with full Freezed support.

## Overview

This package provides a high-level, type-safe API for Lightning DB with seamless Freezed integration. It allows you to work with your Freezed models directly without manual serialization.

## Installation

```yaml
dependencies:
  lightning_db_freezed: ^0.1.0
  lightning_db_dart: ^0.1.0
  freezed_annotation: ^2.4.0

dev_dependencies:
  build_runner: ^2.4.0
  freezed: ^2.4.0
  json_serializable: ^6.7.0
```

## Quick Start

### Define Your Models

```dart
import 'package:freezed_annotation/freezed_annotation.dart';

part 'user.freezed.dart';
part 'user.g.dart';

@freezed
class User with _$User {
  const factory User({
    required String id,
    required String name,
    required String email,
    int? age,
    DateTime? createdAt,
    Map<String, dynamic>? metadata,
  }) = _User;

  factory User.fromJson(Map<String, dynamic> json) => _$UserFromJson(json);
}
```

### Use Collections

```dart
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'package:lightning_db_freezed/lightning_db_freezed.dart';

// Open database
final db = LightningDb.create('app.db');

// Create a typed collection
final users = FreezedCollection<User>(
  db,
  'users',
  FreezedAdapter<User>(
    fromJson: User.fromJson,
    toJson: (user) => user.toJson(),
  ),
);

// CRUD operations
final user = User(
  id: '1',
  name: 'John Doe',
  email: 'john@example.com',
  age: 30,
  createdAt: DateTime.now(),
);

await users.add(user);
final retrieved = await users.get('1');
await users.update(user.copyWith(age: 31));
await users.delete('1');
```

## Advanced Features

### Queries

```dart
// Simple queries
final adults = await users.query()
  .where('age', isGreaterThan: 18)
  .findAll();

// Complex queries
final results = await users.query()
  .where('age', isGreaterThan: 25)
  .where('metadata.active', isEqualTo: true)
  .where('email', contains: '@company.com')
  .orderBy('createdAt', descending: true)
  .limit(10)
  .offset(20)
  .findAll();

// Find first match
final firstAdmin = await users.query()
  .where('metadata.role', isEqualTo: 'admin')
  .findFirst();

// Count results
final count = await users.query()
  .where('age', isLessThan: 30)
  .count();
```

### Reactive Streams

```dart
// Listen to all changes
users.changes.listen((change) {
  switch (change.type) {
    case ChangeType.added:
      print('User added: ${change.document.name}');
      break;
    case ChangeType.modified:
      print('User modified: ${change.document.name}');
      break;
    case ChangeType.removed:
      print('User removed: ${change.document.name}');
      break;
  }
});

// Watch query results
final youngUsersStream = users.query()
  .where('age', isLessThan: 25)
  .snapshots();

youngUsersStream.listen((users) {
  print('Young users count: ${users.length}');
});

// Watch specific document
final userStream = users.watch('user_1');
userStream.listen((user) {
  if (user != null) {
    print('User updated: ${user.name}');
  } else {
    print('User deleted');
  }
});
```

### Batch Operations

```dart
// Batch add
final newUsers = List.generate(100, (i) => User(
  id: 'user_$i',
  name: 'User $i',
  email: 'user$i@example.com',
  age: 20 + (i % 50),
));
await users.addAll(newUsers);

// Batch update
final updates = newUsers
  .take(50)
  .map((u) => u.copyWith(metadata: {'updated': true}))
  .toList();
await users.updateAll(updates);

// Batch delete
final idsToDelete = newUsers.skip(50).map((u) => u.id).toList();
await users.deleteAll(idsToDelete);
```

### Union Types Support

```dart
@freezed
class UserState with _$UserState {
  const factory UserState.idle() = IdleState;
  const factory UserState.loading(double progress) = LoadingState;
  const factory UserState.success(User user) = SuccessState;
  const factory UserState.error(String message) = ErrorState;

  factory UserState.fromJson(Map<String, dynamic> json) => 
    _$UserStateFromJson(json);
}

// Use with collections
final states = FreezedCollection<UserState>(
  db,
  'user_states',
  FreezedAdapter<UserState>(
    fromJson: UserState.fromJson,
    toJson: (state) => state.toJson(),
  ),
);

// Pattern matching
final state = await states.get('current_state');
state?.when(
  idle: () => print('Idle'),
  loading: (progress) => print('Loading: ${progress * 100}%'),
  success: (user) => print('User: ${user.name}'),
  error: (message) => print('Error: $message'),
);
```

### Custom ID Fields

```dart
// Use custom ID extractor
final products = FreezedCollection<Product>(
  db,
  'products',
  FreezedAdapter<Product>(
    fromJson: Product.fromJson,
    toJson: (product) => product.toJson(),
    getId: (product) => product.sku, // Use SKU as ID
  ),
);

// Or use a different field
final orders = FreezedCollection<Order>(
  db,
  'orders',
  FreezedAdapter<Order>(
    fromJson: Order.fromJson,
    toJson: (order) => order.toJson(),
    getId: (order) => order.orderNumber,
  ),
);
```

### Transactions

```dart
// Run in transaction
await db.runInTransaction(() async {
  final user = User(id: '1', name: 'John', email: 'john@example.com');
  final profile = Profile(userId: '1', bio: 'Developer');
  
  await users.add(user);
  await profiles.add(profile);
  
  // Both succeed or both fail
});

// Manual transaction control
final tx = db.beginTransaction();
try {
  await users.addWithTransaction(user, tx);
  await profiles.addWithTransaction(profile, tx);
  tx.commit();
} catch (e) {
  tx.rollback();
  rethrow;
}
```

### Indexes

```dart
// Create indexes for better query performance
await users.createIndex('age_index', ['age']);
await users.createIndex('email_index', ['email']);
await users.createIndex('composite_index', ['age', 'metadata.active']);

// Drop index
await users.dropIndex('age_index');

// List indexes
final indexes = await users.listIndexes();
```

### Query Operators

```dart
// Comparison operators
.where('age', isEqualTo: 30)
.where('age', isNotEqualTo: 30)
.where('age', isGreaterThan: 25)
.where('age', isGreaterThanOrEqualTo: 25)
.where('age', isLessThan: 35)
.where('age', isLessThanOrEqualTo: 35)

// String operators
.where('name', startsWith: 'John')
.where('email', endsWith: '@example.com')
.where('bio', contains: 'developer')
.where('phone', matches: r'^\+1\d{10}$') // Regex

// Array operators
.where('tags', contains: 'premium')
.where('tags', containsAny: ['premium', 'vip'])
.where('tags', containsAll: ['verified', 'premium'])

// Null checks
.where('deletedAt', isNull: true)
.where('metadata', isNull: false)

// Nested fields
.where('address.city', isEqualTo: 'New York')
.where('settings.notifications.email', isEqualTo: true)
```

### Custom Adapters

```dart
class TimestampAdapter extends FreezedAdapter<User> {
  @override
  Map<String, dynamic> toJson(User model) {
    final json = model.toJson();
    // Add server timestamp
    json['lastModified'] = DateTime.now().toIso8601String();
    return json;
  }

  @override
  User fromJson(Map<String, dynamic> json) {
    // Custom deserialization
    return User.fromJson(json);
  }

  @override
  String getId(User model) => model.id;
}

final users = FreezedCollection<User>(
  db,
  'users',
  TimestampAdapter(),
);
```

### Collection Events

```dart
// Global collection events
users.events.listen((event) {
  switch (event.type) {
    case CollectionEventType.queryExecuted:
      print('Query executed: ${event.metadata['duration']}ms');
      break;
    case CollectionEventType.indexCreated:
      print('Index created: ${event.metadata['name']}');
      break;
    case CollectionEventType.cleared:
      print('Collection cleared');
      break;
  }
});
```

## Performance Optimization

### 1. Use Indexes

```dart
// Create indexes for frequently queried fields
await users.createIndex('email_unique', ['email'], unique: true);
await users.createIndex('age_active', ['age', 'metadata.active']);
```

### 2. Projection Queries

```dart
// Only fetch needed fields
final names = await users.query()
  .select(['id', 'name'])
  .findAll();
```

### 3. Batch Operations

```dart
// Instead of multiple individual operations
for (final user in users) {
  await users.add(user); // Slow
}

// Use batch operations
await users.addAll(users); // Fast
```

### 4. Use Transactions

```dart
// Group related operations
await db.runInTransaction(() async {
  await users.addAll(newUsers);
  await profiles.addAll(newProfiles);
  await settings.updateAll(newSettings);
});
```

## Error Handling

```dart
try {
  await users.add(user);
} on CollectionException catch (e) {
  switch (e.type) {
    case CollectionExceptionType.duplicateId:
      print('User with this ID already exists');
      break;
    case CollectionExceptionType.invalidQuery:
      print('Invalid query: ${e.message}');
      break;
    case CollectionExceptionType.serializationError:
      print('Failed to serialize: ${e.message}');
      break;
    default:
      print('Collection error: ${e.message}');
  }
} on LightningDbException catch (e) {
  print('Database error: ${e.message}');
}
```

## Testing

```dart
import 'package:test/test.dart';

void main() {
  late LightningDb db;
  late FreezedCollection<User> users;

  setUp(() {
    db = LightningDb.createInMemory();
    users = FreezedCollection<User>(
      db,
      'users',
      FreezedAdapter<User>(
        fromJson: User.fromJson,
        toJson: (user) => user.toJson(),
      ),
    );
  });

  tearDown(() {
    db.close();
  });

  test('should add and retrieve user', () async {
    final user = User(id: '1', name: 'Test', email: 'test@example.com');
    
    await users.add(user);
    final retrieved = await users.get('1');
    
    expect(retrieved, equals(user));
  });
}
```

## Best Practices

1. **Always define fromJson/toJson**: Required for serialization
2. **Use meaningful collection names**: Helps with debugging
3. **Create indexes early**: Before adding large amounts of data
4. **Use transactions**: For related operations
5. **Handle errors**: Especially for unique constraints
6. **Clean up streams**: Cancel subscriptions when done
7. **Use batch operations**: For better performance
8. **Profile queries**: Use query explanation for optimization

## Migration Guide

### From Raw LightningDb

```dart
// Before
db.putJson('users:1', user.toJson());
final json = db.getJson('users:1');
final user = json != null ? User.fromJson(json) : null;

// After
await users.add(user);
final user = await users.get('1');
```

### From Other Databases

```dart
// From sqflite
final List<Map> maps = await db.query('users', where: 'age > ?', whereArgs: [25]);
final users = maps.map((m) => User.fromMap(m)).toList();

// With Lightning DB Freezed
final users = await users.query()
  .where('age', isGreaterThan: 25)
  .findAll();
```

## API Reference

See the [API documentation](https://pub.dev/documentation/lightning_db_freezed/latest/) for complete reference.

## License

MIT License - see LICENSE file for details.