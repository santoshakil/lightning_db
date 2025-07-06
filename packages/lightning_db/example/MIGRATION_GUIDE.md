# Lightning DB Migration Guide

## Overview

Lightning DB provides a robust migration system for evolving your database schema over time. The migration system supports:

- **Version tracking**: Automatic tracking of current schema version
- **Transactional migrations**: All migrations run in transactions
- **Rollback support**: Ability to rollback to previous versions
- **Type-safe migrations**: Full support for Freezed models
- **Data transformations**: Complex data migrations and transformations

## Basic Usage

### 1. Define Migrations

```dart
class AddUserStatusMigration extends Migration {
  AddUserStatusMigration() : super(
    version: 2,
    description: 'Add status field to users',
  );

  @override
  Future<void> up(MigrationContext context) async {
    // Add new field with default value
    await context.addField('users', 'status', (doc) => 'active');
  }

  @override
  Future<void> down(MigrationContext context) async {
    // Remove field on rollback
    await context.removeField('users', 'status');
  }
}
```

### 2. Run Migrations

```dart
final db = await LightningDb.open('app.db');
final migrationManager = MigrationManager(db);

// Define all migrations
final migrations = [
  InitialSchemaMigration(),     // v1
  AddUserStatusMigration(),      // v2
  CreateUserIndexMigration(),    // v3
];

// Run pending migrations
await migrationManager.migrate(migrations);

// Check current version
final version = await migrationManager.getCurrentVersion();
print('Database is at version: $version');
```

### 3. Rollback Migrations

```dart
// Rollback to version 1
await migrationManager.rollback(1, migrations);
```

## Freezed Model Migrations

Lightning DB provides special support for migrating Freezed models:

### 1. Model Evolution

When your Freezed model changes:

```dart
// Old model (v1)
@freezed
class UserV1 with _$UserV1 {
  const factory UserV1({
    required String id,
    required String name,
    required String email,
  }) = _UserV1;
}

// New model (v2)
@freezed
class UserV2 with _$UserV2 {
  const factory UserV2({
    required String id,
    required String displayName, // renamed from 'name'
    required String email,
    required DateTime createdAt, // new field
    @Default('active') String status, // new field with default
  }) = _UserV2;
}
```

### 2. Create Model Migration

```dart
class UserModelMigration extends FreezedMigration {
  UserModelMigration() : super(
    version: 2,
    description: 'Update user model to v2',
  );

  @override
  Future<void> up(MigrationContext context) async {
    await migrateModel<UserV1, UserV2>(
      context: context,
      collectionName: 'users',
      oldAdapter: FreezedAdapter<UserV1>(
        fromJson: UserV1.fromJson,
        toJson: (user) => user.toJson(),
      ),
      newAdapter: FreezedAdapter<UserV2>(
        fromJson: UserV2.fromJson,
        toJson: (user) => user.toJson(),
      ),
      transformer: (oldUser) => UserV2(
        id: oldUser.id,
        displayName: oldUser.name, // rename field
        email: oldUser.email,
        createdAt: DateTime.now(), // add new field
        status: 'active', // use default
      ),
    );
  }

  @override
  Future<void> down(MigrationContext context) async {
    // Implement reverse transformation
    await migrateModel<UserV2, UserV1>(
      context: context,
      collectionName: 'users',
      oldAdapter: FreezedAdapter<UserV2>(
        fromJson: UserV2.fromJson,
        toJson: (user) => user.toJson(),
      ),
      newAdapter: FreezedAdapter<UserV1>(
        fromJson: UserV1.fromJson,
        toJson: (user) => user.toJson(),
      ),
      transformer: (newUser) => UserV1(
        id: newUser.id,
        name: newUser.displayName, // restore original name
        email: newUser.email,
      ),
    );
  }
}
```

### 3. Schema Builder API

For more complex migrations, use the schema builder:

```dart
class ComplexMigration extends FreezedSchemaMigration {
  ComplexMigration() : super(
    version: 3,
    description: 'Complex schema changes',
    buildSchema: (schema) {
      // Add computed field
      schema.addComputedField('users', 'fullName', (doc) {
        return '${doc['firstName']} ${doc['lastName']}';
      });
      
      // Transform collection structure
      schema.transformCollection<OldUser, NewUser>(
        collectionName: 'users',
        fromAdapter: oldAdapter,
        toAdapter: newAdapter,
        transformer: (old) => NewUser(/* ... */),
      );
      
      // Split collection
      schema.splitCollection(
        sourceCollection: 'users',
        targets: {
          'active_users': (doc) => doc['active'] ? doc['id'] : null,
          'archived_users': (doc) => !doc['active'] ? doc['id'] : null,
        },
        deleteSource: true,
      );
      
      // Merge collections
      schema.mergeCollections(
        sourceCollection: 'temp_users',
        targetCollection: 'users',
        transformer: (doc) => doc..['imported'] = true,
        deleteSource: true,
      );
    },
  );
}
```

## Migration Helpers

### Field Operations

```dart
// Add field with default value
await context.addField('users', 'score', (doc) => 0);

// Add computed field
await context.addField('users', 'age', (doc) {
  final birthDate = DateTime.parse(doc['birthDate']);
  return DateTime.now().year - birthDate.year;
});

// Remove field
await context.removeField('users', 'deprecatedField');

// Rename field
await context.renameField('users', 'username', 'displayName');
```

### Collection Operations

```dart
// Rename collection
await context.renameCollection('user_profiles', 'users');

// Transform all documents
await context.transformCollection('users', (doc) {
  doc['email'] = doc['email'].toLowerCase();
  doc['updatedAt'] = DateTime.now().toIso8601String();
  return doc;
});
```

### Index Operations

```dart
// Create index
await context.createIndex('users', 'email', unique: true);

// Create compound index (using dot notation)
await context.createIndex('users', 'metadata.score');

// Drop index
await context.dropIndex('users', 'email');
```

## Best Practices

### 1. Always Test Migrations

```dart
// Test migration in development
final testDb = await LightningDb.open('test.db');
final testManager = MigrationManager(testDb);

try {
  // Run migration
  await testManager.migrate([myMigration]);
  
  // Verify data
  final users = testDb.freezedCollection<User>('users');
  final allUsers = await users.getAll();
  // Assert data is correct
  
  // Test rollback
  await testManager.rollback(previousVersion, [myMigration]);
  // Verify rollback worked
} finally {
  await testDb.close();
}
```

### 2. Version Your Models

```dart
// models/user_v1.dart
@freezed
class UserV1 with _$UserV1 { /* ... */ }

// models/user_v2.dart
@freezed
class UserV2 with _$UserV2 { /* ... */ }

// Keep old model definitions for migrations
```

### 3. Backup Before Migration

```dart
// Backup database before migration
await db.backup('backup_v${currentVersion}.db');

try {
  await migrationManager.migrate(migrations);
} catch (e) {
  // Restore from backup if needed
  await db.restore('backup_v${currentVersion}.db');
  rethrow;
}
```

### 4. Handle Large Datasets

For large collections, use batched migrations:

```dart
class LargeDataMigration extends DataMigration {
  LargeDataMigration() : super(
    version: 4,
    description: 'Process large dataset',
  );

  @override
  int get batchSize => 5000; // Process 5000 at a time

  @override
  bool shouldProcess(String key, Map<String, dynamic> doc) {
    // Only process documents that need migration
    return doc['version'] == null || doc['version'] < 4;
  }

  @override
  Future<void> processBatch(
    MigrationContext context,
    List<MapEntry<String, Map<String, dynamic>>> batch,
  ) async {
    // Process batch of documents
    for (final entry in batch) {
      entry.value['version'] = 4;
      entry.value['processedAt'] = DateTime.now().toIso8601String();
      await context.db.putJson(entry.key, entry.value);
    }
  }
}
```

### 5. Document Migrations

Always document what each migration does:

```dart
/// Migration 5: User Profile Enhancement
/// 
/// Changes:
/// - Adds 'profile' nested object with avatar, bio, and social links
/// - Migrates existing 'avatarUrl' to 'profile.avatar'
/// - Sets default bio based on user creation date
/// - Creates index on 'profile.avatar' for faster lookups
/// 
/// Rollback:
/// - Extracts avatar back to top level
/// - Removes profile object and index
class UserProfileMigration extends Migration {
  // ...
}
```

## Error Handling

```dart
try {
  await migrationManager.migrate(migrations);
} on MigrationException catch (e) {
  print('Migration failed: ${e.message}');
  print('From version: ${e.fromVersion}');
  print('To version: ${e.toVersion}');
  
  // Handle specific error types
  switch (e.errorType) {
    case 'migration_failed':
      // Migration logic failed
      break;
    case 'rollback_failed':
      // Rollback failed
      break;
    case 'irreversible_migration':
      // Cannot rollback this migration
      break;
  }
}
```

## Example: Complete Migration Flow

```dart
// 1. Define your migrations
final migrations = [
  InitialSchemaMigration(),           // v1: Create initial schema
  AddUserMetadataMigration(),         // v2: Add metadata fields
  CreateIndexesMigration(),           // v3: Add performance indexes  
  UserModelV2Migration(),             // v4: Update to new user model
  SplitUserCollectionMigration(),     // v5: Split active/archived users
];

// 2. Create migration UI
class MigrationService {
  final LightningDb db;
  final MigrationManager manager;
  
  MigrationService(this.db) : manager = MigrationManager(db);
  
  Future<void> runMigrations({
    required Function(String) onProgress,
    required Function(String) onError,
  }) async {
    final current = await manager.getCurrentVersion();
    final latest = migrations.last.version;
    
    if (current >= latest) {
      onProgress('Database is up to date (v$current)');
      return;
    }
    
    onProgress('Migrating from v$current to v$latest...');
    
    try {
      // Backup first
      await db.backup('pre_migration_v$current.db');
      onProgress('Backup created');
      
      // Run migrations
      await manager.migrate(migrations);
      
      onProgress('Migration completed successfully!');
    } catch (e) {
      onError('Migration failed: $e');
      
      // Attempt rollback
      try {
        await manager.rollback(current, migrations);
        onProgress('Rolled back to v$current');
      } catch (rollbackError) {
        onError('Rollback failed: $rollbackError');
      }
    }
  }
}
```

## Conclusion

Lightning DB's migration system provides all the tools needed to evolve your database schema safely and efficiently. By following these patterns and best practices, you can ensure smooth schema transitions as your application grows.