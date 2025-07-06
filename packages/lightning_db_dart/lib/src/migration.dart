import 'dart:async';
import 'package:meta/meta.dart';
import 'database.dart';
import 'errors.dart';

/// Database migration manager
class MigrationManager {
  final LightningDb db;
  final String metadataKey = '_migrations_metadata';
  
  MigrationManager(this.db);
  
  /// Get current schema version
  Future<int> getCurrentVersion() async {
    try {
      final metadata = await db.getJson(metadataKey);
      return metadata?['version'] ?? 0;
    } catch (e) {
      return 0;
    }
  }
  
  /// Set current schema version
  Future<void> setCurrentVersion(int version) async {
    await db.putJson(metadataKey, {
      'version': version,
      'lastMigration': DateTime.now().toIso8601String(),
    });
  }
  
  /// Run migrations
  Future<void> migrate(List<Migration> migrations) async {
    final currentVersion = await getCurrentVersion();
    
    // Filter migrations that need to run
    final pendingMigrations = migrations
        .where((m) => m.version > currentVersion)
        .toList()
      ..sort((a, b) => a.version.compareTo(b.version));
    
    if (pendingMigrations.isEmpty) {
      return;
    }
    
    // Run migrations in transaction
    await db.transaction((tx) async {
      for (final migration in pendingMigrations) {
        try {
          // Create migration context
          final context = MigrationContext(
            db: db,
            tx: tx,
            fromVersion: currentVersion,
            toVersion: migration.version,
          );
          
          // Run migration
          await migration.up(context);
          
          // Update version
          await setCurrentVersion(migration.version);
          
        } catch (e) {
          throw MigrationException(
            message: 'Migration ${migration.version} failed: $e',
            fromVersion: currentVersion,
            toVersion: migration.version,
            errorType: 'migration_failed',
          );
        }
      }
    });
  }
  
  /// Rollback to a specific version
  Future<void> rollback(int targetVersion, List<Migration> migrations) async {
    final currentVersion = await getCurrentVersion();
    
    if (targetVersion >= currentVersion) {
      throw MigrationException(
        message: 'Cannot rollback to version $targetVersion from $currentVersion',
        fromVersion: currentVersion,
        toVersion: targetVersion,
        errorType: 'invalid_rollback',
      );
    }
    
    // Get migrations to rollback
    final rollbackMigrations = migrations
        .where((m) => m.version > targetVersion && m.version <= currentVersion)
        .toList()
      ..sort((a, b) => b.version.compareTo(a.version)); // Reverse order
    
    // Run rollbacks in transaction
    await db.transaction((tx) async {
      for (final migration in rollbackMigrations) {
        try {
          final context = MigrationContext(
            db: db,
            tx: tx,
            fromVersion: currentVersion,
            toVersion: migration.version - 1,
          );
          
          await migration.down(context);
          await setCurrentVersion(migration.version - 1);
          
        } catch (e) {
          throw MigrationException(
            message: 'Rollback of migration ${migration.version} failed: $e',
            fromVersion: currentVersion,
            toVersion: targetVersion,
            errorType: 'rollback_failed',
          );
        }
      }
    });
  }
}

/// Base class for database migrations
abstract class Migration {
  /// Version number for this migration
  final int version;
  
  /// Description of what this migration does
  final String description;
  
  Migration({
    required this.version,
    required this.description,
  });
  
  /// Apply the migration
  Future<void> up(MigrationContext context);
  
  /// Rollback the migration
  Future<void> down(MigrationContext context);
}

/// Context provided to migrations
class MigrationContext {
  final LightningDb db;
  final Transaction tx;
  final int fromVersion;
  final int toVersion;
  
  MigrationContext({
    required this.db,
    required this.tx,
    required this.fromVersion,
    required this.toVersion,
  });
  
  /// Helper to rename a collection
  Future<void> renameCollection(String oldName, String newName) async {
    // Get all keys from old collection
    final oldPrefix = '$oldName:';
    final newPrefix = '$newName:';
    
    await for (final kv in db.scanStream(startKey: oldPrefix)) {
      if (!kv.key.startsWith(oldPrefix)) break;
      
      // Create new key
      final newKey = kv.key.replaceFirst(oldPrefix, newPrefix);
      
      // Copy to new key
      await db.putBytes(newKey, kv.value);
      
      // Delete old key
      await db.delete(kv.key);
    }
  }
  
  /// Helper to transform documents in a collection
  Future<void> transformCollection<T>(
    String collectionName,
    Map<String, dynamic> Function(Map<String, dynamic>) transformer,
  ) async {
    final prefix = '$collectionName:';
    
    await for (final kv in db.scanStream(startKey: prefix)) {
      if (!kv.key.startsWith(prefix)) break;
      
      try {
        // Parse existing data
        final oldData = await db.getJson(kv.key);
        if (oldData == null) continue;
        
        // Transform data
        final newData = transformer(oldData);
        
        // Save transformed data
        await db.putJson(kv.key, newData);
      } catch (e) {
        throw MigrationException(
          message: 'Failed to transform document ${kv.key}: $e',
          fromVersion: fromVersion,
          toVersion: toVersion,
          errorType: 'transform_failed',
        );
      }
    }
  }
  
  /// Helper to add a field to all documents in a collection
  Future<void> addField(
    String collectionName,
    String fieldName,
    dynamic Function(Map<String, dynamic>) valueProvider,
  ) async {
    await transformCollection(collectionName, (doc) {
      doc[fieldName] = valueProvider(doc);
      return doc;
    });
  }
  
  /// Helper to remove a field from all documents in a collection
  Future<void> removeField(String collectionName, String fieldName) async {
    await transformCollection(collectionName, (doc) {
      doc.remove(fieldName);
      return doc;
    });
  }
  
  /// Helper to rename a field in all documents
  Future<void> renameField(
    String collectionName,
    String oldFieldName,
    String newFieldName,
  ) async {
    await transformCollection(collectionName, (doc) {
      if (doc.containsKey(oldFieldName)) {
        doc[newFieldName] = doc[oldFieldName];
        doc.remove(oldFieldName);
      }
      return doc;
    });
  }
  
  /// Helper to create an index
  Future<void> createIndex(
    String collectionName,
    String fieldName, {
    bool unique = false,
  }) async {
    // Store index metadata
    final indexKey = '_index:$collectionName:$fieldName';
    await db.putJson(indexKey, {
      'collection': collectionName,
      'field': fieldName,
      'unique': unique,
      'createdAt': DateTime.now().toIso8601String(),
    });
    
    // Build index entries
    final prefix = '$collectionName:';
    final indexPrefix = '_idx:$collectionName:$fieldName:';
    
    await for (final kv in db.scanStream(startKey: prefix)) {
      if (!kv.key.startsWith(prefix)) break;
      
      final doc = await db.getJson(kv.key);
      if (doc == null) continue;
      
      final fieldValue = _getFieldValue(doc, fieldName);
      if (fieldValue != null) {
        final indexKey = '$indexPrefix$fieldValue:${kv.key}';
        await db.put(indexKey, kv.key);
      }
    }
  }
  
  /// Helper to drop an index
  Future<void> dropIndex(String collectionName, String fieldName) async {
    // Remove index metadata
    final indexKey = '_index:$collectionName:$fieldName';
    await db.delete(indexKey);
    
    // Remove index entries
    final indexPrefix = '_idx:$collectionName:$fieldName:';
    await for (final kv in db.scanStream(startKey: indexPrefix)) {
      if (!kv.key.startsWith(indexPrefix)) break;
      await db.delete(kv.key);
    }
  }
  
  dynamic _getFieldValue(Map<String, dynamic> doc, String fieldPath) {
    final parts = fieldPath.split('.');
    dynamic current = doc;
    
    for (final part in parts) {
      if (current is Map && current.containsKey(part)) {
        current = current[part];
      } else {
        return null;
      }
    }
    
    return current;
  }
}

/// Migration exception
class MigrationException extends LightningDbException {
  final int fromVersion;
  final int toVersion;
  
  MigrationException({
    required String message,
    required this.fromVersion,
    required this.toVersion,
    String? errorType,
  }) : super(
    message: message,
    errorCode: 5000,
    errorType: errorType ?? 'migration_error',
  );
  
  @override
  String get userMessage => 'Database migration failed. Please contact support.';
}

/// Data migration for complex transformations
abstract class DataMigration extends Migration {
  DataMigration({
    required int version,
    required String description,
  }) : super(version: version, description: description);
  
  /// Batch size for processing
  int get batchSize => 1000;
  
  /// Process a batch of documents
  Future<void> processBatch(
    MigrationContext context,
    List<MapEntry<String, Map<String, dynamic>>> batch,
  );
  
  @override
  Future<void> up(MigrationContext context) async {
    final documents = <MapEntry<String, Map<String, dynamic>>>[];
    
    // Collect all documents that need processing
    await for (final kv in context.db.scanStream()) {
      final doc = await context.db.getJson(kv.key);
      if (doc != null && shouldProcess(kv.key, doc)) {
        documents.add(MapEntry(kv.key, doc));
        
        // Process in batches
        if (documents.length >= batchSize) {
          await processBatch(context, documents);
          documents.clear();
        }
      }
    }
    
    // Process remaining documents
    if (documents.isNotEmpty) {
      await processBatch(context, documents);
    }
  }
  
  /// Check if a document should be processed
  bool shouldProcess(String key, Map<String, dynamic> doc);
}

/// Schema migration builder
class SchemaBuilder {
  final List<SchemaChange> changes = [];
  
  /// Add a new collection
  void createCollection(String name) {
    changes.add(CreateCollection(name));
  }
  
  /// Remove a collection
  void dropCollection(String name) {
    changes.add(DropCollection(name));
  }
  
  /// Rename a collection
  void renameCollection(String oldName, String newName) {
    changes.add(RenameCollection(oldName, newName));
  }
  
  /// Add a field to documents
  void addField(
    String collection,
    String fieldName,
    dynamic Function(Map<String, dynamic>) valueProvider,
  ) {
    changes.add(AddField(collection, fieldName, valueProvider));
  }
  
  /// Remove a field from documents
  void removeField(String collection, String fieldName) {
    changes.add(RemoveField(collection, fieldName));
  }
  
  /// Rename a field in documents
  void renameField(String collection, String oldName, String newName) {
    changes.add(RenameField(collection, oldName, newName));
  }
  
  /// Create an index
  void createIndex(String collection, String field, {bool unique = false}) {
    changes.add(CreateIndex(collection, field, unique: unique));
  }
  
  /// Drop an index
  void dropIndex(String collection, String field) {
    changes.add(DropIndex(collection, field));
  }
  
  /// Apply all changes
  Future<void> apply(MigrationContext context) async {
    for (final change in changes) {
      await change.apply(context);
    }
  }
  
  /// Rollback all changes
  Future<void> rollback(MigrationContext context) async {
    for (final change in changes.reversed) {
      await change.rollback(context);
    }
  }
}

/// Base class for schema changes
abstract class SchemaChange {
  Future<void> apply(MigrationContext context);
  Future<void> rollback(MigrationContext context);
}

/// Create collection change
class CreateCollection extends SchemaChange {
  final String name;
  
  CreateCollection(this.name);
  
  @override
  Future<void> apply(MigrationContext context) async {
    // Collections are created automatically, just add metadata
    await context.db.putJson('_collection:$name', {
      'createdAt': DateTime.now().toIso8601String(),
      'version': context.toVersion,
    });
  }
  
  @override
  Future<void> rollback(MigrationContext context) async {
    await context.db.delete('_collection:$name');
  }
}

/// Drop collection change
class DropCollection extends SchemaChange {
  final String name;
  
  DropCollection(this.name);
  
  @override
  Future<void> apply(MigrationContext context) async {
    // Delete all documents in collection
    final prefix = '$name:';
    await for (final kv in context.db.scanStream(startKey: prefix)) {
      if (!kv.key.startsWith(prefix)) break;
      await context.db.delete(kv.key);
    }
    
    // Delete collection metadata
    await context.db.delete('_collection:$name');
  }
  
  @override
  Future<void> rollback(MigrationContext context) async {
    // Cannot rollback dropped collection
    throw MigrationException(
      message: 'Cannot rollback dropped collection $name',
      fromVersion: context.toVersion,
      toVersion: context.fromVersion,
      errorType: 'irreversible_migration',
    );
  }
}

/// Rename collection change
class RenameCollection extends SchemaChange {
  final String oldName;
  final String newName;
  
  RenameCollection(this.oldName, this.newName);
  
  @override
  Future<void> apply(MigrationContext context) async {
    await context.renameCollection(oldName, newName);
  }
  
  @override
  Future<void> rollback(MigrationContext context) async {
    await context.renameCollection(newName, oldName);
  }
}

/// Add field change
class AddField extends SchemaChange {
  final String collection;
  final String fieldName;
  final dynamic Function(Map<String, dynamic>) valueProvider;
  
  AddField(this.collection, this.fieldName, this.valueProvider);
  
  @override
  Future<void> apply(MigrationContext context) async {
    await context.addField(collection, fieldName, valueProvider);
  }
  
  @override
  Future<void> rollback(MigrationContext context) async {
    await context.removeField(collection, fieldName);
  }
}

/// Remove field change
class RemoveField extends SchemaChange {
  final String collection;
  final String fieldName;
  final Map<String, dynamic> _backupData = {};
  
  RemoveField(this.collection, this.fieldName);
  
  @override
  Future<void> apply(MigrationContext context) async {
    // Backup field values before removing
    final prefix = '$collection:';
    await for (final kv in context.db.scanStream(startKey: prefix)) {
      if (!kv.key.startsWith(prefix)) break;
      
      final doc = await context.db.getJson(kv.key);
      if (doc != null && doc.containsKey(fieldName)) {
        _backupData[kv.key] = doc[fieldName];
      }
    }
    
    await context.removeField(collection, fieldName);
  }
  
  @override
  Future<void> rollback(MigrationContext context) async {
    // Restore backed up values
    for (final entry in _backupData.entries) {
      final doc = await context.db.getJson(entry.key);
      if (doc != null) {
        doc[fieldName] = entry.value;
        await context.db.putJson(entry.key, doc);
      }
    }
  }
}

/// Rename field change
class RenameField extends SchemaChange {
  final String collection;
  final String oldName;
  final String newName;
  
  RenameField(this.collection, this.oldName, this.newName);
  
  @override
  Future<void> apply(MigrationContext context) async {
    await context.renameField(collection, oldName, newName);
  }
  
  @override
  Future<void> rollback(MigrationContext context) async {
    await context.renameField(collection, newName, oldName);
  }
}

/// Create index change
class CreateIndex extends SchemaChange {
  final String collection;
  final String field;
  final bool unique;
  
  CreateIndex(this.collection, this.field, {this.unique = false});
  
  @override
  Future<void> apply(MigrationContext context) async {
    await context.createIndex(collection, field, unique: unique);
  }
  
  @override
  Future<void> rollback(MigrationContext context) async {
    await context.dropIndex(collection, field);
  }
}

/// Drop index change
class DropIndex extends SchemaChange {
  final String collection;
  final String field;
  
  DropIndex(this.collection, this.field);
  
  @override
  Future<void> apply(MigrationContext context) async {
    await context.dropIndex(collection, field);
  }
  
  @override
  Future<void> rollback(MigrationContext context) async {
    // Cannot rollback dropped index without rebuilding
    throw MigrationException(
      message: 'Cannot rollback dropped index on $collection.$field',
      fromVersion: context.toVersion,
      toVersion: context.fromVersion,
      errorType: 'irreversible_migration',
    );
  }
}