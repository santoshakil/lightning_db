import 'dart:async';
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'collection.dart';
import 'adapter.dart';

/// Freezed collection migration support
extension FreezedMigrationExtension on LightningDb {
  /// Run migrations for Freezed collections
  Future<void> migrateFreezed({
    required List<FreezedMigration> migrations,
    bool runInTransaction = true,
  }) async {
    final manager = MigrationManager(this);
    
    if (runInTransaction) {
      await transaction((tx) async {
        await manager.migrate(migrations);
      });
    } else {
      await manager.migrate(migrations);
    }
  }
}

/// Base class for Freezed-specific migrations
abstract class FreezedMigration extends Migration {
  FreezedMigration({
    required int version,
    required String description,
  }) : super(version: version, description: description);
  
  /// Helper to migrate between Freezed model versions
  Future<void> migrateModel<OldModel, NewModel>({
    required MigrationContext context,
    required String collectionName,
    required FreezedAdapter<OldModel> oldAdapter,
    required FreezedAdapter<NewModel> newAdapter,
    required NewModel Function(OldModel) transformer,
  }) async {
    final collection = FreezedCollection<OldModel>(
      context.db,
      collectionName,
      adapter: oldAdapter,
    );
    
    // Get all documents
    final documents = await collection.getAll();
    
    // Transform and save with new adapter
    final newCollection = FreezedCollection<NewModel>(
      context.db,
      collectionName,
      adapter: newAdapter,
    );
    
    for (final doc in documents) {
      final transformed = transformer(doc);
      await newCollection.update(transformed);
    }
  }
}

/// Schema migration for Freezed models
class FreezedSchemaMigration extends FreezedMigration {
  final void Function(FreezedSchemaBuilder) buildSchema;
  final void Function(FreezedSchemaBuilder)? buildRollback;
  
  FreezedSchemaMigration({
    required int version,
    required String description,
    required this.buildSchema,
    this.buildRollback,
  }) : super(version: version, description: description);
  
  @override
  Future<void> up(MigrationContext context) async {
    final builder = FreezedSchemaBuilder(context);
    buildSchema(builder);
    await builder.apply();
  }
  
  @override
  Future<void> down(MigrationContext context) async {
    if (buildRollback != null) {
      final builder = FreezedSchemaBuilder(context);
      buildRollback!(builder);
      await builder.apply();
    } else {
      throw MigrationException(
        message: 'No rollback defined for migration $version',
        fromVersion: version,
        toVersion: version - 1,
        errorType: 'no_rollback',
      );
    }
  }
}

/// Schema builder for Freezed collections
class FreezedSchemaBuilder {
  final MigrationContext context;
  final List<FreezedSchemaChange> changes = [];
  
  FreezedSchemaBuilder(this.context);
  
  /// Add a field with default value
  void addFieldWithDefault<T>(
    String collectionName,
    String fieldName,
    T defaultValue,
  ) {
    changes.add(AddFieldWithDefault(
      collectionName,
      fieldName,
      defaultValue,
    ));
  }
  
  /// Add a computed field
  void addComputedField(
    String collectionName,
    String fieldName,
    dynamic Function(Map<String, dynamic>) compute,
  ) {
    changes.add(AddComputedField(
      collectionName,
      fieldName,
      compute,
    ));
  }
  
  /// Transform a collection's data structure
  void transformCollection<OldModel, NewModel>({
    required String collectionName,
    required FreezedAdapter<OldModel> fromAdapter,
    required FreezedAdapter<NewModel> toAdapter,
    required NewModel Function(OldModel) transformer,
  }) {
    changes.add(TransformCollection<OldModel, NewModel>(
      collectionName: collectionName,
      fromAdapter: fromAdapter,
      toAdapter: toAdapter,
      transformer: transformer,
    ));
  }
  
  /// Merge two collections
  void mergeCollections({
    required String sourceCollection,
    required String targetCollection,
    required Map<String, dynamic> Function(Map<String, dynamic> source) transformer,
    bool deleteSource = false,
  }) {
    changes.add(MergeCollections(
      sourceCollection: sourceCollection,
      targetCollection: targetCollection,
      transformer: transformer,
      deleteSource: deleteSource,
    ));
  }
  
  /// Split a collection into multiple collections
  void splitCollection({
    required String sourceCollection,
    required Map<String, String Function(Map<String, dynamic>)> targets,
    bool deleteSource = false,
  }) {
    changes.add(SplitCollection(
      sourceCollection: sourceCollection,
      targets: targets,
      deleteSource: deleteSource,
    ));
  }
  
  /// Add validation to a collection
  void addValidation(
    String collectionName,
    bool Function(Map<String, dynamic>) validator,
    String errorMessage,
  ) {
    changes.add(AddValidation(
      collectionName,
      validator,
      errorMessage,
    ));
  }
  
  /// Apply all changes
  Future<void> apply() async {
    for (final change in changes) {
      await change.apply(context);
    }
  }
}

/// Base class for Freezed schema changes
abstract class FreezedSchemaChange {
  Future<void> apply(MigrationContext context);
}

/// Add field with default value
class AddFieldWithDefault<T> extends FreezedSchemaChange {
  final String collectionName;
  final String fieldName;
  final T defaultValue;
  
  AddFieldWithDefault(this.collectionName, this.fieldName, this.defaultValue);
  
  @override
  Future<void> apply(MigrationContext context) async {
    await context.addField(
      collectionName,
      fieldName,
      (_) => defaultValue,
    );
  }
}

/// Add computed field
class AddComputedField extends FreezedSchemaChange {
  final String collectionName;
  final String fieldName;
  final dynamic Function(Map<String, dynamic>) compute;
  
  AddComputedField(this.collectionName, this.fieldName, this.compute);
  
  @override
  Future<void> apply(MigrationContext context) async {
    await context.addField(collectionName, fieldName, compute);
  }
}

/// Transform collection data structure
class TransformCollection<OldModel, NewModel> extends FreezedSchemaChange {
  final String collectionName;
  final FreezedAdapter<OldModel> fromAdapter;
  final FreezedAdapter<NewModel> toAdapter;
  final NewModel Function(OldModel) transformer;
  
  TransformCollection({
    required this.collectionName,
    required this.fromAdapter,
    required this.toAdapter,
    required this.transformer,
  });
  
  @override
  Future<void> apply(MigrationContext context) async {
    final oldCollection = FreezedCollection<OldModel>(
      context.db,
      collectionName,
      adapter: fromAdapter,
    );
    
    final newCollection = FreezedCollection<NewModel>(
      context.db,
      collectionName,
      adapter: toAdapter,
    );
    
    // Get all documents
    final documents = await oldCollection.getAll();
    
    // Transform each document
    for (final doc in documents) {
      final transformed = transformer(doc);
      await newCollection.update(transformed);
    }
  }
}

/// Merge collections
class MergeCollections extends FreezedSchemaChange {
  final String sourceCollection;
  final String targetCollection;
  final Map<String, dynamic> Function(Map<String, dynamic>) transformer;
  final bool deleteSource;
  
  MergeCollections({
    required this.sourceCollection,
    required this.targetCollection,
    required this.transformer,
    required this.deleteSource,
  });
  
  @override
  Future<void> apply(MigrationContext context) async {
    final sourcePrefix = '$sourceCollection:';
    
    await for (final kv in context.db.scanStream(startKey: sourcePrefix)) {
      if (!kv.key.startsWith(sourcePrefix)) break;
      
      final sourceDoc = await context.db.getJson(kv.key);
      if (sourceDoc == null) continue;
      
      // Transform document
      final targetDoc = transformer(sourceDoc);
      
      // Generate new key for target collection
      final id = kv.key.substring(sourcePrefix.length);
      final targetKey = '$targetCollection:$id';
      
      // Save to target collection
      await context.db.putJson(targetKey, targetDoc);
      
      // Delete source if requested
      if (deleteSource) {
        await context.db.delete(kv.key);
      }
    }
  }
}

/// Split collection
class SplitCollection extends FreezedSchemaChange {
  final String sourceCollection;
  final Map<String, String Function(Map<String, dynamic>)> targets;
  final bool deleteSource;
  
  SplitCollection({
    required this.sourceCollection,
    required this.targets,
    required this.deleteSource,
  });
  
  @override
  Future<void> apply(MigrationContext context) async {
    final sourcePrefix = '$sourceCollection:';
    
    await for (final kv in context.db.scanStream(startKey: sourcePrefix)) {
      if (!kv.key.startsWith(sourcePrefix)) break;
      
      final sourceDoc = await context.db.getJson(kv.key);
      if (sourceDoc == null) continue;
      
      // Determine target collection
      for (final entry in targets.entries) {
        final targetCollection = entry.key;
        final selector = entry.value;
        
        final targetId = selector(sourceDoc);
        if (targetId != null) {
          final targetKey = '$targetCollection:$targetId';
          await context.db.putJson(targetKey, sourceDoc);
          break;
        }
      }
      
      // Delete source if requested
      if (deleteSource) {
        await context.db.delete(kv.key);
      }
    }
  }
}

/// Add validation
class AddValidation extends FreezedSchemaChange {
  final String collectionName;
  final bool Function(Map<String, dynamic>) validator;
  final String errorMessage;
  
  AddValidation(this.collectionName, this.validator, this.errorMessage);
  
  @override
  Future<void> apply(MigrationContext context) async {
    final prefix = '$collectionName:';
    final invalidDocs = <String>[];
    
    await for (final kv in context.db.scanStream(startKey: prefix)) {
      if (!kv.key.startsWith(prefix)) break;
      
      final doc = await context.db.getJson(kv.key);
      if (doc != null && !validator(doc)) {
        invalidDocs.add(kv.key);
      }
    }
    
    if (invalidDocs.isNotEmpty) {
      throw MigrationException(
        message: '$errorMessage. Invalid documents: ${invalidDocs.join(', ')}',
        fromVersion: context.fromVersion,
        toVersion: context.toVersion,
        errorType: 'validation_failed',
      );
    }
  }
}

/// Example migrations
class ExampleMigrations {
  /// V1 -> V2: Add createdAt field
  static final addCreatedAt = FreezedSchemaMigration(
    version: 2,
    description: 'Add createdAt field to all documents',
    buildSchema: (schema) {
      schema.addFieldWithDefault(
        'users',
        'createdAt',
        DateTime.now().toIso8601String(),
      );
    },
    buildRollback: (schema) {
      // Remove createdAt field
      schema.changes.add(RemoveFieldChange('users', 'createdAt'));
    },
  );
  
  /// V2 -> V3: Rename username to displayName
  static final renameUsername = FreezedSchemaMigration(
    version: 3,
    description: 'Rename username field to displayName',
    buildSchema: (schema) {
      schema.changes.add(RenameFieldChange('users', 'username', 'displayName'));
    },
    buildRollback: (schema) {
      schema.changes.add(RenameFieldChange('users', 'displayName', 'username'));
    },
  );
  
  /// V3 -> V4: Split users into active and archived
  static final splitUsers = FreezedSchemaMigration(
    version: 4,
    description: 'Split users collection into active and archived',
    buildSchema: (schema) {
      schema.splitCollection(
        sourceCollection: 'users',
        targets: {
          'active_users': (doc) => doc['active'] == true ? doc['id'] : null,
          'archived_users': (doc) => doc['active'] == false ? doc['id'] : null,
        },
        deleteSource: true,
      );
    },
  );
}

/// Helper schema changes
class RemoveFieldChange extends FreezedSchemaChange {
  final String collectionName;
  final String fieldName;
  
  RemoveFieldChange(this.collectionName, this.fieldName);
  
  @override
  Future<void> apply(MigrationContext context) async {
    await context.removeField(collectionName, fieldName);
  }
}

class RenameFieldChange extends FreezedSchemaChange {
  final String collectionName;
  final String oldName;
  final String newName;
  
  RenameFieldChange(this.collectionName, this.oldName, this.newName);
  
  @override
  Future<void> apply(MigrationContext context) async {
    await context.renameField(collectionName, oldName, newName);
  }
}