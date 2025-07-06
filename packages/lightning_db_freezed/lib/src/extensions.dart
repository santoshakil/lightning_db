import 'dart:typed_data';
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'adapter.dart';
import 'collection.dart';
import 'serialization.dart';

/// Extension methods for LightningDb to work with Freezed models
extension FreezedDbExtensions on LightningDb {
  /// Create a collection with a custom adapter
  FreezedCollection<T> collection<T>({
    required String name,
    required FreezedAdapter<T> adapter,
  }) {
    return FreezedCollection<T>(
      db: this,
      collectionName: name,
      adapter: adapter,
    );
  }
  
  /// Create a JSON-based collection
  FreezedCollection<T> jsonCollection<T>({
    required String name,
    required Map<String, dynamic> Function(T) toJson,
    required T Function(Map<String, dynamic>) fromJson,
    required String Function(T) keyExtractor,
  }) {
    final adapter = JsonFreezedAdapter<T>(
      toJson: toJson,
      fromJson: fromJson,
      keyExtractor: keyExtractor,
      typeName: name,
    );
    
    return collection<T>(name: name, adapter: adapter);
  }
  
  /// Create a collection using a registered adapter
  FreezedCollection<T> typedCollection<T>(String name) {
    final adapter = AdapterFactory.get<T>();
    if (adapter == null) {
      throw StateError(
        'No adapter registered for type $T. '
        'Use AdapterFactory.register() to register an adapter.'
      );
    }
    
    return collection<T>(name: name, adapter: adapter);
  }
  
  /// Store a single Freezed object
  Future<void> putObject<T>(T object, FreezedAdapter<T> adapter) async {
    final key = adapter.createCompoundKey(object);
    final bytes = adapter.serialize(object);
    await put(key, bytes);
  }
  
  /// Get a single Freezed object
  Future<T?> getObject<T>(String key, FreezedAdapter<T> adapter) async {
    final compoundKey = '${adapter.typeName}:$key';
    final bytes = await get(compoundKey);
    if (bytes == null) return null;
    return adapter.deserialize(bytes);
  }
  
  /// Delete a single Freezed object
  Future<bool> deleteObject<T>(T object, FreezedAdapter<T> adapter) async {
    final key = adapter.createCompoundKey(object);
    return delete(key);
  }
}

/// Extension for transactions with Freezed support
extension FreezedTransactionExtensions on Transaction {
  /// Put a Freezed object in the transaction
  Future<void> putObject<T>(T object, FreezedAdapter<T> adapter) async {
    final key = adapter.createCompoundKey(object);
    final bytes = adapter.serialize(object);
    await put(key, bytes);
  }
  
  /// Get a Freezed object in the transaction
  Future<T?> getObject<T>(String key, FreezedAdapter<T> adapter) async {
    final compoundKey = '${adapter.typeName}:$key';
    final bytes = await get(compoundKey);
    if (bytes == null) return null;
    return adapter.deserialize(bytes);
  }
  
  /// Delete a Freezed object in the transaction
  Future<bool> deleteObject<T>(T object, FreezedAdapter<T> adapter) async {
    final key = adapter.createCompoundKey(object);
    return delete(key);
  }
}

/// Extension for batch operations with Freezed support
extension FreezedBatchExtensions on Batch {
  /// Add a put operation for a Freezed object
  void putObject<T>(T object, FreezedAdapter<T> adapter) {
    final key = adapter.createCompoundKey(object);
    final bytes = adapter.serialize(object);
    put(key, bytes);
  }
  
  /// Add a delete operation for a Freezed object
  void deleteObject<T>(T object, FreezedAdapter<T> adapter) {
    final key = adapter.createCompoundKey(object);
    delete(key);
  }
}

/// Convenience methods for creating adapters
class Adapters {
  /// Create a JSON adapter with automatic type inference
  static JsonFreezedAdapter<T> json<T>({
    required Map<String, dynamic> Function(T) toJson,
    required T Function(Map<String, dynamic>) fromJson,
    required String Function(T) keyExtractor,
    String? typeName,
  }) {
    return JsonFreezedAdapter<T>(
      toJson: toJson,
      fromJson: fromJson,
      keyExtractor: keyExtractor,
      typeName: typeName ?? T.toString(),
    );
  }
  
  /// Create a custom adapter
  static CustomFreezedAdapter<T> custom<T>({
    required String Function(T) keyExtractor,
    required Uint8List Function(T) serializer,
    required T Function(Uint8List) deserializer,
    String? typeName,
  }) {
    return CustomFreezedAdapter<T>(
      keyExtractor: keyExtractor,
      customSerializer: serializer,
      customDeserializer: deserializer,
      typeName: typeName ?? T.toString(),
    );
  }
  
  /// Create an adapter for models with numeric IDs
  static JsonFreezedAdapter<T> withNumericId<T>({
    required Map<String, dynamic> Function(T) toJson,
    required T Function(Map<String, dynamic>) fromJson,
    required int Function(T) idExtractor,
    String? typeName,
  }) {
    return JsonFreezedAdapter<T>(
      toJson: toJson,
      fromJson: fromJson,
      keyExtractor: (obj) => idExtractor(obj).toString(),
      typeName: typeName ?? T.toString(),
    );
  }
  
  /// Create an adapter for models with UUID
  static JsonFreezedAdapter<T> withUuid<T>({
    required Map<String, dynamic> Function(T) toJson,
    required T Function(Map<String, dynamic>) fromJson,
    required String Function(T) uuidExtractor,
    String? typeName,
  }) {
    return JsonFreezedAdapter<T>(
      toJson: toJson,
      fromJson: fromJson,
      keyExtractor: uuidExtractor,
      typeName: typeName ?? T.toString(),
    );
  }
}

/// Database configuration extensions for Freezed support
extension FreezedDatabaseConfig on DatabaseConfig {
  /// Create a config optimized for Freezed models
  static DatabaseConfig forFreezed({
    int cacheSize = 50 * 1024 * 1024, // 50MB
    CompressionType compressionType = CompressionType.lz4,
    WalSyncMode walSyncMode = WalSyncMode.periodic,
  }) {
    return DatabaseConfig(
      cacheSize: cacheSize,
      compressionType: compressionType,
      walSyncMode: walSyncMode,
    );
  }
  
  /// Create a config for large Freezed collections
  static DatabaseConfig forLargeCollections() {
    return const DatabaseConfig(
      cacheSize: 200 * 1024 * 1024, // 200MB
      compressionType: CompressionType.zstd, // Better compression
      walSyncMode: WalSyncMode.periodic,
    );
  }
  
  /// Create a config for reactive applications
  static DatabaseConfig forReactiveApps() {
    return const DatabaseConfig(
      cacheSize: 100 * 1024 * 1024, // 100MB
      compressionType: CompressionType.lz4, // Fast compression
      walSyncMode: WalSyncMode.async, // Fast writes for UI updates
    );
  }
}

/// Migration support for Freezed models
abstract class FreezedMigration<T> {
  /// The version this migration upgrades to
  int get version;
  
  /// Migrate an object to the new version
  Future<T> migrate(T oldObject);
  
  /// Check if this migration applies to an object
  bool appliesTo(T object) => true;
}

/// Migration manager for collections
class MigrationManager<T> {
  final List<FreezedMigration<T>> _migrations = [];
  final FreezedCollection<T> collection;
  
  MigrationManager(this.collection);
  
  /// Register a migration
  void register(FreezedMigration<T> migration) {
    _migrations.add(migration);
    _migrations.sort((a, b) => a.version.compareTo(b.version));
  }
  
  /// Run all migrations
  Future<void> runMigrations() async {
    final allObjects = await collection.getAll();
    
    for (final object in allObjects) {
      var current = object;
      
      for (final migration in _migrations) {
        if (migration.appliesTo(current)) {
          current = await migration.migrate(current);
        }
      }
      
      // Update if changed
      if (current != object) {
        await collection.update(current);
      }
    }
  }
}