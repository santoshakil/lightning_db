import 'dart:async';
import 'dart:typed_data';
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'package:meta/meta.dart';
import 'adapter.dart';
import 'query.dart';
import 'reactive.dart';

/// A type-safe collection for Freezed models
class FreezedCollection<T> {
  final LightningDb _db;
  final String _collectionName;
  final FreezedAdapter<T> adapter;
  final StreamController<CollectionChange<T>> _changeController;
  
  FreezedCollection({
    required LightningDb db,
    required String collectionName,
    required this.adapter,
  }) : _db = db,
       _collectionName = collectionName,
       _changeController = StreamController<CollectionChange<T>>.broadcast();
  
  /// The collection name
  String get name => _collectionName;
  
  /// Stream of changes to this collection
  Stream<CollectionChange<T>> get changes => _changeController.stream;
  
  /// Create a key with collection prefix
  String _createKey(String key) => '$_collectionName:$key';
  
  /// Insert a new object
  Future<void> insert(T object) async {
    final key = _createKey(adapter.getKey(object));
    final bytes = adapter.serialize(object);
    
    await _db.put(key, bytes);
    
    _changeController.add(CollectionChange<T>(
      type: ChangeType.insert,
      key: adapter.getKey(object),
      newValue: object,
    ));
  }
  
  /// Insert multiple objects
  Future<void> insertMany(List<T> objects) async {
    if (objects.isEmpty) return;
    
    final batch = _db.batch();
    
    for (final object in objects) {
      final key = _createKey(adapter.getKey(object));
      final bytes = adapter.serialize(object);
      batch.put(key, bytes);
    }
    
    await batch.commit();
    
    // Notify changes
    for (final object in objects) {
      _changeController.add(CollectionChange<T>(
        type: ChangeType.insert,
        key: adapter.getKey(object),
        newValue: object,
      ));
    }
  }
  
  /// Update an existing object
  Future<void> update(T object) async {
    final key = _createKey(adapter.getKey(object));
    final oldBytes = await _db.get(key);
    final oldValue = oldBytes != null ? adapter.deserialize(oldBytes) : null;
    
    final bytes = adapter.serialize(object);
    await _db.put(key, bytes);
    
    _changeController.add(CollectionChange<T>(
      type: ChangeType.update,
      key: adapter.getKey(object),
      oldValue: oldValue,
      newValue: object,
    ));
  }
  
  /// Insert or update an object
  Future<void> upsert(T object) async {
    final key = _createKey(adapter.getKey(object));
    final exists = await _db.exists(key);
    
    if (exists) {
      await update(object);
    } else {
      await insert(object);
    }
  }
  
  /// Find an object by its key
  Future<T?> findByKey(String key) async {
    final dbKey = _createKey(key);
    final bytes = await _db.get(dbKey);
    
    if (bytes == null) return null;
    return adapter.deserialize(bytes);
  }
  
  /// Find multiple objects by their keys
  Future<List<T>> findByKeys(List<String> keys) async {
    final results = <T>[];
    
    for (final key in keys) {
      final object = await findByKey(key);
      if (object != null) {
        results.add(object);
      }
    }
    
    return results;
  }
  
  /// Delete an object by its key
  Future<bool> deleteByKey(String key) async {
    final dbKey = _createKey(key);
    final bytes = await _db.get(dbKey);
    
    if (bytes == null) return false;
    
    final oldValue = adapter.deserialize(bytes);
    final deleted = await _db.delete(dbKey);
    
    if (deleted) {
      _changeController.add(CollectionChange<T>(
        type: ChangeType.delete,
        key: key,
        oldValue: oldValue,
      ));
    }
    
    return deleted;
  }
  
  /// Delete an object
  Future<bool> delete(T object) async {
    return deleteByKey(adapter.getKey(object));
  }
  
  /// Delete multiple objects by their keys
  Future<int> deleteByKeys(List<String> keys) async {
    if (keys.isEmpty) return 0;
    
    int count = 0;
    final batch = _db.batch();
    
    for (final key in keys) {
      final dbKey = _createKey(key);
      final exists = await _db.exists(dbKey);
      
      if (exists) {
        batch.delete(dbKey);
        count++;
      }
    }
    
    if (count > 0) {
      await batch.commit();
    }
    
    return count;
  }
  
  /// Get all objects in the collection
  Future<List<T>> getAll() async {
    final prefix = '$_collectionName:';
    final items = await _db.getRange(startKey: prefix);
    
    final results = <T>[];
    for (final item in items) {
      if (item.key.startsWith(prefix)) {
        try {
          final object = adapter.deserialize(item.value);
          results.add(object);
        } catch (e) {
          // Skip corrupted entries
        }
      }
    }
    
    return results;
  }
  
  /// Get all objects as a stream
  Stream<T> getAllStream() async* {
    final prefix = '$_collectionName:';
    final stream = _db.scanStream(startKey: prefix);
    
    await for (final item in stream) {
      if (item.key.startsWith(prefix)) {
        try {
          final object = adapter.deserialize(item.value);
          yield object;
        } catch (e) {
          // Skip corrupted entries
        }
      }
    }
  }
  
  /// Count objects in the collection
  Future<int> count() async {
    final prefix = '$_collectionName:';
    return _db.count(startKey: prefix);
  }
  
  /// Check if the collection is empty
  Future<bool> get isEmpty async => (await count()) == 0;
  
  /// Check if the collection is not empty
  Future<bool> get isNotEmpty async => (await count()) > 0;
  
  /// Clear all objects from the collection
  Future<void> clear() async {
    final keys = await _db.getKeys(startKey: '$_collectionName:');
    final batch = _db.batch();
    
    for (final key in keys) {
      if (key.startsWith('$_collectionName:')) {
        batch.delete(key);
      }
    }
    
    await batch.commit();
    
    _changeController.add(CollectionChange<T>(
      type: ChangeType.clear,
      key: '',
    ));
  }
  
  /// Create a query builder for this collection
  QueryBuilder<T> query() {
    return QueryBuilder<T>(
      collection: this,
      db: _db,
      adapter: adapter,
    );
  }
  
  /// Watch for changes to a specific object
  Stream<T?> watch(String key) async* {
    // Emit initial value
    yield await findByKey(key);
    
    // Then emit changes
    yield* changes
        .where((change) => change.key == key)
        .map((change) => change.newValue);
  }
  
  /// Watch all objects in the collection
  Stream<List<T>> watchAll() async* {
    // Emit initial value
    yield await getAll();
    
    // Then emit on changes
    await for (final _ in changes) {
      yield await getAll();
    }
  }
  
  /// Create a reactive collection
  ReactiveCollection<T> reactive() {
    return ReactiveCollection<T>(this);
  }
  
  /// Dispose of resources
  void dispose() {
    _changeController.close();
  }
}

/// Represents a change in the collection
class CollectionChange<T> {
  final ChangeType type;
  final String key;
  final T? oldValue;
  final T? newValue;
  
  CollectionChange({
    required this.type,
    required this.key,
    this.oldValue,
    this.newValue,
  });
}

/// Type of change that occurred
enum ChangeType {
  insert,
  update,
  delete,
  clear,
}

/// Stream extension for collection changes
extension CollectionChangeStream<T> on Stream<CollectionChange<T>> {
  /// Filter only insertions
  Stream<T> whereInserted() {
    return where((change) => change.type == ChangeType.insert)
        .map((change) => change.newValue!)
        .where((value) => value != null);
  }
  
  /// Filter only updates
  Stream<T> whereUpdated() {
    return where((change) => change.type == ChangeType.update)
        .map((change) => change.newValue!)
        .where((value) => value != null);
  }
  
  /// Filter only deletions
  Stream<T> whereDeleted() {
    return where((change) => change.type == ChangeType.delete)
        .map((change) => change.oldValue!)
        .where((value) => value != null);
  }
  
  /// Start with an initial value
  Stream<T?> startWithValue(Future<T?> initialValue) async* {
    yield await initialValue;
    yield* map((change) => change.newValue);
  }
}