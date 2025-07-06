import 'dart:typed_data';
import 'database.dart';
import 'types.dart';

/// Represents a batch operation
abstract class BatchOperation {
  Future<void> execute(LightningDb db);
}

/// Put operation for batch
class BatchPut extends BatchOperation {
  final String key;
  final Uint8List value;
  final ConsistencyLevel consistency;
  
  BatchPut(this.key, this.value, [this.consistency = ConsistencyLevel.eventual]);
  
  @override
  Future<void> execute(LightningDb db) => db.put(key, value, consistency);
}

/// Delete operation for batch
class BatchDelete extends BatchOperation {
  final String key;
  
  BatchDelete(this.key);
  
  @override
  Future<void> execute(LightningDb db) => db.delete(key);
}

/// Batch for executing multiple operations atomically
class Batch {
  final LightningDb _database;
  final List<BatchOperation> _operations = [];
  bool _committed = false;
  
  Batch(this._database);
  
  /// Number of operations in the batch
  int get length => _operations.length;
  
  /// Whether the batch is empty
  bool get isEmpty => _operations.isEmpty;
  
  /// Whether the batch is not empty
  bool get isNotEmpty => _operations.isNotEmpty;
  
  /// Whether the batch has been committed
  bool get isCommitted => _committed;
  
  /// Add a put operation to the batch
  void put(String key, Uint8List value, [ConsistencyLevel consistency = ConsistencyLevel.eventual]) {
    _checkNotCommitted();
    _operations.add(BatchPut(key, value, consistency));
  }
  
  /// Add a put string operation to the batch
  void putString(String key, String value, [ConsistencyLevel consistency = ConsistencyLevel.eventual]) {
    put(key, Uint8List.fromList(value.codeUnits), consistency);
  }
  
  /// Add a delete operation to the batch
  void delete(String key) {
    _checkNotCommitted();
    _operations.add(BatchDelete(key));
  }
  
  /// Clear all operations from the batch
  void clear() {
    _checkNotCommitted();
    _operations.clear();
  }
  
  /// Commit the batch atomically using a transaction
  Future<void> commit() async {
    _checkNotCommitted();
    
    if (_operations.isEmpty) {
      _committed = true;
      return;
    }
    
    // Execute all operations in a transaction for atomicity
    final transaction = await _database.beginTransaction();
    
    try {
      for (final op in _operations) {
        if (op is BatchPut) {
          await transaction.put(op.key, op.value);
        } else if (op is BatchDelete) {
          await transaction.delete(op.key);
        }
      }
      
      await transaction.commit();
      _committed = true;
    } catch (e) {
      await transaction.rollback();
      rethrow;
    }
  }
  
  /// Execute the batch non-atomically (operations may partially succeed)
  /// This is faster but less safe than commit()
  Future<BatchResult> executeNonAtomic() async {
    _checkNotCommitted();
    
    final results = <String, bool>{};
    final errors = <String, Exception>{};
    
    for (final op in _operations) {
      try {
        await op.execute(_database);
        if (op is BatchPut) {
          results[op.key] = true;
        } else if (op is BatchDelete) {
          results[(op as BatchDelete).key] = true;
        }
      } catch (e) {
        final key = op is BatchPut ? op.key : (op as BatchDelete).key;
        results[key] = false;
        errors[key] = e as Exception;
      }
    }
    
    _committed = true;
    return BatchResult(results, errors);
  }
  
  void _checkNotCommitted() {
    if (_committed) {
      throw StateError('Batch has already been committed');
    }
  }
}

/// Result of a non-atomic batch execution
class BatchResult {
  /// Map of key to success status
  final Map<String, bool> results;
  
  /// Map of key to error for failed operations
  final Map<String, Exception> errors;
  
  BatchResult(this.results, this.errors);
  
  /// Whether all operations succeeded
  bool get allSucceeded => errors.isEmpty;
  
  /// Number of successful operations
  int get successCount => results.values.where((v) => v).length;
  
  /// Number of failed operations
  int get failureCount => errors.length;
  
  /// Total number of operations
  int get totalCount => results.length;
  
  /// Get the error for a specific key
  Exception? getError(String key) => errors[key];
  
  /// Check if a specific key's operation succeeded
  bool succeeded(String key) => results[key] ?? false;
}