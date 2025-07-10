import 'dart:ffi';
import 'dart:typed_data';
import 'init.dart';
import 'errors.dart';
import 'database.dart';
import 'native/lightning_db_bindings.dart';

/// Key-value pair returned by iterator
class KeyValue {
  final String key;
  final Uint8List value;
  
  const KeyValue(this.key, this.value);
  
  /// Get the value as a string
  String get valueAsString => String.fromCharCodes(value);
  
  @override
  String toString() => 'KeyValue(key: $key, value: ${value.length} bytes)';
}

/// Iterator for scanning key ranges in the database
class Iterator {
  final int _handle;
  bool _closed = false;
  
  Iterator(this._handle);
  
  /// Whether the iterator is closed
  bool get isClosed => _closed;
  
  /// Get the next key-value pair
  /// Returns null when there are no more items
  Future<KeyValue?> next() async {
    _checkNotClosed();
    
    final result = LightningDbInit.bindings.lightning_db_iterator_next(_handle);
    
    // Check for end of iteration
    if (result.ERROR_CODE == ErrorCode.ErrorCodeKeyNotFound || 
        result.KEY == nullptr || 
        result.VALUE == nullptr) {
      return null;
    }
    
    ErrorHandler.checkResult(result.ERROR_CODE, 'Iterator next');
    
    // Copy key and value before freeing
    final key = String.fromCharCodes(
      result.KEY.asTypedList(result.KEY_LEN)
    );
    final value = Uint8List.fromList(
      result.VALUE.asTypedList(result.VALUE_LEN)
    );
    
    // Free the result
    LightningDbInit.bindings.lightning_db_free_key_value(result);
    
    return KeyValue(key, value);
  }
  
  /// Close the iterator
  Future<void> close() async {
    if (_closed) return;
    
    final result = LightningDbInit.bindings.lightning_db_iterator_close(_handle);
    ErrorHandler.checkResult(result, 'Close iterator');
    
    _closed = true;
  }
  
  /// Convert the iterator to a stream
  Stream<KeyValue> toStream() async* {
    try {
      while (!_closed) {
        final kv = await next();
        if (kv == null) break;
        yield kv;
      }
    } finally {
      await close();
    }
  }
  
  /// Collect all remaining items into a list
  Future<List<KeyValue>> toList() async {
    final items = <KeyValue>[];
    
    try {
      while (!_closed) {
        final kv = await next();
        if (kv == null) break;
        items.add(kv);
      }
    } finally {
      await close();
    }
    
    return items;
  }
  
  /// Take a limited number of items
  Future<List<KeyValue>> take(int count) async {
    final items = <KeyValue>[];
    
    try {
      for (int i = 0; i < count && !_closed; i++) {
        final kv = await next();
        if (kv == null) break;
        items.add(kv);
      }
    } finally {
      await close();
    }
    
    return items;
  }
  
  void _checkNotClosed() {
    if (_closed) {
      throw StateError('Iterator is closed');
    }
  }
}

/// Extension methods for database to work with iterators
extension IteratorExtensions on LightningDb {
  /// Scan and return results as a stream
  Stream<KeyValue> scanStream({
    String? startKey,
    String? endKey,
  }) async* {
    final iterator = await scan(startKey: startKey, endKey: endKey);
    yield* iterator.toStream();
  }
  
  /// Get all key-value pairs in a range
  Future<List<KeyValue>> getRange({
    String? startKey,
    String? endKey,
  }) async {
    final iterator = await scan(startKey: startKey, endKey: endKey);
    return iterator.toList();
  }
  
  /// Get all keys in a range
  Future<List<String>> getKeys({
    String? startKey,
    String? endKey,
  }) async {
    final items = await getRange(startKey: startKey, endKey: endKey);
    return items.map((kv) => kv.key).toList();
  }
  
  /// Count items in a range
  Future<int> count({
    String? startKey,
    String? endKey,
  }) async {
    final iterator = await scan(startKey: startKey, endKey: endKey);
    int count = 0;
    
    try {
      while (await iterator.next() != null) {
        count++;
      }
    } finally {
      await iterator.close();
    }
    
    return count;
  }
}