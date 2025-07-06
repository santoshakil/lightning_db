import 'dart:ffi' as ffi;
import 'dart:typed_data';
import '../lightning_db_bindings_generated.dart';
import 'lightning_db.dart';

/// Database iterator for range scans
class DbIterator {
  final int _handle;
  final LightningDbBindings _bindings;
  bool _isClosed = false;

  DbIterator(this._handle, this._bindings);

  /// Get the next key-value pair
  Future<(Uint8List key, Uint8List value)?> next() async {
    if (_isClosed) {
      return null;
    }
    
    final result = _bindings.lightning_db_iterator_next(_handle);
    
    // Check if iterator is exhausted
    if (result.error_code == 0 && result.key == ffi.nullptr) {
      await close();
      return null;
    }
    
    if (result.error_code != 0) {
      throw LightningDbException._fromErrorCode(result.error_code);
    }
    
    try {
      // Copy the data before freeing
      final key = Uint8List.fromList(
        result.key.asTypedList(result.key_len),
      );
      final value = Uint8List.fromList(
        result.value.asTypedList(result.value_len),
      );
      
      return (key, value);
    } finally {
      // Free the result
      _bindings.lightning_db_free_key_value(result);
    }
  }

  /// Close the iterator
  Future<void> close() async {
    if (_isClosed) return;
    
    final result = _bindings.lightning_db_iterator_close(_handle);
    if (result != 0) {
      throw LightningDbException._fromErrorCode(result);
    }
    
    _isClosed = true;
  }

  /// Convert iterator to a stream
  Stream<(Uint8List key, Uint8List value)> toStream() async* {
    while (!_isClosed) {
      final item = await next();
      if (item == null) break;
      yield item;
    }
  }
}