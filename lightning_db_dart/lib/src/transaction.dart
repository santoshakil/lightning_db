import 'dart:ffi' as ffi;
import 'dart:typed_data';
import 'package:ffi/ffi.dart';
import '../lightning_db_bindings_generated.dart' as bindings;
import 'lightning_db.dart';

/// A database transaction
class Transaction {
  final int _handle;
  final bindings.LightningDbBindings _bindings;
  bool _isCommitted = false;
  bool _isRolledBack = false;

  Transaction(this._handle, this._bindings);

  /// Put a key-value pair within the transaction
  Future<void> put(Uint8List key, Uint8List value) async {
    _checkValid();
    
    final keyPtr = _allocateBytes(key);
    final valuePtr = _allocateBytes(value);
    
    try {
      final result = _bindings.lightning_db_put_tx(
        _handle,
        keyPtr,
        key.length,
        valuePtr,
        value.length,
      );
      
      if (result != 0) {
        throw LightningDbException.fromErrorCode(result);
      }
    } finally {
      calloc.free(keyPtr);
      calloc.free(valuePtr);
    }
  }

  /// Get a value within the transaction
  Future<Uint8List?> get(Uint8List key) async {
    _checkValid();
    
    final keyPtr = _allocateBytes(key);
    
    try {
      final result = _bindings.lightning_db_get_tx(_handle, keyPtr, key.length);
      
      if (result.ERROR_CODE == ErrorCode.KeyNotFound) {
        return null;
      }
      
      if (result.ERROR_CODE != 0) {
        throw LightningDbException.fromErrorCode(result.ERROR_CODE);
      }
      
      if (result.DATA == ffi.nullptr || result.LEN == 0) {
        return null;
      }
      
      // Copy the data before freeing
      final data = Uint8List.fromList(
        result.DATA.asTypedList(result.LEN),
      );
      
      // Free the result
      _bindings.lightning_db_free_bytes(result.DATA, result.LEN);
      
      return data;
    } finally {
      calloc.free(keyPtr);
    }
  }

  /// Delete a key within the transaction
  Future<bool> delete(Uint8List key) async {
    _checkValid();
    
    final keyPtr = _allocateBytes(key);
    
    try {
      final result = _bindings.lightning_db_delete_tx(_handle, keyPtr, key.length);
      
      if (result < 0) {
        throw LightningDbException.fromErrorCode(-result);
      }
      
      return result == 0; // 0 = deleted, 1 = not found
    } finally {
      calloc.free(keyPtr);
    }
  }

  /// Commit the transaction
  Future<void> commit() async {
    _checkValid();
    
    final result = _bindings.lightning_db_commit_transaction(_handle);
    if (result != 0) {
      throw LightningDbException.fromErrorCode(result);
    }
    
    _isCommitted = true;
  }

  /// Rollback the transaction
  Future<void> rollback() async {
    _checkValid();
    
    final result = _bindings.lightning_db_abort_transaction(_handle);
    if (result != 0) {
      throw LightningDbException.fromErrorCode(result);
    }
    
    _isRolledBack = true;
  }

  void _checkValid() {
    if (_isCommitted) {
      throw LightningDbException('Transaction is already committed');
    }
    if (_isRolledBack) {
      throw LightningDbException('Transaction is already rolled back');
    }
  }

  static ffi.Pointer<ffi.Uint8> _allocateBytes(Uint8List bytes) {
    final ptr = calloc<ffi.Uint8>(bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      ptr[i] = bytes[i];
    }
    return ptr;
  }
}