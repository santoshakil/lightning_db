import 'dart:ffi';
import 'dart:typed_data';
import 'package:ffi/ffi.dart';
import 'init.dart';
import 'errors.dart';
import 'database.dart';
import 'native/lightning_db_bindings.dart';

/// Represents a database transaction
class Transaction {
  final int _handle;
  final LightningDb _database;
  bool _finished = false;
  
  Transaction(this._handle, this._database);
  
  /// Whether the transaction has been committed or aborted
  bool get isFinished => _finished;
  
  /// Put a key-value pair within the transaction
  Future<void> put(String key, Uint8List value) async {
    _checkNotFinished();
    
    final keyBytes = Uint8List.fromList(key.codeUnits);
    final keyPtr = LightningDb.allocateBytes(keyBytes);
    final valuePtr = LightningDb.allocateBytes(value);
    
    try {
      final result = LightningDbInit.bindings.lightning_db_put_tx(
        _handle,
        keyPtr,
        keyBytes.length,
        valuePtr,
        value.length,
      );
      
      ErrorHandler.checkResult(result, 'Transaction put');
    } finally {
      calloc.free(keyPtr);
      calloc.free(valuePtr);
    }
  }
  
  /// Get a value within the transaction
  Future<Uint8List?> get(String key) async {
    _checkNotFinished();
    
    final keyBytes = Uint8List.fromList(key.codeUnits);
    final keyPtr = LightningDb.allocateBytes(keyBytes);
    
    try {
      final result = LightningDbInit.bindings.lightning_db_get_tx(
        _handle,
        keyPtr,
        keyBytes.length,
      );
      
      if (result.ERROR_CODE == ErrorCode.ErrorCodeKeyNotFound) {
        return null;
      }
      
      ErrorHandler.checkResult(result.ERROR_CODE, 'Transaction get');
      
      if (result.DATA == nullptr || result.LEN == 0) {
        return null;
      }
      
      // Copy data before freeing
      final data = Uint8List.fromList(
        result.DATA.asTypedList(result.LEN)
      );
      
      // Free the result
      LightningDbInit.bindings.lightning_db_free_bytes(result);
      
      return data;
    } finally {
      calloc.free(keyPtr);
    }
  }
  
  /// Delete a key within the transaction
  Future<bool> delete(String key) async {
    _checkNotFinished();
    
    final keyBytes = Uint8List.fromList(key.codeUnits);
    final keyPtr = LightningDb.allocateBytes(keyBytes);
    
    try {
      final result = LightningDbInit.bindings.lightning_db_delete_tx(
        _handle,
        keyPtr,
        keyBytes.length,
      );
      
      if (result == 1) {
        // Key not found
        return false;
      }
      
      ErrorHandler.checkResult(result, 'Transaction delete');
      return true;
    } finally {
      calloc.free(keyPtr);
    }
  }
  
  /// Commit the transaction
  Future<void> commit() async {
    _checkNotFinished();
    
    final result = LightningDbInit.bindings.lightning_db_commit_transaction(_handle);
    ErrorHandler.checkResult(result, 'Commit transaction');
    
    _finished = true;
  }
  
  /// Abort/rollback the transaction
  Future<void> rollback() async {
    if (_finished) return;
    
    final result = LightningDbInit.bindings.lightning_db_abort_transaction(_handle);
    ErrorHandler.checkResult(result, 'Abort transaction');
    
    _finished = true;
  }
  
  /// Alias for rollback
  Future<void> abort() => rollback();
  
  /// Put a string value
  Future<void> putString(String key, String value) async {
    await put(key, Uint8List.fromList(value.codeUnits));
  }
  
  /// Get a string value
  Future<String?> getString(String key) async {
    final bytes = await get(key);
    if (bytes == null) return null;
    return String.fromCharCodes(bytes);
  }
  
  void _checkNotFinished() {
    if (_finished) {
      throw StateError('Transaction is already finished');
    }
    _database.checkNotClosed();
  }
}