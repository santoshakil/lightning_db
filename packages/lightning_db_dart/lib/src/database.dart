import 'dart:ffi';
import 'dart:typed_data';
import 'package:ffi/ffi.dart';
import 'package:meta/meta.dart';
import 'init.dart';
import 'errors.dart';
import 'types.dart';
import 'transaction.dart';
import 'iterator.dart' as iter;
import 'batch.dart';
import 'native/lightning_db_bindings.dart' hide CompressionType, ConsistencyLevel, WalSyncMode;

/// Main Lightning DB database class
class LightningDb {
  final int _handle;
  final String _path;
  bool _closed = false;
  
  LightningDb._(this._handle, this._path);
  
  /// The path to the database
  String get path => _path;
  
  /// Whether the database is closed
  bool get isClosed => _closed;
  
  /// Create a new database at the specified path
  static Future<LightningDb> create(String path, [DatabaseConfig? config]) async {
    LightningDbInit.init();
    
    final pathPtr = path.toNativeUtf8();
    final handlePtr = calloc<Uint64>();
    
    try {
      final result = config == null
        ? LightningDbInit.bindings.lightning_db_create(pathPtr.cast(), handlePtr)
        : LightningDbInit.bindings.lightning_db_create_with_config(
            pathPtr.cast(),
            config.cacheSize,
            config.compressionType.value,
            config.walSyncMode.value,
            handlePtr,
          );
      
      ErrorHandler.checkResult(result, 'Create database');
      
      return LightningDb._(handlePtr.value, path);
    } finally {
      calloc.free(pathPtr);
      calloc.free(handlePtr);
    }
  }
  
  /// Open an existing database
  static Future<LightningDb> open(String path) async {
    LightningDbInit.init();
    
    final pathPtr = path.toNativeUtf8();
    final handlePtr = calloc<Uint64>();
    
    try {
      final result = LightningDbInit.bindings.lightning_db_open(
        pathPtr.cast(),
        handlePtr,
      );
      
      ErrorHandler.checkResult(result, 'Open database');
      
      return LightningDb._(handlePtr.value, path);
    } finally {
      calloc.free(pathPtr);
      calloc.free(handlePtr);
    }
  }
  
  /// Put a key-value pair into the database
  Future<void> put(
    String key, 
    Uint8List value, [
    ConsistencyLevel consistency = ConsistencyLevel.eventual,
  ]) async {
    checkNotClosed();
    
    final keyBytes = Uint8List.fromList(key.codeUnits);
    final keyPtr = allocateBytes(keyBytes);
    final valuePtr = allocateBytes(value);
    
    try {
      final result = consistency == ConsistencyLevel.eventual
        ? LightningDbInit.bindings.lightning_db_put(
            _handle,
            keyPtr,
            keyBytes.length,
            valuePtr,
            value.length,
          )
        : LightningDbInit.bindings.lightning_db_put_with_consistency(
            _handle,
            keyPtr,
            keyBytes.length,
            valuePtr,
            value.length,
            consistency.value,
          );
      
      ErrorHandler.checkResult(result, 'Put operation');
    } finally {
      calloc.free(keyPtr);
      calloc.free(valuePtr);
    }
  }
  
  /// Get a value from the database
  Future<Uint8List?> get(
    String key, [
    ConsistencyLevel consistency = ConsistencyLevel.eventual,
  ]) async {
    checkNotClosed();
    
    final keyBytes = Uint8List.fromList(key.codeUnits);
    final keyPtr = allocateBytes(keyBytes);
    
    try {
      final result = consistency == ConsistencyLevel.eventual
        ? LightningDbInit.bindings.lightning_db_get(
            _handle,
            keyPtr,
            keyBytes.length,
          )
        : LightningDbInit.bindings.lightning_db_get_with_consistency(
            _handle,
            keyPtr,
            keyBytes.length,
            consistency.value,
          );
      
      if (result.ERROR_CODE == ErrorCode.ErrorCodeKeyNotFound) {
        return null;
      }
      
      ErrorHandler.checkResult(result.ERROR_CODE, 'Get operation');
      
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
  
  /// Delete a key from the database
  Future<bool> delete(String key) async {
    checkNotClosed();
    
    final keyBytes = Uint8List.fromList(key.codeUnits);
    final keyPtr = allocateBytes(keyBytes);
    
    try {
      final result = LightningDbInit.bindings.lightning_db_delete(
        _handle,
        keyPtr,
        keyBytes.length,
      );
      
      if (result == 1) {
        // Key not found
        return false;
      }
      
      ErrorHandler.checkResult(result, 'Delete operation');
      return true;
    } finally {
      calloc.free(keyPtr);
    }
  }
  
  /// Begin a new transaction
  Future<Transaction> beginTransaction() async {
    checkNotClosed();
    
    final handlePtr = calloc<Uint64>();
    
    try {
      final result = LightningDbInit.bindings.lightning_db_begin_transaction(
        _handle,
        handlePtr,
      );
      
      ErrorHandler.checkResult(result, 'Begin transaction');
      
      return Transaction(handlePtr.value, this);
    } finally {
      calloc.free(handlePtr);
    }
  }
  
  /// Create an iterator for scanning key ranges
  Future<iter.Iterator> scan({
    String? startKey,
    String? endKey,
  }) async {
    checkNotClosed();
    
    final handlePtr = calloc<Uint64>();
    Pointer<Uint8>? startPtr;
    Pointer<Uint8>? endPtr;
    int startLen = 0;
    int endLen = 0;
    
    try {
      if (startKey != null) {
        final startBytes = Uint8List.fromList(startKey.codeUnits);
        startPtr = allocateBytes(startBytes);
        startLen = startBytes.length;
      }
      
      if (endKey != null) {
        final endBytes = Uint8List.fromList(endKey.codeUnits);
        endPtr = allocateBytes(endBytes);
        endLen = endBytes.length;
      }
      
      final result = LightningDbInit.bindings.lightning_db_scan(
        _handle,
        startPtr ?? nullptr,
        startLen,
        endPtr ?? nullptr,
        endLen,
        handlePtr,
      );
      
      ErrorHandler.checkResult(result, 'Create iterator');
      
      return iter.Iterator(handlePtr.value);
    } finally {
      calloc.free(handlePtr);
      if (startPtr != null) calloc.free(startPtr);
      if (endPtr != null) calloc.free(endPtr);
    }
  }
  
  /// Create a batch for multiple operations
  Batch batch() {
    checkNotClosed();
    return Batch(this);
  }
  
  /// Sync the database to disk
  Future<void> sync() async {
    checkNotClosed();
    
    final result = LightningDbInit.bindings.lightning_db_sync(_handle);
    ErrorHandler.checkResult(result, 'Sync database');
  }
  
  /// Create a checkpoint
  Future<void> checkpoint() async {
    checkNotClosed();
    
    final result = LightningDbInit.bindings.lightning_db_checkpoint(_handle);
    ErrorHandler.checkResult(result, 'Create checkpoint');
  }
  
  /// Close the database
  Future<void> close() async {
    if (_closed) return;
    
    final result = LightningDbInit.bindings.lightning_db_close(_handle);
    ErrorHandler.checkResult(result, 'Close database');
    
    _closed = true;
  }
  
  /// Check if a key exists
  Future<bool> exists(String key) async {
    final value = await get(key);
    return value != null;
  }
  
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
  
  void checkNotClosed() {
    if (_closed) {
      throw DatabaseClosedException();
    }
  }
  
  /// Allocate native memory for bytes
  static Pointer<Uint8> allocateBytes(Uint8List bytes) {
    final ptr = calloc<Uint8>(bytes.length);
    ptr.asTypedList(bytes.length).setAll(0, bytes);
    return ptr;
  }
  
  /// Destroy a database (delete all files)
  static Future<void> destroy(String path) async {
    // This would typically be implemented in the native library
    // For now, throw an unimplemented error
    throw UnimplementedError('Database destruction not yet implemented');
  }
}