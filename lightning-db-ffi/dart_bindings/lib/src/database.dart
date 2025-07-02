import 'dart:ffi' as ffi;
import 'dart:typed_data';
import 'package:ffi/ffi.dart';
import 'package:meta/meta.dart';

import 'ffi_bindings.dart' as bindings;
import 'errors.dart';

/// Configuration for Lightning DB
class LightningDBConfig {
  final int? pageSize;
  final int? cacheSize;
  
  const LightningDBConfig({
    this.pageSize,
    this.cacheSize,
  });
}

/// High-performance embedded database for Dart
class LightningDB {
  final bindings.LightningDBBindings _bindings;
  final ffi.Pointer<bindings.LightningDB> _db;
  final String path;
  bool _closed = false;
  
  LightningDB._(this._bindings, this._db, this.path);
  
  /// Create a new database at the specified path
  factory LightningDB.create(
    String path, {
    LightningDBConfig? config,
    String? libraryPath,
  }) {
    final bindingsInstance = bindings.LightningDBBindings(libraryPath: libraryPath);
    
    // Create config if provided
    ffi.Pointer<bindings.LightningDBConfig> configPtr = ffi.nullptr;
    if (config != null) {
      configPtr = bindingsInstance.lightning_db_config_new();
      if (configPtr == ffi.nullptr) {
        throw const LightningDBException(-1, 'Failed to create config');
      }
      
      if (config.pageSize != null) {
        final err = bindingsInstance.lightning_db_config_set_page_size(configPtr, config.pageSize!);
        if (err != 0) {
          bindingsInstance.lightning_db_config_free(configPtr);
          throw LightningDBException.fromCode(err, 'Failed to set page size');
        }
      }
      
      if (config.cacheSize != null) {
        final err = bindingsInstance.lightning_db_config_set_cache_size(configPtr, config.cacheSize!);
        if (err != 0) {
          bindingsInstance.lightning_db_config_free(configPtr);
          throw LightningDBException.fromCode(err, 'Failed to set cache size');
        }
      }
    }
    
    // Create database
    final pathPtr = path.toNativeUtf8();
    final db = bindingsInstance.lightning_db_create(pathPtr.cast(), configPtr);
    malloc.free(pathPtr);
    
    if (configPtr != ffi.nullptr) {
      bindingsInstance.lightning_db_config_free(configPtr);
    }
    
    if (db == ffi.nullptr) {
      throw const LightningDBException(-1, 'Failed to create database');
    }
    
    return LightningDB._(bindingsInstance, db, path);
  }
  
  /// Open an existing database
  factory LightningDB.open(
    String path, {
    LightningDBConfig? config,
    String? libraryPath,
  }) {
    final bindingsInstance = bindings.LightningDBBindings(libraryPath: libraryPath);
    
    // Create config if provided
    ffi.Pointer<bindings.LightningDBConfig> configPtr = ffi.nullptr;
    if (config != null) {
      configPtr = bindingsInstance.lightning_db_config_new();
      if (configPtr == ffi.nullptr) {
        throw const LightningDBException(-1, 'Failed to create config');
      }
      
      if (config.pageSize != null) {
        final err = bindingsInstance.lightning_db_config_set_page_size(configPtr, config.pageSize!);
        if (err != 0) {
          bindingsInstance.lightning_db_config_free(configPtr);
          throw LightningDBException.fromCode(err, 'Failed to set page size');
        }
      }
      
      if (config.cacheSize != null) {
        final err = bindingsInstance.lightning_db_config_set_cache_size(configPtr, config.cacheSize!);
        if (err != 0) {
          bindingsInstance.lightning_db_config_free(configPtr);
          throw LightningDBException.fromCode(err, 'Failed to set cache size');
        }
      }
    }
    
    // Open database
    final pathPtr = path.toNativeUtf8();
    final db = bindingsInstance.lightning_db_open(pathPtr.cast(), configPtr);
    malloc.free(pathPtr);
    
    if (configPtr != ffi.nullptr) {
      bindingsInstance.lightning_db_config_free(configPtr);
    }
    
    if (db == ffi.nullptr) {
      throw const LightningDBException(-1, 'Failed to open database');
    }
    
    return LightningDB._(bindingsInstance, db, path);
  }
  
  /// Store a key-value pair
  void put(Uint8List key, Uint8List value) {
    _checkClosed();
    
    final keyPtr = malloc<ffi.Uint8>(key.length);
    final valuePtr = malloc<ffi.Uint8>(value.length);
    
    try {
      keyPtr.asTypedList(key.length).setAll(0, key);
      valuePtr.asTypedList(value.length).setAll(0, value);
      
      final err = _bindings.lightning_db_put(
        _db,
        keyPtr,
        key.length,
        valuePtr,
        value.length,
      );
      
      if (err != 0) {
        throw LightningDBException.fromCode(err, 'Put operation failed');
      }
    } finally {
      malloc.free(keyPtr);
      malloc.free(valuePtr);
    }
  }
  
  /// Retrieve a value by key
  Uint8List? get(Uint8List key) {
    _checkClosed();
    
    final keyPtr = malloc<ffi.Uint8>(key.length);
    final result = malloc<bindings.LightningDBResult>();
    
    try {
      keyPtr.asTypedList(key.length).setAll(0, key);
      
      final err = _bindings.lightning_db_get(_db, keyPtr, key.length, result);
      
      if (err != 0) {
        throw LightningDBException.fromCode(err, 'Get operation failed');
      }
      
      if (result.ref.data == ffi.nullptr || result.ref.len == 0) {
        return null;
      }
      
      final data = result.ref.data.asTypedList(result.ref.len);
      final copy = Uint8List.fromList(data);
      
      _bindings.lightning_db_free_result(result.ref.data, result.ref.len);
      
      return copy;
    } finally {
      malloc.free(keyPtr);
      malloc.free(result);
    }
  }
  
  /// Delete a key-value pair
  void delete(Uint8List key) {
    _checkClosed();
    
    final keyPtr = malloc<ffi.Uint8>(key.length);
    
    try {
      keyPtr.asTypedList(key.length).setAll(0, key);
      
      final err = _bindings.lightning_db_delete(_db, keyPtr, key.length);
      
      if (err != 0) {
        throw LightningDBException.fromCode(err, 'Delete operation failed');
      }
    } finally {
      malloc.free(keyPtr);
    }
  }
  
  /// Begin a new transaction
  int beginTransaction() {
    _checkClosed();
    
    final txIdPtr = malloc<ffi.Uint64>();
    
    try {
      final err = _bindings.lightning_db_begin_transaction(_db, txIdPtr);
      
      if (err != 0) {
        throw LightningDBException.fromCode(err, 'Failed to begin transaction');
      }
      
      return txIdPtr.value;
    } finally {
      malloc.free(txIdPtr);
    }
  }
  
  /// Put a key-value pair within a transaction
  void putInTransaction(int txId, Uint8List key, Uint8List value) {
    _checkClosed();
    
    final keyPtr = malloc<ffi.Uint8>(key.length);
    final valuePtr = malloc<ffi.Uint8>(value.length);
    
    try {
      keyPtr.asTypedList(key.length).setAll(0, key);
      valuePtr.asTypedList(value.length).setAll(0, value);
      
      final err = _bindings.lightning_db_put_tx(
        _db,
        txId,
        keyPtr,
        key.length,
        valuePtr,
        value.length,
      );
      
      if (err != 0) {
        throw LightningDBException.fromCode(err, 'Transaction put failed');
      }
    } finally {
      malloc.free(keyPtr);
      malloc.free(valuePtr);
    }
  }
  
  /// Commit a transaction
  void commitTransaction(int txId) {
    _checkClosed();
    
    final err = _bindings.lightning_db_commit_transaction(_db, txId);
    
    if (err != 0) {
      throw LightningDBException.fromCode(err, 'Failed to commit transaction');
    }
  }
  
  /// Abort a transaction
  void abortTransaction(int txId) {
    _checkClosed();
    
    final err = _bindings.lightning_db_abort_transaction(_db, txId);
    
    if (err != 0) {
      throw LightningDBException.fromCode(err, 'Failed to abort transaction');
    }
  }
  
  /// Force a checkpoint (flush to disk)
  void checkpoint() {
    _checkClosed();
    
    final err = _bindings.lightning_db_checkpoint(_db);
    
    if (err != 0) {
      throw LightningDBException.fromCode(err, 'Checkpoint failed');
    }
  }
  
  /// Close the database
  void close() {
    if (!_closed) {
      _bindings.lightning_db_free(_db);
      _closed = true;
    }
  }
  
  void _checkClosed() {
    if (_closed) {
      throw const LightningDBException(-1, 'Database is closed');
    }
  }
  
  /// Convert error code to exception with message
  LightningDBException _createException(int errorCode) {
    final msgPtr = _bindings.lightning_db_error_string(errorCode);
    final message = msgPtr.cast<Utf8>().toDartString();
    return LightningDBException.fromCode(errorCode, message);
  }
}