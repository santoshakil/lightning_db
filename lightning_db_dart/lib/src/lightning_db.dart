import 'dart:ffi' as ffi;
import 'dart:io';
import 'dart:typed_data';
import 'package:ffi/ffi.dart';
import '../lightning_db_bindings_generated.dart' as bindings;
import 'transaction.dart';
import 'iterator.dart';
import 'consistency_level.dart' as ldb;
import 'compression_type.dart' as ldb;
import 'wal_sync_mode.dart' as ldb;

/// Lightning DB database instance
class LightningDb {
  static bindings.LightningDbBindings? _bindings;
  static bindings.LightningDbBindings get _b {
    _bindings ??= bindings.LightningDbBindings(_loadLibrary());
    return _bindings!;
  }

  final int _handle;
  bool _isClosed = false;

  LightningDb._(this._handle);

  /// Create a new Lightning DB database
  static Future<LightningDb> create(String path) async {
    final pathPtr = path.toNativeUtf8();
    final handlePtr = calloc<ffi.Uint64>();
    
    try {
      final result = _b.lightning_db_create(pathPtr.cast(), handlePtr);
      if (result != 0) {
        throw LightningDbException.fromErrorCode(result);
      }
      
      return LightningDb._(handlePtr.value);
    } finally {
      calloc.free(pathPtr);
      calloc.free(handlePtr);
    }
  }

  /// Open an existing Lightning DB database
  static Future<LightningDb> open(String path) async {
    final pathPtr = path.toNativeUtf8();
    final handlePtr = calloc<ffi.Uint64>();
    
    try {
      final result = _b.lightning_db_open(pathPtr.cast(), handlePtr);
      if (result != 0) {
        throw LightningDbException.fromErrorCode(result);
      }
      
      return LightningDb._(handlePtr.value);
    } finally {
      calloc.free(pathPtr);
      calloc.free(handlePtr);
    }
  }

  /// Create a database with custom configuration
  static Future<LightningDb> createWithConfig({
    required String path,
    int? cacheSize,
    ldb.CompressionType? compressionType,
    ldb.WalSyncMode? walSyncMode,
  }) async {
    final pathPtr = path.toNativeUtf8();
    final handlePtr = calloc<ffi.Uint64>();
    
    try {
      final result = _b.lightning_db_create_with_config(
        pathPtr.cast(),
        cacheSize ?? 67108864, // 64MB default
        _mapCompressionType(compressionType),
        _mapWalSyncMode(walSyncMode),
        handlePtr,
      );
      
      if (result != 0) {
        throw LightningDbException.fromErrorCode(result);
      }
      
      return LightningDb._(handlePtr.value);
    } finally {
      calloc.free(pathPtr);
      calloc.free(handlePtr);
    }
  }

  /// Put a key-value pair into the database
  Future<void> put(Uint8List key, Uint8List value) async {
    _checkNotClosed();
    
    final keyPtr = _allocateBytes(key);
    final valuePtr = _allocateBytes(value);
    
    try {
      final result = _b.lightning_db_put(
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

  /// Put with consistency level
  Future<void> putWithConsistency(
    Uint8List key,
    Uint8List value,
    ldb.ConsistencyLevel consistency,
  ) async {
    _checkNotClosed();
    
    final keyPtr = _allocateBytes(key);
    final valuePtr = _allocateBytes(value);
    
    try {
      final result = _b.lightning_db_put_with_consistency(
        _handle,
        keyPtr,
        key.length,
        valuePtr,
        value.length,
        _mapConsistencyLevel(consistency),
      );
      
      if (result != 0) {
        throw LightningDbException.fromErrorCode(result);
      }
    } finally {
      calloc.free(keyPtr);
      calloc.free(valuePtr);
    }
  }

  /// Get a value from the database
  Future<Uint8List?> get(Uint8List key) async {
    _checkNotClosed();
    
    final keyPtr = _allocateBytes(key);
    
    try {
      final result = _b.lightning_db_get(_handle, keyPtr, key.length);
      
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
      _b.lightning_db_free_bytes(result.DATA, result.LEN);
      
      return data;
    } finally {
      calloc.free(keyPtr);
    }
  }

  /// Get with consistency level
  Future<Uint8List?> getWithConsistency(
    Uint8List key,
    ldb.ConsistencyLevel consistency,
  ) async {
    _checkNotClosed();
    
    final keyPtr = _allocateBytes(key);
    
    try {
      final result = _b.lightning_db_get_with_consistency(
        _handle,
        keyPtr,
        key.length,
        _mapConsistencyLevel(consistency),
      );
      
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
      _b.lightning_db_free_bytes(result.DATA, result.LEN);
      
      return data;
    } finally {
      calloc.free(keyPtr);
    }
  }

  /// Delete a key from the database
  Future<bool> delete(Uint8List key) async {
    _checkNotClosed();
    
    final keyPtr = _allocateBytes(key);
    
    try {
      final result = _b.lightning_db_delete(_handle, keyPtr, key.length);
      
      if (result < 0) {
        throw LightningDbException.fromErrorCode(-result);
      }
      
      return result == 0; // 0 = deleted, 1 = not found
    } finally {
      calloc.free(keyPtr);
    }
  }

  /// Begin a new transaction
  Future<Transaction> beginTransaction() async {
    _checkNotClosed();
    
    final handlePtr = calloc<ffi.Uint64>();
    
    try {
      final result = _b.lightning_db_begin_transaction(_handle, handlePtr);
      
      if (result != 0) {
        throw LightningDbException.fromErrorCode(result);
      }
      
      return Transaction(handlePtr.value, _b);
    } finally {
      calloc.free(handlePtr);
    }
  }

  /// Create an iterator for scanning a range of keys
  Future<DbIterator> scan({Uint8List? start, Uint8List? end}) async {
    _checkNotClosed();
    
    final handlePtr = calloc<ffi.Uint64>();
    ffi.Pointer<ffi.Uint8>? startPtr;
    ffi.Pointer<ffi.Uint8>? endPtr;
    
    try {
      if (start != null) {
        startPtr = _allocateBytes(start);
      }
      if (end != null) {
        endPtr = _allocateBytes(end);
      }
      
      final result = _b.lightning_db_scan(
        _handle,
        startPtr ?? ffi.nullptr,
        start?.length ?? 0,
        endPtr ?? ffi.nullptr,
        end?.length ?? 0,
        handlePtr,
      );
      
      if (result != 0) {
        throw LightningDbException.fromErrorCode(result);
      }
      
      return DbIterator(handlePtr.value, _b);
    } finally {
      calloc.free(handlePtr);
      if (startPtr != null) calloc.free(startPtr);
      if (endPtr != null) calloc.free(endPtr);
    }
  }

  /// Sync the database to disk
  Future<void> sync() async {
    _checkNotClosed();
    
    final result = _b.lightning_db_sync(_handle);
    if (result != 0) {
      throw LightningDbException.fromErrorCode(result);
    }
  }

  /// Checkpoint the database
  Future<void> checkpoint() async {
    _checkNotClosed();
    
    final result = _b.lightning_db_checkpoint(_handle);
    if (result != 0) {
      throw LightningDbException.fromErrorCode(result);
    }
  }

  /// Close the database
  Future<void> close() async {
    if (_isClosed) return;
    
    final result = _b.lightning_db_close(_handle);
    if (result != 0) {
      throw LightningDbException.fromErrorCode(result);
    }
    
    _isClosed = true;
  }

  void _checkNotClosed() {
    if (_isClosed) {
      throw LightningDbException('Database is closed');
    }
  }

  static ffi.Pointer<ffi.Uint8> _allocateBytes(Uint8List bytes) {
    final ptr = calloc<ffi.Uint8>(bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      ptr[i] = bytes[i];
    }
    return ptr;
  }

  static ffi.DynamicLibrary _loadLibrary() {
    if (Platform.isMacOS) {
      return ffi.DynamicLibrary.open('liblightning_db_ffi.dylib');
    } else if (Platform.isLinux) {
      return ffi.DynamicLibrary.open('liblightning_db_ffi.so');
    } else if (Platform.isWindows) {
      return ffi.DynamicLibrary.open('lightning_db_ffi.dll');
    } else if (Platform.isIOS) {
      return ffi.DynamicLibrary.process();
    } else if (Platform.isAndroid) {
      return ffi.DynamicLibrary.open('liblightning_db_ffi.so');
    } else {
      throw UnsupportedError('Unsupported platform: ${Platform.operatingSystem}');
    }
  }
}

/// Lightning DB exception
class LightningDbException implements Exception {
  final String message;
  final int? errorCode;

  LightningDbException(this.message, {this.errorCode});

  factory LightningDbException.fromErrorCode(int code) {
    final errorMsg = LightningDb._b.lightning_db_get_last_error();
    String message;
    
    if (errorMsg != ffi.nullptr) {
      try {
        message = errorMsg.cast<Utf8>().toDartString();
      } catch (e) {
        message = _getErrorMessage(code);
      }
    } else {
      message = _getErrorMessage(code);
    }
    
    return LightningDbException(message, errorCode: code);
  }

  static String _getErrorMessage(int code) {
    switch (code) {
      case 1:
        return 'Invalid argument';
      case 2:
        return 'Database not found';
      case 3:
        return 'Database exists';
      case 4:
        return 'Transaction not found';
      case 5:
        return 'Transaction conflict';
      case 6:
        return 'Key not found';
      case 7:
        return 'IO error';
      case 8:
        return 'Corrupted data';
      case 9:
        return 'Out of memory';
      default:
        return 'Unknown error (code: $code)';
    }
  }

  @override
  String toString() => 'LightningDbException: $message';
}

// Helper functions to map between custom and generated enums
bindings.CompressionType _mapCompressionType(ldb.CompressionType? type) {
  if (type == null) return bindings.CompressionType.CompressionTypeNone;
  switch (type) {
    case ldb.CompressionType.none:
      return bindings.CompressionType.CompressionTypeNone;
    case ldb.CompressionType.zstd:
      return bindings.CompressionType.CompressionTypeZstd;
    case ldb.CompressionType.lz4:
      return bindings.CompressionType.CompressionTypeLz4;
    case ldb.CompressionType.snappy:
      return bindings.CompressionType.CompressionTypeSnappy;
  }
}

bindings.WalSyncMode _mapWalSyncMode(ldb.WalSyncMode? mode) {
  if (mode == null) return bindings.WalSyncMode.WalSyncModePeriodic;
  switch (mode) {
    case ldb.WalSyncMode.sync:
      return bindings.WalSyncMode.WalSyncModeSync;
    case ldb.WalSyncMode.periodic:
      return bindings.WalSyncMode.WalSyncModePeriodic;
    case ldb.WalSyncMode.async:
      return bindings.WalSyncMode.WalSyncModeAsync;
  }
}

bindings.ConsistencyLevel _mapConsistencyLevel(ldb.ConsistencyLevel level) {
  switch (level) {
    case ldb.ConsistencyLevel.eventual:
      return bindings.ConsistencyLevel.ConsistencyLevelEventual;
    case ldb.ConsistencyLevel.strong:
      return bindings.ConsistencyLevel.ConsistencyLevelStrong;
  }
}

class ErrorCode {
  static const int Success = 0;
  static const int InvalidArgument = 1;
  static const int DatabaseNotFound = 2;
  static const int DatabaseExists = 3;
  static const int TransactionNotFound = 4;
  static const int TransactionConflict = 5;
  static const int KeyNotFound = 6;
  static const int IoError = 7;
  static const int CorruptedData = 8;
  static const int OutOfMemory = 9;
  static const int Unknown = 999;
}