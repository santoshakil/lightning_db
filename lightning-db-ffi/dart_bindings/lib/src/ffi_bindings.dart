// ignore_for_file: non_constant_identifier_names, camel_case_types

import 'dart:ffi' as ffi;
import 'dart:io' show Platform;

// FFI types
typedef lightning_db_config_new_native = ffi.Pointer<LightningDBConfig> Function();
typedef lightning_db_config_new_dart = ffi.Pointer<LightningDBConfig> Function();

typedef lightning_db_config_set_page_size_native = ffi.Int32 Function(
    ffi.Pointer<LightningDBConfig> config, ffi.Size page_size);
typedef lightning_db_config_set_page_size_dart = int Function(
    ffi.Pointer<LightningDBConfig> config, int page_size);

typedef lightning_db_config_set_cache_size_native = ffi.Int32 Function(
    ffi.Pointer<LightningDBConfig> config, ffi.Size cache_size);
typedef lightning_db_config_set_cache_size_dart = int Function(
    ffi.Pointer<LightningDBConfig> config, int cache_size);

typedef lightning_db_config_free_native = ffi.Void Function(
    ffi.Pointer<LightningDBConfig> config);
typedef lightning_db_config_free_dart = void Function(
    ffi.Pointer<LightningDBConfig> config);

typedef lightning_db_open_native = ffi.Pointer<LightningDB> Function(
    ffi.Pointer<ffi.Char> path, ffi.Pointer<LightningDBConfig> config);
typedef lightning_db_open_dart = ffi.Pointer<LightningDB> Function(
    ffi.Pointer<ffi.Char> path, ffi.Pointer<LightningDBConfig> config);

typedef lightning_db_create_native = ffi.Pointer<LightningDB> Function(
    ffi.Pointer<ffi.Char> path, ffi.Pointer<LightningDBConfig> config);
typedef lightning_db_create_dart = ffi.Pointer<LightningDB> Function(
    ffi.Pointer<ffi.Char> path, ffi.Pointer<LightningDBConfig> config);

typedef lightning_db_put_native = ffi.Int32 Function(
    ffi.Pointer<LightningDB> db,
    ffi.Pointer<ffi.Uint8> key,
    ffi.Size key_len,
    ffi.Pointer<ffi.Uint8> value,
    ffi.Size value_len);
typedef lightning_db_put_dart = int Function(
    ffi.Pointer<LightningDB> db,
    ffi.Pointer<ffi.Uint8> key,
    int key_len,
    ffi.Pointer<ffi.Uint8> value,
    int value_len);

typedef lightning_db_get_native = ffi.Int32 Function(
    ffi.Pointer<LightningDB> db,
    ffi.Pointer<ffi.Uint8> key,
    ffi.Size key_len,
    ffi.Pointer<LightningDBResult> result);
typedef lightning_db_get_dart = int Function(
    ffi.Pointer<LightningDB> db,
    ffi.Pointer<ffi.Uint8> key,
    int key_len,
    ffi.Pointer<LightningDBResult> result);

typedef lightning_db_delete_native = ffi.Int32 Function(
    ffi.Pointer<LightningDB> db, ffi.Pointer<ffi.Uint8> key, ffi.Size key_len);
typedef lightning_db_delete_dart = int Function(
    ffi.Pointer<LightningDB> db, ffi.Pointer<ffi.Uint8> key, int key_len);

typedef lightning_db_free_result_native = ffi.Void Function(
    ffi.Pointer<ffi.Uint8> data, ffi.Size len);
typedef lightning_db_free_result_dart = void Function(
    ffi.Pointer<ffi.Uint8> data, int len);

typedef lightning_db_checkpoint_native = ffi.Int32 Function(
    ffi.Pointer<LightningDB> db);
typedef lightning_db_checkpoint_dart = int Function(ffi.Pointer<LightningDB> db);

typedef lightning_db_free_native = ffi.Void Function(ffi.Pointer<LightningDB> db);
typedef lightning_db_free_dart = void Function(ffi.Pointer<LightningDB> db);

typedef lightning_db_error_string_native = ffi.Pointer<ffi.Char> Function(
    ffi.Int32 error);
typedef lightning_db_error_string_dart = ffi.Pointer<ffi.Char> Function(
    int error);

typedef lightning_db_begin_transaction_native = ffi.Int32 Function(
    ffi.Pointer<LightningDB> db, ffi.Pointer<ffi.Uint64> tx_id);
typedef lightning_db_begin_transaction_dart = int Function(
    ffi.Pointer<LightningDB> db, ffi.Pointer<ffi.Uint64> tx_id);

typedef lightning_db_put_tx_native = ffi.Int32 Function(
    ffi.Pointer<LightningDB> db,
    ffi.Uint64 tx_id,
    ffi.Pointer<ffi.Uint8> key,
    ffi.Size key_len,
    ffi.Pointer<ffi.Uint8> value,
    ffi.Size value_len);
typedef lightning_db_put_tx_dart = int Function(
    ffi.Pointer<LightningDB> db,
    int tx_id,
    ffi.Pointer<ffi.Uint8> key,
    int key_len,
    ffi.Pointer<ffi.Uint8> value,
    int value_len);

typedef lightning_db_commit_transaction_native = ffi.Int32 Function(
    ffi.Pointer<LightningDB> db, ffi.Uint64 tx_id);
typedef lightning_db_commit_transaction_dart = int Function(
    ffi.Pointer<LightningDB> db, int tx_id);

typedef lightning_db_abort_transaction_native = ffi.Int32 Function(
    ffi.Pointer<LightningDB> db, ffi.Uint64 tx_id);
typedef lightning_db_abort_transaction_dart = int Function(
    ffi.Pointer<LightningDB> db, int tx_id);

// Opaque types
final class LightningDB extends ffi.Opaque {}
final class LightningDBConfig extends ffi.Opaque {}

// Result struct
final class LightningDBResult extends ffi.Struct {
  external ffi.Pointer<ffi.Uint8> data;
  
  @ffi.Size()
  external int len;
}

// Error codes enum
abstract class LightningDBError {
  static const int success = 0;
  static const int nullPointer = -1;
  static const int invalidUtf8 = -2;
  static const int ioError = -3;
  static const int corruptedData = -4;
  static const int invalidArgument = -5;
  static const int outOfMemory = -6;
  static const int databaseLocked = -7;
  static const int transactionConflict = -8;
  static const int unknownError = -99;
}

/// FFI bindings for Lightning DB
class LightningDBBindings {
  late final ffi.DynamicLibrary _lib;
  
  // Function pointers
  late final lightning_db_config_new_dart lightning_db_config_new;
  late final lightning_db_config_set_page_size_dart lightning_db_config_set_page_size;
  late final lightning_db_config_set_cache_size_dart lightning_db_config_set_cache_size;
  late final lightning_db_config_free_dart lightning_db_config_free;
  late final lightning_db_open_dart lightning_db_open;
  late final lightning_db_create_dart lightning_db_create;
  late final lightning_db_put_dart lightning_db_put;
  late final lightning_db_get_dart lightning_db_get;
  late final lightning_db_delete_dart lightning_db_delete;
  late final lightning_db_free_result_dart lightning_db_free_result;
  late final lightning_db_checkpoint_dart lightning_db_checkpoint;
  late final lightning_db_free_dart lightning_db_free;
  late final lightning_db_error_string_dart lightning_db_error_string;
  late final lightning_db_begin_transaction_dart lightning_db_begin_transaction;
  late final lightning_db_put_tx_dart lightning_db_put_tx;
  late final lightning_db_commit_transaction_dart lightning_db_commit_transaction;
  late final lightning_db_abort_transaction_dart lightning_db_abort_transaction;
  
  LightningDBBindings({String? libraryPath}) {
    // Load the library
    if (libraryPath != null) {
      _lib = ffi.DynamicLibrary.open(libraryPath);
    } else {
      // Auto-detect library path based on platform
      if (Platform.isMacOS) {
        _lib = ffi.DynamicLibrary.open('liblightning_db_ffi.dylib');
      } else if (Platform.isLinux) {
        _lib = ffi.DynamicLibrary.open('liblightning_db_ffi.so');
      } else if (Platform.isWindows) {
        _lib = ffi.DynamicLibrary.open('lightning_db_ffi.dll');
      } else {
        throw UnsupportedError('Unsupported platform: ${Platform.operatingSystem}');
      }
    }
    
    // Load all functions
    lightning_db_config_new = _lib
        .lookup<ffi.NativeFunction<lightning_db_config_new_native>>('lightning_db_config_new')
        .asFunction();
    
    lightning_db_config_set_page_size = _lib
        .lookup<ffi.NativeFunction<lightning_db_config_set_page_size_native>>(
            'lightning_db_config_set_page_size')
        .asFunction();
    
    lightning_db_config_set_cache_size = _lib
        .lookup<ffi.NativeFunction<lightning_db_config_set_cache_size_native>>(
            'lightning_db_config_set_cache_size')
        .asFunction();
    
    lightning_db_config_free = _lib
        .lookup<ffi.NativeFunction<lightning_db_config_free_native>>('lightning_db_config_free')
        .asFunction();
    
    lightning_db_open = _lib
        .lookup<ffi.NativeFunction<lightning_db_open_native>>('lightning_db_open')
        .asFunction();
    
    lightning_db_create = _lib
        .lookup<ffi.NativeFunction<lightning_db_create_native>>('lightning_db_create')
        .asFunction();
    
    lightning_db_put = _lib
        .lookup<ffi.NativeFunction<lightning_db_put_native>>('lightning_db_put')
        .asFunction();
    
    lightning_db_get = _lib
        .lookup<ffi.NativeFunction<lightning_db_get_native>>('lightning_db_get')
        .asFunction();
    
    lightning_db_delete = _lib
        .lookup<ffi.NativeFunction<lightning_db_delete_native>>('lightning_db_delete')
        .asFunction();
    
    lightning_db_free_result = _lib
        .lookup<ffi.NativeFunction<lightning_db_free_result_native>>('lightning_db_free_result')
        .asFunction();
    
    lightning_db_checkpoint = _lib
        .lookup<ffi.NativeFunction<lightning_db_checkpoint_native>>('lightning_db_checkpoint')
        .asFunction();
    
    lightning_db_free = _lib
        .lookup<ffi.NativeFunction<lightning_db_free_native>>('lightning_db_free')
        .asFunction();
    
    lightning_db_error_string = _lib
        .lookup<ffi.NativeFunction<lightning_db_error_string_native>>('lightning_db_error_string')
        .asFunction();
    
    lightning_db_begin_transaction = _lib
        .lookup<ffi.NativeFunction<lightning_db_begin_transaction_native>>(
            'lightning_db_begin_transaction')
        .asFunction();
    
    lightning_db_put_tx = _lib
        .lookup<ffi.NativeFunction<lightning_db_put_tx_native>>('lightning_db_put_tx')
        .asFunction();
    
    lightning_db_commit_transaction = _lib
        .lookup<ffi.NativeFunction<lightning_db_commit_transaction_native>>(
            'lightning_db_commit_transaction')
        .asFunction();
    
    lightning_db_abort_transaction = _lib
        .lookup<ffi.NativeFunction<lightning_db_abort_transaction_native>>(
            'lightning_db_abort_transaction')
        .asFunction();
  }
}