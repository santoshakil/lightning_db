#ifndef LIGHTNING_DB_FFI_DART_H
#define LIGHTNING_DB_FFI_DART_H

// Minimal type definitions for Dart FFI without system headers
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef unsigned int uint32_t;
typedef unsigned long long uint64_t;
typedef signed int int32_t;
typedef unsigned long uintptr_t;

// Platform-specific export macro
#if _WIN32
#define FFI_PLUGIN_EXPORT __declspec(dllexport)
#else
#define FFI_PLUGIN_EXPORT
#endif

// Handle types
typedef uint64_t DatabaseHandle;
typedef uint64_t TransactionHandle;
typedef uint64_t IteratorHandle;

// Enums from the original header
typedef enum CompressionType {
  CompressionTypeNone = 0,
  CompressionTypeZstd = 1,
  CompressionTypeLz4 = 2,
  CompressionTypeSnappy = 3,
} CompressionType;

typedef enum ConsistencyLevel {
  ConsistencyLevelEventual = 0,
  ConsistencyLevelStrong = 1,
} ConsistencyLevel;

typedef enum ErrorCode {
  ErrorCodeSuccess = 0,
  ErrorCodeInvalidArgument = 1,
  ErrorCodeDatabaseNotFound = 2,
  ErrorCodeDatabaseExists = 3,
  ErrorCodeTransactionNotFound = 4,
  ErrorCodeTransactionConflict = 5,
  ErrorCodeKeyNotFound = 6,
  ErrorCodeIoError = 7,
  ErrorCodeCorruptedData = 8,
  ErrorCodeOutOfMemory = 9,
  ErrorCodeUnknown = 999,
} ErrorCode;

typedef enum WalSyncMode {
  WalSyncModeSync = 0,
  WalSyncModePeriodic = 1,
  WalSyncModeAsync = 2,
} WalSyncMode;

// Result structures
typedef struct ByteResult {
  uint8_t *DATA;
  uintptr_t LEN;
  int32_t ERROR_CODE;
} ByteResult;

typedef struct KeyValueResult {
  uint8_t *KEY;
  uintptr_t KEY_LEN;
  uint8_t *VALUE;
  uintptr_t VALUE_LEN;
  int32_t ERROR_CODE;
} KeyValueResult;

// Function declarations
FFI_PLUGIN_EXPORT int32_t lightning_db_init(void);
FFI_PLUGIN_EXPORT const char *lightning_db_get_last_error(void);
FFI_PLUGIN_EXPORT int32_t lightning_db_get_last_error_buffer(char *buffer, uintptr_t buffer_len);
FFI_PLUGIN_EXPORT void lightning_db_clear_error(void);
FFI_PLUGIN_EXPORT int32_t lightning_db_create(const char *path, uint64_t *out_handle);
FFI_PLUGIN_EXPORT int32_t lightning_db_open(const char *path, uint64_t *out_handle);
FFI_PLUGIN_EXPORT int32_t lightning_db_create_with_config(const char *path,
                                        uint64_t cache_size,
                                        enum CompressionType compression_type,
                                        enum WalSyncMode wal_sync_mode,
                                        uint64_t *out_handle);
FFI_PLUGIN_EXPORT int32_t lightning_db_close(uint64_t handle);
FFI_PLUGIN_EXPORT int32_t lightning_db_put(uint64_t handle,
                         const uint8_t *key,
                         uintptr_t key_len,
                         const uint8_t *value,
                         uintptr_t value_len);
FFI_PLUGIN_EXPORT struct ByteResult lightning_db_get(uint64_t handle,
                                   const uint8_t *key,
                                   uintptr_t key_len);
FFI_PLUGIN_EXPORT int32_t lightning_db_delete(uint64_t handle,
                            const uint8_t *key,
                            uintptr_t key_len);
FFI_PLUGIN_EXPORT int32_t lightning_db_sync(uint64_t handle);
FFI_PLUGIN_EXPORT int32_t lightning_db_checkpoint(uint64_t handle);
FFI_PLUGIN_EXPORT int32_t lightning_db_put_with_consistency(uint64_t handle,
                                          const uint8_t *key,
                                          uintptr_t key_len,
                                          const uint8_t *value,
                                          uintptr_t value_len,
                                          enum ConsistencyLevel consistency_level);
FFI_PLUGIN_EXPORT struct ByteResult lightning_db_get_with_consistency(uint64_t handle,
                                                    const uint8_t *key,
                                                    uintptr_t key_len,
                                                    enum ConsistencyLevel consistency_level);
FFI_PLUGIN_EXPORT int32_t lightning_db_begin_transaction(uint64_t db_handle, uint64_t *out_handle);
FFI_PLUGIN_EXPORT int32_t lightning_db_commit_transaction(uint64_t handle);
FFI_PLUGIN_EXPORT int32_t lightning_db_abort_transaction(uint64_t handle);
FFI_PLUGIN_EXPORT int32_t lightning_db_put_tx(uint64_t handle,
                            const uint8_t *key,
                            uintptr_t key_len,
                            const uint8_t *value,
                            uintptr_t value_len);
FFI_PLUGIN_EXPORT struct ByteResult lightning_db_get_tx(uint64_t handle,
                                      const uint8_t *key,
                                      uintptr_t key_len);
FFI_PLUGIN_EXPORT int32_t lightning_db_delete_tx(uint64_t handle,
                               const uint8_t *key,
                               uintptr_t key_len);
FFI_PLUGIN_EXPORT int32_t lightning_db_scan(uint64_t db_handle,
                          const uint8_t *start_key,
                          uintptr_t start_key_len,
                          const uint8_t *end_key,
                          uintptr_t end_key_len,
                          uint64_t *out_handle);
FFI_PLUGIN_EXPORT struct KeyValueResult lightning_db_iterator_next(uint64_t handle);
FFI_PLUGIN_EXPORT int32_t lightning_db_iterator_close(uint64_t handle);
FFI_PLUGIN_EXPORT void lightning_db_free_bytes(struct ByteResult result);
FFI_PLUGIN_EXPORT void lightning_db_free_string(char *str);
FFI_PLUGIN_EXPORT void lightning_db_free_key_value(struct KeyValueResult result);

#endif // LIGHTNING_DB_FFI_DART_H