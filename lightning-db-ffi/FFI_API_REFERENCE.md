# Lightning DB FFI API Reference

This document provides a comprehensive reference for the Lightning DB Foreign Function Interface (FFI) API.

## Table of Contents

- [Overview](#overview)
- [Error Handling](#error-handling)
- [Configuration Functions](#configuration-functions)
- [Database Lifecycle Functions](#database-lifecycle-functions)
- [Data Operations](#data-operations)
- [Transaction Functions](#transaction-functions)
- [Memory Management](#memory-management)
- [Thread Safety](#thread-safety)
- [Platform-Specific Notes](#platform-specific-notes)
- [Best Practices](#best-practices)

## Overview

The Lightning DB FFI provides a C-compatible interface to the Lightning DB high-performance embedded database. All functions follow C calling conventions and use only C-compatible types.

### Header File

```c
#include "lightning_db_ffi.h"
```

### Linking

- **Static**: Link against `liblightning_db_ffi.a` (Unix) or `lightning_db_ffi.lib` (Windows)
- **Dynamic**: Link against `liblightning_db_ffi.so` (Linux), `liblightning_db_ffi.dylib` (macOS), or `lightning_db_ffi.dll` (Windows)

## Error Handling

### Error Codes

```c
typedef enum LightningDBError {
    Success = 0,
    NullPointer = -1,
    InvalidUtf8 = -2,
    IoError = -3,
    CorruptedData = -4,
    InvalidArgument = -5,
    OutOfMemory = -6,
    DatabaseLocked = -7,
    TransactionConflict = -8,
    UnknownError = -99
} LightningDBError;
```

### Error String Function

```c
const char* lightning_db_error_string(enum LightningDBError error);
```

Returns a human-readable error message for the given error code.

**Parameters:**
- `error`: The error code

**Returns:**
- Static string describing the error (do not free)

**Example:**
```c
LightningDBError err = lightning_db_put(db, key, key_len, value, value_len);
if (err != Success) {
    fprintf(stderr, "Error: %s\n", lightning_db_error_string(err));
}
```

## Configuration Functions

### lightning_db_config_new

```c
struct LightningDBConfig* lightning_db_config_new(void);
```

Creates a new configuration object with default values.

**Returns:**
- Pointer to new config object, or NULL on failure

**Default Values:**
- Page size: 4096 bytes
- Cache size: 64 MB

### lightning_db_config_set_page_size

```c
enum LightningDBError lightning_db_config_set_page_size(
    struct LightningDBConfig* config,
    size_t page_size
);
```

Sets the page size for the database.

**Parameters:**
- `config`: Configuration object
- `page_size`: Page size in bytes (must be power of 2, typically 4096)

**Returns:**
- `Success` or error code

### lightning_db_config_set_cache_size

```c
enum LightningDBError lightning_db_config_set_cache_size(
    struct LightningDBConfig* config,
    size_t cache_size
);
```

Sets the cache size for the database.

**Parameters:**
- `config`: Configuration object
- `cache_size`: Cache size in bytes

**Returns:**
- `Success` or error code

### lightning_db_config_free

```c
void lightning_db_config_free(struct LightningDBConfig* config);
```

Frees a configuration object.

**Parameters:**
- `config`: Configuration object to free (can be NULL)

## Database Lifecycle Functions

### lightning_db_create

```c
struct LightningDB* lightning_db_create(
    const char* path,
    const struct LightningDBConfig* config
);
```

Creates a new database at the specified path.

**Parameters:**
- `path`: Null-terminated path where database will be created
- `config`: Configuration object (can be NULL for defaults)

**Returns:**
- Database handle, or NULL on failure

**Example:**
```c
LightningDBConfig* config = lightning_db_config_new();
lightning_db_config_set_cache_size(config, 128 * 1024 * 1024); // 128MB
LightningDB* db = lightning_db_create("./mydb", config);
lightning_db_config_free(config);
```

### lightning_db_open

```c
struct LightningDB* lightning_db_open(
    const char* path,
    const struct LightningDBConfig* config
);
```

Opens an existing database.

**Parameters:**
- `path`: Null-terminated path to existing database
- `config`: Configuration object (can be NULL for defaults)

**Returns:**
- Database handle, or NULL on failure

### lightning_db_free

```c
void lightning_db_free(struct LightningDB* db);
```

Closes and frees a database handle.

**Parameters:**
- `db`: Database handle to close (can be NULL)

**Notes:**
- Flushes all pending writes before closing
- Stops all background threads
- After calling, the database pointer is invalid

## Data Operations

### lightning_db_put

```c
enum LightningDBError lightning_db_put(
    struct LightningDB* db,
    const uint8_t* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len
);
```

Stores a key-value pair in the database.

**Parameters:**
- `db`: Database handle
- `key`: Key bytes
- `key_len`: Length of key in bytes
- `value`: Value bytes
- `value_len`: Length of value in bytes

**Returns:**
- `Success` or error code

**Example:**
```c
const char* key = "user:123";
const char* value = "{\"name\":\"John\",\"age\":30}";
LightningDBError err = lightning_db_put(db, 
    (uint8_t*)key, strlen(key),
    (uint8_t*)value, strlen(value)
);
```

### lightning_db_get

```c
enum LightningDBError lightning_db_get(
    struct LightningDB* db,
    const uint8_t* key,
    size_t key_len,
    struct LightningDBResult* result
);
```

Retrieves a value from the database.

**Parameters:**
- `db`: Database handle
- `key`: Key bytes
- `key_len`: Length of key in bytes
- `result`: Pointer to result structure

**Returns:**
- `Success` or error code

**Result Structure:**
```c
typedef struct LightningDBResult {
    uint8_t* data;  // NULL if key not found
    size_t len;     // 0 if key not found
} LightningDBResult;
```

**Example:**
```c
LightningDBResult result;
LightningDBError err = lightning_db_get(db, 
    (uint8_t*)key, strlen(key), &result
);
if (err == Success && result.data != NULL) {
    printf("Value: %.*s\n", (int)result.len, result.data);
    lightning_db_free_result(result.data, result.len);
}
```

### lightning_db_delete

```c
enum LightningDBError lightning_db_delete(
    struct LightningDB* db,
    const uint8_t* key,
    size_t key_len
);
```

Deletes a key-value pair from the database.

**Parameters:**
- `db`: Database handle
- `key`: Key bytes
- `key_len`: Length of key in bytes

**Returns:**
- `Success` or error code

### lightning_db_checkpoint

```c
enum LightningDBError lightning_db_checkpoint(struct LightningDB* db);
```

Forces a checkpoint, flushing all data to disk.

**Parameters:**
- `db`: Database handle

**Returns:**
- `Success` or error code

**Notes:**
- Normally, Lightning DB automatically manages checkpoints
- Use this for explicit durability guarantees

## Transaction Functions

### lightning_db_begin_transaction

```c
enum LightningDBError lightning_db_begin_transaction(
    struct LightningDB* db,
    uint64_t* tx_id
);
```

Begins a new transaction.

**Parameters:**
- `db`: Database handle
- `tx_id`: Pointer to store transaction ID

**Returns:**
- `Success` or error code

### lightning_db_put_tx

```c
enum LightningDBError lightning_db_put_tx(
    struct LightningDB* db,
    uint64_t tx_id,
    const uint8_t* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len
);
```

Puts a key-value pair within a transaction.

**Parameters:**
- `db`: Database handle
- `tx_id`: Transaction ID
- `key`: Key bytes
- `key_len`: Length of key in bytes
- `value`: Value bytes
- `value_len`: Length of value in bytes

**Returns:**
- `Success` or error code

### lightning_db_commit_transaction

```c
enum LightningDBError lightning_db_commit_transaction(
    struct LightningDB* db,
    uint64_t tx_id
);
```

Commits a transaction, making all changes permanent.

**Parameters:**
- `db`: Database handle
- `tx_id`: Transaction ID

**Returns:**
- `Success` or error code

### lightning_db_abort_transaction

```c
enum LightningDBError lightning_db_abort_transaction(
    struct LightningDB* db,
    uint64_t tx_id
);
```

Aborts a transaction, discarding all changes.

**Parameters:**
- `db`: Database handle
- `tx_id`: Transaction ID

**Returns:**
- `Success` or error code

**Transaction Example:**
```c
uint64_t tx_id;
LightningDBError err = lightning_db_begin_transaction(db, &tx_id);
if (err != Success) return err;

// Multiple operations
err = lightning_db_put_tx(db, tx_id, key1, key1_len, val1, val1_len);
if (err != Success) {
    lightning_db_abort_transaction(db, tx_id);
    return err;
}

err = lightning_db_put_tx(db, tx_id, key2, key2_len, val2, val2_len);
if (err != Success) {
    lightning_db_abort_transaction(db, tx_id);
    return err;
}

// Commit all changes atomically
err = lightning_db_commit_transaction(db, tx_id);
```

## Memory Management

### lightning_db_free_result

```c
void lightning_db_free_result(uint8_t* data, size_t len);
```

Frees memory allocated for a query result.

**Parameters:**
- `data`: Data pointer from LightningDBResult
- `len`: Length from LightningDBResult

**Notes:**
- Must be called for every successful get operation
- Safe to call with NULL data pointer

### Memory Rules

1. **Configuration objects**: Created with `lightning_db_config_new()`, freed with `lightning_db_config_free()`
2. **Database handles**: Created with `lightning_db_create()`/`lightning_db_open()`, freed with `lightning_db_free()`
3. **Result data**: Returned by `lightning_db_get()`, freed with `lightning_db_free_result()`
4. **Error strings**: Returned by `lightning_db_error_string()` are static - do NOT free

## Thread Safety

Lightning DB FFI is thread-safe with the following guarantees:

- Multiple threads can safely use the same database handle
- Transactions are isolated between threads
- Internal synchronization prevents data races

**Best Practices:**
- Share a single database handle between threads
- Each thread should manage its own transactions
- Avoid opening the same database file multiple times

## Platform-Specific Notes

### Windows
- Use `lightning_db_ffi.dll` for dynamic linking
- Requires Visual C++ Runtime
- Path separators should use forward slashes or escaped backslashes

### macOS
- Use `liblightning_db_ffi.dylib` for dynamic linking
- Supports both Intel and Apple Silicon
- May require code signing for distribution

### Linux
- Use `liblightning_db_ffi.so` for dynamic linking
- Ensure glibc compatibility
- Set `LD_LIBRARY_PATH` if library is not in standard location

### iOS/Android
- Use static linking for mobile platforms
- Ensure proper architecture support (arm64, armv7)
- Follow platform-specific file access restrictions

## Best Practices

### Error Handling
```c
#define CHECK_ERROR(err, operation) \
    if ((err) != Success) { \
        fprintf(stderr, "%s failed: %s\n", operation, \
            lightning_db_error_string(err)); \
        goto cleanup; \
    }

// Usage
LightningDBError err;
err = lightning_db_put(db, key, key_len, value, value_len);
CHECK_ERROR(err, "put");
```

### Resource Management
```c
typedef struct {
    LightningDB* db;
} AppContext;

AppContext* app_init(const char* db_path) {
    AppContext* ctx = calloc(1, sizeof(AppContext));
    if (!ctx) return NULL;
    
    ctx->db = lightning_db_create(db_path, NULL);
    if (!ctx->db) {
        free(ctx);
        return NULL;
    }
    
    return ctx;
}

void app_cleanup(AppContext* ctx) {
    if (ctx) {
        if (ctx->db) {
            lightning_db_free(ctx->db);
        }
        free(ctx);
    }
}
```

### Performance Tips

1. **Batch Operations**: Use transactions for multiple related operations
2. **Key Design**: Use fixed-size keys when possible for better performance
3. **Value Size**: Keep values under 100KB for optimal performance
4. **Cache Tuning**: Set cache size based on working set size
5. **Checkpoint Strategy**: Let auto-checkpoint handle durability unless specific needs

### Common Pitfalls

1. **Forgetting to free results**: Always call `lightning_db_free_result()`
2. **Using freed pointers**: Database pointers are invalid after `lightning_db_free()`
3. **Path encoding**: Ensure paths are valid UTF-8
4. **Transaction leaks**: Always commit or abort transactions
5. **Error checking**: Check return values for all operations

## Example Programs

Complete example programs are available in the `examples/` directory:
- `basic_usage.c`: Basic operations and error handling
- `performance_test.c`: Performance benchmarking
- `python_example.py`: Python bindings using ctypes

## Version History

- **0.1.0**: Initial FFI release
  - Basic CRUD operations
  - Transaction support
  - Configuration API
  - Error handling