#ifndef LIGHTNING_DB_FFI_H
#define LIGHTNING_DB_FFI_H

#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Error codes for FFI
 */
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
  UnknownError = -99,
} LightningDBError;

/**
 * Opaque type for database handle
 */
typedef struct LightningDB LightningDB;

/**
 * Opaque type for database configuration
 */
typedef struct LightningDBConfig LightningDBConfig;

/**
 * Result type for data operations
 */
typedef struct LightningDBResult {
  uint8_t *data;
  size_t len;
} LightningDBResult;

/**
 * Create a new configuration with default values
 *
 * # Safety
 *
 * The returned pointer must be freed with `lightning_db_config_free`
 */
struct LightningDBConfig *lightning_db_config_new(void);

/**
 * Set the page size for the configuration
 *
 * # Safety
 *
 * `config` must be a valid pointer returned by `lightning_db_config_new`
 */
enum LightningDBError lightning_db_config_set_page_size(struct LightningDBConfig *config,
                                                        size_t page_size);

/**
 * Set the cache size for the configuration
 *
 * # Safety
 *
 * `config` must be a valid pointer returned by `lightning_db_config_new`
 */
enum LightningDBError lightning_db_config_set_cache_size(struct LightningDBConfig *config,
                                                         size_t cache_size);

/**
 * Free a configuration object
 *
 * # Safety
 *
 * `config` must be a valid pointer returned by `lightning_db_config_new`
 * After calling this function, the pointer becomes invalid
 */
void lightning_db_config_free(struct LightningDBConfig *config);

/**
 * Open a database at the specified path
 *
 * # Safety
 *
 * - `path` must be a valid null-terminated C string
 * - `config` must be a valid pointer or NULL (for default config)
 * - The returned pointer must be freed with `lightning_db_free`
 */
struct LightningDB *lightning_db_open(const char *path, const struct LightningDBConfig *config);

/**
 * Create a new database at the specified path
 *
 * # Safety
 *
 * - `path` must be a valid null-terminated C string
 * - `config` must be a valid pointer or NULL (for default config)
 * - The returned pointer must be freed with `lightning_db_free`
 */
struct LightningDB *lightning_db_create(const char *path, const struct LightningDBConfig *config);

/**
 * Put a key-value pair into the database
 *
 * # Safety
 *
 * - `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
 * - `key` and `value` must be valid pointers to arrays of at least `key_len` and `value_len` bytes
 */
enum LightningDBError lightning_db_put(struct LightningDB *db,
                                       const uint8_t *key,
                                       size_t key_len,
                                       const uint8_t *value,
                                       size_t value_len);

/**
 * Get a value from the database
 *
 * # Safety
 *
 * - `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
 * - `key` must be a valid pointer to an array of at least `key_len` bytes
 * - `result` must be a valid pointer to store the result
 * - The returned data pointer in result must be freed with `lightning_db_free_result`
 */
enum LightningDBError lightning_db_get(struct LightningDB *db,
                                       const uint8_t *key,
                                       size_t key_len,
                                       struct LightningDBResult *result);

/**
 * Delete a key from the database
 *
 * # Safety
 *
 * - `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
 * - `key` must be a valid pointer to an array of at least `key_len` bytes
 */
enum LightningDBError lightning_db_delete(struct LightningDB *db,
                                          const uint8_t *key,
                                          size_t key_len);

/**
 * Free a result returned by get operations
 *
 * # Safety
 *
 * - `data` must be a valid pointer returned in a `LightningDBResult`
 * - After calling this function, the pointer becomes invalid
 */
void lightning_db_free_result(uint8_t *data, size_t len);

/**
 * Checkpoint the database (flush to disk)
 *
 * # Safety
 *
 * `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
 */
enum LightningDBError lightning_db_checkpoint(struct LightningDB *db);

/**
 * Close and free a database
 *
 * # Safety
 *
 * - `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
 * - After calling this function, the pointer becomes invalid
 */
void lightning_db_free(struct LightningDB *db);

/**
 * Get the last error message as a C string
 *
 * # Safety
 *
 * The returned string is statically allocated and should not be freed
 */
const char *lightning_db_error_string(enum LightningDBError error);

/**
 * Begin a new transaction
 *
 * # Safety
 *
 * `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
 */
enum LightningDBError lightning_db_begin_transaction(struct LightningDB *db, uint64_t *tx_id);

/**
 * Put a key-value pair within a transaction
 *
 * # Safety
 *
 * Same as `lightning_db_put` but operates within the specified transaction
 */
enum LightningDBError lightning_db_put_tx(struct LightningDB *db,
                                          uint64_t tx_id,
                                          const uint8_t *key,
                                          size_t key_len,
                                          const uint8_t *value,
                                          size_t value_len);

/**
 * Commit a transaction
 *
 * # Safety
 *
 * `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
 */
enum LightningDBError lightning_db_commit_transaction(struct LightningDB *db, uint64_t tx_id);

/**
 * Abort a transaction
 *
 * # Safety
 *
 * `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
 */
enum LightningDBError lightning_db_abort_transaction(struct LightningDB *db, uint64_t tx_id);

#endif  /* LIGHTNING_DB_FFI_H */
