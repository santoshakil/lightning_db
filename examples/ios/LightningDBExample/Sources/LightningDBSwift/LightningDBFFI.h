#ifndef LIGHTNING_DB_FFI_H
#define LIGHTNING_DB_FFI_H

#include <stdint.h>
#include <stdbool.h>

// Initialize Lightning DB FFI
void lightning_db_init(void);

// Database operations
void* lightning_db_create(const char* path, uint64_t cache_size, bool compression_enabled, 
                         const char* sync_mode, char** error);
void lightning_db_close(void* db);

// Basic operations
bool lightning_db_put(void* db, const char* key, const uint8_t* value, size_t value_len, char** error);
bool lightning_db_get(void* db, const char* key, uint8_t** value, size_t* value_len, char** error);
bool lightning_db_delete(void* db, const char* key, char** error);

// Transaction operations
uint64_t lightning_db_begin_transaction(void* db, char** error);
bool lightning_db_tx_put(void* db, uint64_t tx_id, const char* key, 
                        const uint8_t* value, size_t value_len, char** error);
bool lightning_db_commit_transaction(void* db, uint64_t tx_id, char** error);
bool lightning_db_rollback_transaction(void* db, uint64_t tx_id, char** error);

// Range queries
bool lightning_db_range(void* db, const char* start, const char* end, size_t limit,
                       char*** keys, uint8_t*** values, size_t** value_lengths, 
                       size_t* count, char** error);

// Statistics
typedef struct {
    uint64_t total_keys;
    uint64_t total_size;
    float cache_hit_rate;
    float compression_ratio;
} lightning_db_stats;

bool lightning_db_get_stats(void* db, lightning_db_stats* stats, char** error);

// Memory management
void lightning_db_free_string(char* str);
void lightning_db_free_data(uint8_t* data);

#endif // LIGHTNING_DB_FFI_H