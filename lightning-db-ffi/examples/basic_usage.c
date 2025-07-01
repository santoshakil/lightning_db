#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "../include/lightning_db_ffi.h"

void check_error(enum LightningDBError error, const char* operation) {
    if (error != Success) {
        fprintf(stderr, "Error during %s: %s\n", operation, lightning_db_error_string(error));
        exit(1);
    }
}

int main() {
    printf("Lightning DB C FFI Example\n");
    printf("==========================\n\n");
    
    // Create database configuration
    LightningDBConfig* config = lightning_db_config_new();
    lightning_db_config_set_cache_size(config, 64 * 1024 * 1024); // 64MB cache
    
    // Create database
    const char* db_path = "./test_db";
    LightningDB* db = lightning_db_create(db_path, config);
    if (!db) {
        fprintf(stderr, "Failed to create database\n");
        return 1;
    }
    
    printf("Database created at: %s\n", db_path);
    
    // Put some data
    const char* key1 = "hello";
    const char* value1 = "world";
    enum LightningDBError err = lightning_db_put(
        db, 
        (const uint8_t*)key1, strlen(key1),
        (const uint8_t*)value1, strlen(value1)
    );
    check_error(err, "put");
    printf("Put: %s = %s\n", key1, value1);
    
    // Get the data back
    struct LightningDBResult result;
    err = lightning_db_get(db, (const uint8_t*)key1, strlen(key1), &result);
    check_error(err, "get");
    
    if (result.data) {
        printf("Get: %s = %.*s\n", key1, (int)result.len, result.data);
        lightning_db_free_result(result.data, result.len);
    } else {
        printf("Key not found: %s\n", key1);
    }
    
    // Transaction example
    printf("\nTransaction example:\n");
    uint64_t tx_id;
    err = lightning_db_begin_transaction(db, &tx_id);
    check_error(err, "begin transaction");
    
    // Add multiple keys in transaction
    for (int i = 0; i < 5; i++) {
        char key[32], value[32];
        snprintf(key, sizeof(key), "tx_key_%d", i);
        snprintf(value, sizeof(value), "tx_value_%d", i);
        
        err = lightning_db_put_tx(
            db, tx_id,
            (const uint8_t*)key, strlen(key),
            (const uint8_t*)value, strlen(value)
        );
        check_error(err, "put in transaction");
    }
    
    // Commit transaction
    err = lightning_db_commit_transaction(db, tx_id);
    check_error(err, "commit transaction");
    printf("Transaction committed with 5 keys\n");
    
    // Verify transaction data
    for (int i = 0; i < 5; i++) {
        char key[32];
        snprintf(key, sizeof(key), "tx_key_%d", i);
        
        err = lightning_db_get(db, (const uint8_t*)key, strlen(key), &result);
        check_error(err, "get transaction data");
        
        if (result.data) {
            printf("  %s = %.*s\n", key, (int)result.len, result.data);
            lightning_db_free_result(result.data, result.len);
        }
    }
    
    // Delete example
    printf("\nDelete example:\n");
    err = lightning_db_delete(db, (const uint8_t*)key1, strlen(key1));
    check_error(err, "delete");
    printf("Deleted key: %s\n", key1);
    
    // Verify deletion
    err = lightning_db_get(db, (const uint8_t*)key1, strlen(key1), &result);
    check_error(err, "get after delete");
    printf("Key %s found: %s\n", key1, result.data ? "yes" : "no");
    
    // Checkpoint
    err = lightning_db_checkpoint(db);
    check_error(err, "checkpoint");
    printf("\nDatabase checkpoint completed\n");
    
    // Cleanup
    lightning_db_free(db);
    lightning_db_config_free(config);
    
    printf("\nDatabase closed successfully\n");
    return 0;
}