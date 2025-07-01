#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "../include/lightning_db_ffi.h"

int main() {
    printf("Simple Lightning DB FFI Test\n");
    printf("============================\n\n");
    
    // Create database
    const char* db_path = "./simple_test_db";
    LightningDB* db = lightning_db_create(db_path, NULL);
    
    if (!db) {
        fprintf(stderr, "Failed to create database\n");
        return 1;
    }
    
    printf("Database created successfully\n");
    
    // Test a small number of operations
    const int num_ops = 1000;
    
    printf("\nInserting %d items...\n", num_ops);
    for (int i = 0; i < num_ops; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "key_%04d", i);
        snprintf(value, sizeof(value), "value_%04d", i);
        
        enum LightningDBError err = lightning_db_put(
            db,
            (const uint8_t*)key, strlen(key),
            (const uint8_t*)value, strlen(value)
        );
        
        if (err != Success) {
            fprintf(stderr, "Failed to insert %s: %s\n", key, 
                    lightning_db_error_string(err));
            break;
        }
        
        if (i % 100 == 0) {
            printf("  Inserted %d items...\n", i);
        }
    }
    
    printf("\nCheckpointing...\n");
    enum LightningDBError err = lightning_db_checkpoint(db);
    if (err != Success) {
        fprintf(stderr, "Checkpoint failed: %s\n", lightning_db_error_string(err));
    } else {
        printf("Checkpoint completed\n");
    }
    
    printf("\nReading back data...\n");
    int found = 0;
    for (int i = 0; i < num_ops; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%04d", i);
        
        struct LightningDBResult result;
        err = lightning_db_get(db, (const uint8_t*)key, strlen(key), &result);
        
        if (err == Success && result.data) {
            found++;
            lightning_db_free_result(result.data, result.len);
        }
    }
    
    printf("Found %d/%d items\n", found, num_ops);
    
    // Cleanup
    printf("\nClosing database...\n");
    lightning_db_free(db);
    
    printf("\nTest completed successfully\n");
    return 0;
}