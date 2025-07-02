#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "../include/lightning_db_ffi.h"

#define NUM_OPERATIONS 75000
#define KEY_SIZE 32
#define VALUE_SIZE 100

double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

void benchmark_writes(LightningDB* db) {
    printf("\nBenchmarking writes...\n");
    
    char key[KEY_SIZE];
    char value[VALUE_SIZE];
    
    double start = get_time();
    
    for (int i = 0; i < NUM_OPERATIONS; i++) {
        snprintf(key, sizeof(key), "key_%08d", i);
        snprintf(value, sizeof(value), "value_%08d_padding", i);
        
        enum LightningDBError err = lightning_db_put(
            db,
            (const uint8_t*)key, strlen(key),
            (const uint8_t*)value, strlen(value)
        );
        
        if (err != Success) {
            fprintf(stderr, "Write error at %d: %s\n", i, lightning_db_error_string(err));
            break;
        }
    }
    
    double elapsed = get_time() - start;
    double ops_per_sec = NUM_OPERATIONS / elapsed;
    
    printf("  Writes: %d operations in %.2f seconds\n", NUM_OPERATIONS, elapsed);
    printf("  Performance: %.0f ops/sec\n", ops_per_sec);
    printf("  Latency: %.2f µs/op\n", (elapsed * 1000000) / NUM_OPERATIONS);
}

void benchmark_reads(LightningDB* db) {
    printf("\nBenchmarking reads...\n");
    
    char key[KEY_SIZE];
    struct LightningDBResult result;
    
    double start = get_time();
    int found = 0;
    
    for (int i = 0; i < NUM_OPERATIONS; i++) {
        snprintf(key, sizeof(key), "key_%08d", i);
        
        enum LightningDBError err = lightning_db_get(
            db,
            (const uint8_t*)key, strlen(key),
            &result
        );
        
        if (err != Success) {
            fprintf(stderr, "Read error at %d: %s\n", i, lightning_db_error_string(err));
            break;
        }
        
        if (result.data) {
            found++;
            lightning_db_free_result(result.data, result.len);
        }
    }
    
    double elapsed = get_time() - start;
    double ops_per_sec = NUM_OPERATIONS / elapsed;
    
    printf("  Reads: %d operations in %.2f seconds (found: %d)\n", 
           NUM_OPERATIONS, elapsed, found);
    printf("  Performance: %.0f ops/sec\n", ops_per_sec);
    printf("  Latency: %.2f µs/op\n", (elapsed * 1000000) / NUM_OPERATIONS);
}

void benchmark_mixed_workload(LightningDB* db) {
    printf("\nBenchmarking mixed workload (70%% read, 20%% write, 10%% delete)...\n");
    
    char key[KEY_SIZE];
    char value[VALUE_SIZE];
    struct LightningDBResult result;
    
    int reads = 0, writes = 0, deletes = 0;
    double start = get_time();
    
    srand(time(NULL));
    
    for (int i = 0; i < NUM_OPERATIONS; i++) {
        int op = rand() % 100;
        int key_num = rand() % NUM_OPERATIONS;
        
        snprintf(key, sizeof(key), "key_%08d", key_num);
        
        if (op < 70) {
            // Read
            lightning_db_get(db, (const uint8_t*)key, strlen(key), &result);
            if (result.data) {
                lightning_db_free_result(result.data, result.len);
            }
            reads++;
        } else if (op < 90) {
            // Write
            snprintf(value, sizeof(value), "updated_value_%08d", i);
            lightning_db_put(
                db,
                (const uint8_t*)key, strlen(key),
                (const uint8_t*)value, strlen(value)
            );
            writes++;
        } else {
            // Delete
            lightning_db_delete(db, (const uint8_t*)key, strlen(key));
            deletes++;
        }
    }
    
    double elapsed = get_time() - start;
    double ops_per_sec = NUM_OPERATIONS / elapsed;
    
    printf("  Operations: %d total (%d reads, %d writes, %d deletes)\n", 
           NUM_OPERATIONS, reads, writes, deletes);
    printf("  Time: %.2f seconds\n", elapsed);
    printf("  Performance: %.0f ops/sec\n", ops_per_sec);
}

void benchmark_transactions(LightningDB* db) {
    printf("\nBenchmarking transactions...\n");
    
    const int tx_size = 100;
    const int num_transactions = NUM_OPERATIONS / tx_size;
    
    char key[KEY_SIZE];
    char value[VALUE_SIZE];
    
    double start = get_time();
    int committed = 0;
    
    for (int tx = 0; tx < num_transactions; tx++) {
        uint64_t tx_id;
        enum LightningDBError err = lightning_db_begin_transaction(db, &tx_id);
        
        if (err != Success) {
            continue;
        }
        
        int success = 1;
        for (int i = 0; i < tx_size; i++) {
            snprintf(key, sizeof(key), "tx_%d_key_%d", tx, i);
            snprintf(value, sizeof(value), "tx_%d_value_%d", tx, i);
            
            err = lightning_db_put_tx(
                db, tx_id,
                (const uint8_t*)key, strlen(key),
                (const uint8_t*)value, strlen(value)
            );
            
            if (err != Success) {
                success = 0;
                break;
            }
        }
        
        if (success) {
            err = lightning_db_commit_transaction(db, tx_id);
            if (err == Success) {
                committed++;
            }
        } else {
            lightning_db_abort_transaction(db, tx_id);
        }
    }
    
    double elapsed = get_time() - start;
    double tx_per_sec = committed / elapsed;
    
    printf("  Transactions: %d committed of %d attempted\n", committed, num_transactions);
    printf("  Time: %.2f seconds\n", elapsed);
    printf("  Performance: %.0f transactions/sec\n", tx_per_sec);
    printf("  Throughput: %.0f ops/sec\n", (committed * tx_size) / elapsed);
}

int main() {
    printf("Lightning DB FFI Performance Test\n");
    printf("=================================\n");
    
    // Create configuration
    LightningDBConfig* config = lightning_db_config_new();
    lightning_db_config_set_cache_size(config, 256 * 1024 * 1024); // 256MB cache
    lightning_db_config_set_page_size(config, 4096);
    
    // Create database
    const char* db_path = "./perf_test_db";
    LightningDB* db = lightning_db_create(db_path, config);
    
    if (!db) {
        fprintf(stderr, "Failed to create database\n");
        lightning_db_config_free(config);
        return 1;
    }
    
    printf("Database created with 256MB cache\n");
    
    // Run benchmarks
    benchmark_writes(db);
    benchmark_reads(db);
    benchmark_mixed_workload(db);
    benchmark_transactions(db);
    
    // Checkpoint
    printf("\nPerforming checkpoint...\n");
    double checkpoint_start = get_time();
    lightning_db_checkpoint(db);
    double checkpoint_time = get_time() - checkpoint_start;
    printf("  Checkpoint completed in %.2f seconds\n", checkpoint_time);
    
    // Cleanup
    lightning_db_free(db);
    lightning_db_config_free(config);
    
    printf("\nTest completed successfully\n");
    return 0;
}