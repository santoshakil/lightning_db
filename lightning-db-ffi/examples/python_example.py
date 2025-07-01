#!/usr/bin/env python3
"""
Lightning DB Python bindings example using ctypes
"""

import ctypes
import os
import sys
import time
from enum import IntEnum
from typing import Optional, Tuple

# Load the shared library
lib_path = os.path.join(os.path.dirname(__file__), '../../target/release')
if sys.platform == 'darwin':
    lib_name = 'liblightning_db_ffi.dylib'
elif sys.platform == 'linux':
    lib_name = 'liblightning_db_ffi.so'
else:
    lib_name = 'lightning_db_ffi.dll'

lib = ctypes.CDLL(os.path.join(lib_path, lib_name))

# Error codes
class LightningDBError(IntEnum):
    Success = 0
    NullPointer = -1
    InvalidUtf8 = -2
    IoError = -3
    CorruptedData = -4
    InvalidArgument = -5
    OutOfMemory = -6
    DatabaseLocked = -7
    TransactionConflict = -8
    UnknownError = -99

# Result structure
class LightningDBResult(ctypes.Structure):
    _fields_ = [
        ('data', ctypes.POINTER(ctypes.c_uint8)),
        ('len', ctypes.c_size_t)
    ]

# Function signatures
lib.lightning_db_config_new.restype = ctypes.c_void_p
lib.lightning_db_config_set_cache_size.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
lib.lightning_db_config_set_cache_size.restype = ctypes.c_int
lib.lightning_db_config_free.argtypes = [ctypes.c_void_p]

lib.lightning_db_create.argtypes = [ctypes.c_char_p, ctypes.c_void_p]
lib.lightning_db_create.restype = ctypes.c_void_p
lib.lightning_db_open.argtypes = [ctypes.c_char_p, ctypes.c_void_p]
lib.lightning_db_open.restype = ctypes.c_void_p
lib.lightning_db_free.argtypes = [ctypes.c_void_p]

lib.lightning_db_put.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint8), 
                                  ctypes.c_size_t, ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t]
lib.lightning_db_put.restype = ctypes.c_int

lib.lightning_db_get.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint8), 
                                 ctypes.c_size_t, ctypes.POINTER(LightningDBResult)]
lib.lightning_db_get.restype = ctypes.c_int

lib.lightning_db_delete.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t]
lib.lightning_db_delete.restype = ctypes.c_int

lib.lightning_db_free_result.argtypes = [ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t]

lib.lightning_db_checkpoint.argtypes = [ctypes.c_void_p]
lib.lightning_db_checkpoint.restype = ctypes.c_int

lib.lightning_db_error_string.argtypes = [ctypes.c_int]
lib.lightning_db_error_string.restype = ctypes.c_char_p

# Transaction functions
lib.lightning_db_begin_transaction.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64)]
lib.lightning_db_begin_transaction.restype = ctypes.c_int

lib.lightning_db_commit_transaction.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
lib.lightning_db_commit_transaction.restype = ctypes.c_int

lib.lightning_db_abort_transaction.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
lib.lightning_db_abort_transaction.restype = ctypes.c_int

class LightningDB:
    """Python wrapper for Lightning DB"""
    
    def __init__(self, path: str, cache_size: int = 64 * 1024 * 1024):
        self.config = lib.lightning_db_config_new()
        if self.config is None:
            raise RuntimeError("Failed to create config")
        
        # Set cache size
        err = lib.lightning_db_config_set_cache_size(self.config, cache_size)
        if err != LightningDBError.Success:
            lib.lightning_db_config_free(self.config)
            raise RuntimeError(f"Failed to set cache size: {self._get_error_string(err)}")
        
        # Create database
        self.db = lib.lightning_db_create(path.encode('utf-8'), self.config)
        if self.db is None:
            lib.lightning_db_config_free(self.config)
            raise RuntimeError("Failed to create database")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        """Close the database"""
        if hasattr(self, 'db') and self.db:
            lib.lightning_db_free(self.db)
            self.db = None
        if hasattr(self, 'config') and self.config:
            lib.lightning_db_config_free(self.config)
            self.config = None
    
    def _get_error_string(self, error: int) -> str:
        """Get error string from error code"""
        return lib.lightning_db_error_string(error).decode('utf-8')
    
    def put(self, key: bytes, value: bytes):
        """Put a key-value pair"""
        key_arr = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
        value_arr = (ctypes.c_uint8 * len(value)).from_buffer_copy(value)
        
        err = lib.lightning_db_put(self.db, key_arr, len(key), value_arr, len(value))
        if err != LightningDBError.Success:
            raise RuntimeError(f"Put failed: {self._get_error_string(err)}")
    
    def get(self, key: bytes) -> Optional[bytes]:
        """Get a value by key"""
        key_arr = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
        result = LightningDBResult()
        
        err = lib.lightning_db_get(self.db, key_arr, len(key), ctypes.byref(result))
        if err != LightningDBError.Success:
            raise RuntimeError(f"Get failed: {self._get_error_string(err)}")
        
        if result.data:
            # Copy the data
            value = bytes(result.data[i] for i in range(result.len))
            lib.lightning_db_free_result(result.data, result.len)
            return value
        return None
    
    def delete(self, key: bytes):
        """Delete a key"""
        key_arr = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
        
        err = lib.lightning_db_delete(self.db, key_arr, len(key))
        if err != LightningDBError.Success:
            raise RuntimeError(f"Delete failed: {self._get_error_string(err)}")
    
    def checkpoint(self):
        """Checkpoint the database"""
        err = lib.lightning_db_checkpoint(self.db)
        if err != LightningDBError.Success:
            raise RuntimeError(f"Checkpoint failed: {self._get_error_string(err)}")
    
    def begin_transaction(self) -> int:
        """Begin a new transaction"""
        tx_id = ctypes.c_uint64()
        err = lib.lightning_db_begin_transaction(self.db, ctypes.byref(tx_id))
        if err != LightningDBError.Success:
            raise RuntimeError(f"Begin transaction failed: {self._get_error_string(err)}")
        return tx_id.value
    
    def commit_transaction(self, tx_id: int):
        """Commit a transaction"""
        err = lib.lightning_db_commit_transaction(self.db, tx_id)
        if err != LightningDBError.Success:
            raise RuntimeError(f"Commit transaction failed: {self._get_error_string(err)}")
    
    def abort_transaction(self, tx_id: int):
        """Abort a transaction"""
        err = lib.lightning_db_abort_transaction(self.db, tx_id)
        if err != LightningDBError.Success:
            raise RuntimeError(f"Abort transaction failed: {self._get_error_string(err)}")

def main():
    """Example usage"""
    print("Lightning DB Python Example")
    print("==========================\n")
    
    # Basic usage
    with LightningDB("./python_test_db") as db:
        # Put some data
        db.put(b"hello", b"world")
        db.put(b"foo", b"bar")
        print("Put: hello = world")
        print("Put: foo = bar")
        
        # Get data
        value = db.get(b"hello")
        print(f"Get: hello = {value.decode() if value else 'not found'}")
        
        # Delete
        db.delete(b"foo")
        value = db.get(b"foo")
        print(f"After delete: foo = {value.decode() if value else 'not found'}")
        
        # Transaction
        print("\nTransaction example:")
        tx_id = db.begin_transaction()
        
        # Note: Python wrapper doesn't support transactional puts yet
        # This would require additional wrapper methods
        
        db.commit_transaction(tx_id)
        print("Transaction committed")
        
        # Performance test
        print("\nPerformance test:")
        num_ops = 10000
        
        # Write test
        start = time.time()
        for i in range(num_ops):
            key = f"key_{i:06d}".encode()
            value = f"value_{i:06d}".encode()
            db.put(key, value)
        write_time = time.time() - start
        print(f"Writes: {num_ops} ops in {write_time:.2f}s = {num_ops/write_time:.0f} ops/sec")
        
        # Read test
        start = time.time()
        for i in range(num_ops):
            key = f"key_{i:06d}".encode()
            _ = db.get(key)
        read_time = time.time() - start
        print(f"Reads: {num_ops} ops in {read_time:.2f}s = {num_ops/read_time:.0f} ops/sec")
        
        # Checkpoint
        db.checkpoint()
        print("\nDatabase checkpointed")

if __name__ == "__main__":
    main()