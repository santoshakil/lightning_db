# Lightning DB FFI Bindings

This crate provides C-compatible FFI (Foreign Function Interface) bindings for Lightning DB, allowing it to be used from C, C++, Python, and other languages that support C FFI.

## Features

- **Safe C API**: All functions handle null pointers and provide error codes
- **Opaque types**: Implementation details are hidden from C code
- **Memory safety**: Clear ownership rules and proper cleanup functions
- **Transaction support**: Full ACID transaction capabilities
- **Cross-platform**: Works on Linux, macOS, and Windows

## Building

### Prerequisites

- Rust toolchain (stable)
- C compiler (gcc, clang, or MSVC)
- Make (for examples)

### Build the library

```bash
# Build the Rust library
cargo build --release

# Generate C header
cargo build --release  # cbindgen runs automatically
```

This will create:
- Static library: `target/release/liblightning_db_ffi.a` (Linux/macOS) or `lightning_db_ffi.lib` (Windows)
- Dynamic library: `target/release/liblightning_db_ffi.so` (Linux), `.dylib` (macOS), or `.dll` (Windows)
- C header: `lightning-db-ffi/include/lightning_db_ffi.h`

### Build examples

```bash
cd lightning-db-ffi/examples
make all
```

## API Overview

### Basic Operations

```c
// Create configuration
lightning_db_config_t* config = lightning_db_config_new();
lightning_db_config_set_cache_size(config, 64 * 1024 * 1024); // 64MB

// Open database
lightning_db_t* db = lightning_db_create("./mydb", config);

// Put data
lightning_db_put(db, key, key_len, value, value_len);

// Get data
lightning_db_result_t result;
lightning_db_get(db, key, key_len, &result);
if (result.data) {
    // Use data...
    lightning_db_free_result(result.data, result.len);
}

// Delete data
lightning_db_delete(db, key, key_len);

// Cleanup
lightning_db_free(db);
lightning_db_config_free(config);
```

### Transactions

```c
uint64_t tx_id;
lightning_db_begin_transaction(db, &tx_id);

// Multiple operations...
lightning_db_put_tx(db, tx_id, key1, key1_len, value1, value1_len);
lightning_db_put_tx(db, tx_id, key2, key2_len, value2, value2_len);

// Commit or abort
lightning_db_commit_transaction(db, tx_id);
// or
lightning_db_abort_transaction(db, tx_id);
```

### Error Handling

All functions return `lightning_db_error_t`:

```c
lightning_db_error_t err = lightning_db_put(db, key, key_len, value, value_len);
if (err != Success) {
    const char* error_msg = lightning_db_error_string(err);
    fprintf(stderr, "Error: %s\n", error_msg);
}
```

## Memory Management

**Important rules:**

1. **Configuration**: Created with `lightning_db_config_new()`, freed with `lightning_db_config_free()`
2. **Database**: Created with `lightning_db_create()`/`lightning_db_open()`, freed with `lightning_db_free()`
3. **Results**: Data returned by `lightning_db_get()` must be freed with `lightning_db_free_result()`
4. **Strings**: Error strings returned by `lightning_db_error_string()` are static and should NOT be freed

## Language Bindings

### C/C++

See `examples/basic_usage.c` and `examples/performance_test.c`

### Python

Using ctypes (see `examples/python_example.py`):

```python
from lightning_db import LightningDB

with LightningDB("./mydb") as db:
    db.put(b"key", b"value")
    value = db.get(b"key")
    print(value)  # b"value"
```

### Other Languages

The C API can be used from any language that supports C FFI:
- **Ruby**: Using FFI gem
- **Node.js**: Using node-ffi or N-API
- **Java**: Using JNI or JNA
- **Go**: Using cgo
- **Rust**: Using bindgen (though native API is preferred)

## Performance

The FFI layer adds minimal overhead:
- ~100ns per operation for the FFI boundary crossing
- No additional memory allocations for keys/values
- Zero-copy where possible

See `examples/performance_test.c` for benchmarks.

## Safety

The FFI layer is designed with safety in mind:
- All pointers are checked for NULL
- All sizes are validated
- Proper error codes for all failure cases
- No undefined behavior on misuse
- Thread-safe (follows Lightning DB's thread safety guarantees)

## Platform Support

- **Linux**: x86_64, aarch64
- **macOS**: x86_64, Apple Silicon
- **Windows**: x86_64 (MSVC and MinGW)

## License

Same as Lightning DB - MIT OR Apache-2.0