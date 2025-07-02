# Lightning DB FFI Integration Guide

This guide provides step-by-step instructions for integrating Lightning DB into your application using the FFI bindings.

## Quick Start

### 1. Choose Your Language

Lightning DB provides FFI bindings that work with any language that supports C FFI:

- **C/C++**: Native support, see [C Integration](#c-integration)
- **Python**: Using ctypes, see [Python Integration](#python-integration)
- **Dart/Flutter**: Native Dart package, see [Dart Integration](#dart-integration)
- **Other Languages**: Follow the [Generic Integration](#generic-integration) guide

### 2. Get the Library

#### Pre-built Binaries
Download pre-built binaries from the releases page (when available).

#### Build from Source
```bash
# Clone the repository
git clone https://github.com/your-org/lightning_db.git
cd lightning_db/lightning-db-ffi

# Build the FFI library
cargo build --release

# Find the library in:
# - Linux: target/release/liblightning_db_ffi.so
# - macOS: target/release/liblightning_db_ffi.dylib
# - Windows: target/release/lightning_db_ffi.dll
```

## C Integration

### Basic Setup

1. Copy the header file:
```bash
cp include/lightning_db_ffi.h /path/to/your/project/
```

2. Copy the library:
```bash
# Linux
cp target/release/liblightning_db_ffi.so /path/to/your/project/

# macOS
cp target/release/liblightning_db_ffi.dylib /path/to/your/project/

# Windows
cp target/release/lightning_db_ffi.dll /path/to/your/project/
```

### Compilation

```bash
# Static linking
gcc -o myapp myapp.c -L. -llightning_db_ffi -lpthread -ldl -lm

# Dynamic linking with rpath (Linux/macOS)
gcc -o myapp myapp.c -L. -llightning_db_ffi -Wl,-rpath,'$ORIGIN'
```

### Example Code

```c
#include <stdio.h>
#include <string.h>
#include "lightning_db_ffi.h"

int main() {
    // Create database
    LightningDB* db = lightning_db_create("./mydb", NULL);
    if (!db) {
        fprintf(stderr, "Failed to create database\n");
        return 1;
    }
    
    // Store data
    const char* key = "hello";
    const char* value = "world";
    LightningDBError err = lightning_db_put(
        db,
        (const uint8_t*)key, strlen(key),
        (const uint8_t*)value, strlen(value)
    );
    
    if (err != Success) {
        fprintf(stderr, "Put failed: %s\n", lightning_db_error_string(err));
        lightning_db_free(db);
        return 1;
    }
    
    // Retrieve data
    LightningDBResult result;
    err = lightning_db_get(
        db,
        (const uint8_t*)key, strlen(key),
        &result
    );
    
    if (err == Success && result.data) {
        printf("Value: %.*s\n", (int)result.len, result.data);
        lightning_db_free_result(result.data, result.len);
    }
    
    // Clean up
    lightning_db_free(db);
    return 0;
}
```

## Python Integration

### Installation

```python
# Copy the library to your project or system library path
# Or set LD_LIBRARY_PATH (Linux) / DYLD_LIBRARY_PATH (macOS)
```

### Example Code

```python
import ctypes
import os
from enum import IntEnum

# Load the library
if sys.platform == 'darwin':
    lib = ctypes.CDLL('./liblightning_db_ffi.dylib')
elif sys.platform == 'linux':
    lib = ctypes.CDLL('./liblightning_db_ffi.so')
else:
    lib = ctypes.CDLL('./lightning_db_ffi.dll')

# Define error codes
class LightningDBError(IntEnum):
    Success = 0
    NullPointer = -1
    InvalidUtf8 = -2
    IoError = -3
    # ... etc

# Define result structure
class LightningDBResult(ctypes.Structure):
    _fields_ = [
        ('data', ctypes.POINTER(ctypes.c_uint8)),
        ('len', ctypes.c_size_t)
    ]

# Set up function signatures
lib.lightning_db_create.restype = ctypes.c_void_p
lib.lightning_db_create.argtypes = [ctypes.c_char_p, ctypes.c_void_p]

lib.lightning_db_put.restype = ctypes.c_int
lib.lightning_db_put.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t
]

# Use the database
db = lib.lightning_db_create(b"./mydb", None)
if not db:
    raise Exception("Failed to create database")

# Store data
key = b"hello"
value = b"world"
err = lib.lightning_db_put(
    db,
    ctypes.cast(key, ctypes.POINTER(ctypes.c_uint8)), len(key),
    ctypes.cast(value, ctypes.POINTER(ctypes.c_uint8)), len(value)
)

if err != LightningDBError.Success:
    raise Exception(f"Put failed: {err}")

# Clean up
lib.lightning_db_free(db)
```

See `examples/python_example.py` for a complete implementation.

## Dart Integration

### Installation

Add to your `pubspec.yaml`:

```yaml
dependencies:
  lightning_db:
    path: /path/to/lightning_db/lightning-db-ffi/dart_bindings
```

### Example Code

```dart
import 'dart:convert';
import 'package:lightning_db/lightning_db.dart';

void main() {
  // Create database
  final db = LightningDB.create('./mydb');
  
  try {
    // Store data
    db.put(utf8.encode('hello'), utf8.encode('world'));
    
    // Retrieve data
    final value = db.get(utf8.encode('hello'));
    if (value != null) {
      print('Value: ${utf8.decode(value)}');
    }
    
    // Use transactions
    final txId = db.beginTransaction();
    db.putInTransaction(txId, utf8.encode('key1'), utf8.encode('value1'));
    db.putInTransaction(txId, utf8.encode('key2'), utf8.encode('value2'));
    db.commitTransaction(txId);
    
  } finally {
    db.close();
  }
}
```

### Flutter-specific Setup

#### iOS
Add to your `ios/Runner/Info.plist`:
```xml
<key>NSAppTransportSecurity</key>
<dict>
    <key>NSAllowsArbitraryLoads</key>
    <true/>
</dict>
```

#### Android
Add to your `android/app/build.gradle`:
```gradle
android {
    sourceSets {
        main {
            jniLibs.srcDirs += ['path/to/lightning_db/android']
        }
    }
}
```

## Generic Integration

For other languages, follow these steps:

### 1. Understand the C API

Key structures:
```c
// Opaque types (pointers)
typedef struct LightningDB LightningDB;
typedef struct LightningDBConfig LightningDBConfig;

// Result structure
typedef struct LightningDBResult {
    uint8_t* data;
    size_t len;
} LightningDBResult;

// Error enum
typedef enum LightningDBError {
    Success = 0,
    NullPointer = -1,
    // ...
} LightningDBError;
```

### 2. Load the Library

Most languages provide a way to load dynamic libraries:
- **Ruby**: `FFI::Library`
- **Node.js**: `ffi-napi` or N-API
- **Java**: JNA or JNI
- **C#**: P/Invoke
- **Go**: `cgo`
- **Rust**: `bindgen` (though native API is preferred)

### 3. Define Function Signatures

Map the C function signatures to your language's FFI system:

```c
// C signature
LightningDB* lightning_db_create(const char* path, const LightningDBConfig* config);

// Example: Ruby FFI
attach_function :lightning_db_create, [:string, :pointer], :pointer

// Example: Node.js ffi-napi
'lightning_db_create': ['pointer', ['string', 'pointer']]
```

### 4. Handle Memory Management

Follow these rules:
1. Configs: `lightning_db_config_new()` → `lightning_db_config_free()`
2. Databases: `lightning_db_create()`/`open()` → `lightning_db_free()`
3. Results: `lightning_db_get()` → `lightning_db_free_result()`
4. Error strings: Do NOT free (they're static)

### 5. Error Handling

Always check return values:
```python
err = lib.some_operation(...)
if err != 0:  # Success = 0
    error_str = lib.lightning_db_error_string(err)
    raise Exception(f"Operation failed: {error_str}")
```

## Performance Considerations

### Memory Usage

- **Default cache**: 64MB
- **Minimum practical**: 10MB
- **Formula**: Cache size = working set size × 1.2

### Optimal Patterns

1. **Batch writes in transactions**:
```c
uint64_t tx_id;
lightning_db_begin_transaction(db, &tx_id);
for (int i = 0; i < 1000; i++) {
    lightning_db_put_tx(db, tx_id, keys[i], key_lens[i], 
                        values[i], value_lens[i]);
}
lightning_db_commit_transaction(db, tx_id);
```

2. **Reuse database handles**: Don't open/close repeatedly
3. **Use appropriate key sizes**: Shorter keys = better performance
4. **Tune cache size**: Monitor hit rate and adjust

## Troubleshooting

### Common Issues

1. **Library not found**
   - Check library path
   - Set `LD_LIBRARY_PATH` (Linux) or `DYLD_LIBRARY_PATH` (macOS)
   - Use absolute paths during development

2. **Segmentation faults**
   - Check for null pointers
   - Ensure proper memory management
   - Verify data lifetimes

3. **Database locked errors**
   - Only open database once per process
   - Check for unclosed handles
   - Ensure proper cleanup on exit

4. **Performance issues**
   - Increase cache size
   - Use transactions for bulk operations
   - Check key distribution

### Debug Tips

1. Enable debug logging:
```c
// Set RUST_LOG=lightning_db=debug before running
```

2. Check error details:
```c
if (err != Success) {
    const char* msg = lightning_db_error_string(err);
    fprintf(stderr, "Error %d: %s\n", err, msg);
}
```

3. Verify library loading:
```python
# Python
import ctypes
lib = ctypes.CDLL('./liblightning_db_ffi.so')
print(lib)  # Should show library handle
```

## Advanced Topics

### Custom Memory Allocators

Lightning DB uses standard system allocation. For custom allocators, modify the Rust source.

### Multi-process Access

Lightning DB supports single-writer, multiple-reader access:
- One process can write
- Multiple processes can read
- Use file locking for coordination

### Backup and Recovery

1. **Hot backup**: Use checkpoint + file copy
2. **Recovery**: Automatic on open
3. **Verification**: Use integrity check tools

### Migration from Other Databases

#### From SQLite
```python
import sqlite3
import lightning_db

# Open both databases
sqlite_conn = sqlite3.connect('old.db')
ldb = lightning_db.create('new.ldb')

# Migrate data
cursor = sqlite_conn.execute('SELECT key, value FROM mytable')
for key, value in cursor:
    ldb.put(key.encode(), value.encode())

# Clean up
sqlite_conn.close()
ldb.close()
```

#### From RocksDB
Similar pattern - iterate and copy.

## Support

- **Documentation**: See FFI_API_REFERENCE.md
- **Examples**: Check examples/ directory
- **Issues**: File on GitHub
- **Performance**: See benchmarks/ directory