# Lightning DB - Error Handling Fix Examples

## Example Fixes for Common Error Patterns

### 1. Fixing Mutex Lock Unwraps

**Before (can panic):**
```rust
// src/storage/page.rs
fn allocate_page(&self) -> Result<u32> {
    self.inner.lock().unwrap().allocate_page()
}
```

**After (handles poisoned mutex):**
```rust
fn allocate_page(&self) -> Result<u32> {
    self.inner
        .lock()
        .map_err(|_| Error::Internal("Page manager mutex poisoned".to_string()))?
        .allocate_page()
}
```

### 2. Fixing Panic in B+Tree Delete

**Before:**
```rust
// src/btree/delete.rs
panic!("Failed to insert key during redistribution");
```

**After:**
```rust
return Err(Error::Internal(format!(
    "B+Tree invariant violated: failed to insert key during redistribution. \
     Node ID: {}, Key: {:?}, Node entries: {}", 
    node_id, 
    key, 
    node.entries.len()
)));
```

### 3. Adding Context to File Operations

**Before:**
```rust
// src/backup/mod.rs
fs::create_dir_all(backup_dir)?;
```

**After:**
```rust
fs::create_dir_all(backup_dir)
    .map_err(|e| Error::Io(format!(
        "Failed to create backup directory '{}': {}", 
        backup_dir.display(), 
        e
    )))?;
```

### 4. Preserving Error Types

**Before (loses error type):**
```rust
// src/compression/mod.rs
encode_all(data, self.level).map_err(|e| Error::Compression(e.to_string()))
```

**After (preserves error chain):**
```rust
encode_all(data, self.level)
    .map_err(|e| Error::Compression(format!(
        "ZSTD compression failed at level {}: {}", 
        self.level, 
        e
    )))
```

### 5. Improving WAL Error Context

**Before:**
```rust
// src/wal/mod.rs
let mut file = self.file.lock().unwrap();
file.sync_all()
    .map_err(|e| Error::Io(e.to_string()))?;
```

**After:**
```rust
let mut file = self.file
    .lock()
    .map_err(|_| Error::Internal("WAL file mutex poisoned".to_string()))?;
    
file.sync_all()
    .map_err(|e| Error::Io(format!(
        "Failed to sync WAL to disk (fsync failed): {}. \
         This may indicate disk issues or filesystem errors.", 
        e
    )))?;
```

### 6. Transaction Error Context

**Before:**
```rust
// src/transaction/optimized_manager.rs
let results: Vec<_> = handles.into_iter()
    .map(|h| h.join().unwrap())
    .collect();
```

**After:**
```rust
let results: Vec<_> = handles
    .into_iter()
    .enumerate()
    .map(|(idx, h)| {
        h.join()
            .map_err(|_| Error::Internal(format!(
                "Transaction worker thread {} panicked", 
                idx
            )))
    })
    .collect::<Result<Vec<_>>>()?;
```

### 7. Timestamp Handling

**Before:**
```rust
// src/wal/mod.rs
let timestamp = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64;
```

**After:**
```rust
let timestamp = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map_err(|e| Error::Internal(format!(
        "System time is before UNIX epoch: {}. \
         This indicates a serious system clock issue.", 
        e
    )))?
    .as_millis() as u64;
```

### 8. Enhanced Error Types

**Add to src/error.rs:**
```rust
use std::fmt;

#[derive(Debug)]
pub struct ErrorContext {
    pub operation: String,
    pub details: String,
    pub recovery_hint: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error during {context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    
    #[error("Storage error in page {page_id}: {message}")]
    Storage {
        page_id: u32,
        message: String,
    },
    
    #[error("Transaction {tx_id} failed at {phase}: {reason}")]
    Transaction {
        tx_id: u64,
        phase: String,
        reason: String,
    },
    
    #[error("WAL error at LSN {lsn}: {message}")]
    Wal {
        lsn: u64,
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    
    #[error("Lock acquisition failed: {resource}")]
    LockTimeout {
        resource: String,
        waited: std::time::Duration,
    },
    
    #[error("Data corruption detected: {details}")]
    Corruption {
        details: String,
        recovery_hint: String,
    },
}
```

### 9. Result Extension Trait for Context

**Add utility trait:**
```rust
pub trait ResultExt<T> {
    fn with_context_lazy<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String;
}

impl<T, E> ResultExt<T> for Result<T, E>
where
    E: Into<Error>,
{
    fn with_context_lazy<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| {
            let base_error = e.into();
            Error::WithContext {
                context: f(),
                source: Box::new(base_error),
            }
        })
    }
}

// Usage:
page_manager
    .get_page(page_id)
    .with_context_lazy(|| format!(
        "Failed to load page {} during compaction phase", 
        page_id
    ))?;
```

### 10. Structured Error Logging

**Add error logging helper:**
```rust
use tracing::{error, warn};

pub fn log_and_convert_error<E: std::error::Error>(
    operation: &str,
    error: E,
) -> Error {
    error!(
        operation = %operation,
        error = %error,
        backtrace = ?error.backtrace(),
        "Operation failed"
    );
    
    Error::Internal(format!("{}: {}", operation, error))
}

// Usage:
file.sync_all()
    .map_err(|e| log_and_convert_error("WAL sync", e))?;
```

## Testing Error Conditions

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_context_preserved() {
        let page_id = 42;
        let error = Error::Storage {
            page_id,
            message: "Checksum mismatch".to_string(),
        };
        
        let error_string = error.to_string();
        assert!(error_string.contains(&page_id.to_string()));
        assert!(error_string.contains("Checksum mismatch"));
    }
    
    #[test]
    fn test_poisoned_mutex_handling() {
        use std::sync::Mutex;
        use std::thread;
        
        let mutex = Arc::new(Mutex::new(42));
        let mutex_clone = mutex.clone();
        
        // Poison the mutex
        let _ = thread::spawn(move || {
            let _lock = mutex_clone.lock().unwrap();
            panic!("Poisoning mutex");
        }).join();
        
        // Verify we handle poisoned mutex gracefully
        let result = mutex.lock()
            .map_err(|_| Error::Internal("Mutex poisoned".to_string()));
            
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Mutex poisoned"));
    }
}
```

## Migration Strategy

1. **Phase 1**: Fix all `panic!` calls (1 day)
2. **Phase 2**: Fix mutex `.unwrap()` calls (1 day)  
3. **Phase 3**: Add context to I/O operations (2 days)
4. **Phase 4**: Enhance error types (1 day)
5. **Phase 5**: Add comprehensive error tests (2 days)

Total estimated time: 1 week for production-ready error handling