# Lightning DB - unwrap() Pattern Analysis and Fixes

## Common unwrap() Patterns Found

### Pattern 1: Lock Acquisitions
```rust
// BEFORE:
let guard = self.data.read().unwrap();

// AFTER:
let guard = self.data.read()
    .map_err(|_| LightningError::LockPoisoned("data lock poisoned"))?;
```

### Pattern 2: Arc::try_unwrap
```rust
// BEFORE:  
Arc::try_unwrap(page).unwrap()

// AFTER:
Arc::try_unwrap(page)
    .map_err(|_| LightningError::Internal("Cannot unwrap Arc, references still exist"))?
```

### Pattern 3: Option::unwrap on lookups
```rust
// BEFORE:
let value = map.get(&key).unwrap();

// AFTER:
let value = map.get(&key)
    .ok_or_else(|| LightningError::KeyNotFound(key.to_string()))?;
```

### Pattern 4: Path/String conversions
```rust
// BEFORE:
path.to_str().unwrap()

// AFTER:
path.to_str()
    .ok_or_else(|| LightningError::InvalidPath(path.to_path_buf()))?
```

### Pattern 5: Channel operations
```rust
// BEFORE:
tx.send(msg).unwrap();

// AFTER:
tx.send(msg)
    .map_err(|_| LightningError::ChannelClosed)?;
```

## Automated Fix Script

```bash
#!/bin/bash
# save as fix_unwraps.sh

# Pattern 1: Fix lock unwraps
echo "Fixing lock unwraps..."
sd '\.read\(\)\.unwrap\(\)' '.read().map_err(|_| LightningError::LockPoisoned("read lock"))?' src/**/*.rs
sd '\.write\(\)\.unwrap\(\)' '.write().map_err(|_| LightningError::LockPoisoned("write lock"))?' src/**/*.rs

# Pattern 2: Fix Arc::try_unwrap
echo "Fixing Arc::try_unwrap..."
sd 'Arc::try_unwrap\((.*?)\)\.unwrap\(\)' 'Arc::try_unwrap($1).map_err(|_| LightningError::Internal("Arc still has references"))?' src/**/*.rs

# Pattern 3: Fix HashMap/BTreeMap gets
echo "Fixing map lookups..."
sd '\.get\(&(.*?)\)\.unwrap\(\)' '.get(&$1).ok_or_else(|| LightningError::KeyNotFound($1.to_string()))?' src/**/*.rs

# Pattern 4: Fix path conversions
echo "Fixing path conversions..."
sd '\.to_str\(\)\.unwrap\(\)' '.to_str().ok_or_else(|| LightningError::InvalidPath)?' src/**/*.rs

# Pattern 5: Fix channel operations  
echo "Fixing channel operations..."
sd '\.send\((.*?)\)\.unwrap\(\)' '.send($1).map_err(|_| LightningError::ChannelClosed)?' src/**/*.rs
sd '\.recv\(\)\.unwrap\(\)' '.recv().map_err(|_| LightningError::ChannelClosed)?' src/**/*.rs
```

## Required Error Types to Add

```rust
// Add to src/error.rs
#[derive(Debug, thiserror::Error)]
pub enum LightningError {
    // Existing errors...
    
    #[error("Lock poisoned: {0}")]
    LockPoisoned(&'static str),
    
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    
    #[error("Invalid path: {0:?}")]
    InvalidPath(PathBuf),
    
    #[error("Channel closed")]
    ChannelClosed,
    
    #[error("Internal error: {0}")]
    Internal(&'static str),
    
    #[error("Invalid state: {0}")]
    InvalidState(String),
    
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(&'static str),
}
```

## Module-Specific Fixes

### src/lib.rs - Database Core
```rust
// Fix 1: Database creation
impl Database {
    pub fn create(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            return Err(LightningError::DatabaseExists(path.to_path_buf()));
        }
        
        fs::create_dir_all(path)
            .map_err(|e| LightningError::Io(e))?;
            
        // ... rest of implementation
    }
}

// Fix 2: Database operations
impl Database {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let guard = self.data.read()
            .map_err(|_| LightningError::LockPoisoned("database read lock"))?;
        
        // ... rest of implementation
    }
}
```

### src/transaction.rs - Transaction Management
```rust
// Fix 1: Begin transaction
pub fn begin(&self) -> Result<TransactionId> {
    let mut next_id = self.next_tx_id.lock()
        .map_err(|_| LightningError::LockPoisoned("transaction ID lock"))?;
    
    let tx_id = *next_id;
    *next_id += 1;
    
    // ... rest of implementation
    
    Ok(tx_id)
}

// Fix 2: Commit transaction
pub fn commit(&self, tx_id: TransactionId) -> Result<()> {
    let mut active = self.active_transactions.write()
        .map_err(|_| LightningError::LockPoisoned("active transactions lock"))?;
        
    let tx = active.remove(&tx_id)
        .ok_or_else(|| LightningError::TransactionNotFound(tx_id))?;
        
    // ... rest of implementation
    
    Ok(())
}
```

### src/storage/mmap_optimized.rs - Memory Mapping
```rust
// Fix 1: Memory map creation
pub fn create_mmap(file: &File, len: usize) -> Result<MmapMut> {
    unsafe {
        MmapOptions::new()
            .len(len)
            .map_mut(file)
            .map_err(|e| LightningError::Io(e))
    }
}

// Fix 2: Page access
pub fn get_page(&self, page_id: PageId) -> Result<&[u8]> {
    let offset = page_id as usize * PAGE_SIZE;
    if offset + PAGE_SIZE > self.mmap.len() {
        return Err(LightningError::InvalidPageId(page_id));
    }
    
    Ok(&self.mmap[offset..offset + PAGE_SIZE])
}
```

## Testing Strategy

### 1. Error Injection Tests
```rust
#[test]
fn test_lock_poisoning_handling() {
    let db = Database::create_temp().unwrap();
    
    // Poison a lock
    let _guard = db.data.write().unwrap();
    panic!("Poisoning the lock");
    
    // Verify error handling
    match db.get(b"key") {
        Err(LightningError::LockPoisoned(_)) => {
            // Expected
        }
        _ => panic!("Should have returned LockPoisoned error"),
    }
}
```

### 2. Fuzzing Targets
```rust
#[cfg(fuzzing)]
pub fn fuzz_database_operations(data: &[u8]) {
    let db = Database::create_temp().unwrap();
    
    // Never panic on any input
    let _ = db.put(&data[..data.len()/2], &data[data.len()/2..]);
    let _ = db.get(&data[..data.len()/2]);
    let _ = db.delete(&data[..data.len()/2]);
}
```

## Verification Commands

```bash
# Count remaining unwraps by module
for dir in src/*; do
    if [ -d "$dir" ]; then
        echo "=== $dir ==="
        rg -c "\.unwrap\(\)" "$dir" | awk -F: '{sum += $2} END {print "Total:", sum}'
    fi
done

# Find unwraps in critical paths
rg "\.unwrap\(\)" src/ -B2 -A2 | grep -E "(pub fn|impl.*for|trait)" -A5

# Verify no unwraps in public APIs
rg "pub fn.*\{" src/ -A20 | rg "\.unwrap\(\)"
```