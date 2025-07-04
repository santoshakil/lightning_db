# Example: Fixing src/lib.rs unwrap() calls

## Current State
src/lib.rs has 82 unwrap() calls, making it the highest priority file to fix.

## Step-by-Step Fix Examples

### 1. Fix Database Creation
**Before:**
```rust
impl Database {
    pub fn create(path: impl AsRef<Path>, config: LightningDbConfig) -> Self {
        let path = path.as_ref();
        fs::create_dir_all(&path).unwrap();
        
        let db = Self {
            config,
            path: path.to_path_buf(),
            // ... other fields initialized with unwrap()
        };
        
        db.initialize().unwrap();
        db
    }
}
```

**After:**
```rust
impl Database {
    pub fn create(path: impl AsRef<Path>, config: LightningDbConfig) -> Result<Self> {
        let path = path.as_ref();
        
        // Check if database already exists
        if path.exists() {
            return Err(LightningError::DatabaseExists(path.to_path_buf()));
        }
        
        // Create directory with proper error handling
        fs::create_dir_all(&path)
            .map_err(|e| LightningError::Io(e))?;
        
        let db = Self {
            config,
            path: path.to_path_buf(),
            // Initialize other fields with ? operator
        };
        
        // Initialize with error handling
        db.initialize()?;
        
        Ok(db)
    }
}
```

### 2. Fix Lock Operations
**Before:**
```rust
pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let guard = self.data.read().unwrap();
    // ... rest of implementation
}
```

**After:**
```rust
pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let guard = self.data.read()
        .map_err(|_| LightningError::LockPoisoned("database read lock"))?;
    // ... rest of implementation
}
```

### 3. Fix Test Code
**Before (in tests):**
```rust
#[test]
fn test_basic_operations() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();
    
    db.put(b"key1", b"value1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
}
```

**After (in tests):**
```rust
#[test]
fn test_basic_operations() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = Database::create(&db_path, LightningDbConfig::default())?;
    
    db.put(b"key1", b"value1")?;
    assert_eq!(db.get(b"key1")?, Some(b"value1".to_vec()));
    
    Ok(())
}
```

Or use expect for better error messages:
```rust
#[test]
fn test_basic_operations() {
    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("test.db");
    let db = Database::create(&db_path, LightningDbConfig::default())
        .expect("Failed to create database");
    
    db.put(b"key1", b"value1").expect("Failed to put key");
    assert_eq!(
        db.get(b"key1").expect("Failed to get key"), 
        Some(b"value1".to_vec())
    );
}
```

### 4. Fix Path Operations
**Before:**
```rust
let path_str = path.to_str().unwrap();
let file = File::open(path_str).unwrap();
```

**After:**
```rust
let path_str = path.to_str()
    .ok_or_else(|| LightningError::InvalidPath(path.to_path_buf()))?;
let file = File::open(path_str)?;  // std::io::Error auto-converts to LightningError
```

### 5. Fix HashMap/BTreeMap Operations
**Before:**
```rust
let value = cache.get(&key).unwrap().clone();
```

**After:**
```rust
let value = cache.get(&key)
    .ok_or_else(|| LightningError::KeyNotFound(key.to_vec()))?
    .clone();
```

### 6. Fix Channel Operations
**Before:**
```rust
tx.send(Message::Shutdown).unwrap();
let response = rx.recv().unwrap();
```

**After:**
```rust
tx.send(Message::Shutdown)
    .map_err(|_| LightningError::ChannelClosed)?;
    
let response = rx.recv()
    .map_err(|_| LightningError::ChannelClosed)?;
```

## Testing the Fixes

### 1. Error Path Testing
```rust
#[test]
fn test_create_existing_database_fails() {
    let dir = tempdir().expect("Failed to create temp dir");
    let db_path = dir.path().join("test.db");
    
    // Create database once
    let _db1 = Database::create(&db_path, LightningDbConfig::default())
        .expect("First create should succeed");
    
    // Try to create again - should fail
    match Database::create(&db_path, LightningDbConfig::default()) {
        Err(LightningError::DatabaseExists(_)) => {
            // Expected error
        }
        Ok(_) => panic!("Should not create database twice"),
        Err(e) => panic!("Wrong error type: {:?}", e),
    }
}
```

### 2. Lock Poisoning Test
```rust
#[test]
fn test_poisoned_lock_handling() {
    let db = create_test_db();
    
    // Poison the lock in a thread
    let db_clone = db.clone();
    let handle = thread::spawn(move || {
        let _guard = db_clone.data.write().unwrap();
        panic!("Poisoning the lock");
    });
    
    // Wait for panic
    let _ = handle.join();
    
    // Verify we get proper error, not panic
    match db.get(b"key") {
        Err(LightningError::LockPoisoned(_)) => {
            // Expected - properly handled poisoned lock
        }
        _ => panic!("Should return LockPoisoned error"),
    }
}
```

## Automated Fix Script for lib.rs

```bash
#!/bin/bash
# fix_lib_rs.sh

# Backup original
cp src/lib.rs src/lib.rs.backup

# Fix common patterns
sd '\.read\(\)\.unwrap\(\)' '.read().map_err(|_| LightningError::LockPoisoned("read lock"))?' src/lib.rs
sd '\.write\(\)\.unwrap\(\)' '.write().map_err(|_| LightningError::LockPoisoned("write lock"))?' src/lib.rs
sd 'tempdir\(\)\.unwrap\(\)' 'tempdir()?' src/lib.rs
sd '\.to_str\(\)\.unwrap\(\)' '.to_str().ok_or_else(|| LightningError::InvalidPath)?' src/lib.rs

# Add Result to function signatures where needed
# (This requires manual review)

echo "Automated fixes applied. Manual review required for:"
echo "1. Function signatures that need Result<T> return type"
echo "2. Error context for specific operations"
echo "3. Test functions that may need Result<()> return"
```

## Verification After Fixes

```bash
# Check remaining unwraps
echo "Remaining unwraps in lib.rs:"
grep -n "\.unwrap()" src/lib.rs | wc -l

# Compile check
cargo check

# Run tests
cargo test --lib

# Check with clippy
cargo clippy -- -D warnings
```