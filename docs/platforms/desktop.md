# Desktop Integration Guide

## Overview

Lightning DB provides native desktop integration across Windows, macOS, and Linux. This guide covers integration with various desktop application frameworks including Tauri, Electron, and native applications.

## Prerequisites

- **Windows**: Windows 10+ with Visual Studio Build Tools
- **macOS**: macOS 10.15+ with Xcode Command Line Tools
- **Linux**: Ubuntu 18.04+ with GCC/Clang

## Platform-Specific Setup

### Windows

```bash
# Install Visual Studio Build Tools
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Add Windows targets
rustup target add x86_64-pc-windows-msvc
rustup target add i686-pc-windows-msvc
```

### macOS

```bash
# Install Xcode Command Line Tools
xcode-select --install

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Add macOS targets
rustup target add x86_64-apple-darwin
rustup target add aarch64-apple-darwin
```

### Linux

```bash
# Install dependencies
sudo apt update
sudo apt install build-essential pkg-config libssl-dev

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Add Linux targets
rustup target add x86_64-unknown-linux-gnu
rustup target add aarch64-unknown-linux-gnu
```

## Tauri Integration

### Setup

```json
// package.json
{
  "name": "lightning-db-desktop",
  "version": "1.0.0",
  "scripts": {
    "tauri": "tauri",
    "tauri:dev": "tauri dev",
    "tauri:build": "tauri build"
  },
  "dependencies": {
    "@tauri-apps/api": "^1.5.0"
  },
  "devDependencies": {
    "@tauri-apps/cli": "^1.5.0"
  }
}
```

```toml
# src-tauri/Cargo.toml
[package]
name = "lightning-db-desktop"
version = "1.0.0"
edition = "2021"

[dependencies]
tauri = { version = "1.5", features = ["api-all"] }
lightning_db = { path = "../../../", features = ["desktop"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
tokio = { version = "1.0", features = ["full"] }
```

### Rust Backend

```rust
// src-tauri/src/main.rs
use lightning_db::{Database, LightningDbConfig};
use std::sync::Mutex;
use tauri::State;

struct DatabaseState {
    db: Mutex<Option<Database>>,
}

#[tauri::command]
async fn initialize_database(state: State<'_, DatabaseState>) -> Result<(), String> {
    let app_data_dir = tauri::api::path::app_data_dir(&tauri::Config::default())
        .ok_or("Failed to get app data directory")?;
    
    let db_path = app_data_dir.join("lightning_db");
    std::fs::create_dir_all(&db_path).map_err(|e| e.to_string())?;
    
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024, // 100MB
        compression_enabled: true,
        use_optimized_page_manager: true,
        ..Default::default()
    };
    
    let db = Database::create(db_path, config).map_err(|e| e.to_string())?;
    
    let mut db_state = state.db.lock().unwrap();
    *db_state = Some(db);
    
    Ok(())
}

#[tauri::command]
async fn db_put(
    key: String,
    value: String,
    state: State<'_, DatabaseState>,
) -> Result<(), String> {
    let db_state = state.db.lock().unwrap();
    let db = db_state.as_ref().ok_or("Database not initialized")?;
    
    db.put(key.as_bytes(), value.as_bytes())
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn db_get(key: String, state: State<'_, DatabaseState>) -> Result<Option<String>, String> {
    let db_state = state.db.lock().unwrap();
    let db = db_state.as_ref().ok_or("Database not initialized")?;
    
    match db.get(key.as_bytes()) {
        Ok(Some(value)) => Ok(Some(String::from_utf8_lossy(&value).to_string())),
        Ok(None) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

#[tauri::command]
async fn db_delete(key: String, state: State<'_, DatabaseState>) -> Result<(), String> {
    let db_state = state.db.lock().unwrap();
    let db = db_state.as_ref().ok_or("Database not initialized")?;
    
    db.delete(key.as_bytes()).map_err(|e| e.to_string())
}

#[tauri::command]
async fn db_transaction(
    operations: Vec<TransactionOperation>,
    state: State<'_, DatabaseState>,
) -> Result<(), String> {
    let db_state = state.db.lock().unwrap();
    let db = db_state.as_ref().ok_or("Database not initialized")?;
    
    let tx_id = db.begin_transaction().map_err(|e| e.to_string())?;
    
    for op in operations {
        match op.operation_type.as_str() {
            "put" => {
                db.put_tx(tx_id, op.key.as_bytes(), op.value.as_bytes())
                    .map_err(|e| e.to_string())?;
            }
            "delete" => {
                db.delete_tx(tx_id, op.key.as_bytes())
                    .map_err(|e| e.to_string())?;
            }
            _ => return Err("Unknown operation type".to_string()),
        }
    }
    
    db.commit_transaction(tx_id).map_err(|e| e.to_string())
}

#[derive(serde::Deserialize)]
struct TransactionOperation {
    operation_type: String,
    key: String,
    value: String,
}

#[tokio::main]
async fn main() {
    tauri::Builder::default()
        .manage(DatabaseState {
            db: Mutex::new(None),
        })
        .invoke_handler(tauri::generate_handler![
            initialize_database,
            db_put,
            db_get,
            db_delete,
            db_transaction
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

### Frontend Integration

```typescript
// src/database.ts
import { invoke } from '@tauri-apps/api/tauri';

export interface TransactionOperation {
  operation_type: 'put' | 'delete';
  key: string;
  value: string;
}

export class LightningDB {
  private initialized = false;
  
  async initialize(): Promise<void> {
    if (this.initialized) return;
    
    try {
      await invoke('initialize_database');
      this.initialized = true;
    } catch (error) {
      throw new Error(`Failed to initialize database: ${error}`);
    }
  }
  
  async put(key: string, value: string): Promise<void> {
    await this.ensureInitialized();
    await invoke('db_put', { key, value });
  }
  
  async get(key: string): Promise<string | null> {
    await this.ensureInitialized();
    return await invoke('db_get', { key });
  }
  
  async delete(key: string): Promise<void> {
    await this.ensureInitialized();
    await invoke('db_delete', { key });
  }
  
  async transaction(operations: TransactionOperation[]): Promise<void> {
    await this.ensureInitialized();
    await invoke('db_transaction', { operations });
  }
  
  private async ensureInitialized(): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }
  }
}
```

```typescript
// src/App.tsx
import React, { useEffect, useState } from 'react';
import { LightningDB } from './database';

const App: React.FC = () => {
  const [db, setDb] = useState<LightningDB | null>(null);
  const [data, setData] = useState<string>('');
  const [key, setKey] = useState<string>('');
  const [value, setValue] = useState<string>('');
  
  useEffect(() => {
    const initDb = async () => {
      const database = new LightningDB();
      await database.initialize();
      setDb(database);
    };
    
    initDb().catch(console.error);
  }, []);
  
  const handlePut = async () => {
    if (!db || !key || !value) return;
    
    try {
      await db.put(key, value);
      setData(`Saved: ${key} = ${value}`);
    } catch (error) {
      setData(`Error: ${error}`);
    }
  };
  
  const handleGet = async () => {
    if (!db || !key) return;
    
    try {
      const result = await db.get(key);
      setData(result ? `Retrieved: ${key} = ${result}` : `Key not found: ${key}`);
    } catch (error) {
      setData(`Error: ${error}`);
    }
  };
  
  const handleTransaction = async () => {
    if (!db) return;
    
    try {
      await db.transaction([
        { operation_type: 'put', key: 'tx_key1', value: 'tx_value1' },
        { operation_type: 'put', key: 'tx_key2', value: 'tx_value2' },
        { operation_type: 'delete', key: 'old_key' },
      ]);
      setData('Transaction completed successfully');
    } catch (error) {
      setData(`Transaction failed: ${error}`);
    }
  };
  
  return (
    <div className="App">
      <h1>Lightning DB Desktop App</h1>
      
      <div>
        <input
          type="text"
          placeholder="Key"
          value={key}
          onChange={(e) => setKey(e.target.value)}
        />
        <input
          type="text"
          placeholder="Value"
          value={value}
          onChange={(e) => setValue(e.target.value)}
        />
      </div>
      
      <div>
        <button onClick={handlePut}>Put</button>
        <button onClick={handleGet}>Get</button>
        <button onClick={handleTransaction}>Run Transaction</button>
      </div>
      
      <div>
        <h3>Result:</h3>
        <pre>{data}</pre>
      </div>
    </div>
  );
};

export default App;
```

## Electron Integration

### Setup

```json
// package.json
{
  "name": "lightning-db-electron",
  "version": "1.0.0",
  "main": "dist/main.js",
  "scripts": {
    "start": "electron .",
    "build": "webpack --mode production",
    "dev": "concurrently \"webpack --mode development --watch\" \"electron .\"",
    "rebuild": "electron-rebuild"
  },
  "dependencies": {
    "electron": "^27.0.0",
    "lightning-db-node": "^0.1.0"
  },
  "devDependencies": {
    "electron-rebuild": "^3.2.0",
    "webpack": "^5.0.0",
    "typescript": "^5.0.0"
  }
}
```

### Main Process

```typescript
// src/main.ts
import { app, BrowserWindow, ipcMain } from 'electron';
import * as path from 'path';
import { LightningDB } from 'lightning-db-node';

let mainWindow: BrowserWindow;
let db: LightningDB | null = null;

const createWindow = (): void => {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js'),
    },
  });

  mainWindow.loadFile('index.html');
};

app.whenReady().then(() => {
  createWindow();
  
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

// Database IPC handlers
ipcMain.handle('db-initialize', async () => {
  try {
    const userDataPath = app.getPath('userData');
    const dbPath = path.join(userDataPath, 'lightning_db');
    
    const config = {
      cache_size: 100 * 1024 * 1024, // 100MB
      compression_enabled: true,
      sync_mode: 'async',
    };
    
    db = new LightningDB(dbPath, config);
    return { success: true };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('db-put', async (_, key: string, value: string) => {
  try {
    if (!db) throw new Error('Database not initialized');
    await db.put(key, value);
    return { success: true };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('db-get', async (_, key: string) => {
  try {
    if (!db) throw new Error('Database not initialized');
    const value = await db.get(key);
    return { success: true, value };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('db-delete', async (_, key: string) => {
  try {
    if (!db) throw new Error('Database not initialized');
    await db.delete(key);
    return { success: true };
  } catch (error) {
    return { success: false, error: error.message };
  }
});

ipcMain.handle('db-transaction', async (_, operations) => {
  try {
    if (!db) throw new Error('Database not initialized');
    
    const tx = await db.beginTransaction();
    
    try {
      for (const op of operations) {
        switch (op.type) {
          case 'put':
            await tx.put(op.key, op.value);
            break;
          case 'delete':
            await tx.delete(op.key);
            break;
        }
      }
      
      await tx.commit();
      return { success: true };
    } catch (error) {
      await tx.rollback();
      throw error;
    }
  } catch (error) {
    return { success: false, error: error.message };
  }
});
```

### Preload Script

```typescript
// src/preload.ts
import { contextBridge, ipcRenderer } from 'electron';

contextBridge.exposeInMainWorld('electronAPI', {
  database: {
    initialize: () => ipcRenderer.invoke('db-initialize'),
    put: (key: string, value: string) => ipcRenderer.invoke('db-put', key, value),
    get: (key: string) => ipcRenderer.invoke('db-get', key),
    delete: (key: string) => ipcRenderer.invoke('db-delete', key),
    transaction: (operations: any[]) => ipcRenderer.invoke('db-transaction', operations),
  },
});
```

### Renderer Process

```typescript
// src/renderer.ts
declare global {
  interface Window {
    electronAPI: {
      database: {
        initialize(): Promise<{ success: boolean; error?: string }>;
        put(key: string, value: string): Promise<{ success: boolean; error?: string }>;
        get(key: string): Promise<{ success: boolean; value?: string; error?: string }>;
        delete(key: string): Promise<{ success: boolean; error?: string }>;
        transaction(operations: any[]): Promise<{ success: boolean; error?: string }>;
      };
    };
  }
}

class ElectronLightningDB {
  private initialized = false;
  
  async initialize(): Promise<void> {
    if (this.initialized) return;
    
    const result = await window.electronAPI.database.initialize();
    if (!result.success) {
      throw new Error(result.error);
    }
    
    this.initialized = true;
  }
  
  async put(key: string, value: string): Promise<void> {
    await this.ensureInitialized();
    
    const result = await window.electronAPI.database.put(key, value);
    if (!result.success) {
      throw new Error(result.error);
    }
  }
  
  async get(key: string): Promise<string | null> {
    await this.ensureInitialized();
    
    const result = await window.electronAPI.database.get(key);
    if (!result.success) {
      throw new Error(result.error);
    }
    
    return result.value || null;
  }
  
  async delete(key: string): Promise<void> {
    await this.ensureInitialized();
    
    const result = await window.electronAPI.database.delete(key);
    if (!result.success) {
      throw new Error(result.error);
    }
  }
  
  async transaction(operations: Array<{ type: 'put' | 'delete'; key: string; value?: string }>): Promise<void> {
    await this.ensureInitialized();
    
    const result = await window.electronAPI.database.transaction(operations);
    if (!result.success) {
      throw new Error(result.error);
    }
  }
  
  private async ensureInitialized(): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }
  }
}

// Usage
const db = new ElectronLightningDB();

document.addEventListener('DOMContentLoaded', async () => {
  await db.initialize();
  
  // Set up event listeners
  document.getElementById('put-btn')?.addEventListener('click', async () => {
    const key = (document.getElementById('key-input') as HTMLInputElement).value;
    const value = (document.getElementById('value-input') as HTMLInputElement).value;
    
    try {
      await db.put(key, value);
      updateStatus(`Saved: ${key} = ${value}`);
    } catch (error) {
      updateStatus(`Error: ${error}`);
    }
  });
  
  // Additional event listeners...
});

function updateStatus(message: string): void {
  const statusEl = document.getElementById('status');
  if (statusEl) {
    statusEl.textContent = message;
  }
}
```

## Native C++ Integration

### CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.15)
project(LightningDBDesktop)

set(CMAKE_CXX_STANDARD 17)

# Find Lightning DB
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIGHTNING_DB REQUIRED lightning-db)

# Find GUI framework (e.g., Qt)
find_package(Qt6 REQUIRED COMPONENTS Core Widgets)

# Include directories
include_directories(${LIGHTNING_DB_INCLUDE_DIRS})

# Source files
set(SOURCES
    src/main.cpp
    src/database_manager.cpp
    src/main_window.cpp
)

# Create executable
add_executable(${PROJECT_NAME} ${SOURCES})

# Link libraries
target_link_libraries(${PROJECT_NAME} 
    Qt6::Core
    Qt6::Widgets
    ${LIGHTNING_DB_LIBRARIES}
)
```

### Database Manager (C++)

```cpp
// src/database_manager.h
#pragma once

#include <lightning_db/lightning_db.h>
#include <memory>
#include <string>
#include <optional>

class DatabaseManager {
public:
    DatabaseManager();
    ~DatabaseManager();
    
    bool initialize(const std::string& db_path);
    bool put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    bool remove(const std::string& key);
    
    bool beginTransaction();
    bool commitTransaction();
    bool rollbackTransaction();
    
    bool putTx(const std::string& key, const std::string& value);
    bool deleteTx(const std::string& key);
    
private:
    std::unique_ptr<lightning_db::Database> db_;
    std::optional<uint64_t> current_tx_;
};
```

```cpp
// src/database_manager.cpp
#include "database_manager.h"
#include <iostream>

DatabaseManager::DatabaseManager() = default;
DatabaseManager::~DatabaseManager() = default;

bool DatabaseManager::initialize(const std::string& db_path) {
    try {
        lightning_db::Config config;
        config.cache_size = 100 * 1024 * 1024; // 100MB
        config.compression_enabled = true;
        
        db_ = std::make_unique<lightning_db::Database>(db_path, config);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize database: " << e.what() << std::endl;
        return false;
    }
}

bool DatabaseManager::put(const std::string& key, const std::string& value) {
    try {
        return db_->put(key, value);
    } catch (const std::exception& e) {
        std::cerr << "Put failed: " << e.what() << std::endl;
        return false;
    }
}

std::optional<std::string> DatabaseManager::get(const std::string& key) {
    try {
        return db_->get(key);
    } catch (const std::exception& e) {
        std::cerr << "Get failed: " << e.what() << std::endl;
        return std::nullopt;
    }
}

bool DatabaseManager::remove(const std::string& key) {
    try {
        return db_->remove(key);
    } catch (const std::exception& e) {
        std::cerr << "Remove failed: " << e.what() << std::endl;
        return false;
    }
}

bool DatabaseManager::beginTransaction() {
    try {
        current_tx_ = db_->begin_transaction();
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Begin transaction failed: " << e.what() << std::endl;
        return false;
    }
}

bool DatabaseManager::commitTransaction() {
    if (!current_tx_) return false;
    
    try {
        bool result = db_->commit_transaction(*current_tx_);
        current_tx_.reset();
        return result;
    } catch (const std::exception& e) {
        std::cerr << "Commit transaction failed: " << e.what() << std::endl;
        return false;
    }
}

bool DatabaseManager::rollbackTransaction() {
    if (!current_tx_) return false;
    
    try {
        bool result = db_->rollback_transaction(*current_tx_);
        current_tx_.reset();
        return result;
    } catch (const std::exception& e) {
        std::cerr << "Rollback transaction failed: " << e.what() << std::endl;
        return false;
    }
}

bool DatabaseManager::putTx(const std::string& key, const std::string& value) {
    if (!current_tx_) return false;
    
    try {
        return db_->put_tx(*current_tx_, key, value);
    } catch (const std::exception& e) {
        std::cerr << "Transaction put failed: " << e.what() << std::endl;
        return false;
    }
}

bool DatabaseManager::deleteTx(const std::string& key) {
    if (!current_tx_) return false;
    
    try {
        return db_->delete_tx(*current_tx_, key);
    } catch (const std::exception& e) {
        std::cerr << "Transaction delete failed: " << e.what() << std::endl;
        return false;
    }
}
```

## Performance Optimization

### Configuration Tuning

```rust
// High-performance desktop configuration
let config = LightningDbConfig {
    cache_size: 1024 * 1024 * 1024, // 1GB cache for desktop
    compression_enabled: true,
    use_optimized_page_manager: true,
    use_improved_wal: true,
    wal_sync_mode: WalSyncMode::Async,
    max_concurrent_transactions: 1000,
    prefetch_enabled: true,
    mmap_config: Some(MmapConfig {
        enable_huge_pages: true,
        enable_prefault: true,
        enable_async_msync: true,
        max_mapped_regions: 64,
        region_size: 64 * 1024 * 1024, // 64MB regions
        ..Default::default()
    }),
    ..Default::default()
};
```

### Memory Management

```rust
// Memory monitoring and management
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct MemoryMonitor {
    current_usage: AtomicUsize,
    max_usage: usize,
}

impl MemoryMonitor {
    fn new(max_usage: usize) -> Self {
        Self {
            current_usage: AtomicUsize::new(0),
            max_usage,
        }
    }
    
    fn check_memory(&self) -> bool {
        self.current_usage.load(Ordering::Relaxed) < self.max_usage
    }
    
    fn update_usage(&self, size: usize) {
        self.current_usage.store(size, Ordering::Relaxed);
    }
}

// Usage in your application
let memory_monitor = Arc::new(MemoryMonitor::new(2 * 1024 * 1024 * 1024)); // 2GB limit

// Periodic memory check
let monitor = memory_monitor.clone();
std::thread::spawn(move || {
    loop {
        if !monitor.check_memory() {
            // Trigger garbage collection or cache cleanup
            println!("Memory usage high, triggering cleanup");
        }
        std::thread::sleep(std::time::Duration::from_secs(30));
    }
});
```

### Background Processing

```rust
// Background database maintenance
use tokio::time::{interval, Duration};

async fn background_maintenance(db: Arc<Database>) {
    let mut interval = interval(Duration::from_secs(300)); // 5 minutes
    
    loop {
        interval.tick().await;
        
        // Perform maintenance tasks
        if let Err(e) = db.checkpoint() {
            eprintln!("Checkpoint failed: {}", e);
        }
        
        if let Err(e) = db.compact() {
            eprintln!("Compaction failed: {}", e);
        }
        
        // Update statistics
        if let Ok(stats) = db.get_stats() {
            println!("DB Stats: {} keys, {} MB", stats.total_keys, stats.total_size / (1024 * 1024));
        }
    }
}
```

## Best Practices

### 1. Application Lifecycle Management

```rust
// Proper initialization and cleanup
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct AppDatabase {
    db: Arc<RwLock<Option<Database>>>,
}

impl AppDatabase {
    pub fn new() -> Self {
        Self {
            db: Arc::new(RwLock::new(None)),
        }
    }
    
    pub async fn initialize(&self, app_data_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
        let db_path = std::path::Path::new(app_data_dir).join("database");
        std::fs::create_dir_all(&db_path)?;
        
        let config = LightningDbConfig {
            cache_size: 200 * 1024 * 1024, // 200MB
            compression_enabled: true,
            ..Default::default()
        };
        
        let database = Database::create(db_path, config)?;
        
        let mut db_guard = self.db.write().await;
        *db_guard = Some(database);
        
        Ok(())
    }
    
    pub async fn shutdown(&self) {
        let mut db_guard = self.db.write().await;
        if let Some(db) = db_guard.take() {
            // Perform final checkpoint
            let _ = db.checkpoint();
            // Database will be closed when dropped
        }
    }
    
    pub async fn get_database(&self) -> Option<Database> {
        let db_guard = self.db.read().await;
        db_guard.clone()
    }
}
```

### 2. Error Handling and Recovery

```rust
// Robust error handling
use std::time::Duration;
use tokio::time::sleep;

async fn robust_operation<T, F>(
    operation: F,
    max_retries: usize,
    base_delay: Duration,
) -> Result<T, Box<dyn std::error::Error>>
where
    F: Fn() -> Result<T, Box<dyn std::error::Error>>,
{
    let mut attempts = 0;
    
    loop {
        match operation() {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                if attempts >= max_retries {
                    return Err(e);
                }
                
                // Exponential backoff
                let delay = base_delay * 2_u32.pow(attempts as u32 - 1);
                sleep(delay).await;
            }
        }
    }
}

// Usage
let result = robust_operation(
    || db.put("key", "value"),
    3,
    Duration::from_millis(100),
).await?;
```

### 3. Testing

```rust
// Desktop application testing
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_desktop_database() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        
        let config = LightningDbConfig::default();
        let db = Database::create(&db_path, config).unwrap();
        
        // Test basic operations
        db.put("test_key", "test_value").unwrap();
        let value = db.get("test_key").unwrap();
        assert_eq!(value, Some("test_value".to_string()));
        
        // Test transactions
        let tx_id = db.begin_transaction().unwrap();
        db.put_tx(tx_id, "tx_key", "tx_value").unwrap();
        db.commit_transaction(tx_id).unwrap();
        
        let tx_value = db.get("tx_key").unwrap();
        assert_eq!(tx_value, Some("tx_value".to_string()));
    }
    
    #[tokio::test]
    async fn test_concurrent_access() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("concurrent_test_db");
        
        let db = Arc::new(Database::create(&db_path, LightningDbConfig::default()).unwrap());
        
        let mut handles = vec![];
        
        for i in 0..100 {
            let db_clone = db.clone();
            let handle = tokio::spawn(async move {
                db_clone.put(&format!("key_{}", i), &format!("value_{}", i)).unwrap();
            });
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        // Verify all data was written
        for i in 0..100 {
            let value = db.get(&format!("key_{}", i)).unwrap();
            assert_eq!(value, Some(format!("value_{}", i)));
        }
    }
}
```

## Deployment

### Building for Multiple Platforms

```bash
# Build script for cross-platform deployment
#!/bin/bash

# Windows
echo "Building for Windows..."
cargo build --release --target x86_64-pc-windows-msvc
cargo build --release --target i686-pc-windows-msvc

# macOS
echo "Building for macOS..."
cargo build --release --target x86_64-apple-darwin
cargo build --release --target aarch64-apple-darwin

# Linux
echo "Building for Linux..."
cargo build --release --target x86_64-unknown-linux-gnu
cargo build --release --target aarch64-unknown-linux-gnu

echo "All builds completed!"
```

### Distribution

```toml
# Cargo.toml for release builds
[profile.release]
opt-level = 3
lto = true
codegen-units = 1
strip = true
panic = "abort"
```

This comprehensive desktop guide covers integration with major desktop application frameworks and provides production-ready examples for building cross-platform desktop applications with Lightning DB.