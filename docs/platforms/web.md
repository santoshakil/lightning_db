# Web/WebAssembly Integration Guide

## Overview

Lightning DB provides seamless WebAssembly integration for web applications, offering near-native performance in the browser. This guide covers integration with vanilla JavaScript, React, Vue, and other web frameworks.

## Prerequisites

- Modern browser with WebAssembly support
- Node.js 16+ for development
- Bundler (Webpack, Vite, Rollup, etc.)

## Installation

### NPM Package

```bash
npm install lightning-db-wasm
```

### CDN (Browser)

```html
<script type="module">
import init, { LightningDB } from 'https://cdn.jsdelivr.net/npm/lightning-db-wasm@latest/lightning_db_wasm.js';
</script>
```

### Bundler Configuration

#### Webpack

```javascript
// webpack.config.js
module.exports = {
  experiments: {
    asyncWebAssembly: true,
  },
  resolve: {
    fallback: {
      "fs": false,
      "path": false,
    }
  }
};
```

#### Vite

```javascript
// vite.config.js
export default {
  server: {
    headers: {
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Opener-Policy': 'same-origin',
    },
  },
  optimizeDeps: {
    exclude: ['lightning-db-wasm']
  }
};
```

## Quick Start

### Basic Usage

```javascript
import init, { LightningDB, Config } from 'lightning-db-wasm';

async function main() {
  // Initialize WASM
  await init();
  
  // Create database
  const config = new Config();
  config.cache_size = 50 * 1024 * 1024; // 50MB
  config.compression_enabled = true;
  config.sync_mode = 'async';
  
  const db = new LightningDB(config);
  
  // Basic operations
  await db.put('greeting', new TextEncoder().encode('Hello, Lightning DB!'));
  const value = await db.get('greeting');
  
  if (value) {
    const message = new TextDecoder().decode(value);
    console.log('Retrieved:', message);
  }
}

main().catch(console.error);
```

### With Async/Await

```javascript
class DatabaseManager {
  constructor() {
    this.db = null;
    this.initialized = false;
  }
  
  async init() {
    if (this.initialized) return;
    
    await init();
    
    const config = new Config();
    this.db = new LightningDB(config);
    this.initialized = true;
  }
  
  async put(key, value) {
    await this.init();
    if (typeof value === 'string') {
      value = new TextEncoder().encode(value);
    }
    return await this.db.put_async(key, value);
  }
  
  async get(key) {
    await this.init();
    const result = await this.db.get_async(key);
    return result ? new TextDecoder().decode(result) : null;
  }
  
  async delete(key) {
    await this.init();
    return await this.db.delete_async(key);
  }
}

// Usage
const dbManager = new DatabaseManager();
await dbManager.put('user:123', JSON.stringify({ name: 'John', age: 30 }));
const userData = await dbManager.get('user:123');
```

## Framework Integration

### React Integration

```jsx
import React, { createContext, useContext, useEffect, useState } from 'react';
import init, { LightningDB, Config } from 'lightning-db-wasm';

// Database Context
const DatabaseContext = createContext(null);

export function DatabaseProvider({ children }) {
  const [db, setDb] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  useEffect(() => {
    async function initDatabase() {
      try {
        await init();
        const config = new Config();
        config.cache_size = 100 * 1024 * 1024; // 100MB
        const database = new LightningDB(config);
        setDb(database);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    }
    
    initDatabase();
  }, []);
  
  if (loading) return <div>Loading database...</div>;
  if (error) return <div>Error: {error}</div>;
  
  return (
    <DatabaseContext.Provider value={db}>
      {children}
    </DatabaseContext.Provider>
  );
}

// Hook
export function useDatabase() {
  const db = useContext(DatabaseContext);
  if (!db) {
    throw new Error('useDatabase must be used within DatabaseProvider');
  }
  
  return {
    async put(key, value) {
      if (typeof value === 'object') {
        value = JSON.stringify(value);
      }
      return await db.put_async(key, new TextEncoder().encode(value));
    },
    
    async get(key) {
      const result = await db.get_async(key);
      if (!result) return null;
      
      try {
        return JSON.parse(new TextDecoder().decode(result));
      } catch {
        return new TextDecoder().decode(result);
      }
    },
    
    async delete(key) {
      return await db.delete_async(key);
    },
    
    async getStats() {
      return await db.get_stats();
    }
  };
}

// Component Example
function UserProfile({ userId }) {
  const db = useDatabase();
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  
  useEffect(() => {
    async function loadUser() {
      try {
        const userData = await db.get(`user:${userId}`);
        setUser(userData);
      } catch (error) {
        console.error('Failed to load user:', error);
      } finally {
        setLoading(false);
      }
    }
    
    loadUser();
  }, [userId, db]);
  
  const saveUser = async (userData) => {
    try {
      await db.put(`user:${userId}`, userData);
      setUser(userData);
    } catch (error) {
      console.error('Failed to save user:', error);
    }
  };
  
  if (loading) return <div>Loading...</div>;
  
  return (
    <div>
      <h1>{user?.name || 'Unknown User'}</h1>
      <p>{user?.email}</p>
      <button onClick={() => saveUser({ ...user, lastSeen: new Date() })}>
        Update Last Seen
      </button>
    </div>
  );
}
```

### Vue 3 Integration

```vue
<!-- DatabaseProvider.vue -->
<template>
  <div v-if="loading">Loading database...</div>
  <div v-else-if="error">Error: {{ error }}</div>
  <slot v-else :db="db" />
</template>

<script setup>
import { ref, onMounted, provide } from 'vue';
import init, { LightningDB, Config } from 'lightning-db-wasm';

const db = ref(null);
const loading = ref(true);
const error = ref(null);

onMounted(async () => {
  try {
    await init();
    const config = new Config();
    config.cache_size = 100 * 1024 * 1024;
    db.value = new LightningDB(config);
    
    // Provide database to child components
    provide('database', {
      async put(key, value) {
        if (typeof value === 'object') {
          value = JSON.stringify(value);
        }
        return await db.value.put_async(key, new TextEncoder().encode(value));
      },
      
      async get(key) {
        const result = await db.value.get_async(key);
        if (!result) return null;
        
        try {
          return JSON.parse(new TextDecoder().decode(result));
        } catch {
          return new TextDecoder().decode(result);
        }
      },
      
      async delete(key) {
        return await db.value.delete_async(key);
      }
    });
  } catch (err) {
    error.value = err.message;
  } finally {
    loading.value = false;
  }
});
</script>
```

```vue
<!-- UserComponent.vue -->
<template>
  <div>
    <h1>{{ user?.name || 'Loading...' }}</h1>
    <input v-model="user.name" @change="saveUser" />
  </div>
</template>

<script setup>
import { ref, inject, onMounted } from 'vue';

const props = defineProps(['userId']);
const db = inject('database');
const user = ref(null);

onMounted(async () => {
  user.value = await db.get(`user:${props.userId}`) || { name: '', email: '' };
});

const saveUser = async () => {
  await db.put(`user:${props.userId}`, user.value);
};
</script>
```

### Svelte Integration

```svelte
<!-- DatabaseStore.js -->
import { writable } from 'svelte/store';
import init, { LightningDB, Config } from 'lightning-db-wasm';

function createDatabase() {
  const { subscribe, set, update } = writable(null);
  
  let db = null;
  let initialized = false;
  
  async function initialize() {
    if (initialized) return db;
    
    await init();
    const config = new Config();
    db = new LightningDB(config);
    initialized = true;
    set(db);
    return db;
  }
  
  return {
    subscribe,
    initialize,
    async put(key, value) {
      const database = await initialize();
      if (typeof value === 'object') {
        value = JSON.stringify(value);
      }
      return await database.put_async(key, new TextEncoder().encode(value));
    },
    
    async get(key) {
      const database = await initialize();
      const result = await database.get_async(key);
      if (!result) return null;
      
      try {
        return JSON.parse(new TextDecoder().decode(result));
      } catch {
        return new TextDecoder().decode(result);
      }
    }
  };
}

export const database = createDatabase();
```

```svelte
<!-- App.svelte -->
<script>
  import { onMount } from 'svelte';
  import { database } from './DatabaseStore.js';
  
  let users = [];
  let newUser = { name: '', email: '' };
  
  onMount(async () => {
    await database.initialize();
    loadUsers();
  });
  
  async function loadUsers() {
    // Load users from database
    const userIds = await database.get('user_ids') || [];
    users = await Promise.all(
      userIds.map(id => database.get(`user:${id}`))
    );
  }
  
  async function addUser() {
    const id = Date.now().toString();
    await database.put(`user:${id}`, newUser);
    
    const userIds = await database.get('user_ids') || [];
    userIds.push(id);
    await database.put('user_ids', userIds);
    
    newUser = { name: '', email: '' };
    loadUsers();
  }
</script>

<main>
  <h1>Lightning DB + Svelte</h1>
  
  <div>
    <input bind:value={newUser.name} placeholder="Name" />
    <input bind:value={newUser.email} placeholder="Email" />
    <button on:click={addUser}>Add User</button>
  </div>
  
  <ul>
    {#each users as user}
      <li>{user.name} - {user.email}</li>
    {/each}
  </ul>
</main>
```

## Advanced Features

### Transaction Management

```javascript
class TransactionManager {
  constructor(db) {
    this.db = db;
  }
  
  async execute(operations) {
    const txId = await this.db.begin_transaction();
    
    try {
      for (const op of operations) {
        switch (op.type) {
          case 'put':
            await this.db.tx_put(txId, op.key, new TextEncoder().encode(op.value));
            break;
          case 'delete':
            await this.db.tx_delete(txId, op.key);
            break;
        }
      }
      
      await this.db.commit_transaction(txId);
      return true;
    } catch (error) {
      await this.db.rollback_transaction(txId);
      throw error;
    }
  }
}

// Usage
const txManager = new TransactionManager(db);
await txManager.execute([
  { type: 'put', key: 'user:1', value: JSON.stringify({ name: 'John' }) },
  { type: 'put', key: 'user:2', value: JSON.stringify({ name: 'Jane' }) },
  { type: 'delete', key: 'temp:data' }
]);
```

### Data Synchronization

```javascript
class SyncManager {
  constructor(db, serverUrl) {
    this.db = db;
    this.serverUrl = serverUrl;
    this.syncInterval = null;
  }
  
  startSync(intervalMs = 30000) {
    this.syncInterval = setInterval(() => {
      this.performSync();
    }, intervalMs);
  }
  
  stopSync() {
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
    }
  }
  
  async performSync() {
    try {
      // Get local changes
      const localChanges = await this.getLocalChanges();
      
      // Send to server
      const response = await fetch(`${this.serverUrl}/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ changes: localChanges })
      });
      
      const { serverChanges } = await response.json();
      
      // Apply server changes
      await this.applyServerChanges(serverChanges);
      
      // Mark local changes as synced
      await this.markChangesSynced(localChanges);
    } catch (error) {
      console.error('Sync failed:', error);
    }
  }
  
  async getLocalChanges() {
    // Implementation depends on your change tracking strategy
    return [];
  }
  
  async applyServerChanges(changes) {
    const txId = await this.db.begin_transaction();
    
    try {
      for (const change of changes) {
        if (change.type === 'put') {
          await this.db.tx_put(txId, change.key, new TextEncoder().encode(change.value));
        } else if (change.type === 'delete') {
          await this.db.tx_delete(txId, change.key);
        }
      }
      
      await this.db.commit_transaction(txId);
    } catch (error) {
      await this.db.rollback_transaction(txId);
      throw error;
    }
  }
}
```

### Performance Monitoring

```javascript
class PerformanceMonitor {
  constructor(db) {
    this.db = db;
    this.metrics = {
      operations: 0,
      totalTime: 0,
      errors: 0
    };
  }
  
  async measureOperation(name, operation) {
    const startTime = performance.now();
    
    try {
      const result = await operation();
      const endTime = performance.now();
      
      this.metrics.operations++;
      this.metrics.totalTime += (endTime - startTime);
      
      console.log(`${name}: ${(endTime - startTime).toFixed(2)}ms`);
      
      return result;
    } catch (error) {
      this.metrics.errors++;
      throw error;
    }
  }
  
  getStats() {
    return {
      ...this.metrics,
      averageTime: this.metrics.totalTime / this.metrics.operations,
      errorRate: this.metrics.errors / this.metrics.operations
    };
  }
  
  async getDatabaseStats() {
    return await this.db.get_stats();
  }
}

// Usage
const monitor = new PerformanceMonitor(db);

await monitor.measureOperation('User Save', async () => {
  await db.put('user:123', userData);
});
```

## Browser Storage Integration

### IndexedDB Fallback

```javascript
class HybridStorage {
  constructor() {
    this.lightningDB = null;
    this.indexedDB = null;
    this.useIndexedDB = false;
  }
  
  async init() {
    try {
      // Try Lightning DB first
      await init();
      const config = new Config();
      this.lightningDB = new LightningDB(config);
    } catch (error) {
      console.warn('Lightning DB not available, falling back to IndexedDB');
      this.useIndexedDB = true;
      await this.initIndexedDB();
    }
  }
  
  async initIndexedDB() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open('LightningDBFallback', 1);
      
      request.onerror = () => reject(request.error);
      request.onsuccess = () => {
        this.indexedDB = request.result;
        resolve();
      };
      
      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        db.createObjectStore('data', { keyPath: 'key' });
      };
    });
  }
  
  async put(key, value) {
    if (this.useIndexedDB) {
      return this.putIndexedDB(key, value);
    } else {
      return await this.lightningDB.put_async(key, new TextEncoder().encode(value));
    }
  }
  
  async get(key) {
    if (this.useIndexedDB) {
      return this.getIndexedDB(key);
    } else {
      const result = await this.lightningDB.get_async(key);
      return result ? new TextDecoder().decode(result) : null;
    }
  }
  
  async putIndexedDB(key, value) {
    const transaction = this.indexedDB.transaction(['data'], 'readwrite');
    const store = transaction.objectStore('data');
    return store.put({ key, value });
  }
  
  async getIndexedDB(key) {
    const transaction = this.indexedDB.transaction(['data'], 'readonly');
    const store = transaction.objectStore('data');
    const request = store.get(key);
    
    return new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(request.result?.value || null);
      request.onerror = () => reject(request.error);
    });
  }
}
```

## Testing

### Unit Tests (Jest)

```javascript
// __tests__/database.test.js
import { jest } from '@jest/globals';

// Mock WebAssembly
global.WebAssembly = {
  instantiate: jest.fn(),
  Module: jest.fn(),
};

// Mock the WASM module
jest.mock('lightning-db-wasm', () => ({
  __esModule: true,
  default: jest.fn(() => Promise.resolve()),
  LightningDB: class MockLightningDB {
    constructor() {
      this.data = new Map();
    }
    
    async put_async(key, value) {
      this.data.set(key, value);
    }
    
    async get_async(key) {
      return this.data.get(key) || null;
    }
    
    async delete_async(key) {
      this.data.delete(key);
    }
  },
  Config: class MockConfig {
    constructor() {
      this.cache_size = 1024 * 1024;
      this.compression_enabled = true;
    }
  }
}));

describe('Database Operations', () => {
  let db;
  
  beforeEach(async () => {
    const { LightningDB, Config } = await import('lightning-db-wasm');
    const config = new Config();
    db = new LightningDB(config);
  });
  
  test('should store and retrieve data', async () => {
    const key = 'test-key';
    const value = new TextEncoder().encode('test-value');
    
    await db.put_async(key, value);
    const retrieved = await db.get_async(key);
    
    expect(retrieved).toEqual(value);
  });
  
  test('should return null for non-existent key', async () => {
    const result = await db.get_async('non-existent');
    expect(result).toBeNull();
  });
});
```

### Integration Tests (Playwright)

```javascript
// tests/integration.spec.js
const { test, expect } = require('@playwright/test');

test.describe('Lightning DB Web Integration', () => {
  test('should initialize and perform basic operations', async ({ page }) => {
    await page.goto('/');
    
    // Wait for database to initialize
    await page.waitForSelector('[data-testid="db-ready"]');
    
    // Test put operation
    await page.fill('[data-testid="key-input"]', 'test-key');
    await page.fill('[data-testid="value-input"]', 'test-value');
    await page.click('[data-testid="put-button"]');
    
    // Test get operation
    await page.click('[data-testid="get-button"]');
    const result = await page.textContent('[data-testid="result"]');
    expect(result).toBe('test-value');
  });
  
  test('should handle transactions', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('[data-testid="db-ready"]');
    
    // Start transaction
    await page.click('[data-testid="start-tx-button"]');
    
    // Add operations
    await page.fill('[data-testid="tx-key-input"]', 'tx-key');
    await page.fill('[data-testid="tx-value-input"]', 'tx-value');
    await page.click('[data-testid="tx-put-button"]');
    
    // Commit transaction
    await page.click('[data-testid="commit-button"]');
    
    // Verify data
    await page.fill('[data-testid="key-input"]', 'tx-key');
    await page.click('[data-testid="get-button"]');
    const result = await page.textContent('[data-testid="result"]');
    expect(result).toBe('tx-value');
  });
});
```

## Best Practices

### 1. Resource Management

```javascript
class DatabaseManager {
  constructor() {
    this.db = null;
    this.isInitialized = false;
  }
  
  async initialize() {
    if (this.isInitialized) return;
    
    await init();
    const config = new Config();
    this.db = new LightningDB(config);
    this.isInitialized = true;
  }
  
  async cleanup() {
    if (this.db) {
      // Lightning DB automatically cleans up when garbage collected
      this.db = null;
      this.isInitialized = false;
    }
  }
}
```

### 2. Error Handling

```javascript
class SafeDatabase {
  constructor(db) {
    this.db = db;
  }
  
  async safeOperation(operation) {
    try {
      return await operation();
    } catch (error) {
      console.error('Database operation failed:', error);
      throw new Error(`Database operation failed: ${error.message}`);
    }
  }
  
  async put(key, value) {
    return this.safeOperation(async () => {
      await this.db.put_async(key, new TextEncoder().encode(value));
    });
  }
  
  async get(key) {
    return this.safeOperation(async () => {
      const result = await this.db.get_async(key);
      return result ? new TextDecoder().decode(result) : null;
    });
  }
}
```

### 3. Performance Optimization

```javascript
class OptimizedDatabase {
  constructor(db) {
    this.db = db;
    this.writeQueue = [];
    this.processing = false;
  }
  
  async batchPut(key, value) {
    this.writeQueue.push({ key, value });
    
    if (!this.processing) {
      this.processing = true;
      setTimeout(() => this.processBatch(), 100); // Batch every 100ms
    }
  }
  
  async processBatch() {
    if (this.writeQueue.length === 0) {
      this.processing = false;
      return;
    }
    
    const batch = this.writeQueue.splice(0, 100); // Process 100 items at a time
    const txId = await this.db.begin_transaction();
    
    try {
      for (const { key, value } of batch) {
        await this.db.tx_put(txId, key, new TextEncoder().encode(value));
      }
      await this.db.commit_transaction(txId);
    } catch (error) {
      await this.db.rollback_transaction(txId);
      throw error;
    }
    
    // Process remaining items
    if (this.writeQueue.length > 0) {
      setTimeout(() => this.processBatch(), 10);
    } else {
      this.processing = false;
    }
  }
}
```

## Deployment

### Production Build

```javascript
// webpack.config.js
module.exports = {
  mode: 'production',
  experiments: {
    asyncWebAssembly: true,
  },
  optimization: {
    usedExports: true,
    sideEffects: false,
  },
  plugins: [
    new webpack.DefinePlugin({
      'process.env.NODE_ENV': JSON.stringify('production'),
    }),
  ],
};
```

### Service Worker Integration

```javascript
// sw.js
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open('lightning-db-v1').then((cache) => {
      return cache.addAll([
        '/lightning_db_wasm.js',
        '/lightning_db_wasm_bg.wasm',
      ]);
    })
  );
});
```

This comprehensive guide covers all aspects of integrating Lightning DB with web applications. For more advanced topics, see the [API Reference](../api/README.md).