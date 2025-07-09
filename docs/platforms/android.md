# Android Integration Guide

## Overview

Lightning DB provides seamless integration with Android applications through JNI bindings and AAR packages. This guide will help you integrate Lightning DB into your Android project.

## Prerequisites

- Android SDK 21+ (Android 5.0 Lollipop)
- Android NDK r25c or later
- Gradle 7.0+
- Java 8+ or Kotlin 1.5+

## Installation

### Method 1: AAR Package (Recommended)

Add to your `build.gradle` (Module: app):

```gradle
android {
    compileSdk 34
    
    defaultConfig {
        minSdk 21
        targetSdk 34
        
        ndk {
            abiFilters 'arm64-v8a', 'armeabi-v7a', 'x86', 'x86_64'
        }
    }
    
    packagingOptions {
        pickFirst '**/liblightning_db_ffi.so'
    }
}

dependencies {
    implementation 'io.lightningdb:lightning-db-android:0.1.0'
}
```

### Method 2: Local AAR

1. Download the AAR from releases
2. Place in `app/libs/`
3. Add to `build.gradle`:

```gradle
dependencies {
    implementation files('libs/lightning-db-android-0.1.0.aar')
}
```

## Quick Start

### Basic Usage

```kotlin
import io.lightningdb.LightningDB
import io.lightningdb.LightningDBConfig

class MainActivity : AppCompatActivity() {
    private lateinit var db: LightningDB
    
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        
        // Initialize database
        val config = LightningDBConfig.Builder()
            .cacheSize(100 * 1024 * 1024) // 100MB cache
            .compressionEnabled(true)
            .syncMode(LightningDBConfig.SyncMode.ASYNC)
            .build()
            
        db = LightningDB.create(filesDir.path + "/mydb", config)
        
        // Basic operations
        db.put("key1", "Hello, Lightning DB!")
        val value = db.get("key1")
        println("Retrieved: $value")
    }
    
    override fun onDestroy() {
        super.onDestroy()
        db.close()
    }
}
```

### Advanced Usage with Coroutines

```kotlin
import kotlinx.coroutines.*

class DatabaseRepository {
    private val db: LightningDB
    private val scope = CoroutineScope(Dispatchers.IO)
    
    init {
        val config = LightningDBConfig.Builder()
            .cacheSize(200 * 1024 * 1024)
            .compressionEnabled(true)
            .build()
            
        db = LightningDB.create(context.filesDir.path + "/app.db", config)
    }
    
    suspend fun putAsync(key: String, value: String) = withContext(Dispatchers.IO) {
        db.put(key, value)
    }
    
    suspend fun getAsync(key: String): String? = withContext(Dispatchers.IO) {
        db.get(key)
    }
    
    suspend fun batchOperation(operations: List<Pair<String, String>>) = withContext(Dispatchers.IO) {
        val txId = db.beginTransaction()
        try {
            operations.forEach { (key, value) ->
                db.putTx(txId, key, value)
            }
            db.commitTransaction(txId)
        } catch (e: Exception) {
            db.rollbackTransaction(txId)
            throw e
        }
    }
}
```

## Data Models with Serialization

### Using Gson

```kotlin
import com.google.gson.Gson

data class User(
    val id: String,
    val name: String,
    val email: String,
    val createdAt: Long
)

class UserRepository(private val db: LightningDB) {
    private val gson = Gson()
    
    fun saveUser(user: User) {
        val json = gson.toJson(user)
        db.put("user:${user.id}", json)
    }
    
    fun getUser(id: String): User? {
        val json = db.get("user:$id") ?: return null
        return gson.fromJson(json, User::class.java)
    }
    
    fun getUsersByPrefix(prefix: String): List<User> {
        return db.scan("user:$prefix")
            .mapNotNull { (_, json) ->
                try {
                    gson.fromJson(json, User::class.java)
                } catch (e: Exception) {
                    null
                }
            }
    }
}
```

### Using Kotlinx Serialization

```kotlin
import kotlinx.serialization.*
import kotlinx.serialization.json.*

@Serializable
data class Product(
    val id: String,
    val name: String,
    val price: Double,
    val category: String
)

class ProductRepository(private val db: LightningDB) {
    private val json = Json {
        encodeDefaults = true
        ignoreUnknownKeys = true
    }
    
    fun saveProduct(product: Product) {
        val jsonString = json.encodeToString(product)
        db.put("product:${product.id}", jsonString)
    }
    
    fun getProduct(id: String): Product? {
        val jsonString = db.get("product:$id") ?: return null
        return json.decodeFromString<Product>(jsonString)
    }
}
```

## Transaction Management

### Basic Transactions

```kotlin
class OrderService(private val db: LightningDB) {
    
    fun processOrder(order: Order, items: List<OrderItem>) {
        val txId = db.beginTransaction()
        try {
            // Save order
            db.putTx(txId, "order:${order.id}", order.toJson())
            
            // Save items
            items.forEach { item ->
                db.putTx(txId, "item:${item.id}", item.toJson())
            }
            
            // Update inventory
            items.forEach { item ->
                val currentStock = getStock(item.productId)
                val newStock = currentStock - item.quantity
                db.putTx(txId, "stock:${item.productId}", newStock.toString())
            }
            
            db.commitTransaction(txId)
        } catch (e: Exception) {
            db.rollbackTransaction(txId)
            throw e
        }
    }
}
```

### Transaction with Retry Logic

```kotlin
class RetryableTransaction(private val db: LightningDB) {
    
    fun executeWithRetry(
        maxRetries: Int = 3,
        operation: (Long) -> Unit
    ) {
        var attempt = 0
        while (attempt < maxRetries) {
            val txId = db.beginTransaction()
            try {
                operation(txId)
                db.commitTransaction(txId)
                return
            } catch (e: LightningDBException) {
                db.rollbackTransaction(txId)
                attempt++
                if (attempt >= maxRetries) throw e
                
                // Exponential backoff
                Thread.sleep(100L * (1 shl attempt))
            }
        }
    }
}
```

## Performance Optimization

### Database Configuration

```kotlin
val config = LightningDBConfig.Builder()
    .cacheSize(512 * 1024 * 1024) // 512MB for large datasets
    .compressionEnabled(true) // Enable compression
    .syncMode(LightningDBConfig.SyncMode.ASYNC) // Async for better performance
    .walBufferSize(64 * 1024 * 1024) // 64MB WAL buffer
    .maxConcurrentTransactions(100)
    .build()
```

### Batch Operations

```kotlin
class BatchProcessor(private val db: LightningDB) {
    
    fun processBatch(items: List<DataItem>, batchSize: Int = 1000) {
        items.chunked(batchSize).forEach { batch ->
            val txId = db.beginTransaction()
            try {
                batch.forEach { item ->
                    db.putTx(txId, item.key, item.value)
                }
                db.commitTransaction(txId)
            } catch (e: Exception) {
                db.rollbackTransaction(txId)
                throw e
            }
        }
    }
}
```

## Background Processing

### Using WorkManager

```kotlin
class DatabaseSyncWorker(
    context: Context,
    workerParams: WorkerParameters
) : CoroutineWorker(context, workerParams) {
    
    override suspend fun doWork(): Result = withContext(Dispatchers.IO) {
        try {
            val db = LightningDB.open(applicationContext.filesDir.path + "/app.db")
            
            // Perform background operations
            db.checkpoint()
            db.compact()
            
            Result.success()
        } catch (e: Exception) {
            Result.failure()
        }
    }
}

// Schedule work
val syncWork = PeriodicWorkRequestBuilder<DatabaseSyncWorker>(
    15, TimeUnit.MINUTES
).build()

WorkManager.getInstance(context).enqueueUniquePeriodicWork(
    "db-sync",
    ExistingPeriodicWorkPolicy.KEEP,
    syncWork
)
```

## Error Handling

### Custom Exception Handling

```kotlin
class DatabaseManager(private val db: LightningDB) {
    
    fun safeGet(key: String): String? {
        return try {
            db.get(key)
        } catch (e: LightningDBException) {
            Log.e("DatabaseManager", "Error getting key: $key", e)
            null
        }
    }
    
    fun safePut(key: String, value: String): Boolean {
        return try {
            db.put(key, value)
            true
        } catch (e: LightningDBException) {
            Log.e("DatabaseManager", "Error putting key: $key", e)
            false
        }
    }
}
```

## Testing

### Unit Tests

```kotlin
@RunWith(AndroidJUnit4::class)
class DatabaseTest {
    
    private lateinit var db: LightningDB
    private lateinit var tempDir: File
    
    @Before
    fun setup() {
        tempDir = Files.createTempDirectory("test_db").toFile()
        db = LightningDB.create(tempDir.path, LightningDBConfig.default())
    }
    
    @After
    fun tearDown() {
        db.close()
        tempDir.deleteRecursively()
    }
    
    @Test
    fun testBasicOperations() {
        // Test put/get
        db.put("test_key", "test_value")
        val value = db.get("test_key")
        assertEquals("test_value", value)
        
        // Test delete
        db.delete("test_key")
        assertNull(db.get("test_key"))
    }
    
    @Test
    fun testTransactions() {
        val txId = db.beginTransaction()
        db.putTx(txId, "tx_key", "tx_value")
        
        // Value not visible before commit
        assertNull(db.get("tx_key"))
        
        db.commitTransaction(txId)
        
        // Value visible after commit
        assertEquals("tx_value", db.get("tx_key"))
    }
}
```

## Best Practices

### 1. Resource Management
```kotlin
class DatabaseManager {
    private val db: LightningDB
    
    init {
        db = LightningDB.create(path, config)
    }
    
    fun close() {
        db.close()
    }
}

// Always close in onDestroy
override fun onDestroy() {
    super.onDestroy()
    databaseManager.close()
}
```

### 2. Thread Safety
```kotlin
class ThreadSafeRepository {
    private val db: LightningDB
    private val executor = Executors.newCachedThreadPool()
    
    fun putAsync(key: String, value: String, callback: (Boolean) -> Unit) {
        executor.submit {
            try {
                db.put(key, value)
                callback(true)
            } catch (e: Exception) {
                callback(false)
            }
        }
    }
}
```

### 3. Error Recovery
```kotlin
class RobustDatabase(private val dbPath: String) {
    private var db: LightningDB? = null
    
    fun getDb(): LightningDB {
        if (db == null) {
            db = try {
                LightningDB.open(dbPath)
            } catch (e: Exception) {
                // Try to recover
                LightningDB.create(dbPath, LightningDBConfig.default())
            }
        }
        return db!!
    }
}
```

## Troubleshooting

### Common Issues

1. **Library Not Found**
   - Ensure AAR is properly included
   - Check ABI filters match your device
   - Verify NDK version compatibility

2. **Performance Issues**
   - Increase cache size
   - Use async sync mode
   - Implement proper batching

3. **Memory Issues**
   - Monitor database size
   - Implement regular compaction
   - Use appropriate cache sizes

### Debug Logging

```kotlin
if (BuildConfig.DEBUG) {
    LightningDB.setLogLevel(LightningDB.LogLevel.DEBUG)
}
```

## Migration from Other Databases

### From SQLite
```kotlin
class SQLiteToLightningMigration {
    fun migrate(sqliteDb: SQLiteDatabase, lightningDb: LightningDB) {
        val cursor = sqliteDb.rawQuery("SELECT * FROM your_table", null)
        
        val txId = lightningDb.beginTransaction()
        try {
            while (cursor.moveToNext()) {
                val key = cursor.getString("key_column")
                val value = cursor.getString("value_column")
                lightningDb.putTx(txId, key, value)
            }
            lightningDb.commitTransaction(txId)
        } catch (e: Exception) {
            lightningDb.rollbackTransaction(txId)
            throw e
        } finally {
            cursor.close()
        }
    }
}
```

This guide provides a comprehensive foundation for integrating Lightning DB into your Android applications. For more advanced usage and API reference, see the [API Documentation](../api/README.md).