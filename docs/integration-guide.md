# Lightning DB Integration Guide

## Table of Contents

1. [Rust Integration](#rust-integration)
2. [Web Frameworks](#web-frameworks)
3. [Async Runtime Integration](#async-runtime-integration)
4. [FFI Bindings](#ffi-bindings)
5. [Language Bindings](#language-bindings)
6. [ORM Integration](#orm-integration)
7. [Microservices](#microservices)
8. [Testing Integration](#testing-integration)
9. [CI/CD Integration](#cicd-integration)
10. [Container Integration](#container-integration)

---

## Rust Integration

### Basic Cargo Setup

```toml
# Cargo.toml
[dependencies]
lightning_db = "1.0"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
anyhow = "1"

[features]
default = ["lightning_db/compression", "lightning_db/encryption"]
```

### Simple Application

```rust
use lightning_db::{Database, LightningDbConfig, Result};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct User {
    id: u64,
    name: String,
    email: String,
}

fn main() -> Result<()> {
    // Initialize database
    let config = LightningDbConfig::default();
    let db = Database::open_or_create("./data/users.db", config)?;
    
    // Store user
    let user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    };
    
    let key = format!("user:{}", user.id);
    let value = serde_json::to_vec(&user)?;
    db.put(key.as_bytes(), &value)?;
    
    // Retrieve user
    if let Some(data) = db.get(key.as_bytes())? {
        let retrieved: User = serde_json::from_slice(&data)?;
        println!("Retrieved user: {:?}", retrieved);
    }
    
    Ok(())
}
```

---

## Web Frameworks

### Actix-Web Integration

```rust
use actix_web::{web, App, HttpServer, Result, HttpResponse};
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    db: Arc<Database>,
}

async fn get_user(
    data: web::Data<AppState>,
    path: web::Path<u64>,
) -> Result<HttpResponse> {
    let user_id = path.into_inner();
    let key = format!("user:{}", user_id);
    
    match data.db.get(key.as_bytes()) {
        Ok(Some(value)) => {
            let user: serde_json::Value = serde_json::from_slice(&value)?;
            Ok(HttpResponse::Ok().json(user))
        }
        Ok(None) => Ok(HttpResponse::NotFound().finish()),
        Err(e) => Ok(HttpResponse::InternalServerError().body(e.to_string())),
    }
}

async fn create_user(
    data: web::Data<AppState>,
    user: web::Json<User>,
) -> Result<HttpResponse> {
    let key = format!("user:{}", user.id);
    let value = serde_json::to_vec(&user.into_inner())?;
    
    match data.db.put(key.as_bytes(), &value) {
        Ok(_) => Ok(HttpResponse::Created().finish()),
        Err(e) => Ok(HttpResponse::InternalServerError().body(e.to_string())),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize database
    let db = Database::open_or_create("./data/web.db", LightningDbConfig::default())
        .expect("Failed to open database");
    
    let app_state = AppState {
        db: Arc::new(db),
    };
    
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app_state.clone()))
            .route("/users/{id}", web::get().to(get_user))
            .route("/users", web::post().to(create_user))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
```

### Axum Integration

```rust
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tokio::sync::RwLock;

type SharedDb = Arc<RwLock<Database>>;

async fn get_item(
    Path(id): Path<String>,
    State(db): State<SharedDb>,
) -> impl IntoResponse {
    let db = db.read().await;
    match db.get(id.as_bytes()) {
        Ok(Some(value)) => {
            match serde_json::from_slice::<serde_json::Value>(&value) {
                Ok(json) => (StatusCode::OK, Json(json)),
                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({}))),
            }
        }
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({}))),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({}))),
    }
}

async fn create_item(
    State(db): State<SharedDb>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let db = db.write().await;
    let id = payload["id"].as_str().unwrap_or("unknown");
    let value = serde_json::to_vec(&payload).unwrap();
    
    match db.put(id.as_bytes(), &value) {
        Ok(_) => StatusCode::CREATED,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[tokio::main]
async fn main() {
    let db = Database::open_or_create("./data/axum.db", LightningDbConfig::default())
        .expect("Failed to open database");
    let shared_db = Arc::new(RwLock::new(db));
    
    let app = Router::new()
        .route("/items/:id", get(get_item))
        .route("/items", post(create_item))
        .with_state(shared_db);
    
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
```

### Rocket Integration

```rust
#[macro_use] extern crate rocket;

use lightning_db::{Database, LightningDbConfig};
use rocket::{State, serde::json::Json};
use std::sync::Arc;
use parking_lot::RwLock;

type DbState = Arc<RwLock<Database>>;

#[get("/items/<id>")]
fn get_item(id: String, db: &State<DbState>) -> Option<Json<serde_json::Value>> {
    let db = db.read();
    db.get(id.as_bytes()).ok()
        .and_then(|data| data)
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .map(Json)
}

#[post("/items", data = "<item>")]
fn create_item(item: Json<serde_json::Value>, db: &State<DbState>) -> rocket::http::Status {
    let db = db.write();
    let id = item["id"].as_str().unwrap_or("unknown");
    
    match serde_json::to_vec(&item.into_inner()) {
        Ok(value) => {
            match db.put(id.as_bytes(), &value) {
                Ok(_) => rocket::http::Status::Created,
                Err(_) => rocket::http::Status::InternalServerError,
            }
        }
        Err(_) => rocket::http::Status::BadRequest,
    }
}

#[launch]
fn rocket() -> _ {
    let db = Database::open_or_create("./data/rocket.db", LightningDbConfig::default())
        .expect("Failed to open database");
    
    rocket::build()
        .manage(Arc::new(RwLock::new(db)))
        .mount("/", routes![get_item, create_item])
}
```

---

## Async Runtime Integration

### Tokio Integration

```rust
use lightning_db::{Database, LightningDbConfig};
use tokio::task;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Database::open_or_create(
        "./data/tokio.db", 
        LightningDbConfig::default()
    )?);
    
    // Spawn blocking tasks for database operations
    let db_clone = db.clone();
    let handle = task::spawn_blocking(move || {
        // Perform blocking database operations
        db_clone.put(b"key1", b"value1")?;
        db_clone.put(b"key2", b"value2")?;
        Ok::<_, lightning_db::Error>(())
    });
    
    // Wait for completion
    handle.await??;
    
    // Concurrent reads
    let mut handles = vec![];
    for i in 0..10 {
        let db_clone = db.clone();
        let handle = task::spawn_blocking(move || {
            let key = format!("key{}", i % 2 + 1);
            db_clone.get(key.as_bytes())
        });
        handles.push(handle);
    }
    
    // Collect results
    for handle in handles {
        if let Ok(Ok(Some(value))) = handle.await {
            println!("Retrieved: {:?}", String::from_utf8_lossy(&value));
        }
    }
    
    Ok(())
}
```

### Async-std Integration

```rust
use async_std::task;
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Database::open_or_create(
        "./data/async_std.db",
        LightningDbConfig::default()
    )?);
    
    // Spawn blocking operations
    let db_clone = db.clone();
    let result = task::spawn_blocking(move || {
        let mut batch = vec![];
        for i in 0..100 {
            let key = format!("item:{}", i);
            let value = format!("value {}", i);
            batch.push((key, value));
        }
        
        // Batch insert
        for (key, value) in batch {
            db_clone.put(key.as_bytes(), value.as_bytes())?;
        }
        Ok::<_, lightning_db::Error>(())
    }).await?;
    
    Ok(())
}
```

---

## FFI Bindings

### C API Example

```c
// lightning_db_example.c
#include "lightning_db.h"
#include <stdio.h>
#include <string.h>

int main() {
    // Open database
    lightning_db_t* db = lightning_db_open("./data/c_example.db", NULL);
    if (!db) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }
    
    // Write data
    const char* key = "hello";
    const char* value = "world";
    int result = lightning_db_put(db, 
        (uint8_t*)key, strlen(key),
        (uint8_t*)value, strlen(value)
    );
    
    if (result != 0) {
        fprintf(stderr, "Failed to write data\n");
        lightning_db_close(db);
        return 1;
    }
    
    // Read data
    size_t value_len;
    uint8_t* read_value = lightning_db_get(db, 
        (uint8_t*)key, strlen(key), &value_len
    );
    
    if (read_value) {
        printf("Read: %.*s\n", (int)value_len, read_value);
        lightning_db_free_value(read_value);
    }
    
    // Close database
    lightning_db_close(db);
    return 0;
}
```

### Python Bindings

```python
# lightning_db_py.py
import ctypes
from ctypes import c_void_p, c_char_p, c_size_t, POINTER, c_uint8

# Load the shared library
lib = ctypes.CDLL('./target/release/liblightning_db.so')

# Define function signatures
lib.lightning_db_open.argtypes = [c_char_p, c_void_p]
lib.lightning_db_open.restype = c_void_p

lib.lightning_db_put.argtypes = [c_void_p, POINTER(c_uint8), c_size_t, 
                                 POINTER(c_uint8), c_size_t]
lib.lightning_db_put.restype = ctypes.c_int

lib.lightning_db_get.argtypes = [c_void_p, POINTER(c_uint8), c_size_t, 
                                 POINTER(c_size_t)]
lib.lightning_db_get.restype = POINTER(c_uint8)

lib.lightning_db_close.argtypes = [c_void_p]
lib.lightning_db_close.restype = None

class LightningDB:
    def __init__(self, path):
        self.db = lib.lightning_db_open(path.encode('utf-8'), None)
        if not self.db:
            raise RuntimeError("Failed to open database")
    
    def put(self, key, value):
        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')
        result = lib.lightning_db_put(
            self.db,
            ctypes.cast(key_bytes, POINTER(c_uint8)), len(key_bytes),
            ctypes.cast(value_bytes, POINTER(c_uint8)), len(value_bytes)
        )
        if result != 0:
            raise RuntimeError("Failed to put value")
    
    def get(self, key):
        key_bytes = key.encode('utf-8')
        value_len = c_size_t()
        value_ptr = lib.lightning_db_get(
            self.db,
            ctypes.cast(key_bytes, POINTER(c_uint8)), len(key_bytes),
            ctypes.byref(value_len)
        )
        if value_ptr:
            value = ctypes.string_at(value_ptr, value_len.value)
            lib.lightning_db_free_value(value_ptr)
            return value.decode('utf-8')
        return None
    
    def close(self):
        if self.db:
            lib.lightning_db_close(self.db)
            self.db = None
    
    def __del__(self):
        self.close()

# Example usage
if __name__ == "__main__":
    db = LightningDB("./data/python_example.db")
    
    # Store data
    db.put("user:1", '{"name": "Alice", "age": 30}')
    db.put("user:2", '{"name": "Bob", "age": 25}')
    
    # Retrieve data
    user1 = db.get("user:1")
    print(f"User 1: {user1}")
    
    db.close()
```

---

## Language Bindings

### Node.js Integration

```javascript
// lightning-db-node.js
const ffi = require('ffi-napi');
const ref = require('ref-napi');

// Define types
const voidPtr = ref.refType(ref.types.void);
const uint8Ptr = ref.refType(ref.types.uint8);
const sizeT = ref.types.size_t;
const sizeTPtr = ref.refType(sizeT);

// Load the library
const lightningDb = ffi.Library('./target/release/liblightning_db', {
    'lightning_db_open': [voidPtr, ['string', voidPtr]],
    'lightning_db_close': ['void', [voidPtr]],
    'lightning_db_put': ['int', [voidPtr, uint8Ptr, sizeT, uint8Ptr, sizeT]],
    'lightning_db_get': [uint8Ptr, [voidPtr, uint8Ptr, sizeT, sizeTPtr]],
    'lightning_db_free_value': ['void', [uint8Ptr]]
});

class LightningDB {
    constructor(path) {
        this.db = lightningDb.lightning_db_open(path, null);
        if (this.db.isNull()) {
            throw new Error('Failed to open database');
        }
    }
    
    put(key, value) {
        const keyBuf = Buffer.from(key);
        const valueBuf = Buffer.from(value);
        
        const result = lightningDb.lightning_db_put(
            this.db,
            keyBuf, keyBuf.length,
            valueBuf, valueBuf.length
        );
        
        if (result !== 0) {
            throw new Error('Failed to put value');
        }
    }
    
    get(key) {
        const keyBuf = Buffer.from(key);
        const lenPtr = ref.alloc(sizeT);
        
        const valuePtr = lightningDb.lightning_db_get(
            this.db,
            keyBuf, keyBuf.length,
            lenPtr
        );
        
        if (valuePtr.isNull()) {
            return null;
        }
        
        const len = lenPtr.deref();
        const value = ref.reinterpret(valuePtr, len).toString();
        lightningDb.lightning_db_free_value(valuePtr);
        
        return value;
    }
    
    close() {
        if (this.db && !this.db.isNull()) {
            lightningDb.lightning_db_close(this.db);
            this.db = null;
        }
    }
}

// Example usage
const db = new LightningDB('./data/nodejs_example.db');

// Store data
db.put('config:app', JSON.stringify({
    name: 'MyApp',
    version: '1.0.0',
    settings: {
        theme: 'dark',
        language: 'en'
    }
}));

// Retrieve data
const config = JSON.parse(db.get('config:app'));
console.log('Config:', config);

db.close();
```

### Go Integration

```go
// lightning_db.go
package main

/*
#cgo LDFLAGS: -L./target/release -llightning_db
#include <stdlib.h>
#include "lightning_db.h"
*/
import "C"
import (
    "encoding/json"
    "fmt"
    "unsafe"
)

type LightningDB struct {
    db unsafe.Pointer
}

func OpenDB(path string) (*LightningDB, error) {
    cPath := C.CString(path)
    defer C.free(unsafe.Pointer(cPath))
    
    db := C.lightning_db_open(cPath, nil)
    if db == nil {
        return nil, fmt.Errorf("failed to open database")
    }
    
    return &LightningDB{db: db}, nil
}

func (db *LightningDB) Put(key, value []byte) error {
    result := C.lightning_db_put(
        db.db,
        (*C.uint8_t)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
        (*C.uint8_t)(unsafe.Pointer(&value[0])), C.size_t(len(value)),
    )
    
    if result != 0 {
        return fmt.Errorf("failed to put value")
    }
    return nil
}

func (db *LightningDB) Get(key []byte) ([]byte, error) {
    var valueLen C.size_t
    valuePtr := C.lightning_db_get(
        db.db,
        (*C.uint8_t)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
        &valueLen,
    )
    
    if valuePtr == nil {
        return nil, nil
    }
    defer C.lightning_db_free_value(valuePtr)
    
    return C.GoBytes(unsafe.Pointer(valuePtr), C.int(valueLen)), nil
}

func (db *LightningDB) Close() {
    if db.db != nil {
        C.lightning_db_close(db.db)
        db.db = nil
    }
}

// Example usage
func main() {
    db, err := OpenDB("./data/go_example.db")
    if err != nil {
        panic(err)
    }
    defer db.Close()
    
    // Store JSON data
    user := map[string]interface{}{
        "name": "Charlie",
        "email": "charlie@example.com",
        "active": true,
    }
    
    userData, _ := json.Marshal(user)
    err = db.Put([]byte("user:charlie"), userData)
    if err != nil {
        panic(err)
    }
    
    // Retrieve data
    retrieved, err := db.Get([]byte("user:charlie"))
    if err != nil {
        panic(err)
    }
    
    if retrieved != nil {
        var retrievedUser map[string]interface{}
        json.Unmarshal(retrieved, &retrievedUser)
        fmt.Printf("Retrieved user: %v\n", retrievedUser)
    }
}
```

---

## ORM Integration

### Diesel ORM Integration

```rust
// schema.rs
table! {
    users (id) {
        id -> Integer,
        name -> Text,
        email -> Text,
        created_at -> Timestamp,
    }
}

// models.rs
use diesel::prelude::*;
use serde::{Serialize, Deserialize};

#[derive(Queryable, Insertable, Serialize, Deserialize)]
#[table_name = "users"]
pub struct User {
    pub id: i32,
    pub name: String,
    pub email: String,
    pub created_at: chrono::NaiveDateTime,
}

// lightning_diesel_backend.rs
use diesel::backend::Backend;
use diesel::connection::SimpleConnection;
use lightning_db::{Database, LightningDbConfig};

pub struct LightningDbBackend {
    db: Database,
}

impl Backend for LightningDbBackend {
    type QueryBuilder = LightningQueryBuilder;
    type BindCollector = LightningBindCollector;
    type RawValue = Vec<u8>;
}

// Usage example
use diesel::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = LightningDbConnection::establish("./data/diesel.db")?;
    
    // Insert user
    let new_user = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        created_at: chrono::Utc::now().naive_utc(),
    };
    
    diesel::insert_into(users::table)
        .values(&new_user)
        .execute(&connection)?;
    
    // Query users
    let results = users::table
        .filter(users::name.eq("Alice"))
        .load::<User>(&connection)?;
    
    for user in results {
        println!("Found user: {} - {}", user.name, user.email);
    }
    
    Ok(())
}
```

### SQLx Integration

```rust
use sqlx::{Pool, Sqlite};
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;

// Custom SQLx driver for Lightning DB
pub struct LightningDbDriver {
    db: Arc<Database>,
}

impl LightningDbDriver {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config = LightningDbConfig::default();
        let db = Database::open_or_create(path, config)?;
        Ok(Self { db: Arc::new(db) })
    }
}

// Example usage with async query
async fn example_sqlx() -> Result<(), Box<dyn std::error::Error>> {
    // For demonstration - would need full SQLx driver implementation
    let pool = Pool::<Sqlite>::connect("sqlite::memory:").await?;
    
    // Create table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        )
        "#,
    )
    .execute(&pool)
    .await?;
    
    // Insert user
    sqlx::query("INSERT INTO users (name, email) VALUES (?, ?)")
        .bind("Bob")
        .bind("bob@example.com")
        .execute(&pool)
        .await?;
    
    // Query users
    let users = sqlx::query_as::<_, (i32, String, String)>(
        "SELECT id, name, email FROM users WHERE name = ?"
    )
    .bind("Bob")
    .fetch_all(&pool)
    .await?;
    
    for (id, name, email) in users {
        println!("User {}: {} - {}", id, name, email);
    }
    
    Ok(())
}
```

---

## Microservices

### gRPC Service

```proto
// lightning_db.proto
syntax = "proto3";

package lightning_db;

service LightningDbService {
    rpc Get(GetRequest) returns (GetResponse);
    rpc Put(PutRequest) returns (PutResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc Scan(ScanRequest) returns (stream ScanResponse);
}

message GetRequest {
    bytes key = 1;
}

message GetResponse {
    bool found = 1;
    bytes value = 2;
}

message PutRequest {
    bytes key = 1;
    bytes value = 2;
}

message PutResponse {
    bool success = 1;
}
```

```rust
// grpc_server.rs
use tonic::{transport::Server, Request, Response, Status};
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod lightning_db_proto {
    tonic::include_proto!("lightning_db");
}

use lightning_db_proto::{
    lightning_db_service_server::{LightningDbService, LightningDbServiceServer},
    GetRequest, GetResponse, PutRequest, PutResponse,
};

pub struct LightningDbGrpcService {
    db: Arc<RwLock<Database>>,
}

#[tonic::async_trait]
impl LightningDbService for LightningDbGrpcService {
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;
        let db = self.db.read().await;
        
        match db.get(&key) {
            Ok(Some(value)) => Ok(Response::new(GetResponse {
                found: true,
                value,
            })),
            Ok(None) => Ok(Response::new(GetResponse {
                found: false,
                value: vec![],
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
    
    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let db = self.db.write().await;
        
        match db.put(&req.key, &req.value) {
            Ok(_) => Ok(Response::new(PutResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open_or_create("./data/grpc.db", LightningDbConfig::default())?;
    let service = LightningDbGrpcService {
        db: Arc::new(RwLock::new(db)),
    };
    
    Server::builder()
        .add_service(LightningDbServiceServer::new(service))
        .serve("[::1]:50051".parse()?)
        .await?;
    
    Ok(())
}
```

### REST API Service

```rust
use warp::{Filter, Rejection, Reply};
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

type Db = Arc<RwLock<Database>>;

#[derive(Deserialize, Serialize)]
struct KeyValue {
    key: String,
    value: serde_json::Value,
}

async fn get_handler(key: String, db: Db) -> Result<impl Reply, Rejection> {
    let db = db.read().await;
    match db.get(key.as_bytes()) {
        Ok(Some(value)) => {
            match serde_json::from_slice::<serde_json::Value>(&value) {
                Ok(json) => Ok(warp::reply::json(&json)),
                Err(_) => Err(warp::reject::reject()),
            }
        }
        Ok(None) => Err(warp::reject::not_found()),
        Err(_) => Err(warp::reject::reject()),
    }
}

async fn put_handler(kv: KeyValue, db: Db) -> Result<impl Reply, Rejection> {
    let db = db.write().await;
    let value = serde_json::to_vec(&kv.value).map_err(|_| warp::reject::reject())?;
    
    match db.put(kv.key.as_bytes(), &value) {
        Ok(_) => Ok(warp::reply::with_status("Created", warp::http::StatusCode::CREATED)),
        Err(_) => Err(warp::reject::reject()),
    }
}

#[tokio::main]
async fn main() {
    let db = Database::open_or_create("./data/rest.db", LightningDbConfig::default())
        .expect("Failed to open database");
    let db = Arc::new(RwLock::new(db));
    
    let db_filter = warp::any().map(move || db.clone());
    
    let get_route = warp::path!("api" / "v1" / "data" / String)
        .and(warp::get())
        .and(db_filter.clone())
        .and_then(get_handler);
    
    let put_route = warp::path!("api" / "v1" / "data")
        .and(warp::post())
        .and(warp::body::json())
        .and(db_filter.clone())
        .and_then(put_handler);
    
    let routes = get_route.or(put_route);
    
    warp::serve(routes)
        .run(([127, 0, 0, 1], 3030))
        .await;
}
```

---

## Testing Integration

### Unit Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use lightning_db::{Database, LightningDbConfig};
    use tempfile::TempDir;
    
    fn create_test_db() -> (Database, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let config = LightningDbConfig::default();
        let db = Database::create(db_path, config).unwrap();
        (db, temp_dir)
    }
    
    #[test]
    fn test_basic_operations() {
        let (db, _temp_dir) = create_test_db();
        
        // Test put
        db.put(b"key1", b"value1").unwrap();
        
        // Test get
        let value = db.get(b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
        
        // Test delete
        db.delete(b"key1").unwrap();
        assert_eq!(db.get(b"key1").unwrap(), None);
    }
    
    #[test]
    fn test_transactions() {
        let (db, _temp_dir) = create_test_db();
        
        // Successful transaction
        let mut tx = db.begin_transaction().unwrap();
        tx.put(b"tx_key", b"tx_value").unwrap();
        tx.commit().unwrap();
        
        assert_eq!(db.get(b"tx_key").unwrap(), Some(b"tx_value".to_vec()));
        
        // Aborted transaction
        let mut tx = db.begin_transaction().unwrap();
        tx.put(b"abort_key", b"abort_value").unwrap();
        tx.rollback().unwrap();
        
        assert_eq!(db.get(b"abort_key").unwrap(), None);
    }
}
```

### Integration Testing

```rust
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_concurrent_access() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().join("concurrent.db");
    let db = Arc::new(Database::create(&db_path, LightningDbConfig::default()).unwrap());
    
    let mut handles = vec![];
    
    // Spawn writers
    for i in 0..5 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            for j in 0..100 {
                let key = format!("writer{}:key{}", i, j);
                let value = format!("value{}", j);
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                thread::sleep(Duration::from_micros(10));
            }
        });
        handles.push(handle);
    }
    
    // Spawn readers
    for i in 0..5 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            for j in 0..100 {
                let key = format!("writer{}:key{}", i % 5, j);
                let _ = db_clone.get(key.as_bytes());
                thread::sleep(Duration::from_micros(10));
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify data
    for i in 0..5 {
        for j in 0..100 {
            let key = format!("writer{}:key{}", i, j);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some());
        }
    }
}
```

### Property-Based Testing

```rust
use proptest::prelude::*;
use lightning_db::{Database, LightningDbConfig};

proptest! {
    #[test]
    fn test_put_get_consistency(
        key in prop::collection::vec(any::<u8>(), 1..100),
        value in prop::collection::vec(any::<u8>(), 1..1000)
    ) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db = Database::create(
            temp_dir.path().join("prop.db"),
            LightningDbConfig::default()
        ).unwrap();
        
        // Put value
        db.put(&key, &value).unwrap();
        
        // Get should return same value
        let retrieved = db.get(&key).unwrap();
        prop_assert_eq!(retrieved, Some(value));
    }
    
    #[test]
    fn test_transaction_isolation(
        keys in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 1..50),
            1..10
        ),
        values in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 1..100),
            1..10
        )
    ) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db = Database::create(
            temp_dir.path().join("iso.db"),
            LightningDbConfig::default()
        ).unwrap();
        
        // Start transaction
        let mut tx = db.begin_transaction().unwrap();
        
        // Put all key-value pairs
        for (key, value) in keys.iter().zip(values.iter()) {
            tx.put(key, value).unwrap();
        }
        
        // Values should not be visible outside transaction
        for key in &keys {
            prop_assert_eq!(db.get(key).unwrap(), None);
        }
        
        // Commit transaction
        tx.commit().unwrap();
        
        // Now values should be visible
        for (key, value) in keys.iter().zip(values.iter()) {
            prop_assert_eq!(db.get(key).unwrap(), Some(value.clone()));
        }
    }
}
```

---

## CI/CD Integration

### GitHub Actions

```yaml
# .github/workflows/lightning-db-ci.yml
name: Lightning DB CI

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Install Rust
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true
    
    - name: Cache dependencies
      uses: actions/cache@v3
      with:
        path: |
          ~/.cargo/registry
          ~/.cargo/git
          target
        key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}
    
    - name: Run tests
      run: cargo test --all-features
    
    - name: Run benchmarks
      run: cargo bench --no-run
    
    - name: Check formatting
      run: cargo fmt -- --check
    
    - name: Run clippy
      run: cargo clippy -- -D warnings
    
    - name: Build release
      run: cargo build --release
    
    - name: Integration tests
      run: |
        cd integration_tests
        cargo test --release
```

### GitLab CI

```yaml
# .gitlab-ci.yml
stages:
  - build
  - test
  - deploy

variables:
  CARGO_HOME: $CI_PROJECT_DIR/.cargo

build:
  stage: build
  image: rust:latest
  script:
    - cargo build --release
  artifacts:
    paths:
      - target/release/
    expire_in: 1 week
  cache:
    key: ${CI_COMMIT_REF_SLUG}
    paths:
      - .cargo/
      - target/

test:
  stage: test
  image: rust:latest
  script:
    - cargo test --all-features
    - cargo bench --no-run
  dependencies:
    - build

integration-test:
  stage: test
  image: rust:latest
  script:
    - cd integration_tests
    - cargo test --release
  dependencies:
    - build
```

---

## Container Integration

### Docker

```dockerfile
# Dockerfile
FROM rust:1.75 as builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/lightning_db_server /usr/local/bin/

EXPOSE 8080

CMD ["lightning_db_server"]
```

### Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  lightning-db:
    build: .
    ports:
      - "8080:8080"
    volumes:
      - db_data:/data
    environment:
      - LIGHTNING_DB_PATH=/data/lightning.db
      - LIGHTNING_DB_CACHE_SIZE=1073741824
      - RUST_LOG=info
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s

volumes:
  db_data:
```

### Kubernetes

```yaml
# k8s-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lightning-db
spec:
  replicas: 3
  selector:
    matchLabels:
      app: lightning-db
  template:
    metadata:
      labels:
        app: lightning-db
    spec:
      containers:
      - name: lightning-db
        image: lightning-db:latest
        ports:
        - containerPort: 8080
        env:
        - name: LIGHTNING_DB_PATH
          value: /data/lightning.db
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: lightning-db-pvc
```

---

## Best Practices

1. **Connection Pooling** - Reuse database connections
2. **Error Handling** - Always handle database errors gracefully
3. **Resource Cleanup** - Ensure proper cleanup in destructors
4. **Configuration** - Use environment variables for configuration
5. **Monitoring** - Add metrics and health checks
6. **Testing** - Test with realistic data sizes
7. **Documentation** - Document your integration patterns

---

For more examples, see the [examples directory](../examples/).