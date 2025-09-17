use lightning_db::Database;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct User {
    id: u64,
    username: String,
    email: String,
    created_at: u64,
    last_login: u64,
    profile: UserProfile,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct UserProfile {
    full_name: String,
    bio: String,
    avatar_url: String,
    preferences: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Session {
    session_id: String,
    user_id: u64,
    created_at: u64,
    expires_at: u64,
    ip_address: String,
    user_agent: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TimeSeries {
    timestamp: u64,
    metric: String,
    value: f64,
    tags: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogEntry {
    timestamp: u64,
    level: String,
    message: String,
    context: HashMap<String, serde_json::Value>,
}

#[test]
fn test_user_management_workflow() {
    let db = Database::create_temp().unwrap();

    let mut users = Vec::new();
    for i in 0..100 {
        let user = User {
            id: i as u64,
            username: format!("user_{}", i),
            email: format!("user{}@example.com", i),
            created_at: get_timestamp(),
            last_login: get_timestamp(),
            profile: UserProfile {
                full_name: format!("User {}", i),
                bio: format!("Bio for user {}", i),
                avatar_url: format!("https://avatar.com/{}", i),
                preferences: {
                    let mut prefs = HashMap::new();
                    prefs.insert("theme".to_string(), "dark".to_string());
                    prefs.insert("language".to_string(), "en".to_string());
                    prefs
                },
            },
        };
        users.push(user.clone());

        let key = format!("user:{}", user.id);
        let value = serde_json::to_vec(&user).unwrap();
        db.put(key.as_bytes(), &value).unwrap();
    }

    for user in &users {
        let key = format!("user:{}", user.id);
        let stored = db.get(key.as_bytes()).unwrap().unwrap();
        let retrieved: User = serde_json::from_slice(&stored).unwrap();
        assert_eq!(&retrieved, user);
    }

    let key = format!("user:{}", 50);
    let stored = db.get(key.as_bytes()).unwrap().unwrap();
    let mut user: User = serde_json::from_slice(&stored).unwrap();
    user.last_login = get_timestamp();
    user.profile.preferences.insert("notifications".to_string(), "enabled".to_string());

    let value = serde_json::to_vec(&user).unwrap();
    db.put(key.as_bytes(), &value).unwrap();

    let updated = db.get(key.as_bytes()).unwrap().unwrap();
    let retrieved: User = serde_json::from_slice(&updated).unwrap();
    assert_eq!(retrieved.profile.preferences.get("notifications"), Some(&"enabled".to_string()));
}

#[test]
fn test_session_management() {
    let db = Database::create_temp().unwrap();

    let sessions: Vec<Session> = (0..1000)
        .map(|i| Session {
            session_id: format!("sess_{:032x}", i),
            user_id: (i % 100) as u64,
            created_at: get_timestamp(),
            expires_at: get_timestamp() + 3600,
            ip_address: format!("192.168.1.{}", i % 256),
            user_agent: format!("Mozilla/5.0 (Test {})", i),
        })
        .collect();

    let tx = db.begin_transaction().unwrap();
    for session in &sessions {
        let key = format!("session:{}", session.session_id);
        let value = serde_json::to_vec(&session).unwrap();
        db.put_tx(tx, key.as_bytes(), &value).unwrap();

        let user_key = format!("user_sessions:{}", session.user_id);
        db.put_tx(tx, user_key.as_bytes(), session.session_id.as_bytes()).unwrap();
    }
    db.commit_transaction(tx).unwrap();

    for session in sessions.iter().take(10) {
        let key = format!("session:{}", session.session_id);
        let stored = db.get(key.as_bytes()).unwrap().unwrap();
        let retrieved: Session = serde_json::from_slice(&stored).unwrap();
        assert_eq!(retrieved.session_id, session.session_id);
    }

    for session in sessions.iter().take(50) {
        let key = format!("session:{}", session.session_id);
        db.delete(key.as_bytes()).unwrap();
    }

    for session in sessions.iter().take(50) {
        let key = format!("session:{}", session.session_id);
        assert!(db.get(key.as_bytes()).unwrap().is_none());
    }
}

#[test]
fn test_time_series_data() {
    let db = Database::create_temp().unwrap();

    let metrics = ["cpu_usage", "memory_usage", "disk_io", "network_throughput"];
    let servers = ["server1", "server2", "server3", "server4", "server5"];

    let base_time = get_timestamp();

    for hour in 0..24 {
        let tx = db.begin_transaction().unwrap();

        for minute in 0..60 {
            for metric in &metrics {
                for server in &servers {
                    let timestamp = base_time + (hour * 3600) + (minute * 60);
                    let ts = TimeSeries {
                        timestamp,
                        metric: metric.to_string(),
                        value: fastrand::f64() * 100.0,
                        tags: {
                            let mut tags = HashMap::new();
                            tags.insert("server".to_string(), server.to_string());
                            tags.insert("datacenter".to_string(), "dc1".to_string());
                            tags
                        },
                    };

                    let key = format!("ts:{}:{}:{}", metric, server, timestamp);
                    let value = serde_json::to_vec(&ts).unwrap();
                    db.put_tx(tx, key.as_bytes(), &value).unwrap();
                }
            }
        }

        db.commit_transaction(tx).unwrap();
    }

    let key_prefix = "ts:cpu_usage:server1:";
    let start_time = base_time;
    let end_time = base_time + 3600;

    let mut count = 0;
    for minute in 0..60 {
        let timestamp = start_time + (minute * 60);
        let key = format!("{}{}", key_prefix, timestamp);

        if let Some(value) = db.get(key.as_bytes()).unwrap() {
            let ts: TimeSeries = serde_json::from_slice(&value).unwrap();
            assert!(ts.timestamp >= start_time && ts.timestamp < end_time);
            count += 1;
        }
    }
    assert_eq!(count, 60);
}

#[test]
fn test_cache_pattern() {
    let db = Arc::new(Database::create_temp().unwrap());
    let barrier = Arc::new(Barrier::new(5));
    let mut handles = vec![];

    for thread_id in 0..5 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..200 {
                let cache_key = format!("cache:item:{}", (thread_id * 200) + i);
                let cache_value = format!("cached_value_{}", i);

                if db_clone.get(cache_key.as_bytes()).unwrap().is_none() {
                    db_clone.put(cache_key.as_bytes(), cache_value.as_bytes()).unwrap();
                }

                let _ = db_clone.get(cache_key.as_bytes()).unwrap();

                if i % 10 == 0 {
                    db_clone.delete(cache_key.as_bytes()).unwrap();
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_document_storage() {
    let db = Database::create_temp().unwrap();

    let documents = vec![
        serde_json::json!({
            "id": "doc1",
            "type": "article",
            "title": "Introduction to Rust",
            "content": "Rust is a systems programming language...",
            "author": {
                "name": "Jane Doe",
                "email": "jane@example.com"
            },
            "tags": ["rust", "programming", "systems"],
            "created_at": get_timestamp(),
            "views": 1000
        }),
        serde_json::json!({
            "id": "doc2",
            "type": "product",
            "name": "Lightning DB",
            "description": "A fast embedded database",
            "price": 0.0,
            "features": ["ACID", "B+Tree", "LSM", "MVCC"],
            "metadata": {
                "version": "0.1.0",
                "license": "MIT"
            }
        }),
        serde_json::json!({
            "id": "doc3",
            "type": "config",
            "settings": {
                "database": {
                    "max_connections": 100,
                    "timeout": 30,
                    "cache_size": "1GB"
                },
                "logging": {
                    "level": "info",
                    "format": "json"
                }
            }
        }),
    ];

    for doc in &documents {
        let id = doc["id"].as_str().unwrap();
        let key = format!("doc:{}", id);
        let value = serde_json::to_vec(&doc).unwrap();
        db.put(key.as_bytes(), &value).unwrap();
    }

    for doc in &documents {
        let id = doc["id"].as_str().unwrap();
        let key = format!("doc:{}", id);
        let stored = db.get(key.as_bytes()).unwrap().unwrap();
        let retrieved: serde_json::Value = serde_json::from_slice(&stored).unwrap();
        assert_eq!(&retrieved, doc);
    }
}

#[test]
fn test_graph_relationships() {
    let db = Database::create_temp().unwrap();

    for i in 0..100 {
        let node_key = format!("node:{}", i);
        let node_data = format!("{{\"id\":{},\"label\":\"node_{}\"}}", i, i);
        db.put(node_key.as_bytes(), node_data.as_bytes()).unwrap();
    }

    for i in 0..100 {
        for j in 0..5 {
            let target = (i * 7 + j * 13) % 100;
            let edge_key = format!("edge:{}:{}", i, target);
            let edge_data = format!("{{\"from\":{},\"to\":{},\"weight\":{}}}", i, target, j + 1);
            db.put(edge_key.as_bytes(), edge_data.as_bytes()).unwrap();

            let index_key = format!("index:edges_from:{}", i);
            let existing = db.get(index_key.as_bytes()).unwrap().unwrap_or_default();
            let mut edges: Vec<u32> = if existing.is_empty() {
                Vec::new()
            } else {
                serde_json::from_slice(&existing).unwrap_or_default()
            };
            edges.push(target as u32);
            let updated = serde_json::to_vec(&edges).unwrap();
            db.put(index_key.as_bytes(), &updated).unwrap();
        }
    }

    let node_50_edges_key = b"index:edges_from:50";
    let edges_data = db.get(node_50_edges_key).unwrap().unwrap();
    let edges: Vec<u32> = serde_json::from_slice(&edges_data).unwrap();
    assert_eq!(edges.len(), 5);
}

#[test]
fn test_high_throughput_logging() {
    let db = Arc::new(Database::create_temp().unwrap());
    let barrier = Arc::new(Barrier::new(10));
    let mut handles = vec![];

    let levels = vec!["DEBUG", "INFO", "WARN", "ERROR"];

    for thread_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let levels = levels.clone();

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..100 {
                let timestamp = get_timestamp() + i;
                let log_entry = LogEntry {
                    timestamp,
                    level: levels[(i as usize) % levels.len()].to_string(),
                    message: format!("Log message {} from thread {}", i, thread_id),
                    context: {
                        let mut ctx = HashMap::new();
                        ctx.insert("thread_id".to_string(), serde_json::json!(thread_id));
                        ctx.insert("iteration".to_string(), serde_json::json!(i));
                        ctx.insert("timestamp".to_string(), serde_json::json!(timestamp));
                        ctx
                    },
                };

                let key = format!("log:{}:{}:{}", timestamp, thread_id, i);
                let value = serde_json::to_vec(&log_entry).unwrap();
                db_clone.put(key.as_bytes(), &value).unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_analytics_aggregation() {
    let db = Database::create_temp().unwrap();

    #[derive(Serialize, Deserialize)]
    struct Event {
        event_type: String,
        user_id: u64,
        timestamp: u64,
        properties: HashMap<String, serde_json::Value>,
    }

    let event_types = ["page_view", "click", "purchase", "signup"];

    for day in 0..7 {
        let tx = db.begin_transaction().unwrap();

        for hour in 0..24 {
            for user_id in 0..100 {
                for event_type in &event_types {
                    if fastrand::f64() > 0.7 {
                        continue;
                    }

                    let timestamp = get_timestamp() + (day * 86400) + (hour * 3600);
                    let event = Event {
                        event_type: event_type.to_string(),
                        user_id,
                        timestamp,
                        properties: {
                            let mut props = HashMap::new();
                            props.insert("page".to_string(), serde_json::json!("/home"));
                            props.insert("duration".to_string(), serde_json::json!(fastrand::u64(1..300)));
                            props
                        },
                    };

                    let key = format!("event:{}:{}:{}:{}", day, hour, user_id, event_type);
                    let value = serde_json::to_vec(&event).unwrap();
                    db.put_tx(tx, key.as_bytes(), &value).unwrap();

                    let daily_key = format!("stats:daily:{}:{}", day, event_type);
                    let count = if let Some(data) = db.get_tx(tx, daily_key.as_bytes()).unwrap() {
                        let mut c: u64 = serde_json::from_slice(&data).unwrap();
                        c += 1;
                        c
                    } else {
                        1
                    };
                    db.put_tx(tx, daily_key.as_bytes(), &serde_json::to_vec(&count).unwrap()).unwrap();
                }
            }
        }

        db.commit_transaction(tx).unwrap();
    }

    for day in 0..7 {
        for event_type in &event_types {
            let key = format!("stats:daily:{}:{}", day, event_type);
            if let Some(data) = db.get(key.as_bytes()).unwrap() {
                let count: u64 = serde_json::from_slice(&data).unwrap();
                assert!(count > 0);
            }
        }
    }
}

#[test]
fn test_bulk_import_export() {
    let db = Database::create_temp().unwrap();

    let mut batch_data = Vec::new();
    for i in 0..10000 {
        let key = format!("bulk:item:{:08}", i);
        let value = format!("{{\"id\":{},\"data\":\"value_{}\"}}", i, i);
        batch_data.push((key, value));
    }

    let chunk_size = 1000;
    for chunk in batch_data.chunks(chunk_size) {
        let tx = db.begin_transaction().unwrap();
        for (key, value) in chunk {
            db.put_tx(tx, key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.commit_transaction(tx).unwrap();
    }

    for i in (0..10000).step_by(100) {
        let key = format!("bulk:item:{:08}", i);
        let value = db.get(key.as_bytes()).unwrap().unwrap();
        let expected = format!("{{\"id\":{},\"data\":\"value_{}\"}}", i, i);
        assert_eq!(value, expected.as_bytes());
    }
}

#[test]
fn test_mixed_workload() {
    let db = Arc::new(Database::create_temp().unwrap());
    let barrier = Arc::new(Barrier::new(8));
    let mut handles = vec![];

    for _ in 0..2 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier_clone.wait();
            for i in 0..500 {
                let key = format!("write:{}", i);
                db_clone.put(key.as_bytes(), format!("value_{}", i).as_bytes()).unwrap();
                thread::sleep(Duration::from_micros(100));
            }
        }));
    }

    for _ in 0..3 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier_clone.wait();
            for i in 0..1000 {
                let key = format!("write:{}", i % 500);
                let _ = db_clone.get(key.as_bytes());
                thread::sleep(Duration::from_micros(50));
            }
        }));
    }

    for _ in 0..2 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier_clone.wait();
            for i in 0..100 {
                let tx = db_clone.begin_transaction().unwrap();
                let mut success = true;
                for j in 0..10 {
                    let key = format!("tx:{}:{}", i, j);
                    if db_clone.put_tx(tx, key.as_bytes(), b"tx_value").is_err() {
                        success = false;
                        break;
                    }
                }
                if success {
                    let _ = db_clone.commit_transaction(tx);
                } else {
                    let _ = db_clone.abort_transaction(tx);
                }
                thread::sleep(Duration::from_millis(5));
            }
        }));
    }

    let db_clone = Arc::clone(&db);
    let barrier_clone = Arc::clone(&barrier);
    handles.push(thread::spawn(move || {
        barrier_clone.wait();
        for i in 0..200 {
            let key = format!("write:{}", i);
            db_clone.delete(key.as_bytes()).unwrap();
            thread::sleep(Duration::from_millis(10));
        }
    }));

    for handle in handles {
        handle.join().unwrap();
    }
}

fn get_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}