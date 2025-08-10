//! Encryption Performance Benchmarks for Lightning DB
//!
//! Tests the performance impact of encryption on various database operations

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use lightning_db::{
    encryption::{EncryptionAlgorithm, EncryptionConfig, EncryptionManager, KeyDerivationFunction},
    Database, LightningDbConfig,
};
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use std::hint::black_box;
use std::time::Duration;
use tempfile::TempDir;

/// Create test configuration with specified encryption settings
fn create_config(enable_encryption: bool, algorithm: EncryptionAlgorithm) -> LightningDbConfig {
    let mut config = LightningDbConfig::default();

    // Configure cache settings
    config.cache_size = 64 * 1024 * 1024; // 64MB
    config.compression_enabled = false; // Disable to isolate encryption performance

    // Configure encryption
    config.encryption_config = EncryptionConfig {
        enabled: enable_encryption,
        algorithm,
        kdf: KeyDerivationFunction::Argon2id,
        key_rotation_interval_days: 90,
        hardware_acceleration: true,
        encrypt_wal: true,
        encrypt_pages: true,
    };

    config
}

/// Generate random key-value pairs
fn generate_data(
    count: usize,
    key_size: usize,
    value_size: usize,
    seed: u64,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = Vec::with_capacity(count);

    for _ in 0..count {
        let mut key = vec![0u8; key_size];
        let mut value = vec![0u8; value_size];
        rng.fill(&mut key[..]);
        rng.fill(&mut value[..]);
        data.push((key, value));
    }

    data
}

/// Benchmark page encryption performance
fn bench_page_encryption(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_encryption");
    group.measurement_time(Duration::from_secs(10));

    // Test different page sizes
    for page_size in &[4096, 8192, 16384, 32768] {
        let page_data = vec![0u8; *page_size];

        // AES-256-GCM
        group.bench_function(&format!("aes_gcm_{}_bytes", page_size), |b| {
            let config = EncryptionConfig {
                enabled: true,
                algorithm: EncryptionAlgorithm::Aes256Gcm,
                ..Default::default()
            };
            let manager = EncryptionManager::new(config).unwrap();
            let master_key = vec![0x42; 32];
            manager.initialize(&master_key).unwrap();

            b.iter(|| {
                let encrypted = manager
                    .encrypt_page(black_box(1), black_box(&page_data))
                    .unwrap();
                black_box(encrypted);
            });
        });

        // ChaCha20-Poly1305
        group.bench_function(&format!("chacha20_{}_bytes", page_size), |b| {
            let config = EncryptionConfig {
                enabled: true,
                algorithm: EncryptionAlgorithm::ChaCha20Poly1305,
                ..Default::default()
            };
            let manager = EncryptionManager::new(config).unwrap();
            let master_key = vec![0x42; 32];
            manager.initialize(&master_key).unwrap();

            b.iter(|| {
                let encrypted = manager
                    .encrypt_page(black_box(1), black_box(&page_data))
                    .unwrap();
                black_box(encrypted);
            });
        });
    }

    group.finish();
}

/// Benchmark page decryption performance
fn bench_page_decryption(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_decryption");
    group.measurement_time(Duration::from_secs(10));

    for page_size in &[4096, 8192, 16384, 32768] {
        let page_data = vec![0u8; *page_size];

        // AES-256-GCM
        group.bench_function(&format!("aes_gcm_{}_bytes", page_size), |b| {
            let config = EncryptionConfig {
                enabled: true,
                algorithm: EncryptionAlgorithm::Aes256Gcm,
                ..Default::default()
            };
            let manager = EncryptionManager::new(config).unwrap();
            let master_key = vec![0x42; 32];
            manager.initialize(&master_key).unwrap();
            let encrypted = manager.encrypt_page(1, &page_data).unwrap();

            b.iter(|| {
                let decrypted = manager
                    .decrypt_page(black_box(1), black_box(&encrypted))
                    .unwrap();
                black_box(decrypted);
            });
        });

        // ChaCha20-Poly1305
        group.bench_function(&format!("chacha20_{}_bytes", page_size), |b| {
            let config = EncryptionConfig {
                enabled: true,
                algorithm: EncryptionAlgorithm::ChaCha20Poly1305,
                ..Default::default()
            };
            let manager = EncryptionManager::new(config).unwrap();
            let master_key = vec![0x42; 32];
            manager.initialize(&master_key).unwrap();
            let encrypted = manager.encrypt_page(1, &page_data).unwrap();

            b.iter(|| {
                let decrypted = manager
                    .decrypt_page(black_box(1), black_box(&encrypted))
                    .unwrap();
                black_box(decrypted);
            });
        });
    }

    group.finish();
}

/// Benchmark database operations with encryption enabled vs disabled
fn bench_database_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("database_operations");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(50);

    let data_count = 1000;
    let test_data = generate_data(data_count, 32, 1024, 42);

    // Benchmark PUT operations
    group.bench_function("put_no_encryption", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let config = create_config(false, EncryptionAlgorithm::Aes256Gcm);
                let db = Database::create(temp_dir.path(), config).unwrap();
                (db, temp_dir, test_data.clone())
            },
            |(db, _temp_dir, data)| {
                for (key, value) in data.iter() {
                    db.put(black_box(key), black_box(value)).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("put_aes_gcm", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let config = create_config(true, EncryptionAlgorithm::Aes256Gcm);
                let db = Database::create(temp_dir.path(), config).unwrap();
                let master_key = vec![0x42; 32];
                db.initialize_encryption(&master_key).unwrap();
                (db, temp_dir, test_data.clone())
            },
            |(db, _temp_dir, data)| {
                for (key, value) in data.iter() {
                    db.put(black_box(key), black_box(value)).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("put_chacha20", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let config = create_config(true, EncryptionAlgorithm::ChaCha20Poly1305);
                let db = Database::create(temp_dir.path(), config).unwrap();
                let master_key = vec![0x42; 32];
                db.initialize_encryption(&master_key).unwrap();
                (db, temp_dir, test_data.clone())
            },
            |(db, _temp_dir, data)| {
                for (key, value) in data.iter() {
                    db.put(black_box(key), black_box(value)).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });

    // Benchmark GET operations
    group.bench_function("get_no_encryption", |b| {
        let temp_dir = TempDir::new().unwrap();
        let config = create_config(false, EncryptionAlgorithm::Aes256Gcm);
        let db = Database::create(temp_dir.path(), config).unwrap();

        // Insert test data
        for (key, value) in test_data.iter() {
            db.put(key, value).unwrap();
        }

        b.iter(|| {
            for (key, _) in test_data.iter() {
                let value = db.get(black_box(key)).unwrap();
                black_box(value);
            }
        });
    });

    group.bench_function("get_aes_gcm", |b| {
        let temp_dir = TempDir::new().unwrap();
        let config = create_config(true, EncryptionAlgorithm::Aes256Gcm);
        let db = Database::create(temp_dir.path(), config).unwrap();
        let master_key = vec![0x42; 32];
        db.initialize_encryption(&master_key).unwrap();

        // Insert test data
        for (key, value) in test_data.iter() {
            db.put(key, value).unwrap();
        }

        b.iter(|| {
            for (key, _) in test_data.iter() {
                let value = db.get(black_box(key)).unwrap();
                black_box(value);
            }
        });
    });

    group.bench_function("get_chacha20", |b| {
        let temp_dir = TempDir::new().unwrap();
        let config = create_config(true, EncryptionAlgorithm::ChaCha20Poly1305);
        let db = Database::create(temp_dir.path(), config).unwrap();
        let master_key = vec![0x42; 32];
        db.initialize_encryption(&master_key).unwrap();

        // Insert test data
        for (key, value) in test_data.iter() {
            db.put(key, value).unwrap();
        }

        b.iter(|| {
            for (key, _) in test_data.iter() {
                let value = db.get(black_box(key)).unwrap();
                black_box(value);
            }
        });
    });

    group.finish();
}

/// Benchmark key derivation functions
fn bench_key_derivation(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_derivation");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20); // KDF operations are slow

    let master_key = vec![0x42; 32];

    // We can benchmark the initialization process which includes key derivation
    group.bench_function("argon2id_init", |b| {
        b.iter(|| {
            let config = EncryptionConfig {
                enabled: true,
                kdf: KeyDerivationFunction::Argon2id,
                ..Default::default()
            };
            let manager = EncryptionManager::new(config).unwrap();
            manager.initialize(&master_key).unwrap();
        });
    });

    group.bench_function("pbkdf2_sha256_init", |b| {
        b.iter(|| {
            let config = EncryptionConfig {
                enabled: true,
                kdf: KeyDerivationFunction::Pbkdf2Sha256,
                ..Default::default()
            };
            let manager = EncryptionManager::new(config).unwrap();
            manager.initialize(&master_key).unwrap();
        });
    });

    group.finish();
}

/// Benchmark WAL encryption performance
fn bench_wal_encryption(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_encryption");
    group.measurement_time(Duration::from_secs(10));

    // Test different WAL entry sizes
    for entry_size in &[64, 256, 1024, 4096] {
        let entry_data = vec![0u8; *entry_size];

        group.bench_function(&format!("encrypt_{}_bytes", entry_size), |b| {
            let config = EncryptionConfig {
                enabled: true,
                algorithm: EncryptionAlgorithm::Aes256Gcm,
                ..Default::default()
            };
            let manager = EncryptionManager::new(config).unwrap();
            let master_key = vec![0x42; 32];
            manager.initialize(&master_key).unwrap();

            let mut entry_id = 0;
            b.iter(|| {
                let encrypted = manager
                    .encrypt_wal_entry(black_box(entry_id), black_box(&entry_data))
                    .unwrap();
                entry_id += 1;
                black_box(encrypted);
            });
        });

        group.bench_function(&format!("decrypt_{}_bytes", entry_size), |b| {
            let config = EncryptionConfig {
                enabled: true,
                algorithm: EncryptionAlgorithm::Aes256Gcm,
                ..Default::default()
            };
            let manager = EncryptionManager::new(config).unwrap();
            let master_key = vec![0x42; 32];
            manager.initialize(&master_key).unwrap();

            let encrypted = manager.encrypt_wal_entry(1, &entry_data).unwrap();

            b.iter(|| {
                let decrypted = manager
                    .decrypt_wal_entry(black_box(1), black_box(&encrypted))
                    .unwrap();
                black_box(decrypted);
            });
        });
    }

    group.finish();
}

/// Benchmark encryption overhead on transactions
fn bench_transaction_encryption(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_encryption");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(30);

    let tx_size = 100; // Operations per transaction
    let test_data = generate_data(tx_size, 32, 256, 12345);

    group.bench_function("transaction_no_encryption", |b| {
        let temp_dir = TempDir::new().unwrap();
        let config = create_config(false, EncryptionAlgorithm::Aes256Gcm);
        let db = Database::create(temp_dir.path(), config).unwrap();

        b.iter(|| {
            let tx_id = db.begin_transaction().unwrap();
            for (key, value) in test_data.iter() {
                db.put_tx(tx_id, black_box(key), black_box(value)).unwrap();
            }
            db.commit_transaction(tx_id).unwrap();
        });
    });

    group.bench_function("transaction_aes_gcm", |b| {
        let temp_dir = TempDir::new().unwrap();
        let config = create_config(true, EncryptionAlgorithm::Aes256Gcm);
        let db = Database::create(temp_dir.path(), config).unwrap();
        let master_key = vec![0x42; 32];
        db.initialize_encryption(&master_key).unwrap();

        b.iter(|| {
            let tx_id = db.begin_transaction().unwrap();
            for (key, value) in test_data.iter() {
                db.put_tx(tx_id, black_box(key), black_box(value)).unwrap();
            }
            db.commit_transaction(tx_id).unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_page_encryption,
    bench_page_decryption,
    bench_database_operations,
    bench_key_derivation,
    bench_wal_encryption,
    bench_transaction_encryption
);

criterion_main!(benches);
