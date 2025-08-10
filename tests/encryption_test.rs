//! Integration tests for encryption functionality

use lightning_db::{
    encryption::{EncryptionAlgorithm, EncryptionConfig, KeyDerivationFunction},
    Database, LightningDbConfig,
};
use std::fs;
use tempfile::TempDir;

/// Test basic encryption functionality
#[test]
fn test_encryption_basic_operations() {
    let temp_dir = TempDir::new().unwrap();

    // Create configuration with encryption enabled
    let mut config = LightningDbConfig::default();
    config.encryption_config = EncryptionConfig {
        enabled: true,
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        kdf: KeyDerivationFunction::Argon2id,
        key_rotation_interval_days: 90,
        hardware_acceleration: true,
        encrypt_wal: true,
        encrypt_pages: true,
    };

    // Create database with encryption
    let db = Database::create(temp_dir.path(), config).unwrap();

    // Initialize encryption with a master key
    let master_key = b"this_is_a_32_byte_master_key_ok!";
    db.initialize_encryption(master_key).unwrap();

    // Test basic put/get operations
    let test_data = vec![
        (&b"key1"[..], &b"value1"[..]),
        (&b"key2"[..], &b"value2_longer_value"[..]),
        (&b"key3"[..], &b"value3_with_special_chars_!@#$%^&*()"[..]),
        (&b"key4"[..], "value4_with_unicode_🚀🔒💾".as_bytes()),
    ];

    // Insert data
    for (key, value) in &test_data {
        db.put(key, value).unwrap();
    }

    // Verify data can be read back
    for (key, expected_value) in &test_data {
        let value = db.get(key).unwrap();
        assert_eq!(value.as_deref(), Some(*expected_value));
    }

    // Test transactions with encryption
    let tx_id = db.begin_transaction().unwrap();
    db.put_tx(tx_id, b"tx_key1", b"tx_value1").unwrap();
    db.put_tx(tx_id, b"tx_key2", b"tx_value2").unwrap();
    db.commit_transaction(tx_id).unwrap();

    // Verify transaction data
    assert_eq!(
        db.get(b"tx_key1").unwrap().as_deref(),
        Some(b"tx_value1".as_ref())
    );
    assert_eq!(
        db.get(b"tx_key2").unwrap().as_deref(),
        Some(b"tx_value2".as_ref())
    );
}

/// Test that encrypted data cannot be read without the correct key
#[test]
fn test_encryption_data_protection() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Create configuration with encryption enabled
    let mut config = LightningDbConfig::default();
    config.encryption_config = EncryptionConfig {
        enabled: true,
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        kdf: KeyDerivationFunction::Argon2id,
        key_rotation_interval_days: 90,
        hardware_acceleration: true,
        encrypt_wal: true,
        encrypt_pages: true,
    };

    // Create and populate encrypted database
    {
        let db = Database::create(&db_path, config.clone()).unwrap();
        let master_key = b"correct_master_key_32_bytes_long";
        db.initialize_encryption(master_key).unwrap();

        // Insert sensitive data
        db.put(b"secret_key", b"secret_value").unwrap();
        db.put(b"password", b"my_secure_password").unwrap();

        // Force flush to disk
        drop(db);
    }

    // Verify raw data files are encrypted (not readable as plaintext)
    let data_files = fs::read_dir(&db_path).unwrap();
    for entry in data_files {
        let entry = entry.unwrap();
        if entry.path().extension().map_or(false, |ext| ext == "db") {
            let contents = fs::read(&entry.path()).unwrap();
            // Verify the sensitive strings don't appear in plaintext
            let contents_str = String::from_utf8_lossy(&contents);
            assert!(!contents_str.contains("secret_value"));
            assert!(!contents_str.contains("my_secure_password"));
        }
    }

    // Try to open with wrong key - should fail or return wrong data
    {
        let db = Database::open(&db_path, config.clone()).unwrap();
        let wrong_key = b"wrong_master_key_32_bytes_long!!";

        // This might fail or succeed depending on implementation
        // If it succeeds, the decrypted data should be garbage
        match db.initialize_encryption(wrong_key) {
            Ok(_) => {
                // If initialization succeeds, reads should fail or return None
                let result = db.get(b"secret_key").unwrap();
                assert!(result.is_none() || result.as_deref() != Some(b"secret_value".as_ref()));
            }
            Err(_) => {
                // Expected - wrong key should fail
            }
        }
    }

    // Open with correct key - should work
    {
        let db = Database::open(&db_path, config).unwrap();
        let correct_key = b"correct_master_key_32_bytes_long";
        db.initialize_encryption(correct_key).unwrap();

        // Should be able to read data correctly
        assert_eq!(
            db.get(b"secret_key").unwrap().as_deref(),
            Some(b"secret_value".as_ref())
        );
        assert_eq!(
            db.get(b"password").unwrap().as_deref(),
            Some(b"my_secure_password".as_ref())
        );
    }
}

/// Test encryption with different algorithms
#[test]
fn test_encryption_algorithms() {
    // Test AES-256-GCM
    {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LightningDbConfig::default();
        config.encryption_config = EncryptionConfig {
            enabled: true,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            ..Default::default()
        };

        let db = Database::create(temp_dir.path(), config).unwrap();
        db.initialize_encryption(b"aes_gcm_master_key_32_bytes_ok!!")
            .unwrap();

        db.put(b"aes_key", b"aes_value").unwrap();
        assert_eq!(
            db.get(b"aes_key").unwrap().as_deref(),
            Some(b"aes_value".as_ref())
        );
    }

    // Test ChaCha20-Poly1305
    {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LightningDbConfig::default();
        config.encryption_config = EncryptionConfig {
            enabled: true,
            algorithm: EncryptionAlgorithm::ChaCha20Poly1305,
            ..Default::default()
        };

        let db = Database::create(temp_dir.path(), config).unwrap();
        db.initialize_encryption(b"chacha20_master_key_32_bytes_ok!")
            .unwrap();

        db.put(b"chacha_key", b"chacha_value").unwrap();
        assert_eq!(
            db.get(b"chacha_key").unwrap().as_deref(),
            Some(b"chacha_value".as_ref())
        );
    }
}

/// Test large data encryption
#[test]
fn test_encryption_large_data() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = LightningDbConfig::default();
    config.encryption_config = EncryptionConfig {
        enabled: true,
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        ..Default::default()
    };

    let db = Database::create(temp_dir.path(), config).unwrap();
    db.initialize_encryption(b"large_data_master_key_32_bytes!!")
        .unwrap();

    // Test with various data sizes
    let sizes = vec![
        1024,        // 1KB
        16 * 1024,   // 16KB
        256 * 1024,  // 256KB
        1024 * 1024, // 1MB
    ];

    for (i, size) in sizes.iter().enumerate() {
        let key = format!("large_key_{}", i);
        let value = vec![i as u8; *size];

        db.put(key.as_bytes(), &value).unwrap();

        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(retrieved.len(), *size);
        assert_eq!(&retrieved[0..10], &value[0..10]);
        assert_eq!(&retrieved[size - 10..], &value[size - 10..]);
    }
}

/// Test encryption statistics
#[test]
fn test_encryption_stats() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = LightningDbConfig::default();
    config.encryption_config = EncryptionConfig {
        enabled: true,
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        ..Default::default()
    };

    let db = Database::create(temp_dir.path(), config).unwrap();
    db.initialize_encryption(b"stats_test_master_key_32_bytes!!")
        .unwrap();

    // Get initial stats
    let initial_stats = db.get_encryption_stats().unwrap();
    assert!(initial_stats.enabled);
    assert_eq!(initial_stats.algorithm, EncryptionAlgorithm::Aes256Gcm);

    // Perform some operations
    for i in 0..10 {
        db.put(
            format!("key_{}", i).as_bytes(),
            format!("value_{}", i).as_bytes(),
        )
        .unwrap();
    }

    // Check stats updated
    let stats = db.get_encryption_stats().unwrap();
    assert!(stats.pages_encrypted > 0);

    // Read data to increase decryption count
    for i in 0..10 {
        db.get(format!("key_{}", i).as_bytes()).unwrap();
    }
}

/// Test that encryption can be disabled
#[test]
fn test_encryption_disabled() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = LightningDbConfig::default();
    config.encryption_config = EncryptionConfig {
        enabled: false,
        ..Default::default()
    };

    let db = Database::create(temp_dir.path(), config).unwrap();

    // Should not need to initialize encryption
    db.put(b"plain_key", b"plain_value").unwrap();
    assert_eq!(
        db.get(b"plain_key").unwrap().as_deref(),
        Some(b"plain_value".as_ref())
    );

    // Trying to initialize encryption should fail gracefully
    match db.initialize_encryption(b"unused_key_since_encryption_off!") {
        Ok(_) => (),  // OK if it's a no-op
        Err(_) => (), // Also OK if it returns error
    }
}
