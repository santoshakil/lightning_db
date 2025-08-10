//! Example demonstrating encryption at rest with Lightning DB
//!
//! This example shows how to:
//! - Enable encryption for your database
//! - Initialize encryption with a master key
//! - Perform encrypted database operations
//! - Check encryption statistics
//! - Handle key rotation

use lightning_db::{
    Database, LightningDbConfig,
    encryption::{EncryptionConfig, EncryptionAlgorithm, KeyDerivationFunction},
};
use std::time::Instant;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logging
    env_logger::init();
    
    println!("Lightning DB Encryption Example");
    println!("==============================\n");
    
    // Create a temporary directory for the database
    let temp_dir = TempDir::new()?;
    println!("Database path: {:?}\n", temp_dir.path());
    
    // Configure encryption settings
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
    
    println!("Encryption Configuration:");
    println!("  Algorithm: AES-256-GCM");
    println!("  Key Derivation: Argon2id");
    println!("  Hardware Acceleration: Enabled");
    println!("  Encrypt WAL: Yes");
    println!("  Encrypt Pages: Yes\n");
    
    // Create the encrypted database
    let db = Database::create(temp_dir.path(), config)?;
    
    // Initialize encryption with a master key
    // In production, this key should be securely stored (e.g., in a key management service)
    let master_key = b"my_secure_32_byte_master_key_123";
    println!("Initializing encryption with master key...");
    let start = Instant::now();
    db.initialize_encryption(master_key)?;
    println!("Encryption initialized in {:?}\n", start.elapsed());
    
    // Perform some encrypted operations
    println!("Performing encrypted database operations...");
    
    // Insert some sensitive data
    let sensitive_data = vec![
        ("user:1:password", "super_secret_password"),
        ("user:1:ssn", "123-45-6789"),
        ("user:1:credit_card", "4111-1111-1111-1111"),
        ("api:key:production", "sk_live_abcdef123456"),
        ("config:database:password", "db_password_2024"),
    ];
    
    let start = Instant::now();
    for (key, value) in &sensitive_data {
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    println!("  Inserted {} sensitive records in {:?}", sensitive_data.len(), start.elapsed());
    
    // Read back the data to verify encryption/decryption works
    let start = Instant::now();
    for (key, expected_value) in &sensitive_data {
        let value = db.get(key.as_bytes())?;
        assert_eq!(value.as_deref(), Some(expected_value.as_bytes()));
    }
    println!("  Verified {} records in {:?}\n", sensitive_data.len(), start.elapsed());
    
    // Demonstrate transactions with encryption
    println!("Testing encrypted transactions...");
    let tx_start = Instant::now();
    let tx_id = db.begin_transaction()?;
    
    // Add some transactional data
    db.put_tx(tx_id, b"tx:user:2:name", b"John Doe")?;
    db.put_tx(tx_id, b"tx:user:2:email", b"john@example.com")?;
    db.put_tx(tx_id, b"tx:user:2:phone", b"+1-555-0123")?;
    
    db.commit_transaction(tx_id)?;
    println!("  Transaction committed in {:?}\n", tx_start.elapsed());
    
    // Get encryption statistics
    let stats = db.get_encryption_stats()
        .ok_or("Encryption stats not available")?;
    println!("Encryption Statistics:");
    println!("  Enabled: {}", stats.enabled);
    println!("  Algorithm: {:?}", stats.algorithm);
    println!("  Pages Encrypted: {}", stats.pages_encrypted);
    println!("  WAL Entries Encrypted: {}", stats.wal_entries_encrypted);
    println!("  Current Key ID: {}", stats.current_key_id);
    if let Some(last_rotation) = stats.last_rotation {
        println!("  Last Key Rotation: {:?}", last_rotation);
    }
    if let Some(next_rotation) = stats.next_rotation {
        println!("  Next Key Rotation: {:?}", next_rotation);
    }
    println!();
    
    // Demonstrate key rotation check
    println!("Checking if key rotation is needed...");
    if db.needs_key_rotation()? {
        println!("  Key rotation is due. Rotating keys...");
        let rotation_start = Instant::now();
        db.rotate_encryption_keys()?;
        println!("  Key rotation completed in {:?}", rotation_start.elapsed());
    } else {
        println!("  Key rotation is not needed yet.");
    }
    println!();
    
    // Demonstrate working with different encryption algorithms
    println!("Testing ChaCha20-Poly1305 encryption...");
    let chacha_dir = TempDir::new()?;
    let mut chacha_config = LightningDbConfig::default();
    chacha_config.encryption_config = EncryptionConfig {
        enabled: true,
        algorithm: EncryptionAlgorithm::ChaCha20Poly1305,
        ..Default::default()
    };
    
    let chacha_db = Database::create(chacha_dir.path(), chacha_config)?;
    chacha_db.initialize_encryption(b"chacha20_master_key_32_bytes_ok!")?;
    
    let start = Instant::now();
    chacha_db.put(b"chacha:test", b"encrypted_with_chacha20")?;
    let value = chacha_db.get(b"chacha:test")?;
    assert_eq!(value.as_deref(), Some(b"encrypted_with_chacha20".as_ref()));
    println!("  ChaCha20-Poly1305 encryption working correctly");
    println!("  Operation completed in {:?}\n", start.elapsed());
    
    // Performance comparison
    println!("Performance Comparison (1000 operations):");
    
    // Test unencrypted performance
    let unencrypted_dir = TempDir::new()?;
    let mut unencrypted_config = LightningDbConfig::default();
    unencrypted_config.encryption_config.enabled = false;
    let unencrypted_db = Database::create(unencrypted_dir.path(), unencrypted_config)?;
    
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("perf:key:{}", i);
        let value = format!("value_{}", i);
        unencrypted_db.put(key.as_bytes(), value.as_bytes())?;
    }
    let unencrypted_time = start.elapsed();
    println!("  Unencrypted: {:?} ({:.0} ops/sec)", 
        unencrypted_time, 
        1000.0 / unencrypted_time.as_secs_f64()
    );
    
    // Test encrypted performance
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("perf:key:{}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    let encrypted_time = start.elapsed();
    println!("  Encrypted (AES-256-GCM): {:?} ({:.0} ops/sec)", 
        encrypted_time,
        1000.0 / encrypted_time.as_secs_f64()
    );
    
    let overhead = ((encrypted_time.as_secs_f64() / unencrypted_time.as_secs_f64()) - 1.0) * 100.0;
    println!("  Encryption overhead: {:.1}%\n", overhead);
    
    // Security notes
    println!("Security Best Practices:");
    println!("  ✓ Never hardcode master keys in production");
    println!("  ✓ Use a secure key management service (AWS KMS, HashiCorp Vault, etc.)");
    println!("  ✓ Rotate keys regularly (every 90 days recommended)");
    println!("  ✓ Enable hardware acceleration when available");
    println!("  ✓ Monitor encryption statistics and performance");
    println!("  ✓ Test disaster recovery with encrypted backups");
    println!("  ✓ Use strong random keys (32 bytes for AES-256)");
    
    println!("\nEncryption example completed successfully!");
    
    Ok(())
}