//! Encryption at Rest Module for Lightning DB
//!
//! This module provides comprehensive encryption capabilities for data at rest,
//! including key management, key rotation, and transparent encryption/decryption
//! of database pages and WAL entries.
//!
//! Features:
//! - AES-256-GCM encryption for data pages
//! - Argon2id for key derivation
//! - Hardware acceleration support (AES-NI)
//! - Key rotation with versioning
//! - Encrypted WAL support
//! - Zero-copy encryption where possible

use crate::core::error::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use zeroize::Zeroize;

pub mod key_manager;
pub mod key_rotation;
pub mod page_encryptor;
pub mod wal_encryptor;

/// Configuration for encryption
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// Enable encryption at rest
    pub enabled: bool,
    /// Encryption algorithm (currently only AES-256-GCM)
    pub algorithm: EncryptionAlgorithm,
    /// Key derivation function
    pub kdf: KeyDerivationFunction,
    /// Key rotation interval in days
    pub key_rotation_interval_days: u32,
    /// Enable hardware acceleration
    pub hardware_acceleration: bool,
    /// WAL encryption enabled
    pub encrypt_wal: bool,
    /// Page encryption enabled
    pub encrypt_pages: bool,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            kdf: KeyDerivationFunction::Argon2id,
            key_rotation_interval_days: 90,
            hardware_acceleration: true,
            encrypt_wal: true,
            encrypt_pages: true,
        }
    }
}

/// Supported encryption algorithms
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, bincode::Encode, bincode::Decode,
)]
pub enum EncryptionAlgorithm {
    /// AES-256 in GCM mode (authenticated encryption)
    Aes256Gcm,
    /// ChaCha20-Poly1305 (for platforms without AES-NI)
    ChaCha20Poly1305,
}

/// Key derivation functions
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, bincode::Encode, bincode::Decode,
)]
pub enum KeyDerivationFunction {
    /// Argon2id - memory-hard, resistant to side-channel attacks
    Argon2id,
    /// PBKDF2 with SHA-256 (legacy support)
    Pbkdf2Sha256,
}

/// Encryption key with metadata
#[derive(Clone, Debug)]
pub struct EncryptionKey {
    /// Key ID for versioning
    pub key_id: u64,
    /// Raw key material (will be zeroed on drop)
    pub key_material: Vec<u8>,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Expiration timestamp (if any)
    pub expires_at: Option<SystemTime>,
    /// Key version
    pub version: u32,
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        self.key_material.zeroize();
    }
}

/// Encryption metadata stored with encrypted data
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct EncryptionMetadata {
    /// Key ID used for encryption
    pub key_id: u64,
    /// Nonce/IV used
    pub nonce: Vec<u8>,
    /// Algorithm used
    pub algorithm: EncryptionAlgorithm,
    /// Additional authenticated data (AAD)
    pub aad: Option<Vec<u8>>,
    /// Timestamp of encryption
    pub encrypted_at: u64,
}

/// Main encryption manager
#[derive(Debug)]
pub struct EncryptionManager {
    config: EncryptionConfig,
    key_manager: Arc<key_manager::KeyManager>,
    page_encryptor: Arc<page_encryptor::PageEncryptor>,
    wal_encryptor: Arc<wal_encryptor::WalEncryptor>,
    pub rotation_manager: Arc<key_rotation::RotationManager>,
}

impl EncryptionManager {
    /// Create a new encryption manager
    pub fn new(config: EncryptionConfig) -> Result<Self> {
        if !config.enabled {
            return Ok(Self {
                config,
                key_manager: Arc::new(key_manager::KeyManager::disabled()),
                page_encryptor: Arc::new(page_encryptor::PageEncryptor::disabled()),
                wal_encryptor: Arc::new(wal_encryptor::WalEncryptor::disabled()),
                rotation_manager: Arc::new(key_rotation::RotationManager::disabled()),
            });
        }

        let key_manager = Arc::new(key_manager::KeyManager::new(config.clone())?);
        let page_encryptor = Arc::new(page_encryptor::PageEncryptor::new(
            key_manager.clone(),
            config.algorithm,
        )?);
        let wal_encryptor = Arc::new(wal_encryptor::WalEncryptor::new(
            key_manager.clone(),
            config.algorithm,
        )?);
        let rotation_manager = Arc::new(key_rotation::RotationManager::new(
            key_manager.clone(),
            config.key_rotation_interval_days,
        )?);

        Ok(Self {
            config,
            key_manager,
            page_encryptor,
            wal_encryptor,
            rotation_manager,
        })
    }

    /// Initialize encryption with a master key
    pub fn initialize(&self, master_key: &[u8]) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        self.key_manager.initialize_master_key(master_key)?;

        // Generate initial data encryption key
        self.key_manager.generate_data_key()?;

        Ok(())
    }

    /// Encrypt a data page
    pub fn encrypt_page(&self, page_id: u64, data: &[u8]) -> Result<Vec<u8>> {
        if !self.config.enabled || !self.config.encrypt_pages {
            return Ok(data.to_vec());
        }

        self.page_encryptor.encrypt_page(page_id, data)
    }

    /// Decrypt a data page
    pub fn decrypt_page(&self, page_id: u64, encrypted_data: &[u8]) -> Result<Vec<u8>> {
        if !self.config.enabled || !self.config.encrypt_pages {
            return Ok(encrypted_data.to_vec());
        }

        self.page_encryptor.decrypt_page(page_id, encrypted_data)
    }

    /// Encrypt a WAL entry
    pub fn encrypt_wal_entry(&self, entry_id: u64, data: &[u8]) -> Result<Vec<u8>> {
        if !self.config.enabled || !self.config.encrypt_wal {
            return Ok(data.to_vec());
        }

        self.wal_encryptor.encrypt_entry(entry_id, data)
    }

    /// Decrypt a WAL entry
    pub fn decrypt_wal_entry(&self, entry_id: u64, encrypted_data: &[u8]) -> Result<Vec<u8>> {
        if !self.config.enabled || !self.config.encrypt_wal {
            return Ok(encrypted_data.to_vec());
        }

        self.wal_encryptor.decrypt_entry(entry_id, encrypted_data)
    }

    /// Check if key rotation is needed
    pub fn needs_rotation(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }

        self.rotation_manager.needs_rotation()
    }

    /// Perform key rotation
    pub fn rotate_keys(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        self.rotation_manager.rotate_keys()
    }

    /// Get encryption statistics
    pub fn get_stats(&self) -> EncryptionStats {
        EncryptionStats {
            enabled: self.config.enabled,
            algorithm: self.config.algorithm,
            pages_encrypted: self.page_encryptor.get_encrypted_count(),
            wal_entries_encrypted: self.wal_encryptor.get_encrypted_count(),
            current_key_id: self.key_manager.get_current_key_id().unwrap_or(0),
            last_rotation: self.rotation_manager.get_last_rotation_time(),
            next_rotation: self.rotation_manager.get_next_rotation_time(),
        }
    }
}

/// Encryption statistics
#[derive(Debug, Serialize)]
pub struct EncryptionStats {
    pub enabled: bool,
    pub algorithm: EncryptionAlgorithm,
    pub pages_encrypted: u64,
    pub wal_entries_encrypted: u64,
    pub current_key_id: u64,
    pub last_rotation: Option<SystemTime>,
    pub next_rotation: Option<SystemTime>,
}

/// Generate a secure random nonce using cryptographically secure OsRng
pub fn generate_nonce(size: usize) -> Vec<u8> {
    use rand_core::{OsRng, RngCore};
    let mut nonce = vec![0u8; size];
    OsRng.fill_bytes(&mut nonce);
    nonce
}

/// Constant-time comparison for cryptographic data
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (byte_a, byte_b) in a.iter().zip(b.iter()) {
        result |= byte_a ^ byte_b;
    }

    result == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_config_default() {
        let config = EncryptionConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.algorithm, EncryptionAlgorithm::Aes256Gcm);
        assert_eq!(config.key_rotation_interval_days, 90);
    }

    #[test]
    fn test_generate_nonce() {
        let nonce1 = generate_nonce(12);
        let nonce2 = generate_nonce(12);

        assert_eq!(nonce1.len(), 12);
        assert_eq!(nonce2.len(), 12);
        assert_ne!(nonce1, nonce2); // Should be different
    }

    #[test]
    fn test_constant_time_eq() {
        let a = b"hello world";
        let b = b"hello world";
        let c = b"hello worlD";

        assert!(constant_time_eq(a, b));
        assert!(!constant_time_eq(a, c));
        assert!(!constant_time_eq(&a[..5], a)); // Different lengths
    }
}
