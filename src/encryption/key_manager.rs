//! Key Management System for Lightning DB Encryption
//!
//! Handles secure key generation, storage, and lifecycle management.
//! Supports hierarchical key derivation and secure key storage.

use super::{EncryptionConfig, EncryptionKey, KeyDerivationFunction};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tracing::{debug, info};
use zeroize::Zeroize;

/// Key types in the hierarchy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeyType {
    /// Master key - root of key hierarchy
    Master,
    /// Key encryption key - encrypts data keys
    KeyEncryption,
    /// Data encryption key - encrypts actual data
    DataEncryption,
}

/// Key manager for handling encryption keys
#[derive(Debug)]
pub struct KeyManager {
    config: EncryptionConfig,
    /// Master key (kept in memory, zeroized on drop)
    master_key: Arc<RwLock<Option<Vec<u8>>>>,
    /// Key encryption keys
    kek_cache: Arc<RwLock<HashMap<u64, EncryptionKey>>>,
    /// Data encryption keys
    dek_cache: Arc<RwLock<HashMap<u64, EncryptionKey>>>,
    /// Current key IDs
    current_kek_id: Arc<RwLock<Option<u64>>>,
    current_dek_id: Arc<RwLock<Option<u64>>>,
    /// Key store path
    key_store_path: Option<PathBuf>,
    /// Next key ID
    next_key_id: Arc<RwLock<u64>>,
}

/// Persisted key metadata
#[derive(Debug, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
struct KeyMetadata {
    pub key_id: u64,
    pub key_type: String,
    pub created_at: u64,
    pub expires_at: Option<u64>,
    pub version: u32,
    pub encrypted_key: Vec<u8>,
    pub salt: Vec<u8>,
    pub nonce: Vec<u8>,
}

/// Key store for persisting encrypted keys
#[derive(Debug, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
struct KeyStore {
    pub version: u32,
    pub keys: Vec<KeyMetadata>,
    pub current_kek_id: Option<u64>,
    pub current_dek_id: Option<u64>,
    pub next_key_id: u64,
}

impl KeyManager {
    /// Create a new key manager
    pub fn new(config: EncryptionConfig) -> Result<Self> {
        Ok(Self {
            config,
            master_key: Arc::new(RwLock::new(None)),
            kek_cache: Arc::new(RwLock::new(HashMap::new())),
            dek_cache: Arc::new(RwLock::new(HashMap::new())),
            current_kek_id: Arc::new(RwLock::new(None)),
            current_dek_id: Arc::new(RwLock::new(None)),
            key_store_path: None,
            next_key_id: Arc::new(RwLock::new(1)),
        })
    }

    /// Create a disabled key manager (for when encryption is off)
    pub fn disabled() -> Self {
        Self::new(EncryptionConfig {
            enabled: false,
            ..Default::default()
        })
        .unwrap()
    }

    /// Initialize with a master key
    pub fn initialize_master_key(&self, master_key: &[u8]) -> Result<()> {
        if master_key.len() != 32 {
            return Err(Error::Encryption("Master key must be 256 bits".to_string()));
        }

        let mut master_key_guard = self.master_key.write().unwrap();
        let mut key = master_key.to_vec();
        *master_key_guard = Some(key.clone());
        key.zeroize(); // Clear the local copy

        // Generate initial KEK
        self.generate_kek()?;

        info!("Master key initialized");
        Ok(())
    }

    /// Set the key store path for persistence
    pub fn set_key_store_path(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref().to_path_buf();

        // Load existing key store if it exists
        if path.exists() {
            self.load_key_store(&path)?;
        }

        Ok(())
    }

    /// Generate a new key encryption key
    pub fn generate_kek(&self) -> Result<u64> {
        let master_key = self.master_key.read().unwrap();
        let master_key = master_key
            .as_ref()
            .ok_or_else(|| Error::Encryption("Master key not initialized".to_string()))?;

        let key_id = self.get_next_key_id();
        let key_material =
            self.derive_key(master_key, &format!("kek_{}", key_id).as_bytes(), 32)?;

        let key = EncryptionKey {
            key_id,
            key_material,
            created_at: SystemTime::now(),
            expires_at: None,
            version: 1,
        };

        // Store in cache
        self.kek_cache.write().unwrap().insert(key_id, key);
        *self.current_kek_id.write().unwrap() = Some(key_id);

        // Persist if key store is configured
        if self.key_store_path.is_some() {
            self.save_key_store()?;
        }

        debug!("Generated new KEK with ID: {}", key_id);
        Ok(key_id)
    }

    /// Generate a new data encryption key
    pub fn generate_data_key(&self) -> Result<u64> {
        let current_kek_id = self
            .current_kek_id
            .read()
            .unwrap()
            .ok_or_else(|| Error::Encryption("No current KEK".to_string()))?;

        let kek = self
            .kek_cache
            .read()
            .unwrap()
            .get(&current_kek_id)
            .ok_or_else(|| Error::Encryption("KEK not found".to_string()))?
            .clone();

        let key_id = self.get_next_key_id();
        let key_material =
            self.derive_key(&kek.key_material, &format!("dek_{}", key_id).as_bytes(), 32)?;

        let key = EncryptionKey {
            key_id,
            key_material,
            created_at: SystemTime::now(),
            expires_at: None,
            version: 1,
        };

        // Store in cache
        self.dek_cache.write().unwrap().insert(key_id, key);
        *self.current_dek_id.write().unwrap() = Some(key_id);

        // Persist if key store is configured
        if self.key_store_path.is_some() {
            self.save_key_store()?;
        }

        debug!("Generated new DEK with ID: {}", key_id);
        Ok(key_id)
    }

    /// Get the current data encryption key
    pub fn get_current_dek(&self) -> Result<EncryptionKey> {
        let current_dek_id = self
            .current_dek_id
            .read()
            .unwrap()
            .ok_or_else(|| Error::Encryption("No current DEK".to_string()))?;

        self.dek_cache
            .read()
            .unwrap()
            .get(&current_dek_id)
            .cloned()
            .ok_or_else(|| Error::Encryption("Current DEK not found".to_string()))
    }

    /// Get a specific data encryption key by ID
    pub fn get_dek(&self, key_id: u64) -> Result<EncryptionKey> {
        self.dek_cache
            .read()
            .unwrap()
            .get(&key_id)
            .cloned()
            .ok_or_else(|| Error::Encryption(format!("DEK {} not found", key_id)))
    }

    /// Get current key ID
    pub fn get_current_key_id(&self) -> Option<u64> {
        *self.current_dek_id.read().unwrap()
    }

    /// Derive a key using the configured KDF
    fn derive_key(&self, master_key: &[u8], context: &[u8], key_length: usize) -> Result<Vec<u8>> {
        match self.config.kdf {
            KeyDerivationFunction::Argon2id => {
                self.derive_key_argon2(master_key, context, key_length)
            }
            KeyDerivationFunction::Pbkdf2Sha256 => {
                self.derive_key_pbkdf2(master_key, context, key_length)
            }
        }
    }

    /// Derive key using Argon2id
    fn derive_key_argon2(
        &self,
        master_key: &[u8],
        salt: &[u8],
        key_length: usize,
    ) -> Result<Vec<u8>> {
        use argon2::{Algorithm, Argon2, Params, Version};

        // Configure Argon2id parameters
        let params = Params::new(
            64 * 1024, // 64 MB memory
            3,         // 3 iterations
            4,         // 4 parallel threads
            Some(key_length),
        )
        .map_err(|e| Error::Encryption(format!("Invalid Argon2 params: {}", e)))?;

        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

        let mut output = vec![0u8; key_length];
        argon2
            .hash_password_into(master_key, salt, &mut output)
            .map_err(|e| Error::Encryption(format!("Argon2 derivation failed: {}", e)))?;

        Ok(output)
    }

    /// Derive key using PBKDF2-SHA256
    fn derive_key_pbkdf2(
        &self,
        master_key: &[u8],
        salt: &[u8],
        key_length: usize,
    ) -> Result<Vec<u8>> {
        use pbkdf2::pbkdf2_hmac;
        use sha2::Sha256;

        let mut output = vec![0u8; key_length];
        pbkdf2_hmac::<Sha256>(master_key, salt, 100_000, &mut output);
        Ok(output)
    }

    /// Get next key ID
    fn get_next_key_id(&self) -> u64 {
        let mut next_id = self.next_key_id.write().unwrap();
        let id = *next_id;
        *next_id += 1;
        id
    }

    /// Load key store from disk
    fn load_key_store(&self, path: &Path) -> Result<()> {
        let mut file = File::open(path).map_err(|e| Error::Io(e.to_string()))?;

        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .map_err(|e| Error::Io(e.to_string()))?;

        let store: KeyStore = bincode::decode_from_slice(&contents, bincode::config::standard())
            .map_err(|e| Error::Encryption(format!("Failed to deserialize key store: {}", e)))?
            .0;

        // Restore state
        *self.current_kek_id.write().unwrap() = store.current_kek_id;
        *self.current_dek_id.write().unwrap() = store.current_dek_id;
        *self.next_key_id.write().unwrap() = store.next_key_id;

        // Note: Encrypted keys would need to be decrypted here
        // This is a simplified version - real implementation would decrypt keys

        info!("Loaded key store with {} keys", store.keys.len());
        Ok(())
    }

    /// Save key store to disk
    fn save_key_store(&self) -> Result<()> {
        let path = self
            .key_store_path
            .as_ref()
            .ok_or_else(|| Error::Encryption("Key store path not set".to_string()))?;

        let store = KeyStore {
            version: 1,
            keys: Vec::new(), // Simplified - would include encrypted keys
            current_kek_id: *self.current_kek_id.read().unwrap(),
            current_dek_id: *self.current_dek_id.read().unwrap(),
            next_key_id: *self.next_key_id.read().unwrap(),
        };

        let contents = bincode::encode_to_vec(&store, bincode::config::standard())
            .map_err(|e| Error::Encryption(format!("Failed to serialize key store: {}", e)))?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| Error::Io(e.to_string()))?;

        file.write_all(&contents)
            .map_err(|e| Error::Io(e.to_string()))?;

        file.sync_all().map_err(|e| Error::Io(e.to_string()))?;

        Ok(())
    }

    /// Rotate all keys
    pub fn rotate_keys(&self) -> Result<()> {
        // Generate new KEK
        let new_kek_id = self.generate_kek()?;

        // Generate new DEK encrypted with new KEK
        let new_dek_id = self.generate_data_key()?;

        info!(
            "Rotated keys: KEK {} -> {}, DEK {} -> {}",
            self.current_kek_id.read().unwrap().unwrap_or(0),
            new_kek_id,
            self.current_dek_id.read().unwrap().unwrap_or(0),
            new_dek_id,
        );

        Ok(())
    }

    /// Clear all sensitive data from memory
    pub fn clear_keys(&self) {
        let mut master_key = self.master_key.write().unwrap();
        if let Some(ref mut key) = *master_key {
            key.zeroize();
        }
        *master_key = None;

        let mut kek_cache = self.kek_cache.write().unwrap();
        for (_, mut key) in kek_cache.drain() {
            key.key_material.zeroize();
        }

        let mut dek_cache = self.dek_cache.write().unwrap();
        for (_, mut key) in dek_cache.drain() {
            key.key_material.zeroize();
        }
    }
}

impl Drop for KeyManager {
    fn drop(&mut self) {
        self.clear_keys();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_manager_creation() {
        let config = EncryptionConfig::default();
        let manager = KeyManager::new(config).unwrap();
        assert!(manager.get_current_key_id().is_none());
    }

    #[test]
    fn test_master_key_initialization() {
        let config = EncryptionConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = KeyManager::new(config).unwrap();

        let master_key = vec![0u8; 32];
        assert!(manager.initialize_master_key(&master_key).is_ok());

        // Wrong size key should fail
        let wrong_key = vec![0u8; 16];
        assert!(manager.initialize_master_key(&wrong_key).is_err());
    }
}
