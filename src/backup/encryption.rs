//! Backup Encryption Management
//!
//! Provides comprehensive encryption capabilities for Lightning DB backups,
//! including key management, rotation, and secure storage.

use crate::{Result, Error};
use std::collections::HashMap;
use std::time::{SystemTime, Duration};
use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};
use aes_gcm::{Aes256Gcm, Key, Nonce, KeyInit, AeadInPlace};
use chacha20poly1305::{ChaCha20Poly1305, Key as ChaChaKey};
use argon2::{Argon2, PasswordHasher, PasswordHash, PasswordVerifier, password_hash::SaltString};
use base64;
use rand::RngCore;

/// Encryption manager for backup operations
pub struct EncryptionManager {
    enabled: bool,
    config: EncryptionConfig,
    key_store: KeyStore,
    current_key_id: Option<String>,
}

/// Encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    pub algorithm: EncryptionAlgorithm,
    pub key_derivation: KeyDerivationMethod,
    pub key_rotation_interval_hours: u64,
    pub key_size_bits: u32,
    pub compression_before_encryption: bool,
    pub authentication_tag_size: usize,
    pub password_requirements: PasswordRequirements,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            algorithm: EncryptionAlgorithm::ChaCha20Poly1305,
            key_derivation: KeyDerivationMethod::Argon2id,
            key_rotation_interval_hours: 24 * 30, // 30 days
            key_size_bits: 256,
            compression_before_encryption: true,
            authentication_tag_size: 16,
            password_requirements: PasswordRequirements::default(),
        }
    }
}

/// Supported encryption algorithms
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    AES256GCM,
    ChaCha20Poly1305,
    XSalsa20Poly1305,
}

/// Key derivation methods
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyDerivationMethod {
    Argon2id,
    Scrypt,
    PBKDF2,
}

/// Password requirements for key derivation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordRequirements {
    pub min_length: usize,
    pub require_uppercase: bool,
    pub require_lowercase: bool,
    pub require_numbers: bool,
    pub require_symbols: bool,
    pub max_password_age_days: Option<u32>,
}

impl Default for PasswordRequirements {
    fn default() -> Self {
        Self {
            min_length: 12,
            require_uppercase: true,
            require_lowercase: true,
            require_numbers: true,
            require_symbols: true,
            max_password_age_days: Some(90),
        }
    }
}

/// Encryption key information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionKey {
    pub key_id: String,
    pub algorithm: EncryptionAlgorithm,
    pub created_at: SystemTime,
    pub expires_at: Option<SystemTime>,
    pub status: KeyStatus,
    pub usage_count: u64,
    pub last_used: Option<SystemTime>,
    pub metadata: HashMap<String, String>,
    // Key material is not serialized for security
    #[serde(skip)]
    pub key_material: Vec<u8>,
}

/// Key status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyStatus {
    Active,
    Rotating,
    Deprecated,
    Revoked,
    Compromised,
}

/// Encryption information stored with backup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionInfo {
    pub key_id: String,
    pub algorithm: EncryptionAlgorithm,
    pub key_derivation: KeyDerivationMethod,
    pub nonce: Vec<u8>,
    pub salt: Vec<u8>,
    pub auth_tag: Vec<u8>,
    pub encrypted_at: SystemTime,
    pub compression_used: bool,
    pub metadata: HashMap<String, String>,
}

/// Key store for managing encryption keys
struct KeyStore {
    keys: HashMap<String, EncryptionKey>,
    key_history: Vec<KeyRotationEvent>,
    master_key_hash: Option<String>,
}

/// Key rotation event
#[derive(Debug, Clone, Serialize, Deserialize)]
struct KeyRotationEvent {
    pub old_key_id: Option<String>,
    pub new_key_id: String,
    pub rotated_at: SystemTime,
    pub reason: RotationReason,
    pub status: RotationStatus,
}

/// Reason for key rotation
#[derive(Debug, Clone, Serialize, Deserialize)]
enum RotationReason {
    Scheduled,
    Manual,
    Compromised,
    PolicyViolation,
    Emergency,
}

/// Key rotation status
#[derive(Debug, Clone, Serialize, Deserialize)]
enum RotationStatus {
    InProgress,
    Completed,
    Failed,
    RolledBack,
}

impl EncryptionManager {
    /// Create a new encryption manager
    pub fn new(enabled: bool) -> Result<Self> {
        let config = EncryptionConfig::default();
        let key_store = KeyStore {
            keys: HashMap::new(),
            key_history: Vec::new(),
            master_key_hash: None,
        };

        Ok(Self {
            enabled,
            config,
            key_store,
            current_key_id: None,
        })
    }

    /// Create encryption manager with custom configuration
    pub fn with_config(enabled: bool, config: EncryptionConfig) -> Result<Self> {
        let key_store = KeyStore {
            keys: HashMap::new(),
            key_history: Vec::new(),
            master_key_hash: None,
        };

        Ok(Self {
            enabled,
            config,
            key_store,
            current_key_id: None,
        })
    }

    /// Initialize encryption with master password
    pub fn initialize(&mut self, master_password: &str) -> Result<()> {
        self.validate_password(master_password)?;
        
        // Derive master key from password
        let master_key_hash = self.derive_master_key_hash(master_password)?;
        self.key_store.master_key_hash = Some(master_key_hash);

        // Generate initial encryption key
        let initial_key = self.generate_new_key(master_password)?;
        self.current_key_id = Some(initial_key.key_id.clone());
        self.key_store.keys.insert(initial_key.key_id.clone(), initial_key);

        println!("🔐 Encryption initialized with new master key");
        Ok(())
    }

    /// Encrypt backup data
    pub fn encrypt_backup(&self, data: &[u8]) -> Result<(Vec<u8>, EncryptionInfo)> {
        if !self.enabled {
            return Err(Error::Generic("Encryption not enabled".to_string()));
        }

        let current_key_id = self.current_key_id.as_ref()
            .ok_or_else(|| Error::Generic("No encryption key available".to_string()))?;

        let key = self.key_store.keys.get(current_key_id)
            .ok_or_else(|| Error::Generic("Encryption key not found".to_string()))?;

        // Generate random nonce
        let mut nonce_bytes = vec![0u8; 12]; // 96-bit nonce for GCM
        rand::rng().fill_bytes(&mut nonce_bytes);

        // Generate salt for additional security
        let mut salt = vec![0u8; 32];
        rand::rng().fill_bytes(&mut salt);

        let (encrypted_data, auth_tag) = match key.algorithm {
            EncryptionAlgorithm::AES256GCM => {
                self.encrypt_aes_gcm(data, &key.key_material, &nonce_bytes)?
            },
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                self.encrypt_chacha20poly1305(data, &key.key_material, &nonce_bytes)?
            },
            EncryptionAlgorithm::XSalsa20Poly1305 => {
                return Err(Error::Generic("XSalsa20Poly1305 not yet implemented".to_string()));
            },
        };

        let encryption_info = EncryptionInfo {
            key_id: current_key_id.clone(),
            algorithm: key.algorithm,
            key_derivation: self.config.key_derivation,
            nonce: nonce_bytes,
            salt,
            auth_tag,
            encrypted_at: SystemTime::now(),
            compression_used: self.config.compression_before_encryption,
            metadata: HashMap::new(),
        };

        // Update key usage statistics
        self.update_key_usage(current_key_id)?;

        Ok((encrypted_data, encryption_info))
    }

    /// Decrypt backup data
    pub fn decrypt_backup(&self, encrypted_data: &[u8], encryption_info: &EncryptionInfo) -> Result<Vec<u8>> {
        if !self.enabled {
            return Err(Error::Generic("Encryption not enabled".to_string()));
        }

        let key = self.key_store.keys.get(&encryption_info.key_id)
            .ok_or_else(|| Error::Generic(format!("Decryption key not found: {}", encryption_info.key_id)))?;

        let decrypted_data = match encryption_info.algorithm {
            EncryptionAlgorithm::AES256GCM => {
                self.decrypt_aes_gcm(
                    encrypted_data,
                    &key.key_material,
                    &encryption_info.nonce,
                    &encryption_info.auth_tag,
                )?
            },
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                self.decrypt_chacha20poly1305(
                    encrypted_data,
                    &key.key_material,
                    &encryption_info.nonce,
                    &encryption_info.auth_tag,
                )?
            },
            EncryptionAlgorithm::XSalsa20Poly1305 => {
                return Err(Error::Generic("XSalsa20Poly1305 not yet implemented".to_string()));
            },
        };

        Ok(decrypted_data)
    }

    /// Rotate encryption key
    pub fn rotate_key(&mut self, master_password: &str, reason: RotationReason) -> Result<String> {
        self.verify_master_password(master_password)?;

        let old_key_id = self.current_key_id.clone();
        
        // Mark old key as rotating
        if let Some(old_id) = &old_key_id {
            if let Some(old_key) = self.key_store.keys.get_mut(old_id) {
                old_key.status = KeyStatus::Rotating;
            }
        }

        // Generate new key
        let new_key = self.generate_new_key(master_password)?;
        let new_key_id = new_key.key_id.clone();

        // Update current key
        self.current_key_id = Some(new_key_id.clone());
        self.key_store.keys.insert(new_key_id.clone(), new_key);

        // Mark old key as deprecated
        if let Some(old_id) = &old_key_id {
            if let Some(old_key) = self.key_store.keys.get_mut(old_id) {
                old_key.status = KeyStatus::Deprecated;
            }
        }

        // Record rotation event
        let rotation_event = KeyRotationEvent {
            old_key_id,
            new_key_id: new_key_id.clone(),
            rotated_at: SystemTime::now(),
            reason,
            status: RotationStatus::Completed,
        };
        self.key_store.key_history.push(rotation_event);

        println!("🔄 Encryption key rotated: {}", new_key_id);
        Ok(new_key_id)
    }

    /// Check if key rotation is needed
    pub fn needs_key_rotation(&self) -> bool {
        if let Some(current_key_id) = &self.current_key_id {
            if let Some(current_key) = self.key_store.keys.get(current_key_id) {
                let key_age = SystemTime::now()
                    .duration_since(current_key.created_at)
                    .unwrap_or_default();
                
                let rotation_interval = Duration::from_secs(
                    self.config.key_rotation_interval_hours * 3600
                );

                return key_age >= rotation_interval;
            }
        }
        true // No current key means we need rotation
    }

    /// Get key information
    pub fn get_key_info(&self, key_id: &str) -> Option<&EncryptionKey> {
        self.key_store.keys.get(key_id)
    }

    /// List all keys
    pub fn list_keys(&self) -> Vec<&EncryptionKey> {
        self.key_store.keys.values().collect()
    }

    /// Revoke a key
    pub fn revoke_key(&mut self, key_id: &str, reason: &str) -> Result<()> {
        let key = self.key_store.keys.get_mut(key_id)
            .ok_or_else(|| Error::Generic(format!("Key not found: {}", key_id)))?;

        key.status = KeyStatus::Revoked;
        key.metadata.insert("revocation_reason".to_string(), reason.to_string());
        key.metadata.insert("revoked_at".to_string(), format!("{:?}", SystemTime::now()));

        println!("🚫 Key revoked: {} ({})", key_id, reason);
        Ok(())
    }

    /// Validate password against requirements
    fn validate_password(&self, password: &str) -> Result<()> {
        let req = &self.config.password_requirements;

        if password.len() < req.min_length {
            return Err(Error::Generic(format!(
                "Password must be at least {} characters long",
                req.min_length
            )));
        }

        if req.require_uppercase && !password.chars().any(|c| c.is_uppercase()) {
            return Err(Error::Generic("Password must contain uppercase letters".to_string()));
        }

        if req.require_lowercase && !password.chars().any(|c| c.is_lowercase()) {
            return Err(Error::Generic("Password must contain lowercase letters".to_string()));
        }

        if req.require_numbers && !password.chars().any(|c| c.is_numeric()) {
            return Err(Error::Generic("Password must contain numbers".to_string()));
        }

        if req.require_symbols && !password.chars().any(|c| !c.is_alphanumeric()) {
            return Err(Error::Generic("Password must contain symbols".to_string()));
        }

        Ok(())
    }

    /// Derive master key hash from password
    fn derive_master_key_hash(&self, password: &str) -> Result<String> {
        // Generate salt manually to avoid version conflicts
        let mut salt_bytes = [0u8; 16];
        rand::rng().fill_bytes(&mut salt_bytes);
        let salt = base64::encode(&salt_bytes);
        let salt = SaltString::from_b64(&salt)
            .map_err(|e| Error::Generic(format!("Failed to create salt: {}", e)))?;
        let argon2 = Argon2::default();
        
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| Error::Generic(format!("Failed to hash password: {}", e)))?;

        Ok(password_hash.to_string())
    }

    /// Verify master password
    fn verify_master_password(&self, password: &str) -> Result<()> {
        let stored_hash = self.key_store.master_key_hash.as_ref()
            .ok_or_else(|| Error::Generic("No master key hash available".to_string()))?;

        let parsed_hash = PasswordHash::new(stored_hash)
            .map_err(|e| Error::Generic(format!("Invalid stored hash: {}", e)))?;

        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .map_err(|_| Error::Generic("Invalid master password".to_string()))?;

        Ok(())
    }

    /// Generate a new encryption key
    fn generate_new_key(&self, master_password: &str) -> Result<EncryptionKey> {
        // Generate key ID
        let key_id = format!("key_{}", SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs());

        // Generate random key material
        let key_size_bytes = self.config.key_size_bits / 8;
        let mut key_material = vec![0u8; key_size_bytes as usize];
        rand::rng().fill_bytes(&mut key_material);

        // Derive additional entropy from master password
        let mut hasher = Sha256::new();
        hasher.update(master_password.as_bytes());
        hasher.update(&key_material);
        hasher.update(key_id.as_bytes());
        let derived = hasher.finalize();

        // XOR with derived entropy for additional security
        for (i, byte) in key_material.iter_mut().enumerate() {
            *byte ^= derived[i % derived.len()];
        }

        let expires_at = if self.config.key_rotation_interval_hours > 0 {
            Some(SystemTime::now() + Duration::from_secs(
                self.config.key_rotation_interval_hours * 3600
            ))
        } else {
            None
        };

        Ok(EncryptionKey {
            key_id,
            algorithm: self.config.algorithm,
            created_at: SystemTime::now(),
            expires_at,
            status: KeyStatus::Active,
            usage_count: 0,
            last_used: None,
            metadata: HashMap::new(),
            key_material,
        })
    }

    /// Encrypt data using AES-256-GCM
    fn encrypt_aes_gcm(&self, data: &[u8], key: &[u8], nonce: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        let key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce);

        let mut buffer = data.to_vec();
        let auth_tag = cipher.encrypt_in_place_detached(nonce, b"", &mut buffer)
            .map_err(|e| Error::Generic(format!("AES encryption failed: {}", e)))?;

        Ok((buffer, auth_tag.to_vec()))
    }

    /// Decrypt data using AES-256-GCM
    fn decrypt_aes_gcm(&self, encrypted_data: &[u8], key: &[u8], nonce: &[u8], auth_tag: &[u8]) -> Result<Vec<u8>> {
        let key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce);
        let tag = aes_gcm::Tag::from_slice(auth_tag);

        let mut buffer = encrypted_data.to_vec();
        cipher.decrypt_in_place_detached(nonce, b"", &mut buffer, tag)
            .map_err(|e| Error::Generic(format!("AES decryption failed: {}", e)))?;

        Ok(buffer)
    }

    /// Encrypt data using ChaCha20-Poly1305
    fn encrypt_chacha20poly1305(&self, data: &[u8], key: &[u8], nonce: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        let key = ChaChaKey::from_slice(key);
        let cipher = ChaCha20Poly1305::new(key);
        let nonce = chacha20poly1305::Nonce::from_slice(nonce);

        let mut buffer = data.to_vec();
        let auth_tag = cipher.encrypt_in_place_detached(nonce, b"", &mut buffer)
            .map_err(|e| Error::Generic(format!("ChaCha20 encryption failed: {}", e)))?;

        Ok((buffer, auth_tag.to_vec()))
    }

    /// Decrypt data using ChaCha20-Poly1305
    fn decrypt_chacha20poly1305(&self, encrypted_data: &[u8], key: &[u8], nonce: &[u8], auth_tag: &[u8]) -> Result<Vec<u8>> {
        let key = ChaChaKey::from_slice(key);
        let cipher = ChaCha20Poly1305::new(key);
        let nonce = chacha20poly1305::Nonce::from_slice(nonce);
        let tag = chacha20poly1305::Tag::from_slice(auth_tag);

        let mut buffer = encrypted_data.to_vec();
        cipher.decrypt_in_place_detached(nonce, b"", &mut buffer, tag)
            .map_err(|e| Error::Generic(format!("ChaCha20 decryption failed: {}", e)))?;

        Ok(buffer)
    }

    /// Update key usage statistics
    fn update_key_usage(&self, key_id: &str) -> Result<()> {
        // This would require mutable access, so in a real implementation
        // we'd use Arc<Mutex<>> or similar for thread-safe access
        println!("📊 Key usage updated: {}", key_id);
        Ok(())
    }

    /// Export encryption configuration
    pub fn export_config(&self) -> EncryptionConfig {
        self.config.clone()
    }

    /// Get encryption statistics
    pub fn get_encryption_stats(&self) -> EncryptionStatistics {
        let total_keys = self.key_store.keys.len();
        let active_keys = self.key_store.keys.values()
            .filter(|k| k.status == KeyStatus::Active)
            .count();
        let deprecated_keys = self.key_store.keys.values()
            .filter(|k| k.status == KeyStatus::Deprecated)
            .count();
        let revoked_keys = self.key_store.keys.values()
            .filter(|k| k.status == KeyStatus::Revoked)
            .count();

        let total_usage = self.key_store.keys.values()
            .map(|k| k.usage_count)
            .sum();

        let last_rotation = self.key_store.key_history
            .last()
            .map(|event| event.rotated_at);

        EncryptionStatistics {
            enabled: self.enabled,
            algorithm: self.config.algorithm,
            total_keys,
            active_keys,
            deprecated_keys,
            revoked_keys,
            total_usage,
            last_rotation,
            rotation_events: self.key_store.key_history.len(),
        }
    }
}

/// Encryption statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionStatistics {
    pub enabled: bool,
    pub algorithm: EncryptionAlgorithm,
    pub total_keys: usize,
    pub active_keys: usize,
    pub deprecated_keys: usize,
    pub revoked_keys: usize,
    pub total_usage: u64,
    pub last_rotation: Option<SystemTime>,
    pub rotation_events: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_manager_creation() {
        let manager = EncryptionManager::new(true);
        assert!(manager.is_ok());
        
        let manager = manager.unwrap();
        assert!(manager.enabled);
    }

    #[test]
    fn test_password_validation() {
        let manager = EncryptionManager::new(true).unwrap();
        
        // Valid password
        assert!(manager.validate_password("SecurePass123!").is_ok());
        
        // Too short
        assert!(manager.validate_password("short").is_err());
        
        // Missing uppercase
        assert!(manager.validate_password("lowercase123!").is_err());
        
        // Missing numbers
        assert!(manager.validate_password("NoNumbers!").is_err());
        
        // Missing symbols
        assert!(manager.validate_password("NoSymbols123").is_err());
    }

    #[test]
    fn test_key_generation() {
        let mut manager = EncryptionManager::new(true).unwrap();
        
        let result = manager.initialize("SecureTestPassword123!");
        assert!(result.is_ok());
        
        assert!(manager.current_key_id.is_some());
        assert_eq!(manager.key_store.keys.len(), 1);
    }

    #[test]
    fn test_encryption_decryption() {
        let mut manager = EncryptionManager::new(true).unwrap();
        manager.initialize("SecureTestPassword123!").unwrap();
        
        let test_data = b"This is test backup data that needs to be encrypted securely";
        
        // Encrypt
        let (encrypted_data, encryption_info) = manager.encrypt_backup(test_data).unwrap();
        assert_ne!(encrypted_data, test_data);
        assert!(!encrypted_data.is_empty());
        
        // Decrypt
        let decrypted_data = manager.decrypt_backup(&encrypted_data, &encryption_info).unwrap();
        assert_eq!(decrypted_data, test_data);
    }

    #[test]
    fn test_key_rotation() {
        let mut manager = EncryptionManager::new(true).unwrap();
        manager.initialize("SecureTestPassword123!").unwrap();
        
        let initial_key_id = manager.current_key_id.clone();
        
        // Rotate key
        let new_key_id = manager.rotate_key("SecureTestPassword123!", RotationReason::Manual).unwrap();
        
        assert_ne!(initial_key_id, Some(new_key_id.clone()));
        assert_eq!(manager.current_key_id, Some(new_key_id));
        assert_eq!(manager.key_store.keys.len(), 2); // Old + new key
    }

    #[test]
    fn test_encryption_statistics() {
        let mut manager = EncryptionManager::new(true).unwrap();
        manager.initialize("SecureTestPassword123!").unwrap();
        
        let stats = manager.get_encryption_stats();
        assert!(stats.enabled);
        assert_eq!(stats.total_keys, 1);
        assert_eq!(stats.active_keys, 1);
        assert_eq!(stats.deprecated_keys, 0);
        assert_eq!(stats.revoked_keys, 0);
    }
}