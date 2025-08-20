use crate::security::{SecurityError, SecurityResult};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use aes_gcm::aead::{Aead, KeyInit};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::password_hash::{rand_core::OsRng, SaltString};
use blake3::{Hash, Hasher};
use chacha20poly1305::{ChaCha20Poly1305, KeyInit as ChaChaKeyInit};
use ring::rand::{SecureRandom, SystemRandom};
use secrecy::{ExposeSecret, Secret, Zeroize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zeroize::ZeroizeOnDrop;

const KEY_SIZE: usize = 32;
const NONCE_SIZE: usize = 12;
const SALT_SIZE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct KeyId(pub String);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub enum EncryptionAlgorithm {
    Aes256Gcm,
    ChaCha20Poly1305,
}

#[derive(Serialize, Deserialize)]
pub struct EncryptionKey {
    pub id: KeyId,
    #[serde(skip, default = "default_key_material")]
    pub key_material: Secret<Vec<u8>>,
    pub algorithm: EncryptionAlgorithm,
    pub created_at: SystemTime,
    pub expires_at: Option<SystemTime>,
    pub active: bool,
    pub version: u32,
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionKey")
            .field("id", &self.id)
            .field("key_material", &"[REDACTED]")
            .field("algorithm", &self.algorithm)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .field("active", &self.active)
            .field("version", &self.version)
            .finish()
    }
}

fn default_key_material() -> Secret<Vec<u8>> {
    Secret::new(Vec::new())
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        // The Secret<Vec<u8>> will automatically zeroize the key material
        // Other fields don't need explicit zeroization
    }
}

#[derive(Debug, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct EncryptedData {
    pub key_id: KeyId,
    pub algorithm: EncryptionAlgorithm,
    #[serde(with = "serde_bytes")]
    pub nonce: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub ciphertext: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub tag: Vec<u8>,
}

pub struct CryptographicManager {
    keys: Arc<RwLock<HashMap<KeyId, EncryptionKey>>>,
    active_key: Arc<RwLock<Option<KeyId>>>,
    rng: SystemRandom,
    key_rotation_interval: Duration,
    max_key_age: Duration,
}

impl CryptographicManager {
    pub fn new(key_rotation_interval: Duration) -> SecurityResult<Self> {
        let mut manager = Self {
            keys: Arc::new(RwLock::new(HashMap::new())),
            active_key: Arc::new(RwLock::new(None)),
            rng: SystemRandom::new(),
            key_rotation_interval,
            max_key_age: key_rotation_interval * 2,
        };

        manager.generate_initial_key()?;
        Ok(manager)
    }

    pub fn generate_key(&self, algorithm: EncryptionAlgorithm) -> SecurityResult<KeyId> {
        let mut key_bytes = vec![0u8; KEY_SIZE];
        self.rng.fill(&mut key_bytes)
            .map_err(|e| SecurityError::CryptographicFailure(format!("Key generation failed: {:?}", e)))?;

        let key_id = KeyId(self.generate_key_id()?);
        let key = EncryptionKey {
            id: key_id.clone(),
            key_material: Secret::new(key_bytes),
            algorithm,
            created_at: SystemTime::now(),
            expires_at: Some(SystemTime::now() + self.max_key_age),
            active: true,
            version: 1,
        };

        let mut keys = self.keys.write().unwrap();
        keys.insert(key_id.clone(), key);
        
        let mut active_key = self.active_key.write().unwrap();
        *active_key = Some(key_id.clone());

        Ok(key_id)
    }

    pub fn encrypt(&self, data: &[u8], key_id: Option<&KeyId>) -> SecurityResult<EncryptedData> {
        let key_id = match key_id {
            Some(id) => id.clone(),
            None => self.get_active_key_id()?,
        };

        let keys = self.keys.read().unwrap();
        let key = keys.get(&key_id)
            .ok_or_else(|| SecurityError::CryptographicFailure("Encryption key not found".to_string()))?;

        if !key.active {
            return Err(SecurityError::CryptographicFailure("Encryption key is inactive".to_string()));
        }

        let mut nonce = vec![0u8; NONCE_SIZE];
        self.rng.fill(&mut nonce)
            .map_err(|e| SecurityError::CryptographicFailure(format!("Nonce generation failed: {:?}", e)))?;

        match key.algorithm {
            EncryptionAlgorithm::Aes256Gcm => self.encrypt_aes_gcm(data, key, &nonce),
            EncryptionAlgorithm::ChaCha20Poly1305 => self.encrypt_chacha20(data, key, &nonce),
        }
    }

    pub fn decrypt(&self, encrypted_data: &EncryptedData) -> SecurityResult<Vec<u8>> {
        let keys = self.keys.read().unwrap();
        let key = keys.get(&encrypted_data.key_id)
            .ok_or_else(|| SecurityError::CryptographicFailure("Decryption key not found".to_string()))?;

        match encrypted_data.algorithm {
            EncryptionAlgorithm::Aes256Gcm => self.decrypt_aes_gcm(encrypted_data, key),
            EncryptionAlgorithm::ChaCha20Poly1305 => self.decrypt_chacha20(encrypted_data, key),
        }
    }

    pub fn hash_password(&self, password: &str) -> SecurityResult<String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        
        argon2.hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|e| SecurityError::CryptographicFailure(format!("Password hashing failed: {}", e)))
    }

    pub fn verify_password(&self, password: &str, hash: &str) -> SecurityResult<bool> {
        let parsed_hash = PasswordHash::new(hash)
            .map_err(|e| SecurityError::CryptographicFailure(format!("Invalid password hash: {}", e)))?;

        let argon2 = Argon2::default();
        Ok(argon2.verify_password(password.as_bytes(), &parsed_hash).is_ok())
    }

    pub fn secure_hash(&self, data: &[u8]) -> Hash {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    pub fn secure_compare(&self, a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        
        let mut result = 0u8;
        for i in 0..a.len() {
            result |= a[i] ^ b[i];
        }
        result == 0
    }

    pub fn generate_secure_random(&self, size: usize) -> SecurityResult<Vec<u8>> {
        let mut bytes = vec![0u8; size];
        self.rng.fill(&mut bytes)
            .map_err(|e| SecurityError::CryptographicFailure(format!("Random generation failed: {:?}", e)))?;
        Ok(bytes)
    }

    pub fn rotate_keys(&self) -> SecurityResult<()> {
        let current_key_id = self.get_active_key_id()?;
        let new_key_id = self.generate_key(EncryptionAlgorithm::Aes256Gcm)?;
        
        {
            let mut keys = self.keys.write().unwrap();
            if let Some(old_key) = keys.get_mut(&current_key_id) {
                old_key.active = false;
            }
        }

        let mut active_key = self.active_key.write().unwrap();
        *active_key = Some(new_key_id);

        self.cleanup_expired_keys()?;
        Ok(())
    }

    pub fn cleanup_expired_keys(&self) -> SecurityResult<()> {
        let mut keys = self.keys.write().unwrap();
        let now = SystemTime::now();
        
        keys.retain(|_, key| {
            if let Some(expires_at) = key.expires_at {
                expires_at > now
            } else {
                true
            }
        });
        
        Ok(())
    }

    pub fn get_key_info(&self, key_id: &KeyId) -> Option<(EncryptionAlgorithm, SystemTime, bool)> {
        let keys = self.keys.read().unwrap();
        keys.get(key_id).map(|key| (key.algorithm, key.created_at, key.active))
    }

    pub fn list_keys(&self) -> Vec<(KeyId, EncryptionAlgorithm, SystemTime, bool)> {
        let keys = self.keys.read().unwrap();
        keys.values()
            .map(|key| (key.id.clone(), key.algorithm, key.created_at, key.active))
            .collect()
    }

    fn generate_initial_key(&mut self) -> SecurityResult<()> {
        self.generate_key(EncryptionAlgorithm::Aes256Gcm)?;
        Ok(())
    }

    fn get_active_key_id(&self) -> SecurityResult<KeyId> {
        let active_key = self.active_key.read().unwrap();
        active_key.as_ref()
            .cloned()
            .ok_or_else(|| SecurityError::CryptographicFailure("No active encryption key".to_string()))
    }

    fn generate_key_id(&self) -> SecurityResult<String> {
        let mut bytes = [0u8; 16];
        self.rng.fill(&mut bytes)
            .map_err(|e| SecurityError::CryptographicFailure(format!("Key ID generation failed: {:?}", e)))?;
        Ok(hex::encode(bytes))
    }

    fn encrypt_aes_gcm(&self, data: &[u8], key: &EncryptionKey, nonce: &[u8]) -> SecurityResult<EncryptedData> {
        let key_bytes = key.key_material.expose_secret();
        let cipher_key = Key::<Aes256Gcm>::from_slice(key_bytes);
        let cipher = Aes256Gcm::new(cipher_key);
        let nonce_array = Nonce::from_slice(nonce);

        let ciphertext = cipher.encrypt(nonce_array, data)
            .map_err(|e| SecurityError::CryptographicFailure(format!("AES-GCM encryption failed: {}", e)))?;

        Ok(EncryptedData {
            key_id: key.id.clone(),
            algorithm: key.algorithm,
            nonce: nonce.to_vec(),
            ciphertext,
            tag: Vec::new(),
        })
    }

    fn decrypt_aes_gcm(&self, encrypted_data: &EncryptedData, key: &EncryptionKey) -> SecurityResult<Vec<u8>> {
        let key_bytes = key.key_material.expose_secret();
        let cipher_key = Key::<Aes256Gcm>::from_slice(key_bytes);
        let cipher = Aes256Gcm::new(cipher_key);
        let nonce_array = Nonce::from_slice(&encrypted_data.nonce);

        cipher.decrypt(nonce_array, encrypted_data.ciphertext.as_ref())
            .map_err(|e| SecurityError::CryptographicFailure(format!("AES-GCM decryption failed: {}", e)))
    }

    fn encrypt_chacha20(&self, data: &[u8], key: &EncryptionKey, nonce: &[u8]) -> SecurityResult<EncryptedData> {
        let key_bytes = key.key_material.expose_secret();
        let cipher = ChaCha20Poly1305::new_from_slice(key_bytes)
            .map_err(|e| SecurityError::CryptographicFailure(format!("ChaCha20 key creation failed: {}", e)))?;

        let nonce_array = chacha20poly1305::Nonce::from_slice(nonce);
        let ciphertext = cipher.encrypt(nonce_array, data)
            .map_err(|e| SecurityError::CryptographicFailure(format!("ChaCha20 encryption failed: {}", e)))?;

        Ok(EncryptedData {
            key_id: key.id.clone(),
            algorithm: key.algorithm,
            nonce: nonce.to_vec(),
            ciphertext,
            tag: Vec::new(),
        })
    }

    fn decrypt_chacha20(&self, encrypted_data: &EncryptedData, key: &EncryptionKey) -> SecurityResult<Vec<u8>> {
        let key_bytes = key.key_material.expose_secret();
        let cipher = ChaCha20Poly1305::new_from_slice(key_bytes)
            .map_err(|e| SecurityError::CryptographicFailure(format!("ChaCha20 key creation failed: {}", e)))?;

        let nonce_array = chacha20poly1305::Nonce::from_slice(&encrypted_data.nonce);
        cipher.decrypt(nonce_array, encrypted_data.ciphertext.as_ref())
            .map_err(|e| SecurityError::CryptographicFailure(format!("ChaCha20 decryption failed: {}", e)))
    }
}

pub struct SecureMemory {
    data: Secret<Vec<u8>>,
}

impl SecureMemory {
    pub fn new(size: usize) -> Self {
        Self {
            data: Secret::new(vec![0u8; size]),
        }
    }

    pub fn from_slice(data: &[u8]) -> Self {
        Self {
            data: Secret::new(data.to_vec()),
        }
    }

    pub fn expose(&self) -> &[u8] {
        self.data.expose_secret()
    }

    pub fn len(&self) -> usize {
        self.data.expose_secret().len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.expose_secret().is_empty()
    }
}

impl Drop for SecureMemory {
    fn drop(&mut self) {
        // SecureMemory automatically zeroizes when dropped due to ZeroizeOnDrop trait
        // No explicit zeroization needed here
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_generation() {
        let crypto = CryptographicManager::new(Duration::from_secs(3600)).unwrap();
        let key_id = crypto.generate_key(EncryptionAlgorithm::Aes256Gcm).unwrap();
        
        let keys = crypto.list_keys();
        assert!(keys.iter().any(|(id, _, _, active)| id == &key_id && *active));
    }

    #[test]
    fn test_encryption_decryption() {
        let crypto = CryptographicManager::new(Duration::from_secs(3600)).unwrap();
        let data = b"Hello, World!";
        
        let encrypted = crypto.encrypt(data, None).unwrap();
        let decrypted = crypto.decrypt(&encrypted).unwrap();
        
        assert_eq!(data.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn test_password_hashing() {
        let crypto = CryptographicManager::new(Duration::from_secs(3600)).unwrap();
        let password = "test_password_123";
        
        let hash = crypto.hash_password(password).unwrap();
        assert!(crypto.verify_password(password, &hash).unwrap());
        assert!(!crypto.verify_password("wrong_password", &hash).unwrap());
    }

    #[test]
    fn test_secure_compare() {
        let crypto = CryptographicManager::new(Duration::from_secs(3600)).unwrap();
        
        assert!(crypto.secure_compare(b"test", b"test"));
        assert!(!crypto.secure_compare(b"test", b"Test"));
        assert!(!crypto.secure_compare(b"test", b"testing"));
    }

    #[test]
    fn test_secure_random() {
        let crypto = CryptographicManager::new(Duration::from_secs(3600)).unwrap();
        
        let random1 = crypto.generate_secure_random(32).unwrap();
        let random2 = crypto.generate_secure_random(32).unwrap();
        
        assert_eq!(random1.len(), 32);
        assert_eq!(random2.len(), 32);
        assert_ne!(random1, random2);
    }
}