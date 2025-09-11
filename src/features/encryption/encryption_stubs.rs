use crate::core::error::Result;
use zeroize::ZeroizeOnDrop;
use serde::{Deserialize, Serialize};
use bincode::{Decode, Encode};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct HSMKeyHandle {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct HSMEncryptResult {
    pub encrypted_data: Vec<u8>,
    pub key_handle: HSMKeyHandle,
    pub ciphertext: Vec<u8>,
}

use async_trait::async_trait;

#[async_trait]
pub trait HSMProvider: Send + Sync {
    async fn encrypt(&self, data: &[u8]) -> Result<HSMEncryptResult>;
    async fn decrypt(&self, encrypted: &[u8], key_handle: &HSMKeyHandle) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encode, Decode)]
pub enum SecureAlgorithm {
    AES256GCM,
    ChaCha20Poly1305,
    Aes256Gcm,
}

#[derive(Debug, Clone, ZeroizeOnDrop)]
pub struct SecureKey {
    #[zeroize(skip)]
    pub algorithm: SecureAlgorithm,
    pub key_material: Vec<u8>,
    pub material: Vec<u8>,
    pub id: String,
    #[zeroize(skip)]
    pub created_at: std::time::SystemTime,
}

impl SecureKey {
    pub fn random(algorithm: SecureAlgorithm) -> Self {
        let key_size = match algorithm {
            SecureAlgorithm::AES256GCM | SecureAlgorithm::Aes256Gcm => 32,
            SecureAlgorithm::ChaCha20Poly1305 => 32,
        };
        let mut key_material = vec![0u8; key_size];
        use rand_core::{OsRng, RngCore};
        OsRng.fill_bytes(&mut key_material);
        Self {
            algorithm,
            key_material: key_material.clone(),
            material: key_material,
            id: uuid::Uuid::new_v4().to_string(),
            created_at: std::time::SystemTime::now(),
        }
    }
    
    pub fn from_material(algorithm: SecureAlgorithm, key_material: Vec<u8>) -> Self {
        Self {
            algorithm,
            key_material: key_material.clone(),
            material: key_material,
            id: uuid::Uuid::new_v4().to_string(),
            created_at: std::time::SystemTime::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SecureEncryptionResult {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub tag: Vec<u8>,
}

#[derive(Debug)]
pub struct SecureCryptoProvider {
    algorithm: SecureAlgorithm,
}

impl SecureCryptoProvider {
    pub fn new(algorithm: SecureAlgorithm) -> Self {
        Self { algorithm }
    }

    pub fn encrypt(&self, key: &SecureKey, plaintext: &[u8], associated_data: &[u8], nonce: Option<&[u8]>) -> Result<SecureEncryptionResult> {
        let _ = (key, associated_data, nonce);
        Ok(SecureEncryptionResult {
            ciphertext: plaintext.to_vec(),
            nonce: vec![0; 12],
            tag: vec![0; 16],
        })
    }
    
    pub fn encrypt_with_nonce(&self, key: &SecureKey, plaintext: &[u8]) -> Result<SecureEncryptionResult> {
        self.encrypt(key, plaintext, &[], None)
    }

    pub fn constant_time_compare(&self, a: &[u8], b: &[u8]) -> bool {
        use constant_time_eq::constant_time_eq;
        constant_time_eq(a, b)
    }

    pub fn decrypt(&self, key: &SecureKey, result: &SecureEncryptionResult, associated_data: &[u8], nonce: Option<&[u8]>) -> Result<Vec<u8>> {
        let _ = (key, associated_data, nonce);
        Ok(result.ciphertext.clone())
    }
    
    pub fn decrypt_simple(&self, key: &SecureKey, result: &SecureEncryptionResult) -> Result<Vec<u8>> {
        self.decrypt(key, result, &[], None)
    }

    pub fn generate_key(&self) -> Result<SecureKey> {
        Ok(SecureKey::random(self.algorithm))
    }
    
    pub fn generate_key_with_algorithm(&self, algorithm: SecureAlgorithm) -> Result<SecureKey> {
        Ok(SecureKey::random(algorithm))
    }
}