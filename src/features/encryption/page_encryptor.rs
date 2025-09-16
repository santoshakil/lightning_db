//! Page-level Encryption for Lightning DB
//!
//! Provides transparent encryption and decryption of database pages
//! using authenticated encryption (AES-GCM or ChaCha20-Poly1305).

use super::key_manager::KeyManager;
use super::{constant_time_eq, generate_nonce, EncryptionAlgorithm, EncryptionMetadata};
use crate::core::error::{Error, Result};
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Key, Nonce,
};
use chacha20poly1305::ChaCha20Poly1305;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::trace;

/// Page encryptor for encrypting database pages
#[derive(Debug)]
pub struct PageEncryptor {
    key_manager: Arc<KeyManager>,
    algorithm: EncryptionAlgorithm,
    /// Count of pages encrypted
    pages_encrypted: Arc<AtomicU64>,
    /// Count of pages decrypted
    pages_decrypted: Arc<AtomicU64>,
}

impl PageEncryptor {
    /// Create a new page encryptor
    pub fn new(key_manager: Arc<KeyManager>, algorithm: EncryptionAlgorithm) -> Result<Self> {
        Ok(Self {
            key_manager,
            algorithm,
            pages_encrypted: Arc::new(AtomicU64::new(0)),
            pages_decrypted: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Create a disabled page encryptor
    pub fn disabled() -> Self {
        Self {
            key_manager: Arc::new(KeyManager::disabled()),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            pages_encrypted: Arc::new(AtomicU64::new(0)),
            pages_decrypted: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Encrypt a database page
    pub fn encrypt_page(&self, page_id: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        let key = self.key_manager.get_current_dek()?;

        // Generate nonce - combine page_id with random bytes for uniqueness
        let nonce = self.generate_page_nonce(page_id);

        // Create AAD (Additional Authenticated Data) from page_id
        let aad = page_id.to_le_bytes().to_vec();

        // Encrypt based on algorithm
        let ciphertext = match self.algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                self.encrypt_aes_gcm(&key.key_material, &nonce, plaintext, &aad)?
            }
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                self.encrypt_chacha20(&key.key_material, &nonce, plaintext, &aad)?
            }
        };

        // Create metadata
        let metadata = EncryptionMetadata {
            key_id: key.key_id,
            nonce: nonce.to_vec(),
            algorithm: self.algorithm,
            aad: Some(aad),
            encrypted_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::Generic(format!("System time error during encryption: {}", e)))?
                .as_secs(),
        };

        // Serialize encrypted page
        let encrypted_page = self.serialize_encrypted_page(metadata, ciphertext)?;

        self.pages_encrypted.fetch_add(1, Ordering::Relaxed);
        trace!("Encrypted page {} with key {}", page_id, key.key_id);

        Ok(encrypted_page)
    }

    /// Decrypt a database page
    pub fn decrypt_page(&self, page_id: u64, encrypted_data: &[u8]) -> Result<Vec<u8>> {
        // Deserialize encrypted page
        let (metadata, ciphertext) = self.deserialize_encrypted_page(encrypted_data)?;

        // Verify AAD matches page_id
        if let Some(ref aad) = metadata.aad {
            let expected_aad = page_id.to_le_bytes();
            if !constant_time_eq(aad, &expected_aad) {
                return Err(Error::Encryption("Page ID mismatch in AAD".to_string()));
            }
        }

        // Get the key used for encryption
        let key = self.key_manager.get_dek(metadata.key_id)?;

        // Decrypt based on algorithm
        let plaintext = match metadata.algorithm {
            EncryptionAlgorithm::Aes256Gcm => self.decrypt_aes_gcm(
                &key.key_material,
                &metadata.nonce,
                &ciphertext,
                metadata.aad.as_deref().unwrap_or(&[]),
            )?,
            EncryptionAlgorithm::ChaCha20Poly1305 => self.decrypt_chacha20(
                &key.key_material,
                &metadata.nonce,
                &ciphertext,
                metadata.aad.as_deref().unwrap_or(&[]),
            )?,
        };

        self.pages_decrypted.fetch_add(1, Ordering::Relaxed);
        trace!("Decrypted page {} with key {}", page_id, metadata.key_id);

        Ok(plaintext)
    }

    /// Encrypt using AES-256-GCM
    fn encrypt_aes_gcm(
        &self,
        key: &[u8],
        nonce: &[u8],
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        let key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce);

        cipher
            .encrypt(
                nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|e| Error::Encryption(format!("AES-GCM encryption failed: {}", e)))
    }

    /// Decrypt using AES-256-GCM
    fn decrypt_aes_gcm(
        &self,
        key: &[u8],
        nonce: &[u8],
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        let key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce);

        cipher
            .decrypt(
                nonce,
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|e| Error::Encryption(format!("AES-GCM decryption failed: {}", e)))
    }

    /// Encrypt using ChaCha20-Poly1305
    fn encrypt_chacha20(
        &self,
        key: &[u8],
        nonce: &[u8],
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        use chacha20poly1305::aead::{Aead, KeyInit};

        let key = chacha20poly1305::Key::from_slice(key);
        let cipher = ChaCha20Poly1305::new(key);
        let nonce = chacha20poly1305::Nonce::from_slice(nonce);

        cipher
            .encrypt(
                nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|e| Error::Encryption(format!("ChaCha20 encryption failed: {}", e)))
    }

    /// Decrypt using ChaCha20-Poly1305
    fn decrypt_chacha20(
        &self,
        key: &[u8],
        nonce: &[u8],
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        use chacha20poly1305::aead::{Aead, KeyInit};

        let key = chacha20poly1305::Key::from_slice(key);
        let cipher = ChaCha20Poly1305::new(key);
        let nonce = chacha20poly1305::Nonce::from_slice(nonce);

        cipher
            .decrypt(
                nonce,
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|e| Error::Encryption(format!("ChaCha20 decryption failed: {}", e)))
    }

    /// Generate a unique nonce for a page
    fn generate_page_nonce(&self, page_id: u64) -> Vec<u8> {
        let mut nonce = vec![0u8; 12]; // 96-bit nonce for both AES-GCM and ChaCha20

        // First 8 bytes: page_id
        nonce[..8].copy_from_slice(&page_id.to_le_bytes());

        // Last 4 bytes: random
        let random_bytes = generate_nonce(4);
        nonce[8..].copy_from_slice(&random_bytes);

        nonce
    }

    /// Serialize encrypted page format
    fn serialize_encrypted_page(
        &self,
        metadata: EncryptionMetadata,
        ciphertext: Vec<u8>,
    ) -> Result<Vec<u8>> {
        // Format: [metadata_len:u32][metadata][ciphertext]
        let metadata_bytes = bincode::encode_to_vec(&metadata, bincode::config::standard())
            .map_err(|e| Error::Encryption(format!("Failed to serialize metadata: {}", e)))?;

        let metadata_len = metadata_bytes.len() as u32;
        let total_len = 4 + metadata_bytes.len() + ciphertext.len();

        let mut result = Vec::with_capacity(total_len);
        result.extend_from_slice(&metadata_len.to_le_bytes());
        result.extend_from_slice(&metadata_bytes);
        result.extend_from_slice(&ciphertext);

        Ok(result)
    }

    /// Deserialize encrypted page format
    fn deserialize_encrypted_page(&self, data: &[u8]) -> Result<(EncryptionMetadata, Vec<u8>)> {
        if data.len() < 4 {
            return Err(Error::Encryption(
                "Invalid encrypted page format".to_string(),
            ));
        }

        // Read metadata length
        let metadata_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

        if data.len() < 4 + metadata_len {
            return Err(Error::Encryption("Truncated encrypted page".to_string()));
        }

        // Deserialize metadata
        let (metadata, _): (EncryptionMetadata, _) =
            bincode::decode_from_slice(&data[4..4 + metadata_len], bincode::config::standard())
                .map_err(|e| Error::Encryption(format!("Failed to deserialize metadata: {}", e)))?;

        // Extract ciphertext
        let ciphertext = data[4 + metadata_len..].to_vec();

        Ok((metadata, ciphertext))
    }

    /// Get count of pages encrypted
    pub fn get_encrypted_count(&self) -> u64 {
        self.pages_encrypted.load(Ordering::Relaxed)
    }

    /// Get count of pages decrypted
    pub fn get_decrypted_count(&self) -> u64 {
        self.pages_decrypted.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_encryptor() -> (PageEncryptor, Arc<KeyManager>) {
        let config = super::super::EncryptionConfig {
            enabled: true,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            ..Default::default()
        };

        let key_manager =
            Arc::new(KeyManager::new(config.clone()).expect("Failed to create key manager"));
        let master_key = vec![0x42; 32];
        key_manager
            .initialize_master_key(&master_key)
            .expect("Failed to initialize master key");
        key_manager
            .generate_data_key()
            .expect("Failed to generate data key");

        let encryptor = PageEncryptor::new(key_manager.clone(), config.algorithm)
            .expect("Failed to create page encryptor");
        (encryptor, key_manager)
    }

    #[test]
    #[ignore] // TODO: Fix hanging issue
    fn test_page_encryption_decryption() {
        let (encryptor, _) = create_test_encryptor();

        let page_id = 42;
        let plaintext = b"This is a test database page with some data";

        // Encrypt
        let encrypted = encryptor
            .encrypt_page(page_id, plaintext)
            .expect("Failed to encrypt page");
        assert_ne!(encrypted, plaintext);

        // Decrypt
        let decrypted = encryptor
            .decrypt_page(page_id, &encrypted)
            .expect("Failed to decrypt page");
        assert_eq!(decrypted, plaintext);

        // Verify counters
        assert_eq!(encryptor.get_encrypted_count(), 1);
        assert_eq!(encryptor.get_decrypted_count(), 1);
    }

    #[test]
    #[ignore] // TODO: Fix hanging issue
    fn test_different_pages_different_ciphertexts() {
        let (encryptor, _) = create_test_encryptor();

        let plaintext = b"Same data on different pages";

        let encrypted1 = encryptor
            .encrypt_page(1, plaintext)
            .expect("Failed to encrypt page 1");
        let encrypted2 = encryptor
            .encrypt_page(2, plaintext)
            .expect("Failed to encrypt page 2");

        // Same plaintext should produce different ciphertexts for different pages
        assert_ne!(encrypted1, encrypted2);

        // Both should decrypt correctly
        assert_eq!(
            encryptor
                .decrypt_page(1, &encrypted1)
                .expect("Failed to decrypt page 1"),
            plaintext
        );
        assert_eq!(
            encryptor
                .decrypt_page(2, &encrypted2)
                .expect("Failed to decrypt page 2"),
            plaintext
        );
    }

    #[test]
    fn test_tampered_ciphertext_fails() {
        let (encryptor, _) = create_test_encryptor();

        let page_id = 123;
        let plaintext = b"Sensitive data";

        let mut encrypted = encryptor
            .encrypt_page(page_id, plaintext)
            .expect("Failed to encrypt page for tampering test");

        // Tamper with the ciphertext
        let last_byte = encrypted.len() - 1;
        encrypted[last_byte] ^= 0xFF;

        // Decryption should fail due to authentication
        assert!(encryptor.decrypt_page(page_id, &encrypted).is_err());
    }

    #[test]
    fn test_wrong_page_id_fails() {
        let (encryptor, _) = create_test_encryptor();

        let plaintext = b"Page data";
        let encrypted = encryptor
            .encrypt_page(100, plaintext)
            .expect("Failed to encrypt page for AAD test");

        // Try to decrypt with wrong page ID (AAD mismatch)
        assert!(encryptor.decrypt_page(200, &encrypted).is_err());
    }
}
