//! Write-Ahead Log (WAL) Encryption for Lightning DB
//!
//! Provides encryption for WAL entries to ensure durability of encrypted data
//! and protect against data leaks through WAL files.

use super::key_manager::KeyManager;
use super::{generate_nonce, EncryptionAlgorithm, EncryptionMetadata};
use crate::{Error, Result};
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

/// WAL encryptor for encrypting write-ahead log entries
#[derive(Debug)]
pub struct WalEncryptor {
    key_manager: Arc<KeyManager>,
    algorithm: EncryptionAlgorithm,
    /// Count of WAL entries encrypted
    entries_encrypted: Arc<AtomicU64>,
    /// Count of WAL entries decrypted
    entries_decrypted: Arc<AtomicU64>,
    /// Sequence number for nonce generation
    sequence_number: Arc<AtomicU64>,
}

/// WAL entry header with encryption info
#[derive(Debug, Clone)]
struct WalEntryHeader {
    /// Entry sequence number
    sequence: u64,
    /// Entry type (commit, data, checkpoint, etc.)
    entry_type: u8,
    /// Original entry size before encryption
    original_size: u32,
    /// Timestamp
    timestamp: u64,
}

impl WalEncryptor {
    /// Create a new WAL encryptor
    pub fn new(key_manager: Arc<KeyManager>, algorithm: EncryptionAlgorithm) -> Result<Self> {
        Ok(Self {
            key_manager,
            algorithm,
            entries_encrypted: Arc::new(AtomicU64::new(0)),
            entries_decrypted: Arc::new(AtomicU64::new(0)),
            sequence_number: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Create a disabled WAL encryptor
    pub fn disabled() -> Self {
        Self {
            key_manager: Arc::new(KeyManager::disabled()),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            entries_encrypted: Arc::new(AtomicU64::new(0)),
            entries_decrypted: Arc::new(AtomicU64::new(0)),
            sequence_number: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Encrypt a WAL entry
    pub fn encrypt_entry(&self, entry_id: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        let key = self.key_manager.get_current_dek()?;

        // Get next sequence number
        let sequence = self.sequence_number.fetch_add(1, Ordering::SeqCst);

        // Create WAL entry header
        let header = WalEntryHeader {
            sequence,
            entry_type: 0, // Would be determined from actual entry
            original_size: plaintext.len() as u32,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Generate nonce combining sequence and random bytes
        let nonce = self.generate_wal_nonce(sequence);

        // Create AAD from header
        let aad = self.serialize_header(&header)?;

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
            aad: Some(aad.clone()),
            encrypted_at: header.timestamp,
        };

        // Serialize encrypted entry
        let encrypted_entry = self.serialize_encrypted_entry(header, metadata, ciphertext)?;

        self.entries_encrypted.fetch_add(1, Ordering::Relaxed);
        trace!(
            "Encrypted WAL entry {} (seq: {}) with key {}",
            entry_id,
            sequence,
            key.key_id
        );

        Ok(encrypted_entry)
    }

    /// Decrypt a WAL entry
    pub fn decrypt_entry(&self, entry_id: u64, encrypted_data: &[u8]) -> Result<Vec<u8>> {
        // Deserialize encrypted entry
        let (header, metadata, ciphertext) = self.deserialize_encrypted_entry(encrypted_data)?;

        // Get the key used for encryption
        let key = self.key_manager.get_dek(metadata.key_id)?;

        // Verify AAD
        let expected_aad = self.serialize_header(&header)?;
        if let Some(ref aad) = metadata.aad {
            if aad != &expected_aad {
                return Err(Error::Encryption("WAL entry AAD mismatch".to_string()));
            }
        }

        // Decrypt based on algorithm
        let plaintext = match metadata.algorithm {
            EncryptionAlgorithm::Aes256Gcm => self.decrypt_aes_gcm(
                &key.key_material,
                &metadata.nonce,
                &ciphertext,
                &expected_aad,
            )?,
            EncryptionAlgorithm::ChaCha20Poly1305 => self.decrypt_chacha20(
                &key.key_material,
                &metadata.nonce,
                &ciphertext,
                &expected_aad,
            )?,
        };

        // Verify decrypted size matches header
        if plaintext.len() != header.original_size as usize {
            return Err(Error::Encryption("WAL entry size mismatch".to_string()));
        }

        self.entries_decrypted.fetch_add(1, Ordering::Relaxed);
        trace!(
            "Decrypted WAL entry {} (seq: {}) with key {}",
            entry_id,
            header.sequence,
            metadata.key_id
        );

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
            .map_err(|e| Error::Encryption(format!("WAL AES-GCM encryption failed: {}", e)))
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
            .map_err(|e| Error::Encryption(format!("WAL AES-GCM decryption failed: {}", e)))
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
            .map_err(|e| Error::Encryption(format!("WAL ChaCha20 encryption failed: {}", e)))
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
            .map_err(|e| Error::Encryption(format!("WAL ChaCha20 decryption failed: {}", e)))
    }

    /// Generate a unique nonce for a WAL entry
    fn generate_wal_nonce(&self, sequence: u64) -> Vec<u8> {
        let mut nonce = vec![0u8; 12]; // 96-bit nonce

        // First 8 bytes: sequence number (ensures uniqueness)
        nonce[..8].copy_from_slice(&sequence.to_le_bytes());

        // Last 4 bytes: random (additional entropy)
        let random_bytes = generate_nonce(4);
        nonce[8..].copy_from_slice(&random_bytes);

        nonce
    }

    /// Serialize WAL entry header
    fn serialize_header(&self, header: &WalEntryHeader) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(21); // 8 + 1 + 4 + 8
        result.extend_from_slice(&header.sequence.to_le_bytes());
        result.push(header.entry_type);
        result.extend_from_slice(&header.original_size.to_le_bytes());
        result.extend_from_slice(&header.timestamp.to_le_bytes());
        Ok(result)
    }

    /// Deserialize WAL entry header
    fn deserialize_header(&self, data: &[u8]) -> Result<WalEntryHeader> {
        if data.len() < 21 {
            return Err(Error::Encryption("Invalid WAL header size".to_string()));
        }

        Ok(WalEntryHeader {
            sequence: u64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]),
            entry_type: data[8],
            original_size: u32::from_le_bytes([data[9], data[10], data[11], data[12]]),
            timestamp: u64::from_le_bytes([
                data[13], data[14], data[15], data[16], data[17], data[18], data[19], data[20],
            ]),
        })
    }

    /// Serialize encrypted WAL entry
    fn serialize_encrypted_entry(
        &self,
        header: WalEntryHeader,
        metadata: EncryptionMetadata,
        ciphertext: Vec<u8>,
    ) -> Result<Vec<u8>> {
        // Format: [header][metadata_len:u32][metadata][ciphertext]
        let header_bytes = self.serialize_header(&header)?;
        let metadata_bytes = bincode::encode_to_vec(&metadata, bincode::config::standard())
            .map_err(|e| Error::Encryption(format!("Failed to serialize WAL metadata: {}", e)))?;

        let metadata_len = metadata_bytes.len() as u32;
        let total_len = header_bytes.len() + 4 + metadata_bytes.len() + ciphertext.len();

        let mut result = Vec::with_capacity(total_len);
        result.extend_from_slice(&header_bytes);
        result.extend_from_slice(&metadata_len.to_le_bytes());
        result.extend_from_slice(&metadata_bytes);
        result.extend_from_slice(&ciphertext);

        Ok(result)
    }

    /// Deserialize encrypted WAL entry
    fn deserialize_encrypted_entry(
        &self,
        data: &[u8],
    ) -> Result<(WalEntryHeader, EncryptionMetadata, Vec<u8>)> {
        if data.len() < 25 {
            // 21 (header) + 4 (metadata_len)
            return Err(Error::Encryption(
                "Invalid encrypted WAL entry format".to_string(),
            ));
        }

        // Deserialize header
        let header = self.deserialize_header(&data[..21])?;

        // Read metadata length
        let metadata_len = u32::from_le_bytes([data[21], data[22], data[23], data[24]]) as usize;

        if data.len() < 25 + metadata_len {
            return Err(Error::Encryption(
                "Truncated encrypted WAL entry".to_string(),
            ));
        }

        // Deserialize metadata
        let (metadata, _): (EncryptionMetadata, _) = bincode::decode_from_slice(
            &data[25..25 + metadata_len],
            bincode::config::standard(),
        )
        .map_err(|e| Error::Encryption(format!("Failed to deserialize WAL metadata: {}", e)))?;

        // Extract ciphertext
        let ciphertext = data[25 + metadata_len..].to_vec();

        Ok((header, metadata, ciphertext))
    }

    /// Get count of entries encrypted
    pub fn get_encrypted_count(&self) -> u64 {
        self.entries_encrypted.load(Ordering::Relaxed)
    }

    /// Get count of entries decrypted
    pub fn get_decrypted_count(&self) -> u64 {
        self.entries_decrypted.load(Ordering::Relaxed)
    }

    /// Reset sequence number (for WAL rotation)
    pub fn reset_sequence(&self) {
        self.sequence_number.store(0, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_encryptor() -> (WalEncryptor, Arc<KeyManager>) {
        let config = super::super::EncryptionConfig {
            enabled: true,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            ..Default::default()
        };

        let key_manager = Arc::new(KeyManager::new(config.clone()).unwrap());
        let master_key = vec![0x42; 32];
        key_manager.initialize_master_key(&master_key).unwrap();
        key_manager.generate_data_key().unwrap();

        let encryptor = WalEncryptor::new(key_manager.clone(), config.algorithm).unwrap();
        (encryptor, key_manager)
    }

    #[test]
    fn test_wal_entry_encryption_decryption() {
        let (encryptor, _) = create_test_encryptor();

        let entry_id = 1;
        let plaintext = b"WAL entry: INSERT INTO users VALUES (1, 'Alice')";

        // Encrypt
        let encrypted = encryptor.encrypt_entry(entry_id, plaintext).unwrap();
        assert_ne!(encrypted, plaintext);

        // Decrypt
        let decrypted = encryptor.decrypt_entry(entry_id, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);

        // Verify counters
        assert_eq!(encryptor.get_encrypted_count(), 1);
        assert_eq!(encryptor.get_decrypted_count(), 1);
    }

    #[test]
    fn test_sequential_entries_different_nonces() {
        let (encryptor, _) = create_test_encryptor();

        let plaintext = b"Same WAL entry content";

        let encrypted1 = encryptor.encrypt_entry(1, plaintext).unwrap();
        let encrypted2 = encryptor.encrypt_entry(2, plaintext).unwrap();

        // Same plaintext should produce different ciphertexts due to different sequence numbers
        assert_ne!(encrypted1, encrypted2);

        // Both should decrypt correctly
        assert_eq!(encryptor.decrypt_entry(1, &encrypted1).unwrap(), plaintext);
        assert_eq!(encryptor.decrypt_entry(2, &encrypted2).unwrap(), plaintext);
    }

    #[test]
    fn test_tampered_wal_entry_fails() {
        let (encryptor, _) = create_test_encryptor();

        let entry_id = 42;
        let plaintext = b"Critical transaction data";

        let mut encrypted = encryptor.encrypt_entry(entry_id, plaintext).unwrap();

        // Tamper with the ciphertext
        let tamper_pos = encrypted.len() - 5;
        encrypted[tamper_pos] ^= 0xFF;

        // Decryption should fail due to authentication
        assert!(encryptor.decrypt_entry(entry_id, &encrypted).is_err());
    }

    #[test]
    fn test_sequence_number_increment() {
        let (encryptor, _) = create_test_encryptor();

        // Encrypt multiple entries
        for i in 0..5 {
            let data = format!("Entry {}", i);
            encryptor.encrypt_entry(i, data.as_bytes()).unwrap();
        }

        // Sequence should have incremented
        assert_eq!(encryptor.sequence_number.load(Ordering::SeqCst), 5);

        // Reset sequence
        encryptor.reset_sequence();
        assert_eq!(encryptor.sequence_number.load(Ordering::SeqCst), 0);
    }
}
