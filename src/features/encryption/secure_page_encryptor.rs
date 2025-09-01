//! Secure Page-Level Encryption for Lightning DB
//!
//! Provides transparent, secure encryption of database pages with proper
//! nonce management, authenticated encryption, and envelope encryption for large pages.

use crate::core::error::{Error, Result};
use crate::features::encryption::{
    envelope_encryption::{EnvelopeEncryptionManager, EnvelopeKEK, EnvelopeConfig},
    secure_crypto_provider::{SecureCryptoProvider, SecureAlgorithm, SecureKey},
    hsm_provider::HSMProvider,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use tokio::sync::RwLock;

/// Secure page encryptor with envelope encryption for large pages
pub struct SecurePageEncryptor {
    /// Crypto provider for direct encryption
    crypto_provider: Arc<SecureCryptoProvider>,
    /// Envelope encryption manager for large pages
    envelope_manager: Arc<EnvelopeEncryptionManager>,
    /// Current data encryption keys by key ID
    keys: Arc<RwLock<HashMap<u64, SecureKey>>>,
    /// Current key encryption key
    kek: Arc<RwLock<Option<EnvelopeKEK>>>,
    /// Configuration
    config: SecurePageConfig,
    /// Statistics
    stats: Arc<PageEncryptionStats>,
}

/// Configuration for secure page encryption
#[derive(Debug, Clone)]
pub struct SecurePageConfig {
    /// Algorithm for page encryption
    pub algorithm: SecureAlgorithm,
    /// Threshold for using envelope encryption (bytes)
    pub envelope_threshold: usize,
    /// Enable page compression before encryption
    pub enable_compression: bool,
    /// Page size for alignment
    pub page_size: usize,
    /// Enable integrity verification on every read
    pub verify_integrity: bool,
}

impl Default for SecurePageConfig {
    fn default() -> Self {
        Self {
            algorithm: SecureAlgorithm::Aes256Gcm,
            envelope_threshold: 64 * 1024, // 64KB
            enable_compression: true,
            page_size: 4096,
            verify_integrity: true,
        }
    }
}

/// Encrypted page format
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct SecureEncryptedPage {
    /// Page ID for verification
    pub page_id: u64,
    /// Encryption method used
    pub encryption_method: PageEncryptionMethod,
    /// Page metadata
    pub metadata: PageMetadata,
    /// Integrity hash of original data
    pub integrity_hash: Vec<u8>,
}

/// Page encryption methods
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub enum PageEncryptionMethod {
    /// Direct encryption with DEK
    Direct {
        /// Encrypted page data
        encrypted_data: crate::features::encryption::secure_crypto_provider::SecureEncryptionResult,
        /// Key ID used
        key_id: u64,
    },
    /// Envelope encryption for large pages
    Envelope {
        /// Envelope encrypted data
        envelope_data: crate::features::encryption::envelope_encryption::EnvelopeEncryptedData,
    },
}

/// Page metadata
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct PageMetadata {
    /// Original page size
    pub original_size: u32,
    /// Compressed size (if compression used)
    pub compressed_size: Option<u32>,
    /// Encryption timestamp
    pub encrypted_at: u64,
    /// Algorithm used
    pub algorithm: SecureAlgorithm,
    /// Page checksum before encryption
    pub page_checksum: u32,
}

/// Page encryption statistics
#[derive(Debug)]
pub struct PageEncryptionStats {
    /// Pages encrypted with direct encryption
    pub direct_encrypted: AtomicU64,
    /// Pages encrypted with envelope encryption
    pub envelope_encrypted: AtomicU64,
    /// Pages decrypted
    pub decrypted: AtomicU64,
    /// Integrity check failures
    pub integrity_failures: AtomicU64,
    /// Bytes saved by compression
    pub compression_savings: AtomicU64,
}

impl SecurePageEncryptor {
    /// Create new secure page encryptor
    pub fn new(
        crypto_provider: Arc<SecureCryptoProvider>,
        hsm_provider: Option<Arc<dyn HSMProvider>>,
        config: SecurePageConfig,
    ) -> Result<Self> {
        let envelope_config = EnvelopeConfig {
            data_algorithm: config.algorithm,
            chunk_size: config.page_size,
            parallel_chunks: false, // Pages are typically small
            max_parallel_tasks: 1,
            use_hsm_for_kek: hsm_provider.is_some(),
        };

        let envelope_manager = Arc::new(EnvelopeEncryptionManager::new(
            crypto_provider.clone(),
            hsm_provider,
            envelope_config,
        ));

        let stats = Arc::new(PageEncryptionStats {
            direct_encrypted: AtomicU64::new(0),
            envelope_encrypted: AtomicU64::new(0),
            decrypted: AtomicU64::new(0),
            integrity_failures: AtomicU64::new(0),
            compression_savings: AtomicU64::new(0),
        });

        Ok(Self {
            crypto_provider,
            envelope_manager,
            keys: Arc::new(RwLock::new(HashMap::new())),
            kek: Arc::new(RwLock::new(None)),
            config,
            stats,
        })
    }

    /// Set current encryption keys
    pub async fn set_keys(&self, dek: SecureKey, kek: EnvelopeKEK) -> Result<()> {
        let key_id = dek.id;
        
        {
            let mut keys = self.keys.write().await;
            keys.insert(key_id, dek);
        }
        
        {
            let mut current_kek = self.kek.write().await;
            *current_kek = Some(kek);
        }
        
        Ok(())
    }

    /// Encrypt a database page securely
    pub async fn encrypt_page(&self, page_id: u64, data: &[u8]) -> Result<Vec<u8>> {
        // Compress data if enabled and beneficial
        let (data_to_encrypt, compressed_size) = if self.config.enable_compression {
            self.compress_page_data(data)?
        } else {
            (data.to_vec(), None)
        };

        // Update compression statistics
        if let Some(compressed) = compressed_size {
            if compressed < data.len() as u32 {
                self.stats.compression_savings
                    .fetch_add((data.len() as u32 - compressed) as u64, Ordering::Relaxed);
            }
        }

        // Compute integrity hash of original data
        let integrity_hash = blake3::hash(data).as_bytes().to_vec();
        
        // Compute page checksum
        let page_checksum = crc32fast::hash(data);

        // Create page context for encryption
        let page_context = self.create_page_context(page_id, &integrity_hash);

        // Choose encryption method based on size
        let encryption_method = if data_to_encrypt.len() > self.config.envelope_threshold {
            // Use envelope encryption for large pages
            let kek = {
                let kek_guard = self.kek.read().await;
                kek_guard.as_ref()
                    .ok_or_else(|| Error::Encryption("No KEK configured".to_string()))?
                    .clone()
            };

            let envelope_data = self.envelope_manager
                .encrypt(&data_to_encrypt, &kek, &page_context)
                .await?;

            self.stats.envelope_encrypted.fetch_add(1, Ordering::Relaxed);
            
            PageEncryptionMethod::Envelope { envelope_data }
        } else {
            // Use direct encryption for small pages
            let dek = {
                let keys = self.keys.read().await;
                let key_id = keys.keys().next()
                    .ok_or_else(|| Error::Encryption("No DEK available".to_string()))?;
                keys.get(key_id)
                    .ok_or_else(|| Error::Encryption("DEK not found".to_string()))?
                    .clone()
            };

            let aad = self.create_page_aad(page_id, &integrity_hash);
            let encrypted_data = self.crypto_provider.encrypt(
                &dek,
                &data_to_encrypt,
                &aad,
                &page_context,
            )?;

            self.stats.direct_encrypted.fetch_add(1, Ordering::Relaxed);

            PageEncryptionMethod::Direct {
                encrypted_data,
                key_id: dek.id,
            }
        };

        // Create page metadata
        let metadata = PageMetadata {
            original_size: data.len() as u32,
            compressed_size,
            encrypted_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| Error::Encryption(format!("Time error: {}", e)))?
                .as_secs(),
            algorithm: self.config.algorithm,
            page_checksum,
        };

        // Create encrypted page structure
        let encrypted_page = SecureEncryptedPage {
            page_id,
            encryption_method,
            metadata,
            integrity_hash,
        };

        // Serialize the encrypted page
        self.serialize_encrypted_page(&encrypted_page)
    }

    /// Decrypt a database page securely
    pub async fn decrypt_page(&self, page_id: u64, encrypted_data: &[u8]) -> Result<Vec<u8>> {
        // Deserialize encrypted page
        let encrypted_page = self.deserialize_encrypted_page(encrypted_data)?;

        // Verify page ID matches
        if encrypted_page.page_id != page_id {
            return Err(Error::Encryption("Page ID mismatch during decryption".to_string()));
        }

        // Decrypt based on method
        let decrypted_data = match &encrypted_page.encryption_method {
            PageEncryptionMethod::Direct { encrypted_data, key_id } => {
                let dek = {
                    let keys = self.keys.read().await;
                    keys.get(key_id)
                        .ok_or_else(|| Error::Encryption(format!("DEK {} not found", key_id)))?
                        .clone()
                };

                self.crypto_provider.decrypt(&dek, encrypted_data)?
            }
            PageEncryptionMethod::Envelope { envelope_data } => {
                let kek = {
                    let kek_guard = self.kek.read().await;
                    kek_guard.as_ref()
                        .ok_or_else(|| Error::Encryption("No KEK configured".to_string()))?
                        .clone()
                };

                self.envelope_manager.decrypt(envelope_data, &kek).await?
            }
        };

        // Decompress if needed
        let final_data = if encrypted_page.metadata.compressed_size.is_some() {
            self.decompress_page_data(&decrypted_data)?
        } else {
            decrypted_data
        };

        // Verify integrity
        if self.config.verify_integrity {
            let computed_hash = blake3::hash(&final_data).as_bytes().to_vec();
            if !self.crypto_provider.constant_time_compare(&encrypted_page.integrity_hash, &computed_hash) {
                self.stats.integrity_failures.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Encryption("Page integrity verification failed".to_string()));
            }

            // Verify page checksum
            let computed_checksum = crc32fast::hash(&final_data);
            if computed_checksum != encrypted_page.metadata.page_checksum {
                self.stats.integrity_failures.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Encryption("Page checksum verification failed".to_string()));
            }
        }

        self.stats.decrypted.fetch_add(1, Ordering::Relaxed);
        Ok(final_data)
    }

    /// Create page-specific encryption context
    fn create_page_context(&self, page_id: u64, integrity_hash: &[u8]) -> Vec<u8> {
        let mut context = Vec::new();
        context.extend_from_slice(b"lightning_db_page");
        context.extend_from_slice(&page_id.to_le_bytes());
        context.extend_from_slice(&integrity_hash[..8]); // First 8 bytes of hash
        context
    }

    /// Create Additional Authenticated Data for direct encryption
    fn create_page_aad(&self, page_id: u64, integrity_hash: &[u8]) -> Vec<u8> {
        let mut aad = Vec::new();
        aad.extend_from_slice(&page_id.to_le_bytes());
        aad.extend_from_slice(integrity_hash);
        aad.extend_from_slice(&self.config.page_size.to_le_bytes());
        aad
    }

    /// Compress page data if beneficial
    fn compress_page_data(&self, data: &[u8]) -> Result<(Vec<u8>, Option<u32>)> {
        // Don't compress very small pages
        if data.len() < 256 {
            return Ok((data.to_vec(), None));
        }

        #[cfg(feature = "zstd-compression")]
        {
            let compressed = zstd::encode_all(data, 3)
                .map_err(|e| Error::Encryption(format!("Page compression failed: {}", e)))?;

            // Only use compression if it saves at least 10% space
            if compressed.len() < data.len() * 9 / 10 {
                let len = compressed.len() as u32;
                Ok((compressed, Some(len)))
            } else {
                Ok((data.to_vec(), None))
            }
        }

        #[cfg(not(feature = "zstd-compression"))]
        Ok((data.to_vec(), None))
    }

    /// Decompress page data
    fn decompress_page_data(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        #[cfg(feature = "zstd-compression")]
        {
            zstd::decode_all(compressed_data)
                .map_err(|e| Error::Encryption(format!("Page decompression failed: {}", e)))
        }

        #[cfg(not(feature = "zstd-compression"))]
        Err(Error::Encryption("Zstd compression not available".to_string()))
    }

    /// Serialize encrypted page to bytes
    fn serialize_encrypted_page(&self, page: &SecureEncryptedPage) -> Result<Vec<u8>> {
        bincode::encode_to_vec(page, bincode::config::standard())
            .map_err(|e| Error::Encryption(format!("Page serialization failed: {}", e)))
    }

    /// Deserialize encrypted page from bytes
    fn deserialize_encrypted_page(&self, data: &[u8]) -> Result<SecureEncryptedPage> {
        let (page, _): (SecureEncryptedPage, _) = bincode::decode_from_slice(data, bincode::config::standard())
            .map_err(|e| Error::Encryption(format!("Page deserialization failed: {}", e)))?;
        Ok(page)
    }

    /// Get encryption statistics
    pub fn get_stats(&self) -> PageEncryptionStatsSnapshot {
        PageEncryptionStatsSnapshot {
            direct_encrypted: self.stats.direct_encrypted.load(Ordering::Relaxed),
            envelope_encrypted: self.stats.envelope_encrypted.load(Ordering::Relaxed),
            decrypted: self.stats.decrypted.load(Ordering::Relaxed),
            integrity_failures: self.stats.integrity_failures.load(Ordering::Relaxed),
            compression_savings: self.stats.compression_savings.load(Ordering::Relaxed),
        }
    }

    /// Rotate encryption keys
    pub async fn rotate_keys(&self, new_dek: SecureKey, new_kek: EnvelopeKEK) -> Result<()> {
        self.set_keys(new_dek, new_kek).await
    }

    /// Check if a page is encrypted
    pub fn is_encrypted(&self, data: &[u8]) -> bool {
        // Try to deserialize as encrypted page
        self.deserialize_encrypted_page(data).is_ok()
    }
}

/// Snapshot of page encryption statistics
#[derive(Debug, Clone)]
pub struct PageEncryptionStatsSnapshot {
    pub direct_encrypted: u64,
    pub envelope_encrypted: u64,
    pub decrypted: u64,
    pub integrity_failures: u64,
    pub compression_savings: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_test_encryptor() -> (SecurePageEncryptor, SecureKey) {
        let crypto_provider = Arc::new(SecureCryptoProvider::new().unwrap());
        let config = SecurePageConfig::default();
        
        let encryptor = SecurePageEncryptor::new(crypto_provider.clone(), None, config).unwrap();
        
        // Generate keys
        let dek = crypto_provider.generate_key(SecureAlgorithm::Aes256Gcm).unwrap();
        let kek_key = crypto_provider.generate_key(SecureAlgorithm::Aes256Gcm).unwrap();
        let kek = EnvelopeKEK::Software(kek_key);
        
        encryptor.set_keys(dek.clone(), kek).await.unwrap();
        
        (encryptor, dek)
    }

    #[tokio::test]
    async fn test_page_encryption_decryption() {
        let (encryptor, _) = create_test_encryptor().await;
        
        let page_id = 42;
        let page_data = b"This is a database page with important data".repeat(10);
        
        let encrypted = encryptor.encrypt_page(page_id, &page_data).await.unwrap();
        let decrypted = encryptor.decrypt_page(page_id, &encrypted).await.unwrap();
        
        assert_eq!(page_data, decrypted);
    }

    #[tokio::test]
    async fn test_large_page_envelope_encryption() {
        let (encryptor, _) = create_test_encryptor().await;
        
        let page_id = 123;
        // Create page larger than envelope threshold
        let page_data = vec![0xAB; 100 * 1024]; // 100KB
        
        let encrypted = encryptor.encrypt_page(page_id, &page_data).await.unwrap();
        let decrypted = encryptor.decrypt_page(page_id, &encrypted).await.unwrap();
        
        assert_eq!(page_data, decrypted);
        
        // Should have used envelope encryption
        let stats = encryptor.get_stats();
        assert_eq!(stats.envelope_encrypted, 1);
    }

    #[tokio::test]
    async fn test_page_integrity_protection() {
        let (encryptor, _) = create_test_encryptor().await;
        
        let page_id = 456;
        let page_data = b"sensitive page data";
        
        let mut encrypted = encryptor.encrypt_page(page_id, page_data).await.unwrap();
        
        // Tamper with encrypted data
        if encrypted.len() > 10 {
            encrypted[encrypted.len() - 10] ^= 0xFF;
        }
        
        // Decryption should fail
        assert!(encryptor.decrypt_page(page_id, &encrypted).await.is_err());
    }

    #[tokio::test]
    async fn test_page_id_verification() {
        let (encryptor, _) = create_test_encryptor().await;
        
        let page_data = b"test data";
        let encrypted = encryptor.encrypt_page(100, page_data).await.unwrap();
        
        // Try to decrypt with wrong page ID
        assert!(encryptor.decrypt_page(200, &encrypted).await.is_err());
    }

    #[tokio::test]
    async fn test_compression_effectiveness() {
        let (encryptor, _) = create_test_encryptor().await;
        
        let page_id = 789;
        // Highly compressible data
        let page_data = vec![0x00; 8192]; // 8KB of zeros
        
        let encrypted = encryptor.encrypt_page(page_id, &page_data).await.unwrap();
        let decrypted = encryptor.decrypt_page(page_id, &encrypted).await.unwrap();
        
        assert_eq!(page_data, decrypted);
        
        // Should have saved bytes through compression
        let stats = encryptor.get_stats();
        assert!(stats.compression_savings > 0);
    }
}