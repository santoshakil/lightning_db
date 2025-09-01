//! Envelope Encryption Implementation for Lightning DB
//!
//! Provides secure encryption for large data by using envelope encryption pattern:
//! 1. Generate a random Data Encryption Key (DEK)
//! 2. Encrypt the data with the DEK using symmetric encryption
//! 3. Encrypt the DEK with a Key Encryption Key (KEK) 
//! 4. Store both encrypted data and encrypted DEK

use crate::core::error::{Error, Result};
use crate::features::encryption::{
    hsm_provider::{HSMProvider, HSMKeyHandle, HSMEncryptResult},
    secure_crypto_provider::{SecureCryptoProvider, SecureAlgorithm, SecureKey, SecureEncryptionResult},
};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::task;
use zeroize::ZeroizeOnDrop;

/// Envelope encryption manager
pub struct EnvelopeEncryptionManager {
    /// Crypto provider for DEK operations
    crypto_provider: Arc<SecureCryptoProvider>,
    /// Optional HSM provider for KEK operations
    hsm_provider: Option<Arc<dyn HSMProvider>>,
    /// Configuration
    config: EnvelopeConfig,
}

/// Configuration for envelope encryption
#[derive(Debug, Clone)]
pub struct EnvelopeConfig {
    /// Algorithm for data encryption (DEK operations)
    pub data_algorithm: SecureAlgorithm,
    /// Chunk size for large data processing (bytes)
    pub chunk_size: usize,
    /// Enable parallel processing of chunks
    pub parallel_chunks: bool,
    /// Maximum number of parallel tasks
    pub max_parallel_tasks: usize,
    /// Use HSM for KEK operations
    pub use_hsm_for_kek: bool,
}

impl Default for EnvelopeConfig {
    fn default() -> Self {
        Self {
            data_algorithm: SecureAlgorithm::Aes256Gcm,
            chunk_size: 64 * 1024, // 64KB chunks
            parallel_chunks: true,
            max_parallel_tasks: num_cpus::get(),
            use_hsm_for_kek: false,
        }
    }
}

/// Envelope encrypted data package
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct EnvelopeEncryptedData {
    /// Encrypted DEK using KEK
    pub encrypted_dek: EnvelopeDEK,
    /// Encrypted data using DEK
    pub encrypted_data: EnvelopeDataPayload,
    /// Metadata about the encryption
    pub metadata: EnvelopeMetadata,
    /// Integrity signature of the entire envelope
    pub envelope_signature: Vec<u8>,
}

/// Encrypted Data Encryption Key
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub enum EnvelopeDEK {
    /// DEK encrypted with software KEK
    Software {
        /// Encrypted DEK material
        encrypted_key: SecureEncryptionResult,
        /// KEK identifier used
        kek_id: u64,
    },
    /// DEK encrypted with HSM KEK
    HSM {
        /// HSM encrypted result
        encrypted_key: HSMEncryptResult,
        /// HSM key handle used
        kek_handle: HSMKeyHandle,
    },
}

/// Encrypted data payload - supports chunked encryption for large data
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct EnvelopeDataPayload {
    /// Encrypted chunks for large data
    pub chunks: Vec<EnvelopeChunk>,
    /// Total original data size
    pub total_size: u64,
    /// Chunk size used
    pub chunk_size: usize,
}

/// Individual encrypted chunk
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct EnvelopeChunk {
    /// Chunk index for ordering
    pub index: u32,
    /// Encrypted chunk data
    pub encrypted_data: SecureEncryptionResult,
    /// Chunk hash for integrity
    pub chunk_hash: Vec<u8>,
}

/// Envelope metadata
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct EnvelopeMetadata {
    /// Envelope format version
    pub version: u32,
    /// Creation timestamp
    pub created_at: u64,
    /// Data algorithm used
    pub algorithm: SecureAlgorithm,
    /// Whether HSM was used for KEK
    pub hsm_protected: bool,
    /// Additional context
    pub context: Vec<u8>,
    /// Compression algorithm if used
    pub compression: Option<CompressionAlgorithm>,
}

/// Compression algorithms
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub enum CompressionAlgorithm {
    None,
    Zstd,
    Lz4,
}

/// Temporary DEK for encryption operations
#[derive(ZeroizeOnDrop)]
struct EphemeralDEK {
    /// Key material (automatically zeroed)
    key: SecureKey,
    /// Key ID for tracking
    #[zeroize(skip)]
    id: u64,
}

impl EnvelopeEncryptionManager {
    /// Create new envelope encryption manager
    pub fn new(
        crypto_provider: Arc<SecureCryptoProvider>,
        hsm_provider: Option<Arc<dyn HSMProvider>>,
        config: EnvelopeConfig,
    ) -> Self {
        Self {
            crypto_provider,
            hsm_provider,
            config,
        }
    }

    /// Encrypt large data using envelope encryption
    pub async fn encrypt(
        &self,
        data: &[u8],
        kek: &EnvelopeKEK,
        context: &[u8],
    ) -> Result<EnvelopeEncryptedData> {
        // Generate ephemeral DEK
        let dek = self.generate_ephemeral_dek().await?;
        
        // Encrypt DEK with KEK
        let encrypted_dek = self.encrypt_dek(&dek, kek).await?;
        
        // Compress data if configured
        let (data_to_encrypt, compression) = self.compress_data(data)?;
        
        // Encrypt data with DEK in chunks
        let encrypted_data = if self.config.parallel_chunks && data_to_encrypt.len() > self.config.chunk_size * 2 {
            self.encrypt_data_parallel(&dek.key, &data_to_encrypt, context).await?
        } else {
            self.encrypt_data_sequential(&dek.key, &data_to_encrypt, context).await?
        };
        
        let metadata = EnvelopeMetadata {
            version: 1,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| Error::Encryption(format!("Time error: {}", e)))?
                .as_secs(),
            algorithm: self.config.data_algorithm,
            hsm_protected: matches!(encrypted_dek, EnvelopeDEK::HSM { .. }),
            context: context.to_vec(),
            compression,
        };
        
        let envelope = EnvelopeEncryptedData {
            encrypted_dek,
            encrypted_data,
            metadata,
            envelope_signature: Vec::new(), // Will be computed below
        };
        
        // Sign the entire envelope for integrity
        let mut envelope_with_sig = envelope;
        envelope_with_sig.envelope_signature = self.sign_envelope(&envelope_with_sig, &dek.key).await?;
        
        Ok(envelope_with_sig)
    }

    /// Decrypt envelope encrypted data
    pub async fn decrypt(
        &self,
        encrypted: &EnvelopeEncryptedData,
        kek: &EnvelopeKEK,
    ) -> Result<Vec<u8>> {
        // Decrypt DEK with KEK
        let dek = self.decrypt_dek(&encrypted.encrypted_dek, kek).await?;
        
        // Verify envelope signature
        let expected_sig = self.sign_envelope(encrypted, &dek.key).await?;
        if !self.crypto_provider.constant_time_compare(&encrypted.envelope_signature, &expected_sig) {
            return Err(Error::Encryption("Envelope signature verification failed".to_string()));
        }
        
        // Decrypt data chunks
        let decrypted_data = if self.config.parallel_chunks && encrypted.encrypted_data.chunks.len() > 2 {
            self.decrypt_data_parallel(&dek.key, &encrypted.encrypted_data).await?
        } else {
            self.decrypt_data_sequential(&dek.key, &encrypted.encrypted_data).await?
        };
        
        // Decompress if needed
        self.decompress_data(&decrypted_data, &encrypted.metadata.compression)
    }

    /// Generate ephemeral DEK for single encryption operation
    async fn generate_ephemeral_dek(&self) -> Result<EphemeralDEK> {
        let key = self.crypto_provider.generate_key(self.config.data_algorithm)?;
        let id = {
            let mut id_bytes = [0u8; 8];
            OsRng.fill_bytes(&mut id_bytes);
            u64::from_le_bytes(id_bytes)
        };
        
        Ok(EphemeralDEK { key, id })
    }

    /// Encrypt DEK using KEK
    async fn encrypt_dek(&self, dek: &EphemeralDEK, kek: &EnvelopeKEK) -> Result<EnvelopeDEK> {
        match kek {
            EnvelopeKEK::Software(software_kek) => {
                let encrypted_key = self.crypto_provider.encrypt(
                    software_kek,
                    &dek.key.material,
                    &dek.id.to_le_bytes(),
                    b"dek_encryption",
                )?;
                
                Ok(EnvelopeDEK::Software {
                    encrypted_key,
                    kek_id: software_kek.id,
                })
            }
            EnvelopeKEK::HSM(hsm_handle) => {
                if let Some(hsm) = &self.hsm_provider {
                    let encrypted_key = hsm.encrypt(hsm_handle, &dek.key.material, Some(&dek.id.to_le_bytes())).await?;
                    
                    Ok(EnvelopeDEK::HSM {
                        encrypted_key,
                        kek_handle: hsm_handle.clone(),
                    })
                } else {
                    Err(Error::Encryption("HSM provider not available".to_string()))
                }
            }
        }
    }

    /// Decrypt DEK using KEK
    async fn decrypt_dek(&self, encrypted_dek: &EnvelopeDEK, kek: &EnvelopeKEK) -> Result<EphemeralDEK> {
        let (key_material, kek_id) = match (encrypted_dek, kek) {
            (EnvelopeDEK::Software { encrypted_key, kek_id }, EnvelopeKEK::Software(software_kek)) => {
                if software_kek.id != *kek_id {
                    return Err(Error::Encryption("KEK ID mismatch".to_string()));
                }
                let material = self.crypto_provider.decrypt(software_kek, encrypted_key)?;
                (material, *kek_id)
            }
            (EnvelopeDEK::HSM { encrypted_key, kek_handle }, EnvelopeKEK::HSM(hsm_handle)) => {
                if let Some(hsm) = &self.hsm_provider {
                    if kek_handle.id != hsm_handle.id {
                        return Err(Error::Encryption("HSM key handle mismatch".to_string()));
                    }
                    let material = hsm.decrypt(hsm_handle, encrypted_key).await?;
                    (material, 0) // HSM handles don't have numeric IDs
                } else {
                    return Err(Error::Encryption("HSM provider not available".to_string()));
                }
            }
            _ => {
                return Err(Error::Encryption("KEK type mismatch".to_string()));
            }
        };

        // Reconstruct DEK
        let key = SecureKey {
            id: kek_id,
            material: key_material,
            algorithm: self.config.data_algorithm,
            created_at: std::time::SystemTime::now(),
        };

        Ok(EphemeralDEK { key, id: kek_id })
    }

    /// Encrypt data in sequential chunks
    async fn encrypt_data_sequential(
        &self,
        dek: &SecureKey,
        data: &[u8],
        context: &[u8],
    ) -> Result<EnvelopeDataPayload> {
        let mut chunks = Vec::new();
        let chunk_size = self.config.chunk_size;
        
        for (index, chunk_data) in data.chunks(chunk_size).enumerate() {
            let chunk_context = format!("chunk_{}_{}", index, hex::encode(context)).into_bytes();
            
            let encrypted_data = self.crypto_provider.encrypt(
                dek,
                chunk_data,
                &index.to_le_bytes(),
                &chunk_context,
            )?;
            
            let chunk_hash = blake3::hash(chunk_data).as_bytes().to_vec();
            
            chunks.push(EnvelopeChunk {
                index: index as u32,
                encrypted_data,
                chunk_hash,
            });
        }
        
        Ok(EnvelopeDataPayload {
            chunks,
            total_size: data.len() as u64,
            chunk_size,
        })
    }

    /// Encrypt data in parallel chunks
    async fn encrypt_data_parallel(
        &self,
        dek: &SecureKey,
        data: &[u8],
        context: &[u8],
    ) -> Result<EnvelopeDataPayload> {
        let chunk_size = self.config.chunk_size;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_parallel_tasks));
        
        let mut tasks = Vec::new();
        
        for (index, chunk_data) in data.chunks(chunk_size).enumerate() {
            let permit = Arc::clone(&semaphore);
            let crypto_provider = Arc::clone(&self.crypto_provider);
            let dek_clone = dek.clone();
            let chunk_data = chunk_data.to_vec();
            let context = context.to_vec();
            
            let task = task::spawn(async move {
                let _permit = permit.acquire().await.unwrap();
                
                let chunk_context = format!("chunk_{}_{}", index, hex::encode(&context)).into_bytes();
                
                let encrypted_data = crypto_provider.encrypt(
                    &dek_clone,
                    &chunk_data,
                    &index.to_le_bytes(),
                    &chunk_context,
                )?;
                
                let chunk_hash = blake3::hash(&chunk_data).as_bytes().to_vec();
                
                Ok::<_, Error>(EnvelopeChunk {
                    index: index as u32,
                    encrypted_data,
                    chunk_hash,
                })
            });
            
            tasks.push(task);
        }
        
        let mut chunks = Vec::new();
        for task in tasks {
            chunks.push(task.await.map_err(|e| Error::Encryption(format!("Task join error: {}", e)))??);
        }
        
        // Sort chunks by index to maintain order
        chunks.sort_by_key(|chunk| chunk.index);
        
        Ok(EnvelopeDataPayload {
            chunks,
            total_size: data.len() as u64,
            chunk_size,
        })
    }

    /// Decrypt data chunks sequentially
    async fn decrypt_data_sequential(
        &self,
        dek: &SecureKey,
        payload: &EnvelopeDataPayload,
    ) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(payload.total_size as usize);
        
        for chunk in &payload.chunks {
            let decrypted = self.crypto_provider.decrypt(dek, &chunk.encrypted_data)?;
            
            // Verify chunk integrity
            let computed_hash = blake3::hash(&decrypted).as_bytes().to_vec();
            if !self.crypto_provider.constant_time_compare(&chunk.chunk_hash, &computed_hash) {
                return Err(Error::Encryption(format!("Chunk {} integrity check failed", chunk.index)));
            }
            
            result.extend_from_slice(&decrypted);
        }
        
        Ok(result)
    }

    /// Decrypt data chunks in parallel
    async fn decrypt_data_parallel(
        &self,
        dek: &SecureKey,
        payload: &EnvelopeDataPayload,
    ) -> Result<Vec<u8>> {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_parallel_tasks));
        let mut tasks = Vec::new();
        
        for chunk in &payload.chunks {
            let permit = Arc::clone(&semaphore);
            let crypto_provider = Arc::clone(&self.crypto_provider);
            let dek_clone = dek.clone();
            let chunk = chunk.clone();
            
            let task = task::spawn(async move {
                let _permit = permit.acquire().await.unwrap();
                
                let decrypted = crypto_provider.decrypt(&dek_clone, &chunk.encrypted_data)?;
                
                // Verify chunk integrity
                let computed_hash = blake3::hash(&decrypted).as_bytes().to_vec();
                if !crypto_provider.constant_time_compare(&chunk.chunk_hash, &computed_hash) {
                    return Err(Error::Encryption(format!("Chunk {} integrity check failed", chunk.index)));
                }
                
                Ok::<_, Error>((chunk.index, decrypted))
            });
            
            tasks.push(task);
        }
        
        let mut chunks = Vec::new();
        for task in tasks {
            chunks.push(task.await.map_err(|e| Error::Encryption(format!("Task join error: {}", e)))??);
        }
        
        // Sort chunks by index and reconstruct data
        chunks.sort_by_key(|(index, _)| *index);
        
        let mut result = Vec::with_capacity(payload.total_size as usize);
        for (_, data) in chunks {
            result.extend_from_slice(&data);
        }
        
        Ok(result)
    }

    /// Compress data if configured
    fn compress_data(&self, data: &[u8]) -> Result<(Vec<u8>, Option<CompressionAlgorithm>)> {
        // Simple compression threshold - could be configurable
        if data.len() < 1024 {
            return Ok((data.to_vec(), Some(CompressionAlgorithm::None)));
        }

        // For now, always use Zstd compression for large data
        // In production, this could be configurable
        #[cfg(feature = "zstd-compression")]
        {
            let compressed = zstd::encode_all(data, 3) // Level 3 compression
                .map_err(|e| Error::Encryption(format!("Compression failed: {}", e)))?;
                
            // Only use compression if it actually reduces size
            if compressed.len() < data.len() {
                Ok((compressed, Some(CompressionAlgorithm::Zstd)))
            } else {
                Ok((data.to_vec(), Some(CompressionAlgorithm::None)))
            }
        }
        
        #[cfg(not(feature = "zstd-compression"))]
        Ok((data.to_vec(), Some(CompressionAlgorithm::None)))
    }

    /// Decompress data if needed
    fn decompress_data(
        &self,
        data: &[u8],
        compression: &Option<CompressionAlgorithm>,
    ) -> Result<Vec<u8>> {
        match compression {
            Some(CompressionAlgorithm::None) | None => Ok(data.to_vec()),
            #[cfg(feature = "zstd-compression")]
            Some(CompressionAlgorithm::Zstd) => {
                zstd::decode_all(data)
                    .map_err(|e| Error::Encryption(format!("Decompression failed: {}", e)))
            }
            #[cfg(not(feature = "zstd-compression"))]
            Some(CompressionAlgorithm::Zstd) => {
                Err(Error::Encryption("Zstd compression not available".to_string()))
            }
            Some(CompressionAlgorithm::Lz4) => {
                Err(Error::Encryption("Lz4 compression not yet implemented".to_string()))
            }
        }
    }

    /// Sign envelope for integrity
    async fn sign_envelope(&self, envelope: &EnvelopeEncryptedData, dek: &SecureKey) -> Result<Vec<u8>> {
        // Create signature over envelope metadata and encrypted DEK
        let mut sig_data = Vec::new();
        
        // Serialize metadata
        let metadata_bytes = bincode::encode_to_vec(&envelope.metadata, bincode::config::standard())
            .map_err(|e| Error::Encryption(format!("Metadata serialization failed: {}", e)))?;
        sig_data.extend_from_slice(&metadata_bytes);
        
        // Add encrypted DEK info
        match &envelope.encrypted_dek {
            EnvelopeDEK::Software { encrypted_key, kek_id } => {
                sig_data.extend_from_slice(&kek_id.to_le_bytes());
                sig_data.extend_from_slice(&encrypted_key.nonce);
                sig_data.extend_from_slice(&encrypted_key.ciphertext);
            }
            EnvelopeDEK::HSM { encrypted_key, kek_handle } => {
                sig_data.extend_from_slice(kek_handle.id.to_string().as_bytes());
                sig_data.extend_from_slice(&encrypted_key.ciphertext);
            }
        }
        
        // Add chunk hashes
        for chunk in &envelope.encrypted_data.chunks {
            sig_data.extend_from_slice(&chunk.chunk_hash);
        }
        
        // Sign with BLAKE3 keyed hash using DEK
        let key_array: [u8; 32] = dek.material.clone().try_into().map_err(|_| Error::Encryption("Invalid key length".to_string()))?;
        let hash = blake3::keyed_hash(&key_array, &sig_data);
        Ok(hash.as_bytes().to_vec())
    }
}

/// Key Encryption Key (KEK) types
#[derive(Debug, Clone)]
pub enum EnvelopeKEK {
    /// Software-based KEK
    Software(SecureKey),
    /// HSM-based KEK
    HSM(HSMKeyHandle),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_envelope_encryption_roundtrip() {
        let crypto_provider = Arc::new(SecureCryptoProvider::new().unwrap());
        let config = EnvelopeConfig::default();
        
        let manager = EnvelopeEncryptionManager::new(crypto_provider.clone(), None, config);
        
        // Generate KEK
        let kek_key = crypto_provider.generate_key(SecureAlgorithm::Aes256Gcm).unwrap();
        let kek = EnvelopeKEK::Software(kek_key);
        
        // Test data
        let data = b"This is sensitive data that needs envelope encryption protection".repeat(1000);
        let context = b"test_context";
        
        // Encrypt
        let encrypted = manager.encrypt(&data, &kek, context).await.unwrap();
        
        // Decrypt
        let decrypted = manager.decrypt(&encrypted, &kek).await.unwrap();
        
        assert_eq!(data, decrypted);
    }

    #[tokio::test]
    async fn test_large_data_parallel_encryption() {
        let crypto_provider = Arc::new(SecureCryptoProvider::new().unwrap());
        let mut config = EnvelopeConfig::default();
        config.chunk_size = 1024; // Small chunks for testing
        config.parallel_chunks = true;
        
        let manager = EnvelopeEncryptionManager::new(crypto_provider.clone(), None, config);
        
        let kek_key = crypto_provider.generate_key(SecureAlgorithm::Aes256Gcm).unwrap();
        let kek = EnvelopeKEK::Software(kek_key);
        
        // Large test data (>10KB)
        let data = vec![42u8; 10240];
        let context = b"large_data_test";
        
        let encrypted = manager.encrypt(&data, &kek, context).await.unwrap();
        let decrypted = manager.decrypt(&encrypted, &kek).await.unwrap();
        
        assert_eq!(data, decrypted);
        assert!(encrypted.encrypted_data.chunks.len() > 1);
    }

    #[tokio::test]
    async fn test_envelope_integrity_protection() {
        let crypto_provider = Arc::new(SecureCryptoProvider::new().unwrap());
        let config = EnvelopeConfig::default();
        
        let manager = EnvelopeEncryptionManager::new(crypto_provider.clone(), None, config);
        
        let kek_key = crypto_provider.generate_key(SecureAlgorithm::Aes256Gcm).unwrap();
        let kek = EnvelopeKEK::Software(kek_key);
        
        let data = b"sensitive data";
        let context = b"test";
        
        let mut encrypted = manager.encrypt(data, &kek, context).await.unwrap();
        
        // Tamper with signature
        encrypted.envelope_signature[0] ^= 0xFF;
        
        // Decryption should fail
        assert!(manager.decrypt(&encrypted, &kek).await.is_err());
    }
}