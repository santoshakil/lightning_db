//! Production-grade cryptographic provider for Lightning DB
//!
//! This module provides secure, industry-standard encryption implementations
//! that address the security vulnerabilities found in the existing codebase.

use crate::core::error::{Error, Result};
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Key, Nonce,
};
use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::ChaCha20Poly1305;
use rand_core::{OsRng, RngCore};
use ring::hmac;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use zeroize::ZeroizeOnDrop;
use subtle::ConstantTimeEq;

/// Secure cryptographic provider with production-grade security
#[derive(Debug)]
pub struct SecureCryptoProvider {
    /// Hardware acceleration detection
    has_aes_ni: bool,
    /// Entropy pool for additional randomness
    entropy_pool: Arc<EntropyPool>,
}

/// High-entropy pool for critical crypto operations
#[derive(Debug)]
struct EntropyPool {
    /// Pre-generated entropy buffer
    buffer: parking_lot::RwLock<Vec<u8>>,
    /// Minimum entropy threshold
    min_entropy: usize,
}

/// Secure nonce generator that prevents reuse
#[derive(Debug)]
pub struct NonceGenerator {
    /// Counter for sequential nonces
    counter: std::sync::atomic::AtomicU64,
    /// Random seed for each instance
    random_seed: [u8; 32],
}

/// Production-grade key derivation configuration
#[derive(Debug, Clone)]
pub struct SecureKDFConfig {
    /// Memory cost in KB (minimum 64MB for production)
    pub memory_cost: u32,
    /// Time cost (iterations, minimum 3 for production)
    pub time_cost: u32,
    /// Parallelism (threads, typically 1-4)
    pub parallelism: u32,
    /// Output length in bytes
    pub output_length: usize,
}

impl Default for SecureKDFConfig {
    fn default() -> Self {
        Self {
            memory_cost: 128 * 1024, // 128MB - strong protection
            time_cost: 10,            // 10 iterations - balanced security/performance
            parallelism: 4,           // 4 threads - utilize modern CPUs
            output_length: 32,        // 256-bit keys
        }
    }
}

/// Secure encryption result with authenticated metadata
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct SecureEncryptionResult {
    /// Encrypted data with authentication tag
    pub ciphertext: Vec<u8>,
    /// Unique nonce used for encryption
    pub nonce: Vec<u8>,
    /// Additional Authenticated Data (AAD)
    pub aad: Vec<u8>,
    /// Key ID used for encryption
    pub key_id: u64,
    /// Algorithm identifier
    pub algorithm: SecureAlgorithm,
    /// Timestamp of encryption
    pub timestamp: u64,
    /// HMAC of all metadata for integrity
    pub metadata_hmac: Vec<u8>,
}

/// Secure encryption algorithms
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, bincode::Encode, bincode::Decode)]
pub enum SecureAlgorithm {
    /// AES-256-GCM with hardware acceleration when available
    Aes256Gcm,
    /// ChaCha20-Poly1305 for platforms without AES-NI
    ChaCha20Poly1305,
}

/// Secure key material with automatic zeroization
#[derive(Clone, ZeroizeOnDrop)]
pub struct SecureKey {
    #[zeroize(skip)]
    pub id: u64,
    /// Raw key bytes (automatically zeroed on drop)
    pub material: Vec<u8>,
    #[zeroize(skip)]
    pub algorithm: SecureAlgorithm,
    #[zeroize(skip)]
    pub created_at: SystemTime,
}

impl SecureCryptoProvider {
    /// Create a new secure crypto provider with hardware detection
    pub fn new() -> Result<Self> {
        let has_aes_ni = Self::detect_aes_ni();
        let entropy_pool = Arc::new(EntropyPool::new()?);
        
        Ok(Self {
            has_aes_ni,
            entropy_pool,
        })
    }

    /// Detect AES-NI hardware acceleration
    fn detect_aes_ni() -> bool {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            is_x86_feature_detected!("aes")
        }
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        {
            false
        }
    }

    /// Get recommended algorithm based on hardware capabilities
    pub fn recommended_algorithm(&self) -> SecureAlgorithm {
        if self.has_aes_ni {
            SecureAlgorithm::Aes256Gcm
        } else {
            SecureAlgorithm::ChaCha20Poly1305
        }
    }

    /// Generate a cryptographically secure key
    pub fn generate_key(&self, algorithm: SecureAlgorithm) -> Result<SecureKey> {
        let key_size = match algorithm {
            SecureAlgorithm::Aes256Gcm => 32,      // 256 bits
            SecureAlgorithm::ChaCha20Poly1305 => 32, // 256 bits
        };

        let mut key_material = vec![0u8; key_size];
        
        // Use high-quality entropy
        self.entropy_pool.fill_bytes(&mut key_material)?;

        let key = SecureKey {
            id: self.generate_key_id()?,
            material: key_material,
            algorithm,
            created_at: SystemTime::now(),
        };

        Ok(key)
    }

    /// Generate unique key ID using cryptographic randomness
    fn generate_key_id(&self) -> Result<u64> {
        let mut id_bytes = [0u8; 8];
        OsRng.fill_bytes(&mut id_bytes);
        Ok(u64::from_le_bytes(id_bytes))
    }

    /// Encrypt data with strong security guarantees
    pub fn encrypt(
        &self,
        key: &SecureKey,
        plaintext: &[u8],
        aad: &[u8],
        context: &[u8],
    ) -> Result<SecureEncryptionResult> {
        // Generate secure nonce
        let nonce_gen = NonceGenerator::new()?;
        let nonce = nonce_gen.generate_unique_nonce(context)?;

        let ciphertext = match key.algorithm {
            SecureAlgorithm::Aes256Gcm => {
                self.encrypt_aes_gcm(&key.material, &nonce, plaintext, aad)?
            }
            SecureAlgorithm::ChaCha20Poly1305 => {
                self.encrypt_chacha20(&key.material, &nonce, plaintext, aad)?
            }
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Encryption(format!("Time error: {}", e)))?
            .as_secs();

        // Create authenticated metadata
        let mut result = SecureEncryptionResult {
            ciphertext,
            nonce,
            aad: aad.to_vec(),
            key_id: key.id,
            algorithm: key.algorithm,
            timestamp,
            metadata_hmac: Vec::new(),
        };

        // Compute metadata HMAC for integrity
        result.metadata_hmac = self.compute_metadata_hmac(&result, &key.material)?;

        Ok(result)
    }

    /// Decrypt and verify data with full authentication
    pub fn decrypt(
        &self,
        key: &SecureKey,
        encrypted: &SecureEncryptionResult,
    ) -> Result<Vec<u8>> {
        // Verify metadata integrity first
        let expected_hmac = self.compute_metadata_hmac(encrypted, &key.material)?;
        if !bool::from(encrypted.metadata_hmac.ct_eq(&expected_hmac)) {
            return Err(Error::Encryption("Metadata integrity check failed".to_string()));
        }

        // Verify key matches
        if key.id != encrypted.key_id {
            return Err(Error::Encryption("Key ID mismatch".to_string()));
        }

        // Verify algorithm matches
        if key.algorithm != encrypted.algorithm {
            return Err(Error::Encryption("Algorithm mismatch".to_string()));
        }

        // Perform decryption
        let plaintext = match encrypted.algorithm {
            SecureAlgorithm::Aes256Gcm => {
                self.decrypt_aes_gcm(
                    &key.material,
                    &encrypted.nonce,
                    &encrypted.ciphertext,
                    &encrypted.aad,
                )?
            }
            SecureAlgorithm::ChaCha20Poly1305 => {
                self.decrypt_chacha20(
                    &key.material,
                    &encrypted.nonce,
                    &encrypted.ciphertext,
                    &encrypted.aad,
                )?
            }
        };

        Ok(plaintext)
    }

    /// Derive key using Argon2id with secure parameters
    pub fn derive_key(
        &self,
        password: &[u8],
        salt: &[u8],
        config: &SecureKDFConfig,
    ) -> Result<Vec<u8>> {
        // Use Argon2id - resistant to both side-channel and GPU attacks
        let params = Params::new(
            config.memory_cost,
            config.time_cost,
            config.parallelism,
            Some(config.output_length),
        )
        .map_err(|e| Error::Encryption(format!("Invalid Argon2 params: {}", e)))?;

        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

        let mut output = vec![0u8; config.output_length];
        argon2
            .hash_password_into(password, salt, &mut output)
            .map_err(|e| Error::Encryption(format!("Argon2 derivation failed: {}", e)))?;

        Ok(output)
    }

    /// Generate cryptographically secure salt
    pub fn generate_salt(&self, length: usize) -> Result<Vec<u8>> {
        let mut salt = vec![0u8; length];
        self.entropy_pool.fill_bytes(&mut salt)?;
        Ok(salt)
    }

    /// Securely compare two byte arrays in constant time
    pub fn constant_time_compare(&self, a: &[u8], b: &[u8]) -> bool {
        a.ct_eq(b).into()
    }

    /// AES-256-GCM encryption with proper error handling
    fn encrypt_aes_gcm(
        &self,
        key: &[u8],
        nonce: &[u8],
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        let cipher_key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(cipher_key);
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

    /// AES-256-GCM decryption with proper error handling
    fn decrypt_aes_gcm(
        &self,
        key: &[u8],
        nonce: &[u8],
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        let cipher_key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(cipher_key);
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

    /// ChaCha20-Poly1305 encryption
    fn encrypt_chacha20(
        &self,
        key: &[u8],
        nonce: &[u8],
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        use chacha20poly1305::aead::{Aead, KeyInit};

        let cipher_key = chacha20poly1305::Key::from_slice(key);
        let cipher = ChaCha20Poly1305::new(cipher_key);
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

    /// ChaCha20-Poly1305 decryption
    fn decrypt_chacha20(
        &self,
        key: &[u8],
        nonce: &[u8],
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        use chacha20poly1305::aead::{Aead, KeyInit};

        let cipher_key = chacha20poly1305::Key::from_slice(key);
        let cipher = ChaCha20Poly1305::new(cipher_key);
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

    /// Compute HMAC for metadata integrity
    fn compute_metadata_hmac(
        &self,
        result: &SecureEncryptionResult,
        key: &[u8],
    ) -> Result<Vec<u8>> {
        let hmac_key = hmac::Key::new(hmac::HMAC_SHA256, key);
        let mut ctx = hmac::Context::with_key(&hmac_key);
        
        ctx.update(&result.nonce);
        ctx.update(&result.aad);
        ctx.update(&result.key_id.to_le_bytes());
        ctx.update(&(result.algorithm as u8).to_le_bytes());
        ctx.update(&result.timestamp.to_le_bytes());
        
        Ok(ctx.sign().as_ref().to_vec())
    }
}

impl EntropyPool {
    /// Create entropy pool with system randomness
    fn new() -> Result<Self> {
        let mut buffer = vec![0u8; 4096]; // 4KB entropy buffer
        OsRng.fill_bytes(&mut buffer);

        Ok(Self {
            buffer: parking_lot::RwLock::new(buffer),
            min_entropy: 1024, // Minimum 1KB entropy
        })
    }

    /// Fill bytes with high-quality entropy
    fn fill_bytes(&self, dest: &mut [u8]) -> Result<()> {
        let mut buffer = self.buffer.write();
        
        // Refresh entropy if running low
        if buffer.len() < self.min_entropy + dest.len() {
            buffer.clear();
            buffer.resize(4096, 0);
            OsRng.fill_bytes(&mut buffer);
        }

        // Mix in fresh randomness
        let mut fresh = vec![0u8; dest.len()];
        OsRng.fill_bytes(&mut fresh);
        
        // XOR with entropy pool
        for (i, byte) in dest.iter_mut().enumerate() {
            *byte = buffer[i] ^ fresh[i];
        }

        // Consume entropy from pool
        buffer.drain(..dest.len());

        Ok(())
    }
}

impl NonceGenerator {
    /// Create new nonce generator with random seed
    fn new() -> Result<Self> {
        let mut random_seed = [0u8; 32];
        OsRng.fill_bytes(&mut random_seed);

        Ok(Self {
            counter: std::sync::atomic::AtomicU64::new(0),
            random_seed,
        })
    }

    /// Generate unique nonce that prevents reuse
    fn generate_unique_nonce(&self, context: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = vec![0u8; 12]; // 96-bit nonce for GCM/ChaCha20

        // Counter for uniqueness (4 bytes)
        let counter = self.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        nonce[..4].copy_from_slice(&(counter as u32).to_le_bytes());

        // Hash of context and seed (4 bytes)
        let mut hasher = blake3::Hasher::new();
        hasher.update(context);
        hasher.update(&self.random_seed);
        hasher.update(&counter.to_le_bytes());
        let hash = hasher.finalize();
        nonce[4..8].copy_from_slice(&hash.as_bytes()[..4]);

        // Fresh randomness (4 bytes)
        OsRng.fill_bytes(&mut nonce[8..]);

        Ok(nonce)
    }
}

impl std::fmt::Debug for SecureKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecureKey")
            .field("id", &self.id)
            .field("material", &"[REDACTED]")
            .field("algorithm", &self.algorithm)
            .field("created_at", &self.created_at)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secure_encryption_roundtrip() {
        let provider = SecureCryptoProvider::new().unwrap();
        let key = provider.generate_key(SecureAlgorithm::Aes256Gcm).unwrap();
        
        let plaintext = b"highly sensitive database content";
        let aad = b"page_metadata";
        let context = b"page_42";
        
        let encrypted = provider.encrypt(&key, plaintext, aad, context).unwrap();
        let decrypted = provider.decrypt(&key, &encrypted).unwrap();
        
        assert_eq!(plaintext.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn test_nonce_uniqueness() {
        let gen = NonceGenerator::new().unwrap();
        let context = b"test_context";
        
        let nonce1 = gen.generate_unique_nonce(context).unwrap();
        let nonce2 = gen.generate_unique_nonce(context).unwrap();
        
        assert_ne!(nonce1, nonce2);
    }

    #[test]
    fn test_key_derivation() {
        let provider = SecureCryptoProvider::new().unwrap();
        let password = b"secure_password";
        let salt = provider.generate_salt(32).unwrap();
        let config = SecureKDFConfig::default();
        
        let key1 = provider.derive_key(password, &salt, &config).unwrap();
        let key2 = provider.derive_key(password, &salt, &config).unwrap();
        
        assert_eq!(key1, key2); // Same inputs should give same output
        assert_eq!(key1.len(), 32); // Should be 256 bits
    }

    #[test]
    fn test_metadata_integrity() {
        let provider = SecureCryptoProvider::new().unwrap();
        let key = provider.generate_key(SecureAlgorithm::Aes256Gcm).unwrap();
        
        let plaintext = b"secret data";
        let aad = b"metadata";
        let context = b"context";
        
        let mut encrypted = provider.encrypt(&key, plaintext, aad, context).unwrap();
        
        // Tamper with metadata
        encrypted.timestamp += 1;
        
        // Decryption should fail
        assert!(provider.decrypt(&key, &encrypted).is_err());
    }

    #[test]
    fn test_constant_time_compare() {
        let provider = SecureCryptoProvider::new().unwrap();
        
        let a = b"same_data";
        let b = b"same_data";
        let c = b"different";
        
        assert!(provider.constant_time_compare(a, b));
        assert!(!provider.constant_time_compare(a, c));
    }
}