use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumType {
    CRC32,
    XXHash64,
    XXHash3,
    Blake3,
}

impl Default for ChecksumType {
    fn default() -> Self {
        ChecksumType::XXHash64
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChecksumError {
    pub page_id: u64,
    pub expected: u64,
    pub actual: u64,
    pub checksum_type: ChecksumType,
    pub data_size: usize,
    pub timestamp: std::time::SystemTime,
}

#[derive(Debug, Clone)]
pub struct ChecksumEntry {
    pub checksum: u64,
    pub algorithm: ChecksumType,
    pub data_size: usize,
    pub created_at: std::time::SystemTime,
}

pub struct ChecksumManager {
    algorithm: ChecksumType,
    checksums: Arc<RwLock<HashMap<u64, ChecksumEntry>>>,
}

impl ChecksumManager {
    pub fn new(algorithm: ChecksumType) -> Self {
        Self {
            algorithm,
            checksums: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn calculate_checksum(&self, data: &[u8]) -> u64 {
        match self.algorithm {
            ChecksumType::CRC32 => {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(data);
                hasher.finalize() as u64
            }
            ChecksumType::XXHash64 => xxhash_rust::xxh64::xxh64(data, 0),
            ChecksumType::XXHash3 => xxhash_rust::xxh3::xxh3_64(data),
            ChecksumType::Blake3 => {
                let hash = blake3::hash(data);
                let hash_bytes = hash.as_bytes();
                u64::from_le_bytes([
                    hash_bytes[0],
                    hash_bytes[1],
                    hash_bytes[2],
                    hash_bytes[3],
                    hash_bytes[4],
                    hash_bytes[5],
                    hash_bytes[6],
                    hash_bytes[7],
                ])
            }
        }
    }

    pub async fn store_checksum(&self, page_id: u64, data: &[u8]) -> Result<u64> {
        let checksum = self.calculate_checksum(data).await;

        let entry = ChecksumEntry {
            checksum,
            algorithm: self.algorithm,
            data_size: data.len(),
            created_at: std::time::SystemTime::now(),
        };

        self.checksums.write().await.insert(page_id, entry);
        Ok(checksum)
    }

    pub async fn verify_checksum(&self, page_id: u64, data: &[u8]) -> Result<bool> {
        let checksums = self.checksums.read().await;

        if let Some(entry) = checksums.get(&page_id) {
            let current_checksum = self.calculate_checksum(data).await;
            Ok(current_checksum == entry.checksum)
        } else {
            // No stored checksum, calculate and store new one
            drop(checksums);
            self.store_checksum(page_id, data).await?;
            Ok(true)
        }
    }

    pub async fn verify_and_report(
        &self,
        page_id: u64,
        data: &[u8],
    ) -> Result<Option<ChecksumError>> {
        let checksums = self.checksums.read().await;

        if let Some(entry) = checksums.get(&page_id) {
            let current_checksum = self.calculate_checksum(data).await;

            if current_checksum != entry.checksum {
                return Ok(Some(ChecksumError {
                    page_id,
                    expected: entry.checksum,
                    actual: current_checksum,
                    checksum_type: entry.algorithm,
                    data_size: data.len(),
                    timestamp: std::time::SystemTime::now(),
                }));
            }
        }

        Ok(None)
    }

    pub async fn remove_checksum(&self, page_id: u64) -> Result<()> {
        self.checksums.write().await.remove(&page_id);
        Ok(())
    }

    pub async fn get_checksum(&self, page_id: u64) -> Option<ChecksumEntry> {
        self.checksums.read().await.get(&page_id).cloned()
    }

    pub async fn bulk_verify(&self, pages: &[(u64, Vec<u8>)]) -> Result<Vec<ChecksumError>> {
        let mut errors = Vec::new();

        for (page_id, data) in pages {
            if let Some(error) = self.verify_and_report(*page_id, data).await? {
                errors.push(error);
            }
        }

        Ok(errors)
    }

    pub async fn update_checksum(&self, page_id: u64, data: &[u8]) -> Result<()> {
        self.store_checksum(page_id, data).await?;
        Ok(())
    }

    pub async fn validate_page_header(&self, _page_id: u64, header_data: &[u8]) -> Result<bool> {
        // Validate page header checksum separately
        let _header_checksum = self.calculate_checksum(header_data).await;

        // Page header should include its own checksum at a known offset
        if header_data.len() >= 8 {
            let stored_checksum = u64::from_le_bytes([
                header_data[0],
                header_data[1],
                header_data[2],
                header_data[3],
                header_data[4],
                header_data[5],
                header_data[6],
                header_data[7],
            ]);

            // Calculate checksum excluding the stored checksum bytes
            let data_without_checksum = &header_data[8..];
            let calculated_checksum = self.calculate_checksum(data_without_checksum).await;

            return Ok(stored_checksum == calculated_checksum);
        }

        Ok(false)
    }

    pub async fn repair_checksum(&self, page_id: u64, data: &[u8]) -> Result<()> {
        self.store_checksum(page_id, data).await?;
        Ok(())
    }

    pub async fn get_algorithm(&self) -> ChecksumType {
        self.algorithm
    }

    pub async fn get_stored_checksums_count(&self) -> usize {
        self.checksums.read().await.len()
    }

    pub async fn clear_all_checksums(&self) -> Result<()> {
        self.checksums.write().await.clear();
        Ok(())
    }

    pub async fn export_checksums(&self) -> HashMap<u64, ChecksumEntry> {
        self.checksums.read().await.clone()
    }

    pub async fn import_checksums(&self, checksums: HashMap<u64, ChecksumEntry>) -> Result<()> {
        *self.checksums.write().await = checksums;
        Ok(())
    }

    pub fn fast_checksum(data: &[u8]) -> u32 {
        // Fast checksum for quick validation
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    pub fn secure_checksum(data: &[u8]) -> [u8; 32] {
        // Secure checksum using Blake3
        blake3::hash(data).into()
    }

    pub async fn batch_calculate(&self, data_chunks: &[&[u8]]) -> Vec<u64> {
        let mut checksums = Vec::with_capacity(data_chunks.len());

        for chunk in data_chunks {
            checksums.push(self.calculate_checksum(chunk).await);
        }

        checksums
    }

    pub async fn verify_key_value_pair(&self, key: &[u8], value: &[u8]) -> u64 {
        // Combined checksum for key-value pairs
        let mut combined = Vec::with_capacity(key.len() + value.len());
        combined.extend_from_slice(key);
        combined.extend_from_slice(value);
        self.calculate_checksum(&combined).await
    }

    pub async fn calculate_incremental_checksum(
        &self,
        previous_checksum: u64,
        new_data: &[u8],
    ) -> u64 {
        // For algorithms that support incremental updates
        match self.algorithm {
            ChecksumType::CRC32 => {
                // CRC32 supports incremental updates
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&previous_checksum.to_le_bytes());
                hasher.update(new_data);
                hasher.finalize() as u64
            }
            _ => {
                // For others, just calculate normally
                self.calculate_checksum(new_data).await
            }
        }
    }
}
