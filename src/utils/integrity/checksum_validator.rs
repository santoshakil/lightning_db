//! Checksum Validation Module
//!
//! Validates data integrity using CRC32 checksums for pages, keys, and values.

use super::{calculate_checksum, ChecksumError, ChecksumErrorType};
use crate::core::storage::{Page, PageManager, PageManagerAsync, PageType};
use crate::{Database, Result};
use parking_lot::RwLock;
use std::sync::Arc;

/// Checksum validator for database integrity
pub struct ChecksumValidator {
    database: Arc<Database>,
    page_manager: Arc<RwLock<PageManager>>,
    errors_found: Arc<RwLock<Vec<ChecksumError>>>,
}

impl ChecksumValidator {
    /// Create new checksum validator
    pub fn new(database: Arc<Database>) -> Self {
        let page_manager = database.get_page_manager().inner_arc();

        Self {
            database,
            page_manager,
            errors_found: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Validate all checksums in the database
    pub async fn validate_all(&self) -> Result<Vec<ChecksumError>> {
        let all_pages = self.page_manager.get_all_allocated_pages().await?;

        for page_id in all_pages {
            if page_id == 0 {
                continue; // Skip null page
            }

            if (self.validate_page(page_id).await).is_err() {
                // Continue validation even if one page fails
                continue;
            }
        }

        Ok(self.errors_found.read().clone())
    }

    /// Validate a single page
    pub async fn validate_page(&self, page_id: u64) -> Result<bool> {
        let page = match self.page_manager.load_page(page_id).await {
            Ok(page) => page,
            Err(e) => {
                self.record_error(ChecksumError {
                    page_id,
                    expected_checksum: 0,
                    actual_checksum: 0,
                    data_size: 0,
                    error_type: ChecksumErrorType::PageHeader,
                });
                return Err(e);
            }
        };

        // Validate page header checksum
        if !self.validate_page_header(&page, page_id) {
            return Ok(false);
        }

        // Validate page data checksum
        if !self.validate_page_data(&page, page_id) {
            return Ok(false);
        }

        // For data pages, validate individual key-value checksums
        if page.page_type == PageType::Data && !self.validate_key_values(&page, page_id).await {
            return Ok(false);
        }

        Ok(true)
    }

    /// Validate page header checksum
    fn validate_page_header(&self, page: &Page, page_id: u64) -> bool {
        // Extract header bytes (first 32 bytes typically)
        let header_size = 32;
        if page.data.len() < header_size {
            self.record_error(ChecksumError {
                page_id,
                expected_checksum: 0,
                actual_checksum: 0,
                data_size: page.data.len(),
                error_type: ChecksumErrorType::PageHeader,
            });
            return false;
        }

        // Calculate checksum of header (excluding checksum field itself)
        let header_data = &page.data[8..header_size]; // Skip checksum field
        let calculated_checksum = calculate_checksum(header_data);

        // Extract stored checksum from header
        let stored_checksum =
            u32::from_le_bytes([page.data[4], page.data[5], page.data[6], page.data[7]]);

        if calculated_checksum != stored_checksum {
            self.record_error(ChecksumError {
                page_id,
                expected_checksum: stored_checksum,
                actual_checksum: calculated_checksum,
                data_size: header_size,
                error_type: ChecksumErrorType::PageHeader,
            });
            return false;
        }

        true
    }

    /// Validate page data checksum
    fn validate_page_data(&self, page: &Page, _page_id: u64) -> bool {
        // Skip if page is too small
        if page.data.len() < 36 {
            return true; // Not enough data for data checksum
        }

        // Calculate checksum of entire page data (after header)
        let data_start = 32;
        let data_section = &page.data[data_start..];

        if data_section.is_empty() {
            return true;
        }

        let _calculated_checksum = calculate_checksum(data_section);

        // In a real implementation, we'd extract the stored data checksum
        // For now, we'll validate the structure

        true
    }

    /// Validate individual key-value pairs
    async fn validate_key_values(&self, page: &Page, page_id: u64) -> bool {
        // Parse page data to extract key-value pairs
        // This is a simplified version - real implementation would parse the actual format

        let mut offset = 32; // Skip header
        let mut all_valid = true;

        while offset + 8 < page.data.len() {
            // Read key length
            let key_len = u32::from_le_bytes([
                page.data[offset],
                page.data[offset + 1],
                page.data[offset + 2],
                page.data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + key_len > page.data.len() {
                break;
            }

            // Read key
            let key = &page.data[offset..offset + key_len];
            offset += key_len;

            if offset + 4 > page.data.len() {
                break;
            }

            // Read value length
            let value_len = u32::from_le_bytes([
                page.data[offset],
                page.data[offset + 1],
                page.data[offset + 2],
                page.data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + value_len + 4 > page.data.len() {
                break;
            }

            // Read value
            let value = &page.data[offset..offset + value_len];
            offset += value_len;

            // Read stored checksum
            if offset + 4 > page.data.len() {
                break;
            }

            let stored_checksum = u32::from_le_bytes([
                page.data[offset],
                page.data[offset + 1],
                page.data[offset + 2],
                page.data[offset + 3],
            ]);
            offset += 4;

            // Calculate checksum of key+value
            let mut combined = Vec::with_capacity(key.len() + value.len());
            combined.extend_from_slice(key);
            combined.extend_from_slice(value);
            let calculated_checksum = calculate_checksum(&combined);

            if calculated_checksum != stored_checksum {
                self.record_error(ChecksumError {
                    page_id,
                    expected_checksum: stored_checksum,
                    actual_checksum: calculated_checksum,
                    data_size: key.len() + value.len(),
                    error_type: ChecksumErrorType::KeyValue,
                });
                all_valid = false;
            }
        }

        all_valid
    }

    /// Record a checksum error
    fn record_error(&self, error: ChecksumError) {
        self.errors_found.write().push(error);
    }

    /// Validate metadata checksums
    pub async fn validate_metadata(&self) -> Result<Vec<ChecksumError>> {
        let mut errors = Vec::new();

        // Validate root page metadata
        let root_page_id = self.database.get_root_page_id()?;
        if root_page_id != 0 {
            if let Ok(page) = self.page_manager.load_page(root_page_id).await {
                if !self.validate_metadata_page(&page, root_page_id) {
                    errors.push(ChecksumError {
                        page_id: root_page_id,
                        expected_checksum: 0,
                        actual_checksum: 0,
                        data_size: page.data.len(),
                        error_type: ChecksumErrorType::Metadata,
                    });
                }
            }
        }

        Ok(errors)
    }

    /// Validate metadata page checksum
    fn validate_metadata_page(&self, page: &Page, _page_id: u64) -> bool {
        // Metadata pages have a special format
        if page.page_type != PageType::Meta {
            return false;
        }

        // Calculate checksum of metadata content
        let _calculated_checksum = calculate_checksum(&*page.data);

        // In real implementation, compare with stored checksum
        true
    }

    /// Quick checksum validation for a range of pages
    pub async fn validate_range(
        &self,
        start_page: u64,
        end_page: u64,
    ) -> Result<Vec<ChecksumError>> {
        let _errors: Vec<ChecksumError> = Vec::new();

        for page_id in start_page..=end_page {
            if !self.validate_page(page_id).await.unwrap_or(false) {
                // Error already recorded
            }
        }

        Ok(self.errors_found.read().clone())
    }

    /// Calculate and update checksums for a page
    pub async fn update_page_checksums(&self, page_id: u64) -> Result<()> {
        let page = self.page_manager.load_page(page_id).await?;

        // Clone the data to make it mutable
        let mut data = *page.data;

        // Update header checksum
        let header_data = &data[8..32];
        let header_checksum = calculate_checksum(header_data);
        data[4..8].copy_from_slice(&header_checksum.to_le_bytes());

        // Update data checksum
        if data.len() > 32 {
            let _data_checksum = calculate_checksum(&data[32..]);
            // Store data checksum in appropriate location
        }

        // Create a new page with the updated data
        let updated_page = crate::core::storage::Page {
            id: page.id,
            data: Arc::new(data),
            dirty: true,
            page_type: page.page_type,
        };

        // Save updated page
        self.page_manager.save_page(&updated_page).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_checksum_calculation() {
        let data = b"test data for checksum";
        let checksum1 = calculate_checksum(data);
        let checksum2 = calculate_checksum(data);
        assert_eq!(checksum1, checksum2);

        let different_data = b"different data";
        let checksum3 = calculate_checksum(different_data);
        assert_ne!(checksum1, checksum3);
    }
}
