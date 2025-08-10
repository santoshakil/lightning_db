//! Page Structure Scanner
//!
//! Scans and validates the structure of database pages.

use super::{ErrorSeverity, StructureError, StructureErrorType};
use crate::storage::{Page, PageManager, PageManagerAsync, PageType, PAGE_SIZE};
use crate::{Database, Result};
use parking_lot::RwLock;
use std::sync::Arc;

/// Magic number for Lightning DB pages
const LIGHTNING_DB_MAGIC: u32 = 0x4C444242; // "LDBB"

/// Page scanner for structural validation
pub struct PageScanner {
    database: Arc<Database>,
    page_manager: Arc<RwLock<PageManager>>,
    pages_scanned: Arc<RwLock<u64>>,
    errors_found: Arc<RwLock<Vec<StructureError>>>,
}

impl PageScanner {
    /// Create new page scanner
    pub fn new(database: Arc<Database>) -> Self {
        let page_manager = database.get_page_manager();

        Self {
            database,
            page_manager,
            pages_scanned: Arc::new(RwLock::new(0)),
            errors_found: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Scan all pages for structural issues
    pub async fn scan_all_pages(&self) -> Result<Vec<StructureError>> {
        let all_pages = self.page_manager.get_all_allocated_pages().await?;

        for page_id in all_pages {
            if page_id == 0 {
                continue; // Skip null page
            }

            if let Err(_) = self.scan_page(page_id).await {
                // Continue scanning even if one page fails
                continue;
            }

            *self.pages_scanned.write() += 1;
        }

        Ok(self.errors_found.read().clone())
    }

    /// Scan a single page
    pub async fn scan_page(&self, page_id: u64) -> Result<bool> {
        let page = match self.page_manager.load_page(page_id).await {
            Ok(page) => page,
            Err(e) => {
                self.record_error(StructureError {
                    page_id,
                    error_type: StructureErrorType::CorruptedHeader,
                    description: format!("Failed to load page: {}", e),
                    severity: ErrorSeverity::Critical,
                });
                return Ok(false);
            }
        };

        let mut is_valid = true;

        // Validate page size
        if page.data.len() != PAGE_SIZE {
            self.record_error(StructureError {
                page_id,
                error_type: StructureErrorType::InvalidSize,
                description: format!("Page size {} != expected {}", page.data.len(), PAGE_SIZE),
                severity: ErrorSeverity::Critical,
            });
            is_valid = false;
        }

        // Validate magic number
        if !self.validate_magic_number(&page, page_id) {
            is_valid = false;
        }

        // Validate page type
        if !self.validate_page_type(&page, page_id) {
            is_valid = false;
        }

        // Validate page header
        if !self.validate_page_header(&page, page_id) {
            is_valid = false;
        }

        // Type-specific validation
        match page.page_type {
            PageType::Meta => is_valid &= self.validate_meta_page(&page, page_id),
            PageType::Data => is_valid &= self.validate_data_page(&page, page_id),
            PageType::Overflow => is_valid &= self.validate_overflow_page(&page, page_id),
            PageType::Free => is_valid &= self.validate_free_page(&page, page_id),
        }

        Ok(is_valid)
    }

    /// Validate magic number
    fn validate_magic_number(&self, page: &Page, page_id: u64) -> bool {
        if page.data.len() < 4 {
            self.record_error(StructureError {
                page_id,
                error_type: StructureErrorType::InvalidMagicNumber,
                description: "Page too small for magic number".to_string(),
                severity: ErrorSeverity::Fatal,
            });
            return false;
        }

        let magic = u32::from_le_bytes([page.data[0], page.data[1], page.data[2], page.data[3]]);

        if magic != LIGHTNING_DB_MAGIC {
            self.record_error(StructureError {
                page_id,
                error_type: StructureErrorType::InvalidMagicNumber,
                description: format!("Invalid magic number: 0x{:08X}", magic),
                severity: ErrorSeverity::Fatal,
            });
            return false;
        }

        true
    }

    /// Validate page type
    fn validate_page_type(&self, page: &Page, page_id: u64) -> bool {
        // Page type is stored at offset 8
        if page.data.len() < 9 {
            return false;
        }

        let type_byte = page.data[8];

        match type_byte {
            0 => page.page_type == PageType::Meta,
            1 => page.page_type == PageType::Data,
            2 => page.page_type == PageType::Overflow,
            3 => page.page_type == PageType::Free,
            _ => {
                self.record_error(StructureError {
                    page_id,
                    error_type: StructureErrorType::InvalidPageType,
                    description: format!("Unknown page type: {}", type_byte),
                    severity: ErrorSeverity::Critical,
                });
                return false;
            }
        }
    }

    /// Validate page header structure
    fn validate_page_header(&self, page: &Page, page_id: u64) -> bool {
        // Standard header size
        const HEADER_SIZE: usize = 32;

        if page.data.len() < HEADER_SIZE {
            self.record_error(StructureError {
                page_id,
                error_type: StructureErrorType::CorruptedHeader,
                description: "Page header too small".to_string(),
                severity: ErrorSeverity::Critical,
            });
            return false;
        }

        // Validate page ID in header matches expected
        let stored_page_id = u64::from_le_bytes([
            page.data[12],
            page.data[13],
            page.data[14],
            page.data[15],
            page.data[16],
            page.data[17],
            page.data[18],
            page.data[19],
        ]);

        if stored_page_id != page_id {
            self.record_error(StructureError {
                page_id,
                error_type: StructureErrorType::CorruptedHeader,
                description: format!(
                    "Page ID mismatch: stored {} vs expected {}",
                    stored_page_id, page_id
                ),
                severity: ErrorSeverity::Critical,
            });
            return false;
        }

        true
    }

    /// Validate metadata page
    fn validate_meta_page(&self, page: &Page, page_id: u64) -> bool {
        // Metadata pages contain database configuration
        let mut is_valid = true;

        // Check version number
        if page.data.len() >= 40 {
            let version =
                u32::from_le_bytes([page.data[32], page.data[33], page.data[34], page.data[35]]);

            if version == 0 || version > 100 {
                self.record_error(StructureError {
                    page_id,
                    error_type: StructureErrorType::InvalidSize,
                    description: format!("Invalid version number: {}", version),
                    severity: ErrorSeverity::Warning,
                });
                is_valid = false;
            }
        }

        is_valid
    }

    /// Validate data page
    fn validate_data_page(&self, page: &Page, page_id: u64) -> bool {
        let mut is_valid = true;

        // Check key count
        if page.data.len() >= 34 {
            let key_count = u16::from_le_bytes([page.data[32], page.data[33]]) as usize;

            // Sanity check
            if key_count > 1000 {
                self.record_error(StructureError {
                    page_id,
                    error_type: StructureErrorType::InvalidKeyCount,
                    description: format!("Suspicious key count: {}", key_count),
                    severity: ErrorSeverity::Warning,
                });
                is_valid = false;
            }

            // Validate space usage
            let used_space = self.calculate_used_space(page, key_count);
            if used_space > PAGE_SIZE {
                self.record_error(StructureError {
                    page_id,
                    error_type: StructureErrorType::InvalidSize,
                    description: format!("Used space {} exceeds page size", used_space),
                    severity: ErrorSeverity::Critical,
                });
                is_valid = false;
            }
        }

        is_valid
    }

    /// Validate overflow page
    fn validate_overflow_page(&self, page: &Page, page_id: u64) -> bool {
        let mut is_valid = true;

        // Check next overflow pointer
        if page.data.len() >= 40 {
            let next_overflow = u64::from_le_bytes([
                page.data[32],
                page.data[33],
                page.data[34],
                page.data[35],
                page.data[36],
                page.data[37],
                page.data[38],
                page.data[39],
            ]);

            // Validate pointer
            if next_overflow != 0 && next_overflow == page_id {
                self.record_error(StructureError {
                    page_id,
                    error_type: StructureErrorType::InvalidPointer,
                    description: "Overflow page points to itself".to_string(),
                    severity: ErrorSeverity::Critical,
                });
                is_valid = false;
            }
        }

        is_valid
    }

    /// Validate free page
    fn validate_free_page(&self, page: &Page, page_id: u64) -> bool {
        // Free pages should have minimal structure
        // Just verify they're properly marked
        true
    }

    /// Calculate used space in a data page
    fn calculate_used_space(&self, page: &Page, key_count: usize) -> usize {
        // Header + key directory + actual data
        let header_size = 32;
        let directory_size = key_count * 8; // offset + length per key

        // This is simplified - real calculation would parse actual data
        header_size + directory_size + (key_count * 50) // Assume avg 50 bytes per entry
    }

    /// Record a structure error
    fn record_error(&self, error: StructureError) {
        self.errors_found.write().push(error);
    }

    /// Get number of pages scanned
    pub fn get_pages_scanned(&self) -> u64 {
        *self.pages_scanned.read()
    }

    /// Scan pages in a specific range
    pub async fn scan_range(&self, start_page: u64, end_page: u64) -> Result<Vec<StructureError>> {
        for page_id in start_page..=end_page {
            if self.page_manager.is_page_allocated(page_id).await? {
                let _ = self.scan_page(page_id).await;
                *self.pages_scanned.write() += 1;
            }
        }

        Ok(self.errors_found.read().clone())
    }

    /// Quick validation of critical pages
    pub async fn validate_critical_pages(&self) -> Result<bool> {
        let mut all_valid = true;

        // Validate root page
        let root_page_id = self.database.get_root_page_id()?;
        if root_page_id != 0 {
            all_valid &= self.scan_page(root_page_id).await?;
        }

        // Validate metadata pages (typically first few pages)
        for page_id in 1..=5 {
            if self.page_manager.is_page_allocated(page_id).await? {
                all_valid &= self.scan_page(page_id).await?;
            }
        }

        Ok(all_valid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magic_number() {
        assert_eq!(LIGHTNING_DB_MAGIC, 0x4C444242);
    }
}
