use super::{Page, PageType, MAGIC, PAGE_SIZE};
use crate::core::error::{Error, Result};
use crate::utils::integrity::{
    data_integrity::{validators, DataIntegrityValidator},
    error_types::{IntegrityError, ValidationResult, ViolationSeverity},
    validation_config::{IntegrityConfig, OperationType, ValidationContext},
};
use std::sync::Arc;

/// Enhanced Page with integrated integrity validation
pub struct ValidatedPage {
    inner: Page,
    validator: Arc<DataIntegrityValidator>,
}

impl ValidatedPage {
    /// Create a new validated page
    pub fn new(id: u32, config: IntegrityConfig) -> Self {
        Self {
            inner: Page::new(id),
            validator: Arc::new(DataIntegrityValidator::new(config)),
        }
    }

    /// Create from existing page with validation
    pub fn from_page(page: Page, config: IntegrityConfig) -> Result<Self> {
        let validator = Arc::new(DataIntegrityValidator::new(config));

        // Validate the existing page
        validator.validate_critical_path(
            "page_creation",
            &format!("page_{}", page.id),
            OperationType::Read,
            |context| validate_page_structure(&page, context),
        )?;

        Ok(Self {
            inner: page,
            validator,
        })
    }

    /// Get the underlying page (read-only)
    pub fn inner(&self) -> &Page {
        &self.inner
    }

    /// Get mutable access to page data with integrity validation
    pub fn get_mut_data_validated(&mut self) -> Result<&mut [u8; PAGE_SIZE]> {
        // Pre-validation before allowing mutation
        self.validator.validate_critical_path(
            "page_mutation_prep",
            &format!("page_{}", self.inner.id),
            OperationType::Write,
            |context| validate_page_mutation_safety(&self.inner, context),
        )?;

        Ok(self.inner.get_mut_data())
    }

    /// Write page to disk with comprehensive validation
    pub fn write_to_disk_validated<F>(&mut self, write_fn: F) -> Result<()>
    where
        F: FnOnce(&Page) -> Result<()>,
    {
        // Pre-write validation
        self.validator.validate_critical_path(
            "page_pre_write_validation",
            &format!("page_{}", self.inner.id),
            OperationType::Write,
            |context| validate_page_before_write(&self.inner, context),
        )?;

        // Update checksum before write
        self.update_checksum()?;

        // Perform the write operation
        write_fn(&self.inner)?;

        // Post-write verification
        self.validator.validate_critical_path(
            "page_post_write_verification",
            &format!("page_{}", self.inner.id),
            OperationType::Write,
            |context| validate_page_write_success(&self.inner, context),
        )?;

        Ok(())
    }

    /// Read page from disk with integrity validation
    pub fn read_from_disk_validated<F>(id: u32, config: IntegrityConfig, read_fn: F) -> Result<Self>
    where
        F: FnOnce(u32) -> Result<Page>,
    {
        let validator = Arc::new(DataIntegrityValidator::new(config.clone()));

        // Read the page
        let page = read_fn(id)?;

        // Post-read validation
        validator.validate_critical_path(
            "page_post_read_validation",
            &format!("page_{}", id),
            OperationType::Read,
            |context| validate_page_after_read(&page, context),
        )?;

        Ok(Self {
            inner: page,
            validator,
        })
    }

    /// Update page checksum
    fn update_checksum(&mut self) -> Result<()> {
        let checksum = self.inner.calculate_checksum();
        let data = self.inner.get_mut_data();
        data[12..16].copy_from_slice(&checksum.to_le_bytes());
        Ok(())
    }

    /// Verify page integrity
    pub fn verify_integrity(&self) -> Result<()> {
        self.validator.validate_critical_path(
            "page_integrity_verification",
            &format!("page_{}", self.inner.id),
            OperationType::Read,
            |context| validate_page_comprehensive(&self.inner, context),
        )
    }
}

impl std::ops::Deref for ValidatedPage {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Validate page structure and basic integrity
fn validate_page_structure(page: &Page, _context: &ValidationContext) -> ValidationResult<()> {
    let data = page.get_data();
    let location = format!("page_{}", page.id);

    // Validate magic number
    let magic_result = validators::validate_magic_number(data, MAGIC, &location);
    if !magic_result.is_valid() {
        return magic_result;
    }

    // Validate page size
    if data.len() != PAGE_SIZE {
        let violation = crate::utils::integrity::error_types::create_critical_violation(
            IntegrityError::SizeViolation {
                expected: PAGE_SIZE,
                actual: data.len(),
                location: location.clone(),
            },
            &location,
            "Page size is invalid - possible corruption",
        );
        return ValidationResult::Invalid(vec![violation]);
    }

    // Validate page type
    if data.len() >= 12 {
        let page_type_byte = data[8];
        if PageType::from_byte(page_type_byte).is_none() {
            let violation = crate::utils::integrity::error_types::create_violation(
                IntegrityError::PageTypeViolation {
                    page_id: page.id,
                    expected: "valid PageType".to_string(),
                    found: format!("invalid byte: {}", page_type_byte),
                },
                ViolationSeverity::Error,
                &location,
                None,
            );
            return ValidationResult::Invalid(vec![violation]);
        }
    }

    ValidationResult::Valid(())
}

/// Validate page before write operation
fn validate_page_before_write(page: &Page, _context: &ValidationContext) -> ValidationResult<()> {
    let location = format!("page_{}_pre_write", page.id);

    // Validate basic structure first
    let structure_result = validate_page_structure(page, _context);
    if !structure_result.is_valid() {
        return structure_result;
    }

    // Additional pre-write validations
    let data = page.get_data();

    // Ensure page is properly initialized
    if page.id == 0 && data.iter().all(|&b| b == 0) {
        let violation = crate::utils::integrity::error_types::create_violation(
            IntegrityError::StructuralViolation {
                structure: "header_page".to_string(),
                violation: "Header page appears uninitialized".to_string(),
            },
            ViolationSeverity::Error,
            &location,
            None,
        );
        return ValidationResult::Invalid(vec![violation]);
    }

    ValidationResult::Valid(())
}

/// Validate page after read operation
fn validate_page_after_read(page: &Page, _context: &ValidationContext) -> ValidationResult<()> {
    let location = format!("page_{}_post_read", page.id);

    // Validate structure
    let structure_result = validate_page_structure(page, _context);
    if !structure_result.is_valid() {
        return structure_result;
    }

    // Validate checksum if not a special page
    if page.id > 0 && !page.get_data().iter().all(|&b| b == 0) {
        let checksum_result = validate_page_checksum(page, &location);
        if !checksum_result.is_valid() {
            return checksum_result;
        }
    }

    ValidationResult::Valid(())
}

/// Validate page checksum
fn validate_page_checksum(page: &Page, location: &str) -> ValidationResult<()> {
    let data = page.get_data();

    if data.len() < 16 {
        let violation = crate::utils::integrity::error_types::create_critical_violation(
            IntegrityError::SizeViolation {
                expected: 16,
                actual: data.len(),
                location: location.to_string(),
            },
            location,
            "Page too small for checksum validation",
        );
        return ValidationResult::Invalid(vec![violation]);
    }

    let stored_checksum = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
    let _computed_checksum = {
        use crc32fast::Hasher;
        let mut hasher = Hasher::new();
        hasher.update(&data[16..]);
        hasher.finalize()
    };

    validators::validate_checksum(&data[16..], stored_checksum, location)
}

/// Validate it's safe to mutate the page
fn validate_page_mutation_safety(
    page: &Page,
    _context: &ValidationContext,
) -> ValidationResult<()> {
    let _location = format!("page_{}_mutation_safety", page.id);

    // Check if page is in a valid state for mutation
    if page.id == 0 {
        // Header page - extra caution needed
        tracing::warn!("Mutating header page {} - ensure this is intended", page.id);
    }

    // Validate current state is consistent
    validate_page_structure(page, _context)
}

/// Validate write operation succeeded
fn validate_page_write_success(page: &Page, _context: &ValidationContext) -> ValidationResult<()> {
    let location = format!("page_{}_write_success", page.id);

    // Re-validate the page structure after write
    let structure_result = validate_page_structure(page, _context);
    if !structure_result.is_valid() {
        return structure_result;
    }

    // Verify checksum is valid
    if page.id > 0 && !page.get_data().iter().all(|&b| b == 0) {
        validate_page_checksum(page, &location)
    } else {
        ValidationResult::Valid(())
    }
}

/// Comprehensive page validation
fn validate_page_comprehensive(page: &Page, context: &ValidationContext) -> ValidationResult<()> {
    let mut violations = Vec::new();

    // Basic structure validation
    match validate_page_structure(page, context) {
        ValidationResult::Invalid(mut v) => violations.append(&mut v),
        ValidationResult::Error(e) => return ValidationResult::Error(e),
        ValidationResult::Valid(_) => {}
    }

    // Checksum validation
    if page.id > 0 && !page.get_data().iter().all(|&b| b == 0) {
        let location = format!("page_{}_comprehensive", page.id);
        match validate_page_checksum(page, &location) {
            ValidationResult::Invalid(mut v) => violations.append(&mut v),
            ValidationResult::Error(e) => return ValidationResult::Error(e),
            ValidationResult::Valid(_) => {}
        }
    }

    // Advanced structural validations
    let data = page.get_data();
    let location = format!("page_{}_advanced", page.id);

    // Validate version field if present
    if data.len() >= 8 {
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        if version == 0 || version > 1000 {
            // Reasonable version range
            violations.push(crate::utils::integrity::error_types::create_violation(
                IntegrityError::RangeViolation {
                    value: version as u64,
                    min: 1,
                    max: 1000,
                    field: "page_version".to_string(),
                },
                ViolationSeverity::Warning,
                &location,
                None,
            ));
        }
    }

    // Check for data alignment issues
    let data_ptr = data.as_ptr() as usize;
    if data_ptr % 8 != 0 {
        // Check 8-byte alignment
        violations.push(crate::utils::integrity::error_types::create_violation(
            IntegrityError::AlignmentViolation {
                address: data_ptr,
                required_alignment: 8,
            },
            ViolationSeverity::Warning,
            &location,
            None,
        ));
    }

    if violations.is_empty() {
        ValidationResult::Valid(())
    } else {
        ValidationResult::Invalid(violations)
    }
}

/// Page integrity validation utilities
pub mod page_validators {
    use super::*;

    /// Validate a batch of pages
    pub fn validate_page_batch(
        pages: &[&Page],
        config: &IntegrityConfig,
    ) -> Result<Vec<ValidationResult<()>>> {
        let validator = DataIntegrityValidator::new(config.clone());
        let mut results = Vec::new();

        for page in pages {
            let result = validator.validate_critical_path(
                "batch_page_validation",
                &format!("page_{}", page.id),
                OperationType::Read,
                |context| validate_page_comprehensive(page, context),
            );

            results.push(match result {
                Ok(_) => ValidationResult::Valid(()),
                Err(e) => ValidationResult::Error(IntegrityError::StructuralViolation {
                    structure: "page".to_string(),
                    violation: e.to_string(),
                }),
            });
        }

        Ok(results)
    }

    /// Quick validation for frequently accessed pages
    pub fn quick_validate_page(page: &Page) -> ValidationResult<()> {
        let data = page.get_data();
        let location = format!("page_{}_quick", page.id);

        // Just check magic number and basic size
        if data.len() != PAGE_SIZE {
            let violation = crate::utils::integrity::error_types::create_critical_violation(
                IntegrityError::SizeViolation {
                    expected: PAGE_SIZE,
                    actual: data.len(),
                    location: location.clone(),
                },
                &location,
                "Invalid page size",
            );
            return ValidationResult::Invalid(vec![violation]);
        }

        validators::validate_magic_number(data, MAGIC, &location)
    }

    /// Deep validation with extensive checks
    pub fn deep_validate_page(
        page: &Page,
        config: &IntegrityConfig,
    ) -> Result<ValidationResult<()>> {
        let validator = DataIntegrityValidator::new(config.clone());

        validator
            .validate_critical_path(
                "deep_page_validation",
                &format!("page_{}", page.id),
                OperationType::Read,
                |context| {
                    // Run comprehensive validation
                    let result = validate_page_comprehensive(page, context);

                    // Add additional deep checks
                    if let ValidationResult::Valid(_) = result {
                        deep_validate_page_content(page, context)
                    } else {
                        result
                    }
                },
            )
            .map_err(|_| Error::InvalidArgument("Deep validation failed".to_string()))?;

        Ok(ValidationResult::Valid(()))
    }

    /// Deep validation of page content structure
    fn deep_validate_page_content(
        page: &Page,
        _context: &ValidationContext,
    ) -> ValidationResult<()> {
        let data = page.get_data();
        let location = format!("page_{}_deep_content", page.id);

        // Validate internal structure based on page type
        if data.len() >= 12 {
            if let Some(page_type) = PageType::from_byte(data[8]) {
                match page_type {
                    PageType::Data => validate_btree_page_structure(page, &location),
                    PageType::Meta => validate_header_page_structure(page, &location),
                    _ => ValidationResult::Valid(()),
                }
            } else {
                ValidationResult::Valid(())
            }
        } else {
            ValidationResult::Valid(())
        }
    }

    /// Validate B-tree page internal structure
    fn validate_btree_page_structure(_page: &Page, _location: &str) -> ValidationResult<()> {
        // Validation implementation pending storage module integration B-tree specific validations
        // - Key ordering
        // - Child pointer validity
        // - Entry count consistency
        ValidationResult::Valid(())
    }

    /// Validate header page structure
    fn validate_header_page_structure(_page: &Page, _location: &str) -> ValidationResult<()> {
        // Validation implementation pending storage module integration header page specific validations
        // - Database metadata consistency
        // - Version compatibility
        ValidationResult::Valid(())
    }

    /// Validate LSM page structure
    fn validate_lsm_page_structure(_page: &Page, _location: &str) -> ValidationResult<()> {
        // Validation implementation pending storage module integration LSM specific validations
        // - Key range consistency
        // - Compression integrity
        ValidationResult::Valid(())
    }
}
