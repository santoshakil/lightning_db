use lightning_db::integrity::{
    data_integrity::{DataIntegrityValidator, validators},
    error_types::{IntegrityError, ValidationResult, ViolationSeverity},
    validation_config::{IntegrityConfig, ValidationLevel, OperationType, ChecksumAlgorithm, ValidationContext},
    metrics::IntegrityMetrics,
};
use lightning_db::storage::{Page, ValidatedPage, page_validators, PAGE_SIZE, MAGIC};
use lightning_db::btree::{BTreeIntegrityValidator, btree_validators, BTreeNode, NodeType, KeyEntry};
use lightning_db::error::{Error, Result};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_integrity_config_creation() {
    let config = IntegrityConfig::default();
    assert!(config.enabled);
    assert_eq!(config.level, ValidationLevel::Standard);
    
    let prod_config = IntegrityConfig::production();
    assert!(!prod_config.validate_all_reads);
    assert!(prod_config.validate_all_writes);
    
    let dev_config = IntegrityConfig::development();
    assert!(dev_config.validate_all_reads);
    assert!(dev_config.validate_all_writes);
    assert_eq!(dev_config.sampling_rate, 1.0);
    
    let perf_config = IntegrityConfig::performance();
    assert!(!perf_config.validate_all_reads);
    assert!(!perf_config.validate_all_writes);
    assert_eq!(perf_config.sampling_rate, 0.01);
}

#[test]
fn test_checksum_validation() {
    let data = b"test data for checksum validation";
    let expected_checksum = crc32fast::hash(data);
    
    // Valid checksum should pass
    let result = validators::validate_checksum(data, expected_checksum, "test_location");
    assert!(result.is_valid());
    
    // Invalid checksum should fail
    let result = validators::validate_checksum(data, expected_checksum + 1, "test_location");
    assert!(result.is_invalid());
    
    if let ValidationResult::Invalid(violations) = result {
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].severity, ViolationSeverity::Critical);
        assert!(violations[0].error.to_string().contains("checksum"));
    }
}

#[test]
fn test_magic_number_validation() {
    let mut data = vec![0u8; 16];
    data[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    
    // Valid magic number should pass
    let result = validators::validate_magic_number(&data, MAGIC, "test_location");
    assert!(result.is_valid());
    
    // Invalid magic number should fail
    let result = validators::validate_magic_number(&data, MAGIC + 1, "test_location");
    assert!(result.is_invalid());
    
    // Insufficient data should fail
    let short_data = vec![0u8; 2];
    let result = validators::validate_magic_number(&short_data, MAGIC, "test_location");
    assert!(result.is_invalid());
}

#[test]
fn test_size_validation() {
    let result = validators::validate_size_bounds(100, 100, 0, "test_location");
    assert!(result.is_valid());
    
    let result = validators::validate_size_bounds(105, 100, 10, "test_location");
    assert!(result.is_valid());
    
    let result = validators::validate_size_bounds(150, 100, 10, "test_location");
    assert!(result.is_invalid());
}

#[test]
fn test_data_integrity_validator() {
    let config = IntegrityConfig::development();
    let validator = DataIntegrityValidator::new(config.clone());
    
    // Test successful validation
    let result = validator.validate_critical_path(
        "test_operation",
        "test_location",
        OperationType::Read,
        |_context| ValidationResult::Valid(42),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
    
    // Test validation failure
    let result = validator.validate_critical_path(
        "test_operation",
        "test_location",
        OperationType::Write,
        |_context| ValidationResult::Invalid(vec![
            lightning_db::integrity::error_types::create_violation(
                IntegrityError::ChecksumMismatch {
                    expected: 123,
                    computed: 456,
                    location: "test".to_string(),
                },
                ViolationSeverity::Error,
                "test_location",
                None,
            )
        ]),
    );
    assert!(result.is_err());
    
    // Check metrics were recorded
    let metrics = validator.get_metrics();
    assert!(metrics.get_total_validations() > 0);
}

#[test]
fn test_page_integrity_validation() {
    let config = IntegrityConfig::development();
    let page = create_valid_test_page(1);
    
    // Test basic page validation
    let result = page_validators::quick_validate_page(&page);
    assert!(result.is_valid());
    
    // Test deep validation
    let result = page_validators::deep_validate_page(&page, &config);
    assert!(result.is_ok());
    assert!(result.unwrap().is_valid());
    
    // Test corrupted page detection
    let mut corrupted_page = create_valid_test_page(2);
    let corrupted_data = corrupted_page.get_mut_data();
    corrupted_data[0] = 0xFF; // Corrupt magic number
    
    let result = page_validators::quick_validate_page(&corrupted_page);
    assert!(result.is_invalid());
}

#[test]
fn test_btree_node_validation() {
    let config = IntegrityConfig::development();
    let validator = BTreeIntegrityValidator::new(config);
    
    // Test valid leaf node
    let leaf_node = create_valid_leaf_node(1);
    let result = validator.validate_node(&leaf_node, 1);
    assert!(result.is_ok());
    
    // Test valid internal node  
    let internal_node = create_valid_internal_node(2);
    let result = validator.validate_node(&internal_node, 2);
    assert!(result.is_ok());
    
    // Test invalid node (page ID mismatch)
    let result = validator.validate_node(&leaf_node, 999);
    assert!(result.is_err());
}

#[test]
fn test_btree_quick_validation() {
    let node = create_valid_leaf_node(1);
    
    // Valid node should pass quick validation
    let result = btree_validators::quick_validate_node(&node, 1);
    assert!(result.is_valid());
    
    // Invalid node should fail
    let result = btree_validators::quick_validate_node(&node, 999);
    assert!(result.is_invalid());
}

#[test]
fn test_key_insertion_validation() {
    let node = create_valid_leaf_node(1);
    
    // Valid insertion should pass
    let result = btree_validators::validate_key_insertion(&node, b"test_key", b"test_value");
    assert!(result.is_valid());
    
    // Empty key should fail
    let result = btree_validators::validate_key_insertion(&node, b"", b"test_value");
    assert!(result.is_invalid());
    
    // Oversized key should generate warning
    let large_key = vec![0u8; 5000];
    let result = btree_validators::validate_key_insertion(&node, &large_key, b"test_value");
    assert!(result.is_invalid());
}

#[test]
fn test_integrity_metrics() {
    let metrics = IntegrityMetrics::new();
    
    // Record some validations
    metrics.record_validation_with_path(
        "test_op",
        "test_path",
        true,
        Duration::from_millis(5),
        None,
    );
    
    metrics.record_validation_with_path(
        "test_op2",
        "test_path2",
        false,
        Duration::from_millis(10),
        Some("checksum_error"),
    );
    
    let snapshot = metrics.get_snapshot();
    assert_eq!(snapshot.total_validations, 2);
    assert_eq!(snapshot.successful_validations, 1);
    assert_eq!(snapshot.failed_validations, 1);
    assert!(snapshot.avg_validation_time_micros > 0);
    assert_eq!(snapshot.checksum_errors, 1);
    
    // Test health check
    assert!(snapshot.has_health_issues()); // Due to the failed validation
    
    // Test recommendations
    let recommendations = snapshot.get_recommendations();
    assert!(!recommendations.is_empty());
}

#[test]
fn test_corruption_injection_detection() {
    let config = IntegrityConfig::development();
    let validator = DataIntegrityValidator::new(config);
    
    // Test detecting various corruption types
    let corruptions = [
        ("checksum_mismatch", create_checksum_corruption()),
        ("magic_number_corruption", create_magic_number_corruption()),
        ("size_corruption", create_size_corruption()),
        ("structural_corruption", create_structural_corruption()),
    ];
    
    for (corruption_type, corruption_validator) in corruptions.iter() {
        let result = validator.validate_critical_path(
            "corruption_test",
            &format!("corruption_{}", corruption_type),
            OperationType::Read,
            corruption_validator,
        );
        
        assert!(result.is_err(), "Failed to detect {}", corruption_type);
        
        let error_message = result.unwrap_err().to_string();
        assert!(error_message.contains("integrity") || error_message.contains("validation"));
    }
}

#[test]
fn test_validation_performance() {
    let config = IntegrityConfig::performance(); // Minimal validation
    let validator = DataIntegrityValidator::new(config);
    
    let start = std::time::Instant::now();
    let iterations = 10000;
    
    for i in 0..iterations {
        let result = validator.validate_critical_path(
            "performance_test",
            &format!("iteration_{}", i),
            OperationType::Read,
            |_| ValidationResult::Valid(()),
        );
        assert!(result.is_ok());
    }
    
    let duration = start.elapsed();
    let avg_per_validation = duration / iterations;
    
    // Should be very fast with minimal validation
    assert!(avg_per_validation < Duration::from_micros(100));
    
    println!("Average validation time: {:?}", avg_per_validation);
}

#[test]
fn test_checksum_algorithms() {
    let data = b"test data for checksum algorithm comparison";
    
    let algorithms = [
        ChecksumAlgorithm::CRC32Fast,
        ChecksumAlgorithm::XXHash32,
        ChecksumAlgorithm::XXHash64,
        ChecksumAlgorithm::SHA256,
    ];
    
    for algorithm in algorithms.iter() {
        let checksum1 = algorithm.calculate(data);
        let checksum2 = algorithm.calculate(data);
        assert_eq!(checksum1, checksum2, "Algorithm {:?} not deterministic", algorithm);
        
        // Different data should produce different checksums (with very high probability)
        let different_data = b"different test data";
        let checksum3 = algorithm.calculate(different_data);
        assert_ne!(checksum1, checksum3, "Algorithm {:?} collision detected", algorithm);
        
        // Fast algorithms should be marked as such
        if matches!(algorithm, ChecksumAlgorithm::CRC32Fast | ChecksumAlgorithm::XXHash32 | ChecksumAlgorithm::XXHash64) {
            assert!(algorithm.is_fast());
        }
    }
}

// Helper functions for test setup

fn create_valid_test_page(page_id: u32) -> Page {
    let mut page = Page::new(page_id);
    let data = page.get_mut_data();
    
    // Set up valid page structure
    data[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    data[4..8].copy_from_slice(&1u32.to_le_bytes()); // version
    data[8..12].copy_from_slice(&0u32.to_le_bytes()); // page type
    
    // Calculate and set checksum
    let checksum = {
        use crc32fast::Hasher;
        let mut hasher = Hasher::new();
        hasher.update(&data[16..]);
        hasher.finalize()
    };
    data[12..16].copy_from_slice(&checksum.to_le_bytes());
    
    page
}

fn create_valid_leaf_node(page_id: u32) -> BTreeNode {
    let mut node = BTreeNode::new_leaf(page_id);
    
    // Add some test entries with proper ordering
    node.entries.push(KeyEntry::new(b"key1".to_vec(), b"value1".to_vec()));
    node.entries.push(KeyEntry::new(b"key2".to_vec(), b"value2".to_vec()));
    node.entries.push(KeyEntry::new(b"key3".to_vec(), b"value3".to_vec()));
    
    node
}

fn create_valid_internal_node(page_id: u32) -> BTreeNode {
    let mut node = BTreeNode::new_internal(page_id);
    
    // Add entries and children
    node.entries.push(KeyEntry::new(b"key2".to_vec(), vec![]));
    node.entries.push(KeyEntry::new(b"key4".to_vec(), vec![]));
    
    // Internal nodes need children (entries + 1)
    node.children.push(10); // page ids for child nodes
    node.children.push(11);
    node.children.push(12);
    
    node
}

// Corruption injection functions for testing

fn create_checksum_corruption() -> impl Fn(&ValidationContext) -> ValidationResult<()> {
    |_context| {
        let data = b"test data";
        let wrong_checksum = crc32fast::hash(data) + 1;
        validators::validate_checksum(data, wrong_checksum, "corruption_test")
    }
}

fn create_magic_number_corruption() -> impl Fn(&ValidationContext) -> ValidationResult<()> {
    |_context| {
        let data = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00]; // Wrong magic number
        validators::validate_magic_number(&data, MAGIC, "corruption_test")
    }
}

fn create_size_corruption() -> impl Fn(&ValidationContext) -> ValidationResult<()> {
    |_context| {
        validators::validate_size_bounds(1000, 100, 10, "corruption_test")
    }
}

fn create_structural_corruption() -> impl Fn(&ValidationContext) -> ValidationResult<()> {
    |_context| {
        let violation = lightning_db::integrity::error_types::create_critical_violation(
            IntegrityError::StructuralViolation {
                structure: "test_structure".to_string(),
                violation: "Intentional corruption for testing".to_string(),
            },
            "corruption_test",
            "This is a test corruption",
        );
        ValidationResult::Invalid(vec![violation])
    }
}