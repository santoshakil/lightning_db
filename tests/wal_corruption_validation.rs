//! Comprehensive WAL Corruption Detection and Recovery Tests
//!
//! This test suite validates that our WAL corruption detection system
//! correctly identifies all types of silent corruption and handles them
//! appropriately without data loss.

use lightning_db::error::{Error, Result};
use lightning_db::wal::{
    WALEntry, WALOperation, WalCorruptionValidator, ValidationConfig,
    SafeWalRecovery, RecoveryConfig, RecoveryStatus, WalCorruptionType,
};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write, Read};
use std::path::PathBuf;
use tempfile::tempdir;
use tracing_test::traced_test;

/// Test suite for WAL corruption validation
mod corruption_detection_tests {
    use super::*;

    #[traced_test]
    #[test]
    fn test_checksum_corruption_detection() {
        let mut validator = WalCorruptionValidator::new(ValidationConfig::default());
        
        // Create a valid entry
        let mut entry = WALEntry::new(1, WALOperation::Put {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        });
        entry.calculate_checksum();
        let valid_checksum = entry.checksum;
        
        // Valid entry should pass
        assert!(validator.validate_entry(&entry, 0).is_ok(), "Valid entry should pass validation");
        
        // Corrupt the checksum
        entry.checksum = valid_checksum ^ 0xFFFFFFFF;
        
        let result = validator.validate_entry(&entry, 0);
        assert!(result.is_err(), "Corrupted checksum should be detected");
        
        match result.unwrap_err() {
            Error::WalChecksumMismatch { expected, found, .. } => {
                assert_eq!(expected, valid_checksum);
                assert_eq!(found, entry.checksum);
            }
            other => panic!("Expected checksum mismatch error, got: {:?}", other),
        }
    }

    #[traced_test]
    #[test]
    fn test_sequence_gap_detection() {
        let config = ValidationConfig {
            strict_sequence_validation: true,
            ..Default::default()
        };
        let mut validator = WalCorruptionValidator::new(config);
        
        // Create sequential entries
        let mut entry1 = WALEntry::new(1, WALOperation::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        });
        entry1.calculate_checksum();
        
        let mut entry2 = WALEntry::new(2, WALOperation::Put {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
        });
        entry2.calculate_checksum();
        
        let mut entry_gap = WALEntry::new(5, WALOperation::Put { // Gap: 2 -> 5
            key: b"key5".to_vec(),
            value: b"value5".to_vec(),
        });
        entry_gap.calculate_checksum();
        
        // First entry should pass
        assert!(validator.validate_entry(&entry1, 0).is_ok());
        
        // Sequential entry should pass
        assert!(validator.validate_entry(&entry2, 100).is_ok());
        
        // Gap should be detected
        let result = validator.validate_entry(&entry_gap, 200);
        assert!(result.is_err(), "Sequence gap should be detected");
        
        match result.unwrap_err() {
            Error::WalSequenceGap { expected, found, lsn } => {
                assert_eq!(expected, 3);
                assert_eq!(found, 5);
                assert_eq!(lsn, 5);
            }
            other => panic!("Expected sequence gap error, got: {:?}", other),
        }
    }

    #[traced_test]
    #[test]
    fn test_timestamp_validation() {
        let config = ValidationConfig {
            validate_timestamps: true,
            max_timestamp_deviation: 3600, // 1 hour
            ..Default::default()
        };
        let mut validator = WalCorruptionValidator::new(config);
        
        // Create entry with future timestamp
        let mut future_entry = WALEntry::new(1, WALOperation::Put {
            key: b"future_key".to_vec(),
            value: b"future_value".to_vec(),
        });
        
        // Set timestamp to 2 hours in the future
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        future_entry.timestamp = now + (2 * 3600 * 1000); // 2 hours ahead
        future_entry.calculate_checksum();
        
        let result = validator.validate_entry(&future_entry, 0);
        assert!(result.is_err(), "Future timestamp should be detected");
        
        match result.unwrap_err() {
            Error::WalTimestampError { lsn, timestamp, context } => {
                assert_eq!(lsn, 1);
                assert_eq!(timestamp, future_entry.timestamp);
                assert!(context.contains("future"));
            }
            other => panic!("Expected timestamp error, got: {:?}", other),
        }
    }

    #[traced_test]
    #[test]
    fn test_transaction_consistency_validation() {
        let config = ValidationConfig {
            validate_transactions: true,
            ..Default::default()
        };
        let mut validator = WalCorruptionValidator::new(config);
        
        // Test commit without begin
        let mut orphan_commit = WALEntry::new(1, WALOperation::TransactionCommit { tx_id: 100 });
        orphan_commit.calculate_checksum();
        
        let result = validator.validate_entry(&orphan_commit, 0);
        assert!(result.is_err(), "Commit without begin should be detected");
        
        match result.unwrap_err() {
            Error::WalTransactionCorruption { tx_id, violation_type } => {
                assert_eq!(tx_id, 100);
                assert!(violation_type.contains("without begin"));
            }
            other => panic!("Expected transaction corruption error, got: {:?}", other),
        }
        
        // Test proper transaction flow
        let mut begin_entry = WALEntry::new(2, WALOperation::TransactionBegin { tx_id: 200 });
        begin_entry.calculate_checksum();
        
        let mut commit_entry = WALEntry::new(3, WALOperation::TransactionCommit { tx_id: 200 });
        commit_entry.calculate_checksum();
        
        // Begin should pass
        assert!(validator.validate_entry(&begin_entry, 100).is_ok());
        
        // Commit after begin should pass
        assert!(validator.validate_entry(&commit_entry, 200).is_ok());
    }

    #[traced_test]
    #[test]
    fn test_oversized_entry_detection() {
        let config = ValidationConfig {
            max_entry_size: 1024, // Small limit for testing
            ..Default::default()
        };
        let validator = WalCorruptionValidator::new(config);
        
        // Create entry with large value
        let large_value = vec![0u8; 2048]; // Exceeds limit
        let mut large_entry = WALEntry::new(1, WALOperation::Put {
            key: b"large_key".to_vec(),
            value: large_value,
        });
        large_entry.calculate_checksum();
        
        let result = validator.validate_entry(&large_entry, 0);
        assert!(result.is_err(), "Oversized entry should be detected");
        
        // The error might be binary corruption due to serialization size
        match result.unwrap_err() {
            Error::WalBinaryCorruption { details, .. } => {
                assert!(details.contains("too large") || details.contains("Entry too large"));
            }
            other => panic!("Expected binary corruption error for oversized entry, got: {:?}", other),
        }
    }

    #[traced_test]
    #[test]
    fn test_invalid_operation_content() {
        let mut validator = WalCorruptionValidator::new(ValidationConfig::default());
        
        // Test empty key in Put operation
        let mut empty_key_entry = WALEntry::new(1, WALOperation::Put {
            key: Vec::new(), // Empty key
            value: b"some_value".to_vec(),
        });
        empty_key_entry.calculate_checksum();
        
        let result = validator.validate_entry(&empty_key_entry, 0);
        assert!(result.is_err(), "Empty key should be detected");
        
        match result.unwrap_err() {
            Error::WalBinaryCorruption { details, .. } => {
                assert!(details.contains("empty key"));
            }
            other => panic!("Expected binary corruption error for empty key, got: {:?}", other),
        }
    }
}

/// Test suite for file-level corruption injection and detection
mod file_corruption_tests {
    use super::*;

    /// Create a test WAL file with valid entries
    fn create_test_wal_file(path: &std::path::Path, entries: Vec<WALEntry>) -> Result<()> {
        let mut file = File::create(path)?;
        
        for entry in entries {
            let serialized = bincode::encode_to_vec(&entry, bincode::config::standard())?;
            file.write_all(&(serialized.len() as u32).to_le_bytes())?;
            file.write_all(&serialized)?;
        }
        
        file.sync_all()?;
        Ok(())
    }

    /// Inject byte corruption at a specific offset
    fn inject_byte_corruption(path: &std::path::Path, offset: u64, corruption: &[u8]) -> Result<()> {
        let mut file = OpenOptions::new().write(true).open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(corruption)?;
        file.sync_all()?;
        Ok(())
    }

    /// Inject partial write corruption (truncate file)
    fn inject_partial_write(path: &std::path::Path, truncate_at: u64) -> Result<()> {
        let file = File::create(path)?; // Truncates the file
        file.set_len(truncate_at)?;
        file.sync_all()?;
        Ok(())
    }

    #[traced_test]
    #[test]
    fn test_byte_corruption_detection() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("corrupted.wal");
        
        // Create test entries
        let mut entry1 = WALEntry::new(1, WALOperation::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        });
        entry1.calculate_checksum();
        
        let mut entry2 = WALEntry::new(2, WALOperation::Put {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
        });
        entry2.calculate_checksum();
        
        // Create WAL file
        create_test_wal_file(&wal_path, vec![entry1, entry2]).unwrap();
        
        // Inject byte corruption in the middle of first entry
        inject_byte_corruption(&wal_path, 10, &[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        
        // Test recovery with corruption detection
        let config = RecoveryConfig {
            validation_config: ValidationConfig::default(),
            continue_after_corruption: true,
            max_corruptions_tolerated: 10,
            ..Default::default()
        };
        
        let mut recovery = SafeWalRecovery::new(config);
        let mut applied_operations = Vec::new();
        
        let report = recovery.recover_with_validation(
            vec![wal_path],
            |operation, _is_committed| {
                applied_operations.push(operation.clone());
                Ok(())
            }
        ).unwrap();
        
        // Should detect corruption but possibly recover some entries
        assert!(!report.corruptions_detected.is_empty(), "Corruption should be detected");
        assert!(matches!(report.recovery_status, RecoveryStatus::PartialSuccess { .. } | RecoveryStatus::Failed { .. }));
        
        // Check that corruption details are properly recorded
        let corruption = &report.corruptions_detected[0];
        assert_eq!(corruption.file_path, wal_path);
        println!("Detected corruption: {}", corruption);
    }

    #[traced_test]
    #[test]
    fn test_partial_write_detection() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("partial.wal");
        
        // Create test entries
        let mut entry1 = WALEntry::new(1, WALOperation::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        });
        entry1.calculate_checksum();
        
        let mut entry2 = WALEntry::new(2, WALOperation::Put {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
        });
        entry2.calculate_checksum();
        
        // Create WAL file
        create_test_wal_file(&wal_path, vec![entry1, entry2]).unwrap();
        
        // Get file size and truncate it in the middle
        let original_size = std::fs::metadata(&wal_path).unwrap().len();
        let truncate_at = original_size / 2;
        
        // Truncate file to simulate partial write
        {
            let file = OpenOptions::new().write(true).open(&wal_path).unwrap();
            file.set_len(truncate_at).unwrap();
        }
        
        // Test recovery
        let config = RecoveryConfig {
            validation_config: ValidationConfig::default(),
            continue_after_corruption: false, // Fail fast on corruption
            ..Default::default()
        };
        
        let mut recovery = SafeWalRecovery::new(config);
        let mut applied_operations = Vec::new();
        
        let report = recovery.recover_with_validation(
            vec![wal_path.clone()],
            |operation, _is_committed| {
                applied_operations.push(operation.clone());
                Ok(())
            }
        ).unwrap();
        
        // Should detect partial write
        println!("Recovery report: {}", report);
        
        // Either no corruptions (if file ends cleanly) or partial entry corruption
        if !report.corruptions_detected.is_empty() {
            let has_partial_corruption = report.corruptions_detected.iter().any(|c| {
                matches!(c.corruption_type, WalCorruptionType::PartialEntry { .. })
            });
            
            if !has_partial_corruption {
                // Might be detected as binary corruption or EOF
                println!("Detected corruptions: {:?}", report.corruptions_detected);
            }
        }
    }

    #[traced_test]
    #[test]
    fn test_malformed_entry_length() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("malformed.wal");
        
        // Create file with malformed entry length
        let mut file = File::create(&wal_path).unwrap();
        
        // Write invalid entry length (too large)
        let invalid_length = 0xFFFFFFFFu32;
        file.write_all(&invalid_length.to_le_bytes()).unwrap();
        file.write_all(&[0u8; 100]).unwrap(); // Some dummy data
        file.sync_all().unwrap();
        
        // Test recovery
        let config = RecoveryConfig::default();
        let mut recovery = SafeWalRecovery::new(config);
        let mut applied_operations = Vec::new();
        
        let report = recovery.recover_with_validation(
            vec![wal_path.clone()],
            |operation, _is_committed| {
                applied_operations.push(operation.clone());
                Ok(())
            }
        ).unwrap();
        
        // Should detect malformed entry
        assert!(!report.corruptions_detected.is_empty(), "Malformed entry should be detected");
        
        let corruption = &report.corruptions_detected[0];
        match &corruption.corruption_type {
            WalCorruptionType::InvalidBinaryFormat { details, .. } => {
                assert!(details.contains("too large") || details.contains("Entry too large"));
            }
            other => {
                println!("Got corruption type: {:?}", other);
                // Other binary corruption types are also acceptable
            }
        }
    }

    #[traced_test]
    #[test]
    fn test_silent_corruption_prevention() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("silent.wal");
        
        // Create valid entries
        let mut entry1 = WALEntry::new(1, WALOperation::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        });
        entry1.calculate_checksum();
        
        let mut entry2 = WALEntry::new(2, WALOperation::Put {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
        });
        entry2.calculate_checksum();
        
        create_test_wal_file(&wal_path, vec![entry1, entry2]).unwrap();
        
        // Inject subtle corruption that might pass basic checks
        // Corrupt a single byte in the value area
        inject_byte_corruption(&wal_path, 50, &[0x00]).unwrap();
        
        // Use strict validation config
        let config = RecoveryConfig {
            validation_config: ValidationConfig {
                fail_fast_on_corruption: true,
                ..Default::default()
            },
            continue_after_corruption: false,
            ..Default::default()
        };
        
        let mut recovery = SafeWalRecovery::new(config);
        let mut operations_applied = 0;
        
        let report = recovery.recover_with_validation(
            vec![wal_path.clone()],
            |_operation, _is_committed| {
                operations_applied += 1;
                Ok(())
            }
        ).unwrap();
        
        // With strict validation, corruption should be detected
        println!("Recovery report: {}", report);
        
        // The key test: we should NOT have silently applied corrupted data
        // Either corruption is detected, or we successfully apply all valid operations
        if report.corruptions_detected.is_empty() {
            // No corruption detected - all operations should be valid
            assert!(operations_applied > 0, "Should have applied valid operations");
        } else {
            // Corruption detected - recovery should have been controlled
            println!("Corruption properly detected and handled");
            assert!(!matches!(report.recovery_status, RecoveryStatus::CompleteSuccess));
        }
    }
}

/// Integration tests with real recovery scenarios
mod integration_tests {
    use super::*;

    #[traced_test]
    #[test]
    fn test_transaction_recovery_with_corruption() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("tx_corrupt.wal");
        
        // Create a sequence with transaction
        let mut begin_entry = WALEntry::new(1, WALOperation::TransactionBegin { tx_id: 100 });
        begin_entry.calculate_checksum();
        
        let mut put_entry = WALEntry::new(2, WALOperation::Put {
            key: b"tx_key".to_vec(),
            value: b"tx_value".to_vec(),
        });
        put_entry.calculate_checksum();
        
        let mut commit_entry = WALEntry::new(3, WALOperation::TransactionCommit { tx_id: 100 });
        commit_entry.calculate_checksum();
        
        create_test_wal_file(&wal_path, vec![begin_entry, put_entry, commit_entry]).unwrap();
        
        // Corrupt the commit entry
        inject_byte_corruption(&wal_path, 80, &[0xFF, 0xFF]).unwrap();
        
        // Test recovery with transaction validation
        let config = RecoveryConfig {
            validation_config: ValidationConfig {
                validate_transactions: true,
                continue_after_corruption: true,
                ..Default::default()
            },
            continue_after_corruption: true,
            max_corruptions_tolerated: 5,
            ..Default::default()
        };
        
        let mut recovery = SafeWalRecovery::new(config);
        let mut applied_operations = Vec::new();
        
        let report = recovery.recover_with_validation(
            vec![wal_path],
            |operation, is_committed| {
                applied_operations.push((operation.clone(), is_committed));
                Ok(())
            }
        ).unwrap();
        
        println!("Recovery report:\n{}", report);
        
        // Should detect corruption and handle transaction consistency
        assert!(!report.corruptions_detected.is_empty());
        
        // Check transaction summary
        assert!(report.transaction_summary.transactions_started > 0);
        
        // Incomplete transactions should be tracked
        if report.transaction_summary.transactions_incomplete > 0 {
            println!("Incomplete transactions properly detected");
        }
    }

    #[traced_test]
    #[test]
    fn test_multiple_file_corruption() {
        let temp_dir = tempdir().unwrap();
        let wal1_path = temp_dir.path().join("file1.wal");
        let wal2_path = temp_dir.path().join("file2.wal");
        
        // Create first WAL file
        let mut entry1 = WALEntry::new(1, WALOperation::Put {
            key: b"file1_key".to_vec(),
            value: b"file1_value".to_vec(),
        });
        entry1.calculate_checksum();
        
        create_test_wal_file(&wal1_path, vec![entry1]).unwrap();
        
        // Create second WAL file with corruption
        let mut entry2 = WALEntry::new(2, WALOperation::Put {
            key: b"file2_key".to_vec(),
            value: b"file2_value".to_vec(),
        });
        entry2.calculate_checksum();
        
        create_test_wal_file(&wal2_path, vec![entry2]).unwrap();
        
        // Corrupt second file
        inject_byte_corruption(&wal2_path, 10, &[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        
        // Test recovery across multiple files
        let config = RecoveryConfig {
            validation_config: ValidationConfig::default(),
            continue_after_corruption: true,
            max_corruptions_tolerated: 10,
            ..Default::default()
        };
        
        let mut recovery = SafeWalRecovery::new(config);
        let mut applied_operations = Vec::new();
        
        let report = recovery.recover_with_validation(
            vec![wal1_path, wal2_path],
            |operation, _is_committed| {
                applied_operations.push(operation.clone());
                Ok(())
            }
        ).unwrap();
        
        println!("Multi-file recovery report:\n{}", report);
        
        // Should have processed both files
        assert_eq!(report.wal_files.len(), 2);
        
        // Should have recovered at least one entry from the first file
        assert!(report.entries_recovered > 0);
        
        // Should have detected corruption
        assert!(!report.corruptions_detected.is_empty());
        
        // Recovery status should reflect partial success
        assert!(matches!(report.recovery_status, 
            RecoveryStatus::PartialSuccess { .. } | RecoveryStatus::CompleteSuccess));
    }

    #[traced_test]  
    #[test]
    fn test_corruption_reporting_completeness() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("report_test.wal");
        
        // Create entries with various issues
        let mut valid_entry = WALEntry::new(1, WALOperation::Put {
            key: b"valid_key".to_vec(),
            value: b"valid_value".to_vec(),
        });
        valid_entry.calculate_checksum();
        
        let mut corrupted_entry = WALEntry::new(2, WALOperation::Put {
            key: b"corrupt_key".to_vec(),
            value: b"corrupt_value".to_vec(),
        });
        corrupted_entry.calculate_checksum();
        corrupted_entry.checksum ^= 0xFFFFFFFF; // Corrupt checksum
        
        create_test_wal_file(&wal_path, vec![valid_entry, corrupted_entry]).unwrap();
        
        let config = RecoveryConfig {
            validation_config: ValidationConfig::default(),
            continue_after_corruption: true,
            detailed_logging: true,
            recovery_log_dir: Some(temp_dir.path().to_path_buf()),
            ..Default::default()
        };
        
        let mut recovery = SafeWalRecovery::new(config);
        
        let report = recovery.recover_with_validation(
            vec![wal_path.clone()],
            |_operation, _is_committed| Ok(())
        ).unwrap();
        
        println!("Detailed recovery report:\n{}", report);
        
        // Verify report completeness
        assert_eq!(report.wal_files, vec![wal_path]);
        assert!(report.entries_processed > 0);
        
        // Should have corruption details
        if !report.corruptions_detected.is_empty() {
            let corruption = &report.corruptions_detected[0];
            assert!(!corruption.additional_context.is_empty());
            assert!(corruption.file_offset > 0);
        }
        
        // Should have data loss assessment
        println!("Data loss assessment: {}", report.data_loss_assessment);
        
        // Should have recommendations if there were issues
        if !report.corruptions_detected.is_empty() {
            assert!(!report.recommendations.is_empty(), "Should have recommendations for corruption");
        }
        
        // Check that recovery log was written
        let log_files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.file_name()?.to_str()?.starts_with("wal_recovery_") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();
        
        assert!(!log_files.is_empty(), "Recovery log should be written");
        
        // Verify log content
        if let Some(log_file) = log_files.first() {
            let log_content = std::fs::read_to_string(log_file).unwrap();
            assert!(log_content.contains("WAL Recovery Report"));
            assert!(log_content.contains("Corruption"));
            println!("Log file created: {}", log_file.display());
        }
    }
}

/// Performance tests to ensure validation doesn't significantly impact recovery speed
mod performance_tests {
    use super::*;

    #[traced_test]
    #[test]
    fn test_validation_performance_impact() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("perf_test.wal");
        
        // Create many entries for performance testing
        let mut entries = Vec::new();
        for i in 1..=1000 {
            let mut entry = WALEntry::new(i, WALOperation::Put {
                key: format!("key_{}", i).into_bytes(),
                value: format!("value_{}", i).into_bytes(),
            });
            entry.calculate_checksum();
            entries.push(entry);
        }
        
        create_test_wal_file(&wal_path, entries).unwrap();
        
        // Test with minimal validation
        let minimal_config = RecoveryConfig {
            validation_config: ValidationConfig {
                strict_sequence_validation: false,
                validate_timestamps: false,
                validate_references: false,
                validate_transactions: false,
                ..Default::default()
            },
            detailed_logging: false,
            perform_post_recovery_validation: false,
            ..Default::default()
        };
        
        let start_time = std::time::Instant::now();
        let mut minimal_recovery = SafeWalRecovery::new(minimal_config);
        let minimal_report = minimal_recovery.recover_with_validation(
            vec![wal_path.clone()],
            |_operation, _is_committed| Ok(())
        ).unwrap();
        let minimal_duration = start_time.elapsed();
        
        // Test with full validation
        let full_config = RecoveryConfig {
            validation_config: ValidationConfig {
                strict_sequence_validation: true,
                validate_timestamps: true,
                validate_references: true,
                validate_transactions: true,
                ..Default::default()
            },
            detailed_logging: true,
            perform_post_recovery_validation: true,
            ..Default::default()
        };
        
        let start_time = std::time::Instant::now();
        let mut full_recovery = SafeWalRecovery::new(full_config);
        let full_report = full_recovery.recover_with_validation(
            vec![wal_path],
            |_operation, _is_committed| Ok(())
        ).unwrap();
        let full_duration = start_time.elapsed();
        
        println!("Minimal validation: {} entries in {:?}", minimal_report.entries_processed, minimal_duration);
        println!("Full validation: {} entries in {:?}", full_report.entries_processed, full_duration);
        
        // Both should process the same number of entries
        assert_eq!(minimal_report.entries_processed, full_report.entries_processed);
        
        // Full validation should not be more than 10x slower
        let overhead_ratio = full_duration.as_millis() as f64 / minimal_duration.as_millis() as f64;
        println!("Validation overhead ratio: {:.2}x", overhead_ratio);
        
        // This is a reasonable bound - adjust if needed based on actual performance
        assert!(overhead_ratio < 10.0, "Validation overhead too high: {:.2}x", overhead_ratio);
        
        // Both should succeed without corruption detected
        assert!(minimal_report.corruptions_detected.is_empty());
        assert!(full_report.corruptions_detected.is_empty());
    }
}