// Unit test runner to include our recovery unit tests

#[cfg(test)]
mod recovery {
    #[path = "unit/recovery/checksum_validator_tests.rs"]
    mod checksum_validator_tests;
    
    #[path = "unit/recovery/consistency_checker_tests.rs"]
    mod consistency_checker_tests;
    
    #[path = "unit/recovery/comprehensive_recovery_tests.rs"]
    mod comprehensive_recovery_tests;
    
    #[path = "unit/recovery/wal_recovery_tests.rs"]
    mod wal_recovery_tests;
    
    #[path = "unit/recovery/page_recovery_tests.rs"]
    mod page_recovery_tests;
    
    #[path = "unit/recovery/transaction_recovery_tests.rs"]
    mod transaction_recovery_tests;
}