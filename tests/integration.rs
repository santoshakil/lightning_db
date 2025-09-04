#![cfg(feature = "integration_tests")]
//! Integration Test Entry Point
//! 
//! This file serves as the entry point for all integration tests.
//! It imports and organizes all test modules.

#[path = "integration/mod.rs"]
mod integration_tests;

#[cfg(test)]
mod tests {
    use super::integration_tests::*;

    #[test]
    fn test_integration_suite_available() {
        // This test ensures the integration test suite is properly set up
        // and all modules are accessible
        println!("Lightning DB Integration Test Suite");
        println!("Available test modules:");
        println!("  - end_to_end_tests");
        println!("  - concurrency_tests");
        println!("  - recovery_integration_tests");
        println!("  - performance_integration_tests");
        println!("  - ha_tests");
        println!("  - security_integration_tests");
        println!("  - system_integration_tests");
        println!("  - chaos_tests");
        println!("  - test_orchestrator");
    }

    #[test]
    fn test_environment_setup() {
        // Test that we can create a test environment
        let env = TestEnvironment::new().expect("Failed to create test environment");
        assert!(env.db_path.exists() || env.db_path.parent().unwrap().exists());
        println!("Test environment created successfully at: {:?}", env.db_path);
    }
}
