use super::*;
use crate::security::{
    access_control::{AuthenticationManager, RoleId, UserId},
    audit::{AuditConfig, AuditLogger},
    crypto::CryptographicManager,
    input_validation::InputValidator,
    monitoring::{MonitoringConfig, SecurityMonitor},
    rate_limiting::RateLimitingManager,
    resource_protection::{ResourceProtectionManager, ResourceQuotas},
    SecurityConfig,
};
use std::collections::HashSet;
use std::time::Duration;

#[tokio::test]
async fn test_comprehensive_security_integration() {
    let config = SecurityConfig::default();
    
    // Test authentication
    let auth_manager = AuthenticationManager::new(
        "test_secret".to_string(),
        Duration::from_secs(3600),
    );
    
    let roles = [RoleId("reader".to_string())].into_iter().collect();
    let user_id = auth_manager.create_user(
        "testuser".to_string(),
        "securepassword123".to_string(),
        roles,
    ).unwrap();
    
    let token = auth_manager.authenticate("testuser", "securepassword123", None).unwrap();
    let session_id = auth_manager.validate_session(&token).unwrap();
    
    // Test authorization
    let auth_result = auth_manager.check_permission(&session_id, "database", "read");
    assert!(auth_result.is_ok());
    
    let auth_fail = auth_manager.check_permission(&session_id, "database", "write");
    assert!(auth_fail.is_err());
    
    // Test input validation
    let validator = InputValidator::new(1024, 1024 * 1024, true);
    
    assert!(validator.validate_key(b"valid_key").is_ok());
    assert!(validator.validate_value(b"valid_value").is_ok());
    assert!(validator.validate_key(b"'; DROP TABLE users; --").is_err());
    assert!(validator.validate_string_input("<script>alert('xss')</script>", "test").is_err());
    
    // Test encryption
    let crypto_manager = CryptographicManager::new(Duration::from_secs(3600)).unwrap();
    let test_data = b"secret data";
    let encrypted = crypto_manager.encrypt(test_data, None).unwrap();
    let decrypted = crypto_manager.decrypt(&encrypted).unwrap();
    assert_eq!(test_data, decrypted.as_slice());
    
    // Test rate limiting
    let rate_limiter = RateLimitingManager::new(1000, 100, 200, 10, 1000).unwrap();
    assert!(rate_limiter.check_global_limit().await.is_ok());
    
    // Test resource protection
    let quotas = ResourceQuotas::default();
    let resource_manager = ResourceProtectionManager::new(quotas);
    let _memory_guard = resource_manager.allocate_memory(10, "test".to_string()).unwrap();
    
    // Test monitoring
    let monitor_config = MonitoringConfig::default();
    let security_monitor = SecurityMonitor::new(monitor_config).unwrap();
    security_monitor.report_failed_authentication(None, Some("testuser".to_string())).await.unwrap();
    
    // Test audit logging
    let audit_config = AuditConfig {
        enabled: false, // Disable for testing
        ..Default::default()
    };
    let audit_logger = AuditLogger::new(audit_config).unwrap();
    audit_logger.log_authentication(
        Some("testuser".to_string()),
        Some(session_id.0.clone()),
        None,
        true,
        std::collections::HashMap::new(),
    ).await.unwrap();
}

#[test]
fn test_password_hashing() {
    let crypto_manager = CryptographicManager::new(Duration::from_secs(3600)).unwrap();
    
    let password = "test_password_123";
    let hash = crypto_manager.hash_password(password).unwrap();
    
    assert!(crypto_manager.verify_password(password, &hash).unwrap());
    assert!(!crypto_manager.verify_password("wrong_password", &hash).unwrap());
}

#[test]
fn test_secure_random_generation() {
    let crypto_manager = CryptographicManager::new(Duration::from_secs(3600)).unwrap();
    
    let random1 = crypto_manager.generate_secure_random(32).unwrap();
    let random2 = crypto_manager.generate_secure_random(32).unwrap();
    
    assert_eq!(random1.len(), 32);
    assert_eq!(random2.len(), 32);
    assert_ne!(random1, random2);
}

#[test]
fn test_path_traversal_detection() {
    let validator = InputValidator::new(1000, 1000, true);
    
    assert!(validator.validate_path("../../../etc/passwd").is_err());
    assert!(validator.validate_path("..\\..\\windows\\system32").is_err());
    assert!(validator.validate_path("normal/path/file.txt").is_ok());
}

#[test]
fn test_sql_injection_detection() {
    let validator = InputValidator::new(1000, 1000, true);
    
    assert!(validator.validate_string_input("'; DROP TABLE users; --", "test").is_err());
    assert!(validator.validate_string_input("admin' OR '1'='1", "test").is_err());
    assert!(validator.validate_string_input("UNION SELECT * FROM passwords", "test").is_err());
    assert!(validator.validate_string_input("normal text", "test").is_ok());
}

#[test]
fn test_secure_memory_comparison() {
    let crypto_manager = CryptographicManager::new(Duration::from_secs(3600)).unwrap();
    
    assert!(crypto_manager.secure_compare(b"test", b"test"));
    assert!(!crypto_manager.secure_compare(b"test", b"Test"));
    assert!(!crypto_manager.secure_compare(b"test", b"testing"));
}

#[tokio::test]
async fn test_rate_limiting() {
    let rate_limiter = RateLimitingManager::new(1000, 2, 100, 10, 1000).unwrap();
    let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 168, 1, 1));
    
    assert!(rate_limiter.check_ip_limit(ip).await.is_ok());
    assert!(rate_limiter.check_ip_limit(ip).await.is_ok());
    
    // Should fail on the third attempt within the window
    for _ in 0..60 {
        let _ = rate_limiter.check_ip_limit(ip).await;
    }
    assert!(rate_limiter.check_ip_limit(ip).await.is_err());
}