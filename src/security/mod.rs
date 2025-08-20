pub mod access_control;
pub mod audit;
pub mod crypto;
pub mod input_validation;
pub mod monitoring;
pub mod network;
pub mod rate_limiting;
pub mod resource_protection;
pub mod secure_database;

#[cfg(test)]
mod tests;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SecurityError {
    #[error("Access denied: {0}")]
    AccessDenied(String),
    
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),
    
    #[error("Authorization failed: {0}")]
    AuthorizationFailed(String),
    
    #[error("Input validation failed: {0}")]
    InputValidationFailed(String),
    
    #[error("Rate limit exceeded: {0}")]
    RateLimitExceeded(String),
    
    #[error("Resource quota exceeded: {0}")]
    ResourceQuotaExceeded(String),
    
    #[error("Cryptographic operation failed: {0}")]
    CryptographicFailure(String),
    
    #[error("Network security violation: {0}")]
    NetworkSecurityViolation(String),
    
    #[error("Security policy violation: {0}")]
    PolicyViolation(String),
    
    #[error("Audit logging failed: {0}")]
    AuditFailure(String),
}

pub type SecurityResult<T> = Result<T, SecurityError>;

pub struct SecurityConfig {
    pub max_key_size: usize,
    pub max_value_size: usize,
    pub max_connections: usize,
    pub rate_limit_per_minute: u32,
    pub audit_enabled: bool,
    pub encryption_required: bool,
    pub tls_required: bool,
    pub jwt_secret: Option<String>,
    pub session_timeout_seconds: u64,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            max_key_size: 1024,
            max_value_size: 1024 * 1024,
            max_connections: 1000,
            rate_limit_per_minute: 1000,
            audit_enabled: true,
            encryption_required: true,
            tls_required: false,
            jwt_secret: None,
            session_timeout_seconds: 3600,
        }
    }
}