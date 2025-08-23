use crate::security::{SecurityError, SecurityResult};
use bcrypt::{hash, verify, DEFAULT_COST};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use secrecy::{ExposeSecret, Secret};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoleId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PermissionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Clone)]
pub struct User {
    pub id: UserId,
    pub username: String,
    pub password_hash: Secret<String>,
    pub roles: HashSet<RoleId>,
    pub active: bool,
    pub created_at: SystemTime,
    pub last_login: Option<SystemTime>,
    pub failed_login_attempts: u32,
    pub locked_until: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct Role {
    pub id: RoleId,
    pub name: String,
    pub permissions: HashSet<PermissionId>,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct Permission {
    pub id: PermissionId,
    pub name: String,
    pub resource: String,
    pub action: String,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct Session {
    pub id: SessionId,
    pub user_id: UserId,
    pub created_at: SystemTime,
    pub expires_at: SystemTime,
    pub last_activity: SystemTime,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
    pub iat: usize,
    pub roles: Vec<String>,
    pub session_id: String,
}

pub struct AuthenticationManager {
    users: Arc<RwLock<HashMap<UserId, User>>>,
    roles: Arc<RwLock<HashMap<RoleId, Role>>>,
    permissions: Arc<RwLock<HashMap<PermissionId, Permission>>>,
    sessions: Arc<RwLock<HashMap<SessionId, Session>>>,
    jwt_secret: Secret<String>,
    session_timeout: Duration,
    max_failed_attempts: u32,
    lockout_duration: Duration,
}

impl AuthenticationManager {
    pub fn new(jwt_secret: String, session_timeout: Duration) -> Self {
        let mut manager = Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(HashMap::new())),
            permissions: Arc::new(RwLock::new(HashMap::new())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            jwt_secret: Secret::new(jwt_secret),
            session_timeout,
            max_failed_attempts: 5,
            lockout_duration: Duration::from_secs(900),
        };
        
        manager.initialize_default_permissions();
        manager.initialize_default_roles();
        manager
    }

    fn initialize_default_permissions(&mut self) {
        let permissions = vec![
            Permission {
                id: PermissionId("read".to_string()),
                name: "Read".to_string(),
                resource: "*".to_string(),
                action: "read".to_string(),
                description: "Read access to database".to_string(),
            },
            Permission {
                id: PermissionId("write".to_string()),
                name: "Write".to_string(),
                resource: "*".to_string(),
                action: "write".to_string(),
                description: "Write access to database".to_string(),
            },
            Permission {
                id: PermissionId("delete".to_string()),
                name: "Delete".to_string(),
                resource: "*".to_string(),
                action: "delete".to_string(),
                description: "Delete access to database".to_string(),
            },
            Permission {
                id: PermissionId("admin".to_string()),
                name: "Admin".to_string(),
                resource: "*".to_string(),
                action: "*".to_string(),
                description: "Full administrative access".to_string(),
            },
        ];

        let mut perms_map = self.permissions.write().unwrap();
        for perm in permissions {
            perms_map.insert(perm.id.clone(), perm);
        }
    }

    fn initialize_default_roles(&mut self) {
        let roles = vec![
            Role {
                id: RoleId("reader".to_string()),
                name: "Reader".to_string(),
                permissions: [PermissionId("read".to_string())].into_iter().collect(),
                description: "Read-only access".to_string(),
            },
            Role {
                id: RoleId("writer".to_string()),
                name: "Writer".to_string(),
                permissions: [PermissionId("read".to_string()), PermissionId("write".to_string())].into_iter().collect(),
                description: "Read and write access".to_string(),
            },
            Role {
                id: RoleId("admin".to_string()),
                name: "Administrator".to_string(),
                permissions: [PermissionId("admin".to_string())].into_iter().collect(),
                description: "Full administrative access".to_string(),
            },
        ];

        let mut roles_map = self.roles.write().unwrap();
        for role in roles {
            roles_map.insert(role.id.clone(), role);
        }
    }

    pub fn create_user(&self, username: String, password: String, roles: HashSet<RoleId>) -> SecurityResult<UserId> {
        if username.is_empty() || password.len() < 8 {
            return Err(SecurityError::InputValidationFailed(
                "Username cannot be empty and password must be at least 8 characters".to_string()
            ));
        }

        let password_hash = hash(password, DEFAULT_COST)
            .map_err(|e| SecurityError::CryptographicFailure(format!("Password hashing failed: {}", e)))?;

        let user_id = UserId(Uuid::new_v4().to_string());
        let user = User {
            id: user_id.clone(),
            username,
            password_hash: Secret::new(password_hash),
            roles,
            active: true,
            created_at: SystemTime::now(),
            last_login: None,
            failed_login_attempts: 0,
            locked_until: None,
        };

        let mut users = self.users.write().unwrap();
        users.insert(user_id.clone(), user);
        
        Ok(user_id)
    }

    pub fn authenticate(&self, username: &str, password: &str, ip_address: Option<String>) -> SecurityResult<String> {
        let user_id = {
            let users = self.users.read().unwrap();
            users.values()
                .find(|u| u.username == username)
                .map(|u| u.id.clone())
                .ok_or_else(|| SecurityError::AuthenticationFailed("Invalid credentials".to_string()))?
        };

        let mut users = self.users.write().unwrap();
        let user = users.get_mut(&user_id)
            .ok_or_else(|| SecurityError::AuthenticationFailed("Invalid credentials".to_string()))?;

        if !user.active {
            return Err(SecurityError::AuthenticationFailed("Account disabled".to_string()));
        }

        if let Some(locked_until) = user.locked_until {
            if SystemTime::now() < locked_until {
                return Err(SecurityError::AuthenticationFailed("Account temporarily locked".to_string()));
            } else {
                user.locked_until = None;
                user.failed_login_attempts = 0;
            }
        }

        let password_valid = verify(password, user.password_hash.expose_secret())
            .map_err(|e| SecurityError::CryptographicFailure(format!("Password verification failed: {}", e)))?;

        if !password_valid {
            user.failed_login_attempts += 1;
            if user.failed_login_attempts >= self.max_failed_attempts {
                user.locked_until = Some(SystemTime::now() + self.lockout_duration);
            }
            return Err(SecurityError::AuthenticationFailed("Invalid credentials".to_string()));
        }

        user.failed_login_attempts = 0;
        user.locked_until = None;
        user.last_login = Some(SystemTime::now());

        let session_id = SessionId::new();
        let now = SystemTime::now();
        let session = Session {
            id: session_id.clone(),
            user_id: user_id.clone(),
            created_at: now,
            expires_at: now + self.session_timeout,
            last_activity: now,
            ip_address,
            user_agent: None,
        };

        let mut sessions = self.sessions.write().unwrap();
        sessions.insert(session_id.clone(), session);

        self.generate_jwt(&user_id, &session_id, &user.roles)
    }

    pub fn validate_session(&self, token: &str) -> SecurityResult<SessionId> {
        let claims = self.decode_jwt(token)?;
        let session_id = SessionId(claims.session_id);

        let mut sessions = self.sessions.write().unwrap();
        let session = sessions.get_mut(&session_id)
            .ok_or_else(|| SecurityError::AuthenticationFailed("Invalid session".to_string()))?;

        if SystemTime::now() > session.expires_at {
            sessions.remove(&session_id);
            return Err(SecurityError::AuthenticationFailed("Session expired".to_string()));
        }

        session.last_activity = SystemTime::now();
        session.expires_at = SystemTime::now() + self.session_timeout;

        Ok(session_id)
    }

    pub fn check_permission(&self, session_id: &SessionId, resource: &str, action: &str) -> SecurityResult<()> {
        let sessions = self.sessions.read().unwrap();
        let session = sessions.get(session_id)
            .ok_or_else(|| SecurityError::AuthorizationFailed("Invalid session".to_string()))?;

        let users = self.users.read().unwrap();
        let user = users.get(&session.user_id)
            .ok_or_else(|| SecurityError::AuthorizationFailed("User not found".to_string()))?;

        if !user.active {
            return Err(SecurityError::AuthorizationFailed("User account disabled".to_string()));
        }

        let roles = self.roles.read().unwrap();
        let permissions = self.permissions.read().unwrap();

        for role_id in &user.roles {
            if let Some(role) = roles.get(role_id) {
                for perm_id in &role.permissions {
                    if let Some(permission) = permissions.get(perm_id) {
                        if (permission.resource == "*" || permission.resource == resource) &&
                           (permission.action == "*" || permission.action == action) {
                            return Ok(());
                        }
                    }
                }
            }
        }

        Err(SecurityError::AuthorizationFailed(
            format!("Access denied to {} on {}", action, resource)
        ))
    }

    pub fn logout(&self, session_id: &SessionId) -> SecurityResult<()> {
        let mut sessions = self.sessions.write().unwrap();
        sessions.remove(session_id)
            .ok_or_else(|| SecurityError::AuthenticationFailed("Session not found".to_string()))?;
        Ok(())
    }

    pub fn cleanup_expired_sessions(&self) {
        let mut sessions = self.sessions.write().unwrap();
        let now = SystemTime::now();
        sessions.retain(|_, session| session.expires_at > now);
    }

    fn generate_jwt(&self, user_id: &UserId, session_id: &SessionId, roles: &HashSet<RoleId>) -> SecurityResult<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;

        let claims = Claims {
            sub: user_id.0.clone(),
            exp: now + self.session_timeout.as_secs() as usize,
            iat: now,
            roles: roles.iter().map(|r| r.0.clone()).collect(),
            session_id: session_id.0.clone(),
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.jwt_secret.expose_secret().as_bytes()),
        )
        .map_err(|e| SecurityError::CryptographicFailure(format!("JWT generation failed: {}", e)))
    }

    fn decode_jwt(&self, token: &str) -> SecurityResult<Claims> {
        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.jwt_secret.expose_secret().as_bytes()),
            &Validation::default(),
        )
        .map(|data| data.claims)
        .map_err(|e| SecurityError::AuthenticationFailed(format!("JWT validation failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_creation() {
        let auth = AuthenticationManager::new("test_secret".to_string(), Duration::from_secs(3600));
        let roles = [RoleId("reader".to_string())].into_iter().collect();
        
        let user_id = auth.create_user("testuser".to_string(), "password123".to_string(), roles);
        assert!(user_id.is_ok());
    }

    #[test]
    fn test_authentication() {
        let auth = AuthenticationManager::new("test_secret".to_string(), Duration::from_secs(3600));
        let roles = [RoleId("reader".to_string())].into_iter().collect();
        
        auth.create_user("testuser".to_string(), "password123".to_string(), roles).unwrap();
        
        let token = auth.authenticate("testuser", "password123", None);
        assert!(token.is_ok());
        
        let invalid_token = auth.authenticate("testuser", "wrongpassword", None);
        assert!(invalid_token.is_err());
    }

    #[test]
    fn test_authorization() {
        let auth = AuthenticationManager::new("test_secret".to_string(), Duration::from_secs(3600));
        let roles = [RoleId("reader".to_string())].into_iter().collect();
        
        auth.create_user("testuser".to_string(), "password123".to_string(), roles).unwrap();
        let token = auth.authenticate("testuser", "password123", None).unwrap();
        let session_id = auth.validate_session(&token).unwrap();
        
        let read_result = auth.check_permission(&session_id, "database", "read");
        assert!(read_result.is_ok());
        
        let write_result = auth.check_permission(&session_id, "database", "write");
        assert!(write_result.is_err());
    }
}