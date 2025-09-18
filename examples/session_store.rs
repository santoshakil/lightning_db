use lightning_db::{Database, LightningDbConfig, Result};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Simple session store example demonstrating Lightning DB usage
/// for a web application session management system
pub struct SessionStore {
    db: Database,
}

impl SessionStore {
    pub fn new(path: &str) -> Result<Self> {
        let config = LightningDbConfig {
            cache_size: 32 * 1024 * 1024, // 32MB cache for session data
            compression_enabled: true,
            ..Default::default()
        };

        let db = Database::open(path, config)?;
        Ok(SessionStore { db })
    }

    pub fn create_session(&self, user_id: &str, data: &[u8]) -> Result<String> {
        let session_id = generate_session_id();
        let key = format!("session:{}", session_id);

        // Store session data with user ID prefix for easy lookup
        let value = format!("{}:{}", user_id, base64::encode(data));
        self.db.put(key.as_bytes(), value.as_bytes())?;

        // Also create a reverse index for finding sessions by user
        let user_sessions_key = format!("user_sessions:{}", user_id);
        let existing = self.db.get(user_sessions_key.as_bytes())?
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_default();

        let updated = if existing.is_empty() {
            session_id.clone()
        } else {
            format!("{},{}", existing, session_id)
        };

        self.db.put(user_sessions_key.as_bytes(), updated.as_bytes())?;

        // Store session expiry (24 hours from now)
        let expiry = SystemTime::now() + Duration::from_secs(86400);
        let expiry_key = format!("session_expiry:{}", session_id);
        let expiry_millis = expiry.duration_since(UNIX_EPOCH).unwrap().as_millis();
        self.db.put(expiry_key.as_bytes(), expiry_millis.to_string().as_bytes())?;

        Ok(session_id)
    }

    pub fn get_session(&self, session_id: &str) -> Result<Option<(String, Vec<u8>)>> {
        // Check if session is expired first
        let expiry_key = format!("session_expiry:{}", session_id);
        if let Some(expiry_data) = self.db.get(expiry_key.as_bytes())? {
            let expiry_millis: u128 = String::from_utf8_lossy(&expiry_data)
                .parse()
                .unwrap_or(0);

            let now_millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();

            if now_millis > expiry_millis {
                // Session expired, clean it up
                self.delete_session(session_id)?;
                return Ok(None);
            }
        }

        let key = format!("session:{}", session_id);
        if let Some(value) = self.db.get(key.as_bytes())? {
            let value_str = String::from_utf8_lossy(&value);
            if let Some(colon_pos) = value_str.find(':') {
                let user_id = value_str[..colon_pos].to_string();
                let data = base64::decode(&value_str[colon_pos + 1..])
                    .unwrap_or_default();
                return Ok(Some((user_id, data)));
            }
        }
        Ok(None)
    }

    pub fn delete_session(&self, session_id: &str) -> Result<()> {
        // Get user ID from session first
        let session_key = format!("session:{}", session_id);
        let user_id = if let Some(value) = self.db.get(session_key.as_bytes())? {
            let value_str = String::from_utf8_lossy(&value);
            value_str.split(':').next().map(|s| s.to_string())
        } else {
            None
        };

        // Delete session data
        self.db.delete(session_key.as_bytes())?;

        // Delete expiry data
        let expiry_key = format!("session_expiry:{}", session_id);
        self.db.delete(expiry_key.as_bytes())?;

        // Update user's session list
        if let Some(user_id) = user_id {
            let user_sessions_key = format!("user_sessions:{}", user_id);
            if let Some(sessions_data) = self.db.get(user_sessions_key.as_bytes())? {
                let sessions = String::from_utf8_lossy(&sessions_data);
                let updated_sessions: Vec<&str> = sessions
                    .split(',')
                    .filter(|s| *s != session_id)
                    .collect();

                if updated_sessions.is_empty() {
                    self.db.delete(user_sessions_key.as_bytes())?;
                } else {
                    let updated = updated_sessions.join(",");
                    self.db.put(user_sessions_key.as_bytes(), updated.as_bytes())?;
                }
            }
        }

        Ok(())
    }

    pub fn get_user_sessions(&self, user_id: &str) -> Result<Vec<String>> {
        let key = format!("user_sessions:{}", user_id);
        if let Some(data) = self.db.get(key.as_bytes())? {
            let sessions_str = String::from_utf8_lossy(&data);
            Ok(sessions_str.split(',').map(|s| s.to_string()).collect())
        } else {
            Ok(Vec::new())
        }
    }

    pub fn cleanup_expired_sessions(&self) -> Result<u32> {
        let mut count = 0;
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        // Scan all session expiry keys
        let prefix = b"session_expiry:";
        let iter = self.db.scan_prefix(prefix)?;

        for result in iter {
            let (key, value) = result?;
            let expiry_millis: u128 = String::from_utf8_lossy(&value)
                .parse()
                .unwrap_or(0);

            if now_millis > expiry_millis {
                // Extract session ID from key
                let key_str = String::from_utf8_lossy(&key);
                if let Some(session_id) = key_str.strip_prefix("session_expiry:") {
                    self.delete_session(session_id)?;
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    pub fn count_sessions(&self) -> Result<usize> {
        let mut count = 0;
        let prefix = b"session:";
        let iter = self.db.scan_prefix(prefix)?;

        for _ in iter {
            count += 1;
        }

        Ok(count)
    }
}

fn generate_session_id() -> String {
    // Simple session ID generation using timestamp and random component
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    // In a real application, use a proper random generator
    let random_component = (timestamp ^ (timestamp >> 32)) & 0xFFFFFFFF;

    format!("sess_{}_{:08x}", timestamp, random_component)
}

fn main() -> Result<()> {
    println!("Lightning DB Session Store Example");
    println!("==================================\n");

    let store = SessionStore::new("session_store_db")?;

    // Create some sessions for different users
    println!("Creating sessions...");

    let session1 = store.create_session("user123", b"{'theme': 'dark', 'lang': 'en'}")?;
    println!("Created session for user123: {}", session1);

    let session2 = store.create_session("user456", b"{'theme': 'light', 'lang': 'es'}")?;
    println!("Created session for user456: {}", session2);

    let session3 = store.create_session("user123", b"{'theme': 'dark', 'lang': 'fr'}")?;
    println!("Created another session for user123: {}", session3);

    // Retrieve a session
    println!("\nRetrieving session {}...", session1);
    if let Some((user_id, data)) = store.get_session(&session1)? {
        println!("  User: {}", user_id);
        println!("  Data: {}", String::from_utf8_lossy(&data));
    }

    // Get all sessions for a user
    println!("\nSessions for user123:");
    let user_sessions = store.get_user_sessions("user123")?;
    for session_id in &user_sessions {
        println!("  - {}", session_id);
    }

    // Count total sessions
    let total = store.count_sessions()?;
    println!("\nTotal active sessions: {}", total);

    // Delete a session
    println!("\nDeleting session {}...", session1);
    store.delete_session(&session1)?;

    // Verify deletion
    println!("Sessions for user123 after deletion:");
    let user_sessions = store.get_user_sessions("user123")?;
    for session_id in &user_sessions {
        println!("  - {}", session_id);
    }

    // Clean up expired sessions (none should be expired yet)
    println!("\nCleaning up expired sessions...");
    let cleaned = store.cleanup_expired_sessions()?;
    println!("Cleaned up {} expired sessions", cleaned);

    // Final count
    let final_count = store.count_sessions()?;
    println!("\nFinal active sessions: {}", final_count);

    println!("\nSession store example completed successfully!");

    Ok(())
}