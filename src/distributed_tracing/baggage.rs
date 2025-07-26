//! Baggage Management
//!
//! Handles baggage (key-value pairs) that propagate across trace boundaries
//! following W3C Baggage specification for distributed context.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Baggage item with metadata
#[derive(Debug, Clone, PartialEq)]
pub struct BaggageItem {
    pub value: String,
    pub metadata: HashMap<String, String>,
}

impl BaggageItem {
    /// Create a new baggage item
    pub fn new(value: String) -> Self {
        Self {
            value,
            metadata: HashMap::new(),
        }
    }
    
    /// Create baggage item with metadata
    pub fn with_metadata(value: String, metadata: HashMap<String, String>) -> Self {
        Self { value, metadata }
    }
    
    /// Add metadata to baggage item
    pub fn add_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
    
    /// Get metadata value
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
    
    /// Set TTL for baggage item
    pub fn with_ttl(self, seconds: u64) -> Self {
        self.add_metadata("ttl".to_string(), seconds.to_string())
    }
    
    /// Set priority for baggage item
    pub fn with_priority(self, priority: u8) -> Self {
        self.add_metadata("priority".to_string(), priority.to_string())
    }
    
    /// Mark as system baggage (internal use)
    pub fn as_system(self) -> Self {
        self.add_metadata("system".to_string(), "true".to_string())
    }
    
    /// Check if item is system baggage
    pub fn is_system(&self) -> bool {
        self.get_metadata("system") == Some(&"true".to_string())
    }
    
    /// Get TTL if set
    pub fn ttl(&self) -> Option<u64> {
        self.get_metadata("ttl")
            .and_then(|ttl| ttl.parse().ok())
    }
    
    /// Get priority if set
    pub fn priority(&self) -> Option<u8> {
        self.get_metadata("priority")
            .and_then(|p| p.parse().ok())
    }
}

/// Baggage container with advanced management features
#[derive(Debug, Clone)]
pub struct Baggage {
    items: HashMap<String, BaggageItem>,
    max_size: usize,
    max_key_length: usize,
    max_value_length: usize,
}

impl Baggage {
    /// Create a new empty baggage container
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
            max_size: 64,           // Maximum 64 baggage items
            max_key_length: 256,    // Maximum key length 256 chars
            max_value_length: 4096, // Maximum value length 4KB
        }
    }
    
    /// Create baggage with custom limits
    pub fn with_limits(max_size: usize, max_key_length: usize, max_value_length: usize) -> Self {
        Self {
            items: HashMap::new(),
            max_size,
            max_key_length,
            max_value_length,
        }
    }
    
    /// Set a baggage item
    pub fn set(&mut self, key: String, item: BaggageItem) -> Result<(), BaggageError> {
        // Validate key length
        if key.len() > self.max_key_length {
            return Err(BaggageError::KeyTooLong(key.len(), self.max_key_length));
        }
        
        // Validate value length
        if item.value.len() > self.max_value_length {
            return Err(BaggageError::ValueTooLong(item.value.len(), self.max_value_length));
        }
        
        // Check size limit (unless updating existing key)
        if !self.items.contains_key(&key) && self.items.len() >= self.max_size {
            return Err(BaggageError::TooManyItems(self.items.len(), self.max_size));
        }
        
        // Validate key format (no special characters that interfere with serialization)
        if !is_valid_baggage_key(&key) {
            return Err(BaggageError::InvalidKey(key));
        }
        
        self.items.insert(key, item);
        Ok(())
    }
    
    /// Set a simple baggage value
    pub fn set_value(&mut self, key: String, value: String) -> Result<(), BaggageError> {
        self.set(key, BaggageItem::new(value))
    }
    
    /// Get a baggage item
    pub fn get(&self, key: &str) -> Option<&BaggageItem> {
        self.items.get(key)
    }
    
    /// Get a baggage value
    pub fn get_value(&self, key: &str) -> Option<&String> {
        self.items.get(key).map(|item| &item.value)
    }
    
    /// Remove a baggage item
    pub fn remove(&mut self, key: &str) -> Option<BaggageItem> {
        self.items.remove(key)
    }
    
    /// Check if baggage contains key
    pub fn contains_key(&self, key: &str) -> bool {
        self.items.contains_key(key)
    }
    
    /// Get all keys
    pub fn keys(&self) -> Vec<&String> {
        self.items.keys().collect()
    }
    
    /// Get number of items
    pub fn len(&self) -> usize {
        self.items.len()
    }
    
    /// Check if baggage is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    
    /// Clear all baggage
    pub fn clear(&mut self) {
        self.items.clear();
    }
    
    /// Merge another baggage into this one
    pub fn merge(&mut self, other: &Baggage) -> Result<(), BaggageError> {
        for (key, item) in &other.items {
            self.set(key.clone(), item.clone())?;
        }
        Ok(())
    }
    
    /// Filter baggage by predicate
    pub fn filter<F>(&self, predicate: F) -> Baggage
    where
        F: Fn(&String, &BaggageItem) -> bool,
    {
        let mut filtered = Baggage::with_limits(
            self.max_size,
            self.max_key_length,
            self.max_value_length,
        );
        
        for (key, item) in &self.items {
            if predicate(key, item) {
                let _ = filtered.set(key.clone(), item.clone());
            }
        }
        
        filtered
    }
    
    /// Get only system baggage
    pub fn system_baggage(&self) -> Baggage {
        self.filter(|_, item| item.is_system())
    }
    
    /// Get only user baggage (non-system)
    pub fn user_baggage(&self) -> Baggage {
        self.filter(|_, item| !item.is_system())
    }
    
    /// Get baggage by priority (higher numbers = higher priority)
    pub fn high_priority_baggage(&self, min_priority: u8) -> Baggage {
        self.filter(|_, item| {
            item.priority().map_or(false, |p| p >= min_priority)
        })
    }
    
    /// Estimate serialized size
    pub fn estimated_size(&self) -> usize {
        self.items.iter()
            .map(|(k, v)| k.len() + v.value.len() + 10) // Rough estimate including separators
            .sum()
    }
}

impl Default for Baggage {
    fn default() -> Self {
        Self::new()
    }
}

/// Baggage errors
#[derive(Debug, Clone, PartialEq)]
pub enum BaggageError {
    KeyTooLong(usize, usize),
    ValueTooLong(usize, usize),
    TooManyItems(usize, usize),
    InvalidKey(String),
    ParseError(String),
}

impl std::fmt::Display for BaggageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BaggageError::KeyTooLong(actual, max) => {
                write!(f, "Baggage key too long: {} characters (max: {})", actual, max)
            }
            BaggageError::ValueTooLong(actual, max) => {
                write!(f, "Baggage value too long: {} characters (max: {})", actual, max)
            }
            BaggageError::TooManyItems(actual, max) => {
                write!(f, "Too many baggage items: {} (max: {})", actual, max)
            }
            BaggageError::InvalidKey(key) => {
                write!(f, "Invalid baggage key: {}", key)
            }
            BaggageError::ParseError(msg) => {
                write!(f, "Baggage parse error: {}", msg)
            }
        }
    }
}

impl std::error::Error for BaggageError {}

/// Baggage serializer/deserializer following W3C specification
pub struct BaggageSerializer;

impl BaggageSerializer {
    /// Serialize baggage to W3C baggage header format
    /// Format: key1=value1;metadata1=val1,key2=value2;metadata2=val2
    pub fn serialize(baggage: &Baggage) -> String {
        baggage.items
            .iter()
            .map(|(key, item)| {
                let mut entry = format!("{}={}", 
                    percent_encode(key), 
                    percent_encode(&item.value)
                );
                
                // Add metadata
                for (meta_key, meta_value) in &item.metadata {
                    entry.push_str(&format!(";{}={}", 
                        percent_encode(meta_key),
                        percent_encode(meta_value)
                    ));
                }
                
                entry
            })
            .collect::<Vec<_>>()
            .join(",")
    }
    
    /// Deserialize baggage from W3C baggage header format
    pub fn deserialize(header: &str) -> Result<Baggage, BaggageError> {
        let mut baggage = Baggage::new();
        
        if header.trim().is_empty() {
            return Ok(baggage);
        }
        
        for entry in header.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            
            // Split by semicolon to separate key=value from metadata
            let parts: Vec<&str> = entry.split(';').collect();
            if parts.is_empty() {
                continue;
            }
            
            // Parse key=value
            let key_value = parts[0];
            let (key, value) = parse_key_value(key_value)?;
            
            let mut item = BaggageItem::new(value);
            
            // Parse metadata
            for metadata_part in parts.iter().skip(1) {
                let (meta_key, meta_value) = parse_key_value(metadata_part)?;
                item.metadata.insert(meta_key, meta_value);
            }
            
            baggage.set(key, item)?;
        }
        
        Ok(baggage)
    }
}

/// Baggage manager for thread-safe baggage operations
pub struct BaggageManager {
    baggage: Arc<RwLock<Baggage>>,
}

impl BaggageManager {
    /// Create a new baggage manager
    pub fn new() -> Self {
        Self {
            baggage: Arc::new(RwLock::new(Baggage::new())),
        }
    }
    
    /// Create with initial baggage
    pub fn with_baggage(baggage: Baggage) -> Self {
        Self {
            baggage: Arc::new(RwLock::new(baggage)),
        }
    }
    
    /// Set baggage item
    pub fn set(&self, key: String, item: BaggageItem) -> Result<(), BaggageError> {
        if let Ok(mut baggage) = self.baggage.write() {
            baggage.set(key, item)
        } else {
            Err(BaggageError::ParseError("Failed to acquire write lock".to_string()))
        }
    }
    
    /// Get baggage item
    pub fn get(&self, key: &str) -> Option<BaggageItem> {
        if let Ok(baggage) = self.baggage.read() {
            baggage.get(key).cloned()
        } else {
            None
        }
    }
    
    /// Get baggage value
    pub fn get_value(&self, key: &str) -> Option<String> {
        if let Ok(baggage) = self.baggage.read() {
            baggage.get_value(key).cloned()
        } else {
            None
        }
    }
    
    /// Remove baggage item
    pub fn remove(&self, key: &str) -> Option<BaggageItem> {
        if let Ok(mut baggage) = self.baggage.write() {
            baggage.remove(key)
        } else {
            None
        }
    }
    
    /// Get all baggage
    pub fn get_all(&self) -> Baggage {
        if let Ok(baggage) = self.baggage.read() {
            baggage.clone()
        } else {
            Baggage::new()
        }
    }
    
    /// Clear all baggage
    pub fn clear(&self) {
        if let Ok(mut baggage) = self.baggage.write() {
            baggage.clear();
        }
    }
    
    /// Serialize to header format
    pub fn serialize(&self) -> String {
        BaggageSerializer::serialize(&self.get_all())
    }
}

impl Default for BaggageManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Database-specific baggage utilities
pub struct DatabaseBaggage;

impl DatabaseBaggage {
    /// Create baggage for database operations
    pub fn for_operation(operation: &str, table: Option<&str>) -> Baggage {
        let mut baggage = Baggage::new();
        
        let _ = baggage.set_value("db.operation".to_string(), operation.to_string());
        
        if let Some(table_name) = table {
            let _ = baggage.set_value("db.table".to_string(), table_name.to_string());
        }
        
        let _ = baggage.set_value("db.system".to_string(), "lightning_db".to_string());
        
        baggage
    }
    
    /// Create baggage for transaction
    pub fn for_transaction(tx_id: u64, isolation_level: &str) -> Baggage {
        let mut baggage = Baggage::new();
        
        let _ = baggage.set("db.transaction_id".to_string(), 
            BaggageItem::new(tx_id.to_string()).as_system()
        );
        let _ = baggage.set_value("db.isolation_level".to_string(), isolation_level.to_string());
        let _ = baggage.set_value("db.operation_type".to_string(), "transaction".to_string());
        
        baggage
    }
    
    /// Create baggage for user context
    pub fn for_user(user_id: &str, session_id: Option<&str>) -> Baggage {
        let mut baggage = Baggage::new();
        
        let _ = baggage.set("user.id".to_string(), 
            BaggageItem::new(user_id.to_string()).with_priority(10)
        );
        
        if let Some(session) = session_id {
            let _ = baggage.set("user.session_id".to_string(),
                BaggageItem::new(session.to_string()).with_priority(5)
            );
        }
        
        baggage
    }
    
    /// Create baggage for request context
    pub fn for_request(request_id: &str, client_ip: Option<&str>) -> Baggage {
        let mut baggage = Baggage::new();
        
        let _ = baggage.set("request.id".to_string(),
            BaggageItem::new(request_id.to_string()).with_priority(8)
        );
        
        if let Some(ip) = client_ip {
            let _ = baggage.set_value("request.client_ip".to_string(), ip.to_string());
        }
        
        baggage
    }
}

/// Validate baggage key format
fn is_valid_baggage_key(key: &str) -> bool {
    !key.is_empty() 
        && key.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
        && !key.starts_with('-')
        && !key.ends_with('-')
}

/// Parse key=value pair with percent decoding
fn parse_key_value(input: &str) -> Result<(String, String), BaggageError> {
    if let Some(eq_pos) = input.find('=') {
        let key = percent_decode(&input[..eq_pos]);
        let value = percent_decode(&input[eq_pos + 1..]);
        Ok((key, value))
    } else {
        Err(BaggageError::ParseError(format!("Invalid key=value format: {}", input)))
    }
}

/// Simple percent encoding for baggage values
fn percent_encode(input: &str) -> String {
    input.chars()
        .map(|c| match c {
            ' ' => "%20".to_string(),
            ',' => "%2C".to_string(),
            ';' => "%3B".to_string(),
            '=' => "%3D".to_string(),
            '%' => "%25".to_string(),
            _ => c.to_string(),
        })
        .collect()
}

/// Simple percent decoding for baggage values
fn percent_decode(input: &str) -> String {
    let mut result = String::new();
    let mut chars = input.chars().peekable();
    
    while let Some(c) = chars.next() {
        if c == '%' {
            // Read next two characters for hex code
            if let (Some(h1), Some(h2)) = (chars.next(), chars.next()) {
                if let Ok(byte) = u8::from_str_radix(&format!("{}{}", h1, h2), 16) {
                    result.push(byte as char);
                } else {
                    // Invalid hex, keep original
                    result.push('%');
                    result.push(h1);
                    result.push(h2);
                }
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_baggage_item() {
        let item = BaggageItem::new("test_value".to_string())
            .with_priority(5)
            .with_ttl(3600)
            .as_system();
        
        assert_eq!(item.value, "test_value");
        assert_eq!(item.priority(), Some(5));
        assert_eq!(item.ttl(), Some(3600));
        assert!(item.is_system());
    }

    #[test]
    fn test_baggage_operations() {
        let mut baggage = Baggage::new();
        
        let item = BaggageItem::new("value1".to_string());
        baggage.set("key1".to_string(), item).unwrap();
        
        assert_eq!(baggage.get_value("key1"), Some(&"value1".to_string()));
        assert!(baggage.contains_key("key1"));
        assert_eq!(baggage.len(), 1);
        
        baggage.remove("key1");
        assert!(!baggage.contains_key("key1"));
        assert_eq!(baggage.len(), 0);
    }

    #[test]
    fn test_baggage_limits() {
        let mut baggage = Baggage::with_limits(2, 10, 20);
        
        // Test key too long
        let result = baggage.set_value("very_long_key".to_string(), "value".to_string());
        assert!(matches!(result, Err(BaggageError::KeyTooLong(_, _))));
        
        // Test value too long
        let result = baggage.set_value("key".to_string(), "very_long_value_that_exceeds_limit".to_string());
        assert!(matches!(result, Err(BaggageError::ValueTooLong(_, _))));
        
        // Test too many items
        baggage.set_value("key1".to_string(), "value1".to_string()).unwrap();
        baggage.set_value("key2".to_string(), "value2".to_string()).unwrap();
        let result = baggage.set_value("key3".to_string(), "value3".to_string());
        assert!(matches!(result, Err(BaggageError::TooManyItems(_, _))));
    }

    #[test]
    fn test_baggage_serialization() {
        let mut baggage = Baggage::new();
        
        let item1 = BaggageItem::new("value1".to_string())
            .add_metadata("priority".to_string(), "5".to_string());
        baggage.set("key1".to_string(), item1).unwrap();
        
        baggage.set_value("key2".to_string(), "value with spaces".to_string()).unwrap();
        
        let serialized = BaggageSerializer::serialize(&baggage);
        let deserialized = BaggageSerializer::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.get_value("key1"), Some(&"value1".to_string()));
        assert_eq!(deserialized.get_value("key2"), Some(&"value with spaces".to_string()));
        assert_eq!(deserialized.get("key1").unwrap().get_metadata("priority"), 
                   Some(&"5".to_string()));
    }

    #[test]
    fn test_baggage_filtering() {
        let mut baggage = Baggage::new();
        
        baggage.set("user_key".to_string(), 
            BaggageItem::new("user_value".to_string()).with_priority(10)
        ).unwrap();
        
        baggage.set("system_key".to_string(),
            BaggageItem::new("system_value".to_string()).as_system()
        ).unwrap();
        
        let user_baggage = baggage.user_baggage();
        let system_baggage = baggage.system_baggage();
        let high_priority = baggage.high_priority_baggage(8);
        
        assert_eq!(user_baggage.len(), 1);
        assert_eq!(system_baggage.len(), 1);
        assert_eq!(high_priority.len(), 1);
        assert!(user_baggage.contains_key("user_key"));
        assert!(system_baggage.contains_key("system_key"));
        assert!(high_priority.contains_key("user_key"));
    }

    #[test]
    fn test_baggage_manager() {
        let manager = BaggageManager::new();
        
        let item = BaggageItem::new("test_value".to_string());
        manager.set("test_key".to_string(), item).unwrap();
        
        assert_eq!(manager.get_value("test_key"), Some("test_value".to_string()));
        
        let serialized = manager.serialize();
        assert!(!serialized.is_empty());
    }

    #[test]
    fn test_database_baggage() {
        let op_baggage = DatabaseBaggage::for_operation("SELECT", Some("users"));
        assert_eq!(op_baggage.get_value("db.operation"), Some(&"SELECT".to_string()));
        assert_eq!(op_baggage.get_value("db.table"), Some(&"users".to_string()));
        
        let tx_baggage = DatabaseBaggage::for_transaction(42, "READ_COMMITTED");
        assert_eq!(tx_baggage.get_value("db.transaction_id"), Some(&"42".to_string()));
        assert_eq!(tx_baggage.get_value("db.isolation_level"), Some(&"READ_COMMITTED".to_string()));
        
        let user_baggage = DatabaseBaggage::for_user("user123", Some("session456"));
        assert_eq!(user_baggage.get_value("user.id"), Some(&"user123".to_string()));
        assert_eq!(user_baggage.get_value("user.session_id"), Some(&"session456".to_string()));
    }

    #[test]
    fn test_percent_encoding() {
        assert_eq!(percent_encode("hello world"), "hello%20world");
        assert_eq!(percent_encode("key=value,more"), "key%3Dvalue%2Cmore");
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("key%3Dvalue%2Cmore"), "key=value,more");
    }

    #[test]
    fn test_key_validation() {
        assert!(is_valid_baggage_key("valid_key"));
        assert!(is_valid_baggage_key("valid-key"));
        assert!(is_valid_baggage_key("valid.key"));
        assert!(is_valid_baggage_key("valid123"));
        
        assert!(!is_valid_baggage_key("-invalid"));
        assert!(!is_valid_baggage_key("invalid-"));
        assert!(!is_valid_baggage_key(""));
        assert!(!is_valid_baggage_key("invalid key"));
        assert!(!is_valid_baggage_key("invalid/key"));
    }
}