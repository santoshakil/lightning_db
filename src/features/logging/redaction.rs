use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Redactor {
    patterns: Vec<Regex>,
    replacement: String,
    key_patterns: Vec<Regex>,
    redact_keys: bool,
    redact_values: bool,
}

impl Redactor {
    pub fn new(patterns: Vec<String>, replacement: String) -> Self {
        let compiled_patterns: Vec<Regex> = patterns
            .into_iter()
            .filter_map(|p| Regex::new(&p).ok())
            .collect();
            
        Self {
            patterns: compiled_patterns,
            replacement,
            key_patterns: Vec::new(),
            redact_keys: false,
            redact_values: true,
        }
    }
    
    pub fn with_key_patterns(mut self, patterns: Vec<String>) -> Self {
        self.key_patterns = patterns
            .into_iter()
            .filter_map(|p| Regex::new(&p).ok())
            .collect();
        self
    }
    
    pub fn with_redaction_settings(mut self, redact_keys: bool, redact_values: bool) -> Self {
        self.redact_keys = redact_keys;
        self.redact_values = redact_values;
        self
    }
    
    pub fn redact(&self, text: &str) -> String {
        let mut result = text.to_string();
        
        for pattern in &self.patterns {
            result = pattern.replace_all(&result, &self.replacement).to_string();
        }
        
        result
    }
    
    pub fn redact_json(&self, json_str: &str) -> String {
        match serde_json::from_str::<Value>(json_str) {
            Ok(value) => {
                let redacted = self.redact_json_value(&value);
                serde_json::to_string(&redacted).unwrap_or_else(|_| json_str.to_string())
            }
            Err(_) => self.redact(json_str),
        }
    }
    
    fn redact_json_value(&self, value: &Value) -> Value {
        match value {
            Value::Object(obj) => {
                let mut new_obj = serde_json::Map::new();
                for (key, val) in obj {
                    let new_key = if self.redact_keys {
                        self.redact_if_sensitive_key(key)
                    } else {
                        key.clone()
                    };
                    
                    let new_val = if self.redact_values && self.is_sensitive_key(key) {
                        Value::String(self.replacement.clone())
                    } else {
                        self.redact_json_value(val)
                    };
                    
                    new_obj.insert(new_key, new_val);
                }
                Value::Object(new_obj)
            }
            Value::Array(arr) => {
                Value::Array(arr.iter().map(|v| self.redact_json_value(v)).collect())
            }
            Value::String(s) => {
                if self.redact_values {
                    Value::String(self.redact(s))
                } else {
                    value.clone()
                }
            }
            _ => value.clone(),
        }
    }
    
    fn is_sensitive_key(&self, key: &str) -> bool {
        let sensitive_keys = [
            "password", "passwd", "pwd", "secret", "token", "key", "authorization",
            "auth", "credential", "cred", "private", "ssn", "social_security_number",
            "credit_card", "cc", "card_number", "cvv", "cvc", "pin", "api_key",
            "access_token", "refresh_token", "jwt", "session_id", "cookie",
            "email", "phone", "address", "zip", "postal_code",
        ];
        
        let key_lower = key.to_lowercase();
        sensitive_keys.iter().any(|&pattern| key_lower.contains(pattern)) ||
        self.key_patterns.iter().any(|pattern| pattern.is_match(key))
    }
    
    fn redact_if_sensitive_key(&self, key: &str) -> String {
        if self.is_sensitive_key(key) {
            format!("{}_{}", self.replacement, key.len())
        } else {
            key.to_string()
        }
    }
}

pub struct StructuredRedactor {
    field_redactors: HashMap<String, Box<dyn Fn(&str) -> String + Send + Sync>>,
    global_redactor: Redactor,
}

impl StructuredRedactor {
    pub fn new(global_redactor: Redactor) -> Self {
        Self {
            field_redactors: HashMap::new(),
            global_redactor,
        }
    }
    
    pub fn add_field_redactor<F>(mut self, field: String, redactor: F) -> Self
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        self.field_redactors.insert(field, Box::new(redactor));
        self
    }
    
    pub fn redact_structured(&self, data: &mut HashMap<String, String>) {
        for (key, value) in data.iter_mut() {
            if let Some(redactor) = self.field_redactors.get(key) {
                *value = redactor(value);
            } else {
                *value = self.global_redactor.redact(value);
            }
        }
    }
}

// Predefined redaction patterns
pub struct RedactionPatterns;

impl RedactionPatterns {
    pub fn credit_card() -> String {
        r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b".to_string()
    }
    
    pub fn email() -> String {
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b".to_string()
    }
    
    pub fn phone() -> String {
        r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b".to_string()
    }
    
    pub fn ssn() -> String {
        r"\b\d{3}-?\d{2}-?\d{4}\b".to_string()
    }
    
    pub fn ipv4() -> String {
        r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b".to_string()
    }
    
    pub fn jwt() -> String {
        r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b".to_string()
    }
    
    pub fn api_key() -> String {
        r"\b[A-Za-z0-9]{32,}\b".to_string()
    }
    
    pub fn default_patterns() -> Vec<String> {
        vec![
            Self::credit_card(),
            Self::email(),
            Self::phone(),
            Self::ssn(),
            Self::jwt(),
            Self::api_key(),
        ]
    }
}

// Sensitive data detector
pub struct SensitiveDataDetector {
    patterns: Vec<(String, Regex)>,
}

impl SensitiveDataDetector {
    pub fn new() -> Self {
        let patterns = vec![
            ("credit_card".to_string(), Regex::new(&RedactionPatterns::credit_card()).unwrap()),
            ("email".to_string(), Regex::new(&RedactionPatterns::email()).unwrap()),
            ("phone".to_string(), Regex::new(&RedactionPatterns::phone()).unwrap()),
            ("ssn".to_string(), Regex::new(&RedactionPatterns::ssn()).unwrap()),
            ("jwt".to_string(), Regex::new(&RedactionPatterns::jwt()).unwrap()),
        ];
        
        Self { patterns }
    }
    
    pub fn detect_sensitive_data(&self, text: &str) -> Vec<(String, Vec<String>)> {
        let mut detections = Vec::new();
        
        for (name, pattern) in &self.patterns {
            let matches: Vec<String> = pattern
                .find_iter(text)
                .map(|m| m.as_str().to_string())
                .collect();
            
            if !matches.is_empty() {
                detections.push((name.clone(), matches));
            }
        }
        
        detections
    }
    
    pub fn has_sensitive_data(&self, text: &str) -> bool {
        self.patterns.iter().any(|(_, pattern)| pattern.is_match(text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_redaction() {
        let redactor = Redactor::new(
            vec!["secret".to_string()],
            "[REDACTED]".to_string(),
        );
        
        let input = "This is a secret message";
        let expected = "This is a [REDACTED] message";
        assert_eq!(redactor.redact(input), expected);
    }
    
    #[test]
    fn test_credit_card_redaction() {
        let redactor = Redactor::new(
            vec![RedactionPatterns::credit_card()],
            "[CARD]".to_string(),
        );
        
        let input = "My card number is 4532-1234-5678-9012";
        let expected = "My card number is [CARD]";
        assert_eq!(redactor.redact(input), expected);
    }
    
    #[test]
    fn test_json_redaction() {
        let redactor = Redactor::new(
            vec![],
            "[REDACTED]".to_string(),
        ).with_redaction_settings(false, true);
        
        let json = r#"{"username": "john", "password": "secret123"}"#;
        let result = redactor.redact_json(json);
        
        assert!(result.contains("[REDACTED]"));
        assert!(result.contains("john"));
    }
    
    #[test]
    fn test_sensitive_data_detector() {
        let detector = SensitiveDataDetector::new();
        
        let text = "Contact me at john@example.com or call 555-123-4567";
        let detections = detector.detect_sensitive_data(text);
        
        assert_eq!(detections.len(), 2); // email and phone
        assert!(detector.has_sensitive_data(text));
    }
    
    #[test]
    fn test_structured_redactor() {
        let global_redactor = Redactor::new(vec![], "[REDACTED]".to_string());
        let structured = StructuredRedactor::new(global_redactor)
            .add_field_redactor("credit_card".to_string(), |_| "[CARD_REDACTED]".to_string());
        
        let mut data = HashMap::new();
        data.insert("username".to_string(), "john".to_string());
        data.insert("credit_card".to_string(), "4532123456789012".to_string());
        
        structured.redact_structured(&mut data);
        
        assert_eq!(data.get("credit_card").unwrap(), "[CARD_REDACTED]");
        assert_eq!(data.get("username").unwrap(), "john");
    }
}