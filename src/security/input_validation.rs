use crate::security::{SecurityError, SecurityResult};
use regex::Regex;
use std::collections::HashSet;
use std::sync::LazyLock;

static SQL_INJECTION_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"(?i)\b(union|select|insert|update|delete|drop|create|alter|exec|execute)\b")
            .unwrap(),
        Regex::new(r#"(?i)['";]"#).unwrap(),
        Regex::new(r"(?i)--").unwrap(),
        Regex::new(r"/\*.*\*/").unwrap(),
        Regex::new(r"(?i)\bor\s+\d+\s*=\s*\d+").unwrap(),
        Regex::new(r"(?i)\band\s+\d+\s*=\s*\d+").unwrap(),
    ]
});

static PATH_TRAVERSAL_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"\.\.[/\\]").unwrap(),
        Regex::new(r"[/\\]\.\.").unwrap(),
        Regex::new(r"\\\\").unwrap(),
        Regex::new(r"//").unwrap(),
        Regex::new(r"%2e%2e").unwrap(),
        Regex::new(r"%252e%252e").unwrap(),
        Regex::new(r"\.{3,}").unwrap(),
    ]
});

static COMMAND_INJECTION_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"[;&|`$()]").unwrap(),
        Regex::new(r"(?i)\b(cat|ls|pwd|whoami|id|ps|netstat|ifconfig|ping|nslookup|curl|wget)\b")
            .unwrap(),
        Regex::new(r"[\r\n]").unwrap(),
    ]
});

static XSS_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"(?i)<\s*script").unwrap(),
        Regex::new(r"(?i)javascript:").unwrap(),
        Regex::new(r"(?i)on\w+\s*=").unwrap(),
        Regex::new(r"(?i)<\s*iframe").unwrap(),
        Regex::new(r"(?i)<\s*object").unwrap(),
        Regex::new(r"(?i)<\s*embed").unwrap(),
        Regex::new(r"(?i)expression\s*\(").unwrap(),
        Regex::new(r"(?i)eval\s*\(").unwrap(),
    ]
});

static BLOCKED_CHARS: LazyLock<HashSet<char>> = LazyLock::new(|| {
    [
        '\0', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08', '\x0e', '\x0f',
        '\x10', '\x11', '\x12', '\x13', '\x14', '\x15', '\x16', '\x17', '\x18', '\x19', '\x1a',
        '\x1b', '\x1c', '\x1d', '\x1e', '\x1f', '\x7f',
    ]
    .iter()
    .copied()
    .collect()
});

pub struct InputValidator {
    max_key_size: usize,
    max_value_size: usize,
    max_path_length: usize,
    strict_mode: bool,
}

impl InputValidator {
    pub fn new(max_key_size: usize, max_value_size: usize, strict_mode: bool) -> Self {
        Self {
            max_key_size,
            max_value_size,
            max_path_length: 4096,
            strict_mode,
        }
    }

    pub fn validate_key(&self, key: &[u8]) -> SecurityResult<()> {
        if key.is_empty() {
            return Err(SecurityError::InputValidationFailed(
                "Key cannot be empty".to_string(),
            ));
        }

        if key.len() > self.max_key_size {
            return Err(SecurityError::InputValidationFailed(format!(
                "Key size {} exceeds maximum allowed size {}",
                key.len(),
                self.max_key_size
            )));
        }

        if self.strict_mode {
            self.validate_no_control_chars(key, "key")?;
        }

        let key_str = String::from_utf8_lossy(key);
        self.validate_no_injection_patterns(&key_str, "key")?;
        self.validate_no_path_traversal(&key_str, "key")?;

        Ok(())
    }

    pub fn validate_value(&self, value: &[u8]) -> SecurityResult<()> {
        if value.len() > self.max_value_size {
            return Err(SecurityError::InputValidationFailed(format!(
                "Value size {} exceeds maximum allowed size {}",
                value.len(),
                self.max_value_size
            )));
        }

        if self.strict_mode {
            self.validate_no_control_chars(value, "value")?;
        }

        let value_str = String::from_utf8_lossy(value);
        self.validate_no_injection_patterns(&value_str, "value")?;
        self.validate_no_xss_patterns(&value_str, "value")?;

        Ok(())
    }

    pub fn validate_path(&self, path: &str) -> SecurityResult<()> {
        if path.is_empty() {
            return Err(SecurityError::InputValidationFailed(
                "Path cannot be empty".to_string(),
            ));
        }

        if path.len() > self.max_path_length {
            return Err(SecurityError::InputValidationFailed(format!(
                "Path length {} exceeds maximum allowed length {}",
                path.len(),
                self.max_path_length
            )));
        }

        self.validate_no_path_traversal(path, "path")?;
        self.validate_no_injection_patterns(path, "path")?;

        if path.contains('\0') {
            return Err(SecurityError::InputValidationFailed(
                "Path contains null byte".to_string(),
            ));
        }

        if cfg!(windows)
            && path
                .chars()
                .any(|c| matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
        {
            return Err(SecurityError::InputValidationFailed(
                "Path contains invalid Windows characters".to_string(),
            ));
        }

        Ok(())
    }

    pub fn validate_string_input(&self, input: &str, context: &str) -> SecurityResult<()> {
        if input.len() > self.max_value_size {
            return Err(SecurityError::InputValidationFailed(format!(
                "{} length {} exceeds maximum allowed size {}",
                context,
                input.len(),
                self.max_value_size
            )));
        }

        self.validate_no_injection_patterns(input, context)?;
        self.validate_no_xss_patterns(input, context)?;
        self.validate_no_command_injection(input, context)?;

        if self.strict_mode && input.chars().any(|c| BLOCKED_CHARS.contains(&c)) {
            return Err(SecurityError::InputValidationFailed(format!(
                "{} contains blocked control characters",
                context
            )));
        }

        Ok(())
    }

    pub fn sanitize_for_logging(&self, input: &str) -> String {
        input
            .chars()
            .filter(|c| !BLOCKED_CHARS.contains(c))
            .take(256)
            .collect()
    }

    fn validate_no_control_chars(&self, data: &[u8], context: &str) -> SecurityResult<()> {
        if data.iter().any(|&b| b < 32 && b != 9 && b != 10 && b != 13) {
            return Err(SecurityError::InputValidationFailed(format!(
                "{} contains control characters",
                context
            )));
        }
        Ok(())
    }

    fn validate_no_injection_patterns(&self, input: &str, context: &str) -> SecurityResult<()> {
        for pattern in SQL_INJECTION_PATTERNS.iter() {
            if pattern.is_match(input) {
                return Err(SecurityError::InputValidationFailed(format!(
                    "{} contains potential SQL injection pattern",
                    context
                )));
            }
        }
        Ok(())
    }

    fn validate_no_path_traversal(&self, input: &str, context: &str) -> SecurityResult<()> {
        for pattern in PATH_TRAVERSAL_PATTERNS.iter() {
            if pattern.is_match(input) {
                return Err(SecurityError::InputValidationFailed(format!(
                    "{} contains potential path traversal pattern",
                    context
                )));
            }
        }
        Ok(())
    }

    fn validate_no_xss_patterns(&self, input: &str, context: &str) -> SecurityResult<()> {
        for pattern in XSS_PATTERNS.iter() {
            if pattern.is_match(input) {
                return Err(SecurityError::InputValidationFailed(format!(
                    "{} contains potential XSS pattern",
                    context
                )));
            }
        }
        Ok(())
    }

    fn validate_no_command_injection(&self, input: &str, context: &str) -> SecurityResult<()> {
        for pattern in COMMAND_INJECTION_PATTERNS.iter() {
            if pattern.is_match(input) {
                return Err(SecurityError::InputValidationFailed(format!(
                    "{} contains potential command injection pattern",
                    context
                )));
            }
        }
        Ok(())
    }
}

pub fn validate_utf8_safe(data: &[u8]) -> SecurityResult<String> {
    match std::str::from_utf8(data) {
        Ok(s) => Ok(s.to_string()),
        Err(_) => Err(SecurityError::InputValidationFailed(
            "Invalid UTF-8 sequence".to_string(),
        )),
    }
}

pub fn escape_for_json(input: &str) -> String {
    input
        .chars()
        .map(|c| match c {
            '"' => "\\\"".to_string(),
            '\\' => "\\\\".to_string(),
            '\n' => "\\n".to_string(),
            '\r' => "\\r".to_string(),
            '\t' => "\\t".to_string(),
            c if c.is_control() => format!("\\u{:04x}", c as u32),
            c => c.to_string(),
        })
        .collect()
}

pub fn escape_for_sql(input: &str) -> String {
    input.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_validation() {
        let validator = InputValidator::new(10, 100, true);

        assert!(validator.validate_key(b"valid_key").is_ok());
        assert!(validator.validate_key(b"").is_err());
        assert!(validator.validate_key(b"this_key_is_too_long").is_err());
        assert!(validator
            .validate_key(b"key'; DROP TABLE users; --")
            .is_err());
    }

    #[test]
    fn test_sql_injection_detection() {
        let validator = InputValidator::new(1000, 1000, true);

        assert!(validator
            .validate_string_input("'; DROP TABLE users; --", "test")
            .is_err());
        assert!(validator
            .validate_string_input("admin' OR '1'='1", "test")
            .is_err());
        assert!(validator
            .validate_string_input("UNION SELECT * FROM passwords", "test")
            .is_err());
        assert!(validator
            .validate_string_input("normal text", "test")
            .is_ok());
    }

    #[test]
    fn test_path_traversal_detection() {
        let validator = InputValidator::new(1000, 1000, true);

        assert!(validator.validate_path("../../../etc/passwd").is_err());
        assert!(validator
            .validate_path("..\\..\\windows\\system32")
            .is_err());
        assert!(validator.validate_path("%2e%2e%2f%2e%2e%2f").is_err());
        assert!(validator.validate_path("normal/path/file.txt").is_ok());
    }

    #[test]
    fn test_xss_detection() {
        let validator = InputValidator::new(1000, 1000, true);

        assert!(validator
            .validate_string_input("<script>alert('xss')</script>", "test")
            .is_err());
        assert!(validator
            .validate_string_input("javascript:alert(1)", "test")
            .is_err());
        assert!(validator
            .validate_string_input("<img onerror=alert(1)>", "test")
            .is_err());
        assert!(validator
            .validate_string_input("normal content", "test")
            .is_ok());
    }
}
