//! Display and output utilities for CLI commands
//!
//! Provides formatting and output helpers for consistent CLI presentation.
//! Supports both text and JSON output formats for enterprise use.

#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{self, Write};

/// Format bytes as human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Format duration in human-readable form
pub fn format_duration(duration: std::time::Duration) -> String {
    let secs = duration.as_secs_f64();
    if secs < 0.001 {
        format!("{:.0} μs", duration.as_micros())
    } else if secs < 1.0 {
        format!("{:.1} ms", duration.as_millis())
    } else if secs < 60.0 {
        format!("{:.2} s", secs)
    } else {
        let mins = secs / 60.0;
        format!("{:.1} min", mins)
    }
}

/// Print success message with checkmark
pub fn print_success(message: &str) {
    println!("[OK] {}", message);
}

/// Print error message with X mark
pub fn print_error(message: &str) {
    eprintln!("[ERROR] {}", message);
}

/// Print info message
pub fn print_info(message: &str) {
    println!("[INFO] {}", message);
}

/// Print warning message
pub fn print_warning(message: &str) {
    println!("[WARN] {}", message);
}

/// Print a progress indicator (inline, no newline)
pub fn print_progress(message: &str) {
    print!("{}... ", message);
    io::stdout().flush().ok();
}

/// Complete a progress indicator
pub fn complete_progress() {
    println!("Done");
}

/// Encode binary data as hex string
pub fn hex_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    data.iter().fold(String::new(), |mut output, b| {
        let _ = write!(output, "{:02x}", b);
        output
    })
}

/// Format a key-value pair for display
pub fn format_kv(key: &[u8], value: &[u8]) -> (String, String) {
    let key_str = String::from_utf8_lossy(key).to_string();
    let value_str = match String::from_utf8(value.to_vec()) {
        Ok(s) => s,
        Err(_) => format!("<binary: {}>", hex_encode(value)),
    };
    (key_str, value_str)
}

/// Escape a string for JSON output
fn escape_json_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c.is_control() => {
                // Escape other control characters as \uXXXX
                for unit in c.encode_utf16(&mut [0; 2]) {
                    result.push_str(&format!("\\u{:04x}", unit));
                }
            }
            c => result.push(c),
        }
    }
    result
}

/// Format value based on output format
pub fn format_value(value: &[u8], format: &str) -> String {
    match format {
        "hex" => hex_encode(value),
        "json" => {
            match String::from_utf8(value.to_vec()) {
                Ok(s) => format!("\"{}\"", escape_json_string(&s)),
                Err(_) => format!("\"{}\"", hex_encode(value)),
            }
        }
        _ => {
            // Default to text, fallback to hex if not valid UTF-8
            match String::from_utf8(value.to_vec()) {
                Ok(s) => s,
                Err(_) => format!("<binary: {}>", hex_encode(value)),
            }
        }
    }
}

/// Print a separator line
pub fn print_separator(width: usize) {
    println!("{}", "=".repeat(width));
}

/// Print a table header
pub fn print_header(title: &str) {
    println!("\n=== {} ===", title);
}

// ============================================================================
// JSON Output Support
// ============================================================================

/// JSON output builder for structured CLI output
#[derive(Debug, Clone, Default)]
pub struct JsonOutput {
    fields: HashMap<String, serde_json::Value>,
}

impl JsonOutput {
    /// Create a new JSON output builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a string field
    pub fn add_str(&mut self, key: &str, value: &str) -> &mut Self {
        self.fields.insert(key.to_string(), serde_json::Value::String(value.to_string()));
        self
    }

    /// Add an integer field
    pub fn add_int(&mut self, key: &str, value: i64) -> &mut Self {
        self.fields.insert(key.to_string(), serde_json::Value::Number(value.into()));
        self
    }

    /// Add an unsigned integer field
    pub fn add_uint(&mut self, key: &str, value: u64) -> &mut Self {
        self.fields.insert(
            key.to_string(),
            serde_json::Value::Number(serde_json::Number::from(value)),
        );
        self
    }

    /// Add a float field
    pub fn add_float(&mut self, key: &str, value: f64) -> &mut Self {
        if let Some(num) = serde_json::Number::from_f64(value) {
            self.fields.insert(key.to_string(), serde_json::Value::Number(num));
        }
        self
    }

    /// Add a boolean field
    pub fn add_bool(&mut self, key: &str, value: bool) -> &mut Self {
        self.fields.insert(key.to_string(), serde_json::Value::Bool(value));
        self
    }

    /// Add an array of strings
    pub fn add_string_array(&mut self, key: &str, values: &[String]) -> &mut Self {
        let array: Vec<serde_json::Value> = values
            .iter()
            .map(|s| serde_json::Value::String(s.clone()))
            .collect();
        self.fields.insert(key.to_string(), serde_json::Value::Array(array));
        self
    }

    /// Add an array of key-value pairs
    pub fn add_kv_array(&mut self, key: &str, pairs: &[(String, String)]) -> &mut Self {
        let array: Vec<serde_json::Value> = pairs
            .iter()
            .map(|(k, v)| {
                let mut obj = serde_json::Map::new();
                obj.insert("key".to_string(), serde_json::Value::String(k.clone()));
                obj.insert("value".to_string(), serde_json::Value::String(v.clone()));
                serde_json::Value::Object(obj)
            })
            .collect();
        self.fields.insert(key.to_string(), serde_json::Value::Array(array));
        self
    }

    /// Add nested object
    pub fn add_object(&mut self, key: &str, nested: JsonOutput) -> &mut Self {
        let obj: serde_json::Map<String, serde_json::Value> = nested.fields.into_iter().collect();
        self.fields.insert(key.to_string(), serde_json::Value::Object(obj));
        self
    }

    /// Set the status field (common for all responses)
    pub fn status(&mut self, success: bool) -> &mut Self {
        self.add_str("status", if success { "success" } else { "error" })
    }

    /// Set an error message
    pub fn error(&mut self, message: &str) -> &mut Self {
        self.add_str("status", "error");
        self.add_str("error", message)
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> String {
        let obj: serde_json::Map<String, serde_json::Value> =
            self.fields.clone().into_iter().collect();
        serde_json::to_string_pretty(&serde_json::Value::Object(obj))
            .unwrap_or_else(|_| "{}".to_string())
    }

    /// Print JSON output to stdout
    pub fn print(&self) {
        println!("{}", self.to_json());
    }
}

/// Create a success JSON response
pub fn json_success() -> JsonOutput {
    let mut output = JsonOutput::new();
    output.status(true);
    output
}

/// Create an error JSON response
pub fn json_error(message: &str) -> JsonOutput {
    let mut output = JsonOutput::new();
    output.error(message);
    output
}

/// Output formatter that supports both text and JSON output
pub struct OutputFormatter {
    json_mode: bool,
    quiet: bool,
    json: JsonOutput,
}

impl OutputFormatter {
    /// Create a new output formatter
    pub fn new(json_mode: bool, quiet: bool) -> Self {
        Self {
            json_mode,
            quiet,
            json: JsonOutput::new(),
        }
    }

    /// Check if in JSON mode
    pub fn is_json(&self) -> bool {
        self.json_mode
    }

    /// Check if in quiet mode
    pub fn is_quiet(&self) -> bool {
        self.quiet
    }

    /// Print a message (suppressed in quiet mode, not shown in JSON mode)
    pub fn info(&self, message: &str) {
        if !self.json_mode && !self.quiet {
            println!("{}", message);
        }
    }

    /// Print a success message
    pub fn success(&self, message: &str) {
        if !self.json_mode && !self.quiet {
            print_success(message);
        }
    }

    /// Print an error message (always shown except in JSON mode)
    pub fn error(&self, message: &str) {
        if !self.json_mode {
            print_error(message);
        }
    }

    /// Print a warning message
    pub fn warning(&self, message: &str) {
        if !self.json_mode && !self.quiet {
            print_warning(message);
        }
    }

    /// Get mutable reference to JSON builder for adding fields
    pub fn json_builder(&mut self) -> &mut JsonOutput {
        &mut self.json
    }

    /// Finalize and output the result
    pub fn finish(&self, success: bool) {
        if self.json_mode {
            let mut output = self.json.clone();
            output.status(success);
            output.print();
        }
    }

    /// Finalize with error and output
    pub fn finish_error(&self, error: &str) {
        if self.json_mode {
            json_error(error).print();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes_bytes() {
        assert_eq!(format_bytes(0), "0 bytes");
        assert_eq!(format_bytes(100), "100 bytes");
        assert_eq!(format_bytes(1023), "1023 bytes");
    }

    #[test]
    fn test_format_bytes_kb() {
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
    }

    #[test]
    fn test_format_bytes_mb() {
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 10), "10.00 MB");
    }

    #[test]
    fn test_format_bytes_gb() {
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_format_bytes_tb() {
        assert_eq!(format_bytes(1024u64 * 1024 * 1024 * 1024), "1.00 TB");
    }

    #[test]
    fn test_format_duration_microseconds() {
        let duration = std::time::Duration::from_micros(500);
        assert!(format_duration(duration).contains("μs"));
    }

    #[test]
    fn test_format_duration_milliseconds() {
        let duration = std::time::Duration::from_millis(50);
        assert!(format_duration(duration).contains("ms"));
    }

    #[test]
    fn test_format_duration_seconds() {
        let duration = std::time::Duration::from_secs(30);
        assert!(format_duration(duration).contains("s"));
    }

    #[test]
    fn test_format_duration_minutes() {
        let duration = std::time::Duration::from_secs(120);
        assert!(format_duration(duration).contains("min"));
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(&[0x00]), "00");
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn test_format_kv_utf8() {
        let (key, value) = format_kv(b"test_key", b"test_value");
        assert_eq!(key, "test_key");
        assert_eq!(value, "test_value");
    }

    #[test]
    fn test_format_kv_binary() {
        let (key, value) = format_kv(b"key", &[0x80, 0x81, 0x82]);
        assert_eq!(key, "key");
        assert!(value.contains("binary"));
        assert!(value.contains("808182"));
    }

    #[test]
    fn test_format_value_text() {
        assert_eq!(format_value(b"hello", "text"), "hello");
    }

    #[test]
    fn test_format_value_hex() {
        assert_eq!(format_value(&[0xca, 0xfe], "hex"), "cafe");
    }

    #[test]
    fn test_format_value_json() {
        let result = format_value(b"hello", "json");
        assert!(result.starts_with("\""));
        assert!(result.ends_with("\""));
    }

    #[test]
    fn test_escape_json_string() {
        assert_eq!(escape_json_string("hello"), "hello");
        assert_eq!(escape_json_string("hello\"world"), "hello\\\"world");
        assert_eq!(escape_json_string("line\nbreak"), "line\\nbreak");
        assert_eq!(escape_json_string("tab\there"), "tab\\there");
    }

    // JSON output tests
    #[test]
    fn test_json_output_basic() {
        let mut output = JsonOutput::new();
        output.add_str("key", "value");
        output.add_int("count", 42);
        output.add_bool("enabled", true);

        let json = output.to_json();
        assert!(json.contains("\"key\": \"value\""));
        assert!(json.contains("\"count\": 42"));
        assert!(json.contains("\"enabled\": true"));
    }

    #[test]
    fn test_json_output_status() {
        let mut output = JsonOutput::new();
        output.status(true);
        let json = output.to_json();
        assert!(json.contains("\"status\": \"success\""));

        let mut output = JsonOutput::new();
        output.status(false);
        let json = output.to_json();
        assert!(json.contains("\"status\": \"error\""));
    }

    #[test]
    fn test_json_success() {
        let output = json_success();
        let json = output.to_json();
        assert!(json.contains("\"status\": \"success\""));
    }

    #[test]
    fn test_json_error() {
        let output = json_error("something went wrong");
        let json = output.to_json();
        assert!(json.contains("\"status\": \"error\""));
        assert!(json.contains("\"error\": \"something went wrong\""));
    }

    #[test]
    fn test_output_formatter_text_mode() {
        let formatter = OutputFormatter::new(false, false);
        assert!(!formatter.is_json());
        assert!(!formatter.is_quiet());
    }

    #[test]
    fn test_output_formatter_json_mode() {
        let formatter = OutputFormatter::new(true, false);
        assert!(formatter.is_json());
    }

    #[test]
    fn test_output_formatter_quiet_mode() {
        let formatter = OutputFormatter::new(false, true);
        assert!(formatter.is_quiet());
    }
}
