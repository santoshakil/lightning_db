//! Output Formatting for Lightning DB REPL
//!
//! Provides multiple output formats (table, JSON, CSV, YAML) with
//! customizable styling, colors, and layout options.

use crate::core::error::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Output formatter for REPL results
pub struct OutputFormatter {
    /// Default format style
    default_style: FormatStyle,
    /// Formatting configuration
    config: FormatterConfig,
    /// Color scheme
    color_scheme: ColorScheme,
}

/// Available formatting styles
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FormatStyle {
    Table,
    Json,
    JsonPretty,
    Csv,
    Yaml,
    Raw,
    Compact,
    Tree,
}

impl Default for FormatStyle {
    fn default() -> Self {
        FormatStyle::Table
    }
}

/// Configuration for output formatting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatterConfig {
    /// Enable color output
    pub enable_colors: bool,
    /// Enable Unicode characters for tables
    pub enable_unicode: bool,
    /// Maximum column width for tables
    pub max_column_width: usize,
    /// Maximum rows to display (0 = unlimited)
    pub max_rows: usize,
    /// Show row numbers
    pub show_row_numbers: bool,
    /// Indent size for pretty printing
    pub indent_size: usize,
    /// Table border style
    pub table_border_style: BorderStyle,
    /// Date/time format
    pub datetime_format: String,
}

impl Default for FormatterConfig {
    fn default() -> Self {
        Self {
            enable_colors: true,
            enable_unicode: true,
            max_column_width: 50,
            max_rows: 0,
            show_row_numbers: false,
            indent_size: 2,
            table_border_style: BorderStyle::Rounded,
            datetime_format: "%Y-%m-%d %H:%M:%S".to_string(),
        }
    }
}

/// Table border styles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BorderStyle {
    None,
    Simple,
    Rounded,
    Double,
    Thick,
}

/// Color scheme for output
#[derive(Debug, Clone)]
pub struct ColorScheme {
    pub header: &'static str,
    pub border: &'static str,
    pub data: &'static str,
    pub key: &'static str,
    pub value: &'static str,
    pub null: &'static str,
    pub number: &'static str,
    pub boolean: &'static str,
    pub string: &'static str,
    pub error: &'static str,
    pub warning: &'static str,
    pub success: &'static str,
    pub reset: &'static str,
}

impl Default for ColorScheme {
    fn default() -> Self {
        Self {
            header: "\x1b[1;36m",  // Bold cyan
            border: "\x1b[90m",    // Dark gray
            data: "\x1b[0m",       // Default
            key: "\x1b[1;34m",     // Bold blue
            value: "\x1b[32m",     // Green
            null: "\x1b[90m",      // Dark gray
            number: "\x1b[33m",    // Yellow
            boolean: "\x1b[35m",   // Magenta
            string: "\x1b[32m",    // Green
            error: "\x1b[1;31m",   // Bold red
            warning: "\x1b[1;33m", // Bold yellow
            success: "\x1b[1;32m", // Bold green
            reset: "\x1b[0m",      // Reset
        }
    }
}

/// Data structure for formatting
#[derive(Debug, Clone, Serialize)]
pub enum FormattableData {
    Single(HashMap<String, JsonValue>),
    Multiple(Vec<HashMap<String, JsonValue>>),
    KeyValue { key: String, value: Option<Vec<u8>> },
    Keys(Vec<String>),
    Message(String),
    Error(String),
    Stats(DatabaseStats),
}

/// Database statistics for formatting
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseStats {
    pub total_keys: u64,
    pub total_size_bytes: u64,
    pub cache_hit_rate: f64,
    pub operations_per_second: f64,
    pub active_transactions: u64,
    pub uptime_seconds: u64,
    pub version: String,
}

/// Result formatter trait for custom formatting
pub trait ResultFormatter {
    fn format(&self, data: &FormattableData, style: &FormatStyle) -> Result<String>;
}

impl OutputFormatter {
    /// Create a new output formatter
    pub fn new(default_style: FormatStyle) -> Self {
        Self {
            default_style,
            config: FormatterConfig::default(),
            color_scheme: ColorScheme::default(),
        }
    }

    /// Create formatter with custom configuration
    pub fn with_config(default_style: FormatStyle, config: FormatterConfig) -> Self {
        Self {
            default_style,
            config,
            color_scheme: ColorScheme::default(),
        }
    }

    /// Format data using the default style
    pub fn format_data(&self, data: &FormattableData) -> String {
        self.format_data_with_style(data, &self.default_style)
    }

    /// Format data with a specific style
    pub fn format_data_with_style(&self, data: &FormattableData, style: &FormatStyle) -> String {
        match self.format_data_impl(data, style) {
            Ok(formatted) => formatted,
            Err(e) => format!(
                "{}Formatting error: {}{}",
                self.color_scheme.error, e, self.color_scheme.reset
            ),
        }
    }

    /// Internal formatting implementation
    fn format_data_impl(&self, data: &FormattableData, style: &FormatStyle) -> Result<String> {
        match data {
            FormattableData::Single(record) => self.format_single_record(record, style),
            FormattableData::Multiple(records) => self.format_multiple_records(records, style),
            FormattableData::KeyValue { key, value } => self.format_key_value(key, value, style),
            FormattableData::Keys(keys) => self.format_keys(keys, style),
            FormattableData::Message(message) => self.format_message(message, style),
            FormattableData::Error(error) => self.format_error(error, style),
            FormattableData::Stats(stats) => self.format_stats(stats, style),
        }
    }

    /// Format a single record
    fn format_single_record(
        &self,
        record: &HashMap<String, JsonValue>,
        style: &FormatStyle,
    ) -> Result<String> {
        match style {
            FormatStyle::Table => self.format_single_record_as_table(record),
            FormatStyle::Json => Ok(serde_json::to_string(record).unwrap_or_default()),
            FormatStyle::JsonPretty => Ok(serde_json::to_string_pretty(record).unwrap_or_default()),
            FormatStyle::Csv => self.format_single_record_as_csv(record),
            FormatStyle::Yaml => self.format_single_record_as_yaml(record),
            FormatStyle::Raw => self.format_single_record_as_raw(record),
            FormatStyle::Compact => self.format_single_record_as_compact(record),
            FormatStyle::Tree => self.format_single_record_as_tree(record),
        }
    }

    /// Format multiple records
    fn format_multiple_records(
        &self,
        records: &[HashMap<String, JsonValue>],
        style: &FormatStyle,
    ) -> Result<String> {
        if records.is_empty() {
            return Ok("No results".to_string());
        }

        match style {
            FormatStyle::Table => self.format_multiple_records_as_table(records),
            FormatStyle::Json => Ok(serde_json::to_string(records).unwrap_or_default()),
            FormatStyle::JsonPretty => {
                Ok(serde_json::to_string_pretty(records).unwrap_or_default())
            }
            FormatStyle::Csv => self.format_multiple_records_as_csv(records),
            FormatStyle::Yaml => self.format_multiple_records_as_yaml(records),
            FormatStyle::Raw => self.format_multiple_records_as_raw(records),
            FormatStyle::Compact => self.format_multiple_records_as_compact(records),
            FormatStyle::Tree => self.format_multiple_records_as_tree(records),
        }
    }

    /// Format key-value pair
    fn format_key_value(
        &self,
        key: &str,
        value: &Option<Vec<u8>>,
        style: &FormatStyle,
    ) -> Result<String> {
        match value {
            Some(val) => {
                let value_str = String::from_utf8_lossy(val);
                match style {
                    FormatStyle::Table => {
                        let mut output = String::new();
                        output.push_str(&self.format_table_border_top(&["key", "value"]));
                        output.push_str(&self.format_table_header(&["key", "value"]));
                        output.push_str(&self.format_table_border_middle(&["key", "value"]));
                        output.push_str(&self.format_table_row(&[key, &value_str]));
                        output.push_str(&self.format_table_border_bottom(&["key", "value"]));
                        Ok(output)
                    }
                    FormatStyle::Json | FormatStyle::JsonPretty => {
                        let mut map = HashMap::new();
                        map.insert("key".to_string(), JsonValue::String(key.to_string()));
                        map.insert(
                            "value".to_string(),
                            JsonValue::String(value_str.to_string()),
                        );
                        if *style == FormatStyle::JsonPretty {
                            Ok(serde_json::to_string_pretty(&map).unwrap_or_default())
                        } else {
                            Ok(serde_json::to_string(&map).unwrap_or_default())
                        }
                    }
                    FormatStyle::Csv => Ok(format!("key,value\n\"{}\",\"{}\"", key, value_str)),
                    FormatStyle::Raw => Ok(value_str.to_string()),
                    FormatStyle::Compact => Ok(format!(
                        "{} = {}",
                        self.colorize(key, self.color_scheme.key),
                        self.colorize(&value_str, self.color_scheme.value)
                    )),
                    _ => Ok(format!("{}: {}", key, value_str)),
                }
            }
            None => match style {
                FormatStyle::Json | FormatStyle::JsonPretty => Ok("null".to_string()),
                _ => Ok(format!(
                    "{}: {}",
                    key,
                    self.colorize("(not found)", self.color_scheme.null)
                )),
            },
        }
    }

    /// Format list of keys
    fn format_keys(&self, keys: &[String], style: &FormatStyle) -> Result<String> {
        match style {
            FormatStyle::Table => {
                let mut output = String::new();
                output.push_str(&self.format_table_border_top(&["key"]));
                output.push_str(&self.format_table_header(&["key"]));
                output.push_str(&self.format_table_border_middle(&["key"]));

                let display_keys = if self.config.max_rows > 0 && keys.len() > self.config.max_rows
                {
                    &keys[..self.config.max_rows]
                } else {
                    keys
                };

                for key in display_keys {
                    output.push_str(&self.format_table_row(&[key]));
                }

                if self.config.max_rows > 0 && keys.len() > self.config.max_rows {
                    output.push_str(&format!(
                        "{}... and {} more{}\n",
                        self.color_scheme.border,
                        keys.len() - self.config.max_rows,
                        self.color_scheme.reset
                    ));
                }

                output.push_str(&self.format_table_border_bottom(&["key"]));
                Ok(output)
            }
            FormatStyle::Json | FormatStyle::JsonPretty => {
                if *style == FormatStyle::JsonPretty {
                    Ok(serde_json::to_string_pretty(keys).unwrap_or_default())
                } else {
                    Ok(serde_json::to_string(keys).unwrap_or_default())
                }
            }
            FormatStyle::Csv => {
                let mut output = String::from("key\n");
                for key in keys {
                    output.push_str(&format!("\"{}\"\n", key));
                }
                Ok(output)
            }
            FormatStyle::Raw | FormatStyle::Compact => Ok(keys.join("\n")),
            _ => Ok(keys.join(", ")),
        }
    }

    /// Format message
    fn format_message(&self, message: &str, style: &FormatStyle) -> Result<String> {
        match style {
            FormatStyle::Json | FormatStyle::JsonPretty => {
                let mut map = HashMap::new();
                map.insert(
                    "message".to_string(),
                    JsonValue::String(message.to_string()),
                );
                if *style == FormatStyle::JsonPretty {
                    Ok(serde_json::to_string_pretty(&map).unwrap_or_default())
                } else {
                    Ok(serde_json::to_string(&map).unwrap_or_default())
                }
            }
            _ => Ok(self.colorize(message, self.color_scheme.success)),
        }
    }

    /// Format error message
    fn format_error(&self, error: &str, _style: &FormatStyle) -> Result<String> {
        Ok(self.colorize(error, self.color_scheme.error))
    }

    /// Format database statistics
    fn format_stats(&self, stats: &DatabaseStats, style: &FormatStyle) -> Result<String> {
        match style {
            FormatStyle::Table => self.format_stats_as_table(stats),
            FormatStyle::Json => Ok(serde_json::to_string(stats).unwrap_or_default()),
            FormatStyle::JsonPretty => Ok(serde_json::to_string_pretty(stats).unwrap_or_default()),
            _ => self.format_stats_as_compact(stats),
        }
    }

    // Table formatting helpers

    /// Format single record as table
    fn format_single_record_as_table(&self, record: &HashMap<String, JsonValue>) -> Result<String> {
        let mut output = String::new();

        output.push_str(&self.format_table_border_top(&["property", "value"]));
        output.push_str(&self.format_table_header(&["property", "value"]));
        output.push_str(&self.format_table_border_middle(&["property", "value"]));

        for (key, value) in record {
            let value_str = self.format_json_value(value);
            output.push_str(&self.format_table_row(&[key, &value_str]));
        }

        output.push_str(&self.format_table_border_bottom(&["property", "value"]));
        Ok(output)
    }

    /// Format multiple records as table
    fn format_multiple_records_as_table(
        &self,
        records: &[HashMap<String, JsonValue>],
    ) -> Result<String> {
        if records.is_empty() {
            return Ok("No results".to_string());
        }

        // Get all unique column names
        let mut columns = std::collections::BTreeSet::new();
        for record in records {
            for key in record.keys() {
                columns.insert(key.clone());
            }
        }
        let columns: Vec<String> = columns.into_iter().collect();

        let mut output = String::new();

        // Add row numbers column if enabled
        let headers = if self.config.show_row_numbers {
            let mut h = vec!["#".to_string()];
            h.extend(columns.clone());
            h
        } else {
            columns.clone()
        };

        let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();

        output.push_str(&self.format_table_border_top(&header_refs));
        output.push_str(&self.format_table_header(&header_refs));
        output.push_str(&self.format_table_border_middle(&header_refs));

        let display_records = if self.config.max_rows > 0 && records.len() > self.config.max_rows {
            &records[..self.config.max_rows]
        } else {
            records
        };

        for (row_idx, record) in display_records.iter().enumerate() {
            let mut row_data = Vec::new();

            if self.config.show_row_numbers {
                row_data.push((row_idx + 1).to_string());
            }

            for column in &columns {
                let value = record
                    .get(column)
                    .map(|v| self.format_json_value(v))
                    .unwrap_or_else(|| self.colorize("", self.color_scheme.null));
                row_data.push(value);
            }

            let row_refs: Vec<&str> = row_data.iter().map(|s| s.as_str()).collect();
            output.push_str(&self.format_table_row(&row_refs));
        }

        if self.config.max_rows > 0 && records.len() > self.config.max_rows {
            output.push_str(&format!(
                "{}... and {} more rows{}\n",
                self.color_scheme.border,
                records.len() - self.config.max_rows,
                self.color_scheme.reset
            ));
        }

        output.push_str(&self.format_table_border_bottom(&header_refs));
        Ok(output)
    }

    /// Format stats as table
    fn format_stats_as_table(&self, stats: &DatabaseStats) -> Result<String> {
        let mut output = String::new();

        output.push_str(&self.format_table_border_top(&["metric", "value"]));
        output.push_str(&self.format_table_header(&["metric", "value"]));
        output.push_str(&self.format_table_border_middle(&["metric", "value"]));

        let stats_data = vec![
            ("Total Keys", format!("{}", stats.total_keys)),
            ("Total Size", self.format_bytes(stats.total_size_bytes)),
            (
                "Cache Hit Rate",
                format!("{:.2}%", stats.cache_hit_rate * 100.0),
            ),
            (
                "Operations/sec",
                format!("{:.0}", stats.operations_per_second),
            ),
            (
                "Active Transactions",
                format!("{}", stats.active_transactions),
            ),
            ("Uptime", self.format_duration(stats.uptime_seconds)),
            ("Version", stats.version.clone()),
        ];

        for (metric, value) in stats_data {
            output.push_str(&self.format_table_row(&[metric, &value]));
        }

        output.push_str(&self.format_table_border_bottom(&["metric", "value"]));
        Ok(output)
    }

    // Table drawing helpers

    fn format_table_border_top(&self, columns: &[&str]) -> String {
        if matches!(self.config.table_border_style, BorderStyle::None) {
            return String::new();
        }

        let chars = self.get_border_chars();
        let mut border = String::new();

        border.push_str(&self.colorize(&chars.top_left.to_string(), self.color_scheme.border));

        for (i, column) in columns.iter().enumerate() {
            let width = self.calculate_column_width(column);
            border.push_str(&self.colorize(
                &chars.horizontal.to_string().repeat(width + 2),
                self.color_scheme.border,
            ));

            if i < columns.len() - 1 {
                border
                    .push_str(&self.colorize(&chars.top_tee.to_string(), self.color_scheme.border));
            }
        }

        border.push_str(&self.colorize(&chars.top_right.to_string(), self.color_scheme.border));
        border.push('\n');
        border
    }

    fn format_table_header(&self, columns: &[&str]) -> String {
        if matches!(self.config.table_border_style, BorderStyle::None) {
            return format!("{}\n", columns.join(" | "));
        }

        let chars = self.get_border_chars();
        let mut header = String::new();

        header.push_str(&self.colorize(&chars.vertical.to_string(), self.color_scheme.border));

        for (i, column) in columns.iter().enumerate() {
            let width = self.calculate_column_width(column);
            let padded = format!(" {:<width$} ", column, width = width);
            header.push_str(&self.colorize(&padded, self.color_scheme.header));

            if i < columns.len() - 1 {
                header.push_str(
                    &self.colorize(&chars.vertical.to_string(), self.color_scheme.border),
                );
            }
        }

        header.push_str(&self.colorize(&chars.vertical.to_string(), self.color_scheme.border));
        header.push('\n');
        header
    }

    fn format_table_border_middle(&self, columns: &[&str]) -> String {
        if matches!(self.config.table_border_style, BorderStyle::None) {
            return String::new();
        }

        let chars = self.get_border_chars();
        let mut border = String::new();

        border.push_str(&self.colorize(&chars.left_tee.to_string(), self.color_scheme.border));

        for (i, column) in columns.iter().enumerate() {
            let width = self.calculate_column_width(column);
            border.push_str(&self.colorize(
                &chars.horizontal.to_string().repeat(width + 2),
                self.color_scheme.border,
            ));

            if i < columns.len() - 1 {
                border.push_str(&self.colorize(&chars.cross.to_string(), self.color_scheme.border));
            }
        }

        border.push_str(&self.colorize(&chars.right_tee.to_string(), self.color_scheme.border));
        border.push('\n');
        border
    }

    fn format_table_row(&self, columns: &[&str]) -> String {
        if matches!(self.config.table_border_style, BorderStyle::None) {
            return format!("{}\n", columns.join(" | "));
        }

        let chars = self.get_border_chars();
        let mut row = String::new();

        row.push_str(&self.colorize(&chars.vertical.to_string(), self.color_scheme.border));

        for (i, column) in columns.iter().enumerate() {
            let width = self.calculate_column_width(column);
            let truncated = self.truncate_text(column, width);
            let padded = format!(" {:<width$} ", truncated, width = width);
            row.push_str(&self.colorize(&padded, self.color_scheme.data));

            if i < columns.len() - 1 {
                row.push_str(&self.colorize(&chars.vertical.to_string(), self.color_scheme.border));
            }
        }

        row.push_str(&self.colorize(&chars.vertical.to_string(), self.color_scheme.border));
        row.push('\n');
        row
    }

    fn format_table_border_bottom(&self, columns: &[&str]) -> String {
        if matches!(self.config.table_border_style, BorderStyle::None) {
            return String::new();
        }

        let chars = self.get_border_chars();
        let mut border = String::new();

        border.push_str(&self.colorize(&chars.bottom_left.to_string(), self.color_scheme.border));

        for (i, column) in columns.iter().enumerate() {
            let width = self.calculate_column_width(column);
            border.push_str(&self.colorize(
                &chars.horizontal.to_string().repeat(width + 2),
                self.color_scheme.border,
            ));

            if i < columns.len() - 1 {
                border.push_str(
                    &self.colorize(&chars.bottom_tee.to_string(), self.color_scheme.border),
                );
            }
        }

        border.push_str(&self.colorize(&chars.bottom_right.to_string(), self.color_scheme.border));
        border.push('\n');
        border
    }

    // Other format implementations (simplified)

    fn format_single_record_as_csv(&self, record: &HashMap<String, JsonValue>) -> Result<String> {
        let keys: Vec<_> = record.keys().collect();
        let mut output = format!(
            "{}\n",
            keys.iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );

        let values: Vec<String> = keys
            .iter()
            .map(|k| format!("\"{}\"", self.format_json_value(record.get(*k).unwrap())))
            .collect();
        output.push_str(&values.join(","));

        Ok(output)
    }

    fn format_multiple_records_as_csv(
        &self,
        records: &[HashMap<String, JsonValue>],
    ) -> Result<String> {
        if records.is_empty() {
            return Ok(String::new());
        }

        // Get all unique column names
        let mut columns = std::collections::BTreeSet::new();
        for record in records {
            for key in record.keys() {
                columns.insert(key.clone());
            }
        }
        let columns: Vec<String> = columns.into_iter().collect();

        let mut output = format!("{}\n", columns.join(","));

        for record in records {
            let values: Vec<String> = columns
                .iter()
                .map(|col| {
                    let value = record
                        .get(col)
                        .map(|v| self.format_json_value(v))
                        .unwrap_or_default();
                    format!("\"{}\"", value.replace('"', "\"\""))
                })
                .collect();
            output.push_str(&format!("{}\n", values.join(",")));
        }

        Ok(output)
    }

    fn format_single_record_as_yaml(&self, record: &HashMap<String, JsonValue>) -> Result<String> {
        // Simple YAML-like format
        let mut output = String::new();
        for (key, value) in record {
            output.push_str(&format!("{}: {}\n", key, self.format_json_value(value)));
        }
        Ok(output)
    }

    fn format_multiple_records_as_yaml(
        &self,
        records: &[HashMap<String, JsonValue>],
    ) -> Result<String> {
        let mut output = String::new();
        for (i, record) in records.iter().enumerate() {
            output.push_str(&format!("---\nrecord_{}:\n", i));
            for (key, value) in record {
                output.push_str(&format!("  {}: {}\n", key, self.format_json_value(value)));
            }
        }
        Ok(output)
    }

    fn format_single_record_as_raw(&self, record: &HashMap<String, JsonValue>) -> Result<String> {
        Ok(record
            .values()
            .map(|v| self.format_json_value(v))
            .collect::<Vec<_>>()
            .join(" "))
    }

    fn format_multiple_records_as_raw(
        &self,
        records: &[HashMap<String, JsonValue>],
    ) -> Result<String> {
        let mut output = String::new();
        for record in records {
            let line = record
                .values()
                .map(|v| self.format_json_value(v))
                .collect::<Vec<_>>()
                .join(" ");
            output.push_str(&format!("{}\n", line));
        }
        Ok(output)
    }

    fn format_single_record_as_compact(
        &self,
        record: &HashMap<String, JsonValue>,
    ) -> Result<String> {
        let pairs: Vec<String> = record
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    self.colorize(k, self.color_scheme.key),
                    self.colorize(&self.format_json_value(v), self.color_scheme.value)
                )
            })
            .collect();
        Ok(pairs.join(" "))
    }

    fn format_multiple_records_as_compact(
        &self,
        records: &[HashMap<String, JsonValue>],
    ) -> Result<String> {
        let mut output = String::new();
        for record in records {
            let pairs: Vec<String> = record
                .iter()
                .map(|(k, v)| format!("{}={}", k, self.format_json_value(v)))
                .collect();
            output.push_str(&format!("{}\n", pairs.join(" ")));
        }
        Ok(output)
    }

    fn format_single_record_as_tree(&self, record: &HashMap<String, JsonValue>) -> Result<String> {
        let mut output = String::new();
        let keys: Vec<_> = record.keys().collect();

        for (i, key) in keys.iter().enumerate() {
            let is_last = i == keys.len() - 1;
            let prefix = if is_last { "└── " } else { "├── " };
            let value = self.format_json_value(record.get(*key).unwrap());

            output.push_str(&format!(
                "{}{}: {}\n",
                prefix,
                self.colorize(key, self.color_scheme.key),
                self.colorize(&value, self.color_scheme.value)
            ));
        }

        Ok(output)
    }

    fn format_multiple_records_as_tree(
        &self,
        records: &[HashMap<String, JsonValue>],
    ) -> Result<String> {
        let mut output = String::new();

        for (record_idx, record) in records.iter().enumerate() {
            output.push_str(&format!("Record {}\n", record_idx + 1));

            let keys: Vec<_> = record.keys().collect();
            for (i, key) in keys.iter().enumerate() {
                let is_last = i == keys.len() - 1;
                let prefix = if is_last { "└── " } else { "├── " };
                let value = self.format_json_value(record.get(*key).unwrap());

                output.push_str(&format!(
                    "{}{}: {}\n",
                    prefix,
                    self.colorize(key, self.color_scheme.key),
                    self.colorize(&value, self.color_scheme.value)
                ));
            }

            if record_idx < records.len() - 1 {
                output.push('\n');
            }
        }

        Ok(output)
    }

    fn format_stats_as_compact(&self, stats: &DatabaseStats) -> Result<String> {
        Ok(format!(
            "Keys: {} | Size: {} | Cache: {:.1}% | Ops/sec: {:.0} | Transactions: {} | Uptime: {} | Version: {}",
            stats.total_keys,
            self.format_bytes(stats.total_size_bytes),
            stats.cache_hit_rate * 100.0,
            stats.operations_per_second,
            stats.active_transactions,
            self.format_duration(stats.uptime_seconds),
            stats.version
        ))
    }

    // Helper methods

    fn format_json_value(&self, value: &JsonValue) -> String {
        match value {
            JsonValue::Null => self.colorize("null", self.color_scheme.null),
            JsonValue::Bool(b) => self.colorize(&b.to_string(), self.color_scheme.boolean),
            JsonValue::Number(n) => self.colorize(&n.to_string(), self.color_scheme.number),
            JsonValue::String(s) => self.colorize(s, self.color_scheme.string),
            JsonValue::Array(_) | JsonValue::Object(_) => {
                self.colorize(&value.to_string(), self.color_scheme.data)
            }
        }
    }

    fn colorize(&self, text: &str, color: &str) -> String {
        if self.config.enable_colors && !text.is_empty() {
            format!("{}{}{}", color, text, self.color_scheme.reset)
        } else {
            text.to_string()
        }
    }

    fn calculate_column_width(&self, column: &str) -> usize {
        column.len().min(self.config.max_column_width)
    }

    fn truncate_text(&self, text: &str, max_width: usize) -> String {
        if text.len() <= max_width {
            text.to_string()
        } else {
            format!("{}...", &text[..max_width.saturating_sub(3)])
        }
    }

    fn format_bytes(&self, bytes: u64) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", bytes, UNITS[0])
        } else {
            format!("{:.1} {}", size, UNITS[unit_index])
        }
    }

    fn format_duration(&self, seconds: u64) -> String {
        let days = seconds / 86400;
        let hours = (seconds % 86400) / 3600;
        let minutes = (seconds % 3600) / 60;
        let secs = seconds % 60;

        if days > 0 {
            format!("{}d {}h", days, hours)
        } else if hours > 0 {
            format!("{}h {}m", hours, minutes)
        } else if minutes > 0 {
            format!("{}m {}s", minutes, secs)
        } else {
            format!("{}s", secs)
        }
    }

    fn get_border_chars(&self) -> BorderChars {
        match self.config.table_border_style {
            BorderStyle::None => BorderChars::none(),
            BorderStyle::Simple => BorderChars::simple(),
            BorderStyle::Rounded => BorderChars::rounded(),
            BorderStyle::Double => BorderChars::double(),
            BorderStyle::Thick => BorderChars::thick(),
        }
    }
}

/// Border characters for different table styles
struct BorderChars {
    horizontal: char,
    vertical: char,
    top_left: char,
    top_right: char,
    bottom_left: char,
    bottom_right: char,
    left_tee: char,
    right_tee: char,
    top_tee: char,
    bottom_tee: char,
    cross: char,
}

impl BorderChars {
    fn none() -> Self {
        Self {
            horizontal: ' ',
            vertical: ' ',
            top_left: ' ',
            top_right: ' ',
            bottom_left: ' ',
            bottom_right: ' ',
            left_tee: ' ',
            right_tee: ' ',
            top_tee: ' ',
            bottom_tee: ' ',
            cross: ' ',
        }
    }

    fn simple() -> Self {
        Self {
            horizontal: '-',
            vertical: '|',
            top_left: '+',
            top_right: '+',
            bottom_left: '+',
            bottom_right: '+',
            left_tee: '+',
            right_tee: '+',
            top_tee: '+',
            bottom_tee: '+',
            cross: '+',
        }
    }

    fn rounded() -> Self {
        Self {
            horizontal: '─',
            vertical: '│',
            top_left: '╭',
            top_right: '╮',
            bottom_left: '╰',
            bottom_right: '╯',
            left_tee: '├',
            right_tee: '┤',
            top_tee: '┬',
            bottom_tee: '┴',
            cross: '┼',
        }
    }

    fn double() -> Self {
        Self {
            horizontal: '═',
            vertical: '║',
            top_left: '╔',
            top_right: '╗',
            bottom_left: '╚',
            bottom_right: '╝',
            left_tee: '╠',
            right_tee: '╣',
            top_tee: '╦',
            bottom_tee: '╩',
            cross: '╬',
        }
    }

    fn thick() -> Self {
        Self {
            horizontal: '━',
            vertical: '┃',
            top_left: '┏',
            top_right: '┓',
            bottom_left: '┗',
            bottom_right: '┛',
            left_tee: '┣',
            right_tee: '┫',
            top_tee: '┳',
            bottom_tee: '┻',
            cross: '╋',
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_formatter_creation() {
        let formatter = OutputFormatter::new(FormatStyle::Table);
        assert_eq!(formatter.default_style, FormatStyle::Table);
    }

    #[test]
    fn test_format_key_value() {
        let formatter = OutputFormatter::new(FormatStyle::Raw);
        let result = formatter
            .format_key_value("test_key", &Some(b"test_value".to_vec()), &FormatStyle::Raw)
            .unwrap();
        assert_eq!(result, "test_value");
    }

    #[test]
    fn test_format_bytes() {
        let formatter = OutputFormatter::new(FormatStyle::Table);
        assert_eq!(formatter.format_bytes(1024), "1.0 KB");
        assert_eq!(formatter.format_bytes(1048576), "1.0 MB");
        assert_eq!(formatter.format_bytes(500), "500 B");
    }

    #[test]
    fn test_format_duration() {
        let formatter = OutputFormatter::new(FormatStyle::Table);
        assert_eq!(formatter.format_duration(3661), "1h 1m");
        assert_eq!(formatter.format_duration(90061), "1d 1h");
        assert_eq!(formatter.format_duration(30), "30s");
    }

    #[test]
    fn test_truncate_text() {
        let formatter = OutputFormatter::new(FormatStyle::Table);
        assert_eq!(formatter.truncate_text("short", 10), "short");
        assert_eq!(
            formatter.truncate_text("this is a very long text", 10),
            "this is..."
        );
    }

    #[test]
    fn test_json_value_formatting() {
        let formatter = OutputFormatter::new(FormatStyle::Table);
        assert!(formatter
            .format_json_value(&JsonValue::Null)
            .contains("null"));
        assert!(formatter
            .format_json_value(&JsonValue::Bool(true))
            .contains("true"));
        assert!(formatter
            .format_json_value(&JsonValue::String("test".to_string()))
            .contains("test"));
    }
}
