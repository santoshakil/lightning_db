//! Command Completion Engine for Lightning DB REPL
//!
//! Provides intelligent command completion, parameter suggestions,
//! and context-aware help for the interactive REPL.

use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tracing::debug;

use super::commands::CommandRegistry;

/// Command completion engine
pub struct CompletionEngine {
    /// Command registry for available commands
    _command_registry: Arc<CommandRegistry>,
    /// Completion cache for performance
    completion_cache: Arc<RwLock<HashMap<String, Vec<CompletionCandidate>>>>,
    /// Key cache for dynamic completion
    key_cache: Arc<RwLock<HashSet<String>>>,
    /// Configuration
    config: CompletionConfig,
}

/// Configuration for completion engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionConfig {
    /// Maximum number of completion candidates to return
    pub max_candidates: usize,
    /// Enable fuzzy matching
    pub enable_fuzzy_matching: bool,
    /// Minimum query length for completion
    pub min_query_length: usize,
    /// Enable completion caching
    pub enable_caching: bool,
    /// Cache expiry time in seconds
    pub cache_expiry_seconds: u64,
}

impl Default for CompletionConfig {
    fn default() -> Self {
        Self {
            max_candidates: 50,
            enable_fuzzy_matching: true,
            min_query_length: 1,
            enable_caching: true,
            cache_expiry_seconds: 300, // 5 minutes
        }
    }
}

/// Completion candidate with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionCandidate {
    /// The completion text
    pub text: String,
    /// Display text (may include formatting)
    pub display: String,
    /// Type of completion
    pub completion_type: CompletionType,
    /// Description or help text
    pub description: Option<String>,
    /// Priority/score for ranking
    pub score: f64,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Type of completion candidate
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CompletionType {
    Command,
    Parameter,
    Key,
    Value,
    Function,
    Keyword,
    File,
    Variable,
}

/// Completion context for understanding user input
#[derive(Debug, Clone)]
pub struct CompletionContext {
    /// Full input line
    pub full_input: String,
    /// Current word being completed
    pub current_word: String,
    /// Position in the input
    pub cursor_position: usize,
    /// Previous tokens in the input
    pub previous_tokens: Vec<String>,
    /// Current command context (if any)
    pub current_command: Option<String>,
}

impl CompletionEngine {
    /// Create a new completion engine
    pub fn new(command_registry: Arc<CommandRegistry>) -> Self {
        Self {
            _command_registry: command_registry,
            completion_cache: Arc::new(RwLock::new(HashMap::new())),
            key_cache: Arc::new(RwLock::new(HashSet::new())),
            config: CompletionConfig::default(),
        }
    }

    /// Create completion engine with custom configuration
    pub fn with_config(command_registry: Arc<CommandRegistry>, config: CompletionConfig) -> Self {
        Self {
            _command_registry: command_registry,
            completion_cache: Arc::new(RwLock::new(HashMap::new())),
            key_cache: Arc::new(RwLock::new(HashSet::new())),
            config,
        }
    }

    /// Get completion candidates for the given input
    pub fn get_completions(
        &self,
        input: &str,
        cursor_position: usize,
    ) -> Result<Vec<CompletionCandidate>> {
        let context = self.parse_completion_context(input, cursor_position)?;

        debug!("Completion context: {:?}", context);

        // Check cache first
        if self.config.enable_caching {
            if let Some(cached) = self.get_cached_completions(&context.current_word) {
                return Ok(cached);
            }
        }

        let mut candidates = Vec::new();

        // Add different types of completions based on context
        if context.previous_tokens.is_empty() || context.current_command.is_none() {
            // Complete commands
            candidates.extend(self.get_command_completions(&context)?);
        } else {
            // Complete command parameters
            candidates.extend(self.get_parameter_completions(&context)?);

            // Add key completions for commands that expect keys
            if self.command_expects_key(&context) {
                candidates.extend(self.get_key_completions(&context)?);
            }
        }

        // Add keyword completions
        candidates.extend(self.get_keyword_completions(&context)?);

        // Apply filtering and ranking
        candidates = self.filter_and_rank_candidates(candidates, &context);

        // Limit results
        candidates.truncate(self.config.max_candidates);

        // Cache results
        if self.config.enable_caching {
            self.cache_completions(&context.current_word, &candidates);
        }

        Ok(candidates)
    }

    /// Parse the completion context from input
    fn parse_completion_context(
        &self,
        input: &str,
        cursor_position: usize,
    ) -> Result<CompletionContext> {
        let input_up_to_cursor = &input[..cursor_position.min(input.len())];
        let tokens: Vec<String> = input_up_to_cursor
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();

        let current_word = if input_up_to_cursor.ends_with(' ') {
            String::new()
        } else {
            tokens.last().cloned().unwrap_or_default()
        };

        let previous_tokens = if input_up_to_cursor.ends_with(' ') {
            tokens.clone()
        } else {
            tokens
                .iter()
                .take(tokens.len().saturating_sub(1))
                .cloned()
                .collect()
        };

        let current_command = previous_tokens.first().cloned();

        Ok(CompletionContext {
            full_input: input.to_string(),
            current_word,
            cursor_position,
            previous_tokens,
            current_command,
        })
    }

    /// Get command completions
    fn get_command_completions(
        &self,
        context: &CompletionContext,
    ) -> Result<Vec<CompletionCandidate>> {
        let mut candidates = Vec::new();

        // Add basic database commands
        let commands = vec![
            ("PUT", "Store a key-value pair", "PUT key value"),
            ("GET", "Retrieve value for key", "GET key"),
            ("DELETE", "Remove key-value pair", "DELETE key"),
            ("EXISTS", "Check if key exists", "EXISTS key"),
            (
                "SCAN",
                "Scan keys with optional prefix",
                "SCAN [prefix] [limit]",
            ),
            ("BEGIN", "Start a new transaction", "BEGIN"),
            ("COMMIT", "Commit current transaction", "COMMIT"),
            ("ROLLBACK", "Rollback current transaction", "ROLLBACK"),
            ("TXPUT", "Put within transaction", "TXPUT key value"),
            ("TXGET", "Get within transaction", "TXGET key"),
            ("TXDELETE", "Delete within transaction", "TXDELETE key"),
            ("INFO", "Show database information", "INFO"),
            ("STATS", "Show database statistics", "STATS"),
            ("COMPACT", "Trigger compaction", "COMPACT"),
            ("BACKUP", "Create database backup", "BACKUP path"),
            ("HEALTH", "Show health status", "HEALTH"),
            ("SET", "Set session variable", "SET name value"),
            ("SHOW", "Show session variables", "SHOW [VARIABLES]"),
        ];

        for (cmd, desc, usage) in commands {
            if self.matches_query(&context.current_word, cmd) {
                candidates.push(CompletionCandidate {
                    text: cmd.to_string(),
                    display: format!("{} - {}", cmd, desc),
                    completion_type: CompletionType::Command,
                    description: Some(format!("Usage: {}", usage)),
                    score: self.calculate_match_score(&context.current_word, cmd),
                    metadata: {
                        let mut meta = HashMap::new();
                        meta.insert("usage".to_string(), usage.to_string());
                        meta.insert("category".to_string(), "database".to_string());
                        meta
                    },
                });
            }
        }

        // Add REPL-specific commands
        let repl_commands = vec![
            ("help", "Show help information", "help"),
            ("exit", "Exit the REPL", "exit"),
            ("quit", "Exit the REPL", "quit"),
            ("clear", "Clear the screen", "clear"),
            ("history", "Show command history", "history"),
        ];

        for (cmd, desc, usage) in repl_commands {
            if self.matches_query(&context.current_word, cmd) {
                candidates.push(CompletionCandidate {
                    text: cmd.to_string(),
                    display: format!("{} - {}", cmd, desc),
                    completion_type: CompletionType::Command,
                    description: Some(format!("Usage: {}", usage)),
                    score: self.calculate_match_score(&context.current_word, cmd),
                    metadata: {
                        let mut meta = HashMap::new();
                        meta.insert("usage".to_string(), usage.to_string());
                        meta.insert("category".to_string(), "repl".to_string());
                        meta
                    },
                });
            }
        }

        Ok(candidates)
    }

    /// Get parameter completions for the current command
    fn get_parameter_completions(
        &self,
        context: &CompletionContext,
    ) -> Result<Vec<CompletionCandidate>> {
        let mut candidates = Vec::new();

        if let Some(command) = &context.current_command {
            match command.to_uppercase().as_str() {
                "SCAN" => {
                    // For SCAN command, suggest common patterns
                    if context.previous_tokens.len() == 1 {
                        // First parameter - prefix
                        candidates.push(CompletionCandidate {
                            text: "\"\"".to_string(),
                            display: "\"\" - scan all keys".to_string(),
                            completion_type: CompletionType::Parameter,
                            description: Some("Scan all keys from the beginning".to_string()),
                            score: 1.0,
                            metadata: HashMap::new(),
                        });

                        // Add common prefixes from key cache
                        let key_cache = self.key_cache.read().unwrap();
                        let prefixes = self.extract_common_prefixes(&key_cache);
                        for prefix in prefixes {
                            if self.matches_query(&context.current_word, &prefix) {
                                candidates.push(CompletionCandidate {
                                    text: format!("\"{}\"", prefix),
                                    display: format!(
                                        "\"{}\" - keys starting with {}",
                                        prefix, prefix
                                    ),
                                    completion_type: CompletionType::Parameter,
                                    description: Some(format!(
                                        "Scan keys with prefix '{}'",
                                        prefix
                                    )),
                                    score: self
                                        .calculate_match_score(&context.current_word, &prefix),
                                    metadata: HashMap::new(),
                                });
                            }
                        }
                    } else if context.previous_tokens.len() == 2 {
                        // Second parameter - limit
                        for limit in &["10", "50", "100", "1000"] {
                            if self.matches_query(&context.current_word, limit) {
                                candidates.push(CompletionCandidate {
                                    text: limit.to_string(),
                                    display: format!("{} - limit to {} results", limit, limit),
                                    completion_type: CompletionType::Parameter,
                                    description: Some(format!(
                                        "Limit scan results to {} entries",
                                        limit
                                    )),
                                    score: self.calculate_match_score(&context.current_word, limit),
                                    metadata: HashMap::new(),
                                });
                            }
                        }
                    }
                }
                "BACKUP" => {
                    // Suggest backup file paths
                    candidates.push(CompletionCandidate {
                        text: "./backup.db".to_string(),
                        display: "./backup.db - backup to current directory".to_string(),
                        completion_type: CompletionType::File,
                        description: Some("Create backup in current directory".to_string()),
                        score: 1.0,
                        metadata: HashMap::new(),
                    });
                }
                "SET" => {
                    if context.previous_tokens.len() == 1 {
                        // Variable names
                        let variables = vec![
                            ("format", "Output format (table, json, csv)"),
                            ("timeout", "Command timeout in seconds"),
                            ("verbose", "Enable verbose output (true, false)"),
                            ("paging", "Enable result paging (true, false)"),
                        ];

                        for (var, desc) in variables {
                            if self.matches_query(&context.current_word, var) {
                                candidates.push(CompletionCandidate {
                                    text: var.to_string(),
                                    display: format!("{} - {}", var, desc),
                                    completion_type: CompletionType::Variable,
                                    description: Some(desc.to_string()),
                                    score: self.calculate_match_score(&context.current_word, var),
                                    metadata: HashMap::new(),
                                });
                            }
                        }
                    } else if context.previous_tokens.len() == 2 {
                        // Variable values based on variable name
                        let var_name = &context.previous_tokens[1];
                        match var_name.as_str() {
                            "format" => {
                                for format in &["table", "json", "csv", "yaml"] {
                                    candidates.push(CompletionCandidate {
                                        text: format.to_string(),
                                        display: format!("{} format", format),
                                        completion_type: CompletionType::Value,
                                        description: Some(format!(
                                            "Set output format to {}",
                                            format
                                        )),
                                        score: 1.0,
                                        metadata: HashMap::new(),
                                    });
                                }
                            }
                            "verbose" | "paging" => {
                                for value in &["true", "false"] {
                                    candidates.push(CompletionCandidate {
                                        text: value.to_string(),
                                        display: value.to_string(),
                                        completion_type: CompletionType::Value,
                                        description: Some(format!("Set {} to {}", var_name, value)),
                                        score: 1.0,
                                        metadata: HashMap::new(),
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(candidates)
    }

    /// Get key completions from the key cache
    fn get_key_completions(&self, context: &CompletionContext) -> Result<Vec<CompletionCandidate>> {
        let mut candidates = Vec::new();

        let key_cache = self.key_cache.read().unwrap();
        for key in key_cache.iter() {
            if self.matches_query(&context.current_word, key) {
                candidates.push(CompletionCandidate {
                    text: if key.contains(' ') || key.contains('"') {
                        format!("\"{}\"", key.replace('"', "\\\""))
                    } else {
                        key.clone()
                    },
                    display: key.clone(),
                    completion_type: CompletionType::Key,
                    description: Some("Database key".to_string()),
                    score: self.calculate_match_score(&context.current_word, key),
                    metadata: HashMap::new(),
                });
            }
        }

        Ok(candidates)
    }

    /// Get keyword completions
    fn get_keyword_completions(
        &self,
        context: &CompletionContext,
    ) -> Result<Vec<CompletionCandidate>> {
        let mut candidates = Vec::new();

        // Add SQL-like keywords that might be useful
        let keywords = vec![
            "WHERE", "ORDER", "BY", "LIMIT", "ASC", "DESC", "AND", "OR", "NOT", "NULL", "TRUE",
            "FALSE",
        ];

        for keyword in keywords {
            if self.matches_query(&context.current_word, keyword) {
                candidates.push(CompletionCandidate {
                    text: keyword.to_string(),
                    display: keyword.to_string(),
                    completion_type: CompletionType::Keyword,
                    description: Some(format!("{} keyword", keyword)),
                    score: self.calculate_match_score(&context.current_word, keyword) * 0.5, // Lower priority
                    metadata: HashMap::new(),
                });
            }
        }

        Ok(candidates)
    }

    /// Check if a command expects a key parameter
    fn command_expects_key(&self, context: &CompletionContext) -> bool {
        if let Some(command) = &context.current_command {
            match command.to_uppercase().as_str() {
                "GET" | "DELETE" | "EXISTS" | "TXGET" | "TXDELETE" => {
                    context.previous_tokens.len() == 1
                }
                "PUT" | "TXPUT" => context.previous_tokens.len() == 1,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Check if query matches candidate (with fuzzy matching if enabled)
    fn matches_query(&self, query: &str, candidate: &str) -> bool {
        if query.is_empty() {
            return true;
        }

        if query.len() < self.config.min_query_length {
            return false;
        }

        // Exact prefix match
        if candidate.to_lowercase().starts_with(&query.to_lowercase()) {
            return true;
        }

        // Fuzzy matching if enabled
        if self.config.enable_fuzzy_matching {
            self.fuzzy_match(query, candidate)
        } else {
            false
        }
    }

    /// Simple fuzzy matching algorithm
    fn fuzzy_match(&self, query: &str, candidate: &str) -> bool {
        let query = query.to_lowercase();
        let candidate = candidate.to_lowercase();

        let mut query_chars = query.chars().peekable();
        let mut candidate_chars = candidate.chars();

        while let Some(query_char) = query_chars.next() {
            let mut found = false;

            while let Some(candidate_char) = candidate_chars.next() {
                if query_char == candidate_char {
                    found = true;
                    break;
                }
            }

            if !found {
                return false;
            }
        }

        true
    }

    /// Calculate match score for ranking
    fn calculate_match_score(&self, query: &str, candidate: &str) -> f64 {
        if query.is_empty() {
            return 0.5;
        }

        let query = query.to_lowercase();
        let candidate = candidate.to_lowercase();

        // Exact match gets highest score
        if query == candidate {
            return 1.0;
        }

        // Prefix match gets high score
        if candidate.starts_with(&query) {
            return 0.9 - (candidate.len() - query.len()) as f64 * 0.01;
        }

        // Fuzzy match gets lower score
        if self.fuzzy_match(&query, &candidate) {
            let distance = self.levenshtein_distance(&query, &candidate);
            let max_len = query.len().max(candidate.len());
            return 0.5 - (distance as f64 / max_len as f64) * 0.3;
        }

        0.0
    }

    /// Calculate Levenshtein distance
    fn levenshtein_distance(&self, a: &str, b: &str) -> usize {
        let a_chars: Vec<char> = a.chars().collect();
        let b_chars: Vec<char> = b.chars().collect();
        let a_len = a_chars.len();
        let b_len = b_chars.len();

        let mut matrix = vec![vec![0; b_len + 1]; a_len + 1];

        for i in 0..=a_len {
            matrix[i][0] = i;
        }
        for j in 0..=b_len {
            matrix[0][j] = j;
        }

        for i in 1..=a_len {
            for j in 1..=b_len {
                let cost = if a_chars[i - 1] == b_chars[j - 1] {
                    0
                } else {
                    1
                };
                matrix[i][j] = (matrix[i - 1][j] + 1)
                    .min(matrix[i][j - 1] + 1)
                    .min(matrix[i - 1][j - 1] + cost);
            }
        }

        matrix[a_len][b_len]
    }

    /// Filter and rank candidates
    fn filter_and_rank_candidates(
        &self,
        mut candidates: Vec<CompletionCandidate>,
        _context: &CompletionContext,
    ) -> Vec<CompletionCandidate> {
        // Sort by score (descending) and then by text (ascending)
        candidates.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.text.cmp(&b.text))
        });

        // Remove duplicates
        candidates.dedup_by(|a, b| a.text == b.text);

        candidates
    }

    /// Extract common prefixes from keys
    fn extract_common_prefixes(&self, keys: &HashSet<String>) -> Vec<String> {
        let mut prefixes = HashSet::new();

        for key in keys.iter() {
            // Extract prefixes by common separators
            for separator in &[":", "_", "-", "/", "."] {
                if let Some(pos) = key.find(separator) {
                    let prefix = &key[..pos];
                    if prefix.len() > 1 {
                        prefixes.insert(prefix.to_string());
                    }
                }
            }

            // Extract prefixes by length
            if key.len() > 4 {
                prefixes.insert(key[..key.len().min(4)].to_string());
            }
        }

        prefixes.into_iter().collect()
    }

    /// Get cached completions
    fn get_cached_completions(&self, query: &str) -> Option<Vec<CompletionCandidate>> {
        let cache = self.completion_cache.read().unwrap();
        cache.get(query).cloned()
    }

    /// Cache completions
    fn cache_completions(&self, query: &str, candidates: &[CompletionCandidate]) {
        let mut cache = self.completion_cache.write().unwrap();
        cache.insert(query.to_string(), candidates.to_vec());

        // Simple cache size management
        if cache.len() > 1000 {
            cache.clear();
        }
    }

    /// Update key cache with new keys
    pub fn update_key_cache(&self, keys: Vec<String>) {
        let mut cache = self.key_cache.write().unwrap();
        for key in keys {
            cache.insert(key);
        }

        // Limit cache size
        if cache.len() > 10000 {
            let keys_to_remove: Vec<_> = cache.iter().take(cache.len() - 8000).cloned().collect();
            for key in keys_to_remove {
                cache.remove(&key);
            }
        }
    }

    /// Clear completion cache
    pub fn clear_cache(&self) {
        self.completion_cache.write().unwrap().clear();
    }

    /// Get completion statistics
    pub fn get_completion_stats(&self) -> CompletionStats {
        let cache = self.completion_cache.read().unwrap();
        let key_cache = self.key_cache.read().unwrap();

        CompletionStats {
            cached_queries: cache.len(),
            cached_keys: key_cache.len(),
            cache_hit_rate: 0.0, // Would need to track hits/misses
        }
    }
}

/// Completion engine statistics
#[derive(Debug, Serialize)]
pub struct CompletionStats {
    pub cached_queries: usize,
    pub cached_keys: usize,
    pub cache_hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completion_context_parsing() {
        let registry = Arc::new(CommandRegistry::new());
        let engine = CompletionEngine::new(registry);

        let context = engine.parse_completion_context("GET key", 7).unwrap();
        assert_eq!(context.current_word, "key");
        assert_eq!(context.previous_tokens, vec!["GET"]);
        assert_eq!(context.current_command, Some("GET".to_string()));
    }

    #[test]
    fn test_fuzzy_matching() {
        let registry = Arc::new(CommandRegistry::new());
        let engine = CompletionEngine::new(registry);

        assert!(engine.fuzzy_match("gt", "GET"));
        assert!(engine.fuzzy_match("del", "DELETE"));
        assert!(!engine.fuzzy_match("xyz", "GET"));
    }

    #[test]
    fn test_match_scoring() {
        let registry = Arc::new(CommandRegistry::new());
        let engine = CompletionEngine::new(registry);

        // Exact match should score highest
        assert!(engine.calculate_match_score("GET", "GET") > 0.9);

        // Prefix match should score high
        assert!(engine.calculate_match_score("G", "GET") > 0.8);

        // Fuzzy match should score lower
        assert!(engine.calculate_match_score("GT", "GET") > 0.3);
        assert!(engine.calculate_match_score("GT", "GET") < 0.8);
    }

    #[test]
    fn test_command_completions() {
        let registry = Arc::new(CommandRegistry::new());
        let engine = CompletionEngine::new(registry);

        let context = CompletionContext {
            full_input: "G".to_string(),
            current_word: "G".to_string(),
            cursor_position: 1,
            previous_tokens: vec![],
            current_command: None,
        };

        let completions = engine.get_command_completions(&context).unwrap();
        let get_completion = completions.iter().find(|c| c.text == "GET");
        assert!(get_completion.is_some());
    }

    #[test]
    fn test_levenshtein_distance() {
        let registry = Arc::new(CommandRegistry::new());
        let engine = CompletionEngine::new(registry);

        assert_eq!(engine.levenshtein_distance("", ""), 0);
        assert_eq!(engine.levenshtein_distance("GET", "GET"), 0);
        assert_eq!(engine.levenshtein_distance("GET", "PUT"), 2);
        assert_eq!(engine.levenshtein_distance("G", "GET"), 2);
    }

    #[test]
    fn test_key_cache_operations() {
        let registry = Arc::new(CommandRegistry::new());
        let engine = CompletionEngine::new(registry);

        engine.update_key_cache(vec!["user:1".to_string(), "user:2".to_string()]);

        let key_cache = engine.key_cache.read().unwrap();
        assert!(key_cache.contains("user:1"));
        assert!(key_cache.contains("user:2"));
    }

    #[test]
    fn test_prefix_extraction() {
        let registry = Arc::new(CommandRegistry::new());
        let engine = CompletionEngine::new(registry);

        let mut keys = HashSet::new();
        keys.insert("user:1:profile".to_string());
        keys.insert("user:2:profile".to_string());
        keys.insert("config_setting".to_string());

        let prefixes = engine.extract_common_prefixes(&keys);
        assert!(prefixes.contains(&"user".to_string()));
        assert!(prefixes.contains(&"config".to_string()));
    }
}
