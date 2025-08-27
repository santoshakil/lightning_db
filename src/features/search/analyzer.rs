use std::sync::Arc;
use crate::error::Result;

pub trait Analyzer: Send + Sync {
    fn analyze(&self, text: &str) -> Result<Vec<String>>;
}

pub struct StandardAnalyzer {
    lowercase: bool,
    stop_words: Vec<String>,
}

pub struct LanguageAnalyzer {
    language: Language,
    stemmer: Arc<dyn Stemmer>,
}

pub enum Language {
    English,
    Spanish,
    French,
    German,
    Chinese,
    Japanese,
}

trait Stemmer: Send + Sync {
    fn stem(&self, word: &str) -> String;
}

impl Analyzer for StandardAnalyzer {
    fn analyze(&self, text: &str) -> Result<Vec<String>> {
        let mut tokens = text.split_whitespace()
            .map(|s| if self.lowercase { s.to_lowercase() } else { s.to_string() })
            .filter(|s| !self.stop_words.contains(s))
            .collect();
        Ok(tokens)
    }
}

impl StandardAnalyzer {
    pub fn new() -> Self {
        Self {
            lowercase: true,
            stop_words: vec!["the".to_string(), "a".to_string(), "an".to_string()],
        }
    }
}