use std::path::Path;
use crate::{Database, LightningDbConfig, Result};

pub struct DatabaseBuilder {
    config: LightningDbConfig,
}

impl DatabaseBuilder {
    pub fn new() -> Self {
        Self {
            config: LightningDbConfig::default(),
        }
    }
    
    pub fn with_config(config: LightningDbConfig) -> Self {
        Self { config }
    }
    
    pub fn cache_size(mut self, size: u64) -> Self {
        self.config.cache_size = size;
        self
    }
    
    pub fn page_size(mut self, size: u64) -> Self {
        self.config.page_size = size;
        self
    }
    
    pub fn compression_enabled(mut self, enabled: bool) -> Self {
        self.config.compression_enabled = enabled;
        self
    }
    
    pub fn compression_level(mut self, level: i32) -> Self {
        self.config.compression_level = Some(level);
        self
    }
    
    pub fn max_active_transactions(mut self, limit: usize) -> Self {
        self.config.max_active_transactions = limit;
        self
    }
    
    pub fn prefetch_enabled(mut self, enabled: bool) -> Self {
        self.config.prefetch_enabled = enabled;
        self
    }
    
    pub fn create<P: AsRef<Path>>(self, path: P) -> Result<Database> {
        Database::create(path, self.config)
    }
    
    pub fn create_temp(self) -> Result<Database> {
        // For now, just use the existing create_temp method
        // In full implementation, we'd pass the config
        Database::create_temp()
    }
    
    pub fn open<P: AsRef<Path>>(self, path: P) -> Result<Database> {
        Database::open(path, self.config)
    }
}

impl Default for DatabaseBuilder {
    fn default() -> Self {
        Self::new()
    }
}