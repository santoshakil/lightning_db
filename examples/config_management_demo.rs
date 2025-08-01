//! Configuration Management Demo
//!
//! This example demonstrates the configuration management capabilities
//! of Lightning DB including templates, environments, and hot-reloading.

use lightning_db::{
    Database, 
    LightningDbConfig,
    config_management::{
        ConfigManager, ConfigSource, ConfigFormat, ConfigBuilder,
        EnvironmentConfig, ResourceLimits,
    }
};
use std::collections::HashMap;
use std::path::Path;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Lightning DB Configuration Management Demo ===\n");

    // 1. Configuration Builder Demo
    println!("1. Using Configuration Builder:");
    let config = ConfigBuilder::new()
        .cache_size(512 * 1024 * 1024)  // 512MB
        .compression(true)
        .compression_type(lightning_db::CompressionType::Zstd)
        .wal_sync_mode(lightning_db::WalSyncMode::Sync)
        .max_transactions(100)
        .prefetch(true)
        .build();
    
    println!("Built configuration:");
    println!("  Cache size: {} MB", config.cache_size / 1024 / 1024);
    println!("  Compression: {} ({})", 
        config.compression_enabled, 
        format!("{:?}", config.compression_type)
    );
    println!("  WAL sync mode: {:?}", config.wal_sync_mode);
    println!();

    // 2. Configuration Manager Demo
    println!("2. Configuration Manager:");
    let mut config_manager = ConfigManager::new();
    
    // Create a sample configuration file
    let config_json = r#"{
        "cache_size": 1073741824,
        "compression_enabled": true,
        "compression_type": "Lz4",
        "wal_sync_mode": "Async",
        "prefetch_enabled": true,
        "prefetch_distance": 32,
        "enable_statistics": true,
        "max_active_transactions": 500
    }"#;
    
    fs::write("demo_config.json", config_json)?;
    
    // Load from file
    config_manager.load_from_source(
        ConfigSource::File("demo_config.json".into())
    )?;
    
    let loaded_config = config_manager.get_config();
    println!("Loaded configuration from file:");
    println!("  Cache size: {} MB", loaded_config.cache_size / 1024 / 1024);
    println!("  Max transactions: {}", loaded_config.max_active_transactions);
    println!();

    // 3. Template Demo
    println!("3. Configuration Templates:");
    println!("Available templates:");
    println!("  - high_performance: Maximum performance, high resource usage");
    println!("  - balanced: Good performance with reasonable resources");
    println!("  - low_resource: Minimal resource usage");
    
    // Apply high performance template
    config_manager.apply_template("high_performance", HashMap::new())?;
    let perf_config = config_manager.get_config();
    println!("\nHigh performance template applied:");
    println!("  Cache size: {} GB", perf_config.cache_size / 1024 / 1024 / 1024);
    println!("  Compression: {}", perf_config.compression_enabled);
    println!("  WAL mode: {:?}", perf_config.wal_sync_mode);
    println!();

    // 4. Environment Configuration Demo
    println!("4. Environment Variables:");
    std::env::set_var("LIGHTNING_DB_CACHE_SIZE", "2147483648"); // 2GB
    std::env::set_var("LIGHTNING_DB_COMPRESSION", "true");
    std::env::set_var("LIGHTNING_DB_WAL_SYNC_MODE", "sync");
    
    config_manager.load_from_source(ConfigSource::Environment)?;
    let env_config = config_manager.get_config();
    println!("Configuration from environment:");
    println!("  Cache size: {} GB", env_config.cache_size / 1024 / 1024 / 1024);
    println!("  WAL mode: {:?}", env_config.wal_sync_mode);
    println!();

    // 5. Configuration Export Demo
    println!("5. Configuration Export:");
    
    // Export to different formats
    config_manager.export_config(Path::new("config_export.json"), ConfigFormat::Json)?;
    config_manager.export_config(Path::new("config_export.yaml"), ConfigFormat::Yaml)?;
    config_manager.export_config(Path::new("config_export.toml"), ConfigFormat::Toml)?;
    
    println!("Exported configuration to:");
    println!("  - config_export.json");
    println!("  - config_export.yaml");
    println!("  - config_export.toml");
    println!();

    // 6. Configuration Diff Demo
    println!("6. Configuration Comparison:");
    let config1 = ConfigBuilder::new()
        .cache_size(1024 * 1024 * 1024)
        .compression(true)
        .build();
        
    let config2 = ConfigBuilder::new()
        .cache_size(2048 * 1024 * 1024)
        .compression(false)
        .prefetch(true)
        .build();
    
    let diff = ConfigManager::diff_configs(&config1, &config2);
    println!("Configuration differences:");
    if !diff.modified.is_empty() {
        println!("  Modified fields:");
        for (key, (val1, val2)) in &diff.modified {
            println!("    {}: {} -> {}", key, val1, val2);
        }
    }
    if !diff.added.is_empty() {
        println!("  Added fields:");
        for (key, val) in &diff.added {
            println!("    {}: {}", key, val);
        }
    }
    println!();

    // 7. Configuration Validation Demo
    println!("7. Configuration Validation:");
    
    // Valid configuration
    let valid_config = ConfigBuilder::new()
        .cache_size(1024 * 1024 * 100)  // 100MB
        .build();
    
    match config_manager.validate_config(&valid_config) {
        Ok(_) => println!("✅ Valid configuration"),
        Err(e) => println!("❌ Invalid configuration: {}", e),
    }
    
    // Invalid configuration (cache size = 0)
    let invalid_config = ConfigBuilder::new()
        .cache_size(0)
        .build();
    
    match config_manager.validate_config(&invalid_config) {
        Ok(_) => println!("✅ Valid configuration"),
        Err(e) => println!("❌ Invalid configuration: {}", e),
    }
    println!();

    // 8. Hot-reload Demo (setup only)
    println!("8. Hot-reload Configuration:");
    println!("Hot-reload can be enabled with:");
    println!("  config_manager.enable_hot_reload()?;");
    println!("  // Configuration will automatically reload when files change");
    println!();

    // 9. Using Configuration with Database
    println!("9. Creating Database with Managed Configuration:");
    let final_config = config_manager.get_config();
    let db = Database::create("./config_demo.db", final_config)?;
    
    // Test database
    db.put(b"config_test", b"Configuration management works!")?;
    let value = db.get(b"config_test")?;
    println!("Database test: {:?}", 
        value.map(|v| String::from_utf8_lossy(&v).to_string())
    );
    
    // Cleanup
    let _ = fs::remove_file("demo_config.json");
    let _ = fs::remove_file("config_export.json");
    let _ = fs::remove_file("config_export.yaml");
    let _ = fs::remove_file("config_export.toml");
    let _ = fs::remove_dir_all("./config_demo.db");
    
    println!("\n✅ Configuration management demo completed!");
    
    Ok(())
}

// Example: Production configuration setup
#[allow(dead_code)]
fn production_setup() -> Result<ConfigManager, Box<dyn std::error::Error>> {
    let mut config_manager = ConfigManager::new();
    
    // Load base configuration from file
    config_manager.load_from_source(
        ConfigSource::File("/etc/lightning_db/config.json".into())
    )?;
    
    // Override with environment-specific settings
    config_manager.load_from_source(ConfigSource::Environment)?;
    
    // Apply command-line overrides
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        config_manager.load_from_source(
            ConfigSource::CommandLine(args[1..].to_vec())
        )?;
    }
    
    // Enable hot-reload for configuration changes
    config_manager.enable_hot_reload()?;
    
    // Register callback for configuration changes
    config_manager.on_change(|new_config| {
        println!("Configuration updated!");
        println!("  New cache size: {} MB", new_config.cache_size / 1024 / 1024);
    });
    
    Ok(config_manager)
}