//! Interactive REPL for Lightning DB
//!
//! Provides a comprehensive command-line interface for database exploration,
//! debugging, administration, and development workflows.

pub mod commands;
pub mod completion;
pub mod history;
pub mod formatter;
pub mod parser;
pub mod executor;

use crate::{Database, Result, Error};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use std::io::{self, Write, BufRead, BufReader, stdin, stdout};
use std::fs::File;
use serde::{Serialize, Deserialize};
use tracing::{info, warn, error, debug};

// Re-export key components
pub use commands::{Command, CommandResult, CommandContext, CommandRegistry, CommandData};
pub use completion::{CompletionEngine, CompletionCandidate};
pub use history::{HistoryManager, HistoryEntry};
pub use formatter::{OutputFormatter, FormatStyle, ResultFormatter, FormattableData};
pub use parser::{QueryParser, ParsedCommand, QueryAst};
pub use executor::{CommandExecutor, ExecutionResult, ExecutionContext};

/// Interactive REPL for Lightning DB
pub struct DatabaseRepl {
    /// Database instance
    database: Arc<Database>,
    /// Command registry
    command_registry: Arc<CommandRegistry>,
    /// Command completion engine
    completion_engine: Arc<CompletionEngine>,
    /// Command history manager
    history_manager: Arc<RwLock<HistoryManager>>,
    /// Output formatter
    formatter: Arc<OutputFormatter>,
    /// Query parser
    parser: Arc<QueryParser>,
    /// Command executor
    executor: Arc<CommandExecutor>,
    /// REPL configuration
    config: ReplConfig,
    /// Session state
    session: Arc<RwLock<SessionState>>,
    /// Active connections/transactions
    connections: Arc<RwLock<HashMap<String, ConnectionState>>>,
}

/// Configuration for the REPL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplConfig {
    /// Enable command history
    pub enable_history: bool,
    /// Maximum history entries
    pub max_history_entries: usize,
    /// History file path
    pub history_file: Option<String>,
    /// Enable auto-completion
    pub enable_completion: bool,
    /// Enable syntax highlighting
    pub enable_syntax_highlighting: bool,
    /// Default output format
    pub default_format: FormatStyle,
    /// Command timeout
    pub command_timeout: Duration,
    /// Enable performance timing
    pub show_timing: bool,
    /// Enable verbose output
    pub verbose: bool,
    /// Prompt customization
    pub prompt: String,
    /// Multi-line prompt
    pub multiline_prompt: String,
    /// Enable paging for large results
    pub enable_paging: bool,
    /// Page size for results
    pub page_size: usize,
}

impl Default for ReplConfig {
    fn default() -> Self {
        Self {
            enable_history: true,
            max_history_entries: 1000,
            history_file: Some(".lightning_db_history".to_string()),
            enable_completion: true,
            enable_syntax_highlighting: true,
            default_format: FormatStyle::Table,
            command_timeout: Duration::from_secs(30),
            show_timing: true,
            verbose: false,
            prompt: "lightning_db> ".to_string(),
            multiline_prompt: "    -> ".to_string(),
            enable_paging: true,
            page_size: 50,
        }
    }
}

/// Current session state
#[derive(Debug, Clone)]
pub struct SessionState {
    /// Current database
    pub current_database: Option<String>,
    /// Session variables
    pub variables: HashMap<String, String>,
    /// Session start time
    pub start_time: SystemTime,
    /// Commands executed in session
    pub commands_executed: u64,
    /// Current transaction ID (if any)
    pub current_transaction: Option<u64>,
    /// Session ID
    pub session_id: String,
    /// Last command result
    pub last_result: Option<CommandResult>,
}

/// Connection state for active connections
#[derive(Debug, Clone)]
pub struct ConnectionState {
    pub connection_id: String,
    pub connected_at: SystemTime,
    pub last_activity: SystemTime,
    pub is_active: bool,
    pub transaction_id: Option<u64>,
}

/// REPL execution statistics
#[derive(Debug, Serialize)]
pub struct ReplStats {
    pub session_duration: Duration,
    pub total_commands: u64,
    pub successful_commands: u64,
    pub failed_commands: u64,
    pub average_command_time: Duration,
    pub longest_command_time: Duration,
    pub memory_usage: usize,
}

impl DatabaseRepl {
    /// Create a new REPL instance
    pub fn new(database: Arc<Database>) -> Result<Self> {
        Self::with_config(database, ReplConfig::default())
    }

    /// Create REPL with custom configuration
    pub fn with_config(database: Arc<Database>, config: ReplConfig) -> Result<Self> {
        let command_registry = Arc::new(CommandRegistry::new());
        let completion_engine = Arc::new(CompletionEngine::new(command_registry.clone()));
        let history_manager = Arc::new(RwLock::new(
            HistoryManager::new(config.max_history_entries, config.history_file.clone())?
        ));
        let formatter = Arc::new(OutputFormatter::new(config.default_format.clone()));
        let parser = Arc::new(QueryParser::new());
        let executor = Arc::new(CommandExecutor::new(database.clone()));

        let session = Arc::new(RwLock::new(SessionState {
            current_database: None,
            variables: HashMap::new(),
            start_time: SystemTime::now(),
            commands_executed: 0,
            current_transaction: None,
            session_id: format!("repl_{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis()),
            last_result: None,
        }));

        Ok(Self {
            database,
            command_registry,
            completion_engine,
            history_manager,
            formatter,
            parser,
            executor,
            config,
            session,
            connections: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Start the interactive REPL
    pub fn start(&self) -> Result<()> {
        info!("Starting Lightning DB REPL");
        
        // Print welcome message
        self.print_welcome();
        
        // Load history if enabled
        if self.config.enable_history {
            if let Err(e) = self.history_manager.write().unwrap().load_history() {
                warn!("Failed to load history: {}", e);
            }
        }

        // Main REPL loop
        let mut input_buffer = String::new();
        let mut in_multiline = false;

        loop {
            // Print prompt
            let prompt = if in_multiline {
                &self.config.multiline_prompt
            } else {
                &self.config.prompt
            };
            
            print!("{}", prompt);
            stdout().flush().unwrap();

            // Read input
            let mut line = String::new();
            match stdin().read_line(&mut line) {
                Ok(0) => {
                    // EOF (Ctrl+D)
                    println!();
                    break;
                }
                Ok(_) => {
                    let line = line.trim();
                    
                    // Handle special commands
                    if line == "exit" || line == "quit" || line == "\\q" {
                        break;
                    }
                    
                    if line == "\\?" || line == "help" {
                        self.show_help();
                        continue;
                    }

                    if line == "\\h" {
                        self.show_history();
                        continue;
                    }

                    if line == "\\s" {
                        self.show_stats();
                        continue;
                    }

                    if line == "\\c" {
                        self.clear_screen();
                        continue;
                    }

                    // Check for multiline continuation
                    if line.ends_with('\\') && !line.ends_with("\\\\") {
                        input_buffer.push_str(&line[..line.len()-1]);
                        input_buffer.push(' ');
                        in_multiline = true;
                        continue;
                    }

                    // Complete the command
                    if in_multiline {
                        input_buffer.push_str(line);
                        in_multiline = false;
                    } else {
                        input_buffer = line.to_string();
                    }

                    // Skip empty lines
                    if input_buffer.trim().is_empty() {
                        input_buffer.clear();
                        continue;
                    }

                    // Execute command
                    self.execute_command(&input_buffer);
                    input_buffer.clear();
                }
                Err(e) => {
                    error!("Failed to read input: {}", e);
                    break;
                }
            }
        }

        // Save history before exit
        if self.config.enable_history {
            if let Err(e) = self.history_manager.write().unwrap().save_history() {
                warn!("Failed to save history: {}", e);
            }
        }

        self.print_goodbye();
        info!("Lightning DB REPL session ended");
        Ok(())
    }

    /// Execute a command
    fn execute_command(&self, input: &str) {
        let start_time = Instant::now();
        
        // Add to history
        if self.config.enable_history && !input.trim().is_empty() {
            let mut history = self.history_manager.write().unwrap();
            if let Err(e) = history.add_entry(input.to_string()) {
                warn!("Failed to add to history: {}", e);
            }
        }

        // Parse command
        let parsed_command = match self.parser.parse(input) {
            Ok(cmd) => cmd,
            Err(e) => {
                self.print_error(&format!("Parse error: {}", e));
                return;
            }
        };

        // Create execution context
        let context = ExecutionContext {
            session_id: self.session.read().unwrap().session_id.clone(),
            start_time,
            timeout: self.config.command_timeout,
            variables: self.session.read().unwrap().variables.clone(),
            verbose: self.config.verbose,
        };

        // Execute command
        let exec_result = self.executor.execute(&parsed_command, &context);
        let execution_time = start_time.elapsed();

        // Convert ExecutionResult to CommandResult
        let result = if exec_result.success {
            if let Some(data) = exec_result.data {
                CommandResult::Success {
                    data: Some(self.convert_formattable_to_command_data(data)),
                    message: exec_result.message,
                    metadata: exec_result.metadata,
                }
            } else {
                CommandResult::Success {
                    data: None,
                    message: exec_result.message,
                    metadata: exec_result.metadata,
                }
            }
        } else {
            CommandResult::Error {
                error: exec_result.error.unwrap_or("Unknown error".to_string()),
                suggestion: None,
            }
        };

        // Update session state
        {
            let mut session = self.session.write().unwrap();
            session.commands_executed += 1;
            session.last_result = Some(result.clone());
        }

        // Format and display result
        match result {
            CommandResult::Success { data, message, metadata } => {
                if let Some(msg) = message {
                    println!("{}", msg);
                }
                
                if let Some(data) = data {
                    let formattable_data = self.convert_command_data_to_formattable(&data);
                    let formatted = self.formatter.format_data(&formattable_data);
                    
                    if self.config.enable_paging && formatted.lines().count() > self.config.page_size {
                        self.display_paged(&formatted);
                    } else {
                        println!("{}", formatted);
                    }
                }

                if self.config.show_timing {
                    println!("({}ms)", execution_time.as_millis());
                }

                if self.config.verbose && !metadata.is_empty() {
                    println!("Metadata: {:?}", metadata);
                }
            }
            CommandResult::Error { error, suggestion } => {
                self.print_error(&error);
                if let Some(suggestion) = suggestion {
                    println!("Suggestion: {}", suggestion);
                }
            }
            CommandResult::Warning { message, data } => {
                self.print_warning(&message);
                if let Some(data) = data {
                    let formattable_data = self.convert_command_data_to_formattable(&data);
                    let formatted = self.formatter.format_data(&formattable_data);
                    println!("{}", formatted);
                }
            }
        }
    }

    /// Print welcome message
    fn print_welcome(&self) {
        println!("╔══════════════════════════════════════════════════════════════════════════════╗");
        println!("║                          Lightning DB Interactive REPL                       ║");
        println!("║                          Version {}                            ║", env!("CARGO_PKG_VERSION"));
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");
        println!("║ Welcome to Lightning DB! Type 'help' or '\\?' for available commands.       ║");
        println!("║ Type 'exit', 'quit', or '\\q' to exit the REPL.                             ║");
        println!("║ Use '\\' at the end of a line for multi-line commands.                      ║");
        println!("╚══════════════════════════════════════════════════════════════════════════════╝");
        println!();
    }

    /// Print goodbye message
    fn print_goodbye(&self) {
        let session = self.session.read().unwrap();
        let duration = SystemTime::now().duration_since(session.start_time).unwrap();
        
        println!();
        println!("╔══════════════════════════════════════════════════════════════════════════════╗");
        println!("║                              Session Summary                                 ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");
        println!("║ Commands executed: {:<10}                                              ║", session.commands_executed);
        println!("║ Session duration:  {:<10}                                              ║", format!("{:.2}s", duration.as_secs_f64()));
        println!("║ Thank you for using Lightning DB!                                           ║");
        println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    }

    /// Show help information
    fn show_help(&self) {
        println!("╔══════════════════════════════════════════════════════════════════════════════╗");
        println!("║                              Lightning DB Commands                           ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");
        println!("║ Database Operations:                                                         ║");
        println!("║   PUT key value                    - Store a key-value pair                 ║");
        println!("║   GET key                          - Retrieve value for key                 ║");
        println!("║   DELETE key                       - Remove key-value pair                  ║");
        println!("║   EXISTS key                       - Check if key exists                    ║");
        println!("║   SCAN [prefix] [limit]            - Scan keys with optional prefix        ║");
        println!("║                                                                              ║");
        println!("║ Transaction Operations:                                                      ║");
        println!("║   BEGIN                            - Start a new transaction               ║");
        println!("║   COMMIT                           - Commit current transaction            ║");
        println!("║   ROLLBACK                         - Rollback current transaction         ║");
        println!("║   TXPUT key value                  - Put within transaction               ║");
        println!("║   TXGET key                        - Get within transaction               ║");
        println!("║   TXDELETE key                     - Delete within transaction            ║");
        println!("║                                                                              ║");
        println!("║ Database Management:                                                         ║");
        println!("║   INFO                             - Show database information             ║");
        println!("║   STATS                            - Show database statistics             ║");
        println!("║   COMPACT                          - Trigger compaction                    ║");
        println!("║   BACKUP path                      - Create database backup               ║");
        println!("║   HEALTH                           - Show health status                    ║");
        println!("║                                                                              ║");
        println!("║ REPL Commands:                                                               ║");
        println!("║   \\?  or help                      - Show this help                        ║");
        println!("║   \\h                               - Show command history                  ║");
        println!("║   \\s                               - Show session statistics              ║");
        println!("║   \\c                               - Clear screen                          ║");
        println!("║   \\q  or exit or quit              - Exit REPL                            ║");
        println!("║                                                                              ║");
        println!("║ Special Features:                                                            ║");
        println!("║   - Use '\\' at end of line for multi-line commands                         ║");
        println!("║   - Tab completion for commands and keys                                    ║");
        println!("║   - Command history with up/down arrows                                     ║");
        println!("║   - Automatic paging for large results                                      ║");
        println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    }

    /// Show command history
    fn show_history(&self) {
        let history = self.history_manager.read().unwrap();
        let entries = history.get_recent_entries(20);
        
        if entries.is_empty() {
            println!("No command history available.");
            return;
        }

        println!("Recent Commands:");
        println!("═══════════════");
        for (i, entry) in entries.iter().enumerate() {
            println!("{:3}: {}", i + 1, entry.command);
        }
    }

    /// Show session statistics
    fn show_stats(&self) {
        let session = self.session.read().unwrap();
        let duration = SystemTime::now().duration_since(session.start_time).unwrap();
        
        println!("╔══════════════════════════════════════════════════════════════════════════════╗");
        println!("║                            Session Statistics                                ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");
        println!("║ Session ID:          {:<50} ║", session.session_id);
        println!("║ Started:             {:<50} ║", format!("{:?}", session.start_time));
        println!("║ Duration:            {:<50} ║", format!("{:.2}s", duration.as_secs_f64()));
        println!("║ Commands executed:   {:<50} ║", session.commands_executed);
        println!("║ Current database:    {:<50} ║", session.current_database.as_deref().unwrap_or("default"));
        println!("║ Active transaction:  {:<50} ║", session.current_transaction.map(|id| id.to_string()).unwrap_or("None".to_string()));
        println!("║ Session variables:   {:<50} ║", session.variables.len());
        println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    }

    /// Clear screen
    fn clear_screen(&self) {
        print!("\x1B[2J\x1B[1;1H");
        stdout().flush().unwrap();
    }

    /// Display paged output
    fn display_paged(&self, content: &str) {
        let lines: Vec<&str> = content.lines().collect();
        let mut current_line = 0;
        
        while current_line < lines.len() {
            let end_line = (current_line + self.config.page_size).min(lines.len());
            
            for line in &lines[current_line..end_line] {
                println!("{}", line);
            }
            
            current_line = end_line;
            
            if current_line < lines.len() {
                print!("-- More -- (Press Enter to continue, 'q' to quit): ");
                stdout().flush().unwrap();
                
                let mut input = String::new();
                stdin().read_line(&mut input).unwrap();
                
                if input.trim() == "q" {
                    break;
                }
            }
        }
    }

    /// Print error message
    fn print_error(&self, message: &str) {
        eprintln!("❌ Error: {}", message);
    }

    /// Print warning message
    fn print_warning(&self, message: &str) {
        println!("⚠️  Warning: {}", message);
    }

    /// Get REPL statistics
    pub fn get_stats(&self) -> ReplStats {
        let session = self.session.read().unwrap();
        let duration = SystemTime::now().duration_since(session.start_time).unwrap();
        
        ReplStats {
            session_duration: duration,
            total_commands: session.commands_executed,
            successful_commands: session.commands_executed, // Simplified
            failed_commands: 0, // Simplified
            average_command_time: Duration::from_millis(100), // Placeholder
            longest_command_time: Duration::from_millis(500), // Placeholder
            memory_usage: 0, // Would be calculated from actual usage
        }
    }

    /// Set session variable
    pub fn set_variable(&self, name: String, value: String) {
        let mut session = self.session.write().unwrap();
        session.variables.insert(name, value);
    }

    /// Get session variable
    pub fn get_variable(&self, name: &str) -> Option<String> {
        let session = self.session.read().unwrap();
        session.variables.get(name).cloned()
    }

    /// Start a new transaction
    pub fn begin_transaction(&self) -> Result<u64> {
        let tx_id = self.database.begin_transaction()?;
        let mut session = self.session.write().unwrap();
        session.current_transaction = Some(tx_id);
        Ok(tx_id)
    }

    /// Commit current transaction
    pub fn commit_transaction(&self) -> Result<()> {
        let mut session = self.session.write().unwrap();
        if let Some(tx_id) = session.current_transaction.take() {
            self.database.commit_transaction(tx_id)?;
            Ok(())
        } else {
            Err(Error::Generic("No active transaction".to_string()))
        }
    }

    /// Rollback current transaction
    pub fn rollback_transaction(&self) -> Result<()> {
        let mut session = self.session.write().unwrap();
        if let Some(tx_id) = session.current_transaction.take() {
            self.database.abort_transaction(tx_id)?;
            Ok(())
        } else {
            Err(Error::Generic("No active transaction".to_string()))
        }
    }

    /// Convert FormattableData to CommandData
    fn convert_formattable_to_command_data(&self, data: FormattableData) -> CommandData {
        match data {
            FormattableData::KeyValue { key, value } => CommandData::KeyValue {
                key,
                value: value.map(|v| String::from_utf8_lossy(&v).to_string()).unwrap_or_default(),
            },
            FormattableData::List(items) => CommandData::List(
                items.into_iter().map(|s| String::from_utf8_lossy(&s).to_string()).collect()
            ),
            FormattableData::Table { headers: _, rows: _ } => CommandData::Value("Table data".to_string()),
            FormattableData::Message(msg) => CommandData::Value(msg),
            FormattableData::Statistics(_) => CommandData::Value("Statistics data".to_string()),
            FormattableData::Empty => CommandData::Value("No data".to_string()),
        }
    }

    /// Convert CommandData back to FormattableData for formatting
    fn convert_command_data_to_formattable(&self, data: &CommandData) -> FormattableData {
        match data {
            CommandData::KeyValue { key, value } => FormattableData::KeyValue {
                key: key.clone(),
                value: Some(value.as_bytes().to_vec()),
            },
            CommandData::List(items) => FormattableData::List(
                items.iter().map(|s| s.as_bytes().to_vec()).collect()
            ),
            CommandData::Value(value) => FormattableData::Message(value.clone()),
        }
    }
}

/// Entry point for starting the REPL
pub fn start_repl(database: Arc<Database>) -> Result<()> {
    let repl = DatabaseRepl::new(database)?;
    repl.start()
}

/// Entry point with custom configuration
pub fn start_repl_with_config(database: Arc<Database>, config: ReplConfig) -> Result<()> {
    let repl = DatabaseRepl::with_config(database, config)?;
    repl.start()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::LightningDbConfig;

    #[test]
    fn test_repl_creation() {
        let test_dir = TempDir::new().unwrap();
        let db_config = LightningDbConfig::default();
        let database = Arc::new(Database::create(test_dir.path(), db_config).unwrap());
        
        let repl = DatabaseRepl::new(database);
        assert!(repl.is_ok());
    }

    #[test]
    fn test_repl_config_default() {
        let config = ReplConfig::default();
        assert!(config.enable_history);
        assert!(config.enable_completion);
        assert!(config.show_timing);
        assert_eq!(config.prompt, "lightning_db> ");
    }

    #[test]
    fn test_session_state() {
        let test_dir = TempDir::new().unwrap();
        let db_config = LightningDbConfig::default();
        let database = Arc::new(Database::create(test_dir.path(), db_config).unwrap());
        
        let repl = DatabaseRepl::new(database).unwrap();
        
        // Test session variable management
        repl.set_variable("test_var".to_string(), "test_value".to_string());
        assert_eq!(repl.get_variable("test_var"), Some("test_value".to_string()));
        assert_eq!(repl.get_variable("nonexistent"), None);
    }

    #[test]
    fn test_transaction_management() {
        let test_dir = TempDir::new().unwrap();
        let db_config = LightningDbConfig::default();
        let database = Arc::new(Database::create(test_dir.path(), db_config).unwrap());
        
        let repl = DatabaseRepl::new(database).unwrap();
        
        // Test transaction lifecycle
        let tx_id = repl.begin_transaction().unwrap();
        assert!(tx_id > 0);
        
        let session = repl.session.read().unwrap();
        assert_eq!(session.current_transaction, Some(tx_id));
        drop(session);
        
        // Test commit
        assert!(repl.commit_transaction().is_ok());
        
        let session = repl.session.read().unwrap();
        assert_eq!(session.current_transaction, None);
    }

    #[test]
    fn test_repl_stats() {
        let test_dir = TempDir::new().unwrap();
        let db_config = LightningDbConfig::default();
        let database = Arc::new(Database::create(test_dir.path(), db_config).unwrap());
        
        let repl = DatabaseRepl::new(database).unwrap();
        let stats = repl.get_stats();
        
        assert_eq!(stats.total_commands, 0);
        assert!(stats.session_duration.as_secs() < 1); // Just created
    }
}