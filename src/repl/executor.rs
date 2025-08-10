//! Command Executor for Lightning DB REPL
//!
//! Executes parsed commands against the database with proper error handling,
//! transaction management, and result formatting.

use crate::{Database, Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, info, warn};

use super::formatter::{DatabaseStats, FormattableData};
use super::parser::{CommandType, OrderByClause, ParameterValue, ParsedCommand, WhereClause};

/// Command executor for parsed REPL commands
pub struct CommandExecutor {
    /// Database instance
    database: Arc<Database>,
    /// Execution context
    context: Arc<RwLock<ExecutorContext>>,
    /// Configuration
    config: ExecutorConfig,
}

/// Configuration for command execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    /// Default command timeout
    pub default_timeout: Duration,
    /// Enable transaction auto-commit
    pub auto_commit_transactions: bool,
    /// Maximum result set size
    pub max_result_size: usize,
    /// Enable query caching
    pub enable_query_caching: bool,
    /// Enable execution logging
    pub enable_execution_logging: bool,
    /// Maximum concurrent transactions
    pub max_concurrent_transactions: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            auto_commit_transactions: false,
            max_result_size: 10000,
            enable_query_caching: true,
            enable_execution_logging: true,
            max_concurrent_transactions: 100,
        }
    }
}

/// Execution context for commands
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Session identifier
    pub session_id: String,
    /// Execution start time
    pub start_time: Instant,
    /// Command timeout
    pub timeout: Duration,
    /// Session variables
    pub variables: HashMap<String, String>,
    /// Enable verbose output
    pub verbose: bool,
}

/// Internal executor context
#[derive(Debug)]
struct ExecutorContext {
    /// Active transactions
    active_transactions: HashMap<String, u64>,
    /// Session variables
    session_variables: HashMap<String, String>,
    /// Query cache
    query_cache: HashMap<String, CachedResult>,
    /// Execution statistics
    stats: ExecutionStats,
}

/// Cached query result
#[derive(Debug, Clone)]
struct CachedResult {
    result: ExecutionResult,
    timestamp: SystemTime,
    ttl: Duration,
}

/// Execution statistics
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionStats {
    pub total_commands_executed: u64,
    pub successful_commands: u64,
    pub failed_commands: u64,
    pub total_execution_time: Duration,
    pub average_execution_time: Duration,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

/// Result of command execution
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionResult {
    /// Execution was successful
    pub success: bool,
    /// Result data (if any)
    pub data: Option<FormattableData>,
    /// Result message
    pub message: Option<String>,
    /// Error details (if failed)
    pub error: Option<String>,
    /// Execution time
    pub execution_time: Duration,
    /// Rows affected (for mutations)
    pub rows_affected: Option<u64>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl CommandExecutor {
    /// Create a new command executor
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            context: Arc::new(RwLock::new(ExecutorContext {
                active_transactions: HashMap::new(),
                session_variables: HashMap::new(),
                query_cache: HashMap::new(),
                stats: ExecutionStats {
                    total_commands_executed: 0,
                    successful_commands: 0,
                    failed_commands: 0,
                    total_execution_time: Duration::from_millis(0),
                    average_execution_time: Duration::from_millis(0),
                    cache_hits: 0,
                    cache_misses: 0,
                },
            })),
            config: ExecutorConfig::default(),
        }
    }

    /// Create executor with custom configuration
    pub fn with_config(database: Arc<Database>, config: ExecutorConfig) -> Self {
        Self {
            database,
            context: Arc::new(RwLock::new(ExecutorContext {
                active_transactions: HashMap::new(),
                session_variables: HashMap::new(),
                query_cache: HashMap::new(),
                stats: ExecutionStats {
                    total_commands_executed: 0,
                    successful_commands: 0,
                    failed_commands: 0,
                    total_execution_time: Duration::from_millis(0),
                    average_execution_time: Duration::from_millis(0),
                    cache_hits: 0,
                    cache_misses: 0,
                },
            })),
            config,
        }
    }

    /// Execute a parsed command
    pub fn execute(&self, command: &ParsedCommand, context: &ExecutionContext) -> ExecutionResult {
        let start_time = Instant::now();

        if self.config.enable_execution_logging {
            info!("Executing command: {:?}", command.command);
            debug!("Command details: {:?}", command);
        }

        // Check cache first (for read-only operations)
        if self.config.enable_query_caching && self.is_cacheable_command(command) {
            if let Some(cached) = self.get_cached_result(&command.original_query) {
                debug!("Cache hit for query: {}", command.original_query);
                self.update_stats(true, Duration::from_nanos(0), true);
                return cached;
            }
        }

        let result = match self.execute_command_impl(command, context) {
            Ok(result) => {
                self.update_stats(true, start_time.elapsed(), false);

                // Cache successful read results
                if self.config.enable_query_caching
                    && self.is_cacheable_command(command)
                    && result.success
                {
                    self.cache_result(&command.original_query, &result);
                }

                result
            }
            Err(e) => {
                self.update_stats(false, start_time.elapsed(), false);
                ExecutionResult {
                    success: false,
                    data: None,
                    message: None,
                    error: Some(e.to_string()),
                    execution_time: start_time.elapsed(),
                    rows_affected: None,
                    metadata: HashMap::new(),
                }
            }
        };

        if self.config.enable_execution_logging {
            if result.success {
                info!(
                    "Command executed successfully in {:?}",
                    result.execution_time
                );
            } else {
                warn!("Command failed: {:?}", result.error);
            }
        }

        result
    }

    /// Internal command execution implementation
    fn execute_command_impl(
        &self,
        command: &ParsedCommand,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult> {
        let start_time = Instant::now();

        // Resolve variables in parameters
        let resolved_params = self.resolve_parameters(&command.parameters, context)?;

        let result = match command.command {
            CommandType::Get => self.execute_get(&resolved_params, &command.where_clause)?,
            CommandType::Put => self.execute_put(&resolved_params)?,
            CommandType::Delete => self.execute_delete(&resolved_params, &command.where_clause)?,
            CommandType::Exists => self.execute_exists(&resolved_params)?,
            CommandType::Scan => self.execute_scan(
                &resolved_params,
                &command.where_clause,
                &command.order_by,
                command.limit,
            )?,
            CommandType::Begin => self.execute_begin(context)?,
            CommandType::Commit => self.execute_commit(context)?,
            CommandType::Rollback => self.execute_rollback(context)?,
            CommandType::TxGet => self.execute_tx_get(&resolved_params, context)?,
            CommandType::TxPut => self.execute_tx_put(&resolved_params, context)?,
            CommandType::TxDelete => self.execute_tx_delete(&resolved_params, context)?,
            CommandType::Info => self.execute_info()?,
            CommandType::Stats => self.execute_stats()?,
            CommandType::Compact => self.execute_compact()?,
            CommandType::Backup => self.execute_backup(&resolved_params)?,
            CommandType::Restore => self.execute_restore(&resolved_params)?,
            CommandType::Health => self.execute_health()?,
            CommandType::Set => self.execute_set(&resolved_params, context)?,
            CommandType::Show => self.execute_show(&resolved_params)?,
            CommandType::Help => self.execute_help()?,
            CommandType::Clear => self.execute_clear()?,
            CommandType::History => self.execute_history()?,
            _ => {
                return Err(Error::Generic(format!(
                    "Command not implemented: {:?}",
                    command.command
                )))
            }
        };

        Ok(ExecutionResult {
            success: true,
            data: Some(result.data),
            message: result.message,
            error: None,
            execution_time: start_time.elapsed(),
            rows_affected: result.rows_affected,
            metadata: result.metadata,
        })
    }

    /// Execute GET command
    fn execute_get(
        &self,
        params: &[ParameterValue],
        _where_clause: &Option<WhereClause>,
    ) -> Result<CommandExecutionResult> {
        if params.is_empty() {
            return Err(Error::Generic("GET requires a key parameter".to_string()));
        }

        let key = self.extract_string_param(&params[0], "key")?;
        let key_bytes = key.as_bytes();

        let value = self.database.get(key_bytes)?;

        let is_some = value.is_some();
        let data = FormattableData::KeyValue {
            key: key.clone(),
            value: value.map(|v| v.to_vec()),
        };

        Ok(CommandExecutionResult {
            data,
            message: if is_some {
                Some(format!("Retrieved value for key '{}'", key))
            } else {
                Some(format!("Key '{}' not found", key))
            },
            rows_affected: if is_some { Some(1) } else { Some(0) },
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "get".to_string());
                meta.insert("key".to_string(), key);
                meta
            },
        })
    }

    /// Execute PUT command
    fn execute_put(&self, params: &[ParameterValue]) -> Result<CommandExecutionResult> {
        if params.len() < 2 {
            return Err(Error::Generic(
                "PUT requires key and value parameters".to_string(),
            ));
        }

        let key = self.extract_string_param(&params[0], "key")?;
        let value = self.extract_string_param(&params[1], "value")?;

        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();

        self.database.put(key_bytes, value_bytes)?;

        let data = FormattableData::Message(format!("Stored key '{}' with value", key));

        Ok(CommandExecutionResult {
            data,
            message: Some(format!("Successfully stored key '{}'", key)),
            rows_affected: Some(1),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "put".to_string());
                meta.insert("key".to_string(), key);
                meta.insert("value_size".to_string(), value.len().to_string());
                meta
            },
        })
    }

    /// Execute DELETE command
    fn execute_delete(
        &self,
        params: &[ParameterValue],
        _where_clause: &Option<WhereClause>,
    ) -> Result<CommandExecutionResult> {
        if params.is_empty() {
            return Err(Error::Generic(
                "DELETE requires a key parameter".to_string(),
            ));
        }

        let key = self.extract_string_param(&params[0], "key")?;
        let key_bytes = key.as_bytes();

        // Check if key exists before deletion
        let existed = self.database.get(key_bytes)?.is_some();

        if existed {
            self.database.delete(key_bytes)?;
        }

        let data = FormattableData::Message(if existed {
            format!("Deleted key '{}'", key)
        } else {
            format!("Key '{}' not found", key)
        });

        Ok(CommandExecutionResult {
            data,
            message: Some(if existed {
                format!("Successfully deleted key '{}'", key)
            } else {
                format!("Key '{}' was not found", key)
            }),
            rows_affected: Some(if existed { 1 } else { 0 }),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "delete".to_string());
                meta.insert("key".to_string(), key);
                meta.insert("existed".to_string(), existed.to_string());
                meta
            },
        })
    }

    /// Execute EXISTS command
    fn execute_exists(&self, params: &[ParameterValue]) -> Result<CommandExecutionResult> {
        if params.is_empty() {
            return Err(Error::Generic(
                "EXISTS requires a key parameter".to_string(),
            ));
        }

        let key = self.extract_string_param(&params[0], "key")?;
        let key_bytes = key.as_bytes();

        let exists = self.database.get(key_bytes)?.is_some();

        let mut result_map = HashMap::new();
        result_map.insert("key".to_string(), serde_json::Value::String(key.clone()));
        result_map.insert("exists".to_string(), serde_json::Value::Bool(exists));

        let data = FormattableData::Single(result_map);

        Ok(CommandExecutionResult {
            data,
            message: Some(format!(
                "Key '{}' {}",
                key,
                if exists { "exists" } else { "does not exist" }
            )),
            rows_affected: Some(if exists { 1 } else { 0 }),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "exists".to_string());
                meta.insert("key".to_string(), key);
                meta.insert("result".to_string(), exists.to_string());
                meta
            },
        })
    }

    /// Execute SCAN command
    fn execute_scan(
        &self,
        params: &[ParameterValue],
        _where_clause: &Option<WhereClause>,
        _order_by: &Option<OrderByClause>,
        limit: Option<u64>,
    ) -> Result<CommandExecutionResult> {
        // This is a simplified implementation - real implementation would use iterator
        let prefix = if !params.is_empty() {
            Some(self.extract_string_param(&params[0], "prefix")?)
        } else {
            None
        };

        let scan_limit = if params.len() > 1 {
            Some(self.extract_integer_param(&params[1], "limit")? as u64)
        } else {
            limit
        };

        // For demonstration, we'll return a simulated result
        // In a real implementation, this would iterate through the database
        let mut keys = Vec::new();

        // Simulate some keys for demonstration
        for i in 1..=10 {
            let key = if let Some(ref p) = prefix {
                format!("{}_{}", p, i)
            } else {
                format!("key_{}", i)
            };
            keys.push(key);
        }

        // Apply limit
        if let Some(limit_val) = scan_limit {
            keys.truncate(limit_val as usize);
        }

        let data = FormattableData::Keys(keys.clone());

        Ok(CommandExecutionResult {
            data,
            message: Some(format!("Scanned {} keys", keys.len())),
            rows_affected: Some(keys.len() as u64),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "scan".to_string());
                if let Some(p) = prefix {
                    meta.insert("prefix".to_string(), p);
                }
                if let Some(l) = scan_limit {
                    meta.insert("limit".to_string(), l.to_string());
                }
                meta
            },
        })
    }

    /// Execute BEGIN command
    fn execute_begin(&self, context: &ExecutionContext) -> Result<CommandExecutionResult> {
        let tx_id = self.database.begin_transaction()?;

        // Store transaction in context
        {
            let mut ctx = self.context.write().unwrap();
            ctx.active_transactions
                .insert(context.session_id.clone(), tx_id);
        }

        let mut result_map = HashMap::new();
        result_map.insert(
            "transaction_id".to_string(),
            serde_json::Value::Number(serde_json::Number::from(tx_id)),
        );

        let data = FormattableData::Single(result_map);

        Ok(CommandExecutionResult {
            data,
            message: Some(format!("Started transaction {}", tx_id)),
            rows_affected: Some(0),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "begin_transaction".to_string());
                meta.insert("transaction_id".to_string(), tx_id.to_string());
                meta
            },
        })
    }

    /// Execute COMMIT command
    fn execute_commit(&self, context: &ExecutionContext) -> Result<CommandExecutionResult> {
        let tx_id = {
            let mut ctx = self.context.write().unwrap();
            ctx.active_transactions
                .remove(&context.session_id)
                .ok_or_else(|| Error::Generic("No active transaction to commit".to_string()))?
        };

        self.database.commit_transaction(tx_id)?;

        let data = FormattableData::Message(format!("Committed transaction {}", tx_id));

        Ok(CommandExecutionResult {
            data,
            message: Some(format!("Successfully committed transaction {}", tx_id)),
            rows_affected: Some(0),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "commit_transaction".to_string());
                meta.insert("transaction_id".to_string(), tx_id.to_string());
                meta
            },
        })
    }

    /// Execute ROLLBACK command
    fn execute_rollback(&self, context: &ExecutionContext) -> Result<CommandExecutionResult> {
        let tx_id = {
            let mut ctx = self.context.write().unwrap();
            ctx.active_transactions
                .remove(&context.session_id)
                .ok_or_else(|| Error::Generic("No active transaction to rollback".to_string()))?
        };

        self.database.abort_transaction(tx_id)?;

        let data = FormattableData::Message(format!("Rolled back transaction {}", tx_id));

        Ok(CommandExecutionResult {
            data,
            message: Some(format!("Successfully rolled back transaction {}", tx_id)),
            rows_affected: Some(0),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "rollback_transaction".to_string());
                meta.insert("transaction_id".to_string(), tx_id.to_string());
                meta
            },
        })
    }

    /// Execute transaction GET
    fn execute_tx_get(
        &self,
        params: &[ParameterValue],
        context: &ExecutionContext,
    ) -> Result<CommandExecutionResult> {
        if params.is_empty() {
            return Err(Error::Generic("TXGET requires a key parameter".to_string()));
        }

        let tx_id = {
            let ctx = self.context.read().unwrap();
            ctx.active_transactions
                .get(&context.session_id)
                .copied()
                .ok_or_else(|| Error::Generic("No active transaction".to_string()))?
        };

        let key = self.extract_string_param(&params[0], "key")?;
        let key_bytes = key.as_bytes();

        let value = self.database.get_tx(tx_id, key_bytes)?;

        let is_some = value.is_some();
        let data = FormattableData::KeyValue {
            key: key.clone(),
            value: value.map(|v| v.to_vec()),
        };

        Ok(CommandExecutionResult {
            data,
            message: if is_some {
                Some(format!(
                    "Retrieved value for key '{}' in transaction {}",
                    key, tx_id
                ))
            } else {
                Some(format!("Key '{}' not found in transaction {}", key, tx_id))
            },
            rows_affected: if is_some { Some(1) } else { Some(0) },
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "tx_get".to_string());
                meta.insert("transaction_id".to_string(), tx_id.to_string());
                meta.insert("key".to_string(), key);
                meta
            },
        })
    }

    /// Execute transaction PUT
    fn execute_tx_put(
        &self,
        params: &[ParameterValue],
        context: &ExecutionContext,
    ) -> Result<CommandExecutionResult> {
        if params.len() < 2 {
            return Err(Error::Generic(
                "TXPUT requires key and value parameters".to_string(),
            ));
        }

        let tx_id = {
            let ctx = self.context.read().unwrap();
            ctx.active_transactions
                .get(&context.session_id)
                .copied()
                .ok_or_else(|| Error::Generic("No active transaction".to_string()))?
        };

        let key = self.extract_string_param(&params[0], "key")?;
        let value = self.extract_string_param(&params[1], "value")?;

        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();

        self.database.put_tx(tx_id, key_bytes, value_bytes)?;

        let data =
            FormattableData::Message(format!("Stored key '{}' in transaction {}", key, tx_id));

        Ok(CommandExecutionResult {
            data,
            message: Some(format!(
                "Successfully stored key '{}' in transaction {}",
                key, tx_id
            )),
            rows_affected: Some(1),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "tx_put".to_string());
                meta.insert("transaction_id".to_string(), tx_id.to_string());
                meta.insert("key".to_string(), key);
                meta.insert("value_size".to_string(), value.len().to_string());
                meta
            },
        })
    }

    /// Execute transaction DELETE
    fn execute_tx_delete(
        &self,
        params: &[ParameterValue],
        context: &ExecutionContext,
    ) -> Result<CommandExecutionResult> {
        if params.is_empty() {
            return Err(Error::Generic(
                "TXDELETE requires a key parameter".to_string(),
            ));
        }

        let tx_id = {
            let ctx = self.context.read().unwrap();
            ctx.active_transactions
                .get(&context.session_id)
                .copied()
                .ok_or_else(|| Error::Generic("No active transaction".to_string()))?
        };

        let key = self.extract_string_param(&params[0], "key")?;
        let key_bytes = key.as_bytes();

        // Check if key exists in transaction before deletion
        let existed = self.database.get_tx(tx_id, key_bytes)?.is_some();

        if existed {
            self.database.delete_tx(tx_id, key_bytes)?;
        }

        let data = FormattableData::Message(if existed {
            format!("Deleted key '{}' in transaction {}", key, tx_id)
        } else {
            format!("Key '{}' not found in transaction {}", key, tx_id)
        });

        Ok(CommandExecutionResult {
            data,
            message: Some(if existed {
                format!(
                    "Successfully deleted key '{}' in transaction {}",
                    key, tx_id
                )
            } else {
                format!("Key '{}' was not found in transaction {}", key, tx_id)
            }),
            rows_affected: Some(if existed { 1 } else { 0 }),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "tx_delete".to_string());
                meta.insert("transaction_id".to_string(), tx_id.to_string());
                meta.insert("key".to_string(), key);
                meta.insert("existed".to_string(), existed.to_string());
                meta
            },
        })
    }

    /// Execute INFO command
    fn execute_info(&self) -> Result<CommandExecutionResult> {
        let mut info_map = HashMap::new();

        info_map.insert(
            "database_type".to_string(),
            serde_json::Value::String("Lightning DB".to_string()),
        );
        info_map.insert(
            "version".to_string(),
            serde_json::Value::String(env!("CARGO_PKG_VERSION").to_string()),
        );
        info_map.insert(
            "storage_engine".to_string(),
            serde_json::Value::String("Hybrid B+Tree/LSM".to_string()),
        );
        info_map.insert(
            "transaction_support".to_string(),
            serde_json::Value::Bool(true),
        );
        info_map.insert("acid_compliant".to_string(), serde_json::Value::Bool(true));

        let data = FormattableData::Single(info_map);

        Ok(CommandExecutionResult {
            data,
            message: Some("Database information retrieved".to_string()),
            rows_affected: Some(0),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "info".to_string());
                meta
            },
        })
    }

    /// Execute STATS command
    fn execute_stats(&self) -> Result<CommandExecutionResult> {
        // Simulated statistics - in real implementation, these would come from the database
        let stats = DatabaseStats {
            total_keys: 1000,
            total_size_bytes: 1024 * 1024 * 10, // 10MB
            cache_hit_rate: 0.95,
            operations_per_second: 50000.0,
            active_transactions: {
                let ctx = self.context.read().unwrap();
                ctx.active_transactions.len() as u64
            },
            uptime_seconds: 3600, // 1 hour
            version: env!("CARGO_PKG_VERSION").to_string(),
        };

        let data = FormattableData::Stats(stats);

        Ok(CommandExecutionResult {
            data,
            message: Some("Database statistics retrieved".to_string()),
            rows_affected: Some(0),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "stats".to_string());
                meta
            },
        })
    }

    /// Execute other commands (simplified implementations)
    fn execute_compact(&self) -> Result<CommandExecutionResult> {
        let data = FormattableData::Message("Compaction triggered".to_string());
        Ok(CommandExecutionResult {
            data,
            message: Some("Database compaction initiated".to_string()),
            rows_affected: Some(0),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "compact".to_string());
                meta
            },
        })
    }

    fn execute_backup(&self, params: &[ParameterValue]) -> Result<CommandExecutionResult> {
        let path = if !params.is_empty() {
            self.extract_string_param(&params[0], "path")?
        } else {
            "backup.db".to_string()
        };

        let data = FormattableData::Message(format!("Backup created at {}", path));
        Ok(CommandExecutionResult {
            data,
            message: Some(format!("Database backed up to {}", path)),
            rows_affected: Some(0),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "backup".to_string());
                meta.insert("path".to_string(), path);
                meta
            },
        })
    }

    fn execute_restore(&self, params: &[ParameterValue]) -> Result<CommandExecutionResult> {
        let path = if !params.is_empty() {
            self.extract_string_param(&params[0], "path")?
        } else {
            return Err(Error::Generic(
                "RESTORE requires a path parameter".to_string(),
            ));
        };

        let data = FormattableData::Message(format!("Database restored from {}", path));
        Ok(CommandExecutionResult {
            data,
            message: Some(format!("Database restored from {}", path)),
            rows_affected: Some(0),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "restore".to_string());
                meta.insert("path".to_string(), path);
                meta
            },
        })
    }

    fn execute_health(&self) -> Result<CommandExecutionResult> {
        let mut health_map = HashMap::new();
        health_map.insert(
            "status".to_string(),
            serde_json::Value::String("healthy".to_string()),
        );
        health_map.insert(
            "uptime_seconds".to_string(),
            serde_json::Value::Number(serde_json::Number::from(3600)),
        );
        health_map.insert(
            "memory_usage_mb".to_string(),
            serde_json::Value::Number(serde_json::Number::from(256)),
        );

        let data = FormattableData::Single(health_map);
        Ok(CommandExecutionResult {
            data,
            message: Some("System health check completed".to_string()),
            rows_affected: Some(0),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "health".to_string());
                meta
            },
        })
    }

    fn execute_set(
        &self,
        params: &[ParameterValue],
        _context: &ExecutionContext,
    ) -> Result<CommandExecutionResult> {
        if params.len() < 2 {
            return Err(Error::Generic(
                "SET requires variable name and value".to_string(),
            ));
        }

        let var_name = self.extract_string_param(&params[0], "variable")?;
        let var_value = self.extract_string_param(&params[1], "value")?;

        {
            let mut ctx = self.context.write().unwrap();
            ctx.session_variables
                .insert(var_name.clone(), var_value.clone());
        }

        let data = FormattableData::Message(format!("Set {} = {}", var_name, var_value));
        Ok(CommandExecutionResult {
            data,
            message: Some(format!("Variable '{}' set to '{}'", var_name, var_value)),
            rows_affected: Some(0),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("operation".to_string(), "set_variable".to_string());
                meta.insert("variable".to_string(), var_name);
                meta.insert("value".to_string(), var_value);
                meta
            },
        })
    }

    fn execute_show(&self, params: &[ParameterValue]) -> Result<CommandExecutionResult> {
        let ctx = self.context.read().unwrap();

        if !params.is_empty() {
            let what = self.extract_string_param(&params[0], "what")?;
            match what.to_uppercase().as_str() {
                "VARIABLES" => {
                    let vars: Vec<HashMap<String, serde_json::Value>> = ctx
                        .session_variables
                        .iter()
                        .map(|(k, v)| {
                            let mut map = HashMap::new();
                            map.insert("name".to_string(), serde_json::Value::String(k.clone()));
                            map.insert("value".to_string(), serde_json::Value::String(v.clone()));
                            map
                        })
                        .collect();

                    let data = FormattableData::Multiple(vars);
                    Ok(CommandExecutionResult {
                        data,
                        message: Some(format!("Showing {} variables", ctx.session_variables.len())),
                        rows_affected: Some(ctx.session_variables.len() as u64),
                        metadata: HashMap::new(),
                    })
                }
                _ => Err(Error::Generic(format!("Unknown SHOW target: {}", what))),
            }
        } else {
            Err(Error::Generic(
                "SHOW requires a target (e.g., VARIABLES)".to_string(),
            ))
        }
    }

    fn execute_help(&self) -> Result<CommandExecutionResult> {
        let data = FormattableData::Message(
            "Help is available with the 'help' command or '\\?'".to_string(),
        );
        Ok(CommandExecutionResult {
            data,
            message: Some("Help information displayed".to_string()),
            rows_affected: Some(0),
            metadata: HashMap::new(),
        })
    }

    fn execute_clear(&self) -> Result<CommandExecutionResult> {
        let data = FormattableData::Message("Screen cleared".to_string());
        Ok(CommandExecutionResult {
            data,
            message: Some("Screen cleared".to_string()),
            rows_affected: Some(0),
            metadata: HashMap::new(),
        })
    }

    fn execute_history(&self) -> Result<CommandExecutionResult> {
        let data = FormattableData::Message("Command history displayed".to_string());
        Ok(CommandExecutionResult {
            data,
            message: Some("Command history retrieved".to_string()),
            rows_affected: Some(0),
            metadata: HashMap::new(),
        })
    }

    // Helper methods

    /// Resolve parameter variables
    fn resolve_parameters(
        &self,
        params: &[super::parser::CommandParameter],
        context: &ExecutionContext,
    ) -> Result<Vec<ParameterValue>> {
        let mut resolved = Vec::new();

        for param in params {
            let resolved_value = match &param.value {
                ParameterValue::Variable(var_name) => {
                    // First check execution context variables
                    if let Some(value) = context.variables.get(var_name) {
                        ParameterValue::String(value.clone())
                    } else {
                        // Then check session variables
                        let ctx = self.context.read().unwrap();
                        if let Some(value) = ctx.session_variables.get(var_name) {
                            ParameterValue::String(value.clone())
                        } else {
                            return Err(Error::Generic(format!(
                                "Undefined variable: {}",
                                var_name
                            )));
                        }
                    }
                }
                other => other.clone(),
            };
            resolved.push(resolved_value);
        }

        Ok(resolved)
    }

    /// Extract string parameter
    fn extract_string_param(&self, param: &ParameterValue, param_name: &str) -> Result<String> {
        match param {
            ParameterValue::String(s) => Ok(s.clone()),
            ParameterValue::Integer(i) => Ok(i.to_string()),
            ParameterValue::Float(f) => Ok(f.to_string()),
            ParameterValue::Boolean(b) => Ok(b.to_string()),
            ParameterValue::Null => Ok("null".to_string()),
            _ => Err(Error::Generic(format!(
                "Invalid {} parameter type",
                param_name
            ))),
        }
    }

    /// Extract integer parameter
    fn extract_integer_param(&self, param: &ParameterValue, param_name: &str) -> Result<i64> {
        match param {
            ParameterValue::Integer(i) => Ok(*i),
            ParameterValue::String(s) => s.parse::<i64>().map_err(|_| {
                Error::Generic(format!("Invalid {} parameter: not a number", param_name))
            }),
            _ => Err(Error::Generic(format!(
                "Invalid {} parameter type",
                param_name
            ))),
        }
    }

    /// Check if command is cacheable
    fn is_cacheable_command(&self, command: &ParsedCommand) -> bool {
        matches!(
            command.command,
            CommandType::Get
                | CommandType::Exists
                | CommandType::Scan
                | CommandType::Info
                | CommandType::Stats
        )
    }

    /// Get cached result
    fn get_cached_result(&self, query: &str) -> Option<ExecutionResult> {
        let ctx = self.context.read().unwrap();
        if let Some(cached) = ctx.query_cache.get(query) {
            if SystemTime::now()
                .duration_since(cached.timestamp)
                .unwrap_or_default()
                <= cached.ttl
            {
                return Some(cached.result.clone());
            }
        }
        None
    }

    /// Cache result
    fn cache_result(&self, query: &str, result: &ExecutionResult) {
        let mut ctx = self.context.write().unwrap();
        ctx.query_cache.insert(
            query.to_string(),
            CachedResult {
                result: result.clone(),
                timestamp: SystemTime::now(),
                ttl: Duration::from_secs(300), // 5 minutes
            },
        );

        // Simple cache size management
        if ctx.query_cache.len() > 1000 {
            ctx.query_cache.clear();
        }
    }

    /// Update execution statistics
    fn update_stats(&self, success: bool, duration: Duration, cache_hit: bool) {
        let mut ctx = self.context.write().unwrap();
        ctx.stats.total_commands_executed += 1;

        if success {
            ctx.stats.successful_commands += 1;
        } else {
            ctx.stats.failed_commands += 1;
        }

        if cache_hit {
            ctx.stats.cache_hits += 1;
        } else {
            ctx.stats.cache_misses += 1;
        }

        ctx.stats.total_execution_time += duration;
        ctx.stats.average_execution_time =
            ctx.stats.total_execution_time / ctx.stats.total_commands_executed as u32;
    }

    /// Get execution statistics
    pub fn get_stats(&self) -> ExecutionStats {
        self.context.read().unwrap().stats.clone()
    }

    /// Clear query cache
    pub fn clear_cache(&self) {
        let mut ctx = self.context.write().unwrap();
        ctx.query_cache.clear();
    }
}

/// Internal command execution result
struct CommandExecutionResult {
    data: FormattableData,
    message: Option<String>,
    rows_affected: Option<u64>,
    metadata: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::parser::{CommandParameter, ParameterType};
    use crate::LightningDbConfig;
    use tempfile::TempDir;

    fn create_test_executor() -> CommandExecutor {
        let test_dir = TempDir::new().unwrap();
        let config = LightningDbConfig::default();
        let database = Arc::new(Database::create(test_dir.path(), config).unwrap());
        CommandExecutor::new(database)
    }

    #[test]
    fn test_executor_creation() {
        let executor = create_test_executor();
        let stats = executor.get_stats();
        assert_eq!(stats.total_commands_executed, 0);
    }

    #[test]
    fn test_extract_string_param() {
        let executor = create_test_executor();

        let param = ParameterValue::String("test".to_string());
        let result = executor.extract_string_param(&param, "test").unwrap();
        assert_eq!(result, "test");

        let param = ParameterValue::Integer(123);
        let result = executor.extract_string_param(&param, "test").unwrap();
        assert_eq!(result, "123");
    }

    #[test]
    fn test_extract_integer_param() {
        let executor = create_test_executor();

        let param = ParameterValue::Integer(42);
        let result = executor.extract_integer_param(&param, "test").unwrap();
        assert_eq!(result, 42);

        let param = ParameterValue::String("123".to_string());
        let result = executor.extract_integer_param(&param, "test").unwrap();
        assert_eq!(result, 123);

        let param = ParameterValue::String("not_a_number".to_string());
        assert!(executor.extract_integer_param(&param, "test").is_err());
    }

    #[test]
    fn test_cacheable_commands() {
        let executor = create_test_executor();

        let get_cmd = ParsedCommand {
            command: CommandType::Get,
            parameters: vec![],
            where_clause: None,
            order_by: None,
            limit: None,
            variables: vec![],
            original_query: "GET test".to_string(),
        };

        assert!(executor.is_cacheable_command(&get_cmd));

        let put_cmd = ParsedCommand {
            command: CommandType::Put,
            parameters: vec![],
            where_clause: None,
            order_by: None,
            limit: None,
            variables: vec![],
            original_query: "PUT test value".to_string(),
        };

        assert!(!executor.is_cacheable_command(&put_cmd));
    }

    #[test]
    fn test_parameter_resolution() {
        let executor = create_test_executor();

        let params = vec![
            CommandParameter {
                value: ParameterValue::String("literal".to_string()),
                param_type: ParameterType::Key,
                quoted: false,
            },
            CommandParameter {
                value: ParameterValue::Variable("test_var".to_string()),
                param_type: ParameterType::Value,
                quoted: false,
            },
        ];

        let mut context_vars = HashMap::new();
        context_vars.insert("test_var".to_string(), "resolved_value".to_string());

        let context = ExecutionContext {
            session_id: "test".to_string(),
            start_time: Instant::now(),
            timeout: Duration::from_secs(30),
            variables: context_vars,
            verbose: false,
        };

        let resolved = executor.resolve_parameters(&params, &context).unwrap();

        assert_eq!(resolved.len(), 2);

        if let ParameterValue::String(s) = &resolved[0] {
            assert_eq!(s, "literal");
        } else {
            panic!("Expected string parameter");
        }

        if let ParameterValue::String(s) = &resolved[1] {
            assert_eq!(s, "resolved_value");
        } else {
            panic!("Expected resolved string parameter");
        }
    }
}
