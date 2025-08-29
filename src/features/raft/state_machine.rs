use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use bytes::Bytes;
use crate::core::error::Error;
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;

#[async_trait]
pub trait StateMachine: Send + Sync {
    async fn apply(&self, command: Vec<u8>) -> CommandResult;
    async fn create_snapshot(&self) -> Result<Vec<u8>, Error>;
    async fn restore_snapshot(&self, snapshot: Vec<u8>) -> Result<(), Error>;
    async fn query(&self, query: Query) -> Result<QueryResult, Error>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    pub id: u64,
    pub operation: Operation,
    pub timestamp: u64,
}

impl Command {
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        bincode::encode_to_vec(self)
            .map_err(|e| Error::Serialization(e.to_string()))
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        bincode::deserialize(data)
            .map_err(|e| Error::Serialization(e.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
    CompareAndSwap { key: String, expected: Option<Vec<u8>>, new: Vec<u8> },
    Batch { operations: Vec<Operation> },
    Transaction { operations: Vec<Operation>, conditions: Vec<Condition> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Condition {
    KeyExists(String),
    KeyNotExists(String),
    ValueEquals(String, Vec<u8>),
    ValueGreaterThan(String, Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResult {
    pub success: bool,
    pub value: Option<Vec<u8>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub operation: QueryOperation,
    pub consistency: ConsistencyLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryOperation {
    Get { key: String },
    Range { start: String, end: String, limit: Option<usize> },
    Prefix { prefix: String, limit: Option<usize> },
    Count { pattern: Option<String> },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    Eventual,
    BoundedStaleness(u64),
    Strong,
    Linearizable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub values: Vec<(String, Vec<u8>)>,
    pub count: Option<usize>,
    pub read_timestamp: u64,
}

pub struct KeyValueStateMachine {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    last_applied_timestamp: Arc<RwLock<u64>>,
    transaction_log: Arc<RwLock<Vec<AppliedTransaction>>>,
}

#[derive(Debug, Clone)]
struct AppliedTransaction {
    id: u64,
    timestamp: u64,
    operations: Vec<Operation>,
    result: CommandResult,
}

impl KeyValueStateMachine {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            last_applied_timestamp: Arc::new(RwLock::new(0)),
            transaction_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn apply_operation(&self, op: &Operation, data: &mut HashMap<String, Vec<u8>>) -> CommandResult {
        match op {
            Operation::Put { key, value } => {
                data.insert(key.clone(), value.clone());
                CommandResult {
                    success: true,
                    value: None,
                    error: None,
                }
            },
            Operation::Delete { key } => {
                let old_value = data.remove(key);
                CommandResult {
                    success: true,
                    value: old_value,
                    error: None,
                }
            },
            Operation::CompareAndSwap { key, expected, new } => {
                let current = data.get(key);
                
                let matches = match (current, expected) {
                    (None, None) => true,
                    (Some(c), Some(e)) => c == e,
                    _ => false,
                };
                
                if matches {
                    data.insert(key.clone(), new.clone());
                    CommandResult {
                        success: true,
                        value: None,
                        error: None,
                    }
                } else {
                    CommandResult {
                        success: false,
                        value: current.cloned(),
                        error: Some("Compare and swap failed".to_string()),
                    }
                }
            },
            Operation::Batch { operations } => {
                for op in operations {
                    self.apply_operation(op, data);
                }
                CommandResult {
                    success: true,
                    value: None,
                    error: None,
                }
            },
            Operation::Transaction { operations, conditions } => {
                for condition in conditions {
                    if !self.check_condition(condition, data) {
                        return CommandResult {
                            success: false,
                            value: None,
                            error: Some("Transaction condition failed".to_string()),
                        };
                    }
                }
                
                for op in operations {
                    self.apply_operation(op, data);
                }
                
                CommandResult {
                    success: true,
                    value: None,
                    error: None,
                }
            },
        }
    }

    fn check_condition(&self, condition: &Condition, data: &HashMap<String, Vec<u8>>) -> bool {
        match condition {
            Condition::KeyExists(key) => data.contains_key(key),
            Condition::KeyNotExists(key) => !data.contains_key(key),
            Condition::ValueEquals(key, expected) => {
                data.get(key).map(|v| v == expected).unwrap_or(false)
            },
            Condition::ValueGreaterThan(key, threshold) => {
                data.get(key).map(|v| v > threshold).unwrap_or(false)
            },
        }
    }
}

#[async_trait]
impl StateMachine for KeyValueStateMachine {
    async fn apply(&self, command_data: Vec<u8>) -> CommandResult {
        let command = match Command::deserialize(&command_data) {
            Ok(cmd) => cmd,
            Err(e) => {
                return CommandResult {
                    success: false,
                    value: None,
                    error: Some(e.to_string()),
                };
            }
        };
        
        let mut data = self.data.write();
        let result = self.apply_operation(&command.operation, &mut data);
        
        *self.last_applied_timestamp.write() = command.timestamp;
        
        self.transaction_log.write().push(AppliedTransaction {
            id: command.id,
            timestamp: command.timestamp,
            operations: vec![command.operation],
            result: result.clone(),
        });
        
        result
    }

    async fn create_snapshot(&self) -> Result<Vec<u8>, Error> {
        let data = self.data.read();
        let snapshot = SnapshotData {
            data: data.clone(),
            last_timestamp: *self.last_applied_timestamp.read(),
        };
        
        bincode::encode_to_vec(&snapshot)
            .map_err(|e| Error::Serialization(e.to_string()))
    }

    async fn restore_snapshot(&self, snapshot_data: Vec<u8>) -> Result<(), Error> {
        let snapshot: SnapshotData = bincode::deserialize(&snapshot_data)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        
        *self.data.write() = snapshot.data;
        *self.last_applied_timestamp.write() = snapshot.last_timestamp;
        self.transaction_log.write().clear();
        
        Ok(())
    }

    async fn query(&self, query: Query) -> Result<QueryResult, Error> {
        let data = self.data.read();
        let timestamp = *self.last_applied_timestamp.read();
        
        let result = match query.operation {
            QueryOperation::Get { key } => {
                let values = if let Some(value) = data.get(&key) {
                    vec![(key, value.clone())]
                } else {
                    vec![]
                };
                
                QueryResult {
                    values,
                    count: None,
                    read_timestamp: timestamp,
                }
            },
            QueryOperation::Range { start, end, limit } => {
                let mut values = Vec::new();
                let mut count = 0;
                
                for (key, value) in data.iter() {
                    if key >= &start && key <= &end {
                        values.push((key.clone(), value.clone()));
                        count += 1;
                        
                        if let Some(l) = limit {
                            if count >= l {
                                break;
                            }
                        }
                    }
                }
                
                values.sort_by(|a, b| a.0.cmp(&b.0));
                
                QueryResult {
                    values,
                    count: None,
                    read_timestamp: timestamp,
                }
            },
            QueryOperation::Prefix { prefix, limit } => {
                let mut values = Vec::new();
                let mut count = 0;
                
                for (key, value) in data.iter() {
                    if key.starts_with(&prefix) {
                        values.push((key.clone(), value.clone()));
                        count += 1;
                        
                        if let Some(l) = limit {
                            if count >= l {
                                break;
                            }
                        }
                    }
                }
                
                values.sort_by(|a, b| a.0.cmp(&b.0));
                
                QueryResult {
                    values,
                    count: None,
                    read_timestamp: timestamp,
                }
            },
            QueryOperation::Count { pattern } => {
                let count = if let Some(pat) = pattern {
                    data.keys().filter(|k| k.contains(&pat)).count()
                } else {
                    data.len()
                };
                
                QueryResult {
                    values: vec![],
                    count: Some(count),
                    read_timestamp: timestamp,
                }
            },
        };
        
        Ok(result)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotData {
    data: HashMap<String, Vec<u8>>,
    last_timestamp: u64,
}

pub struct SQLStateMachine {
    // SQL-based state machine implementation
    // Would integrate with the main database engine
}

#[async_trait]
impl StateMachine for SQLStateMachine {
    async fn apply(&self, command: Vec<u8>) -> CommandResult {
        // Apply SQL commands
        CommandResult {
            success: true,
            value: None,
            error: None,
        }
    }

    async fn create_snapshot(&self) -> Result<Vec<u8>, Error> {
        // Create database snapshot
        Ok(Vec::new())
    }

    async fn restore_snapshot(&self, snapshot: Vec<u8>) -> Result<(), Error> {
        // Restore database from snapshot
        Ok(())
    }

    async fn query(&self, query: Query) -> Result<QueryResult, Error> {
        // Execute SQL query
        Ok(QueryResult {
            values: vec![],
            count: None,
            read_timestamp: 0,
        })
    }
}