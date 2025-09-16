pub mod isolation;
pub mod transaction_log;

pub type TransactionId = u64;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
    Recovering,
    Timeout,
}

pub use transaction_log::{LogEntry, LogType, TransactionLog};
