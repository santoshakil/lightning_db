pub mod deadlock;
pub mod isolation;
pub mod optimistic_cc;
pub mod participant;
pub mod recovery;
pub mod saga;
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

pub use deadlock::{DeadlockDetector, DeadlockVictim, WaitForGraph};
pub use optimistic_cc::{ConflictResolver, OptimisticController, ValidationResult};
pub use participant::{Participant, ParticipantState, VoteDecision};
pub use recovery::{CheckpointManager, RecoveryManager, RecoveryStrategy};
pub use saga::{CompensationAction, SagaCoordinator, SagaStep};
pub use transaction_log::{LogEntry, LogType, TransactionLog};
