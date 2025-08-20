// Recovery system exports for easy access
pub use crate::core::recovery::{
    ComprehensiveRecoveryManager, RecoveryConfiguration, ComprehensiveHealthReport,
    OverallHealthStatus, IoRecoveryManager, IoRecoveryConfig, MemoryRecoveryManager,
    MemoryRecoveryConfig, TransactionRecoveryManager, TransactionRecoveryConfig,
    CorruptionRecoveryManager, CorruptionRecoveryConfig, RecoveryManager,
    DiskHealthReport, DiskHealthStatus, MemoryHealthReport, MemoryHealthStatus,
    TransactionHealthReport, TransactionHealthStatus, CorruptionHealthReport,
    CorruptionHealthStatus, RepairResult, CorruptionReport, CorruptionType,
    CorruptionSeverity, RepairStrategy, ValidationLevel,
};

// Re-export for convenience
pub type ProductionRecoveryManager = ComprehensiveRecoveryManager;
pub type RecoveryConfig = RecoveryConfiguration;
pub type HealthReport = ComprehensiveHealthReport;