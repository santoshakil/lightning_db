pub mod operations;
pub mod transactions;
pub mod indexing;
pub mod maintenance;
pub mod metrics;
pub mod lifecycle;

// Re-export types defined in metrics module
pub use metrics::{TransactionStats, PerformanceMetrics};