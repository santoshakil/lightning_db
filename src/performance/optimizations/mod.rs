pub mod critical_path;
pub mod transaction_batching; 
pub mod memory_layout;
pub mod simd;
pub mod database_integration;

pub use critical_path::{CriticalPathOptimizer, create_critical_path_optimizer};
pub use transaction_batching::{
    TransactionBatcher, create_transaction_batcher, WorkloadType,
    BatchedTransaction, TransactionPriority,
};
pub use memory_layout::{CacheAlignedAllocator, CompactRecord};
pub use simd::safe as simd_ops;
pub use database_integration::{OptimizedDatabaseOps, OptimizedDatabaseStats, EfficiencyMetrics};