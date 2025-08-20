//! Memory management utilities and optimizations

pub mod arena;
pub mod pool;
pub mod thread_local_pools;

pub use arena::{ArenaPool, ArenaString, ArenaVec, BatchAllocator, BatchContext, with_arena};
pub use pool::{ObjectPool, PooledObject, PoolGuard, get_hashmap, get_serialization_buffer, get_vec_buffer, get_string_buffer};
pub use thread_local_pools::{
    ThreadLocalPool, ThreadLocalGuard, PoolStats,
    with_vec_buffer, with_string_buffer, with_hashmap, with_large_buffer, 
    with_small_buffer, with_btree_buffer, with_compression_workspace,
    get_pool_stats, clear_all_thread_pools
};

/// Global arena pool for batch operations
use std::sync::LazyLock;
use std::sync::Arc;

pub static GLOBAL_ARENA_POOL: LazyLock<Arc<ArenaPool>> = LazyLock::new(|| {
    Arc::new(ArenaPool::new(num_cpus::get() * 4))
});

/// Get a batch context from the global pool
pub fn get_batch_context() -> BatchContext {
    BatchContext::with_pool(GLOBAL_ARENA_POOL.clone())
}