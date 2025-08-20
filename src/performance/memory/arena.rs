//! Arena allocators for temporary allocations in batch operations

use bumpalo::Bump;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;

thread_local! {
    static THREAD_ARENA: RefCell<Bump> = RefCell::new(Bump::new());
}

/// Arena pool for reusing bump allocators
pub struct ArenaPool {
    pools: Mutex<VecDeque<Bump>>,
    max_arenas: usize,
}

impl ArenaPool {
    pub fn new(max_arenas: usize) -> Self {
        Self {
            pools: Mutex::new(VecDeque::with_capacity(max_arenas)),
            max_arenas,
        }
    }

    pub fn get_arena(&self) -> Bump {
        let mut pools = self.pools.lock();
        pools.pop_front().unwrap_or_else(|| Bump::new())
    }

    pub fn return_arena(&self, mut arena: Bump) {
        arena.reset();
        let mut pools = self.pools.lock();
        if pools.len() < self.max_arenas {
            pools.push_back(arena);
        }
    }
}

/// Arena-allocated vector for batch operations
pub struct ArenaVec<'a, T> {
    arena: &'a Bump,
    data: &'a mut Vec<T>,
}

impl<'a, T> ArenaVec<'a, T> {
    pub fn new_in(arena: &'a Bump) -> Self {
        let data = arena.alloc(Vec::new());
        Self { arena, data }
    }

    pub fn with_capacity_in(arena: &'a Bump, capacity: usize) -> Self {
        let data = arena.alloc(Vec::with_capacity(capacity));
        Self { arena, data }
    }

    pub fn push(&mut self, value: T) {
        self.data.push(value);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn as_slice(&self) -> &[T] {
        self.data.as_slice()
    }

    pub fn into_inner(self) -> &'a mut Vec<T> {
        self.data
    }
}

/// Arena-allocated string for temporary string operations
pub struct ArenaString<'a> {
    arena: &'a Bump,
    data: &'a mut String,
}

impl<'a> ArenaString<'a> {
    pub fn new_in(arena: &'a Bump) -> Self {
        let data = arena.alloc(String::new());
        Self { arena, data }
    }

    pub fn with_capacity_in(arena: &'a Bump, capacity: usize) -> Self {
        let data = arena.alloc(String::with_capacity(capacity));
        Self { arena, data }
    }

    pub fn push_str(&mut self, s: &str) {
        self.data.push_str(s);
    }

    pub fn push(&mut self, ch: char) {
        self.data.push(ch);
    }

    pub fn as_str(&self) -> &str {
        self.data.as_str()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn into_inner(self) -> &'a mut String {
        self.data
    }
}

/// Batch operation context with arena allocation
pub struct BatchContext {
    arena: Bump,
    pool: Option<Arc<ArenaPool>>,
}

impl BatchContext {
    pub fn new() -> Self {
        Self {
            arena: Bump::new(),
            pool: None,
        }
    }

    pub fn with_pool(pool: Arc<ArenaPool>) -> Self {
        let arena = pool.get_arena();
        Self {
            arena,
            pool: Some(pool),
        }
    }

    pub fn arena(&self) -> &Bump {
        &self.arena
    }

    pub fn alloc_vec<T>(&self) -> ArenaVec<T> {
        ArenaVec::new_in(&self.arena)
    }

    pub fn alloc_vec_with_capacity<T>(&self, capacity: usize) -> ArenaVec<T> {
        ArenaVec::with_capacity_in(&self.arena, capacity)
    }

    pub fn alloc_string(&self) -> ArenaString {
        ArenaString::new_in(&self.arena)
    }

    pub fn alloc_string_with_capacity(&self, capacity: usize) -> ArenaString {
        ArenaString::with_capacity_in(&self.arena, capacity)
    }

    /// Allocate a slice of bytes in the arena
    pub fn alloc_bytes(&self, data: &[u8]) -> &[u8] {
        self.arena.alloc_slice_copy(data)
    }

    /// Allocate a string slice in the arena
    pub fn alloc_str(&self, s: &str) -> &str {
        self.arena.alloc_str(s)
    }

    /// Get memory usage statistics
    pub fn memory_usage(&self) -> usize {
        self.arena.allocated_bytes()
    }
}

impl Drop for BatchContext {
    fn drop(&mut self) {
        if let Some(pool) = &self.pool {
            // Move arena out and return to pool
            let arena = std::mem::replace(&mut self.arena, Bump::new());
            pool.return_arena(arena);
        }
    }
}

/// Convenience function for temporary arena-based operations
pub fn with_arena<F, R>(f: F) -> R 
where
    F: FnOnce(&Bump) -> R,
{
    THREAD_ARENA.with(|arena_cell| {
        let mut arena = arena_cell.borrow_mut();
        arena.reset();
        f(&*arena)
    })
}

/// Batch allocation helper for collections
pub struct BatchAllocator<'a> {
    arena: &'a Bump,
}

impl<'a> BatchAllocator<'a> {
    pub fn new(arena: &'a Bump) -> Self {
        Self { arena }
    }

    /// Allocate a vector with pre-calculated capacity
    pub fn vec_with_capacity<T>(&self, capacity: usize) -> ArenaVec<'a, T> {
        ArenaVec::with_capacity_in(self.arena, capacity)
    }

    /// Allocate multiple vectors in batch
    pub fn multiple_vecs<T>(&self, count: usize, capacity: usize) -> Vec<ArenaVec<'a, T>> {
        (0..count)
            .map(|_| ArenaVec::with_capacity_in(self.arena, capacity))
            .collect()
    }

    /// Allocate a string buffer with capacity
    pub fn string_with_capacity(&self, capacity: usize) -> ArenaString<'a> {
        ArenaString::with_capacity_in(self.arena, capacity)
    }

    /// Allocate a slice from existing data
    pub fn slice_copy<T: Copy>(&self, data: &[T]) -> &'a [T] {
        self.arena.alloc_slice_copy(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_vec() {
        let arena = Bump::new();
        let mut vec = ArenaVec::new_in(&arena);
        
        vec.push(1);
        vec.push(2);
        vec.push(3);
        
        assert_eq!(vec.len(), 3);
        assert_eq!(vec.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_arena_string() {
        let arena = Bump::new();
        let mut s = ArenaString::new_in(&arena);
        
        s.push_str("Hello");
        s.push(' ');
        s.push_str("World");
        
        assert_eq!(s.as_str(), "Hello World");
    }

    #[test]
    fn test_batch_context() {
        let ctx = BatchContext::new();
        let mut vec = ctx.alloc_vec_with_capacity(10);
        
        for i in 0..5 {
            vec.push(i);
        }
        
        assert_eq!(vec.len(), 5);
    }

    #[test]
    fn test_arena_pool() {
        let pool = Arc::new(ArenaPool::new(2));
        
        {
            let ctx = BatchContext::with_pool(pool.clone());
            let _vec = ctx.alloc_vec::<i32>();
            // Arena should be returned to pool on drop
        }
        
        // Pool should now have one arena
        let arena = pool.get_arena();
        assert_eq!(arena.allocated_bytes(), 0); // Should be reset
    }

    #[test]
    fn test_with_arena() {
        let result = with_arena(|arena| {
            let allocator = BatchAllocator::new(arena);
            let mut vec = allocator.vec_with_capacity(5);
            vec.push(42);
            vec.len()
        });
        
        assert_eq!(result, 1);
    }

    #[test]
    fn test_batch_allocator() {
        let arena = Bump::new();
        let allocator = BatchAllocator::new(&arena);
        
        let vecs = allocator.multiple_vecs::<i32>(3, 10);
        assert_eq!(vecs.len(), 3);
        
        let data = [1, 2, 3, 4, 5];
        let slice = allocator.slice_copy(&data);
        assert_eq!(slice, &[1, 2, 3, 4, 5]);
    }
}