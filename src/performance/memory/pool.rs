#![allow(clippy::type_complexity)]
//! Object pooling for frequently allocated structures

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};

/// Generic object pool for reusing expensive-to-create objects
pub struct ObjectPool<T> {
    pool: Mutex<VecDeque<T>>,
    factory: Box<dyn Fn() -> T + Send + Sync>,
    reset: Option<Box<dyn Fn(&mut T) + Send + Sync>>,
    max_size: usize,
}

impl<T> ObjectPool<T> {
    pub fn new<F>(max_size: usize, factory: F) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        Self {
            pool: Mutex::new(VecDeque::with_capacity(max_size.min(16))),
            factory: Box::new(factory),
            reset: None,
            max_size,
        }
    }

    pub fn with_reset<F, R>(max_size: usize, factory: F, reset: R) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
        R: Fn(&mut T) + Send + Sync + 'static,
    {
        Self {
            pool: Mutex::new(VecDeque::with_capacity(max_size.min(16))),
            factory: Box::new(factory),
            reset: Some(Box::new(reset)),
            max_size,
        }
    }

    pub fn get(&self) -> PoolGuard<'_, T> {
        let object = {
            let mut pool = self.pool.lock();
            pool.pop_front().unwrap_or_else(|| (self.factory)())
        };

        PoolGuard {
            object: Some(object),
            pool: self,
        }
    }

    fn return_object(&self, mut object: T) {
        if let Some(ref reset_fn) = self.reset {
            reset_fn(&mut object);
        }

        let mut pool = self.pool.lock();
        if pool.len() < self.max_size {
            pool.push_back(object);
        }
    }

    pub fn len(&self) -> usize {
        self.pool.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.pool.lock().is_empty()
    }

    pub fn clear(&self) {
        self.pool.lock().clear();
    }
}

/// RAII guard for pooled objects
pub struct PoolGuard<'a, T> {
    object: Option<T>,
    pool: &'a ObjectPool<T>,
}

impl<'a, T> PoolGuard<'a, T> {
    /// Take ownership of the object, preventing it from being returned to pool
    pub fn take(mut self) -> T {
        self.object.take().expect("Object already taken")
    }
}

impl<'a, T> Deref for PoolGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.object.as_ref().expect("Object already taken")
    }
}

impl<'a, T> DerefMut for PoolGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.object.as_mut().expect("Object already taken")
    }
}

impl<'a, T> Drop for PoolGuard<'a, T> {
    fn drop(&mut self) {
        if let Some(object) = self.object.take() {
            self.pool.return_object(object);
        }
    }
}

/// Trait for objects that can be pooled
pub trait PooledObject {
    fn reset(&mut self);
}

/// Pool for PooledObjects with automatic reset
pub struct TypedPool<T: PooledObject> {
    inner: ObjectPool<T>,
}

impl<T: PooledObject + Default> TypedPool<T> {
    pub fn new(max_size: usize) -> Self {
        Self {
            inner: ObjectPool::with_reset(
                max_size,
                || T::default(),
                |obj| obj.reset(),
            ),
        }
    }
}

impl<T: PooledObject> TypedPool<T> {
    pub fn with_factory<F>(max_size: usize, factory: F) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        Self {
            inner: ObjectPool::with_reset(
                max_size,
                factory,
                |obj| obj.reset(),
            ),
        }
    }

    pub fn get(&self) -> PoolGuard<'_, T> {
        self.inner.get()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&self) {
        self.inner.clear()
    }
}

/// Pre-defined pools for common database objects
pub mod pools {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::LazyLock;

    /// Pool for Vec<u8> buffers
    pub static VEC_BUFFER_POOL: LazyLock<ObjectPool<Vec<u8>>> = LazyLock::new(|| {
        ObjectPool::with_reset(
            100,
            || Vec::with_capacity(1024),
            |vec| {
                vec.clear();
                // Keep reasonable capacity to avoid frequent reallocations
                if vec.capacity() > 64 * 1024 {
                    vec.shrink_to(16 * 1024);
                }
            },
        )
    });

    /// Pool for String buffers
    pub static STRING_BUFFER_POOL: LazyLock<ObjectPool<String>> = LazyLock::new(|| {
        ObjectPool::with_reset(
            50,
            || String::with_capacity(256),
            |s| {
                s.clear();
                if s.capacity() > 16 * 1024 {
                    s.shrink_to(4 * 1024);
                }
            },
        )
    });

    /// Pool for HashMap buffers
    pub static HASHMAP_POOL: LazyLock<ObjectPool<HashMap<Bytes, Bytes>>> = LazyLock::new(|| {
        ObjectPool::with_reset(
            25,
            || HashMap::with_capacity(32),
            |map| {
                map.clear();
                if map.capacity() > 1024 {
                    map.shrink_to(256);
                }
            },
        )
    });

    /// Pool for temporary byte vectors used in serialization
    pub static SERIALIZATION_BUFFER_POOL: LazyLock<ObjectPool<Vec<u8>>> = LazyLock::new(|| {
        ObjectPool::with_reset(
            200,
            || Vec::with_capacity(4096), // Common page size
            |vec| {
                vec.clear();
                // Keep buffers sized for typical database operations
                if vec.capacity() > 256 * 1024 {
                    vec.shrink_to(64 * 1024);
                }
            },
        )
    });
}

/// Convenience functions for getting pooled objects
pub fn get_vec_buffer() -> PoolGuard<'static, Vec<u8>> {
    pools::VEC_BUFFER_POOL.get()
}

pub fn get_string_buffer() -> PoolGuard<'static, String> {
    pools::STRING_BUFFER_POOL.get()
}

pub fn get_hashmap() -> PoolGuard<'static, HashMap<Bytes, Bytes>> {
    pools::HASHMAP_POOL.get()
}

pub fn get_serialization_buffer() -> PoolGuard<'static, Vec<u8>> {
    pools::SERIALIZATION_BUFFER_POOL.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestObject {
        data: Vec<i32>,
        name: String,
    }

    impl PooledObject for TestObject {
        fn reset(&mut self) {
            self.data.clear();
            self.name.clear();
        }
    }

    #[test]
    fn test_object_pool() {
        let pool = ObjectPool::new(5, || Vec::<i32>::new());

        let mut obj1 = pool.get();
        obj1.push(42);
        assert_eq!(obj1.len(), 1);

        drop(obj1);

        let _obj2 = pool.get();
        // Object should be reused but not necessarily reset
        assert_eq!(pool.len(), 0); // Should be taken from pool
    }

    #[test]
    fn test_object_pool_with_reset() {
        let pool = ObjectPool::with_reset(
            5,
            || Vec::with_capacity(10),
            |vec: &mut Vec<i32>| vec.clear(),
        );

        let mut obj1 = pool.get();
        obj1.push(42);
        obj1.push(24);
        drop(obj1);

        let obj2 = pool.get();
        assert!(obj2.is_empty()); // Should be reset
        assert!(obj2.capacity() >= 10); // Should maintain capacity
    }

    #[test]
    fn test_typed_pool() {
        let pool = TypedPool::<TestObject>::new(3);

        let mut obj1 = pool.get();
        obj1.data.push(1);
        obj1.data.push(2);
        obj1.name.push_str("test");

        drop(obj1);

        let obj2 = pool.get();
        assert!(obj2.data.is_empty());
        assert!(obj2.name.is_empty());
    }

    #[test]
    fn test_pool_guard_take() {
        let pool = ObjectPool::new(1, || Vec::<i32>::new());
        let mut guard = pool.get();
        guard.push(42);

        let vec = guard.take();
        assert_eq!(vec, vec![42]);
        assert_eq!(pool.len(), 0); // Object not returned to pool
    }

    #[test]
    fn test_predefined_pools() {
        let mut vec_buf = get_vec_buffer();
        vec_buf.extend_from_slice(b"hello world");
        assert!(!vec_buf.is_empty());
        drop(vec_buf);

        let mut str_buf = get_string_buffer();
        str_buf.push_str("test string");
        assert!(!str_buf.is_empty());
        drop(str_buf);

        let mut map = get_hashmap();
        map.insert(Bytes::from("key"), Bytes::from("value"));
        assert!(!map.is_empty());
        drop(map);

        // Verify objects are reset when retrieved again
        let vec_buf2 = get_vec_buffer();
        assert!(vec_buf2.is_empty());

        let str_buf2 = get_string_buffer();
        assert!(str_buf2.is_empty());

        let map2 = get_hashmap();
        assert!(map2.is_empty());
    }

    #[test]
    fn test_pool_size_limit() {
        let pool = ObjectPool::new(2, || Vec::<i32>::new());

        // Take and return objects to fill pool
        drop(pool.get());
        drop(pool.get());
        assert_eq!(pool.len(), 2);

        // Third object should not be stored when pool is full
        drop(pool.get());
        assert_eq!(pool.len(), 2);
    }
}
