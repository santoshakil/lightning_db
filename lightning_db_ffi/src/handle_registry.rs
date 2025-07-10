use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

pub struct HandleRegistry<T> {
    handles: Arc<RwLock<HashMap<u64, Arc<T>>>>,
}

impl<T> HandleRegistry<T> {
    pub fn new() -> Self {
        Self {
            handles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn insert(&self, value: T) -> u64 {
        let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
        let mut handles = self.handles.write();
        handles.insert(handle, Arc::new(value));
        handle
    }

    pub fn get(&self, handle: u64) -> Option<Arc<T>> {
        let handles = self.handles.read();
        handles.get(&handle).cloned()
    }

    pub fn remove(&self, handle: u64) -> Option<Arc<T>> {
        let mut handles = self.handles.write();
        handles.remove(&handle)
    }

    pub fn clear(&self) {
        let mut handles = self.handles.write();
        handles.clear();
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        let handles = self.handles.read();
        handles.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        let handles = self.handles.read();
        handles.is_empty()
    }
}

// Special registry for mutable types like iterators
pub struct MutableHandleRegistry<T> {
    handles: Arc<RwLock<HashMap<u64, Mutex<T>>>>,
}

impl<T> MutableHandleRegistry<T> {
    pub fn new() -> Self {
        Self {
            handles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn insert(&self, value: T) -> u64 {
        let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
        let mut handles = self.handles.write();
        handles.insert(handle, Mutex::new(value));
        handle
    }

    pub fn with_mut<F, R>(&self, handle: u64, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        let handles = self.handles.read();
        handles.get(&handle).map(|mutex| {
            let mut value = mutex.lock();
            f(&mut *value)
        })
    }

    pub fn remove(&self, handle: u64) -> Option<T> {
        let mut handles = self.handles.write();
        handles.remove(&handle).map(|mutex| mutex.into_inner())
    }

    pub fn clear(&self) {
        let mut handles = self.handles.write();
        handles.clear();
    }
}

impl<T> Default for HandleRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for HandleRegistry<T> {
    fn clone(&self) -> Self {
        Self {
            handles: self.handles.clone(),
        }
    }
}

impl<T> Default for MutableHandleRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for MutableHandleRegistry<T> {
    fn clone(&self) -> Self {
        Self {
            handles: self.handles.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_registry() {
        let registry = HandleRegistry::new();

        // Test insert
        let handle1 = registry.insert("value1".to_string());
        let handle2 = registry.insert("value2".to_string());

        assert_ne!(handle1, handle2);

        // Test get
        assert_eq!(
            registry.get(handle1).as_ref().map(|v| v.as_str()),
            Some("value1")
        );
        assert_eq!(
            registry.get(handle2).as_ref().map(|v| v.as_str()),
            Some("value2")
        );
        assert!(registry.get(999).is_none());

        // Test remove
        let removed = registry.remove(handle1);
        assert_eq!(removed.as_ref().map(|v| v.as_str()), Some("value1"));
        assert!(registry.get(handle1).is_none());

        // Test clear
        registry.clear();
        assert!(registry.get(handle2).is_none());
    }

    #[test]
    fn test_mutable_handle_registry() {
        let registry = MutableHandleRegistry::new();

        // Test insert
        let handle = registry.insert(vec![1, 2, 3]);

        // Test with_mut
        let result = registry.with_mut(handle, |v| {
            v.push(4);
            v.len()
        });
        assert_eq!(result, Some(4));

        // Verify mutation persisted
        let result = registry.with_mut(handle, |v| v.clone());
        assert_eq!(result, Some(vec![1, 2, 3, 4]));

        // Test remove
        let removed = registry.remove(handle);
        assert_eq!(removed, Some(vec![1, 2, 3, 4]));

        // Verify it's gone
        let result = registry.with_mut(handle, |v| v.len());
        assert_eq!(result, None);
    }
}
