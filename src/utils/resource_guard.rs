use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

/// Guard for ensuring proper cleanup of background threads
pub struct ThreadGuard {
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl ThreadGuard {
    pub fn new(handle: JoinHandle<()>, shutdown: Arc<AtomicBool>) -> Self {
        Self {
            handle: Some(handle),
            shutdown,
        }
    }
    
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ThreadGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Guard for file handles
pub struct FileGuard {
    file: Option<std::fs::File>,
    path: std::path::PathBuf,
    delete_on_drop: bool,
}

impl FileGuard {
    pub fn new(file: std::fs::File, path: std::path::PathBuf, delete_on_drop: bool) -> Self {
        Self {
            file: Some(file),
            path,
            delete_on_drop,
        }
    }
    
    pub fn file(&self) -> &std::fs::File {
        self.file.as_ref().expect("File already closed")
    }
    
    pub fn file_mut(&mut self) -> &mut std::fs::File {
        self.file.as_mut().expect("File already closed")
    }
    
    pub fn close(&mut self) -> std::io::Result<()> {
        if let Some(file) = self.file.take() {
            file.sync_all()?;
            drop(file);
            
            if self.delete_on_drop {
                std::fs::remove_file(&self.path)?;
            }
        }
        Ok(())
    }
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Memory pool with size limits
pub struct BoundedMemoryPool {
    pool: Vec<Vec<u8>>,
    max_items: usize,
    item_size: usize,
    total_size_limit: usize,
}

impl BoundedMemoryPool {
    pub fn new(max_items: usize, item_size: usize, total_size_limit: usize) -> Self {
        Self {
            pool: Vec::with_capacity(max_items),
            max_items,
            item_size,
            total_size_limit,
        }
    }
    
    pub fn allocate(&mut self) -> Option<Vec<u8>> {
        if let Some(buffer) = self.pool.pop() {
            Some(buffer)
        } else if self.pool.len() < self.max_items {
            let current_size = self.pool.len() * self.item_size;
            if current_size + self.item_size <= self.total_size_limit {
                Some(vec![0u8; self.item_size])
            } else {
                None
            }
        } else {
            None
        }
    }
    
    pub fn deallocate(&mut self, mut buffer: Vec<u8>) {
        if buffer.capacity() == self.item_size && self.pool.len() < self.max_items {
            buffer.clear();
            self.pool.push(buffer);
        }
    }
    
    pub fn clear(&mut self) {
        self.pool.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    
    #[test]
    fn test_thread_guard() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        
        let handle = thread::spawn(move || {
            while !shutdown_clone.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(10));
            }
        });
        
        {
            let mut guard = ThreadGuard::new(handle, shutdown.clone());
            guard.shutdown();
        }
        
        assert!(shutdown.load(Ordering::Relaxed));
    }
    
    #[test]
    fn test_bounded_memory_pool() {
        let mut pool = BoundedMemoryPool::new(10, 1024, 10240);
        
        // Allocate some buffers
        let mut buffers = Vec::new();
        for _ in 0..5 {
            buffers.push(pool.allocate().unwrap());
        }
        
        // Return them to pool
        for buffer in buffers {
            pool.deallocate(buffer);
        }
        
        // Pool should reuse them
        let reused = pool.allocate().unwrap();
        assert_eq!(reused.len(), 1024);
    }
}