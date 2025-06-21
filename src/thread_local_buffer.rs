use crate::error::Result;
use crate::Database;
use std::cell::RefCell;
use std::sync::Arc;

const THREAD_LOCAL_BUFFER_SIZE: usize = 1000;

thread_local! {
    /// Thread-local write buffer to reduce lock contention
    static WRITE_BUFFER: RefCell<Vec<(Vec<u8>, Vec<u8>)>> = 
        RefCell::new(Vec::with_capacity(THREAD_LOCAL_BUFFER_SIZE));
}

/// Thread-local write combining for high-performance writes
pub struct ThreadLocalWriter {
    db: Arc<Database>,
}

impl ThreadLocalWriter {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
    
    /// Buffer a write operation thread-locally
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        WRITE_BUFFER.with(|buf| {
            let mut buffer = buf.borrow_mut();
            buffer.push((key, value));
            
            // Auto-flush when buffer is full
            if buffer.len() >= THREAD_LOCAL_BUFFER_SIZE {
                self.flush_buffer(&mut buffer)?;
            }
            
            Ok(())
        })
    }
    
    /// Manually flush the thread-local buffer
    pub fn flush(&self) -> Result<()> {
        WRITE_BUFFER.with(|buf| {
            let mut buffer = buf.borrow_mut();
            if !buffer.is_empty() {
                self.flush_buffer(&mut buffer)?;
            }
            Ok(())
        })
    }
    
    /// Internal method to flush buffer to database
    fn flush_buffer(&self, buffer: &mut Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        
        // Use put_batch for efficiency
        self.db.put_batch(buffer)?;
        buffer.clear();
        
        Ok(())
    }
}

impl Drop for ThreadLocalWriter {
    fn drop(&mut self) {
        // Best effort flush on drop
        let _ = self.flush();
    }
}

/// Extension methods for Database to support thread-local writes
impl Database {
    /// Create a thread-local writer for this database
    pub fn thread_local_writer(self: Arc<Self>) -> ThreadLocalWriter {
        ThreadLocalWriter::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::LightningDbConfig;
    
    #[test]
    fn test_thread_local_writer() -> Result<()> {
        let dir = tempdir()?;
        let db = Arc::new(Database::create(dir.path().join("test.db"), LightningDbConfig::default())?);
        let writer = db.clone().thread_local_writer();
        
        // Write some data
        for i in 0..100 {
            writer.put(format!("key{}", i).into_bytes(), format!("value{}", i).into_bytes())?;
        }
        
        // Flush and verify
        writer.flush()?;
        
        assert_eq!(
            db.get(b"key50")?,
            Some(b"value50".to_vec())
        );
        
        Ok(())
    }
    
    #[test]
    fn test_auto_flush() -> Result<()> {
        let dir = tempdir()?;
        let db = Arc::new(Database::create(dir.path().join("test.db"), LightningDbConfig::default())?);
        let writer = db.clone().thread_local_writer();
        
        // Write enough to trigger auto-flush
        for i in 0..THREAD_LOCAL_BUFFER_SIZE + 10 {
            writer.put(format!("key{}", i).into_bytes(), format!("value{}", i).into_bytes())?;
        }
        
        // First batch should be flushed automatically
        assert_eq!(
            db.get(b"key0")?,
            Some(b"value0".to_vec())
        );
        
        Ok(())
    }
}