use crate::error::Result;
use crate::lsm::LSMTree;
use crate::wal::{WriteAheadLog, WALOperation};
use parking_lot::RwLock;
use std::sync::Arc;

/// Fast path operations that bypass metrics and unnecessary checks
pub struct FastPath;

impl FastPath {
    /// Direct put operation with minimal overhead
    #[inline(always)]
    pub fn put_direct(
        key: &[u8],
        value: &[u8],
        lsm: &Option<Arc<LSMTree>>,
        wal: &Option<Arc<dyn WriteAheadLog + Send + Sync>>,
    ) -> Result<()> {
        // Write to WAL first for durability
        if let Some(ref wal) = wal {
            wal.append(WALOperation::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            })?;
        }

        // Write to LSM tree
        if let Some(ref lsm) = lsm {
            lsm.insert(key.to_vec(), value.to_vec())?;
        }

        Ok(())
    }

    /// Direct get operation with minimal overhead
    #[inline(always)]
    pub fn get_direct(
        key: &[u8],
        lsm: &Option<Arc<LSMTree>>,
    ) -> Result<Option<Vec<u8>>> {
        if let Some(ref lsm) = lsm {
            lsm.get(key)
        } else {
            Ok(None)
        }
    }
}

/// Zero-copy key wrapper to avoid allocations
#[derive(Clone, Copy)]
pub struct KeyRef<'a> {
    data: &'a [u8],
}

impl<'a> KeyRef<'a> {
    #[inline(always)]
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.data
    }
}

/// Optimized batch operations
pub struct BatchOps;

impl BatchOps {
    /// Process batch with pre-allocated buffers
    pub fn put_batch_optimized(
        pairs: &[(Vec<u8>, Vec<u8>)],
        lsm: &Option<Arc<LSMTree>>,
        wal: &Option<Arc<dyn WriteAheadLog + Send + Sync>>,
    ) -> Result<()> {
        // Write all to WAL in one go
        if let Some(ref wal) = wal {
            for (key, value) in pairs {
                wal.append(WALOperation::Put {
                    key: key.clone(),
                    value: value.clone(),
                })?;
            }
        }

        // Batch insert to LSM
        if let Some(ref lsm) = lsm {
            for (key, value) in pairs {
                lsm.insert(key.clone(), value.clone())?;
            }
        }

        Ok(())
    }
}

/// Memory pool for reducing allocations
pub struct MemoryPool {
    key_buffers: RwLock<Vec<Vec<u8>>>,
    value_buffers: RwLock<Vec<Vec<u8>>>,
}

impl MemoryPool {
    pub fn new(capacity: usize) -> Self {
        let mut key_buffers = Vec::with_capacity(capacity);
        let mut value_buffers = Vec::with_capacity(capacity);

        // Pre-allocate buffers
        for _ in 0..capacity {
            key_buffers.push(Vec::with_capacity(128));
            value_buffers.push(Vec::with_capacity(1024));
        }

        Self {
            key_buffers: RwLock::new(key_buffers),
            value_buffers: RwLock::new(value_buffers),
        }
    }

    #[inline(always)]
    pub fn acquire_key_buffer(&self) -> Option<Vec<u8>> {
        self.key_buffers.write().pop()
    }

    #[inline(always)]
    pub fn acquire_value_buffer(&self) -> Option<Vec<u8>> {
        self.value_buffers.write().pop()
    }

    #[inline(always)]
    pub fn return_key_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        if let Some(mut buffers) = self.key_buffers.try_write() {
            if buffers.len() < buffers.capacity() {
                buffers.push(buffer);
            }
        }
    }

    #[inline(always)]
    pub fn return_value_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        if let Some(mut buffers) = self.value_buffers.try_write() {
            if buffers.len() < buffers.capacity() {
                buffers.push(buffer);
            }
        }
    }
}