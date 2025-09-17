use parking_lot::Mutex;
use std::collections::VecDeque;

const SMALL_KEY_SIZE: usize = 64;
const SMALL_VALUE_SIZE: usize = 1024;
const POOL_SIZE: usize = 1000;

pub struct SmallAllocPool {
    key_pool: Mutex<VecDeque<Vec<u8>>>,
    value_pool: Mutex<VecDeque<Vec<u8>>>,
}

impl SmallAllocPool {
    pub fn new() -> Self {
        let mut key_pool = VecDeque::with_capacity(POOL_SIZE);
        let mut value_pool = VecDeque::with_capacity(POOL_SIZE);

        // Pre-allocate buffers
        for _ in 0..POOL_SIZE {
            key_pool.push_back(Vec::with_capacity(SMALL_KEY_SIZE));
            value_pool.push_back(Vec::with_capacity(SMALL_VALUE_SIZE));
        }

        Self {
            key_pool: Mutex::new(key_pool),
            value_pool: Mutex::new(value_pool),
        }
    }

    pub fn alloc_key(&self, data: &[u8]) -> Vec<u8> {
        if data.len() <= SMALL_KEY_SIZE {
            if let Some(mut buf) = self.key_pool.lock().pop_front() {
                buf.clear();
                buf.extend_from_slice(data);
                return buf;
            }
        }
        data.to_vec()
    }

    pub fn alloc_value(&self, data: &[u8]) -> Vec<u8> {
        if data.len() <= SMALL_VALUE_SIZE {
            if let Some(mut buf) = self.value_pool.lock().pop_front() {
                buf.clear();
                buf.extend_from_slice(data);
                return buf;
            }
        }
        data.to_vec()
    }

    pub fn recycle_key(&self, mut buf: Vec<u8>) {
        if buf.capacity() <= SMALL_KEY_SIZE {
            buf.clear();
            if let Ok(mut pool) = self.key_pool.try_lock() {
                if pool.len() < POOL_SIZE {
                    pool.push_back(buf);
                }
            }
        }
    }

    pub fn recycle_value(&self, mut buf: Vec<u8>) {
        if buf.capacity() <= SMALL_VALUE_SIZE {
            buf.clear();
            if let Ok(mut pool) = self.value_pool.try_lock() {
                if pool.len() < POOL_SIZE {
                    pool.push_back(buf);
                }
            }
        }
    }
}

impl Default for SmallAllocPool {
    fn default() -> Self {
        Self::new()
    }
}