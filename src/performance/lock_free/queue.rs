use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use crossbeam::queue::SegQueue;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Lock-free queue implementation using crossbeam channels
/// Provides both bounded and unbounded variants
pub struct LockFreeQueue<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
    len: Arc<AtomicUsize>,
}

impl<T> LockFreeQueue<T> {
    /// Create a new unbounded queue
    pub fn unbounded() -> Self {
        let (sender, receiver) = unbounded();
        Self {
            sender,
            receiver,
            len: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new bounded queue with specified capacity
    pub fn bounded(capacity: usize) -> Self {
        let (sender, receiver) = bounded(capacity);
        Self {
            sender,
            receiver,
            len: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Push an item to the queue (lock-free)
    pub fn push(&self, item: T) -> Result<(), T> {
        match self.sender.try_send(item) {
            Ok(_) => {
                self.len.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(e.into_inner()),
        }
    }

    /// Pop an item from the queue (lock-free)
    pub fn pop(&self) -> Option<T> {
        match self.receiver.try_recv() {
            Ok(item) => {
                self.len.fetch_sub(1, Ordering::Relaxed);
                Some(item)
            }
            Err(_) => None,
        }
    }

    /// Get current queue length (approximate)
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Check if queue is empty (approximate)
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a new sender handle
    pub fn sender(&self) -> Sender<T> {
        self.sender.clone()
    }

    /// Create a new receiver handle
    pub fn receiver(&self) -> Receiver<T> {
        self.receiver.clone()
    }
}

/// Alternative lock-free queue using SegQueue
/// Better for SPSC (single producer, single consumer) scenarios
pub struct LockFreeSegQueue<T> {
    queue: Arc<SegQueue<T>>,
}

impl<T> Default for LockFreeSegQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> LockFreeSegQueue<T> {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
        }
    }

    /// Push an item (wait-free)
    pub fn push(&self, item: T) {
        self.queue.push(item);
    }

    /// Pop an item (lock-free)
    pub fn pop(&self) -> Option<T> {
        self.queue.pop()
    }

    /// Get approximate length
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl<T> Clone for LockFreeSegQueue<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

/// Specialized prefetch request queue
#[derive(Clone)]
pub struct PrefetchQueue {
    high_priority: LockFreeSegQueue<PrefetchRequest>,
    normal_priority: LockFreeSegQueue<PrefetchRequest>,
    pending_count: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
pub struct PrefetchRequest {
    pub key: Vec<u8>,
    pub priority: PrefetchPriority,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PrefetchPriority {
    High,
    Normal,
}

impl Default for PrefetchQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl PrefetchQueue {
    pub fn new() -> Self {
        Self {
            high_priority: LockFreeSegQueue::new(),
            normal_priority: LockFreeSegQueue::new(),
            pending_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add a prefetch request
    pub fn push(&self, request: PrefetchRequest) {
        match request.priority {
            PrefetchPriority::High => self.high_priority.push(request),
            PrefetchPriority::Normal => self.normal_priority.push(request),
        }
        self.pending_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get next request (high priority first)
    pub fn pop(&self) -> Option<PrefetchRequest> {
        let request = self
            .high_priority
            .pop()
            .or_else(|| self.normal_priority.pop());

        if request.is_some() {
            self.pending_count.fetch_sub(1, Ordering::Relaxed);
        }

        request
    }

    /// Get pending request count
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Relaxed)
    }

    /// Clear all pending requests
    pub fn clear(&self) {
        while self.high_priority.pop().is_some() {}
        while self.normal_priority.pop().is_some() {}
        self.pending_count.store(0, Ordering::Relaxed);
    }
}

/// Work-stealing queue for parallel processing
pub struct WorkStealingQueue<T> {
    queues: Vec<LockFreeSegQueue<T>>,
    next_queue: AtomicUsize,
}

impl<T> WorkStealingQueue<T> {
    pub fn new(num_workers: usize) -> Self {
        let mut queues = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            queues.push(LockFreeSegQueue::new());
        }

        Self {
            queues,
            next_queue: AtomicUsize::new(0),
        }
    }

    /// Push work to a queue (round-robin)
    pub fn push(&self, item: T) {
        let idx = self.next_queue.fetch_add(1, Ordering::Relaxed) % self.queues.len();
        self.queues[idx].push(item);
    }

    /// Try to pop from worker's queue, then steal from others
    pub fn pop(&self, worker_id: usize) -> Option<T> {
        // First try own queue
        if let Some(item) = self.queues[worker_id].pop() {
            return Some(item);
        }

        // Try to steal from other queues
        let num_queues = self.queues.len();
        for i in 1..num_queues {
            let target = (worker_id + i) % num_queues;
            if let Some(item) = self.queues[target].pop() {
                return Some(item);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_lock_free_queue() {
        let queue = Arc::new(LockFreeQueue::unbounded());

        // Test concurrent push/pop
        let mut handles: Vec<thread::JoinHandle<()>> = vec![];

        // Producers
        for i in 0..2 {
            let queue_clone = Arc::clone(&queue);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    queue_clone.push(i * 100 + j).unwrap();
                }
            });
            handles.push(handle);
        }

        // Consumers
        let mut consumer_handles: Vec<thread::JoinHandle<i32>> = vec![];
        for _ in 0..2 {
            let queue_clone = Arc::clone(&queue);
            let handle = thread::spawn(move || {
                let mut count = 0;
                while count < 100 {
                    if queue_clone.pop().is_some() {
                        count += 1;
                    } else {
                        thread::yield_now();
                    }
                }
                count
            });
            consumer_handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for handle in consumer_handles {
            handle.join().unwrap();
        }

        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_prefetch_queue() {
        let queue = PrefetchQueue::new();

        // Add requests with different priorities
        queue.push(PrefetchRequest {
            key: b"normal1".to_vec(),
            priority: PrefetchPriority::Normal,
            timestamp: 1,
        });

        queue.push(PrefetchRequest {
            key: b"high1".to_vec(),
            priority: PrefetchPriority::High,
            timestamp: 2,
        });

        queue.push(PrefetchRequest {
            key: b"normal2".to_vec(),
            priority: PrefetchPriority::Normal,
            timestamp: 3,
        });

        // High priority should come first
        let first = queue.pop().unwrap();
        assert_eq!(first.key, b"high1");

        // Then normal priority in FIFO order
        let second = queue.pop().unwrap();
        assert_eq!(second.key, b"normal1");

        assert_eq!(queue.pending_count(), 1);
    }

    #[test]
    fn test_work_stealing() {
        let queue = Arc::new(WorkStealingQueue::new(4));

        // Push items
        for i in 0..16 {
            queue.push(i);
        }

        // Workers steal from each other
        let mut handles = vec![];
        for worker_id in 0..4 {
            let queue_clone = Arc::clone(&queue);
            let handle = thread::spawn(move || {
                let mut items = vec![];
                while let Some(item) = queue_clone.pop(worker_id) {
                    items.push(item);
                }
                items
            });
            handles.push(handle);
        }

        let mut all_items = vec![];
        for handle in handles {
            all_items.extend(handle.join().unwrap());
        }

        assert_eq!(all_items.len(), 16);
    }
}
