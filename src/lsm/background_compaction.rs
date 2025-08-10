use crate::error::Result;
use crate::lsm::compaction::Compactor;
use crate::lsm::SSTable;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

pub struct BackgroundCompactionScheduler {
    compactor: Arc<Compactor>,
    running: Arc<AtomicBool>,
    worker_threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
    compaction_queue: Arc<RwLock<Vec<CompactionTask>>>,
    wake_signal: Arc<(Mutex<bool>, Condvar)>,

    // Statistics
    total_compactions: Arc<AtomicU64>,
    total_compaction_time: Arc<AtomicU64>,
    last_compaction: Arc<AtomicU64>,

    // Configuration
    max_concurrent_compactions: usize,
    compaction_interval: Duration,
    level_0_compaction_threshold: usize,
    level_size_multiplier: usize,
}

#[derive(Debug, Clone)]
pub struct CompactionTask {
    pub level: usize,
    pub files: Vec<Arc<SSTable>>,
    pub priority: CompactionPriority,
    pub created_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompactionPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl BackgroundCompactionScheduler {
    pub fn new(
        compactor: Arc<Compactor>,
        max_concurrent_compactions: usize,
        compaction_interval: Duration,
    ) -> Self {
        Self {
            compactor,
            running: Arc::new(AtomicBool::new(false)),
            worker_threads: Arc::new(Mutex::new(Vec::new())),
            compaction_queue: Arc::new(RwLock::new(Vec::new())),
            wake_signal: Arc::new((Mutex::new(false), Condvar::new())),
            total_compactions: Arc::new(AtomicU64::new(0)),
            total_compaction_time: Arc::new(AtomicU64::new(0)),
            last_compaction: Arc::new(AtomicU64::new(0)),
            max_concurrent_compactions,
            compaction_interval,
            level_0_compaction_threshold: 4,
            level_size_multiplier: 10,
        }
    }

    pub fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        info!("Starting background compaction scheduler");

        let mut threads = match self.worker_threads.lock() {
            Ok(t) => t,
            Err(poisoned) => {
                warn!("Worker threads mutex poisoned, recovering");
                poisoned.into_inner()
            }
        };

        // Start scheduler thread
        let scheduler_handle = self.start_scheduler_thread();
        threads.push(scheduler_handle);

        // Start worker threads
        for i in 0..self.max_concurrent_compactions {
            let worker_handle = self.start_worker_thread(i);
            threads.push(worker_handle);
        }

        Ok(())
    }

    pub fn stop(&self) {
        if !self.running.swap(false, Ordering::SeqCst) {
            return; // Already stopped
        }

        info!("Stopping background compaction scheduler");

        // Signal all threads to wake up and check running status
        {
            let (lock, cvar) = &*self.wake_signal;
            let mut wake = match lock.lock() {
                Ok(w) => w,
                Err(poisoned) => {
                    warn!("Wake condition mutex poisoned, recovering");
                    poisoned.into_inner()
                }
            };
            *wake = true;
            cvar.notify_all();
        }

        // Wait for all threads to finish
        let mut threads = match self.worker_threads.lock() {
            Ok(t) => t,
            Err(poisoned) => {
                warn!("Worker threads mutex poisoned, recovering");
                poisoned.into_inner()
            }
        };
        while let Some(handle) = threads.pop() {
            if let Err(e) = handle.join() {
                error!("Error joining compaction thread: {:?}", e);
            }
        }
    }

    fn start_scheduler_thread(&self) -> JoinHandle<()> {
        let running = Arc::clone(&self.running);
        let compaction_queue = Arc::clone(&self.compaction_queue);
        let wake_signal = Arc::clone(&self.wake_signal);
        let compaction_interval = self.compaction_interval;
        let level_0_threshold = self.level_0_compaction_threshold;
        let level_multiplier = self.level_size_multiplier;

        thread::spawn(move || {
            debug!("Background compaction scheduler thread started");

            while running.load(Ordering::SeqCst) {
                // Check if compaction is needed
                let tasks = Self::analyze_compaction_needs(level_0_threshold, level_multiplier);

                if !tasks.is_empty() {
                    debug!("Scheduling {} compaction tasks", tasks.len());

                    // Add tasks to queue
                    {
                        let mut queue = match compaction_queue.write() {
                            Ok(q) => q,
                            Err(poisoned) => {
                                warn!("Compaction queue RwLock poisoned, recovering");
                                poisoned.into_inner()
                            }
                        };
                        for task in tasks {
                            queue.push(task);
                        }
                        // Sort by priority (highest first)
                        queue.sort_by(|a, b| b.priority.cmp(&a.priority));
                    }

                    // Wake up worker threads
                    let (lock, cvar) = &*wake_signal;
                    let mut wake = match lock.lock() {
                Ok(w) => w,
                Err(poisoned) => {
                    warn!("Wake condition mutex poisoned, recovering");
                    poisoned.into_inner()
                }
            };
                    *wake = true;
                    cvar.notify_all();
                } else {
                    // Reset wake signal when no work is available to prevent spurious wakeups
                    let (lock, _cvar) = &*wake_signal;
                    let mut wake = match lock.lock() {
                Ok(w) => w,
                Err(poisoned) => {
                    warn!("Wake condition mutex poisoned, recovering");
                    poisoned.into_inner()
                }
            };
                    *wake = false;
                }

                // Sleep until next check
                thread::sleep(compaction_interval);
            }

            debug!("Background compaction scheduler thread stopped");
        })
    }

    fn start_worker_thread(&self, worker_id: usize) -> JoinHandle<()> {
        let running = Arc::clone(&self.running);
        let compactor = Arc::clone(&self.compactor);
        let compaction_queue = Arc::clone(&self.compaction_queue);
        let wake_signal = Arc::clone(&self.wake_signal);
        let total_compactions = Arc::clone(&self.total_compactions);
        let total_compaction_time = Arc::clone(&self.total_compaction_time);
        let last_compaction = Arc::clone(&self.last_compaction);

        thread::spawn(move || {
            debug!("Compaction worker {} started", worker_id);

            while running.load(Ordering::SeqCst) {
                // Try to get a task from the queue
                let task = {
                    let mut queue = match compaction_queue.write() {
                        Ok(q) => q,
                        Err(poisoned) => {
                            warn!("Compaction queue RwLock poisoned, recovering");
                            poisoned.into_inner()
                        }
                    };
                    queue.pop()
                };

                if let Some(task) = task {
                    debug!(
                        "Worker {} starting compaction for level {} with {} files",
                        worker_id,
                        task.level,
                        task.files.len()
                    );

                    let start_time = Instant::now();

                    match Self::execute_compaction_task(&compactor, &task) {
                        Ok(()) => {
                            let duration = start_time.elapsed();
                            total_compactions.fetch_add(1, Ordering::Relaxed);
                            total_compaction_time
                                .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
                            last_compaction.store(
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                                Ordering::Relaxed,
                            );

                            info!(
                                "Worker {} completed compaction for level {} in {:?}",
                                worker_id, task.level, duration
                            );
                        }
                        Err(e) => {
                            error!(
                                "Worker {} failed compaction for level {}: {}",
                                worker_id, task.level, e
                            );
                        }
                    }
                } else {
                    // No tasks available, wait for signal
                    let (lock, cvar) = &*wake_signal;
                    let mut wake = match lock.lock() {
                Ok(w) => w,
                Err(poisoned) => {
                    warn!("Wake condition mutex poisoned, recovering");
                    poisoned.into_inner()
                }
            };
                    while !*wake && running.load(Ordering::SeqCst) {
                        wake = match cvar.wait(wake) {
                            Ok(w) => w,
                            Err(poisoned) => {
                                warn!("Condition variable wait poisoned, recovering");
                                poisoned.into_inner()
                            }
                        };
                    }
                    // Don't immediately reset wake flag - let scheduler control it
                    // This prevents race conditions between multiple workers
                }
            }

            debug!("Compaction worker {} stopped", worker_id);
        })
    }

    fn analyze_compaction_needs(
        level_0_threshold: usize,
        level_multiplier: usize,
    ) -> Vec<CompactionTask> {
        let mut tasks = Vec::new();

        // This is a simplified analysis - in a real implementation,
        // you would access the actual LSM tree structure

        // Simulate checking if Level 0 needs compaction
        // In reality, you'd check the actual number of SSTables at each level
        let level_0_files = 5; // Simulated

        if level_0_files >= level_0_threshold {
            tasks.push(CompactionTask {
                level: 0,
                files: Vec::new(), // Would contain actual SSTable references
                priority: if level_0_files > level_0_threshold * 2 {
                    CompactionPriority::Critical
                } else {
                    CompactionPriority::High
                },
                created_at: Instant::now(),
            });
        }

        // Check other levels (simplified)
        for level in 1..7 {
            let max_size = level_multiplier.pow(level as u32);
            let current_size = level; // Simulated

            if current_size > max_size {
                tasks.push(CompactionTask {
                    level,
                    files: Vec::new(),
                    priority: CompactionPriority::Normal,
                    created_at: Instant::now(),
                });
            }
        }

        tasks
    }

    fn execute_compaction_task(_compactor: &Arc<Compactor>, task: &CompactionTask) -> Result<()> {
        let age = task.created_at.elapsed();
        debug!(
            "Executing compaction task for level {} with priority {:?} (age: {:?})",
            task.level, task.priority, age
        );

        // In a real implementation, this would:
        // 1. Lock the files to prevent concurrent access
        // 2. Read and merge the SSTables
        // 3. Write new compacted SSTables
        // 4. Update the LSM tree metadata
        // 5. Delete old SSTables

        // For now, simulate compaction work
        let work_duration = match task.priority {
            CompactionPriority::Critical => Duration::from_millis(100),
            CompactionPriority::High => Duration::from_millis(200),
            CompactionPriority::Normal => Duration::from_millis(500),
            CompactionPriority::Low => Duration::from_millis(1000),
        };

        thread::sleep(work_duration);

        debug!("Compaction task for level {} completed", task.level);
        Ok(())
    }

    pub fn trigger_manual_compaction(&self, level: usize, priority: CompactionPriority) {
        let task = CompactionTask {
            level,
            files: Vec::new(),
            priority,
            created_at: Instant::now(),
        };

        {
            let mut queue = match self.compaction_queue.write() {
                Ok(q) => q,
                Err(poisoned) => {
                    warn!("Compaction queue RwLock poisoned, recovering");
                    poisoned.into_inner()
                }
            };
            queue.push(task);
            queue.sort_by(|a, b| b.priority.cmp(&a.priority));
        }

        // Wake up workers
        let (lock, cvar) = &*self.wake_signal;
        let mut wake = match lock.lock() {
            Ok(w) => w,
            Err(poisoned) => {
                warn!("Wake condition mutex poisoned, recovering");
                poisoned.into_inner()
            }
        };
        *wake = true;
        cvar.notify_all();
    }

    pub fn get_statistics(&self) -> CompactionStatistics {
        CompactionStatistics {
            total_compactions: self.total_compactions.load(Ordering::Relaxed),
            total_compaction_time_ms: self.total_compaction_time.load(Ordering::Relaxed),
            last_compaction_timestamp: self.last_compaction.load(Ordering::Relaxed),
            pending_tasks: self.compaction_queue.read().map_or(0, |q| q.len()),
            average_compaction_time_ms: {
                let total_time = self.total_compaction_time.load(Ordering::Relaxed);
                let total_compactions = self.total_compactions.load(Ordering::Relaxed);
                if total_compactions > 0 {
                    total_time / total_compactions
                } else {
                    0
                }
            },
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn pending_task_count(&self) -> usize {
        self.compaction_queue.read().map_or(0, |q| q.len())
    }
}

#[derive(Debug, Clone)]
pub struct CompactionStatistics {
    pub total_compactions: u64,
    pub total_compaction_time_ms: u64,
    pub last_compaction_timestamp: u64,
    pub pending_tasks: usize,
    pub average_compaction_time_ms: u64,
}

impl Drop for BackgroundCompactionScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_compaction_scheduler_lifecycle() {
        // This test would need a mock compactor
        // For now, just test that the scheduler can be created
        let compactor = Arc::new(Compactor::new(
            std::path::PathBuf::from("/tmp"),
            crate::compression::CompressionType::None,
        ));

        let scheduler =
            BackgroundCompactionScheduler::new(compactor, 2, Duration::from_millis(100));

        assert!(!scheduler.is_running());
        assert_eq!(scheduler.pending_task_count(), 0);
    }

    #[test]
    fn test_compaction_task_priority_ordering() {
        let mut tasks = [
            CompactionTask {
                level: 0,
                files: Vec::new(),
                priority: CompactionPriority::Low,
                created_at: Instant::now(),
            },
            CompactionTask {
                level: 1,
                files: Vec::new(),
                priority: CompactionPriority::Critical,
                created_at: Instant::now(),
            },
            CompactionTask {
                level: 2,
                files: Vec::new(),
                priority: CompactionPriority::Normal,
                created_at: Instant::now(),
            },
        ];

        tasks.sort_by(|a, b| b.priority.cmp(&a.priority));

        assert_eq!(tasks[0].priority, CompactionPriority::Critical);
        assert_eq!(tasks[1].priority, CompactionPriority::Normal);
        assert_eq!(tasks[2].priority, CompactionPriority::Low);
    }
}
