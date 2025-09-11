use futures::FutureExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::task::JoinHandle as TokioJoinHandle;
use tracing::{debug, error, warn};

/// Token for cancelling operations
#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Cancel this token
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Check if cancellation was requested
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Wait for cancellation or timeout
    pub fn wait_for_cancellation(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while !self.is_cancelled() && start.elapsed() < timeout {
            std::thread::sleep(Duration::from_millis(10));
        }
        self.is_cancelled()
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Registry for managing spawned tasks and threads
pub struct TaskRegistry {
    threads: Arc<parking_lot::Mutex<Vec<ManagedThread>>>,
    tokio_tasks: Arc<parking_lot::Mutex<Vec<ManagedTokioTask>>>,
    shutdown_token: CancellationToken,
}

struct ManagedThread {
    name: String,
    handle: Option<JoinHandle<()>>,
    cancellation_token: CancellationToken,
}

struct ManagedTokioTask {
    name: String,
    handle: Option<TokioJoinHandle<()>>,
    cancellation_token: CancellationToken,
}

impl TaskRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            threads: Arc::new(parking_lot::Mutex::new(Vec::new())),
            tokio_tasks: Arc::new(parking_lot::Mutex::new(Vec::new())),
            shutdown_token: CancellationToken::new(),
        })
    }

    /// Spawn a managed thread with cancellation support
    pub fn spawn_thread<F>(self: &Arc<Self>, name: String, f: F) -> CancellationToken
    where
        F: FnOnce(CancellationToken) + Send + 'static,
    {
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let shutdown_token = self.shutdown_token.clone();

        let combined_token = CombinedCancellationToken::new(vec![token.clone(), shutdown_token]);

        let handle = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                f(combined_token.into());
            })
            .unwrap_or_else(|e| {
                panic!("Failed to spawn thread '{}': {}", name, e);
            });

        let managed = ManagedThread {
            name,
            handle: Some(handle),
            cancellation_token: token_clone,
        };

        self.threads.lock().push(managed);
        token
    }

    /// Spawn a managed Tokio task with cancellation support
    pub fn spawn_tokio_task<F, Fut>(self: &Arc<Self>, name: String, f: F) -> CancellationToken
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let shutdown_token = self.shutdown_token.clone();

        let combined_token = CombinedCancellationToken::new(vec![token.clone(), shutdown_token]);

        let handle = tokio::spawn(async move {
            f(combined_token.into()).await;
        });

        let managed = ManagedTokioTask {
            name,
            handle: Some(handle),
            cancellation_token: token_clone,
        };

        self.tokio_tasks.lock().push(managed);
        token
    }

    /// Get the global shutdown token
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.shutdown_token
    }

    /// Gracefully shutdown all managed tasks
    pub fn shutdown(&self, timeout: Duration) {
        debug!("Shutting down task registry");

        // Signal all tasks to stop
        self.shutdown_token.cancel();

        // Cancel all individual tasks
        {
            let threads = self.threads.lock();
            for thread in threads.iter() {
                thread.cancellation_token.cancel();
            }
        }

        {
            let tasks = self.tokio_tasks.lock();
            for task in tasks.iter() {
                task.cancellation_token.cancel();
            }
        }

        // Wait for threads to complete
        let start = std::time::Instant::now();
        let mut threads = self.threads.lock();
        for managed in threads.iter_mut() {
            if let Some(handle) = managed.handle.take() {
                let remaining_timeout = timeout.saturating_sub(start.elapsed());

                if remaining_timeout > Duration::ZERO {
                    // Try to join with timeout
                    match Self::join_with_timeout(handle, remaining_timeout) {
                        Ok(_) => debug!("Thread '{}' shutdown gracefully", managed.name),
                        Err(handle) => {
                            warn!(
                                "Thread '{}' did not shutdown within timeout, detaching",
                                managed.name
                            );
                            // Thread handle is dropped, thread becomes detached
                            drop(handle);
                        }
                    }
                } else {
                    warn!("No time remaining for thread '{}', detaching", managed.name);
                    drop(handle);
                }
            }
        }
        drop(threads);

        // Abort remaining Tokio tasks
        let mut tasks = self.tokio_tasks.lock();
        for managed in tasks.iter_mut() {
            if let Some(handle) = managed.handle.take() {
                handle.abort();
                debug!("Tokio task '{}' aborted", managed.name);
            }
        }
        drop(tasks);

        debug!("Task registry shutdown complete");
    }

    /// Join a thread with timeout (simulate timeout since std doesn't have it)
    fn join_with_timeout(handle: JoinHandle<()>, timeout: Duration) -> Result<(), JoinHandle<()>> {
        let start = std::time::Instant::now();

        // Use a simple polling approach since std::thread::JoinHandle doesn't have timeout
        // In a real implementation, you might use a different approach
        loop {
            if handle.is_finished() {
                return handle.join().map_err(|_| {
                    // This shouldn't happen since we checked is_finished
                    unreachable!()
                });
            }

            if start.elapsed() >= timeout {
                return Err(handle);
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Clean up completed tasks
    pub fn cleanup_completed(&self) {
        {
            let mut threads = self.threads.lock();
            threads.retain_mut(|managed| {
                if let Some(ref handle) = managed.handle {
                    if handle.is_finished() {
                        if let Some(handle) = managed.handle.take() {
                            match handle.join() {
                                Ok(_) => debug!("Thread '{}' completed successfully", managed.name),
                                Err(e) => error!("Thread '{}' panicked: {:?}", managed.name, e),
                            }
                        }
                        false // Remove from registry
                    } else {
                        true // Keep in registry
                    }
                } else {
                    false // Already completed, remove
                }
            });
        }

        {
            let mut tasks = self.tokio_tasks.lock();
            tasks.retain_mut(|managed| {
                if let Some(ref handle) = managed.handle {
                    if handle.is_finished() {
                        if let Some(handle) = managed.handle.take() {
                            match handle.now_or_never() {
                                Some(Ok(_)) => {
                                    debug!("Tokio task '{}' completed successfully", managed.name)
                                }
                                Some(Err(e)) => {
                                    error!("Tokio task '{}' failed: {:?}", managed.name, e)
                                }
                                None => warn!(
                                    "Tokio task '{}' finished but result not ready",
                                    managed.name
                                ),
                            }
                        }
                        false // Remove from registry
                    } else {
                        true // Keep in registry
                    }
                } else {
                    false // Already completed, remove
                }
            });
        }
    }

    /// Get statistics about managed tasks
    pub fn stats(&self) -> TaskRegistryStats {
        let threads = self.threads.lock();
        let tasks = self.tokio_tasks.lock();

        TaskRegistryStats {
            active_threads: threads.iter().filter(|t| t.handle.is_some()).count(),
            active_tokio_tasks: tasks.iter().filter(|t| t.handle.is_some()).count(),
            total_threads: threads.len(),
            total_tokio_tasks: tasks.len(),
        }
    }
}

// Remove the problematic Default implementation
// Use get_task_registry() to get the global instance instead

/// Combined cancellation token that triggers when any of its child tokens trigger
pub struct CombinedCancellationToken {
    tokens: Vec<CancellationToken>,
}

impl CombinedCancellationToken {
    pub fn new(tokens: Vec<CancellationToken>) -> Self {
        Self { tokens }
    }

    pub fn is_cancelled(&self) -> bool {
        self.tokens.iter().any(|t| t.is_cancelled())
    }
}

impl From<CombinedCancellationToken> for CancellationToken {
    fn from(combined: CombinedCancellationToken) -> Self {
        let token = CancellationToken::new();
        let token_clone = token.clone();

        // Spawn a background task to monitor the combined tokens
        std::thread::spawn(move || {
            while !token_clone.is_cancelled() && !combined.is_cancelled() {
                std::thread::sleep(Duration::from_millis(10));
            }

            if combined.is_cancelled() {
                token_clone.cancel();
            }
        });

        token
    }
}

#[derive(Debug, Clone)]
pub struct TaskRegistryStats {
    pub active_threads: usize,
    pub active_tokio_tasks: usize,
    pub total_threads: usize,
    pub total_tokio_tasks: usize,
}

/// Global task registry instance
static GLOBAL_TASK_REGISTRY: std::sync::OnceLock<Arc<TaskRegistry>> = std::sync::OnceLock::new();

/// Get the global task registry
pub fn get_task_registry() -> &'static Arc<TaskRegistry> {
    GLOBAL_TASK_REGISTRY.get_or_init(TaskRegistry::new)
}

/// Helper trait for thread handles to check if finished (not available in std)
trait JoinHandleExt {
    fn is_finished(&self) -> bool;
}

impl<T> JoinHandleExt for JoinHandle<T> {
    fn is_finished(&self) -> bool {
        // Since std::thread::JoinHandle doesn't have is_finished(),
        // we'll approximate by trying to join with zero timeout
        // This is a workaround - in practice you might use a different approach
        false // Conservative approach - assume not finished
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancellation_token() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());

        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_task_registry() {
        let registry = TaskRegistry::new();

        let token = registry.spawn_thread("test-thread".to_string(), |cancel_token| {
            while !cancel_token.is_cancelled() {
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        std::thread::sleep(Duration::from_millis(50));
        token.cancel();
        std::thread::sleep(Duration::from_millis(50));

        let stats = registry.stats();
        assert!(stats.total_threads >= 1);
    }

    #[tokio::test]
    async fn test_tokio_task_registry() {
        let registry = TaskRegistry::new();

        let token = registry.spawn_tokio_task("test-task".to_string(), |cancel_token| async move {
            while !cancel_token.is_cancelled() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stats = registry.stats();
        assert!(stats.total_tokio_tasks >= 1);
    }
}
