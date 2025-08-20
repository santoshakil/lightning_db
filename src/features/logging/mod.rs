use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, trace, warn, Level};
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::{fmt, prelude::*, EnvFilter, Registry};

/// Initialize production logging with configurable settings
pub fn init_logging(level: Level, json_output: bool) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("lightning_db={},warn", level)));

    if json_output {
        // JSON format for production environments
        let fmt_layer = fmt::layer()
            .json()
            .with_timer(SystemTime)
            .with_target(true)
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(true);

        Registry::default().with(env_filter).with(fmt_layer).init();
    } else {
        // Human-readable format for development
        let fmt_layer = fmt::layer()
            .with_timer(SystemTime)
            .with_target(true)
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(false)
            .with_thread_names(true);

        Registry::default().with(env_filter).with(fmt_layer).init();
    }
}

/// Structured logging macros for database operations
#[macro_export]
macro_rules! log_operation {
    ($level:expr, $op:expr, $key:expr, $duration:expr, $result:expr) => {
        match $level {
            tracing::Level::TRACE => tracing::trace!(
                operation = $op,
                key = ?$key,
                duration_us = $duration.as_micros() as u64,
                result = ?$result,
                "Database operation completed"
            ),
            tracing::Level::DEBUG => tracing::debug!(
                operation = $op,
                key = ?$key,
                duration_us = $duration.as_micros() as u64,
                result = ?$result,
                "Database operation completed"
            ),
            _ => {}
        }
    };
}

#[macro_export]
macro_rules! log_transaction {
    ($event:expr, $tx_id:expr, $duration:expr) => {
        tracing::info!(
            event = $event,
            tx_id = $tx_id,
            duration_ms = $duration.as_millis() as u64,
            "Transaction event"
        )
    };
}

#[macro_export]
macro_rules! log_cache_event {
    ($event:expr, $key:expr, $hit:expr) => {
        tracing::debug!(
            event = $event,
            key = ?$key,
            cache_hit = $hit,
            "Cache event"
        )
    };
}

#[macro_export]
macro_rules! log_compaction {
    ($level:expr, $files_before:expr, $files_after:expr, $duration:expr) => {
        tracing::info!(
            level = $level,
            files_before = $files_before,
            files_after = $files_after,
            duration_ms = $duration.as_millis() as u64,
            "Compaction completed"
        )
    };
}

#[macro_export]
macro_rules! log_recovery {
    ($phase:expr, $progress:expr, $total:expr) => {
        tracing::info!(
            phase = $phase,
            progress = $progress,
            total = $total,
            percent = ($progress as f64 / $total as f64 * 100.0) as u32,
            "Recovery progress"
        )
    };
}

/// Operation timing helper
pub struct OperationTimer {
    start: Instant,
    operation: &'static str,
    key: Option<Vec<u8>>,
}

impl OperationTimer {
    pub fn new(operation: &'static str) -> Self {
        Self {
            start: Instant::now(),
            operation,
            key: None,
        }
    }

    pub fn with_key(operation: &'static str, key: &[u8]) -> Self {
        Self {
            start: Instant::now(),
            operation,
            key: Some(key.to_vec()),
        }
    }

    pub fn complete<T>(self, result: &Result<T, crate::core::error::Error>) {
        let duration = self.start.elapsed();

        match result {
            Ok(_) => {
                if duration > Duration::from_millis(100) {
                    warn!(
                        operation = self.operation,
                        key = ?self.key,
                        duration_ms = duration.as_millis() as u64,
                        "Slow operation detected"
                    );
                } else {
                    trace!(
                        operation = self.operation,
                        key = ?self.key,
                        duration_us = duration.as_micros() as u64,
                        "Operation completed"
                    );
                }
            }
            Err(e) => {
                error!(
                    operation = self.operation,
                    key = ?self.key,
                    duration_us = duration.as_micros() as u64,
                    error = %e,
                    "Operation failed"
                );
            }
        }
    }
}

/// Background monitoring task
pub struct DatabaseMonitor {
    db_path: String,
    interval: Duration,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl DatabaseMonitor {
    pub fn new(db_path: String, interval: Duration) -> Self {
        Self {
            db_path,
            interval,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn start(self) -> Arc<std::sync::atomic::AtomicBool> {
        let shutdown = Arc::clone(&self.shutdown);

        std::thread::spawn(move || {
            while !self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                self.collect_metrics();
                std::thread::sleep(self.interval);
            }
        });

        shutdown
    }

    fn collect_metrics(&self) {
        // Collect and log system metrics
        if let Ok(metadata) = std::fs::metadata(&self.db_path) {
            if let Ok(dir_size) = calculate_dir_size(&self.db_path) {
                info!(
                    db_path = %self.db_path,
                    size_mb = dir_size / 1024 / 1024,
                    modified = ?metadata.modified().ok(),
                    "Database metrics"
                );
            }
        }

        // Log memory usage if available
        #[cfg(target_os = "linux")]
        {
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                for line in status.lines() {
                    if line.starts_with("VmRSS:") {
                        if let Some(rss) = line.split_whitespace().nth(1) {
                            if let Ok(rss_kb) = rss.parse::<u64>() {
                                info!(memory_rss_mb = rss_kb / 1024, "Process memory usage");
                            }
                        }
                    }
                }
            }
        }
    }
}

fn calculate_dir_size(path: &str) -> std::io::Result<u64> {
    let mut total_size = 0;

    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;

        if metadata.is_file() {
            total_size += metadata.len();
        } else if metadata.is_dir() {
            if let Ok(size) = calculate_dir_size(&entry.path().to_string_lossy()) {
                total_size += size;
            }
        }
    }

    Ok(total_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_timer() {
        let timer = OperationTimer::with_key("test_op", b"test_key");
        std::thread::sleep(Duration::from_millis(10));
        timer.complete::<()>(&Ok(()));
    }

    #[test]
    fn test_logging_macros() {
        log_operation!(
            Level::DEBUG,
            "put",
            b"key1",
            Duration::from_micros(100),
            Ok::<(), String>(())
        );
        log_transaction!("commit", 123, Duration::from_millis(50));
        log_cache_event!("get", b"key1", true);
        log_compaction!(0, 10, 3, Duration::from_secs(2));
        log_recovery!("replay", 100, 1000);
    }
}
