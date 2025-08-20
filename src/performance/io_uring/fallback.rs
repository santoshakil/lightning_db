//! Fallback I/O Implementation for Non-Linux Systems
//!
//! This module provides a fallback implementation using standard async I/O
//! for platforms that don't support io_uring (macOS, Windows, etc.)

use super::*;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::unix::io::RawFd;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// Define error constants for fallback implementation
const ENOTSUP: i32 = 95; // Operation not supported
const EIO: i32 = 5; // I/O error
const EINVAL: i32 = 22; // Invalid argument

/// Task for async I/O operations
struct IoTask {
    request: IoRequest,
    submit_time: Instant,
}

/// Fallback implementation using tokio async I/O
pub struct FallbackIo {
    queue_depth: u32,
    pending_tasks: Arc<Mutex<VecDeque<IoTask>>>,
    completed: Arc<Mutex<VecDeque<CompletionEntry>>>,
    file_cache: Arc<Mutex<HashMap<RawFd, Arc<Mutex<File>>>>>,
    stats: Arc<Mutex<IoStats>>,
    runtime: tokio::runtime::Handle,
    fixed_buffers: Vec<Vec<u8>>,
    fixed_files: Vec<RawFd>,
}

impl FallbackIo {
    pub fn new(queue_depth: u32) -> Self {
        Self {
            queue_depth,
            pending_tasks: Arc::new(Mutex::new(VecDeque::with_capacity(queue_depth as usize))),
            completed: Arc::new(Mutex::new(VecDeque::with_capacity(
                queue_depth as usize * 2,
            ))),
            file_cache: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(IoStats::new())),
            runtime: tokio::runtime::Handle::current(),
            fixed_buffers: Vec::new(),
            fixed_files: Vec::new(),
        }
    }

    fn process_request(&self, request: IoRequest) {
        let completed = Arc::clone(&self.completed);
        let file_cache = Arc::clone(&self.file_cache);
        let stats = Arc::clone(&self.stats);
        let start_time = Instant::now();

        // Process in a separate thread to avoid blocking
        thread::spawn(move || {
            let result = match request.op_type {
                OpType::Read | OpType::ReadFixed => {
                    Self::handle_read(request, file_cache, stats.clone(), start_time)
                }
                OpType::Write | OpType::WriteFixed => {
                    Self::handle_write(request, file_cache, stats.clone(), start_time)
                }
                OpType::Fsync | OpType::FdataSync => {
                    Self::handle_sync(request, file_cache, stats.clone(), start_time)
                }
                _ => {
                    // Unsupported operation
                    CompletionEntry {
                        user_data: request.user_data,
                        result: -ENOTSUP,
                        flags: 0,
                    }
                }
            };

            if let Ok(mut completions) = completed.lock() {
                completions.push_back(result);
            }
        });
    }

    fn handle_read(
        request: IoRequest,
        file_cache: Arc<Mutex<HashMap<RawFd, Arc<Mutex<File>>>>>,
        stats: Arc<Mutex<IoStats>>,
        start_time: Instant,
    ) -> CompletionEntry {
        let file = match Self::get_or_create_file(request.fd, file_cache) {
            Ok(f) => f,
            Err(e) => {
                return CompletionEntry {
                    user_data: request.user_data,
                    result: -(e.raw_os_error().unwrap_or(EIO)),
                    flags: 0,
                }
            }
        };

        let mut file_guard = match file.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return CompletionEntry {
                    user_data: request.user_data,
                    result: -EIO,
                    flags: 0,
                }
            }
        };

        // Use standard file operations instead of async for the fallback
        // Continue using the locked file guard

        // Seek to position
        if let Err(e) = file_guard.seek(SeekFrom::Start(request.offset)) {
            return CompletionEntry {
                user_data: request.user_data,
                result: -(e.raw_os_error().unwrap_or(EIO)),
                flags: 0,
            };
        }

        // Read data
        let result = match request.buffer {
            Some(IoBuffer::Standard(mut buf)) => {
                match file_guard.read_exact(buf.as_mut_slice()) {
                    Ok(()) => buf.len() as i32,
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        // Partial read
                        match file_guard.read(buf.as_mut_slice()) {
                            Ok(n) => n as i32,
                            Err(e) => -(e.raw_os_error().unwrap_or(EIO)),
                        }
                    }
                    Err(e) => -(e.raw_os_error().unwrap_or(EIO)),
                }
            }
            _ => -EINVAL, // Buffer type not supported in fallback
        };

        let latency_ns = start_time.elapsed().as_nanos() as u64;

        if result > 0 {
            if let Ok(mut stats) = stats.lock() {
                stats.record_read(result as usize, latency_ns);
            }
        }

        CompletionEntry {
            user_data: request.user_data,
            result,
            flags: 0,
        }
    }

    fn handle_write(
        request: IoRequest,
        file_cache: Arc<Mutex<HashMap<RawFd, Arc<Mutex<File>>>>>,
        stats: Arc<Mutex<IoStats>>,
        start_time: Instant,
    ) -> CompletionEntry {
        let file = match Self::get_or_create_file(request.fd, file_cache) {
            Ok(f) => f,
            Err(e) => {
                return CompletionEntry {
                    user_data: request.user_data,
                    result: -(e.raw_os_error().unwrap_or(EIO)),
                    flags: 0,
                }
            }
        };

        let mut file_guard = match file.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return CompletionEntry {
                    user_data: request.user_data,
                    result: -EIO,
                    flags: 0,
                }
            }
        };

        // Seek to position
        if let Err(e) = file_guard.seek(SeekFrom::Start(request.offset)) {
            return CompletionEntry {
                user_data: request.user_data,
                result: -(e.raw_os_error().unwrap_or(EIO)),
                flags: 0,
            };
        }

        // Write data
        let result = match request.buffer {
            Some(IoBuffer::Standard(ref buf)) => match file_guard.write_all(buf.as_slice()) {
                Ok(()) => buf.len() as i32,
                Err(e) => -(e.raw_os_error().unwrap_or(EIO)),
            },
            _ => -EINVAL, // Buffer type not supported in fallback
        };

        let latency_ns = start_time.elapsed().as_nanos() as u64;

        if result > 0 {
            if let Ok(mut stats) = stats.lock() {
                stats.record_write(result as usize, latency_ns);
            }
        }

        CompletionEntry {
            user_data: request.user_data,
            result,
            flags: 0,
        }
    }

    fn handle_sync(
        request: IoRequest,
        file_cache: Arc<Mutex<HashMap<RawFd, Arc<Mutex<File>>>>>,
        _stats: Arc<Mutex<IoStats>>,
        _start_time: Instant,
    ) -> CompletionEntry {
        let file = match Self::get_or_create_file(request.fd, file_cache) {
            Ok(f) => f,
            Err(e) => {
                return CompletionEntry {
                    user_data: request.user_data,
                    result: -(e.raw_os_error().unwrap_or(EIO)),
                    flags: 0,
                }
            }
        };

        let file_guard = match file.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return CompletionEntry {
                    user_data: request.user_data,
                    result: -EIO,
                    flags: 0,
                }
            }
        };

        let result = match file_guard.sync_all() {
            Ok(()) => 0,
            Err(e) => -(e.raw_os_error().unwrap_or(EIO)),
        };

        CompletionEntry {
            user_data: request.user_data,
            result,
            flags: 0,
        }
    }

    fn get_or_create_file(
        fd: RawFd,
        file_cache: Arc<Mutex<HashMap<RawFd, Arc<Mutex<File>>>>>,
    ) -> Result<Arc<Mutex<File>>> {
        let mut cache = file_cache.lock().map_err(|_| Error::new(ErrorKind::Other, "Cache lock poisoned"))?;

        if let Some(file) = cache.get(&fd) {
            Ok(Arc::clone(file))
        } else {
            // Create new file from fd
            use std::os::unix::io::FromRawFd;
            let file = unsafe { File::from_raw_fd(fd) };
            let file = Arc::new(Mutex::new(file));
            cache.insert(fd, Arc::clone(&file));
            Ok(file)
        }
    }
}

impl ZeroCopyIo for FallbackIo {
    fn submit(&mut self, request: IoRequest) -> Result<()> {
        let mut pending = self.pending_tasks.lock().map_err(|_| Error::new(ErrorKind::Other, "Pending tasks lock poisoned"))?;

        if pending.len() >= self.queue_depth as usize {
            if let Ok(mut stats) = self.stats.lock() {
                stats.queue_full_events += 1;
            }
            return Err(Error::new(ErrorKind::WouldBlock, "Queue full"));
        }

        let task = IoTask {
            request: request.clone(),
            submit_time: Instant::now(),
        };

        pending.push_back(task);
        drop(pending); // Release lock

        // Process the request asynchronously
        self.process_request(request);

        Ok(())
    }

    fn submit_batch(&mut self, requests: Vec<IoRequest>) -> Result<usize> {
        let mut submitted = 0;

        for request in requests {
            match self.submit(request) {
                Ok(()) => submitted += 1,
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        Ok(submitted)
    }

    fn wait_completions(
        &mut self,
        min_complete: usize,
        timeout: Option<Duration>,
    ) -> Result<Vec<CompletionEntry>> {
        let start = Instant::now();

        loop {
            let mut completed = self.completed.lock().map_err(|_| Error::new(ErrorKind::Other, "Completed lock poisoned"))?;

            if completed.len() >= min_complete {
                let mut results = Vec::with_capacity(min_complete);
                for _ in 0..min_complete {
                    if let Some(entry) = completed.pop_front() {
                        results.push(entry);
                    }
                }
                return Ok(results);
            }

            drop(completed); // Release lock

            // Check timeout
            if let Some(timeout) = timeout {
                if start.elapsed() >= timeout {
                    let mut completed = self.completed.lock().map_err(|_| Error::new(ErrorKind::Other, "Completed lock poisoned"))?;
                    let results: Vec<_> = completed.drain(..).collect();
                    return Ok(results);
                }
            }

            // Sleep briefly to avoid busy waiting
            std::thread::sleep(Duration::from_micros(100));
        }
    }

    fn register_buffers(&mut self, buffers: &[&[u8]]) -> Result<()> {
        self.fixed_buffers.clear();
        for buffer in buffers {
            self.fixed_buffers.push(buffer.to_vec());
        }
        Ok(())
    }

    fn unregister_buffers(&mut self) -> Result<()> {
        self.fixed_buffers.clear();
        Ok(())
    }

    fn register_files(&mut self, fds: &[RawFd]) -> Result<()> {
        self.fixed_files = fds.to_vec();
        Ok(())
    }

    fn unregister_files(&mut self) -> Result<()> {
        self.fixed_files.clear();
        if let Ok(mut cache) = self.file_cache.lock() {
            cache.clear();
        }
        Ok(())
    }

    fn sq_space_left(&self) -> usize {
        self.pending_tasks.lock()
            .map(|pending| self.queue_depth as usize - pending.len())
            .unwrap_or(0)
    }

    fn cq_ready(&self) -> usize {
        self.completed.lock().map(|c| c.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fallback_io_creation() {
        let io = FallbackIo::new(128);
        assert_eq!(io.sq_space_left(), 128);
        assert_eq!(io.cq_ready(), 0);
    }

    #[test]
    fn test_buffer_registration() {
        let mut io = FallbackIo::new(128);

        let buf1 = vec![0u8; 4096];
        let buf2 = vec![0u8; 8192];
        let buffers: Vec<&[u8]> = vec![&buf1, &buf2];

        assert!(io.register_buffers(&buffers).is_ok());
        assert_eq!(io.fixed_buffers.len(), 2);

        assert!(io.unregister_buffers().is_ok());
        assert_eq!(io.fixed_buffers.len(), 0);
    }

    #[test]
    fn test_file_registration() {
        let mut io = FallbackIo::new(128);

        let fds = vec![1, 2, 3]; // stdin, stdout, stderr

        assert!(io.register_files(&fds).is_ok());
        assert_eq!(io.fixed_files.len(), 3);

        assert!(io.unregister_files().is_ok());
        assert_eq!(io.fixed_files.len(), 0);
    }
}
