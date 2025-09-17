use crate::core::error::{Error, Result};
use std::fs::{File, Metadata, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Centralized file operations with error handling and retry logic
pub struct FileOps;

impl FileOps {
    /// Open a file for reading with retry logic
    pub fn open_for_read<P: AsRef<Path>>(path: P) -> Result<File> {
        let path = path.as_ref();
        Self::retry_operation(|| {
            File::open(path).map_err(|e| Error::Io(format!("open_read on {}: {}", path.display(), e)))
        })
    }

    /// Open a file for writing with retry logic
    pub fn open_for_write<P: AsRef<Path>>(path: P) -> Result<File> {
        let path = path.as_ref();
        Self::retry_operation(|| {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .map_err(|e| Error::Io(format!("open_write on {}: {}", path.display(), e)))
        })
    }

    /// Open a file for appending with retry logic
    pub fn open_for_append<P: AsRef<Path>>(path: P) -> Result<File> {
        let path = path.as_ref();
        Self::retry_operation(|| {
            OpenOptions::new()
                
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| Error::Io(format!("open_append on {}: {}", path.display(), e)))
        })
    }

    /// Open a file for read/write with retry logic
    pub fn open_for_read_write<P: AsRef<Path>>(path: P) -> Result<File> {
        let path = path.as_ref();
        Self::retry_operation(|| {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(|e| Error::Io(format!("open_read_write on {}: {}", path.display(), e)))
        })
    }

    /// Read entire file contents into a vector
    pub fn read_to_vec<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
        let path = path.as_ref();
        Self::retry_operation(|| {
            std::fs::read(path).map_err(|e| Error::Io(format!("read_to_vec on {}: {}", path.display(), e)))
        })
    }

    /// Read entire file contents into a string
    pub fn read_to_string<P: AsRef<Path>>(path: P) -> Result<String> {
        let path = path.as_ref();
        Self::retry_operation(|| {
            std::fs::read_to_string(path).map_err(|e| Error::Io(format!("read_to_string on {}: {}", path.display(), e)))
        })
    }

    /// Write data to a file atomically (write to temp file, then rename)
    pub fn write_atomic<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
        let path = path.as_ref();
        let temp_path = Self::get_temp_path(path)?;

        // Write to temporary file first
        Self::retry_operation(|| {
            std::fs::write(&temp_path, data).map_err(|e| Error::Io(format!("write_temp on {}: {}", temp_path.display(), e)))
        })?;

        // Atomic rename
        Self::retry_operation(|| {
            std::fs::rename(&temp_path, path).map_err(|e| Error::Io(format!("atomic_rename on {}: {}", path.display(), e)))
        })
    }

    /// Copy a file with error handling and retry
    pub fn copy_file<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> Result<u64> {
        let from = from.as_ref();
        let to = to.as_ref();
        Self::retry_operation(|| {
            std::fs::copy(from, to).map_err(|e| Error::Io(format!("copy_to_{} on {}: {}", to.display(), from.display(), e)))
        })
    }

    /// Create directory and all parent directories
    pub fn create_dir_all<P: AsRef<Path>>(path: P) -> Result<()> {
        let path = path.as_ref();
        Self::retry_operation(|| {
            std::fs::create_dir_all(path).map_err(|e| Error::Io(format!("create_dir_all on {}: {}", path.display(), e)))
        })
    }

    /// Remove a file with retry logic
    pub fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
        let path = path.as_ref();
        Self::retry_operation(|| {
            std::fs::remove_file(path).map_err(|e| Error::Io(format!("remove_file on {}: {}", path.display(), e)))
        })
    }

    /// Remove a directory and all its contents
    pub fn remove_dir_all<P: AsRef<Path>>(path: P) -> Result<()> {
        let path = path.as_ref();
        Self::retry_operation(|| {
            std::fs::remove_dir_all(path).map_err(|e| Error::Io(format!("remove_dir_all on {}: {}", path.display(), e)))
        })
    }

    /// Get file metadata with retry logic
    pub fn metadata<P: AsRef<Path>>(path: P) -> Result<Metadata> {
        let path = path.as_ref();
        Self::retry_operation(|| {
            std::fs::metadata(path).map_err(|e| Error::Io(format!("metadata on {}: {}", path.display(), e)))
        })
    }

    /// Check if a path exists
    pub fn exists<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().exists()
    }

    /// Check if a path is a file
    pub fn is_file<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().is_file()
    }

    /// Check if a path is a directory
    pub fn is_dir<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().is_dir()
    }

    /// Get file size
    pub fn file_size<P: AsRef<Path>>(path: P) -> Result<u64> {
        let metadata = Self::metadata(path)?;
        Ok(metadata.len())
    }

    /// Sync file data to disk
    pub fn sync_data(file: &mut File) -> Result<()> {
        Self::retry_operation(|| {
            file.sync_data().map_err(|e| Error::Io(format!("sync_data on <unknown>: {}", e)))
        })
    }

    /// Sync file data and metadata to disk
    pub fn sync_all(file: &mut File) -> Result<()> {
        Self::retry_operation(|| {
            file.sync_all().map_err(|e| Error::Io(format!("sync_all on <unknown>: {}", e)))
        })
    }

    /// Read from a specific position in a file
    pub fn read_at<P: AsRef<Path>>(path: P, offset: u64, length: usize) -> Result<Vec<u8>> {
        let mut file = Self::open_for_read(path)?;
        Self::seek(&mut file, SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; length];
        Self::read_exact(&mut file, &mut buffer)?;
        Ok(buffer)
    }

    /// Write to a specific position in a file
    pub fn write_at<P: AsRef<Path>>(path: P, offset: u64, data: &[u8]) -> Result<()> {
        let mut file = Self::open_for_read_write(path)?;
        Self::seek(&mut file, SeekFrom::Start(offset))?;
        Self::write_all(&mut file, data)?;
        Self::sync_data(&mut file)
    }

    /// Seek to a position in a file
    pub fn seek(file: &mut File, pos: SeekFrom) -> Result<u64> {
        Self::retry_operation(|| {
            file.seek(pos).map_err(|e| Error::Io(format!("seek on <unknown>: {}", e)))
        })
    }

    /// Read exact number of bytes from a file
    pub fn read_exact(file: &mut File, buf: &mut [u8]) -> Result<()> {
        Self::retry_operation(|| {
            file.read_exact(buf).map_err(|e| Error::Io(format!("read_exact on <unknown>: {}", e)))
        })
    }

    /// Write all bytes to a file
    pub fn write_all(file: &mut File, buf: &[u8]) -> Result<()> {
        Self::retry_operation(|| {
            file.write_all(buf).map_err(|e| Error::Io(format!("write_all on <unknown>: {}", e)))
        })
    }

    /// Create a buffered reader
    pub fn buffered_reader(file: File) -> BufReader<File> {
        BufReader::new(file)
    }

    /// Create a buffered writer
    pub fn buffered_writer(file: File) -> BufWriter<File> {
        BufWriter::new(file)
    }

    /// Get a temporary file path for atomic operations
    fn get_temp_path(original: &Path) -> Result<PathBuf> {
        let mut temp_path = original.to_path_buf();
        let mut file_name = original
            .file_name()
            .ok_or_else(|| Error::InvalidArgument("Invalid file path".to_string()))?
            .to_os_string();

        file_name.push(".tmp");
        temp_path.set_file_name(file_name);
        Ok(temp_path)
    }

    /// Retry an operation with exponential backoff
    fn retry_operation<T, F>(mut operation: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        const MAX_RETRIES: u32 = 3;
        const BASE_DELAY: Duration = Duration::from_millis(10);

        let mut delay = BASE_DELAY;
        let mut last_error = None;

        for attempt in 0..=MAX_RETRIES {
            match operation() {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);

                    if attempt < MAX_RETRIES {
                        std::thread::sleep(delay);
                        delay *= 2; // Exponential backoff
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }
}

/// Configuration for file operations
#[derive(Debug, Clone)]
pub struct FileOpConfig {
    /// Enable atomic writes
    pub atomic_writes: bool,
    /// Buffer size for I/O operations
    pub buffer_size: usize,
    /// Number of retry attempts
    pub max_retries: u32,
    /// Base retry delay
    pub retry_delay: Duration,
    /// Enable sync after write
    pub sync_after_write: bool,
}

impl Default for FileOpConfig {
    fn default() -> Self {
        Self {
            atomic_writes: true,
            buffer_size: 64 * 1024, // 64KB
            max_retries: 3,
            retry_delay: Duration::from_millis(10),
            sync_after_write: true,
        }
    }
}

/// Advanced file operations with configuration
pub struct ConfigurableFileOps {
    config: FileOpConfig,
}

impl ConfigurableFileOps {
    pub fn new(config: FileOpConfig) -> Self {
        Self { config }
    }

    /// Write data with configuration options
    pub fn write<P: AsRef<Path>>(&self, path: P, data: &[u8]) -> Result<()> {
        let path = path.as_ref();

        if self.config.atomic_writes {
            self.write_atomic(path, data)
        } else {
            self.write_direct(path, data)
        }
    }

    /// Write data atomically
    fn write_atomic(&self, path: &Path, data: &[u8]) -> Result<()> {
        let temp_path = FileOps::get_temp_path(path)?;

        // Write to temporary file
        let mut file = FileOps::open_for_write(&temp_path)?;
        FileOps::write_all(&mut file, data)?;

        if self.config.sync_after_write {
            FileOps::sync_data(&mut file)?;
        }

        drop(file); // Close the file before rename

        // Atomic rename
        FileOps::retry_operation(|| {
            std::fs::rename(&temp_path, path).map_err(|e| Error::Io(format!("atomic_rename on {}: {}", path.display(), e)))
        })
    }

    /// Write data directly
    fn write_direct(&self, path: &Path, data: &[u8]) -> Result<()> {
        let mut file = FileOps::open_for_write(path)?;
        FileOps::write_all(&mut file, data)?;

        if self.config.sync_after_write {
            FileOps::sync_data(&mut file)?;
        }

        Ok(())
    }

    /// Read with buffering
    pub fn read_buffered<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>> {
        let file = FileOps::open_for_read(path)?;
        let mut reader = BufReader::with_capacity(self.config.buffer_size, file);
        let mut buffer = Vec::new();

        reader
            .read_to_end(&mut buffer)
            .map_err(|e| Error::Io(format!("read_buffered on <buffered_read>: {}", e)))?;

        Ok(buffer)
    }

    /// Copy file with progress callback
    pub fn copy_with_progress<P: AsRef<Path>, Q: AsRef<Path>, F>(
        &self,
        from: P,
        to: Q,
        mut progress_callback: F,
    ) -> Result<u64>
    where
        F: FnMut(u64, u64),
    {
        let from = from.as_ref();
        let to = to.as_ref();

        let total_size = FileOps::file_size(from)?;
        let mut src = FileOps::open_for_read(from)?;
        let mut dst = FileOps::open_for_write(to)?;

        let mut buffer = vec![0u8; self.config.buffer_size];
        let mut copied = 0u64;

        loop {
            let bytes_read = src.read(&mut buffer).map_err(|e| Error::Io(format!("copy_read on {}: {}", from.display(), e)))?;

            if bytes_read == 0 {
                break;
            }

            dst.write_all(&buffer[..bytes_read])
                .map_err(|e| Error::Io(format!("copy_write on {}: {}", to.display(), e)))?;

            copied += bytes_read as u64;
            progress_callback(copied, total_size);
        }

        if self.config.sync_after_write {
            FileOps::sync_data(&mut dst)?;
        }

        Ok(copied)
    }
}

/// File operation statistics
#[derive(Debug, Clone, Default)]
pub struct FileOpStats {
    pub reads: u64,
    pub writes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub errors: u64,
    pub retries: u64,
    pub total_read_time: Duration,
    pub total_write_time: Duration,
}

/// File operations with statistics tracking
pub struct StatisticsFileOps {
    stats: std::sync::Arc<std::sync::Mutex<FileOpStats>>,
    config: FileOpConfig,
}

impl StatisticsFileOps {
    pub fn new(config: FileOpConfig) -> Self {
        Self {
            stats: std::sync::Arc::new(std::sync::Mutex::new(FileOpStats::default())),
            config,
        }
    }

    pub fn get_stats(&self) -> FileOpStats {
        self.stats.lock().unwrap().clone()
    }

    pub fn reset_stats(&self) {
        *self.stats.lock().unwrap() = FileOpStats::default();
    }

    /// Read with statistics tracking
    pub fn read<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>> {
        let start = Instant::now();

        match FileOps::read_to_vec(path) {
            Ok(data) => {
                let mut stats = self.stats.lock().unwrap();
                stats.reads += 1;
                stats.bytes_read += data.len() as u64;
                stats.total_read_time += start.elapsed();
                Ok(data)
            }
            Err(e) => {
                let mut stats = self.stats.lock().unwrap();
                stats.errors += 1;
                Err(e)
            }
        }
    }

    /// Write with statistics tracking
    pub fn write<P: AsRef<Path>>(&self, path: P, data: &[u8]) -> Result<()> {
        let start = Instant::now();

        let configurable = ConfigurableFileOps::new(self.config.clone());
        match configurable.write(path, data) {
            Ok(()) => {
                let mut stats = self.stats.lock().unwrap();
                stats.writes += 1;
                stats.bytes_written += data.len() as u64;
                stats.total_write_time += start.elapsed();
                Ok(())
            }
            Err(e) => {
                let mut stats = self.stats.lock().unwrap();
                stats.errors += 1;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_basic_file_operations() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        let test_data = b"Hello, World!";

        // Write and read
        FileOps::write_atomic(&file_path, test_data).unwrap();
        let read_data = FileOps::read_to_vec(&file_path).unwrap();

        assert_eq!(test_data, read_data.as_slice());
    }

    #[test]
    fn test_atomic_write() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("atomic_test.txt");

        let test_data = b"Atomic write test";

        FileOps::write_atomic(&file_path, test_data).unwrap();
        assert!(file_path.exists());

        let read_data = FileOps::read_to_vec(&file_path).unwrap();
        assert_eq!(test_data, read_data.as_slice());
    }

    #[test]
    fn test_configurable_operations() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("config_test.txt");

        let config = FileOpConfig {
            atomic_writes: true,
            sync_after_write: false,
            ..Default::default()
        };

        let file_ops = ConfigurableFileOps::new(config);
        let test_data = b"Configurable test data";

        file_ops.write(&file_path, test_data).unwrap();
        let read_data = file_ops.read_buffered(&file_path).unwrap();

        assert_eq!(test_data, read_data.as_slice());
    }

    #[test]
    fn test_statistics_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("stats_test.txt");

        let config = FileOpConfig::default();
        let stats_ops = StatisticsFileOps::new(config);

        let test_data = b"Statistics test data";

        stats_ops.write(&file_path, test_data).unwrap();
        let _read_data = stats_ops.read(&file_path).unwrap();

        let stats = stats_ops.get_stats();
        assert_eq!(stats.reads, 1);
        assert_eq!(stats.writes, 1);
        assert_eq!(stats.bytes_read, test_data.len() as u64);
        assert_eq!(stats.bytes_written, test_data.len() as u64);
        assert_eq!(stats.errors, 0);
    }
}
