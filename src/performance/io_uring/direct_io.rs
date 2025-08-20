//! Direct I/O Support
//!
//! This module provides direct I/O operations that bypass the kernel page cache
//! for maximum performance and predictable latency.

use super::zero_copy_buffer::{AlignedBuffer, BufferAlignment};
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Result};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

// O_DIRECT flag for Linux
#[cfg(target_os = "linux")]
const O_DIRECT: i32 = 0x4000;

#[cfg(not(target_os = "linux"))]
const O_DIRECT: i32 = 0; // No-op on non-Linux

/// Direct I/O file handle with optimized buffering
pub struct DirectIoFile {
    file: File,
    fd: RawFd,
    alignment: BufferAlignment,
    block_size: usize,
    optimal_io_size: usize,
    buffer_pool: Arc<super::zero_copy_buffer::HighPerformanceBufferManager>,
    stats: DirectIoStats,
}

#[derive(Debug, Default)]
pub struct DirectIoStats {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub alignment_corrections: AtomicU64,
    pub buffer_reuses: AtomicU64,
}

impl DirectIoFile {
    /// Open a file for direct I/O
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(path)?;

        let fd = file.as_raw_fd();
        let block_size = Self::detect_block_size(fd)?;
        let optimal_io_size = Self::detect_optimal_io_size(fd, block_size);

        Ok(DirectIoFile {
            file,
            fd,
            alignment: BufferAlignment::DirectIo,
            block_size,
            optimal_io_size,
            buffer_pool: Arc::new(super::zero_copy_buffer::HighPerformanceBufferManager::new()),
            stats: DirectIoStats::default(),
        })
    }

    /// Open a file for read-only direct I/O
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(O_DIRECT)
            .open(path)?;

        let fd = file.as_raw_fd();
        let block_size = Self::detect_block_size(fd)?;
        let optimal_io_size = Self::detect_optimal_io_size(fd, block_size);

        Ok(DirectIoFile {
            file,
            fd,
            alignment: BufferAlignment::DirectIo,
            block_size,
            optimal_io_size,
            buffer_pool: Arc::new(super::zero_copy_buffer::HighPerformanceBufferManager::new()),
            stats: DirectIoStats::default(),
        })
    }

    /// Create a new file for direct I/O
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(O_DIRECT)
            .open(path)?;

        let fd = file.as_raw_fd();
        let block_size = Self::detect_block_size(fd)?;
        let optimal_io_size = Self::detect_optimal_io_size(fd, block_size);

        Ok(DirectIoFile {
            file,
            fd,
            alignment: BufferAlignment::DirectIo,
            block_size,
            optimal_io_size,
            buffer_pool: Arc::new(super::zero_copy_buffer::HighPerformanceBufferManager::new()),
            stats: DirectIoStats::default(),
        })
    }

    /// Detect the block size of the underlying device
    fn detect_block_size(fd: RawFd) -> Result<usize> {
        #[cfg(target_os = "linux")]
        {
            use std::os::raw::c_int;

            // BLKSSZGET ioctl to get logical block size
            const BLKSSZGET: c_int = 0x1268;

            let mut block_size: c_int = 0;
            let result = unsafe { libc::ioctl(fd, BLKSSZGET as libc::c_ulong, &mut block_size) };

            if result == 0 && block_size > 0 {
                Ok(block_size as usize)
            } else {
                Ok(512) // Default to 512 bytes
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = fd; // Avoid unused variable warning
            Ok(512) // Default block size
        }
    }
    
    /// Detect optimal I/O size for best performance
    fn detect_optimal_io_size(fd: RawFd, block_size: usize) -> usize {
        #[cfg(target_os = "linux")]
        {
            use std::os::raw::c_int;
            
            // BLKIOOPT ioctl to get optimal I/O size
            const BLKIOOPT: c_int = 0x1279;
            
            let mut optimal_size: c_int = 0;
            let result = unsafe { libc::ioctl(fd, BLKIOOPT as libc::c_ulong, &mut optimal_size) };
            
            if result == 0 && optimal_size > 0 {
                (optimal_size as usize).max(block_size)
            } else {
                // Fallback to reasonable defaults based on block size
                match block_size {
                    512 => 64 * 1024,      // 64KB for traditional HDDs
                    4096 => 256 * 1024,    // 256KB for modern SSDs
                    _ => 128 * 1024,       // 128KB default
                }
            }
        }
        
        #[cfg(not(target_os = "linux"))]
        {
            let _ = fd;
            match block_size {
                512 => 64 * 1024,
                4096 => 256 * 1024,
                _ => 128 * 1024,
            }
        }
    }

    /// Get the file descriptor
    pub fn fd(&self) -> RawFd {
        self.fd
    }

    /// Get the required alignment for this file
    pub fn alignment(&self) -> BufferAlignment {
        self.alignment
    }

    /// Get the block size
    pub fn block_size(&self) -> usize {
        self.block_size
    }
    
    /// Get the optimal I/O size
    pub fn optimal_io_size(&self) -> usize {
        self.optimal_io_size
    }
    
    /// Get statistics
    pub fn stats(&self) -> &DirectIoStats {
        &self.stats
    }

    /// Check if an offset and length are properly aligned
    pub fn is_aligned(&self, offset: u64, len: usize) -> bool {
        offset % self.block_size as u64 == 0 && len % self.block_size == 0
    }

    /// Align an offset to block boundary
    pub fn align_offset(&self, offset: u64) -> u64 {
        let block_size = self.block_size as u64;
        (offset / block_size) * block_size
    }

    /// Align a length to block boundary
    pub fn align_length(&self, len: usize) -> usize {
        let block_size = self.block_size;
        ((len + block_size - 1) / block_size) * block_size
    }

    /// Create an aligned buffer suitable for direct I/O with this file
    pub fn create_buffer(&self, size: usize) -> Result<AlignedBuffer> {
        let aligned_size = self.align_length(size);
        AlignedBuffer::new(aligned_size, self.alignment)
    }

    /// Perform a direct I/O read
    pub fn read_direct(&self, offset: u64, buf: &mut AlignedBuffer) -> Result<usize> {
        if !self.is_aligned(offset, buf.len()) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Offset or length not aligned for direct I/O",
            ));
        }

        if !buf.is_directio_aligned() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Buffer not aligned for direct I/O",
            ));
        }

        // Use pread for positioned read
        #[cfg(target_os = "linux")]
        {
            let result = unsafe {
                libc::pread(
                    self.fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                    offset as libc::off_t,
                )
            };

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(result as usize)
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            // Fallback to regular file operations
            use std::io::{Read, Seek, SeekFrom};
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(buf.as_mut_slice()).map(|_| buf.len())
        }
    }

    /// Perform a direct I/O write
    pub fn write_direct(&self, offset: u64, buf: &AlignedBuffer) -> Result<usize> {
        if !self.is_aligned(offset, buf.len()) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Offset or length not aligned for direct I/O",
            ));
        }

        if !buf.is_directio_aligned() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Buffer not aligned for direct I/O",
            ));
        }

        // Use pwrite for positioned write
        #[cfg(target_os = "linux")]
        {
            let result = unsafe {
                libc::pwrite(
                    self.fd,
                    buf.as_ptr() as *const libc::c_void,
                    buf.len(),
                    offset as libc::off_t,
                )
            };

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(result as usize)
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            // Fallback to regular file operations
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(buf.as_slice()).map(|_| buf.len())
        }
    }

    /// Sync data to disk
    pub fn sync_data(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            let result = unsafe { libc::fdatasync(self.fd) };

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(())
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.file.sync_data()
        }
    }

    /// Sync data and metadata to disk
    pub fn sync_all(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            let result = unsafe { libc::fsync(self.fd) };

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(())
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.file.sync_all()
        }
    }

    /// Pre-allocate space for the file
    pub fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            let result =
                unsafe { libc::fallocate(self.fd, 0, offset as libc::off_t, len as libc::off_t) };

            if result < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(())
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            // Fallback: seek and write
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset + len - 1))?;
            file.write_all(&[0])?;
            Ok(())
        }
    }

    /// Advise kernel about access pattern
    pub fn advise(&self, _offset: u64, _len: u64, _advice: PosixAdvice) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            let result = unsafe {
                libc::posix_fadvise(
                    self.fd,
                    offset as libc::off_t,
                    len as libc::off_t,
                    advice as libc::c_int,
                )
            };

            if result != 0 {
                Err(Error::from_raw_os_error(result))
            } else {
                Ok(())
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            // No-op on non-Linux platforms
            Ok(())
        }
    }
}

/// POSIX fadvise flags
#[derive(Debug, Clone, Copy)]
#[repr(i32)]
pub enum PosixAdvice {
    Normal = 0,
    Random = 1,
    Sequential = 2,
    WillNeed = 3,
    DontNeed = 4,
    NoReuse = 5,
}

/// Direct I/O helper for aligned reads
pub struct DirectIoReader {
    file: Arc<DirectIoFile>,
    position: u64,
    buffer_pool: super::zero_copy_buffer::BufferPool,
}

impl DirectIoReader {
    pub fn new(file: Arc<DirectIoFile>) -> Self {
        Self {
            file,
            position: 0,
            buffer_pool: super::zero_copy_buffer::BufferPool::new(),
        }
    }

    /// Read data handling alignment automatically
    pub fn read(&mut self, len: usize) -> Result<Vec<u8>> {
        let _block_size = self.file.block_size();

        // Calculate aligned read parameters
        let aligned_offset = self.file.align_offset(self.position);
        let offset_delta = (self.position - aligned_offset) as usize;
        let aligned_len = self.file.align_length(len + offset_delta);

        // Get aligned buffer from pool
        let mut buffer = self
            .buffer_pool
            .acquire(aligned_len, self.file.alignment())?;

        // Perform direct I/O read
        let bytes_read = self.file.read_direct(aligned_offset, &mut buffer)?;

        if bytes_read < offset_delta {
            return Ok(Vec::new()); // EOF
        }

        // Extract the requested data
        let available = bytes_read - offset_delta;
        let to_copy = std::cmp::min(len, available);
        let result = buffer[offset_delta..offset_delta + to_copy].to_vec();

        // Update position
        self.position += to_copy as u64;

        // Return buffer to pool
        self.buffer_pool.release(buffer);

        Ok(result)
    }

    /// Seek to a position
    pub fn seek(&mut self, pos: u64) {
        self.position = pos;
    }

    /// Get current position
    pub fn position(&self) -> u64 {
        self.position
    }
}

/// Direct I/O helper for aligned writes
pub struct DirectIoWriter {
    file: Arc<DirectIoFile>,
    position: u64,
    write_buffer: AlignedBuffer,
    buffer_used: usize,
}

impl DirectIoWriter {
    pub fn new(file: Arc<DirectIoFile>) -> Result<Self> {
        let buffer = file.create_buffer(1024 * 1024)?; // 1MB write buffer

        Ok(Self {
            file,
            position: 0,
            write_buffer: buffer,
            buffer_used: 0,
        })
    }

    /// Write data handling alignment and buffering
    pub fn write(&mut self, data: &[u8]) -> Result<usize> {
        let mut written = 0;
        let mut remaining = data;

        while !remaining.is_empty() {
            let available = self.write_buffer.len() - self.buffer_used;
            let to_copy = std::cmp::min(available, remaining.len());

            // Copy to buffer
            self.write_buffer[self.buffer_used..self.buffer_used + to_copy]
                .copy_from_slice(&remaining[..to_copy]);

            self.buffer_used += to_copy;
            written += to_copy;
            remaining = &remaining[to_copy..];

            // Flush if buffer is full
            if self.buffer_used == self.write_buffer.len() {
                self.flush_buffer()?;
            }
        }

        Ok(written)
    }

    /// Flush buffered data
    pub fn flush(&mut self) -> Result<()> {
        if self.buffer_used > 0 {
            self.flush_buffer()?;
        }
        Ok(())
    }

    /// Sync data to disk
    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()
    }

    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer_used == 0 {
            return Ok(());
        }

        let block_size = self.file.block_size();

        // For partial block at end, we need to read-modify-write
        if self.buffer_used % block_size != 0 {
            let aligned_len = self.file.align_length(self.buffer_used);

            // Zero padding
            self.write_buffer[self.buffer_used..aligned_len].fill(0);

            // Write aligned data
            let written = self.file.write_direct(self.position, &self.write_buffer)?;

            if written < self.buffer_used {
                return Err(Error::new(ErrorKind::WriteZero, "Partial write"));
            }
        } else {
            // Buffer is already aligned
            let mut temp_buffer = AlignedBuffer::new(self.buffer_used, self.file.alignment())?;
            temp_buffer[..self.buffer_used].copy_from_slice(&self.write_buffer[..self.buffer_used]);

            let written = self.file.write_direct(self.position, &temp_buffer)?;

            if written < self.buffer_used {
                return Err(Error::new(ErrorKind::WriteZero, "Partial write"));
            }
        }

        self.position += self.buffer_used as u64;
        self.buffer_used = 0;

        Ok(())
    }
}

impl Drop for DirectIoWriter {
    fn drop(&mut self) {
        // Best effort flush on drop
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_direct_io_file_creation() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = DirectIoFile::create(temp_file.path());

        // Direct I/O may not be supported on all filesystems
        if let Ok(file) = file {
            assert!(file.block_size() > 0);
            assert_eq!(file.alignment(), BufferAlignment::DirectIo);
        }
    }

    #[test]
    fn test_alignment_helpers() {
        let temp_file = NamedTempFile::new().unwrap();

        if let Ok(file) = DirectIoFile::create(temp_file.path()) {
            let block_size = file.block_size();

            // Test offset alignment
            assert_eq!(file.align_offset(0), 0);
            assert_eq!(file.align_offset(block_size as u64), block_size as u64);
            assert_eq!(file.align_offset(100), 0); // Assuming block_size > 100

            // Test length alignment
            assert_eq!(file.align_length(0), 0);
            assert_eq!(file.align_length(block_size), block_size);
            assert_eq!(file.align_length(100), block_size); // Rounds up

            // Test alignment check
            assert!(file.is_aligned(0, block_size));
            assert!(!file.is_aligned(1, block_size));
            assert!(!file.is_aligned(0, 100));
        }
    }

    #[test]
    fn test_direct_io_read_write() {
        let temp_file = NamedTempFile::new().unwrap();

        if let Ok(file) = DirectIoFile::create(temp_file.path()) {
            let file = Arc::new(file);

            // Test write
            let mut writer = DirectIoWriter::new(Arc::clone(&file)).unwrap();
            let test_data = vec![0x42u8; 8192]; // 8KB of data
            writer.write(&test_data).unwrap();
            writer.flush().unwrap();
            writer.sync().unwrap();

            // Test read
            let mut reader = DirectIoReader::new(Arc::clone(&file));
            let read_data = reader.read(8192).unwrap();

            assert_eq!(read_data.len(), 8192);
            assert_eq!(read_data, test_data);
        }
    }

    #[test]
    fn test_buffer_creation() {
        let temp_file = NamedTempFile::new().unwrap();

        if let Ok(file) = DirectIoFile::create(temp_file.path()) {
            let buffer = file.create_buffer(1000).unwrap();

            assert!(buffer.is_directio_aligned());
            assert!(buffer.len() >= 1000);
            assert_eq!(buffer.len() % file.block_size(), 0);
        }
    }
}
