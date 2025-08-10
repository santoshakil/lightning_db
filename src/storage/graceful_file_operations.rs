/// Graceful file operations with automatic read-only fallback
///
/// This module provides file operations that gracefully degrade to read-only mode
/// when write permissions are not available, rather than failing completely.
use crate::error::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, Write};
use std::path::Path;

/// File wrapper that tracks whether it's opened in read-only mode
#[derive(Debug)]
pub struct GracefulFile {
    file: File,
    is_read_only: bool,
}

/// Result of opening a file with graceful degradation
#[derive(Debug)]
pub struct FileOpenResult {
    pub file: GracefulFile,
    pub is_read_only: bool,
    pub degraded: bool, // True if we fell back to read-only due to permissions
}

impl GracefulFile {
    /// Try to open a file with graceful degradation to read-only mode
    pub fn open_with_fallback<P: AsRef<Path>>(path: P) -> Result<FileOpenResult> {
        let path = path.as_ref();

        // First, try to open in read-write mode
        match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => Ok(FileOpenResult {
                file: GracefulFile {
                    file,
                    is_read_only: false,
                },
                is_read_only: false,
                degraded: false,
            }),
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                // Permission denied - try read-only fallback
                eprintln!(
                    "Permission denied for read-write access to {:?}, attempting read-only fallback",
                    path
                );

                match OpenOptions::new()
                    .read(true)
                    .open(path)
                {
                    Ok(file) => {
                        eprintln!("Successfully opened {:?} in read-only mode", path);
                        Ok(FileOpenResult {
                            file: GracefulFile {
                                file,
                                is_read_only: true,
                            },
                            is_read_only: true,
                            degraded: true,
                        })
                    }
                    Err(read_err) => {
                        Err(Error::Io(format!(
                            "Failed to open file {:?} in both read-write and read-only modes: write error: {}, read error: {}",
                            path, e, read_err
                        )))
                    }
                }
            }
            Err(e) => {
                // Other errors (not permission-related)
                Err(Error::Io(format!("Failed to open file {:?}: {}", path, e)))
            }
        }
    }

    /// Try to create a file, with fallback if parent directory is read-only
    pub fn create_with_fallback<P: AsRef<Path>>(path: P) -> Result<FileOpenResult> {
        let path = path.as_ref();

        // Try to create the file
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
        {
            Ok(file) => Ok(FileOpenResult {
                file: GracefulFile {
                    file,
                    is_read_only: false,
                },
                is_read_only: false,
                degraded: false,
            }),
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                // Can't create - try to open existing file in read-only mode
                eprintln!(
                    "Permission denied for creating {:?}, attempting to open existing file in read-only mode",
                    path
                );

                Self::open_with_fallback(path)
            }
            Err(e) => Err(Error::Io(format!(
                "Failed to create file {:?}: {}",
                path, e
            ))),
        }
    }

    /// Check if the file is in read-only mode
    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    /// Attempt a write operation with appropriate error for read-only mode
    pub fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        if self.is_read_only {
            return Err(Error::InvalidOperation {
                reason: "Cannot write to file opened in read-only mode".to_string(),
            });
        }

        self.file.write_all(buf).map_err(|e| {
            if e.kind() == ErrorKind::PermissionDenied {
                Error::InvalidOperation {
                    reason: "Write permission denied".to_string(),
                }
            } else {
                Error::Io(format!("Write failed: {}", e))
            }
        })
    }

    /// Attempt a sync operation with appropriate error for read-only mode
    pub fn sync_all(&self) -> Result<()> {
        if self.is_read_only {
            // Sync is a no-op for read-only files
            return Ok(());
        }

        self.file
            .sync_all()
            .map_err(|e| Error::Io(format!("Sync failed: {}", e)))
    }

    /// Attempt a sync_data operation with appropriate error for read-only mode
    pub fn sync_data(&self) -> Result<()> {
        if self.is_read_only {
            // Sync is a no-op for read-only files
            return Ok(());
        }

        self.file
            .sync_data()
            .map_err(|e| Error::Io(format!("Sync data failed: {}", e)))
    }

    /// Set file length (only works in read-write mode)
    pub fn set_len(&self, size: u64) -> Result<()> {
        if self.is_read_only {
            return Err(Error::InvalidOperation {
                reason: "Cannot resize file opened in read-only mode".to_string(),
            });
        }

        self.file
            .set_len(size)
            .map_err(|e| Error::Io(format!("Failed to set file length: {}", e)))
    }
}

// Implement standard traits for GracefulFile
impl Read for GracefulFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for GracefulFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

/// Helper function to detect if a path is writable
pub fn is_path_writable<P: AsRef<Path>>(path: P) -> bool {
    let path = path.as_ref();

    // Try to create a temporary file in the directory
    if let Some(parent) = path.parent() {
        let temp_name = format!(".lightning_db_write_test_{}", std::process::id());
        let temp_path = parent.join(temp_name);

        match OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)
        {
            Ok(file) => {
                drop(file);
                let _ = std::fs::remove_file(temp_path);
                true
            }
            Err(_) => false,
        }
    } else {
        false
    }
}

/// Helper to detect if a file/directory is in read-only mode
pub fn is_path_read_only<P: AsRef<Path>>(path: P) -> Result<bool> {
    let path = path.as_ref();

    if !path.exists() {
        return Ok(false);
    }

    let metadata = std::fs::metadata(path)
        .map_err(|e| Error::Io(format!("Failed to get metadata for {:?}: {}", path, e)))?;

    Ok(metadata.permissions().readonly())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_normal_file_operations() {
        let temp_dir = TempDir::new()
            .expect("Failed to create temp directory");
        let file_path = temp_dir.path().join("test_file");

        // Create file should work normally
        let result = GracefulFile::create_with_fallback(&file_path)
            .expect("Failed to create file with fallback");
        assert!(!result.is_read_only);
        assert!(!result.degraded);

        // Should be able to write
        let mut file = result.file;
        assert!(file.write_all(b"test data").is_ok());
        assert!(file.sync_all().is_ok());
    }

    #[test]
    #[cfg(unix)]
    fn test_read_only_fallback() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new()
            .expect("Failed to create temp directory");
        let file_path = temp_dir.path().join("test_file");

        // Create file first
        std::fs::write(&file_path, b"existing data")
            .expect("Failed to write test file");

        // Make file read-only
        let mut perms = std::fs::metadata(&file_path)
            .expect("Failed to get file metadata")
            .permissions();
        perms.set_mode(0o444);
        std::fs::set_permissions(&file_path, perms)
            .expect("Failed to set file permissions");

        // Open should fall back to read-only mode
        let result = GracefulFile::open_with_fallback(&file_path)
            .expect("Failed to open file with fallback");
        assert!(result.is_read_only);
        assert!(result.degraded);

        // Writes should be rejected
        let mut file = result.file;
        assert!(file.write_all(b"new data").is_err());

        // But we can still read
        let mut buffer = Vec::new();
        assert!(file.read_to_end(&mut buffer).is_ok());
        assert_eq!(buffer, b"existing data");
    }

    #[test]
    fn test_write_permission_detection() {
        let temp_dir = TempDir::new()
            .expect("Failed to create temp directory for permission test");

        // Should be writable initially
        assert!(is_path_writable(temp_dir.path()));

        // Non-existent path should be non-writable
        assert!(!is_path_writable("/this/path/does/not/exist"));
    }
}
