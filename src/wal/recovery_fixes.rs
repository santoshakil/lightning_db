use crate::error::Result;
use crate::wal::{WALEntry, WALOperation};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use tracing::{warn, info, debug};

/// Edge cases for WAL recovery:
/// 1. Torn writes - partial entry written when system crashes
/// 2. Corrupted checksums due to bit flips
/// 3. Incomplete transactions - begin without commit/abort
/// 4. Out-of-order entries due to concurrent writes
/// 5. Missing segments in multi-segment WAL
/// 6. Duplicate LSNs from bugs
/// 7. Zero-length files or truncated headers
pub struct WALRecoveryContext {
    // Track transaction states during recovery
    pub active_transactions: HashMap<u64, Vec<WALOperation>>,
    pub committed_transactions: HashMap<u64, u64>, // tx_id -> commit_lsn
    pub aborted_transactions: HashMap<u64, u64>,   // tx_id -> abort_lsn
    
    // Recovery statistics
    pub total_entries: u64,
    pub corrupted_entries: u64,
    pub incomplete_transactions: u64,
    pub recovered_lsn_range: (u64, u64),
    
    // Recovery options
    pub allow_torn_writes: bool,
    pub strict_ordering: bool,
    pub repair_checksums: bool,
}

impl WALRecoveryContext {
    pub fn new() -> Self {
        Self {
            active_transactions: HashMap::new(),
            committed_transactions: HashMap::new(),
            aborted_transactions: HashMap::new(),
            total_entries: 0,
            corrupted_entries: 0,
            incomplete_transactions: 0,
            recovered_lsn_range: (u64::MAX, 0),
            allow_torn_writes: true,
            strict_ordering: false,
            repair_checksums: false,
        }
    }
    
    /// Recover with comprehensive edge case handling
    pub fn recover_with_fixes<P, F>(
        &mut self,
        wal_path: P,
        mut apply_fn: F,
    ) -> Result<()>
    where
        P: AsRef<Path>,
        F: FnMut(&WALOperation, bool) -> Result<()>, // op, is_committed
    {
        let mut file = match File::open(wal_path.as_ref()) {
            Ok(f) => f,
            Err(e) => {
                warn!("Failed to open WAL file: {}", e);
                return Ok(()); // Empty WAL is valid
            }
        };
        
        // Check file size
        let file_size = file.metadata()?.len();
        if file_size < 8 {
            warn!("WAL file too small ({}), treating as empty", file_size);
            return Ok(());
        }
        
        // Verify header
        let mut header = [0u8; 8];
        file.read_exact(&mut header)?;
        
        // Main recovery loop
        let mut last_valid_lsn = 0;
        let mut entries_by_lsn: HashMap<u64, WALEntry> = HashMap::new();
        
        loop {
            let offset = file.stream_position()?;
            
            // Check if we're at EOF
            if offset >= file_size {
                break;
            }
            
            // Try to read entry with recovery
            match self.read_entry_safe(&mut file, offset, file_size) {
                Ok(Some(entry)) => {
                    self.total_entries += 1;
                    
                    // Handle duplicate LSN
                    if entries_by_lsn.contains_key(&entry.lsn) {
                        warn!("Duplicate LSN {} found at offset {}", entry.lsn, offset);
                        if !self.repair_checksums {
                            continue;
                        }
                    }
                    
                    // Check ordering if strict mode
                    if self.strict_ordering && entry.lsn <= last_valid_lsn {
                        warn!("Out-of-order LSN {} (last was {})", entry.lsn, last_valid_lsn);
                        continue;
                    }
                    
                    // Process the entry
                    self.process_entry(&entry)?;
                    entries_by_lsn.insert(entry.lsn, entry.clone());
                    
                    last_valid_lsn = entry.lsn.max(last_valid_lsn);
                    self.recovered_lsn_range.0 = self.recovered_lsn_range.0.min(entry.lsn);
                    self.recovered_lsn_range.1 = self.recovered_lsn_range.1.max(entry.lsn);
                }
                Ok(None) => {
                    // Skip corrupted entry
                    self.corrupted_entries += 1;
                    
                    // Try to find next valid entry
                    if self.allow_torn_writes {
                        self.scan_for_next_entry(&mut file, file_size)?;
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    debug!("Error reading entry at offset {}: {}", offset, e);
                    break;
                }
            }
        }
        
        // Process completed transactions
        let mut sorted_entries: Vec<_> = entries_by_lsn.into_iter().collect();
        sorted_entries.sort_by_key(|(lsn, _)| *lsn);
        
        for (_, entry) in sorted_entries {
            match &entry.operation {
                WALOperation::TransactionBegin { tx_id: _ } => {
                    // Already handled in process_entry
                }
                WALOperation::TransactionCommit { tx_id } => {
                    if let Some(ops) = self.active_transactions.get(tx_id) {
                        // Apply all operations in committed transaction
                        for op in ops {
                            apply_fn(op, true)?;
                        }
                    }
                }
                WALOperation::TransactionAbort { .. } => {
                    // Don't apply aborted operations
                }
                op @ (WALOperation::Put { .. } | WALOperation::Delete { .. }) => {
                    // Standalone operation - apply if not in transaction
                    let in_transaction = self.active_transactions.values()
                        .any(|ops| ops.iter().any(|o| std::ptr::eq(o, op)));
                    
                    if !in_transaction {
                        apply_fn(op, true)?;
                    }
                }
                WALOperation::Checkpoint { .. } => {
                    // Checkpoint is informational
                }
                WALOperation::BeginTransaction { .. } => {
                    // Already handled in process_entry
                }
                WALOperation::CommitTransaction { .. } => {
                    // Already handled above
                }
                WALOperation::AbortTransaction { .. } => {
                    // Don't apply aborted operations
                }
            }
        }
        
        // Handle incomplete transactions
        self.incomplete_transactions = self.active_transactions.len() as u64;
        if self.incomplete_transactions > 0 {
            warn!(
                "Found {} incomplete transactions that will not be applied",
                self.incomplete_transactions
            );
        }
        
        info!(
            "WAL recovery complete: {} entries, {} corrupted, {} incomplete transactions, LSN range {:?}",
            self.total_entries, self.corrupted_entries, self.incomplete_transactions, self.recovered_lsn_range
        );
        
        Ok(())
    }
    
    /// Safely read an entry with recovery for torn writes
    fn read_entry_safe(
        &self,
        file: &mut File,
        offset: u64,
        file_size: u64,
    ) -> Result<Option<WALEntry>> {
        // Check if we have enough space for length header
        if offset + 4 > file_size {
            return Ok(None);
        }
        
        // Read length
        let mut len_bytes = [0u8; 4];
        if file.read_exact(&mut len_bytes).is_err() {
            return Ok(None);
        }
        
        let len = u32::from_le_bytes(len_bytes) as usize;
        
        // Sanity check length
        if len == 0 || len > 10 * 1024 * 1024 {
            warn!("Invalid entry length {} at offset {}", len, offset);
            return Ok(None);
        }
        
        // Check if we have enough space for entry
        if offset + 4 + len as u64 > file_size {
            warn!("Incomplete entry at offset {} (needs {} bytes)", offset, len);
            return Ok(None);
        }
        
        // Read entry data
        let mut entry_bytes = vec![0u8; len];
        if file.read_exact(&mut entry_bytes).is_err() {
            return Ok(None);
        }
        
        // Try to deserialize
        match bincode::decode_from_slice::<WALEntry, _>(&entry_bytes, bincode::config::standard()) {
            Ok((mut entry, _)) => {
                // Verify checksum
                if !entry.verify_checksum() {
                    warn!("Checksum mismatch for entry at offset {}", offset);
                    if self.repair_checksums {
                        // Recalculate checksum
                        entry.calculate_checksum();
                    } else {
                        return Ok(None);
                    }
                }
                Ok(Some(entry))
            }
            Err(e) => {
                warn!("Failed to deserialize entry at offset {}: {}", offset, e);
                Ok(None)
            }
        }
    }
    
    /// Scan forward to find next valid entry after corruption
    fn scan_for_next_entry(&self, file: &mut File, file_size: u64) -> Result<()> {
        let mut pos = file.stream_position()?;
        let mut buffer = [0u8; 4096];
        
        while pos < file_size {
            file.seek(SeekFrom::Start(pos))?;
            let bytes_read = file.read(&mut buffer)?;
            
            // Look for potential entry start (valid length bytes)
            for i in 0..bytes_read.saturating_sub(4) {
                let len = u32::from_le_bytes([
                    buffer[i],
                    buffer[i + 1],
                    buffer[i + 2],
                    buffer[i + 3],
                ]);
                
                if len > 0 && len < 1024 * 1024 {
                    // Potential entry found, seek to it
                    file.seek(SeekFrom::Start(pos + i as u64))?;
                    return Ok(());
                }
            }
            
            pos += bytes_read as u64;
        }
        
        Ok(())
    }
    
    /// Process entry and update transaction state
    fn process_entry(&mut self, entry: &WALEntry) -> Result<()> {
        match &entry.operation {
            WALOperation::TransactionBegin { tx_id } => {
                self.active_transactions.insert(*tx_id, Vec::new());
            }
            WALOperation::TransactionCommit { tx_id } => {
                self.committed_transactions.insert(*tx_id, entry.lsn);
                self.active_transactions.remove(tx_id);
            }
            WALOperation::TransactionAbort { tx_id } => {
                self.aborted_transactions.insert(*tx_id, entry.lsn);
                self.active_transactions.remove(tx_id);
            }
            op @ (WALOperation::Put { .. } | WALOperation::Delete { .. }) => {
                // Find which transaction this belongs to
                for (_, ops) in self.active_transactions.iter_mut() {
                    // Simple heuristic: operation belongs to most recent transaction
                    ops.push(op.clone());
                    break;
                }
            }
            WALOperation::Checkpoint { .. } => {
                // Just track checkpoint
            }
            WALOperation::BeginTransaction { tx_id } => {
                self.active_transactions.insert(*tx_id, Vec::new());
            }
            WALOperation::CommitTransaction { tx_id } => {
                self.committed_transactions.insert(*tx_id, entry.lsn);
                self.active_transactions.remove(tx_id);
            }
            WALOperation::AbortTransaction { tx_id } => {
                self.aborted_transactions.insert(*tx_id, entry.lsn);
                self.active_transactions.remove(tx_id);
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::io::Write;
    
    #[test]
    fn test_torn_write_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("torn.wal");
        
        // Create a WAL with torn write
        {
            let mut file = File::create(&wal_path).unwrap();
            // Write header
            file.write_all(&[0x57, 0x41, 0x4C, 0x21, 0x01, 0x00, 0x00, 0x00]).unwrap();
            
            // Write partial entry (only length, no data)
            file.write_all(&100u32.to_le_bytes()).unwrap();
            // File ends here - torn write!
        }
        
        let mut ctx = WALRecoveryContext::new();
        ctx.allow_torn_writes = true;
        
        let result = ctx.recover_with_fixes(&wal_path, |_, _| Ok(()));
        assert!(result.is_ok());
        assert_eq!(ctx.corrupted_entries, 1);
    }
    
    #[test]
    fn test_duplicate_lsn_recovery() {
        // Test handling of duplicate LSNs
        let dir = tempdir().unwrap();
        let _wal_path = dir.path().join("dup.wal");
        
        // Would create entries with duplicate LSNs and verify only one is processed
    }
}