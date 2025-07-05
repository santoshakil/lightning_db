// Example fix for the panic on corruption issue in page.rs:179

// Current problematic code:
// let header_data = &self.mmap[0..PAGE_SIZE];

// Fixed version:
fn load_header_page_safe(&mut self) -> Result<()> {
    // Check if mmap has enough data
    if self.mmap.len() < PAGE_SIZE {
        return Err(Error::CorruptedDatabase(
            format!("Database file too small: {} bytes, expected at least {}", 
                    self.mmap.len(), PAGE_SIZE)
        ));
    }
    
    let header_data = &self.mmap[0..PAGE_SIZE];
    
    // Continue with normal processing...
    Ok(())
}

// Additional safety improvements:

// 1. Add bounds checking for all slice operations
fn read_page_safe(&self, page_id: PageId) -> Result<&[u8]> {
    let offset = page_id as usize * PAGE_SIZE;
    let end = offset + PAGE_SIZE;
    
    if end > self.mmap.len() {
        return Err(Error::InvalidPageId(page_id));
    }
    
    Ok(&self.mmap[offset..end])
}

// 2. Validate data before processing
fn validate_page_checksum(data: &[u8]) -> Result<()> {
    if data.len() < PAGE_SIZE {
        return Err(Error::CorruptedPage("Page data too small"));
    }
    
    // Calculate and verify checksum
    let stored_checksum = u32::from_le_bytes([
        data[PAGE_SIZE-4],
        data[PAGE_SIZE-3],
        data[PAGE_SIZE-2],
        data[PAGE_SIZE-1],
    ]);
    
    let calculated_checksum = calculate_checksum(&data[..PAGE_SIZE-4]);
    
    if stored_checksum != calculated_checksum {
        return Err(Error::ChecksumMismatch);
    }
    
    Ok(())
}

// 3. Graceful recovery options
fn open_with_recovery(path: &str) -> Result<Database> {
    match Database::open(path) {
        Ok(db) => Ok(db),
        Err(Error::CorruptedDatabase(_)) => {
            // Try recovery options
            println!("Database corrupted, attempting recovery...");
            
            // Option 1: Try to recover from WAL
            if let Ok(db) = recover_from_wal(path) {
                return Ok(db);
            }
            
            // Option 2: Open in read-only mode
            if let Ok(db) = open_readonly(path) {
                println!("Opened in read-only mode");
                return Ok(db);
            }
            
            // Option 3: Return error with recovery instructions
            Err(Error::CorruptedDatabase(
                "Database is corrupted. Please restore from backup or use repair tool.".into()
            ))
        }
        Err(e) => Err(e),
    }
}