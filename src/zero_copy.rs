use crate::error::Result;
use crate::storage::PAGE_SIZE;
use std::ops::Deref;

/// Zero-copy page reference that directly points to memory-mapped data
pub struct ZeroCopyPage<'a> {
    pub page_id: u32,
    pub data: &'a [u8; PAGE_SIZE],
}

impl<'a> ZeroCopyPage<'a> {
    pub fn new(page_id: u32, data: &'a [u8; PAGE_SIZE]) -> Self {
        Self { page_id, data }
    }
    
    /// Get the page data without copying
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        self.data
    }
    
    /// Verify checksum without copying
    pub fn verify_checksum(&self) -> bool {
        use crc32fast::Hasher;
        
        // Skip checksum verification for header page and empty pages
        if self.page_id == 0 || self.data.iter().all(|&b| b == 0) {
            return true;
        }
        
        // Extract stored checksum
        let stored_checksum = u32::from_le_bytes([
            self.data[12], self.data[13], self.data[14], self.data[15]
        ]);
        
        // Calculate actual checksum
        let mut hasher = Hasher::new();
        hasher.update(&self.data[16..]);
        let calculated = hasher.finalize();
        
        stored_checksum == calculated
    }
}

impl<'a> Deref for ZeroCopyPage<'a> {
    type Target = [u8; PAGE_SIZE];
    
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

/// Extension trait for zero-copy operations
pub trait ZeroCopyExt {
    /// Get a page without copying the data
    fn get_page_zero_copy(&self, page_id: u32) -> Result<ZeroCopyPage>;
}

// We'll implement this for PageManagerWrapper later

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_zero_copy_page() {
        let mut data = [0u8; PAGE_SIZE];
        data[0..4].copy_from_slice(&[1, 2, 3, 4]);
        
        let page = ZeroCopyPage::new(1, &data);
        assert_eq!(page.page_id, 1);
        assert_eq!(&page.data[0..4], &[1, 2, 3, 4]);
    }
}