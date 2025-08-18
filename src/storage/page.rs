use crate::error::{Error, Result};
use crate::storage::PageType;
use crc32fast::Hasher;
use std::sync::Arc;

pub const PAGE_SIZE: usize = 4096;
pub const MAGIC: u32 = 0x4C444200; // "LDB\0"

pub type PageId = u32;

#[derive(Debug, Clone)]
pub struct Page {
    pub id: PageId,
    pub data: Arc<[u8; PAGE_SIZE]>,
    pub dirty: bool,
    pub page_type: PageType,
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: Arc::new([0u8; PAGE_SIZE]),
            dirty: false,
            page_type: PageType::default(),
        }
    }

    pub fn with_data(id: PageId, data: [u8; PAGE_SIZE]) -> Self {
        // Extract page type from data
        let page_type = if data.len() > 8 {
            PageType::from_byte(data[8]).unwrap_or_default()
        } else {
            PageType::default()
        };

        Self {
            id,
            data: Arc::new(data),
            dirty: false,
            page_type,
        }
    }

    pub fn get_data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn get_mut_data(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.dirty = true;
        Arc::make_mut(&mut self.data)
    }

    pub fn calculate_checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.data[16..]); // Skip header including checksum field
        hasher.finalize()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }

    pub fn verify_checksum(&self) -> bool {
        let stored_checksum =
            u32::from_le_bytes([self.data[12], self.data[13], self.data[14], self.data[15]]);
        stored_checksum == self.calculate_checksum()
    }

    pub fn data(&self) -> &[u8] {
        &self.data[..]
    }

    pub fn set_data(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > PAGE_SIZE {
            return Err(Error::Memory);
        }
        let mut new_data = [0u8; PAGE_SIZE];
        new_data[..data.len()].copy_from_slice(data);
        self.data = Arc::new(new_data);
        self.dirty = true;
        Ok(())
    }
}