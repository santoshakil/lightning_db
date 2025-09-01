//! Database header management
//!
//! Provides the Header struct for database metadata storage

use crate::core::error::Result;
use crc32fast::Hasher;

/// Database header structure
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub checksum: u32,
}

impl Header {
    pub const MAGIC: u32 = 0x4C44_4231; // "LDB1"
    pub const VERSION: u32 = 1;

    pub fn new(page_size: u32) -> Self {
        let mut header = Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            page_size,
            checksum: 0,
        };
        header.checksum = header.calculate_checksum();
        header
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend(&self.magic.to_le_bytes());
        bytes.extend(&self.version.to_le_bytes());
        bytes.extend(&self.page_size.to_le_bytes());
        bytes.extend(&self.checksum.to_le_bytes());
        bytes
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 16 {
            return Err(crate::core::error::Error::InvalidFormat(
                "Header too small".to_string(),
            ));
        }

        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        let page_size = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        let checksum = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);

        Ok(Self {
            magic,
            version,
            page_size,
            checksum,
        })
    }

    pub fn calculate_checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.magic.to_le_bytes());
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&self.page_size.to_le_bytes());
        hasher.finalize()
    }

    pub fn verify_checksum(&self) -> bool {
        self.calculate_checksum() == self.checksum
    }
}
