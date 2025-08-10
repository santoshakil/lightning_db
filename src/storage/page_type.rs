//! Page types for Lightning DB
//!
//! Defines the different types of pages used in the database.

use serde::{Deserialize, Serialize};

/// Type of database page
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PageType {
    /// Metadata page containing database configuration
    Meta = 0,

    /// Data page containing B+Tree nodes or key-value pairs
    Data = 1,

    /// Overflow page for large values
    Overflow = 2,

    /// Free page available for reuse
    Free = 3,
}

impl PageType {
    /// Create PageType from byte value
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(PageType::Meta),
            1 => Some(PageType::Data),
            2 => Some(PageType::Overflow),
            3 => Some(PageType::Free),
            _ => None,
        }
    }

    /// Convert PageType to byte value
    pub fn to_byte(&self) -> u8 {
        *self as u8
    }
}

impl Default for PageType {
    fn default() -> Self {
        PageType::Data
    }
}
