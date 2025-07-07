use crate::error::Result;
use crate::storage::Page;

/// Common trait for page managers
pub trait PageManagerTrait: Send + Sync {
    /// Allocate a new page
    fn allocate_page(&self) -> Result<u32>;

    /// Free a page for reuse
    fn free_page(&self, page_id: u32);

    /// Get a page by ID
    fn get_page(&self, page_id: u32) -> Result<Page>;

    /// Write a page
    fn write_page(&self, page: &Page) -> Result<()>;

    /// Sync changes to disk
    fn sync(&self) -> Result<()>;

    /// Get total page count
    fn page_count(&self) -> u32;

    /// Get free page count
    fn free_page_count(&self) -> usize;
}
