//! Async extensions for PageManager
//!
//! Provides async methods for page operations needed by integrity validation.

use super::{Page, PageId, PageManager};
use crate::core::error::Result;
use parking_lot::RwLock;
use std::sync::Arc;

/// Async trait for PageManager operations
pub trait PageManagerAsync {
    /// Load a page asynchronously
    async fn load_page(&self, page_id: u64) -> Result<Page>;

    /// Save a page asynchronously
    async fn save_page(&self, page: &Page) -> Result<()>;

    /// Check if a page is allocated
    async fn is_page_allocated(&self, page_id: u64) -> Result<bool>;

    /// Free a page asynchronously
    async fn free_page(&self, page_id: u64) -> Result<()>;

    /// Get all allocated page IDs
    async fn get_all_allocated_pages(&self) -> Result<Vec<u64>>;
}

/// Implementation of async methods for `Arc<RwLock<PageManager>>`
impl PageManagerAsync for Arc<RwLock<PageManager>> {
    async fn load_page(&self, page_id: u64) -> Result<Page> {
        // Convert u64 to u32 (PageId)
        let page_id = page_id as PageId;
        self.read().get_page(page_id)
    }

    async fn save_page(&self, page: &Page) -> Result<()> {
        self.write().write_page(page)
    }

    async fn is_page_allocated(&self, page_id: u64) -> Result<bool> {
        let page_id = page_id as PageId;
        let guard = self.read();
        // A page is allocated if its ID is less than next_page_id and not in free_pages
        Ok(page_id > 0 && page_id < guard.page_count() && !guard.is_free_page(page_id))
    }

    async fn free_page(&self, page_id: u64) -> Result<()> {
        let page_id = page_id as PageId;
        self.write().free_page(page_id);
        Ok(())
    }

    async fn get_all_allocated_pages(&self) -> Result<Vec<u64>> {
        let guard = self.read();

        let mut pages = Vec::new();
        let page_count = guard.page_count();

        // Skip page 0 (header) and collect all non-free pages
        for page_id in 1..page_count {
            if !guard.is_free_page(page_id) {
                pages.push(page_id as u64);
            }
        }

        Ok(pages)
    }
}
