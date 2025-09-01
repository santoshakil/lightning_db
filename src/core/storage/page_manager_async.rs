//! Async extensions for PageManager
//!
//! Provides async methods for page operations needed by integrity validation.

use super::{Page, PageId, PageManager, page::PAGE_SIZE};
use crate::core::error::Result;
use parking_lot::RwLock;
use std::sync::Arc;
use async_trait::async_trait;

/// Async trait for PageManager operations
#[async_trait]
pub trait PageManagerAsync: Send + Sync {
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

    /// Read raw page data
    async fn read_page(&self, key: Vec<u8>) -> Result<Vec<u8>>;

    /// Write raw page data
    async fn write_page(&self, key: Vec<u8>, data: Vec<u8>) -> Result<()>;

    /// Delete a page
    async fn delete_page(&self, key: Vec<u8>) -> Result<()>;
}

/// Implementation of async methods for `Arc<RwLock<PageManager>>`
#[async_trait]
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

    async fn read_page(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Convert key to page_id if possible, or use a default implementation
        if key.len() >= 8 {
            let page_id = u64::from_le_bytes(key[..8].try_into().map_err(|_| {
                crate::core::error::Error::InvalidArgument("Invalid key format for page ID".to_string())
            })?);
            let page = self.load_page(page_id).await?;
            Ok(page.data().to_vec())
        } else {
            Err(crate::core::error::Error::InvalidArgument(
                "Invalid key for page read".to_string()
            ))
        }
    }

    async fn write_page(&self, key: Vec<u8>, data: Vec<u8>) -> Result<()> {
        // Convert key to page_id if possible, or use a default implementation
        if key.len() >= 8 {
            let page_id = u64::from_le_bytes(key[..8].try_into().map_err(|_| {
                crate::core::error::Error::InvalidArgument("Invalid key format for page ID".to_string())
            })?) as PageId;
            let mut page = Page::new(page_id);
            // Copy data to page (handling size mismatch)
            let data_len = data.len().min(PAGE_SIZE);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    Arc::get_mut(&mut page.data).ok_or_else(|| {
                        crate::core::error::Error::Internal("Page data is shared and cannot be mutated".to_string())
                    })?.as_mut_ptr(),
                    data_len,
                );
            }
            page.dirty = true;
            self.save_page(&page).await
        } else {
            Err(crate::core::error::Error::InvalidArgument(
                "Invalid key for page write".to_string()
            ))
        }
    }

    async fn delete_page(&self, key: Vec<u8>) -> Result<()> {
        // Convert key to page_id if possible
        if key.len() >= 8 {
            let page_id = u64::from_le_bytes(key[..8].try_into().map_err(|_| {
                crate::core::error::Error::InvalidArgument("Invalid key format for page ID".to_string())
            })?);
            self.free_page(page_id).await
        } else {
            Err(crate::core::error::Error::InvalidArgument(
                "Invalid key for page delete".to_string()
            ))
        }
    }
}
