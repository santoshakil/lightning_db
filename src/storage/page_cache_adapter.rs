use crate::error::{Error, Result};
use crate::prefetch::PageCache;
use crate::storage::{Page, PageManager};
use parking_lot::RwLock;
use std::sync::Arc;

/// Adapter to implement PageCache trait for the existing storage system
pub struct PageCacheAdapter {
    page_manager: Arc<RwLock<PageManager>>,
}

impl PageCacheAdapter {
    pub fn new(page_manager: Arc<RwLock<PageManager>>) -> Self {
        Self { page_manager }
    }
}

impl PageCache for PageCacheAdapter {
    fn get_page(&self, _table_id: &str, page_id: u32) -> Result<Option<Arc<Page>>> {
        let page_manager = self.page_manager.read();
        match page_manager.get_page(page_id) {
            Ok(page) => Ok(Some(Arc::new(page))),
            Err(Error::Storage(msg)) if msg.contains("Invalid page") => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn prefetch_page(&self, _table_id: &str, page_id: u32) -> Result<()> {
        // For prefetching, we can read the page into memory/cache
        // This will warm up the cache for future accesses
        let page_manager = self.page_manager.read();
        match page_manager.get_page(page_id) {
            Ok(_) => Ok(()), // Page is now in cache
            Err(Error::Storage(msg)) if msg.contains("Invalid page") => Ok(()), // Page doesn't exist
            Err(e) => Err(e),
        }
    }

    fn is_page_cached(&self, _table_id: &str, page_id: u32) -> bool {
        // For now, assume pages are cached after being read
        // In a real implementation, this would check the actual cache
        let page_manager = self.page_manager.read();
        page_manager.get_page(page_id).is_ok()
    }
}
