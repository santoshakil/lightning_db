use crate::error::{Error, Result};
use crate::prefetch::PageCache;
use crate::storage::{Page, PageManagerWrapper};
use std::sync::Arc;

/// Adapter to implement PageCache trait for the page manager wrapper
pub struct PageCacheAdapterWrapper {
    page_manager: PageManagerWrapper,
}

impl PageCacheAdapterWrapper {
    pub fn new(page_manager: PageManagerWrapper) -> Self {
        Self { page_manager }
    }
}

impl PageCache for PageCacheAdapterWrapper {
    fn get_page(&self, _table_id: &str, page_id: u32) -> Result<Option<Arc<Page>>> {
        match self.page_manager.get_page(page_id) {
            Ok(page) => Ok(Some(Arc::new(page))),
            Err(Error::InvalidPageId) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn prefetch_page(&self, _table_id: &str, page_id: u32) -> Result<()> {
        // For prefetching, we can read the page into memory/cache
        // This will warm up the cache for future accesses
        match self.page_manager.get_page(page_id) {
            Ok(_) => Ok(()), // Page is now in cache
            Err(Error::InvalidPageId) => Ok(()), // Page doesn't exist
            Err(e) => Err(e),
        }
    }

    fn is_page_cached(&self, _table_id: &str, page_id: u32) -> bool {
        // For now, assume pages are cached after being read
        // In a real implementation, this would check the actual cache
        self.page_manager.get_page(page_id).is_ok()
    }
}