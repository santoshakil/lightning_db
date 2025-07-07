use crate::error::Result;
use crate::storage::{OptimizedPageManager, Page, PageManager};
use parking_lot::RwLock;
use std::sync::Arc;

/// Wrapper enum to support both standard and optimized page managers
pub enum PageManagerWrapper {
    Standard(Arc<RwLock<PageManager>>),
    Optimized(Arc<OptimizedPageManager>),
}

impl PageManagerWrapper {
    /// Create a new standard page manager wrapper
    pub fn standard(manager: Arc<RwLock<PageManager>>) -> Self {
        PageManagerWrapper::Standard(manager)
    }

    /// Create a new optimized page manager wrapper
    pub fn optimized(manager: Arc<OptimizedPageManager>) -> Self {
        PageManagerWrapper::Optimized(manager)
    }

    /// Allocate a new page
    pub fn allocate_page(&self) -> Result<u32> {
        match self {
            PageManagerWrapper::Standard(manager) => manager.write().allocate_page(),
            PageManagerWrapper::Optimized(manager) => manager.allocate_page(),
        }
    }

    /// Free a page
    pub fn free_page(&self, page_id: u32) {
        match self {
            PageManagerWrapper::Standard(manager) => manager.write().free_page(page_id),
            PageManagerWrapper::Optimized(manager) => manager.free_page(page_id),
        }
    }

    /// Get a page by ID
    pub fn get_page(&self, page_id: u32) -> Result<Page> {
        match self {
            PageManagerWrapper::Standard(manager) => manager.read().get_page(page_id),
            PageManagerWrapper::Optimized(manager) => manager.get_page(page_id),
        }
    }

    /// Write a page
    pub fn write_page(&self, page: &Page) -> Result<()> {
        match self {
            PageManagerWrapper::Standard(manager) => manager.write().write_page(page),
            PageManagerWrapper::Optimized(manager) => manager.write_page(page),
        }
    }

    /// Sync to disk
    pub fn sync(&self) -> Result<()> {
        match self {
            PageManagerWrapper::Standard(manager) => manager.write().sync(),
            PageManagerWrapper::Optimized(manager) => manager.sync(),
        }
    }

    /// Get page count
    pub fn page_count(&self) -> u32 {
        match self {
            PageManagerWrapper::Standard(manager) => manager.read().page_count(),
            PageManagerWrapper::Optimized(manager) => manager.page_count(),
        }
    }

    /// Get free page count
    pub fn free_page_count(&self) -> usize {
        match self {
            PageManagerWrapper::Standard(manager) => manager.read().free_page_count(),
            PageManagerWrapper::Optimized(manager) => manager.free_page_count(),
        }
    }
}

impl Clone for PageManagerWrapper {
    fn clone(&self) -> Self {
        match self {
            PageManagerWrapper::Standard(manager) => {
                PageManagerWrapper::Standard(Arc::clone(manager))
            }
            PageManagerWrapper::Optimized(manager) => {
                PageManagerWrapper::Optimized(Arc::clone(manager))
            }
        }
    }
}
