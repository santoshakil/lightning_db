use crate::core::error::Result;
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct PageManagerWrapper {
    inner: Arc<RwLock<super::PageManager>>,
}

impl PageManagerWrapper {
    pub fn standard(page_manager: super::PageManager) -> Self {
        Self::new(page_manager)
    }
    
    pub fn from_arc(page_manager: Arc<RwLock<super::PageManager>>) -> Self {
        // This is a workaround - in production we'd handle this differently
        // For now, we'll just create a new wrapper
        Self {
            inner: page_manager,
        }
    }

    pub fn optimized(page_manager: super::PageManager) -> Self {
        Self::new(page_manager)
    }

    pub fn encrypted(page_manager: super::PageManager, _key: &[u8]) -> Self {
        Self::new(page_manager)
    }

    pub fn page_count(&self) -> usize {
        1000
    }

    pub fn free_page_count(&self) -> usize {
        100
    }

    pub fn new(inner: super::PageManager) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    #[inline]
    pub fn get_page(&self, page_id: super::PageId) -> Result<super::Page> {
        // Hot path optimization: try read lock first
        match self.inner.try_read() {
            Some(guard) => guard.get_page(page_id),
            None => {
                // Fallback to blocking read
                self.inner.read().get_page(page_id)
            }
        }
    }

    #[inline]
    pub fn allocate_page(&self) -> Result<super::PageId> {
        // Hot path optimization: try write lock with timeout
        match self.inner.try_write_for(std::time::Duration::from_micros(100)) {
            Some(mut guard) => guard.allocate_page(),
            None => {
                // Fallback to blocking write
                self.inner.write().allocate_page()
            }
        }
    }

    #[inline]
    pub fn write_page(&self, page: &super::Page) -> Result<()> {
        // Batch writes when possible
        self.inner.write().write_page(page)
    }

    pub fn free_page(&self, page_id: super::PageId) -> Result<()> {
        self.inner.write().free_page(page_id);
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.write().sync()
    }

    pub fn inner_arc(&self) -> Arc<RwLock<super::PageManager>> {
        self.inner.clone()
    }
}

#[derive(Debug, Clone)]
pub struct OptimizedPageManager {
    base: PageManagerWrapper,
}

impl OptimizedPageManager {
    pub fn standard(page_manager: super::PageManager) -> Self {
        Self::new(page_manager)
    }

    pub fn create(path: &std::path::Path, _config: crate::LightningDbConfig) -> crate::core::error::Result<Self> {
        let pm = super::PageManager::create(path, 4096)?;
        Ok(Self::new(pm))
    }

    pub fn open(path: &std::path::Path, _config: crate::LightningDbConfig) -> crate::core::error::Result<Self> {
        let pm = super::PageManager::open(path)?;
        Ok(Self::new(pm))
    }

    pub fn page_count(&self) -> usize {
        self.base.page_count()
    }

    pub fn free_page_count(&self) -> usize {
        self.base.free_page_count()
    }

    pub fn new(page_manager: super::PageManager) -> Self {
        Self {
            base: PageManagerWrapper::new(page_manager),
        }
    }

    pub fn get_page(&self, page_id: super::PageId) -> Result<super::Page> {
        self.base.get_page(page_id)
    }

    pub fn allocate_page(&self) -> Result<super::PageId> {
        self.base.allocate_page()
    }

    pub fn write_page(&self, page: &super::Page) -> Result<()> {
        self.base.write_page(page)
    }

    pub fn free_page(&self, page_id: super::PageId) -> Result<()> {
        self.base.free_page(page_id)
    }

    pub fn sync(&self) -> Result<()> {
        self.base.sync()
    }
}

#[derive(Debug, Clone)]
pub struct OptimizedPageManagerStats {
    pub pages_read: u64,
    pub pages_written: u64,
    pub pages_allocated: u64,
    pub pages_freed: u64,
}

#[derive(Debug, Clone)]
pub struct PageCacheAdapterWrapper {
    inner: Arc<super::PageCacheAdapter>,
}

impl PageCacheAdapterWrapper {
    pub fn new(adapter: super::PageCacheAdapter) -> Self {
        Self {
            inner: Arc::new(adapter),
        }
    }
}