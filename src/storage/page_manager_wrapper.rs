use crate::encryption::EncryptionManager;
use crate::error::Result;
use crate::storage::{OptimizedPageManager, Page, PageManager};
use parking_lot::RwLock;
use std::sync::Arc;

/// Wrapper enum to support standard, optimized, and encrypted page managers
pub enum PageManagerWrapper {
    Standard(Arc<RwLock<PageManager>>),
    Optimized(Arc<OptimizedPageManager>),
    Encrypted {
        inner: Box<PageManagerWrapper>,
        encryption_manager: Arc<EncryptionManager>,
    },
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

    /// Create a new encrypted page manager wrapper
    pub fn encrypted(
        inner: PageManagerWrapper,
        encryption_manager: Arc<EncryptionManager>,
    ) -> Self {
        PageManagerWrapper::Encrypted {
            inner: Box::new(inner),
            encryption_manager,
        }
    }

    /// Allocate a new page
    pub fn allocate_page(&self) -> Result<u32> {
        match self {
            PageManagerWrapper::Standard(manager) => manager.write().allocate_page(),
            PageManagerWrapper::Optimized(manager) => manager.allocate_page(),
            PageManagerWrapper::Encrypted { inner, .. } => inner.allocate_page(),
        }
    }

    /// Free a page
    pub fn free_page(&self, page_id: u32) {
        match self {
            PageManagerWrapper::Standard(manager) => manager.write().free_page(page_id),
            PageManagerWrapper::Optimized(manager) => manager.free_page(page_id),
            PageManagerWrapper::Encrypted { inner, .. } => inner.free_page(page_id),
        }
    }

    /// Get a page by ID
    pub fn get_page(&self, page_id: u32) -> Result<Page> {
        match self {
            PageManagerWrapper::Standard(manager) => manager.read().get_page(page_id),
            PageManagerWrapper::Optimized(manager) => manager.get_page(page_id),
            PageManagerWrapper::Encrypted {
                inner,
                encryption_manager,
            } => {
                let page = inner.get_page(page_id)?;

                // Skip encryption for header page (page 0)
                if page_id == 0 {
                    return Ok(page);
                }

                // Decrypt page data
                let decrypted = encryption_manager.decrypt_page(page_id as u64, page.get_data())?;

                // Create new page with decrypted data
                let mut decrypted_data = [0u8; crate::storage::PAGE_SIZE];
                decrypted_data.copy_from_slice(&decrypted);

                Ok(Page::with_data(page_id, decrypted_data))
            }
        }
    }

    /// Write a page
    pub fn write_page(&self, page: &Page) -> Result<()> {
        match self {
            PageManagerWrapper::Standard(manager) => manager.write().write_page(page),
            PageManagerWrapper::Optimized(manager) => manager.write_page(page),
            PageManagerWrapper::Encrypted {
                inner,
                encryption_manager,
            } => {
                // Skip encryption for header page (page 0)
                if page.id == 0 {
                    return inner.write_page(page);
                }

                // Encrypt page data
                let encrypted = encryption_manager.encrypt_page(page.id as u64, page.get_data())?;

                // Create new page with encrypted data
                let mut encrypted_data = [0u8; crate::storage::PAGE_SIZE];

                // Ensure encrypted data fits in page size
                if encrypted.len() > crate::storage::PAGE_SIZE {
                    return Err(crate::Error::Encryption(format!(
                        "Encrypted data ({} bytes) exceeds page size ({} bytes)",
                        encrypted.len(),
                        crate::storage::PAGE_SIZE
                    )));
                }

                // Copy encrypted data
                encrypted_data[..encrypted.len()].copy_from_slice(&encrypted);

                let encrypted_page = Page::with_data(page.id, encrypted_data);
                inner.write_page(&encrypted_page)
            }
        }
    }

    /// Sync to disk
    pub fn sync(&self) -> Result<()> {
        match self {
            PageManagerWrapper::Standard(manager) => manager.write().sync(),
            PageManagerWrapper::Optimized(manager) => manager.sync(),
            PageManagerWrapper::Encrypted { inner, .. } => inner.sync(),
        }
    }

    /// Get page count
    pub fn page_count(&self) -> u32 {
        match self {
            PageManagerWrapper::Standard(manager) => manager.read().page_count(),
            PageManagerWrapper::Optimized(manager) => manager.page_count(),
            PageManagerWrapper::Encrypted { inner, .. } => inner.page_count(),
        }
    }

    /// Get free page count
    pub fn free_page_count(&self) -> usize {
        match self {
            PageManagerWrapper::Standard(manager) => manager.read().free_page_count(),
            PageManagerWrapper::Optimized(manager) => manager.free_page_count(),
            PageManagerWrapper::Encrypted { inner, .. } => inner.free_page_count(),
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
            PageManagerWrapper::Encrypted {
                inner,
                encryption_manager,
            } => PageManagerWrapper::Encrypted {
                inner: inner.clone(),
                encryption_manager: Arc::clone(encryption_manager),
            },
        }
    }
}

impl std::fmt::Debug for PageManagerWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageManagerWrapper::Standard(_) => write!(f, "PageManagerWrapper::Standard"),
            PageManagerWrapper::Optimized(_) => write!(f, "PageManagerWrapper::Optimized"),
            PageManagerWrapper::Encrypted { .. } => write!(f, "PageManagerWrapper::Encrypted"),
        }
    }
}
