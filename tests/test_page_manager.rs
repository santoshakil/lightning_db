use lightning_db::storage::page::PageManager;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_page_manager_create() {
    println!("Starting page manager test...");
    
    let dir = tempdir().unwrap();
    println!("Created temp dir: {:?}", dir.path());
    
    let page_manager = PageManager::create(&dir.path().join("test.db"), 1024 * 1024);
    assert!(page_manager.is_ok(), "Failed to create page manager: {:?}", page_manager.err());
    println!("Successfully created page manager");
    
    let mut page_manager = page_manager.unwrap();
    println!("Page manager created with initial state");
    
    // Try to allocate a page
    let page_id = page_manager.allocate_page();
    assert!(page_id.is_ok(), "Failed to allocate page: {:?}", page_id.err());
    println!("Successfully allocated page: {}", page_id.unwrap());
    
    println!("Test completed successfully!");
}

#[test]
fn test_page_manager_with_rwlock() {
    println!("Starting page manager with RwLock test...");
    
    let dir = tempdir().unwrap();
    println!("Created temp dir: {:?}", dir.path());
    
    let page_manager = Arc::new(RwLock::new(
        PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap()
    ));
    println!("Created Arc<RwLock<PageManager>>");
    
    // Try to get a write lock and allocate a page
    {
        println!("Acquiring write lock...");
        let mut pm = page_manager.write();
        println!("Acquired write lock");
        
        let page_id = pm.allocate_page();
        assert!(page_id.is_ok(), "Failed to allocate page: {:?}", page_id.err());
        println!("Successfully allocated page: {}", page_id.unwrap());
    }
    println!("Released write lock");
    
    println!("Test completed successfully!");
}
