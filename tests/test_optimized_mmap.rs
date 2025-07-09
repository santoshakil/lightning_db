use lightning_db::storage::{MmapConfig, OptimizedPageManager, Page};
use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_optimized_page_manager_basic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_optimized.db");

    let config = MmapConfig {
        enable_huge_pages: false,
        enable_prefault: true,
        enable_async_msync: true,
        max_mapped_regions: 8,
        region_size: 1024 * 1024, // 1MB regions
        ..Default::default()
    };

    let manager = OptimizedPageManager::create(&path, 4096 * 16, config).unwrap();

    // Test page allocation
    let page_id1 = manager.allocate_page().unwrap();
    assert_eq!(page_id1, 1); // Page 0 is header

    let page_id2 = manager.allocate_page().unwrap();
    assert_eq!(page_id2, 2);

    // Test page write and read
    let mut page = Page::new(page_id1);
    let data = page.get_mut_data();
    // Write test pattern, starting after header (16 bytes)
    for (i, item) in data.iter_mut().enumerate().skip(16).take(240) {
        *item = (i - 16) as u8;
    }

    manager.write_page(&page).unwrap();

    // Read back the page
    let read_page = manager.get_page(page_id1).unwrap();
    let read_data = read_page.get_data();
    // Verify data after header
    for (i, &item) in read_data.iter().enumerate().skip(16).take(240) {
        assert_eq!(item, (i - 16) as u8);
    }

    // Test page freeing and reallocation
    manager.free_page(page_id1);
    let realloc_id = manager.allocate_page().unwrap();
    assert_eq!(realloc_id, page_id1);

    // Test statistics
    let stats = manager.get_statistics();
    println!("Optimized Page Manager Stats:");
    println!("  Total pages: {}", stats.page_count);
    println!("  Free pages: {}", stats.free_page_count);
    println!("  Mmap regions: {}", stats.mmap_stats.total_regions);
    println!("  Page faults: {}", stats.mmap_stats.page_faults);
    println!("  Bytes synced: {}", stats.mmap_stats.bytes_synced);
}

#[test]
fn test_optimized_page_manager_performance() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_perf.db");

    let config = MmapConfig {
        enable_prefault: true,
        enable_async_msync: true,
        region_size: 16 * 1024 * 1024, // 16MB regions
        max_mapped_regions: 16,
        ..Default::default()
    };

    let manager = OptimizedPageManager::create(&path, 1024 * 1024 * 64, config).unwrap();

    const NUM_PAGES: u32 = 1000;
    const PAGE_DATA_SIZE: usize = 1024;

    // Allocate and write pages
    let mut page_ids = Vec::with_capacity(NUM_PAGES as usize);

    let start = Instant::now();
    for i in 0..NUM_PAGES {
        let page_id = manager.allocate_page().unwrap();
        page_ids.push(page_id);

        let mut page = Page::new(page_id);
        let data = page.get_mut_data();
        // Write pattern to page (skip header)
        for (j, item) in data.iter_mut().enumerate().skip(16) {
            *item = ((i + j as u32) % 256) as u8;
        }

        manager.write_page(&page).unwrap();
    }
    let write_duration = start.elapsed();

    // Sync to disk
    let sync_start = Instant::now();
    manager.sync().unwrap();
    let sync_duration = sync_start.elapsed();

    // Read pages back in random order
    use rand::{thread_rng, seq::SliceRandom};

    let mut rng = thread_rng();
    page_ids.shuffle(&mut rng);

    let read_start = Instant::now();
    for &page_id in &page_ids {
        let page = manager.get_page(page_id).unwrap();
        let data = page.get_data();
        // Verify pattern (skip header)
        let i = page_id - 1; // Adjust for header page
        for (j, &item) in data.iter().enumerate().skip(16) {
            assert_eq!(item, ((i + j as u32) % 256) as u8);
        }
    }
    let read_duration = read_start.elapsed();

    println!("\nOptimized Page Manager Performance:");
    println!("  Pages: {}", NUM_PAGES);
    println!(
        "  Write time: {:?} ({:.2} pages/sec)",
        write_duration,
        NUM_PAGES as f64 / write_duration.as_secs_f64()
    );
    println!("  Sync time: {:?}", sync_duration);
    println!(
        "  Random read time: {:?} ({:.2} pages/sec)",
        read_duration,
        NUM_PAGES as f64 / read_duration.as_secs_f64()
    );

    let stats = manager.get_statistics();
    println!("\nFinal Statistics:");
    println!("  Total regions: {}", stats.mmap_stats.total_regions);
    println!("  Region creates: {}", stats.mmap_stats.region_creates);
    println!("  Region evictions: {}", stats.mmap_stats.region_evictions);
    println!("  Page faults: {}", stats.mmap_stats.page_faults);
    println!("  Bytes synced: {}", stats.mmap_stats.bytes_synced);
}

#[test]
fn test_database_with_optimized_mmap() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_db");

    // Create database with optimized page manager
    let config = LightningDbConfig {
        use_optimized_page_manager: true,
        mmap_config: Some(MmapConfig {
            enable_prefault: true,
            enable_async_msync: true,
            region_size: 8 * 1024 * 1024, // 8MB regions
            max_mapped_regions: 32,
            ..Default::default()
        }),
        ..Default::default()
    };

    let db = Database::create(&db_path, config).unwrap();

    // Test basic operations
    const NUM_KEYS: usize = 100;

    let start = Instant::now();
    for i in 0..NUM_KEYS {
        let key = format!("key_{:04}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    let write_duration = start.elapsed();

    // Read keys back
    let read_start = Instant::now();
    for i in 0..NUM_KEYS {
        let key = format!("key_{:04}", i);
        let value = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(value, format!("value_{}", i).as_bytes());
    }
    let read_duration = read_start.elapsed();

    println!("\nDatabase with Optimized Mmap:");
    println!("  Keys written: {}", NUM_KEYS);
    println!(
        "  Write time: {:?} ({:.2} ops/sec)",
        write_duration,
        NUM_KEYS as f64 / write_duration.as_secs_f64()
    );
    println!(
        "  Read time: {:?} ({:.2} ops/sec)",
        read_duration,
        NUM_KEYS as f64 / read_duration.as_secs_f64()
    );
}
