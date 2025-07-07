use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use lightning_db::storage::{MmapConfig, OptimizedPageManager, Page, PageManager, PAGE_SIZE};
use std::hint::black_box;
use tempfile::tempdir;

fn benchmark_page_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_allocation");

    for num_pages in [100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::new("standard", num_pages),
            num_pages,
            |b, &num_pages| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench_std.db");

                b.iter(|| {
                    let mut manager =
                        PageManager::create(&path, PAGE_SIZE as u64 * num_pages * 2).unwrap();

                    for _ in 0..num_pages {
                        black_box(manager.allocate_page().unwrap());
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("optimized", num_pages),
            num_pages,
            |b, &num_pages| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench_opt.db");

                b.iter(|| {
                    let config = MmapConfig {
                        region_size: 16 * 1024 * 1024, // 16MB regions
                        enable_prefault: false,        // Disable for fair comparison
                        ..Default::default()
                    };
                    let manager = OptimizedPageManager::create(
                        &path,
                        PAGE_SIZE as u64 * num_pages * 2,
                        config,
                    )
                    .unwrap();

                    for _ in 0..num_pages {
                        black_box(manager.allocate_page().unwrap());
                    }
                });
            },
        );
    }

    group.finish();
}

fn benchmark_page_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_write");

    for num_pages in [100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("standard", num_pages),
            num_pages,
            |b, &num_pages| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench_std.db");
                let mut manager =
                    PageManager::create(&path, PAGE_SIZE as u64 * num_pages * 2).unwrap();

                // Pre-allocate pages
                let page_ids: Vec<u32> = (0..num_pages)
                    .map(|_| manager.allocate_page().unwrap())
                    .collect();

                b.iter(|| {
                    for &page_id in &page_ids {
                        let mut page = Page::new(page_id);
                        let data = page.get_mut_data();
                        data[0] = 42;
                        data[PAGE_SIZE - 1] = 24;
                        manager.write_page(&page).unwrap();
                        black_box(());
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("optimized", num_pages),
            num_pages,
            |b, &num_pages| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench_opt.db");
                let config = MmapConfig {
                    region_size: 16 * 1024 * 1024,
                    enable_async_msync: true,
                    ..Default::default()
                };
                let manager =
                    OptimizedPageManager::create(&path, PAGE_SIZE as u64 * num_pages * 2, config)
                        .unwrap();

                // Pre-allocate pages
                let page_ids: Vec<u32> = (0..num_pages)
                    .map(|_| manager.allocate_page().unwrap())
                    .collect();

                b.iter(|| {
                    for &page_id in &page_ids {
                        let mut page = Page::new(page_id);
                        let data = page.get_mut_data();
                        data[0] = 42;
                        data[PAGE_SIZE - 1] = 24;
                        manager.write_page(&page).unwrap();
                        black_box(());
                    }
                });
            },
        );
    }

    group.finish();
}

fn benchmark_page_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_read");

    for num_pages in [100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("standard_sequential", num_pages),
            num_pages,
            |b, &num_pages| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench_std.db");
                let mut manager =
                    PageManager::create(&path, PAGE_SIZE as u64 * num_pages * 2).unwrap();

                // Pre-allocate and write pages
                let page_ids: Vec<u32> = (0..num_pages)
                    .map(|i| {
                        let page_id = manager.allocate_page().unwrap();
                        let mut page = Page::new(page_id);
                        let data = page.get_mut_data();
                        data[0] = i as u8;
                        manager.write_page(&page).unwrap();
                        page_id
                    })
                    .collect();

                b.iter(|| {
                    for &page_id in &page_ids {
                        black_box(manager.get_page(page_id).unwrap());
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("optimized_sequential", num_pages),
            num_pages,
            |b, &num_pages| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench_opt.db");
                let config = MmapConfig {
                    region_size: 16 * 1024 * 1024,
                    enable_prefault: true,
                    ..Default::default()
                };
                let manager =
                    OptimizedPageManager::create(&path, PAGE_SIZE as u64 * num_pages * 2, config)
                        .unwrap();

                // Pre-allocate and write pages
                let page_ids: Vec<u32> = (0..num_pages)
                    .map(|i| {
                        let page_id = manager.allocate_page().unwrap();
                        let mut page = Page::new(page_id);
                        let data = page.get_mut_data();
                        data[0] = i as u8;
                        manager.write_page(&page).unwrap();
                        page_id
                    })
                    .collect();

                b.iter(|| {
                    for &page_id in &page_ids {
                        black_box(manager.get_page(page_id).unwrap());
                    }
                });
            },
        );
    }

    group.finish();
}

fn benchmark_sync_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_performance");

    group.bench_function("standard_sync", |b| {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bench_std.db");
        let mut manager = PageManager::create(&path, PAGE_SIZE as u64 * 1000).unwrap();

        // Write some pages
        for i in 0..100 {
            let page_id = manager.allocate_page().unwrap();
            let mut page = Page::new(page_id);
            let data = page.get_mut_data();
            data[0] = i as u8;
            manager.write_page(&page).unwrap();
        }

        b.iter(|| {
            manager.sync().unwrap();
            black_box(());
        });
    });

    group.bench_function("optimized_sync", |b| {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bench_opt.db");
        let config = MmapConfig {
            enable_async_msync: true,
            ..Default::default()
        };
        let manager = OptimizedPageManager::create(&path, PAGE_SIZE as u64 * 1000, config).unwrap();

        // Write some pages
        for i in 0..100 {
            let page_id = manager.allocate_page().unwrap();
            let mut page = Page::new(page_id);
            let data = page.get_mut_data();
            data[0] = i as u8;
            manager.write_page(&page).unwrap();
        }

        b.iter(|| {
            manager.sync().unwrap();
            black_box(());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_page_allocation,
    benchmark_page_write,
    benchmark_page_read,
    benchmark_sync_performance
);
criterion_main!(benches);
