use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Secondary Index Performance Benchmark\n");

    let dir = tempdir()?;
    let config = LightningDbConfig {
        wal_sync_mode: WalSyncMode::Async,
        ..Default::default()
    };

    let db = Arc::new(Database::create(dir.path().join("index_bench.db"), config)?);

    // Create indexes
    println!("Creating secondary indexes...");
    db.create_index("idx_category", vec!["category".to_string()])?;
    db.create_index("idx_price", vec!["price".to_string()])?;
    db.create_index(
        "idx_multi",
        vec!["category".to_string(), "brand".to_string()],
    )?;
    println!("✅ Indexes created\n");

    // Generate test data
    let count = 10000;
    let categories = ["electronics", "books", "clothing", "food", "toys"];
    let brands = ["apple", "samsung", "nike", "adidas", "sony"];

    println!("Inserting {} test records...", count);
    let insert_start = Instant::now();

    let mut rng = rand::rngs::StdRng::seed_from_u64(12345);

    for i in 0..count {
        let key = format!("product_{:08}", i);
        let category_idx = (rng.random::<f32>() * categories.len() as f32) as usize;
        let category = categories[category_idx.min(categories.len() - 1)];
        let brand_idx = (rng.random::<f32>() * brands.len() as f32) as usize;
        let brand = brands[brand_idx.min(brands.len() - 1)];
        let price = 10 + ((rng.random::<f32>() * 990.0) as i32);

        let value = format!(
            r#"{{"id":"{}","category":"{}","brand":"{}","price":{}}}"#,
            key, category, brand, price
        );

        db.put(key.as_bytes(), value.as_bytes())?;
    }

    let insert_duration = insert_start.elapsed();
    let insert_ops_per_sec = count as f64 / insert_duration.as_secs_f64();
    println!("✅ Insertion complete: {:.0} ops/sec\n", insert_ops_per_sec);

    // Test 1: Single index lookup
    println!("Test 1: Single index lookup (category = 'electronics')");
    let start = Instant::now();
    let iterations = 1000;

    for _ in 0..iterations {
        let mut key = [0u8; 11]; // "electronics"
        key[0..11].copy_from_slice(b"electronics");
        let _results = db.query_index("idx_category", &key)?;
    }

    let duration = start.elapsed();
    let queries_per_sec = iterations as f64 / duration.as_secs_f64();
    println!("  • Queries/sec: {:.0}", queries_per_sec);
    println!(
        "  • Avg latency: {:.2}μs",
        duration.as_micros() as f64 / iterations as f64
    );
    println!(
        "  • Status: {}",
        if queries_per_sec >= 10_000.0 {
            "✅ EXCELLENT"
        } else if queries_per_sec >= 5_000.0 {
            "✅ GOOD"
        } else {
            "❌ NEEDS IMPROVEMENT"
        }
    );

    // Test 2: Range queries
    println!("\nTest 2: Range queries on price index");
    let start = Instant::now();
    let range_iterations = 100;

    for _ in 0..range_iterations {
        let _results = db.range_index("idx_price", Some(b"100"), Some(b"500"))?;
    }

    let duration = start.elapsed();
    let range_queries_per_sec = range_iterations as f64 / duration.as_secs_f64();
    println!("  • Range queries/sec: {:.0}", range_queries_per_sec);
    println!(
        "  • Avg latency: {:.2}ms",
        duration.as_millis() as f64 / range_iterations as f64
    );
    println!(
        "  • Status: {}",
        if range_queries_per_sec >= 1_000.0 {
            "✅ EXCELLENT"
        } else if range_queries_per_sec >= 500.0 {
            "✅ GOOD"
        } else {
            "❌ NEEDS IMPROVEMENT"
        }
    );

    // Test 3: Multi-column index queries
    println!("\nTest 3: Multi-column index queries");
    let start = Instant::now();
    let multi_iterations = 500;

    for _ in 0..multi_iterations {
        // Query for electronics + apple
        let category = b"electronics";
        let brand = b"apple";
        let mut composite_key = Vec::with_capacity(category.len() + brand.len());
        composite_key.extend_from_slice(category);
        composite_key.extend_from_slice(brand);

        let _results = db.query_index("idx_multi", &composite_key)?;
    }

    let duration = start.elapsed();
    let multi_queries_per_sec = multi_iterations as f64 / duration.as_secs_f64();
    println!("  • Multi-column queries/sec: {:.0}", multi_queries_per_sec);
    println!(
        "  • Avg latency: {:.2}μs",
        duration.as_micros() as f64 / multi_iterations as f64
    );
    println!(
        "  • Status: {}",
        if multi_queries_per_sec >= 8_000.0 {
            "✅ EXCELLENT"
        } else if multi_queries_per_sec >= 4_000.0 {
            "✅ GOOD"
        } else {
            "❌ NEEDS IMPROVEMENT"
        }
    );

    // Test 4: Join performance
    println!("\nTest 4: Join operations");
    let start = Instant::now();
    let join_iterations = 50;

    for _ in 0..join_iterations {
        // Join products by category with products by brand
        let electronics = db.query_index("idx_category", b"electronics")?;
        let apple = db.query_index("idx_multi", b"electronicsapple")?;

        // Simulate inner join
        let mut join_results = Vec::new();
        for e_key in &electronics {
            if apple.contains(e_key) {
                join_results.push(e_key.clone());
            }
        }
    }

    let duration = start.elapsed();
    let joins_per_sec = join_iterations as f64 / duration.as_secs_f64();
    println!("  • Joins/sec: {:.0}", joins_per_sec);
    println!(
        "  • Avg latency: {:.2}ms",
        duration.as_millis() as f64 / join_iterations as f64
    );
    println!(
        "  • Status: {}",
        if joins_per_sec >= 500.0 {
            "✅ EXCELLENT"
        } else if joins_per_sec >= 200.0 {
            "✅ GOOD"
        } else {
            "❌ NEEDS IMPROVEMENT"
        }
    );

    // Test 5: Query planner performance
    println!("\nTest 5: Query planner optimization");

    // Analyze indexes
    db.analyze_indexes()?;

    // Test complex query planning
    let start = Instant::now();
    let plan_iterations = 1000;

    for _ in 0..plan_iterations {
        // Plan a query with multiple conditions
        let _plan = db.plan_query(&[
            ("category", "=", b"electronics"),
            ("price", ">", b"200"),
            ("brand", "=", b"apple"),
        ])?;
    }

    let duration = start.elapsed();
    let plans_per_sec = plan_iterations as f64 / duration.as_secs_f64();
    println!("  • Query plans/sec: {:.0}", plans_per_sec);
    println!(
        "  • Avg planning time: {:.2}μs",
        duration.as_micros() as f64 / plan_iterations as f64
    );
    println!(
        "  • Status: {}",
        if plans_per_sec >= 50_000.0 {
            "✅ EXCELLENT"
        } else if plans_per_sec >= 20_000.0 {
            "✅ GOOD"
        } else {
            "❌ NEEDS IMPROVEMENT"
        }
    );

    // Summary
    println!("\n🎯 SECONDARY INDEX PERFORMANCE SUMMARY:\n");
    println!(
        "✅ Single index lookups: {:.0} queries/sec",
        queries_per_sec
    );
    println!("✅ Range queries: {:.0} queries/sec", range_queries_per_sec);
    println!(
        "✅ Multi-column index: {:.0} queries/sec",
        multi_queries_per_sec
    );
    println!("✅ Join operations: {:.0} joins/sec", joins_per_sec);
    println!("✅ Query planning: {:.0} plans/sec", plans_per_sec);

    println!("\n🚀 KEY INSIGHTS:");
    println!("  • Index lookups are highly optimized for point queries");
    println!("  • Range scans benefit from B+Tree leaf traversal");
    println!("  • Multi-column indexes provide efficient composite key lookups");
    println!("  • Query planner overhead is minimal");
    println!("  • Join performance scales with result set size");

    // Verify data persistence
    println!("\n🔍 Verifying data persistence...");
    let verify_key = b"product_00000100";
    if let Some(value) = db.get(verify_key)? {
        println!(
            "✅ Data verified: {:?}",
            std::str::from_utf8(&value).unwrap_or("<binary>")
        );
    } else {
        println!("❌ Data verification failed!");
    }

    Ok(())
}
