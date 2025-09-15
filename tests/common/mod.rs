use lightning_db::Database;
use tempfile::TempDir;

// Simple init function that doesn't interfere with global state
pub fn init_test_logging() {
    // Do nothing - let the system handle logging naturally
}

pub struct TestDbConfig {
    pub use_memory: bool,
    pub cache_size: u64,
    pub max_active_transactions: usize,
}

impl Default for TestDbConfig {
    fn default() -> Self {
        Self {
            use_memory: true, // Default to in-memory for faster tests
            cache_size: 10 * 1024 * 1024, // 10MB
            max_active_transactions: 50,
        }
    }
}

pub struct TestDatabase {
    pub db: Database,
    pub _temp_dir: Option<TempDir>, // Keep alive if using disk
}

impl TestDatabase {
    pub fn new(_config: TestDbConfig) -> Self {
        // Always use temporary database for speed
        let db = Database::create_temp()
            .expect("Failed to create temporary database");

        Self {
            db,
            _temp_dir: None,
        }
    }

    pub fn new_fast() -> Self {
        // Use the fastest option - temporary database
        let db = Database::create_temp()
            .expect("Failed to create temporary database");

        Self {
            db,
            _temp_dir: None,
        }
    }

    pub fn new_with_disk() -> Self {
        Self::new(TestDbConfig {
            use_memory: false,
            ..Default::default()
        })
    }
}

// Reduced test data for faster execution
pub const FAST_TEST_SIZE: usize = 100;
pub const MEDIUM_TEST_SIZE: usize = 1000;
pub const LARGE_TEST_SIZE: usize = 10000;

// Optimized timeout constants
pub const FAST_TIMEOUT_MS: u64 = 10;
pub const MEDIUM_TIMEOUT_MS: u64 = 50;
pub const SLOW_TIMEOUT_MS: u64 = 100;

#[macro_export]
macro_rules! assert_timing {
    ($expr:expr, $max_duration_ms:expr) => {
        let start = std::time::Instant::now();
        let result = $expr;
        let duration = start.elapsed();
        assert!(
            duration.as_millis() <= $max_duration_ms,
            "Operation took {}ms, expected <= {}ms",
            duration.as_millis(),
            $max_duration_ms
        );
        result
    };
}

pub fn create_test_data(count: usize, key_prefix: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = format!("{}_{:06}", key_prefix, i);
            let value = format!("value_{:06}", i);
            (key.into_bytes(), value.into_bytes())
        })
        .collect()
}

pub fn populate_db_fast(db: &Database, count: usize, key_prefix: &str) {
    let data = create_test_data(count, key_prefix);
    db.put_batch(&data).expect("Failed to populate database");
}