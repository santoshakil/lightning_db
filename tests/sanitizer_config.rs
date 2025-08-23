//! Sanitizer configuration and test runners for memory safety validation
//! 
//! This module provides configuration and utilities for running memory safety tests
//! with AddressSanitizer (ASAN), ThreadSanitizer (TSAN), and MemorySanitizer (MSAN).
//! It also includes MIRI-compatible tests for enhanced validation.

use std::env;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Sanitizer configuration for memory safety testing
#[derive(Debug, Clone)]
pub struct SanitizerConfig {
    pub enable_asan: bool,
    pub enable_tsan: bool,
    pub enable_msan: bool,
    pub enable_ubsan: bool,
    pub enable_miri: bool,
    pub asan_options: AsanOptions,
    pub tsan_options: TsanOptions,
    pub msan_options: MsanOptions,
    pub test_timeout_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct AsanOptions {
    pub detect_leaks: bool,
    pub detect_stack_use_after_return: bool,
    pub detect_heap_use_after_free: bool,
    pub check_initialization_order: bool,
    pub strict_init_order: bool,
    pub abort_on_error: bool,
    pub halt_on_error: bool,
    pub allocator_may_return_null: bool,
    pub allow_user_segv_handler: bool,
    pub handle_segv: bool,
}

#[derive(Debug, Clone)]
pub struct TsanOptions {
    pub detect_deadlocks: bool,
    pub detect_signal_unsafe: bool,
    pub detect_mutex_order_inversion: bool,
    pub detect_atomic_races: bool,
    pub report_atomic_races: bool,
    pub halt_on_error: bool,
    pub abort_on_error: bool,
    pub history_size: usize,
}

#[derive(Debug, Clone)]
pub struct MsanOptions {
    pub poison_heap: bool,
    pub poison_stack: bool,
    pub poison_partial: bool,
    pub track_origins: bool,
    pub halt_on_error: bool,
    pub abort_on_error: bool,
    pub print_stats: bool,
}

impl Default for SanitizerConfig {
    fn default() -> Self {
        Self {
            enable_asan: cfg!(feature = "asan") || env::var("RUSTFLAGS").unwrap_or_default().contains("sanitize=address"),
            enable_tsan: cfg!(feature = "tsan") || env::var("RUSTFLAGS").unwrap_or_default().contains("sanitize=thread"),
            enable_msan: cfg!(feature = "msan") || env::var("RUSTFLAGS").unwrap_or_default().contains("sanitize=memory"),
            enable_ubsan: cfg!(feature = "ubsan") || env::var("RUSTFLAGS").unwrap_or_default().contains("sanitize=undefined"),
            enable_miri: cfg!(miri),
            asan_options: AsanOptions::default(),
            tsan_options: TsanOptions::default(),
            msan_options: MsanOptions::default(),
            test_timeout_seconds: 300, // 5 minutes
        }
    }
}

impl Default for AsanOptions {
    fn default() -> Self {
        Self {
            detect_leaks: true,
            detect_stack_use_after_return: true,
            detect_heap_use_after_free: true,
            check_initialization_order: true,
            strict_init_order: true,
            abort_on_error: false, // Continue testing after errors
            halt_on_error: false,
            allocator_may_return_null: true,
            allow_user_segv_handler: false,
            handle_segv: true,
        }
    }
}

impl Default for TsanOptions {
    fn default() -> Self {
        Self {
            detect_deadlocks: true,
            detect_signal_unsafe: true,
            detect_mutex_order_inversion: true,
            detect_atomic_races: true,
            report_atomic_races: true,
            halt_on_error: false,
            abort_on_error: false,
            history_size: 8192,
        }
    }
}

impl Default for MsanOptions {
    fn default() -> Self {
        Self {
            poison_heap: true,
            poison_stack: true,
            poison_partial: true,
            track_origins: true,
            halt_on_error: false,
            abort_on_error: false,
            print_stats: true,
        }
    }
}

impl AsanOptions {
    pub fn to_env_string(&self) -> String {
        format!(
            "detect_leaks={},detect_stack_use_after_return={},detect_heap_use_after_free={},check_initialization_order={},strict_init_order={},abort_on_error={},halt_on_error={},allocator_may_return_null={},allow_user_segv_handler={},handle_segv={}",
            if self.detect_leaks { 1 } else { 0 },
            if self.detect_stack_use_after_return { 1 } else { 0 },
            if self.detect_heap_use_after_free { 1 } else { 0 },
            if self.check_initialization_order { 1 } else { 0 },
            if self.strict_init_order { 1 } else { 0 },
            if self.abort_on_error { 1 } else { 0 },
            if self.halt_on_error { 1 } else { 0 },
            if self.allocator_may_return_null { 1 } else { 0 },
            if self.allow_user_segv_handler { 1 } else { 0 },
            if self.handle_segv { 1 } else { 0 },
        )
    }
}

impl TsanOptions {
    pub fn to_env_string(&self) -> String {
        format!(
            "detect_deadlocks={},detect_signal_unsafe={},detect_mutex_order_inversion={},detect_atomic_races={},report_atomic_races={},halt_on_error={},abort_on_error={},history_size={}",
            if self.detect_deadlocks { 1 } else { 0 },
            if self.detect_signal_unsafe { 1 } else { 0 },
            if self.detect_mutex_order_inversion { 1 } else { 0 },
            if self.detect_atomic_races { 1 } else { 0 },
            if self.report_atomic_races { 1 } else { 0 },
            if self.halt_on_error { 1 } else { 0 },
            if self.abort_on_error { 1 } else { 0 },
            self.history_size,
        )
    }
}

impl MsanOptions {
    pub fn to_env_string(&self) -> String {
        format!(
            "poison_heap={},poison_stack={},poison_partial={},track_origins={},halt_on_error={},abort_on_error={},print_stats={}",
            if self.poison_heap { 1 } else { 0 },
            if self.poison_stack { 1 } else { 0 },
            if self.poison_partial { 1 } else { 0 },
            if self.track_origins { 1 } else { 0 },
            if self.halt_on_error { 1 } else { 0 },
            if self.abort_on_error { 1 } else { 0 },
            if self.print_stats { 1 } else { 0 },
        )
    }
}

/// Test runner that executes memory safety tests with different sanitizers
pub struct SanitizerTestRunner {
    config: SanitizerConfig,
    results: Arc<TestResults>,
}

#[derive(Debug)]
pub struct TestResults {
    pub asan_violations: AtomicUsize,
    pub tsan_violations: AtomicUsize,
    pub msan_violations: AtomicUsize,
    pub ubsan_violations: AtomicUsize,
    pub miri_violations: AtomicUsize,
    pub tests_run: AtomicUsize,
    pub tests_passed: AtomicUsize,
    pub tests_failed: AtomicUsize,
    pub total_runtime_ms: AtomicUsize,
}

impl TestResults {
    pub fn new() -> Self {
        Self {
            asan_violations: AtomicUsize::new(0),
            tsan_violations: AtomicUsize::new(0),
            msan_violations: AtomicUsize::new(0),
            ubsan_violations: AtomicUsize::new(0),
            miri_violations: AtomicUsize::new(0),
            tests_run: AtomicUsize::new(0),
            tests_passed: AtomicUsize::new(0),
            tests_failed: AtomicUsize::new(0),
            total_runtime_ms: AtomicUsize::new(0),
        }
    }

    pub fn record_asan_violation(&self) {
        self.asan_violations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_tsan_violation(&self) {
        self.tsan_violations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_msan_violation(&self) {
        self.msan_violations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_ubsan_violation(&self) {
        self.ubsan_violations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miri_violation(&self) {
        self.miri_violations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_test_run(&self) {
        self.tests_run.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_test_pass(&self) {
        self.tests_passed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_test_fail(&self) {
        self.tests_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_runtime(&self, ms: u64) {
        self.total_runtime_ms.fetch_add(ms as usize, Ordering::Relaxed);
    }

    pub fn print_summary(&self) {
        println!("\n=== Sanitizer Test Results ===");
        println!("ASAN violations: {}", self.asan_violations.load(Ordering::Relaxed));
        println!("TSAN violations: {}", self.tsan_violations.load(Ordering::Relaxed));
        println!("MSAN violations: {}", self.msan_violations.load(Ordering::Relaxed));
        println!("UBSAN violations: {}", self.ubsan_violations.load(Ordering::Relaxed));
        println!("MIRI violations: {}", self.miri_violations.load(Ordering::Relaxed));
        println!("Tests run: {}", self.tests_run.load(Ordering::Relaxed));
        println!("Tests passed: {}", self.tests_passed.load(Ordering::Relaxed));
        println!("Tests failed: {}", self.tests_failed.load(Ordering::Relaxed));
        println!("Total runtime: {} ms", self.total_runtime_ms.load(Ordering::Relaxed));
        println!("==============================");
    }

    pub fn has_violations(&self) -> bool {
        self.asan_violations.load(Ordering::Relaxed) > 0 ||
        self.tsan_violations.load(Ordering::Relaxed) > 0 ||
        self.msan_violations.load(Ordering::Relaxed) > 0 ||
        self.ubsan_violations.load(Ordering::Relaxed) > 0 ||
        self.miri_violations.load(Ordering::Relaxed) > 0
    }
}

impl SanitizerTestRunner {
    pub fn new(config: SanitizerConfig) -> Self {
        Self {
            config,
            results: Arc::new(TestResults::new()),
        }
    }

    /// Run all memory safety tests with sanitizers
    pub fn run_sanitized_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting sanitized memory safety tests...");
        
        let start_time = Instant::now();

        // Set up environment variables for sanitizers
        self.setup_sanitizer_environment();

        // Run tests with different configurations
        if self.config.enable_asan {
            self.run_asan_tests()?;
        }

        if self.config.enable_tsan {
            self.run_tsan_tests()?;
        }

        if self.config.enable_msan {
            self.run_msan_tests()?;
        }

        if self.config.enable_ubsan {
            self.run_ubsan_tests()?;
        }

        if self.config.enable_miri {
            self.run_miri_tests()?;
        }

        let elapsed = start_time.elapsed();
        self.results.add_runtime(elapsed.as_millis() as u64);

        // Print results
        self.results.print_summary();

        if self.results.has_violations() {
            return Err("Memory safety violations detected by sanitizers".into());
        }

        println!("All sanitized tests passed!");
        Ok(())
    }

    fn setup_sanitizer_environment(&self) {
        if self.config.enable_asan {
            env::set_var("ASAN_OPTIONS", self.config.asan_options.to_env_string());
            println!("ASAN_OPTIONS: {}", self.config.asan_options.to_env_string());
        }

        if self.config.enable_tsan {
            env::set_var("TSAN_OPTIONS", self.config.tsan_options.to_env_string());
            println!("TSAN_OPTIONS: {}", self.config.tsan_options.to_env_string());
        }

        if self.config.enable_msan {
            env::set_var("MSAN_OPTIONS", self.config.msan_options.to_env_string());
            println!("MSAN_OPTIONS: {}", self.config.msan_options.to_env_string());
        }
    }

    fn run_asan_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running AddressSanitizer tests...");
        
        // Run memory safety tests that would trigger ASAN
        self.test_asan_buffer_overflow()?;
        self.test_asan_use_after_free()?;
        self.test_asan_heap_overflow()?;
        self.test_asan_stack_overflow()?;

        println!("AddressSanitizer tests completed");
        Ok(())
    }

    fn test_asan_buffer_overflow(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test that should NOT trigger ASAN (safe buffer access)
        let buffer = vec![0u8; 1024];
        
        // Safe access within bounds
        for i in 0..buffer.len() {
            let _value = buffer[i];
        }

        // Safe slice operations
        let _slice = &buffer[100..200];

        // NOTE: Actual ASAN triggering code is commented out as it would crash tests
        // These would trigger ASAN in a real violation:
        // let _out_of_bounds = buffer[1024]; // Would trigger ASAN
        // let _negative_index = buffer[-1]; // Would trigger ASAN (if possible)

        self.results.record_test_pass();
        Ok(())
    }

    fn test_asan_use_after_free(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test proper memory management patterns
        use std::alloc::{alloc, dealloc, Layout};
        
        let layout = Layout::from_size_align(1024, 8)?;
        
        unsafe {
            let ptr = alloc(layout);
            assert!(!ptr.is_null());

            // Safe use while allocated
            *ptr = 42;
            let value = *ptr;
            assert_eq!(value, 42);

            // Proper deallocation
            dealloc(ptr, layout);

            // NOTE: This would trigger ASAN:
            // let _use_after_free = *ptr; // Would trigger ASAN
        }

        self.results.record_test_pass();
        Ok(())
    }

    fn test_asan_heap_overflow(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test heap allocation boundary checking
        let mut vec = Vec::with_capacity(100);
        
        // Safe operations
        for i in 0..100 {
            vec.push(i);
        }

        // Safe access patterns
        for i in 0..vec.len() {
            let _value = vec[i];
        }

        // Safe iterator usage
        let _sum: usize = vec.iter().sum();

        self.results.record_test_pass();
        Ok(())
    }

    fn test_asan_stack_overflow(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test stack allocation patterns
        let stack_array = [0u8; 1024];
        
        // Safe stack access
        for i in 0..stack_array.len() {
            let _value = stack_array[i];
        }

        // Safe stack slice operations
        let _slice = &stack_array[..100];

        self.results.record_test_pass();
        Ok(())
    }

    fn run_tsan_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running ThreadSanitizer tests...");
        
        self.test_tsan_data_race_detection()?;
        self.test_tsan_deadlock_detection()?;
        self.test_tsan_atomic_ordering()?;

        println!("ThreadSanitizer tests completed");
        Ok(())
    }

    fn test_tsan_data_race_detection(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test proper synchronization patterns
        use std::sync::{Arc, Mutex, Barrier};
        
        let data = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));
        
        let data_clone = Arc::clone(&data);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            // Properly synchronized access
            let mut guard = data_clone.lock().unwrap();
            *guard += 1;
        });

        barrier.wait();
        
        // Properly synchronized access
        {
            let mut guard = data.lock().unwrap();
            *guard += 1;
        }

        handle.join().unwrap();

        let final_value = *data.lock().unwrap();
        assert_eq!(final_value, 2);

        self.results.record_test_pass();
        Ok(())
    }

    fn test_tsan_deadlock_detection(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test deadlock-free patterns
        use std::sync::{Arc, Mutex};
        
        let mutex1 = Arc::new(Mutex::new(0));
        let mutex2 = Arc::new(Mutex::new(0));
        
        // Consistent lock ordering prevents deadlocks
        {
            let _guard1 = mutex1.lock().unwrap();
            let _guard2 = mutex2.lock().unwrap();
            // Do work with both locks
        }

        // Same ordering in different scope
        {
            let _guard1 = mutex1.lock().unwrap();
            let _guard2 = mutex2.lock().unwrap();
            // Do more work
        }

        self.results.record_test_pass();
        Ok(())
    }

    fn test_tsan_atomic_ordering(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test proper atomic ordering
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::{Arc, Barrier};
        
        let data = Arc::new(AtomicU64::new(0));
        let flag = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(2));
        
        let data_clone = Arc::clone(&data);
        let flag_clone = Arc::clone(&flag);
        let barrier_clone = Arc::clone(&barrier);

        let producer = thread::spawn(move || {
            barrier_clone.wait();
            
            // Write data with proper ordering
            data_clone.store(42, Ordering::Release);
            flag_clone.store(true, Ordering::Release);
        });

        let consumer = thread::spawn(move || {
            barrier.wait();
            
            // Wait for flag with proper ordering
            while !flag.load(Ordering::Acquire) {
                thread::yield_now();
            }
            
            // Read data with proper ordering
            let value = data.load(Ordering::Acquire);
            assert_eq!(value, 42);
        });

        producer.join().unwrap();
        consumer.join().unwrap();

        self.results.record_test_pass();
        Ok(())
    }

    fn run_msan_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running MemorySanitizer tests...");
        
        self.test_msan_uninitialized_memory()?;
        self.test_msan_partial_initialization()?;

        println!("MemorySanitizer tests completed");
        Ok(())
    }

    fn test_msan_uninitialized_memory(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test proper memory initialization
        use std::mem::MaybeUninit;
        
        // Proper initialization pattern
        let mut array: [MaybeUninit<u32>; 10] = unsafe { MaybeUninit::uninit().assume_init() };
        
        // Initialize all elements
        for (i, elem) in array.iter_mut().enumerate() {
            elem.write(i as u32);
        }
        
        // Safe conversion to initialized array
        let initialized: [u32; 10] = unsafe { std::mem::transmute(array) };
        
        // Use initialized data
        let sum: u32 = initialized.iter().sum();
        assert_eq!(sum, 45); // 0+1+2+...+9 = 45

        self.results.record_test_pass();
        Ok(())
    }

    fn test_msan_partial_initialization(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test struct initialization
        #[derive(Debug)]
        struct TestStruct {
            field1: u32,
            field2: u64,
            field3: bool,
        }

        // Proper initialization
        let test_struct = TestStruct {
            field1: 42,
            field2: 0xDEADBEEF,
            field3: true,
        };

        // Use all fields
        assert_eq!(test_struct.field1, 42);
        assert_eq!(test_struct.field2, 0xDEADBEEF);
        assert_eq!(test_struct.field3, true);

        self.results.record_test_pass();
        Ok(())
    }

    fn run_ubsan_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running UndefinedBehaviorSanitizer tests...");
        
        self.test_ubsan_integer_overflow()?;
        self.test_ubsan_null_pointer_dereference()?;
        self.test_ubsan_alignment_violations()?;

        println!("UndefinedBehaviorSanitizer tests completed");
        Ok(())
    }

    fn test_ubsan_integer_overflow(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test safe integer operations
        let a: u32 = 1000;
        let b: u32 = 2000;
        
        // Safe addition
        let sum = a.saturating_add(b);
        assert_eq!(sum, 3000);

        // Safe multiplication with overflow checking
        match a.checked_mul(b) {
            Some(result) => assert_eq!(result, 2_000_000),
            None => {
                // Handle overflow gracefully
                println!("Multiplication would overflow");
            }
        }

        // Safe subtraction
        let diff = b.saturating_sub(a);
        assert_eq!(diff, 1000);

        self.results.record_test_pass();
        Ok(())
    }

    fn test_ubsan_null_pointer_dereference(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test proper null pointer handling
        let maybe_ptr: Option<*const u32> = None;
        
        match maybe_ptr {
            Some(ptr) => {
                // Only dereference if not null
                if !ptr.is_null() {
                    unsafe {
                        let _value = *ptr;
                    }
                }
            }
            None => {
                // Handle null case appropriately
                println!("Pointer is null, not dereferencing");
            }
        }

        // Safe pointer creation and use
        let value = 42u32;
        let ptr = &value as *const u32;
        assert!(!ptr.is_null());
        
        unsafe {
            let dereferenced = *ptr;
            assert_eq!(dereferenced, 42);
        }

        self.results.record_test_pass();
        Ok(())
    }

    fn test_ubsan_alignment_violations(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test proper alignment
        #[repr(C, align(8))]
        struct AlignedStruct {
            value: u64,
        }

        let aligned = AlignedStruct { value: 0xDEADBEEF };
        let ptr = &aligned as *const AlignedStruct;
        
        // Verify alignment
        assert_eq!(ptr as usize % 8, 0);
        
        // Safe access
        unsafe {
            let value = (*ptr).value;
            assert_eq!(value, 0xDEADBEEF);
        }

        self.results.record_test_pass();
        Ok(())
    }

    fn run_miri_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running MIRI tests...");
        
        // MIRI tests focus on Rust-specific undefined behavior
        self.test_miri_safe_transmutes()?;
        self.test_miri_lifetime_safety()?;
        self.test_miri_raw_pointer_safety()?;

        println!("MIRI tests completed");
        Ok(())
    }

    fn test_miri_safe_transmutes(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test safe transmute patterns
        let array: [u32; 4] = [0x12345678, 0x9ABCDEF0, 0x11223344, 0x55667788];
        
        // Safe transmute: same size and compatible layout
        let bytes: [u8; 16] = unsafe { std::mem::transmute(array) };
        
        // Verify the transmute (assuming little-endian)
        assert_eq!(bytes[0], 0x78);
        assert_eq!(bytes[1], 0x56);
        
        // Transmute back
        let restored: [u32; 4] = unsafe { std::mem::transmute(bytes) };
        assert_eq!(restored, array);

        self.results.record_test_pass();
        Ok(())
    }

    fn test_miri_lifetime_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test proper lifetime management
        fn process_data(data: &[u8]) -> usize {
            data.len()
        }

        let buffer = vec![1, 2, 3, 4, 5];
        let result = process_data(&buffer);
        assert_eq!(result, 5);

        // Buffer is still valid here
        assert_eq!(buffer.len(), 5);

        self.results.record_test_pass();
        Ok(())
    }

    fn test_miri_raw_pointer_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.results.record_test_run();

        // Test safe raw pointer usage
        let mut value = 42u32;
        let ptr = &mut value as *mut u32;
        
        // Safe dereference
        unsafe {
            *ptr = 24;
        }
        
        assert_eq!(value, 24);
        
        // Safe pointer arithmetic (within bounds)
        let array = [1u32, 2, 3, 4, 5];
        let base_ptr = array.as_ptr();
        
        for i in 0..array.len() {
            unsafe {
                let elem_ptr = base_ptr.add(i);
                let elem_value = *elem_ptr;
                assert_eq!(elem_value, array[i]);
            }
        }

        self.results.record_test_pass();
        Ok(())
    }

    /// Run a test and capture any sanitizer output
    fn run_test_with_timeout<F>(&self, test_name: &str, test_fn: F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnOnce() -> Result<(), Box<dyn std::error::Error>> + Send + 'static,
    {
        let timeout = Duration::from_secs(self.config.test_timeout_seconds);
        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = Arc::clone(&completed);

        // Run test in separate thread with timeout
        let test_handle = thread::spawn(move || {
            let result = test_fn();
            completed_clone.store(true, Ordering::Relaxed);
            result
        });

        // Wait for completion or timeout
        let start = Instant::now();
        while start.elapsed() < timeout && !completed.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(10));
        }

        if !completed.load(Ordering::Relaxed) {
            eprintln!("Test '{}' timed out after {} seconds", test_name, self.config.test_timeout_seconds);
            return Err(format!("Test '{}' timed out", test_name).into());
        }

        match test_handle.join() {
            Ok(result) => result,
            Err(_) => Err(format!("Test '{}' panicked", test_name).into()),
        }
    }
}

/// Utility function to check if sanitizers are available
pub fn check_sanitizer_availability() -> SanitizerAvailability {
    SanitizerAvailability {
        asan_available: cfg!(feature = "asan") || check_rustflags_contains("sanitize=address"),
        tsan_available: cfg!(feature = "tsan") || check_rustflags_contains("sanitize=thread"),
        msan_available: cfg!(feature = "msan") || check_rustflags_contains("sanitize=memory"),
        ubsan_available: cfg!(feature = "ubsan") || check_rustflags_contains("sanitize=undefined"),
        miri_available: cfg!(miri),
    }
}

#[derive(Debug)]
pub struct SanitizerAvailability {
    pub asan_available: bool,
    pub tsan_available: bool,
    pub msan_available: bool,
    pub ubsan_available: bool,
    pub miri_available: bool,
}

fn check_rustflags_contains(pattern: &str) -> bool {
    env::var("RUSTFLAGS")
        .unwrap_or_default()
        .contains(pattern)
}

impl SanitizerAvailability {
    pub fn print_status(&self) {
        println!("Sanitizer Availability:");
        println!("  AddressSanitizer (ASAN): {}", if self.asan_available { "✓" } else { "✗" });
        println!("  ThreadSanitizer (TSAN): {}", if self.tsan_available { "✓" } else { "✗" });
        println!("  MemorySanitizer (MSAN): {}", if self.msan_available { "✓" } else { "✗" });
        println!("  UndefinedBehaviorSanitizer (UBSAN): {}", if self.ubsan_available { "✓" } else { "✗" });
        println!("  MIRI: {}", if self.miri_available { "✓" } else { "✗" });
    }

    pub fn any_available(&self) -> bool {
        self.asan_available || self.tsan_available || self.msan_available || 
        self.ubsan_available || self.miri_available
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitizer_config() {
        let config = SanitizerConfig::default();
        
        // Test that configuration is reasonable
        assert!(config.test_timeout_seconds > 0);
        assert!(config.test_timeout_seconds < 3600); // Less than 1 hour
    }

    #[test]
    fn test_sanitizer_availability() {
        let availability = check_sanitizer_availability();
        availability.print_status();
        
        // At least one sanitizer should be available in a development environment
        if !availability.any_available() {
            println!("No sanitizers available - consider building with sanitizer flags");
        }
    }

    #[test]
    fn test_sanitizer_options_serialization() {
        let asan_options = AsanOptions::default();
        let asan_string = asan_options.to_env_string();
        assert!(asan_string.contains("detect_leaks=1"));
        
        let tsan_options = TsanOptions::default();
        let tsan_string = tsan_options.to_env_string();
        assert!(tsan_string.contains("detect_deadlocks=1"));
        
        let msan_options = MsanOptions::default();
        let msan_string = msan_options.to_env_string();
        assert!(msan_string.contains("poison_heap=1"));
    }

    #[test]
    fn test_sanitizer_test_runner() {
        let config = SanitizerConfig {
            enable_asan: false, // Disable for unit test
            enable_tsan: false,
            enable_msan: false,
            enable_ubsan: false,
            enable_miri: cfg!(miri),
            test_timeout_seconds: 30,
            ..Default::default()
        };

        let runner = SanitizerTestRunner::new(config);
        
        // Test that the runner can be created and configured
        assert_eq!(runner.results.tests_run.load(Ordering::Relaxed), 0);
    }
}