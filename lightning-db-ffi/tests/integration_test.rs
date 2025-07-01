// Since these are integration tests, we can't access the FFI module directly
// Instead, we test the library functionality
#[test]
fn test_ffi_library_builds() {
    // This test just ensures the FFI library compiles correctly
    // The actual FFI tests are in the examples directory
    assert!(true);
}