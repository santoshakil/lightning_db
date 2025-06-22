use std::cell::RefCell;

thread_local! {
    // Thread-local key buffer to avoid allocations
    static KEY_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(256));
    
    // Thread-local value buffer
    static VALUE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
}

/// Get a thread-local key buffer
#[inline(always)]
pub fn with_key_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    KEY_BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.clear();
        f(&mut buf)
    })
}

/// Get a thread-local value buffer
#[inline(always)]
pub fn with_value_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    VALUE_BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.clear();
        f(&mut buf)
    })
}

/// Optimized key formatting that reuses buffers
#[inline(always)]
pub fn format_key_optimized(prefix: &str, id: u64) -> Vec<u8> {
    with_key_buffer(|buf| {
        use std::io::Write;
        let _ = write!(buf, "{}{:08}", prefix, id);
        buf.clone()
    })
}