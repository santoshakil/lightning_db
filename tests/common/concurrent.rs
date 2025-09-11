use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};

pub fn run_concurrent_with_barrier<F>(
    num_threads: usize,
    op: F,
) where
    F: Fn(usize, Arc<Barrier>) + Send + Clone + 'static,
{
    let barrier = Arc::new(Barrier::new(num_threads));
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let barrier = Arc::clone(&barrier);
            let op = op.clone();
            thread::spawn(move || {
                op(thread_id, barrier);
            })
        })
        .collect();

    wait_for_threads(handles);
}

fn wait_for_threads(handles: Vec<JoinHandle<()>>) {
    for handle in handles {
        handle.join().unwrap();
    }
}