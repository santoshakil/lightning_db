use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lightning_db::core::btree::iterator::SafeStackEntryBuffer;
use lightning_db::utils::batching::auto_batcher::SafeStackBuffer;
use lightning_db::performance::cache::unified_cache::{safe_hash_fnv1a, safe_compare_keys};

fn bench_safe_stack_entry_buffer(c: &mut Criterion) {
    c.bench_function("safe_stack_entry_buffer", |b| {
        b.iter(|| {
            let mut buffer = SafeStackEntryBuffer::default();
            for i in 0..32 {
                let key = format!("key_{}", i).into_bytes();
                let value = format!("value_{}", i).into_bytes();
                if !buffer.push(key, value) {
                    break;
                }
            }
            
            for i in 0..buffer.len() {
                black_box(buffer.get(i));
            }
        })
    });
}

fn bench_safe_stack_buffer(c: &mut Criterion) {
    c.bench_function("safe_stack_buffer", |b| {
        b.iter(|| {
            let mut buffer = SafeStackBuffer::default();
            for i in 0..32 {
                let key = format!("key_{}", i).into_bytes();
                let value = format!("value_{}", i).into_bytes();
                if !buffer.push(key, value) {
                    break;
                }
            }
            
            let _drained: Vec<_> = buffer.drain().collect();
        })
    });
}

fn bench_safe_hash_fnv1a(c: &mut Criterion) {
    let data = b"hello world this is some test data for hashing performance";
    
    c.bench_function("safe_hash_fnv1a", |b| {
        b.iter(|| {
            black_box(safe_hash_fnv1a(black_box(data)));
        })
    });
}

fn bench_safe_compare_keys(c: &mut Criterion) {
    let key1 = b"hello world this is some test data for comparison";
    let key2 = b"hello world this is some test data for comparison";
    let key3 = b"different test data for comparison!!!!!!!!!!!!!!";
    
    c.bench_function("safe_compare_keys", |b| {
        b.iter(|| {
            black_box(safe_compare_keys(black_box(key1), black_box(key2)));
            black_box(safe_compare_keys(black_box(key1), black_box(key3)));
        })
    });
}

criterion_group!(
    benches,
    bench_safe_stack_entry_buffer,
    bench_safe_stack_buffer,
    bench_safe_hash_fnv1a,
    bench_safe_compare_keys
);
criterion_main!(benches);