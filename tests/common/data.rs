pub fn generate_sequential_data(count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = format!("key_{:04}", i);
            let value = format!("value_{}", i);
            (key.into_bytes(), value.into_bytes())
        })
        .collect()
}

pub fn generate_large_value(size: usize) -> Vec<u8> {
    vec![42u8; size]
}

pub fn generate_unique_key(thread_id: usize, op_id: usize) -> Vec<u8> {
    format!("thread_{}_key_{}", thread_id, op_id).into_bytes()
}

pub fn generate_unique_value(thread_id: usize, op_id: usize) -> Vec<u8> {
    format!("thread_{}_value_{}", thread_id, op_id).into_bytes()
}