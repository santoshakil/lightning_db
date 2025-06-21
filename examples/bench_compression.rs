use lightning_db::compression::{CompressionBenchmark, CompressionType, get_compressor};
use std::time::Instant;

fn main() {
    println!("⚡ Lightning DB Compression Performance Benchmark\n");

    // Different types of test data
    let test_data = vec![
        ("Highly repetitive", b"AAAAAAAAAA".repeat(10000)),
        ("Text (Lorem Ipsum)", b"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ".repeat(1000)),
        ("JSON-like", br#"{"id": 12345, "name": "John Doe", "age": 30, "email": "john@example.com", "active": true} "#.repeat(1000)),
        ("Random binary", generate_random_data(100_000)),
        ("Mixed content", generate_mixed_data(100_000)),
        ("Already compressed", generate_compressed_like_data(100_000)),
    ];

    println!("Testing compression algorithms on different data types:\n");
    println!("{:<20} {:<10} {:<15} {:<15} {:<15} {:<15} {:<15}", 
             "Data Type", "Size (KB)", "None", "LZ4", "Snappy", "Zstd", "Best Choice");
    println!("{}", "-".repeat(110));

    for (name, data) in test_data {
        let original_size = data.len();
        
        // Test each compression algorithm
        let mut results = Vec::new();
        
        for comp_type in [CompressionType::None, CompressionType::Lz4, CompressionType::Snappy, CompressionType::Zstd] {
            let compressor = get_compressor(comp_type);
            
            let start = Instant::now();
            let compressed = compressor.compress(&data).unwrap_or_else(|_| data.clone());
            let compress_time = start.elapsed();
            
            let ratio = if comp_type == CompressionType::None {
                0.0
            } else {
                1.0 - (compressed.len() as f64 / original_size as f64)
            };
            
            results.push((comp_type, ratio, compress_time));
        }
        
        // Find best compressor using adaptive algorithm
        let best = CompressionBenchmark::find_best_compressor(&data);
        
        // Print results
        print!("{:<20} {:<10}", name, original_size / 1024);
        
        for (comp_type, ratio, _) in &results {
            let ratio_str = if *comp_type == CompressionType::None {
                "N/A".to_string()
            } else {
                format!("{:.1}%", ratio * 100.0)
            };
            print!(" {:<15}", ratio_str);
        }
        
        println!(" {:<15}", format!("{:?}", best));
    }

    println!("\n\nDetailed compression benchmark with timing:\n");
    
    // Detailed benchmark with larger data
    let large_text = b"The quick brown fox jumps over the lazy dog. ".repeat(10000);
    
    println!("{:<15} {:<15} {:<15} {:<15} {:<15} {:<15}", 
             "Algorithm", "Original (KB)", "Compressed (KB)", "Ratio", "Compress (ms)", "Decompress (ms)");
    println!("{}", "-".repeat(90));
    
    for comp_type in [CompressionType::Lz4, CompressionType::Snappy, CompressionType::Zstd] {
        let compressor = get_compressor(comp_type);
        
        let start = Instant::now();
        let compressed = compressor.compress(&large_text).unwrap();
        let compress_time = start.elapsed();
        
        let start = Instant::now();
        let _decompressed = compressor.decompress(&compressed).unwrap();
        let decompress_time = start.elapsed();
        
        let ratio = 1.0 - (compressed.len() as f64 / large_text.len() as f64);
        
        println!("{:<15} {:<15} {:<15} {:<15.1}% {:<15.2} {:<15.2}", 
                 format!("{:?}", comp_type),
                 large_text.len() / 1024,
                 compressed.len() / 1024,
                 ratio * 100.0,
                 compress_time.as_secs_f64() * 1000.0,
                 decompress_time.as_secs_f64() * 1000.0);
    }

    println!("\n✅ Compression benchmark completed!");
}

fn generate_random_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    for i in 0..size {
        data[i] = ((i * 7 + 13) % 256) as u8;
    }
    data
}

fn generate_mixed_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let patterns = [
        b"Hello World",
        b"AAAAAAAAAA!",
        b"12345678901",
        b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A",
    ];
    
    while data.len() < size {
        let pattern = &patterns[data.len() % patterns.len()];
        data.extend_from_slice(*pattern);
    }
    
    data.truncate(size);
    data
}

fn generate_compressed_like_data(size: usize) -> Vec<u8> {
    // Simulate already compressed data with high entropy
    let mut data = vec![0u8; size];
    let mut state = 0x12345678u32;
    
    for i in 0..size {
        // Simple linear congruential generator
        state = state.wrapping_mul(1664525).wrapping_add(1013904223);
        data[i] = (state >> 24) as u8;
    }
    
    data
}