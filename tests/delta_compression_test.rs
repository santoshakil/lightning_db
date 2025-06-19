use lightning_db::error::Result;
use lightning_db::lsm::delta_compression::{
    CompressionLevel, DeltaCompressionConfig, DeltaCompressor, DeltaType,
};
use lightning_db::lsm::{LSMConfig, LSMTree};
use tempfile::TempDir;

#[test]
fn test_delta_compression_basic() -> Result<()> {
    let config = DeltaCompressionConfig {
        enabled: true,
        min_similarity_threshold: 0.5,
        max_delta_chain_length: 3,
        delta_block_size: 1024,
        compression_level: CompressionLevel::Balanced,
    };

    let compressor = DeltaCompressor::new(config);

    // Test compression without reference (should return FullData)
    let input_data = b"Hello, World! This is test data for compression.";
    let result = compressor.compress(input_data, 1)?;

    assert_eq!(result.compression_type, DeltaType::FullData);
    assert_eq!(result.original_size, input_data.len());
    assert!(result.reference_id.is_none());

    Ok(())
}

#[test]
fn test_delta_compression_similarity() -> Result<()> {
    let config = DeltaCompressionConfig::default();
    let compressor = DeltaCompressor::new(config);

    let data1 = b"The quick brown fox jumps over the lazy dog";
    let data2 = b"The quick brown fox jumps over the lazy cat";
    let data3 = b"Completely different data with no similarity";

    // Test similarity calculation
    let sim1 = compressor.calculate_block_similarity(data1, data2);
    let sim2 = compressor.calculate_block_similarity(data1, data3);

    // Similar data should have higher similarity
    assert!(sim1 > sim2);
    assert!(sim1 > 0.5); // Should be quite similar
    assert!(sim2 < 0.5); // Should be less similar

    Ok(())
}

#[test]
fn test_delta_compression_with_lsm() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let lsm_config = LSMConfig {
        delta_compression_config: DeltaCompressionConfig {
            enabled: true,
            min_similarity_threshold: 0.6,
            max_delta_chain_length: 5,
            delta_block_size: 512,
            compression_level: CompressionLevel::Best,
        },
        ..Default::default()
    };

    let lsm = LSMTree::new(db_path, lsm_config)?;

    // Insert some similar data
    lsm.insert(
        b"key1".to_vec(),
        b"The quick brown fox jumps over the lazy dog".to_vec(),
    )?;
    lsm.insert(
        b"key2".to_vec(),
        b"The quick brown fox jumps over the lazy cat".to_vec(),
    )?;
    lsm.insert(
        b"key3".to_vec(),
        b"The quick brown fox jumps over the lazy bird".to_vec(),
    )?;

    // Force flush to create SSTables
    lsm.flush()?;

    // Check that delta compression stats are available
    let stats = lsm.get_delta_compression_stats();
    assert!(stats.enabled);

    // Verify we can retrieve the data
    assert_eq!(
        lsm.get(b"key1")?,
        Some(b"The quick brown fox jumps over the lazy dog".to_vec())
    );
    assert_eq!(
        lsm.get(b"key2")?,
        Some(b"The quick brown fox jumps over the lazy cat".to_vec())
    );
    assert_eq!(
        lsm.get(b"key3")?,
        Some(b"The quick brown fox jumps over the lazy bird".to_vec())
    );

    Ok(())
}

#[test]
fn test_delta_compression_decompression() -> Result<()> {
    let config = DeltaCompressionConfig {
        enabled: true,
        min_similarity_threshold: 0.3,
        compression_level: CompressionLevel::Fast,
        ..Default::default()
    };

    let compressor = DeltaCompressor::new(config);

    // Test full data compression and decompression
    let original_data = b"Test data for compression and decompression cycle";
    let compressed = compressor.compress(original_data, 1)?;

    // Decompress should return original data
    let decompressed = compressor.decompress(&compressed, None)?;
    assert_eq!(decompressed, original_data);

    Ok(())
}

#[test]
fn test_delta_compression_ratio() -> Result<()> {
    let config = DeltaCompressionConfig::default();
    let compressor = DeltaCompressor::new(config);

    let original_data = b"A very long string that repeats itself many times. ".repeat(100);
    let compressed = compressor.compress(&original_data, 1)?;

    let ratio = compressor.get_compression_ratio(&compressed);

    // Since we don't have a reference, ratio should be 1.0 (no compression)
    assert_eq!(ratio, 1.0);
    assert_eq!(compressed.compression_type, DeltaType::FullData);

    Ok(())
}

#[test]
fn test_delta_compression_levels() -> Result<()> {
    let test_data = b"Sample data for testing different compression levels and their behavior";

    // Test different compression levels
    for level in [
        CompressionLevel::Fast,
        CompressionLevel::Balanced,
        CompressionLevel::Best,
    ] {
        let config = DeltaCompressionConfig {
            enabled: true,
            compression_level: level,
            ..Default::default()
        };

        let compressor = DeltaCompressor::new(config);
        let result = compressor.compress(test_data, 1)?;

        // All should produce FullData type without reference
        assert_eq!(result.compression_type, DeltaType::FullData);
        assert_eq!(result.original_size, test_data.len());
    }

    Ok(())
}

#[test]
fn test_delta_compression_disabled() -> Result<()> {
    let config = DeltaCompressionConfig {
        enabled: false,
        ..Default::default()
    };

    let compressor = DeltaCompressor::new(config);
    let result = compressor.compress(b"test data", 1)?;

    // Should return FullData when disabled
    assert_eq!(result.compression_type, DeltaType::FullData);
    assert_eq!(result.metadata.similarity_score, 0.0);

    Ok(())
}

#[test]
fn test_delta_compression_large_data() -> Result<()> {
    let config = DeltaCompressionConfig {
        enabled: true,
        delta_block_size: 256,
        ..Default::default()
    };

    let compressor = DeltaCompressor::new(config);

    // Generate large test data
    let large_data = b"x".repeat(10000);
    let result = compressor.compress(&large_data, 1)?;

    assert_eq!(result.compression_type, DeltaType::FullData);
    assert_eq!(result.original_size, large_data.len());

    // Test decompression
    let decompressed = compressor.decompress(&result, None)?;
    assert_eq!(decompressed, large_data);

    Ok(())
}
