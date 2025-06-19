use lightning_db::error::Result;
use lightning_db::lsm::delta_compression::{
    CompressionLevel, DeltaCompressionConfig, DeltaCompressor, DeltaType,
};

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
