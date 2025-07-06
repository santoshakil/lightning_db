/// Database compression type
enum CompressionType {
  /// No compression
  none(0),
  /// Zstandard compression - balanced speed and ratio
  zstd(1),
  /// LZ4 compression - fastest
  lz4(2),
  /// Snappy compression - fast with moderate ratio
  snappy(3);

  final int value;
  const CompressionType(this.value);
}