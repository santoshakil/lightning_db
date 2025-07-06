import 'native/lightning_db_bindings.dart' as bindings;

/// Compression type for the database
enum CompressionType {
  /// No compression
  none(bindings.CompressionType.CompressionTypeNone),
  
  /// Zstandard compression
  zstd(bindings.CompressionType.CompressionTypeZstd),
  
  /// LZ4 compression
  lz4(bindings.CompressionType.CompressionTypeLz4),
  
  /// Snappy compression
  snappy(bindings.CompressionType.CompressionTypeSnappy);
  
  final int value;
  const CompressionType(this.value);
  
  /// Convert from native value
  static CompressionType fromValue(int value) {
    return CompressionType.values.firstWhere(
      (e) => e.value == value,
      orElse: () => CompressionType.none,
    );
  }
}

/// Consistency level for read/write operations
enum ConsistencyLevel {
  /// Eventual consistency - faster but may read stale data
  eventual(bindings.ConsistencyLevel.ConsistencyLevelEventual),
  
  /// Strong consistency - slower but guarantees latest data
  strong(bindings.ConsistencyLevel.ConsistencyLevelStrong);
  
  final int value;
  const ConsistencyLevel(this.value);
  
  /// Convert from native value
  static ConsistencyLevel fromValue(int value) {
    return ConsistencyLevel.values.firstWhere(
      (e) => e.value == value,
      orElse: () => ConsistencyLevel.eventual,
    );
  }
}

/// Write-ahead log synchronization mode
enum WalSyncMode {
  /// Synchronous - safest but slowest
  sync(bindings.WalSyncMode.WalSyncModeSync),
  
  /// Periodic sync - balance between safety and performance
  periodic(bindings.WalSyncMode.WalSyncModePeriodic),
  
  /// Asynchronous - fastest but less safe
  async(bindings.WalSyncMode.WalSyncModeAsync);
  
  final int value;
  const WalSyncMode(this.value);
  
  /// Convert from native value
  static WalSyncMode fromValue(int value) {
    return WalSyncMode.values.firstWhere(
      (e) => e.value == value,
      orElse: () => WalSyncMode.periodic,
    );
  }
}

/// Configuration for creating a database
class DatabaseConfig {
  /// Cache size in bytes
  final int cacheSize;
  
  /// Compression type to use
  final CompressionType compressionType;
  
  /// WAL synchronization mode
  final WalSyncMode walSyncMode;
  
  const DatabaseConfig({
    this.cacheSize = 10 * 1024 * 1024, // 10MB default
    this.compressionType = CompressionType.none,
    this.walSyncMode = WalSyncMode.periodic,
  });
  
  /// Create a config optimized for performance
  factory DatabaseConfig.performance() {
    return const DatabaseConfig(
      cacheSize: 100 * 1024 * 1024, // 100MB
      compressionType: CompressionType.lz4,
      walSyncMode: WalSyncMode.async,
    );
  }
  
  /// Create a config optimized for safety
  factory DatabaseConfig.safety() {
    return const DatabaseConfig(
      cacheSize: 50 * 1024 * 1024, // 50MB
      compressionType: CompressionType.zstd,
      walSyncMode: WalSyncMode.sync,
    );
  }
  
  /// Create a config optimized for minimal memory usage
  factory DatabaseConfig.minimal() {
    return const DatabaseConfig(
      cacheSize: 1 * 1024 * 1024, // 1MB
      compressionType: CompressionType.snappy,
      walSyncMode: WalSyncMode.periodic,
    );
  }
}