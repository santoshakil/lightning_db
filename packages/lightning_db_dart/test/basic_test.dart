import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:lightning_db_dart/lightning_db_dart.dart';

void main() {
  group('Lightning DB Basic Tests', () {
    test('Exports are available', () {
      // Just verify that the types are available
      expect(LightningDb, isNotNull);
      expect(Transaction, isNotNull);
      expect(Iterator, isNotNull);
      expect(LightningDbException, isNotNull);
      expect(CompressionType.none, isNotNull);
      expect(ConsistencyLevel.eventual, isNotNull);
      expect(WalSyncMode.periodic, isNotNull);
      expect(DatabaseConfig, isNotNull);
      expect(Batch, isNotNull);
    });
    
    test('Database config creation', () {
      final config = DatabaseConfig(
        cacheSize: 50 * 1024 * 1024,
        compressionType: CompressionType.lz4,
        walSyncMode: WalSyncMode.sync,
      );
      
      expect(config.cacheSize, equals(50 * 1024 * 1024));
      expect(config.compressionType, equals(CompressionType.lz4));
      expect(config.walSyncMode, equals(WalSyncMode.sync));
    });
    
    test('Predefined configs', () {
      final performanceConfig = DatabaseConfig.performance();
      expect(performanceConfig.cacheSize, equals(100 * 1024 * 1024));
      expect(performanceConfig.compressionType, equals(CompressionType.lz4));
      expect(performanceConfig.walSyncMode, equals(WalSyncMode.async));
      
      final safetyConfig = DatabaseConfig.safety();
      expect(safetyConfig.compressionType, equals(CompressionType.zstd));
      expect(safetyConfig.walSyncMode, equals(WalSyncMode.sync));
      
      final minimalConfig = DatabaseConfig.minimal();
      expect(minimalConfig.cacheSize, equals(1 * 1024 * 1024));
    });
    
    // Note: Actual database operations would require the native library
    // to be available, which won't work in unit tests without proper setup
  });
}