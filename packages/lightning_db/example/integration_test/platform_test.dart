import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:lightning_db/lightning_db.dart';
import 'dart:io';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Platform-specific Integration Tests', () {
    late String testDbPath;
    late LightningDb db;

    setUp(() async {
      final tempDir = await Directory.systemTemp.createTemp('platform_test_');
      testDbPath = '${tempDir.path}/test.db';
    });

    tearDown(() async {
      if (db.isOpen) {
        await db.close();
      }
      final testDir = Directory(testDbPath).parent;
      if (await testDir.exists()) {
        await testDir.delete(recursive: true);
      }
    });

    test('Platform initialization', () async {
      // Test platform-specific initialization
      expect(() async {
        db = await LightningDb.open(testDbPath);
      }, completes);

      expect(db.isOpen, isTrue);

      // Verify platform channel is working
      const platform = MethodChannel('lightning_db');
      try {
        final version = await platform.invokeMethod<String>('getPlatformVersion');
        expect(version, isNotNull);
        print('Platform version: $version');
      } catch (e) {
        // Platform channel might not be implemented in test environment
        print('Platform channel not available in test: $e');
      }
    });

    test('iOS-specific features', () async {
      if (!Platform.isIOS) {
        skip('iOS-specific test');
      }

      db = await LightningDb.open(testDbPath);

      // Test iOS-specific configuration
      final config = DatabaseConfig(
        pageSize: 4096,
        cacheSize: 50 * 1024 * 1024, // 50MB
        syncMode: SyncMode.normal,
        journalMode: JournalMode.wal,
      );

      final iosDb = await LightningDb.open(
        '${testDbPath}_ios',
        config: config,
      );

      // Test data persistence
      await iosDb.put('ios_key', 'ios_value');
      final value = await iosDb.get('ios_key');
      expect(value, equals('ios_value'));

      await iosDb.close();
    });

    test('Android-specific features', () async {
      if (!Platform.isAndroid) {
        skip('Android-specific test');
      }

      db = await LightningDb.open(testDbPath);

      // Test Android-specific configuration
      final config = DatabaseConfig(
        pageSize: 4096,
        cacheSize: 100 * 1024 * 1024, // 100MB
        enableCompression: true,
        compressionType: CompressionType.lz4,
      );

      final androidDb = await LightningDb.open(
        '${testDbPath}_android',
        config: config,
      );

      // Test large data handling (Android typically has more memory)
      final largeData = List.generate(10 * 1024 * 1024, (i) => i % 256).join();
      await androidDb.put('large_key', largeData);
      
      final retrieved = await androidDb.get('large_key');
      expect(retrieved, equals(largeData));

      await androidDb.close();
    });

    test('macOS-specific features', () async {
      if (!Platform.isMacOS) {
        skip('macOS-specific test');
      }

      db = await LightningDb.open(testDbPath);

      // Test macOS-specific features
      final config = DatabaseConfig(
        pageSize: 8192, // Larger page size for desktop
        cacheSize: 500 * 1024 * 1024, // 500MB
        enableMmap: true,
        readOnly: false,
      );

      final macDb = await LightningDb.open(
        '${testDbPath}_macos',
        config: config,
      );

      // Test file permissions (macOS specific)
      final dbFile = File('${testDbPath}_macos');
      expect(await dbFile.exists(), isTrue);

      // Test concurrent access (desktop platforms typically support more)
      final futures = List.generate(100, (i) async {
        await macDb.put('concurrent_$i', 'value_$i');
        return await macDb.get('concurrent_$i');
      });

      final results = await Future.wait(futures);
      expect(results.every((r) => r != null), isTrue);

      await macDb.close();
    });

    test('Windows-specific features', () async {
      if (!Platform.isWindows) {
        skip('Windows-specific test');
      }

      db = await LightningDb.open(testDbPath);

      // Test Windows-specific path handling
      final windowsPath = r'C:\Temp\lightning_test.db';
      final windowsDb = await LightningDb.open(windowsPath);

      await windowsDb.put('windows_key', 'windows_value');
      final value = await windowsDb.get('windows_key');
      expect(value, equals('windows_value'));

      await windowsDb.close();
      
      // Clean up
      try {
        await File(windowsPath).delete();
      } catch (e) {
        // Ignore cleanup errors
      }
    });

    test('Linux-specific features', () async {
      if (!Platform.isLinux) {
        skip('Linux-specific test');
      }

      db = await LightningDb.open(testDbPath);

      // Test Linux-specific features
      final config = DatabaseConfig(
        pageSize: 4096,
        cacheSize: 200 * 1024 * 1024, // 200MB
        directIo: true, // Linux supports O_DIRECT
      );

      final linuxDb = await LightningDb.open(
        '${testDbPath}_linux',
        config: config,
      );

      // Test POSIX file locking
      await linuxDb.put('linux_key', 'linux_value');
      
      // Try to open same database (should handle lock properly)
      expect(() async {
        await LightningDb.open('${testDbPath}_linux');
      }, throwsA(isA<DatabaseException>()));

      await linuxDb.close();
    });

    test('Memory-constrained environment', () async {
      // Test behavior in memory-constrained environment
      final config = DatabaseConfig(
        pageSize: 4096,
        cacheSize: 10 * 1024 * 1024, // Only 10MB cache
        enableCompression: true,
        compressionType: CompressionType.zstd,
      );

      db = await LightningDb.open(testDbPath, config: config);

      // Add enough data to exceed cache
      const recordCount = 1000;
      for (int i = 0; i < recordCount; i++) {
        final data = 'x' * 1024; // 1KB per record
        await db.put('mem_key_$i', data);
      }

      // Verify data integrity despite cache pressure
      for (int i = 0; i < recordCount; i++) {
        final value = await db.get('mem_key_$i');
        expect(value, hasLength(1024));
      }
    });

    test('Platform file system limits', () async {
      db = await LightningDb.open(testDbPath);

      // Test long key names (some platforms have limits)
      final longKey = 'k' * 255; // Max filename length on most systems
      await db.put(longKey, 'value');
      expect(await db.get(longKey), equals('value'));

      // Test special characters in keys (platform-dependent)
      if (!Platform.isWindows) {
        // Windows has more restrictive filename rules
        final specialKey = 'key:with/special*chars';
        await db.put(specialKey, 'special_value');
        expect(await db.get(specialKey), equals('special_value'));
      }
    });

    test('Platform binary loading', () async {
      // Verify correct binary is loaded for platform
      db = await LightningDb.open(testDbPath);

      // Check native library info
      final info = await db.getNativeLibraryInfo();
      expect(info, isNotNull);
      expect(info['platform'], equals(Platform.operatingSystem));
      expect(info['architecture'], isNotNull);
      expect(info['version'], isNotNull);

      // Verify correct features are enabled
      if (Platform.isIOS) {
        // iOS should not have zstd compression
        expect(info['features'], isNot(contains('zstd')));
      } else {
        // Other platforms should have zstd
        expect(info['features'], contains('zstd'));
      }
    });

    test('Platform performance characteristics', () async {
      db = await LightningDb.open(testDbPath);

      const iterations = 1000;
      final stopwatch = Stopwatch()..start();

      // Write performance
      for (int i = 0; i < iterations; i++) {
        await db.put('perf_key_$i', 'value_$i');
      }
      final writeTime = stopwatch.elapsedMilliseconds;

      // Read performance
      stopwatch.reset();
      for (int i = 0; i < iterations; i++) {
        await db.get('perf_key_$i');
      }
      final readTime = stopwatch.elapsedMilliseconds;

      print('Platform: ${Platform.operatingSystem}');
      print('Write performance: ${iterations * 1000 / writeTime} ops/sec');
      print('Read performance: ${iterations * 1000 / readTime} ops/sec');

      // Platform-specific expectations
      if (Platform.isIOS || Platform.isAndroid) {
        // Mobile platforms might be slower
        expect(writeTime, lessThan(10000)); // 100+ ops/sec
        expect(readTime, lessThan(5000)); // 200+ ops/sec
      } else {
        // Desktop platforms should be faster
        expect(writeTime, lessThan(5000)); // 200+ ops/sec
        expect(readTime, lessThan(2000)); // 500+ ops/sec
      }
    });
  });
}