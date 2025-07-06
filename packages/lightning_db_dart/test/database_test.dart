import 'package:test/test.dart';
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'dart:io';
import 'dart:typed_data';
import 'dart:convert';

void main() {
  group('LightningDb FFI Tests', () {
    late String testDbPath;
    late LightningDb db;

    setUp(() async {
      // Create temporary test directory
      final tempDir = await Directory.systemTemp.createTemp('lightning_ffi_test_');
      testDbPath = '${tempDir.path}/test.db';
      
      // Initialize the FFI library
      LightningDbInit.init();
    });

    tearDown(() async {
      // Close database if open
      try {
        db.close();
      } catch (_) {
        // Ignore if already closed
      }
      
      // Clean up test directory
      final testDir = Directory(testDbPath).parent;
      if (await testDir.exists()) {
        await testDir.delete(recursive: true);
      }
    });

    test('Database creation and opening', () {
      db = LightningDb.create(testDbPath);
      expect(db.path, equals(testDbPath));
      
      // Close and reopen
      db.close();
      
      db = LightningDb.open(testDbPath);
      expect(db.path, equals(testDbPath));
    });

    test('Basic put and get operations', () {
      db = LightningDb.create(testDbPath);
      
      // String key-value
      db.put('string_key', 'string_value');
      expect(db.get('string_key'), equals('string_value'));
      
      // Binary data
      final binaryData = Uint8List.fromList([1, 2, 3, 4, 5]);
      db.putBytes('binary_key', binaryData);
      expect(db.getBytes('binary_key'), equals(binaryData));
      
      // JSON data
      final jsonData = {'name': 'test', 'value': 42};
      db.putJson('json_key', jsonData);
      expect(db.getJson('json_key'), equals(jsonData));
    });

    test('Update operations', () {
      db = LightningDb.create(testDbPath);
      
      db.put('update_key', 'initial_value');
      expect(db.get('update_key'), equals('initial_value'));
      
      db.put('update_key', 'updated_value');
      expect(db.get('update_key'), equals('updated_value'));
    });

    test('Delete operations', () {
      db = LightningDb.create(testDbPath);
      
      db.put('delete_key', 'value');
      expect(db.contains('delete_key'), isTrue);
      
      db.delete('delete_key');
      expect(db.contains('delete_key'), isFalse);
      expect(db.get('delete_key'), isNull);
    });

    test('Exists check', () {
      db = LightningDb.create(testDbPath);
      
      expect(db.contains('nonexistent'), isFalse);
      
      db.put('existent', 'value');
      expect(db.contains('existent'), isTrue);
    });

    test('Error handling', () {
      // Invalid path
      expect(
        () => LightningDb.create('/invalid/path/to/db'),
        throwsA(isA<LightningDbException>()),
      );
      
      // Operations on closed database
      db = LightningDb.create(testDbPath);
      db.close();
      
      expect(
        () => db.put('key', 'value'),
        throwsA(isA<StateError>()),
      );
    });

    test('Large data handling', () {
      db = LightningDb.create(testDbPath);
      
      // 1MB of data
      final largeData = List.generate(1024 * 1024, (i) => i % 256);
      final largeBytes = Uint8List.fromList(largeData);
      
      db.putBytes('large_key', largeBytes);
      expect(db.getBytes('large_key'), equals(largeBytes));
    });

    test('Special characters in keys', () {
      db = LightningDb.create(testDbPath);
      
      final specialKeys = [
        'key with spaces',
        'key-with-dashes',
        'key_with_underscores',
        'key.with.dots',
        'key/with/slashes',
        'key\\with\\backslashes',
        'key:with:colons',
        'key;with;semicolons',
        'key,with,commas',
        'key|with|pipes',
        'unicode_key_🚀',
        'key\nwith\nnewlines',
        'key\twith\ttabs',
      ];
      
      for (final key in specialKeys) {
        db.put(key, 'value for $key');
        expect(db.get(key), equals('value for $key'));
      }
    });

    test('Null and empty values', () {
      db = LightningDb.create(testDbPath);
      
      // Empty string value
      db.put('empty_value', '');
      expect(db.get('empty_value'), equals(''));
      
      // Empty binary value
      db.putBytes('empty_binary', Uint8List(0));
      expect(db.getBytes('empty_binary'), equals(Uint8List(0)));
    });

    test('Type conversions', () {
      db = LightningDb.create(testDbPath);
      
      // Store as string, read as bytes
      db.put('type_test', 'hello');
      final bytes = db.getBytes('type_test');
      expect(utf8.decode(bytes!), equals('hello'));
      
      // Store as bytes, read as string
      db.putBytes('byte_string', utf8.encode('world'));
      expect(db.get('byte_string'), equals('world'));
    });

    test('Concurrent operations', () async {
      db = LightningDb.create(testDbPath);
      
      // Concurrent writes
      final futures = <Future>[];
      for (int i = 0; i < 100; i++) {
        futures.add(Future.microtask(() {
          db.put('concurrent_$i', 'value_$i');
        }));
      }
      
      await Future.wait(futures);
      
      // Verify all writes succeeded
      for (int i = 0; i < 100; i++) {
        expect(db.get('concurrent_$i'), equals('value_$i'));
      }
    });

    test('Database configuration', () {
      final config = DatabaseConfig(
        pageSize: 8192,
        cacheSize: 100 * 1024 * 1024, // 100MB
        syncMode: SyncMode.normal,
        journalMode: JournalMode.wal,
        enableCompression: true,
        compressionType: CompressionType.lz4,
      );
      
      db = LightningDb.create(testDbPath, config: config);
      
      // Test that configuration is applied
      db.put('config_test', 'value');
      expect(db.get('config_test'), equals('value'));
      
      // Get statistics to verify configuration
      final stats = db.getStatistics();
      expect(stats.pageSize, equals(8192));
    });

    test('Iteration over keys', () {
      db = LightningDb.create(testDbPath);
      
      // Add test data
      final testData = {
        'key1': 'value1',
        'key2': 'value2',
        'key3': 'value3',
        'prefix_key1': 'prefix_value1',
        'prefix_key2': 'prefix_value2',
      };
      
      testData.forEach((key, value) {
        db.put(key, value);
      });
      
      // Iterate over all keys
      final allKeys = db.keys().toList();
      expect(allKeys.length, equals(5));
      expect(allKeys.toSet(), equals(testData.keys.toSet()));
      
      // Iterate with prefix
      final prefixKeys = db.keys(prefix: 'prefix_').toList();
      expect(prefixKeys.length, equals(2));
      expect(prefixKeys.every((k) => k.startsWith('prefix_')), isTrue);
    });

    test('Batch operations', () {
      db = LightningDb.create(testDbPath);
      
      // Batch write
      final batch = db.batch();
      for (int i = 0; i < 100; i++) {
        batch.put('batch_key_$i', 'batch_value_$i');
      }
      batch.commit();
      
      // Verify all values were written
      for (int i = 0; i < 100; i++) {
        expect(db.get('batch_key_$i'), equals('batch_value_$i'));
      }
      
      // Batch delete
      final deleteBatch = db.batch();
      for (int i = 0; i < 50; i++) {
        deleteBatch.delete('batch_key_$i');
      }
      deleteBatch.commit();
      
      // Verify deletions
      for (int i = 0; i < 50; i++) {
        expect(db.contains('batch_key_$i'), isFalse);
      }
      for (int i = 50; i < 100; i++) {
        expect(db.contains('batch_key_$i'), isTrue);
      }
    });

    test('Memory usage', () {
      db = LightningDb.create(testDbPath);
      
      final initialStats = db.getStatistics();
      final initialMemory = initialStats.memoryUsage;
      
      // Add significant amount of data
      for (int i = 0; i < 1000; i++) {
        final data = 'x' * 1024; // 1KB per entry
        db.put('memory_key_$i', data);
      }
      
      final afterStats = db.getStatistics();
      expect(afterStats.memoryUsage, greaterThan(initialMemory));
      expect(afterStats.keyCount, equals(1000));
    });

    test('Database compaction', () {
      db = LightningDb.create(testDbPath);
      
      // Add and delete data to create fragmentation
      for (int i = 0; i < 1000; i++) {
        db.put('compact_key_$i', 'value_$i');
      }
      
      for (int i = 0; i < 500; i++) {
        db.delete('compact_key_$i');
      }
      
      final beforeSize = File(testDbPath).lengthSync();
      
      // Compact database
      db.compact();
      
      final afterSize = File(testDbPath).lengthSync();
      // Size might not always decrease due to page alignment
      expect(afterSize, lessThanOrEqualTo(beforeSize));
      
      // Verify remaining data is intact
      for (int i = 500; i < 1000; i++) {
        expect(db.get('compact_key_$i'), equals('value_$i'));
      }
    });

    test('Native library info', () {
      db = LightningDb.create(testDbPath);
      
      final info = db.getNativeLibraryInfo();
      expect(info.version, isNotEmpty);
      expect(info.platform, equals(Platform.operatingSystem));
      expect(info.features, isNotEmpty);
      
      // Check for expected features
      if (Platform.isIOS) {
        expect(info.features, isNot(contains('zstd')));
      } else {
        expect(info.features, contains('compression'));
      }
    });
  });
}