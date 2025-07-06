import 'dart:io';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:lightning_db_dart/lightning_db_dart.dart';

void main() {
  group('LightningDb', () {
    late Directory tempDir;
    late String dbPath;

    setUp(() {
      tempDir = Directory.systemTemp.createTempSync('lightning_db_test_');
      dbPath = '${tempDir.path}/test.db';
    });

    tearDown(() {
      if (tempDir.existsSync()) {
        tempDir.deleteSync(recursive: true);
      }
    });

    test('create and open database', () async {
      // Create database
      final db = await LightningDb.create(dbPath);
      expect(db, isNotNull);
      await db.close();

      // Open existing database
      final db2 = await LightningDb.open(dbPath);
      expect(db2, isNotNull);
      await db2.close();
    });

    test('basic put and get operations', () async {
      final db = await LightningDb.create(dbPath);

      final key = Uint8List.fromList('test_key'.codeUnits);
      final value = Uint8List.fromList('test_value'.codeUnits);

      // Put
      await db.put(key, value);

      // Get
      final retrieved = await db.get(key);
      expect(retrieved, isNotNull);
      expect(String.fromCharCodes(retrieved!), equals('test_value'));

      await db.close();
    });

    test('get non-existent key returns null', () async {
      final db = await LightningDb.create(dbPath);

      final key = Uint8List.fromList('non_existent'.codeUnits);
      final value = await db.get(key);
      expect(value, isNull);

      await db.close();
    });

    test('delete operations', () async {
      final db = await LightningDb.create(dbPath);

      final key = Uint8List.fromList('delete_test'.codeUnits);
      final value = Uint8List.fromList('value'.codeUnits);

      // Put a value
      await db.put(key, value);

      // Verify it exists
      var retrieved = await db.get(key);
      expect(retrieved, isNotNull);

      // Delete it
      final deleted = await db.delete(key);
      expect(deleted, isTrue);

      // Verify it's gone
      retrieved = await db.get(key);
      expect(retrieved, isNull);

      // Delete again should return false
      final deletedAgain = await db.delete(key);
      expect(deletedAgain, isFalse);

      await db.close();
    });

    test('transaction commit', () async {
      final db = await LightningDb.create(dbPath);

      final key1 = Uint8List.fromList('tx_key1'.codeUnits);
      final value1 = Uint8List.fromList('tx_value1'.codeUnits);
      final key2 = Uint8List.fromList('tx_key2'.codeUnits);
      final value2 = Uint8List.fromList('tx_value2'.codeUnits);

      // Begin transaction
      final tx = await db.beginTransaction();

      // Put values in transaction
      await tx.put(key1, value1);
      await tx.put(key2, value2);

      // Values should be visible within transaction
      var retrieved = await tx.get(key1);
      expect(retrieved, isNotNull);
      expect(String.fromCharCodes(retrieved!), equals('tx_value1'));

      // Commit transaction
      await tx.commit();

      // Values should be visible after commit
      retrieved = await db.get(key1);
      expect(retrieved, isNotNull);
      expect(String.fromCharCodes(retrieved!), equals('tx_value1'));

      retrieved = await db.get(key2);
      expect(retrieved, isNotNull);
      expect(String.fromCharCodes(retrieved!), equals('tx_value2'));

      await db.close();
    });

    test('transaction rollback', () async {
      final db = await LightningDb.create(dbPath);

      final key = Uint8List.fromList('rollback_key'.codeUnits);
      final value = Uint8List.fromList('rollback_value'.codeUnits);

      // Begin transaction
      final tx = await db.beginTransaction();

      // Put value in transaction
      await tx.put(key, value);

      // Value should be visible within transaction
      var retrieved = await tx.get(key);
      expect(retrieved, isNotNull);

      // Rollback transaction
      await tx.rollback();

      // Value should not be visible after rollback
      retrieved = await db.get(key);
      expect(retrieved, isNull);

      await db.close();
    });

    test('iterator scan all', () async {
      final db = await LightningDb.create(dbPath);

      // Put some test data
      final entries = <(String, String)>[
        ('key1', 'value1'),
        ('key2', 'value2'),
        ('key3', 'value3'),
      ];

      for (final (k, v) in entries) {
        await db.put(
          Uint8List.fromList(k.codeUnits),
          Uint8List.fromList(v.codeUnits),
        );
      }

      // Scan all entries
      final iter = await db.scan();
      final results = <(String, String)>[];

      await for (final (key, value) in iter.toStream()) {
        results.add((
          String.fromCharCodes(key),
          String.fromCharCodes(value),
        ));
      }

      expect(results.length, equals(3));
      expect(results, containsAll(entries));

      await db.close();
    });

    test('iterator scan range', () async {
      final db = await LightningDb.create(dbPath);

      // Put some test data with sortable keys
      final entries = [
        ('a_key', 'value_a'),
        ('b_key', 'value_b'),
        ('c_key', 'value_c'),
        ('d_key', 'value_d'),
      ];

      for (final (k, v) in entries) {
        await db.put(
          Uint8List.fromList(k.codeUnits),
          Uint8List.fromList(v.codeUnits),
        );
      }

      // Scan from 'b' to 'd'
      final iter = await db.scan(
        start: Uint8List.fromList('b'.codeUnits),
        end: Uint8List.fromList('d'.codeUnits),
      );

      final results = <String>[];
      await for (final (key, _) in iter.toStream()) {
        results.add(String.fromCharCodes(key));
      }

      // Should include b_key and c_key, but not d_key (exclusive end)
      expect(results, contains('b_key'));
      expect(results, contains('c_key'));
      expect(results, isNot(contains('a_key')));

      await db.close();
    });

    test('consistency levels', () async {
      final db = await LightningDb.create(dbPath);

      final key = Uint8List.fromList('consistency_test'.codeUnits);
      final value = Uint8List.fromList('test_value'.codeUnits);

      // Put with strong consistency
      await db.putWithConsistency(key, value, ConsistencyLevel.strong);

      // Get with eventual consistency
      var retrieved = await db.getWithConsistency(key, ConsistencyLevel.eventual);
      expect(retrieved, isNotNull);

      // Get with strong consistency
      retrieved = await db.getWithConsistency(key, ConsistencyLevel.strong);
      expect(retrieved, isNotNull);

      await db.close();
    });

    test('sync and checkpoint', () async {
      final db = await LightningDb.create(dbPath);

      final key = Uint8List.fromList('sync_test'.codeUnits);
      final value = Uint8List.fromList('sync_value'.codeUnits);

      await db.put(key, value);

      // These should not throw
      await db.sync();
      await db.checkpoint();

      await db.close();
    });

    test('error handling - empty key', () async {
      final db = await LightningDb.create(dbPath);

      final emptyKey = Uint8List(0);
      final value = Uint8List.fromList('value'.codeUnits);

      expect(
        () => db.put(emptyKey, value),
        throwsA(isA<LightningDbException>()),
      );

      await db.close();
    });

    test('error handling - closed database', () async {
      final db = await LightningDb.create(dbPath);
      await db.close();

      final key = Uint8List.fromList('key'.codeUnits);
      final value = Uint8List.fromList('value'.codeUnits);

      expect(
        () => db.put(key, value),
        throwsA(isA<LightningDbException>()),
      );
    });

    test('large values', () async {
      final db = await LightningDb.create(dbPath);

      final key = Uint8List.fromList('large_key'.codeUnits);
      final largeValue = Uint8List(1024 * 1024); // 1MB
      for (int i = 0; i < largeValue.length; i++) {
        largeValue[i] = i % 256;
      }

      await db.put(key, largeValue);

      final retrieved = await db.get(key);
      expect(retrieved, isNotNull);
      expect(retrieved!.length, equals(largeValue.length));
      expect(retrieved, equals(largeValue));

      await db.close();
    });

    test('create with custom config', () async {
      final db = await LightningDb.createWithConfig(
        path: dbPath,
        cacheSize: 1024 * 1024 * 32, // 32MB
        compressionType: CompressionType.lz4,
        walSyncMode: WalSyncMode.periodic,
      );

      final key = Uint8List.fromList('config_test'.codeUnits);
      final value = Uint8List.fromList('config_value'.codeUnits);

      await db.put(key, value);

      final retrieved = await db.get(key);
      expect(retrieved, isNotNull);

      await db.close();
    });
  });
}