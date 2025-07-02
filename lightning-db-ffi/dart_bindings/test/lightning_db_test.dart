import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:lightning_db/lightning_db.dart';

void main() {
  group('LightningDB', () {
    late LightningDB db;
    final testDir = './test_db_${DateTime.now().millisecondsSinceEpoch}';
    
    setUp(() {
      db = LightningDB.create(
        testDir,
        libraryPath: '/Volumes/Data/Projects/ssss/lightning_db/target/release/liblightning_db_ffi.dylib',
      );
    });
    
    tearDown(() {
      db.close();
      // Clean up test directory
      try {
        Directory(testDir).deleteSync(recursive: true);
      } catch (_) {}
    });
    
    test('should store and retrieve values', () {
      final key = utf8.encode('test_key');
      final value = utf8.encode('test_value');
      
      db.put(Uint8List.fromList(key), Uint8List.fromList(value));
      
      final retrieved = db.get(Uint8List.fromList(key));
      expect(retrieved, isNotNull);
      expect(utf8.decode(retrieved!), equals('test_value'));
    });
    
    test('should return null for non-existent keys', () {
      final key = utf8.encode('non_existent');
      
      final retrieved = db.get(Uint8List.fromList(key));
      expect(retrieved, isNull);
    });
    
    test('should delete keys', () {
      final key = utf8.encode('delete_key');
      final value = utf8.encode('delete_value');
      
      // Put then delete
      db.put(Uint8List.fromList(key), Uint8List.fromList(value));
      db.delete(Uint8List.fromList(key));
      
      // Should not exist
      final retrieved = db.get(Uint8List.fromList(key));
      expect(retrieved, isNull);
    });
    
    test('should handle transactions', () {
      final key1 = utf8.encode('tx_key_1');
      final value1 = utf8.encode('tx_value_1');
      final key2 = utf8.encode('tx_key_2');
      final value2 = utf8.encode('tx_value_2');
      
      // Begin transaction
      final txId = db.beginTransaction();
      expect(txId, greaterThan(0));
      
      // Put in transaction
      db.putInTransaction(txId, Uint8List.fromList(key1), Uint8List.fromList(value1));
      db.putInTransaction(txId, Uint8List.fromList(key2), Uint8List.fromList(value2));
      
      // Commit
      db.commitTransaction(txId);
      
      // Verify data
      final retrieved1 = db.get(Uint8List.fromList(key1));
      final retrieved2 = db.get(Uint8List.fromList(key2));
      
      expect(retrieved1, isNotNull);
      expect(utf8.decode(retrieved1!), equals('tx_value_1'));
      expect(retrieved2, isNotNull);
      expect(utf8.decode(retrieved2!), equals('tx_value_2'));
    });
    
    test('should abort transactions', () {
      final key = utf8.encode('abort_key');
      final value = utf8.encode('abort_value');
      
      // Begin transaction
      final txId = db.beginTransaction();
      
      // Put in transaction
      db.putInTransaction(txId, Uint8List.fromList(key), Uint8List.fromList(value));
      
      // Abort
      db.abortTransaction(txId);
      
      // Data should not exist
      final retrieved = db.get(Uint8List.fromList(key));
      expect(retrieved, isNull);
    });
    
    test('should handle binary data', () {
      final key = Uint8List.fromList([0, 1, 2, 3, 4]);
      final value = Uint8List.fromList([255, 254, 253, 252, 251]);
      
      db.put(key, value);
      
      final retrieved = db.get(key);
      expect(retrieved, isNotNull);
      expect(retrieved, equals(value));
    });
    
    test('should checkpoint without error', () {
      // Add some data
      for (int i = 0; i < 10; i++) {
        final key = utf8.encode('checkpoint_key_$i');
        final value = utf8.encode('checkpoint_value_$i');
        db.put(Uint8List.fromList(key), Uint8List.fromList(value));
      }
      
      // Should not throw
      expect(() => db.checkpoint(), returnsNormally);
    });
    
    test('should throw on closed database', () {
      db.close();
      
      final key = utf8.encode('test');
      final value = utf8.encode('test');
      
      expect(
        () => db.put(Uint8List.fromList(key), Uint8List.fromList(value)),
        throwsA(isA<LightningDBException>()),
      );
    });
  });
  
  group('LightningDB Configuration', () {
    final testDir = './test_db_config_${DateTime.now().millisecondsSinceEpoch}';
    
    tearDown(() {
      try {
        Directory(testDir).deleteSync(recursive: true);
      } catch (_) {}
    });
    
    test('should create database with custom config', () {
      final db = LightningDB.create(
        testDir,
        libraryPath: '/Volumes/Data/Projects/ssss/lightning_db/target/release/liblightning_db_ffi.dylib',
        config: LightningDBConfig(
          pageSize: 8192,
          cacheSize: 128 * 1024 * 1024,
        ),
      );
      
      // Should work normally
      final key = utf8.encode('config_test');
      final value = utf8.encode('config_value');
      db.put(Uint8List.fromList(key), Uint8List.fromList(value));
      
      final retrieved = db.get(Uint8List.fromList(key));
      expect(retrieved, isNotNull);
      expect(utf8.decode(retrieved!), equals('config_value'));
      
      db.close();
    });
  });
}