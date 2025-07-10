import 'package:test/test.dart';
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'dart:io';
import 'dart:typed_data';

void main() {
  group('LightningDb Tests', () {
    test('basic database creation', () async {
      final tempDir = await Directory.systemTemp.createTemp();
      final dbPath = '${tempDir.path}/test.db';
      
      try {
        final db = await LightningDb.create(dbPath);
        expect(db.path, equals(dbPath));
        expect(db.isClosed, equals(false));
        await db.close();
        expect(db.isClosed, equals(true));
      } finally {
        await tempDir.delete(recursive: true);
      }
    });
    
    test('basic put and get operations', () async {
      final tempDir = await Directory.systemTemp.createTemp();
      final dbPath = '${tempDir.path}/test.db';
      
      try {
        final db = await LightningDb.create(dbPath);
        
        // Test basic put/get
        await db.put('key1', Uint8List.fromList([1, 2, 3, 4]));
        final result = await db.get('key1');
        expect(result, isNotNull);
        expect(result, equals([1, 2, 3, 4]));
        
        // Test non-existent key
        final missing = await db.get('nonexistent');
        expect(missing, isNull);
        
        await db.close();
      } finally {
        await tempDir.delete(recursive: true);
      }
    });
  });
}