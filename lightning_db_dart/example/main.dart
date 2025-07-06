import 'dart:typed_data';
import 'package:lightning_db_dart/lightning_db_dart.dart';

void main() async {
  // Create a new database
  final db = await LightningDb.create('./example_db');
  
  print('Lightning DB Example');
  print('===================\n');
  
  // Basic key-value operations
  print('1. Basic Operations:');
  final key = Uint8List.fromList('user:1'.codeUnits);
  final value = Uint8List.fromList('{"name": "John Doe", "age": 30}'.codeUnits);
  
  await db.put(key, value);
  print('   ✓ Put key-value pair');
  
  final retrieved = await db.get(key);
  if (retrieved != null) {
    print('   ✓ Retrieved: ${String.fromCharCodes(retrieved)}');
  }
  
  // Transaction example
  print('\n2. Transaction Example:');
  final tx = await db.beginTransaction();
  
  try {
    await tx.put(
      Uint8List.fromList('user:2'.codeUnits),
      Uint8List.fromList('{"name": "Jane Smith", "age": 25}'.codeUnits),
    );
    await tx.put(
      Uint8List.fromList('user:3'.codeUnits),
      Uint8List.fromList('{"name": "Bob Johnson", "age": 35}'.codeUnits),
    );
    
    await tx.commit();
    print('   ✓ Transaction committed successfully');
  } catch (e) {
    await tx.rollback();
    print('   ✗ Transaction failed: $e');
  }
  
  // Range scan example
  print('\n3. Range Scan:');
  final iter = await db.scan(
    start: Uint8List.fromList('user:'.codeUnits),
    end: Uint8List.fromList('user:~'.codeUnits),
  );
  
  print('   All users:');
  await for (final (k, v) in iter.toStream()) {
    final keyStr = String.fromCharCodes(k);
    final valueStr = String.fromCharCodes(v);
    print('   - $keyStr: $valueStr');
  }
  
  // Consistency levels
  print('\n4. Consistency Levels:');
  await db.putWithConsistency(
    Uint8List.fromList('important:1'.codeUnits),
    Uint8List.fromList('critical data'.codeUnits),
    ConsistencyLevel.strong,
  );
  print('   ✓ Wrote with strong consistency');
  
  final criticalData = await db.getWithConsistency(
    Uint8List.fromList('important:1'.codeUnits),
    ConsistencyLevel.strong,
  );
  if (criticalData != null) {
    print('   ✓ Read with strong consistency: ${String.fromCharCodes(criticalData)}');
  }
  
  // Performance test
  print('\n5. Performance Test:');
  final stopwatch = Stopwatch()..start();
  
  for (int i = 0; i < 1000; i++) {
    await db.put(
      Uint8List.fromList('perf:$i'.codeUnits),
      Uint8List.fromList('value $i'.codeUnits),
    );
  }
  
  stopwatch.stop();
  final writeTime = stopwatch.elapsedMilliseconds;
  final writeOps = 1000000 / writeTime;
  print('   ✓ Wrote 1000 entries in ${writeTime}ms (${writeOps.toStringAsFixed(0)} ops/sec)');
  
  // Cleanup
  print('\n6. Cleanup:');
  await db.sync();
  print('   ✓ Database synced to disk');
  
  await db.close();
  print('   ✓ Database closed');
  
  print('\nExample completed successfully!');
}