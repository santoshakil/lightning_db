import 'dart:convert';
import 'dart:typed_data';
import 'package:lightning_db/lightning_db.dart';

void main() {
  print('Lightning DB Dart Example');
  print('========================\n');
  
  // Create database with configuration
  final db = LightningDB.create(
    './dart_test_db',
    libraryPath: '/Volumes/Data/Projects/ssss/lightning_db/target/release/liblightning_db_ffi.dylib',
    config: LightningDBConfig(
      cacheSize: 64 * 1024 * 1024, // 64MB
      pageSize: 4096,
    ),
  );
  
  try {
    // Basic operations
    print('Testing basic operations...');
    
    // Put
    final key1 = utf8.encode('hello');
    final value1 = utf8.encode('world');
    db.put(Uint8List.fromList(key1), Uint8List.fromList(value1));
    print('Put: hello = world');
    
    // Get
    final retrieved = db.get(Uint8List.fromList(key1));
    if (retrieved != null) {
      print('Get: hello = ${utf8.decode(retrieved)}');
    }
    
    // Multiple puts
    for (int i = 0; i < 5; i++) {
      final key = utf8.encode('key_$i');
      final value = utf8.encode('value_$i');
      db.put(Uint8List.fromList(key), Uint8List.fromList(value));
    }
    print('Added 5 more key-value pairs');
    
    // Delete
    db.delete(Uint8List.fromList(key1));
    final afterDelete = db.get(Uint8List.fromList(key1));
    print('After delete: hello = ${afterDelete == null ? "not found" : utf8.decode(afterDelete)}');
    
    // Transaction example
    print('\nTesting transactions...');
    final txId = db.beginTransaction();
    print('Started transaction: $txId');
    
    // Add data in transaction
    for (int i = 0; i < 3; i++) {
      final key = utf8.encode('tx_key_$i');
      final value = utf8.encode('tx_value_$i');
      db.putInTransaction(txId, Uint8List.fromList(key), Uint8List.fromList(value));
    }
    
    // Commit transaction
    db.commitTransaction(txId);
    print('Transaction committed');
    
    // Verify transaction data
    final txKey = utf8.encode('tx_key_1');
    final txValue = db.get(Uint8List.fromList(txKey));
    if (txValue != null) {
      print('Transaction data verified: tx_key_1 = ${utf8.decode(txValue)}');
    }
    
    // Checkpoint
    print('\nPerforming checkpoint...');
    db.checkpoint();
    print('Checkpoint completed');
    
    // Performance test
    print('\nRunning performance test...');
    final stopwatch = Stopwatch()..start();
    
    // Write test
    const numOps = 10000;
    for (int i = 0; i < numOps; i++) {
      final key = utf8.encode('perf_key_$i');
      final value = utf8.encode('perf_value_$i');
      db.put(Uint8List.fromList(key), Uint8List.fromList(value));
    }
    
    final writeTime = stopwatch.elapsedMilliseconds;
    print('Writes: $numOps ops in ${writeTime}ms = ${(numOps * 1000 / writeTime).toStringAsFixed(0)} ops/sec');
    
    // Read test
    stopwatch.reset();
    int found = 0;
    for (int i = 0; i < numOps; i++) {
      final key = utf8.encode('perf_key_$i');
      final value = db.get(Uint8List.fromList(key));
      if (value != null) found++;
    }
    
    final readTime = stopwatch.elapsedMilliseconds;
    print('Reads: $numOps ops in ${readTime}ms = ${(numOps * 1000 / readTime).toStringAsFixed(0)} ops/sec (found: $found)');
    
  } catch (e) {
    print('Error: $e');
  } finally {
    // Always close the database
    db.close();
    print('\nDatabase closed');
  }
}