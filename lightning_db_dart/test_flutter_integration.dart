import 'dart:typed_data';
import 'dart:io';
import 'package:lightning_db_dart/lightning_db_dart.dart';

// Simple test to verify Flutter integration works
void main() async {
  print('Testing Lightning DB Flutter Integration');
  print('=======================================\n');
  
  try {
    // Create database in temp directory
    final dbPath = '${Directory.systemTemp.path}/flutter_test_db';
    final db = await LightningDb.create(dbPath);
    print('✓ Database created at: $dbPath');
    
    // Test basic operations
    final key = Uint8List.fromList('flutter_test'.codeUnits);
    final value = Uint8List.fromList('Hello from Flutter!'.codeUnits);
    
    await db.put(key, value);
    print('✓ Put operation successful');
    
    final retrieved = await db.get(key);
    if (retrieved != null) {
      print('✓ Get operation successful: ${String.fromCharCodes(retrieved)}');
    }
    
    // Test transaction
    final tx = await db.beginTransaction();
    await tx.put(
      Uint8List.fromList('tx_test'.codeUnits),
      Uint8List.fromList('Transaction data'.codeUnits),
    );
    await tx.commit();
    print('✓ Transaction successful');
    
    // Cleanup
    await db.close();
    print('✓ Database closed');
    
    // Clean up test database
    final dbDir = Directory(dbPath);
    if (dbDir.existsSync()) {
      dbDir.deleteSync(recursive: true);
      print('✓ Test database cleaned up');
    }
    
    print('\n✅ Flutter integration test PASSED!');
  } catch (e, stack) {
    print('\n❌ Flutter integration test FAILED!');
    print('Error: $e');
    print('Stack trace:\n$stack');
    exit(1);
  }
}