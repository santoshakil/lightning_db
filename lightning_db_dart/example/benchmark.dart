import 'dart:typed_data';
import 'dart:math';
import 'package:lightning_db_dart/lightning_db_dart.dart';

void main() async {
  print('Lightning DB Benchmark');
  print('=====================\n');
  
  // Create database with optimized settings
  final db = await LightningDb.createWithConfig(
    path: './benchmark_db',
    cacheSize: 256 * 1024 * 1024, // 256MB cache
    compressionType: CompressionType.lz4,
    walSyncMode: WalSyncMode.async,
  );
  
  final random = Random();
  
  // Benchmark 1: Sequential writes
  print('1. Sequential Write Performance:');
  final writeStopwatch = Stopwatch()..start();
  const writeCount = 100000;
  
  for (int i = 0; i < writeCount; i++) {
    final key = Uint8List.fromList('key:${i.toString().padLeft(10, '0')}'.codeUnits);
    final value = Uint8List.fromList('value-$i-${random.nextInt(1000000)}'.codeUnits);
    await db.put(key, value);
  }
  
  writeStopwatch.stop();
  final writeTime = writeStopwatch.elapsedMilliseconds;
  final writeOps = (writeCount * 1000.0) / writeTime;
  print('   Wrote $writeCount entries in ${writeTime}ms');
  print('   ${writeOps.toStringAsFixed(0)} ops/sec\n');
  
  // Benchmark 2: Random reads
  print('2. Random Read Performance:');
  final readStopwatch = Stopwatch()..start();
  const readCount = 100000;
  
  for (int i = 0; i < readCount; i++) {
    final keyNum = random.nextInt(writeCount);
    final key = Uint8List.fromList('key:${keyNum.toString().padLeft(10, '0')}'.codeUnits);
    await db.get(key);
  }
  
  readStopwatch.stop();
  final readTime = readStopwatch.elapsedMilliseconds;
  final readOps = (readCount * 1000.0) / readTime;
  print('   Read $readCount entries in ${readTime}ms');
  print('   ${readOps.toStringAsFixed(0)} ops/sec\n');
  
  // Benchmark 3: Batch writes with transactions
  print('3. Batch Write Performance (Transactions):');
  final batchStopwatch = Stopwatch()..start();
  const batchSize = 1000;
  const batchCount = 100;
  
  for (int batch = 0; batch < batchCount; batch++) {
    final tx = await db.beginTransaction();
    
    for (int i = 0; i < batchSize; i++) {
      final idx = batch * batchSize + i;
      final key = Uint8List.fromList('batch:${idx.toString().padLeft(10, '0')}'.codeUnits);
      final value = Uint8List.fromList('batch-value-$idx'.codeUnits);
      await tx.put(key, value);
    }
    
    await tx.commit();
  }
  
  batchStopwatch.stop();
  final batchTime = batchStopwatch.elapsedMilliseconds;
  final batchOps = (batchCount * batchSize * 1000.0) / batchTime;
  print('   Wrote ${batchCount * batchSize} entries in $batchCount transactions');
  print('   Total time: ${batchTime}ms');
  print('   ${batchOps.toStringAsFixed(0)} ops/sec\n');
  
  // Benchmark 4: Range scan performance
  print('4. Range Scan Performance:');
  final scanStopwatch = Stopwatch()..start();
  int scanCount = 0;
  
  final iter = await db.scan(
    start: Uint8List.fromList('key:'.codeUnits),
    end: Uint8List.fromList('key:~'.codeUnits),
  );
  
  await for (final _ in iter.toStream()) {
    scanCount++;
  }
  
  scanStopwatch.stop();
  final scanTime = scanStopwatch.elapsedMilliseconds;
  final scanOps = (scanCount * 1000.0) / scanTime;
  print('   Scanned $scanCount entries in ${scanTime}ms');
  print('   ${scanOps.toStringAsFixed(0)} entries/sec\n');
  
  // Benchmark 5: Mixed workload
  print('5. Mixed Workload (80% reads, 20% writes):');
  final mixedStopwatch = Stopwatch()..start();
  const mixedOps = 50000;
  
  for (int i = 0; i < mixedOps; i++) {
    if (random.nextDouble() < 0.8) {
      // Read operation
      final keyNum = random.nextInt(writeCount);
      final key = Uint8List.fromList('key:${keyNum.toString().padLeft(10, '0')}'.codeUnits);
      await db.get(key);
    } else {
      // Write operation
      final key = Uint8List.fromList('mixed:${i.toString().padLeft(10, '0')}'.codeUnits);
      final value = Uint8List.fromList('mixed-value-$i'.codeUnits);
      await db.put(key, value);
    }
  }
  
  mixedStopwatch.stop();
  final mixedTime = mixedStopwatch.elapsedMilliseconds;
  final mixedOpsPerSec = (mixedOps * 1000.0) / mixedTime;
  print('   Performed $mixedOps operations in ${mixedTime}ms');
  print('   ${mixedOpsPerSec.toStringAsFixed(0)} ops/sec\n');
  
  // Database statistics
  print('6. Database Statistics:');
  await db.sync();
  print('   ✓ Database synced to disk');
  
  await db.close();
  print('   ✓ Database closed');
  
  print('\nBenchmark completed!');
}