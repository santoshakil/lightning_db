import 'dart:io';
import 'package:flutter/foundation.dart';
import 'benchmark_runner.dart';
import 'lightning_db_benchmark.dart';
import 'sqlite_benchmark.dart';
// import 'realm_benchmark.dart'; // Uncomment when Realm is added to pubspec

/// Complete benchmark suite comparing all databases
class BenchmarkSuite {
  final int recordCount;
  final String outputDir;
  final bool includeRealm;
  
  BenchmarkSuite({
    this.recordCount = 10000,
    this.outputDir = 'benchmark_results',
    this.includeRealm = false, // Set to true when Realm is available
  });
  
  /// Run all benchmarks
  Future<Map<String, BenchmarkComparison>> runAll({
    int warmupIterations = 5,
    int iterations = 100,
  }) async {
    debugPrint('Starting benchmark suite with $recordCount records');
    debugPrint('Warmup: $warmupIterations, Iterations: $iterations');
    debugPrint('');
    
    // Create output directory
    final dir = Directory(outputDir);
    if (!await dir.exists()) {
      await dir.create(recursive: true);
    }
    
    final results = <String, BenchmarkComparison>{};
    
    // CRUD Benchmark
    results['crud'] = await _runBenchmarkSet(
      'CRUD Operations',
      [
        LightningDbCrudBenchmark(
          dbPath: '$outputDir/lightning_crud.db',
          recordCount: recordCount,
        ),
        SqliteCrudBenchmark(
          dbPath: '$outputDir/sqlite_crud.db',
          recordCount: recordCount,
        ),
        // if (includeRealm)
        //   RealmCrudBenchmark(
        //     dbPath: '$outputDir/realm_crud.realm',
        //     recordCount: recordCount,
        //   ),
      ],
      warmupIterations: warmupIterations,
      iterations: iterations,
    );
    
    // Bulk Insert Benchmark
    results['bulk_insert'] = await _runBenchmarkSet(
      'Bulk Insert',
      [
        LightningDbBulkInsertBenchmark(
          dbPath: '$outputDir/lightning_bulk.db',
          recordCount: recordCount,
        ),
        SqliteBulkInsertBenchmark(
          dbPath: '$outputDir/sqlite_bulk.db',
          recordCount: recordCount,
        ),
        // if (includeRealm)
        //   RealmBulkInsertBenchmark(
        //     dbPath: '$outputDir/realm_bulk.realm',
        //     recordCount: recordCount,
        //   ),
      ],
      warmupIterations: 0, // No warmup for bulk
      iterations: 1, // Run only once
    );
    
    // Query Benchmark
    results['query'] = await _runBenchmarkSet(
      'Query Performance',
      [
        LightningDbQueryBenchmark(
          dbPath: '$outputDir/lightning_query.db',
          recordCount: recordCount,
        ),
        SqliteQueryBenchmark(
          dbPath: '$outputDir/sqlite_query.db',
          recordCount: recordCount,
        ),
        // if (includeRealm)
        //   RealmQueryBenchmark(
        //     dbPath: '$outputDir/realm_query.realm',
        //     recordCount: recordCount,
        //   ),
      ],
      warmupIterations: warmupIterations,
      iterations: iterations,
    );
    
    // Transaction Benchmark
    results['transaction'] = await _runBenchmarkSet(
      'Transaction Performance',
      [
        LightningDbTransactionBenchmark(
          dbPath: '$outputDir/lightning_tx.db',
          recordCount: recordCount,
        ),
        SqliteTransactionBenchmark(
          dbPath: '$outputDir/sqlite_tx.db',
          recordCount: recordCount,
        ),
        // if (includeRealm)
        //   RealmTransactionBenchmark(
        //     dbPath: '$outputDir/realm_tx.realm',
        //     recordCount: recordCount,
        //   ),
      ],
      warmupIterations: warmupIterations,
      iterations: iterations,
    );
    
    // Concurrent Access Benchmark
    results['concurrent'] = await _runBenchmarkSet(
      'Concurrent Access',
      [
        LightningDbConcurrentBenchmark(
          dbPath: '$outputDir/lightning_concurrent.db',
          recordCount: recordCount,
        ),
        SqliteConcurrentBenchmark(
          dbPath: '$outputDir/sqlite_concurrent.db',
          recordCount: recordCount,
        ),
        // if (includeRealm)
        //   RealmConcurrentBenchmark(
        //     dbPath: '$outputDir/realm_concurrent.realm',
        //     recordCount: recordCount,
        //   ),
      ],
      warmupIterations: warmupIterations,
      iterations: iterations ~/ 2, // Fewer iterations for concurrent
    );
    
    // Raw Key-Value Benchmark (Lightning DB only)
    final rawBenchmark = LightningDbRawBenchmark(
      dbPath: '$outputDir/lightning_raw.db',
      recordCount: recordCount,
    );
    final rawSummary = await rawBenchmark.run(
      warmupIterations: warmupIterations,
      iterations: iterations,
    );
    rawSummary.printSummary();
    
    // Print overall summary
    _printOverallSummary(results);
    
    return results;
  }
  
  Future<BenchmarkComparison> _runBenchmarkSet(
    String name,
    List<BenchmarkRunner> benchmarks,
    {required int warmupIterations, required int iterations}
  ) async {
    debugPrint('=== $name ===');
    debugPrint('');
    
    final summaries = <BenchmarkSummary>[];
    
    for (final benchmark in benchmarks) {
      try {
        final summary = await benchmark.run(
          warmupIterations: warmupIterations,
          iterations: iterations,
        );
        summary.printSummary();
        summaries.add(summary);
      } catch (e) {
        debugPrint('Error running ${benchmark.name}: $e');
      }
    }
    
    final comparison = BenchmarkComparison(summaries);
    comparison.printComparison();
    
    return comparison;
  }
  
  void _printOverallSummary(Map<String, BenchmarkComparison> results) {
    debugPrint('');
    debugPrint('=== OVERALL PERFORMANCE SUMMARY ===');
    debugPrint('');
    
    // Calculate average speedup for Lightning DB
    double totalSpeedup = 0;
    int count = 0;
    
    for (final entry in results.entries) {
      final comparison = entry.value;
      if (comparison.summaries.length >= 2) {
        final lightning = comparison.summaries.firstWhere(
          (s) => s.name.startsWith('Lightning DB'),
          orElse: () => comparison.summaries.first,
        );
        final sqlite = comparison.summaries.firstWhere(
          (s) => s.name.startsWith('SQLite'),
          orElse: () => comparison.summaries.last,
        );
        
        final speedup = sqlite.meanMilliseconds / lightning.meanMilliseconds;
        totalSpeedup += speedup;
        count++;
        
        debugPrint('${entry.key}: Lightning DB is ${speedup.toStringAsFixed(2)}x faster than SQLite');
      }
    }
    
    if (count > 0) {
      final avgSpeedup = totalSpeedup / count;
      debugPrint('');
      debugPrint('Average speedup: ${avgSpeedup.toStringAsFixed(2)}x');
    }
    
    // Performance highlights
    debugPrint('');
    debugPrint('=== PERFORMANCE HIGHLIGHTS ===');
    
    // Find best performing operations
    for (final entry in results.entries) {
      final comparison = entry.value;
      if (comparison.summaries.isNotEmpty) {
        final fastest = comparison.summaries.reduce((a, b) => 
          a.meanMicroseconds < b.meanMicroseconds ? a : b
        );
        debugPrint('${entry.key}: ${fastest.name} - ${fastest.opsPerSecond.toStringAsFixed(0)} ops/s');
      }
    }
  }
}

/// Benchmark configuration
class BenchmarkConfig {
  final int recordCount;
  final int warmupIterations;
  final int iterations;
  final bool includeRealm;
  final String outputDir;
  
  const BenchmarkConfig({
    this.recordCount = 10000,
    this.warmupIterations = 5,
    this.iterations = 100,
    this.includeRealm = false,
    this.outputDir = 'benchmark_results',
  });
  
  BenchmarkConfig copyWith({
    int? recordCount,
    int? warmupIterations,
    int? iterations,
    bool? includeRealm,
    String? outputDir,
  }) {
    return BenchmarkConfig(
      recordCount: recordCount ?? this.recordCount,
      warmupIterations: warmupIterations ?? this.warmupIterations,
      iterations: iterations ?? this.iterations,
      includeRealm: includeRealm ?? this.includeRealm,
      outputDir: outputDir ?? this.outputDir,
    );
  }
}