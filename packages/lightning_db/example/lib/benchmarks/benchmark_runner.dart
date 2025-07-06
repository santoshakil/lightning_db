import 'dart:async';
import 'dart:math';
import 'package:flutter/foundation.dart';

/// Base class for all benchmarks
abstract class BenchmarkRunner {
  final String name;
  final List<BenchmarkResult> results = [];
  
  BenchmarkRunner(this.name);
  
  /// Setup before running benchmarks
  Future<void> setup();
  
  /// Cleanup after running benchmarks
  Future<void> cleanup();
  
  /// Run a single benchmark iteration
  Future<void> runIteration(int iteration);
  
  /// Run the benchmark
  Future<BenchmarkSummary> run({
    int warmupIterations = 5,
    int iterations = 100,
    Duration? timeout,
  }) async {
    debugPrint('Setting up $name...');
    await setup();
    
    try {
      // Warmup
      debugPrint('Warming up $name...');
      for (int i = 0; i < warmupIterations; i++) {
        await runIteration(i);
      }
      
      results.clear();
      
      // Actual benchmark
      debugPrint('Running $name benchmark...');
      final stopwatch = Stopwatch()..start();
      
      for (int i = 0; i < iterations; i++) {
        final iterationStopwatch = Stopwatch()..start();
        
        if (timeout != null && stopwatch.elapsed > timeout) {
          debugPrint('Benchmark timeout reached');
          break;
        }
        
        await runIteration(i);
        
        iterationStopwatch.stop();
        results.add(BenchmarkResult(
          iteration: i,
          duration: iterationStopwatch.elapsed,
        ));
        
        if (i % 10 == 0) {
          debugPrint('Progress: ${i + 1}/$iterations');
        }
      }
      
      stopwatch.stop();
      
      return BenchmarkSummary(
        name: name,
        results: results,
        totalDuration: stopwatch.elapsed,
      );
    } finally {
      debugPrint('Cleaning up $name...');
      await cleanup();
    }
  }
}

/// Single benchmark result
class BenchmarkResult {
  final int iteration;
  final Duration duration;
  
  BenchmarkResult({
    required this.iteration,
    required this.duration,
  });
  
  double get microseconds => duration.inMicroseconds.toDouble();
  double get milliseconds => duration.inMicroseconds / 1000.0;
}

/// Benchmark summary with statistics
class BenchmarkSummary {
  final String name;
  final List<BenchmarkResult> results;
  final Duration totalDuration;
  
  BenchmarkSummary({
    required this.name,
    required this.results,
    required this.totalDuration,
  });
  
  int get iterations => results.length;
  
  double get meanMicroseconds {
    if (results.isEmpty) return 0;
    final sum = results.fold(0.0, (sum, r) => sum + r.microseconds);
    return sum / results.length;
  }
  
  double get meanMilliseconds => meanMicroseconds / 1000.0;
  
  double get minMicroseconds {
    if (results.isEmpty) return 0;
    return results.map((r) => r.microseconds).reduce(min);
  }
  
  double get maxMicroseconds {
    if (results.isEmpty) return 0;
    return results.map((r) => r.microseconds).reduce(max);
  }
  
  double get stdDevMicroseconds {
    if (results.length < 2) return 0;
    final mean = meanMicroseconds;
    final sumSquares = results.fold(0.0, (sum, r) {
      final diff = r.microseconds - mean;
      return sum + (diff * diff);
    });
    return sqrt(sumSquares / (results.length - 1));
  }
  
  double get opsPerSecond => 1000000.0 / meanMicroseconds;
  
  double percentile(int p) {
    if (results.isEmpty) return 0;
    final sorted = results.map((r) => r.microseconds).toList()..sort();
    final index = (p / 100.0 * sorted.length).floor();
    return sorted[index.clamp(0, sorted.length - 1)];
  }
  
  double get p50 => percentile(50);
  double get p90 => percentile(90);
  double get p95 => percentile(95);
  double get p99 => percentile(99);
  
  Map<String, dynamic> toJson() => {
    'name': name,
    'iterations': iterations,
    'totalDurationMs': totalDuration.inMilliseconds,
    'meanMs': meanMilliseconds,
    'minMs': minMicroseconds / 1000.0,
    'maxMs': maxMicroseconds / 1000.0,
    'stdDevMs': stdDevMicroseconds / 1000.0,
    'opsPerSecond': opsPerSecond,
    'p50Ms': p50 / 1000.0,
    'p90Ms': p90 / 1000.0,
    'p95Ms': p95 / 1000.0,
    'p99Ms': p99 / 1000.0,
  };
  
  void printSummary() {
    debugPrint('=== $name ===');
    debugPrint('Iterations: $iterations');
    debugPrint('Total time: ${totalDuration.inMilliseconds}ms');
    debugPrint('Mean: ${meanMilliseconds.toStringAsFixed(3)}ms (${opsPerSecond.toStringAsFixed(0)} ops/s)');
    debugPrint('Min: ${(minMicroseconds / 1000.0).toStringAsFixed(3)}ms');
    debugPrint('Max: ${(maxMicroseconds / 1000.0).toStringAsFixed(3)}ms');
    debugPrint('Std Dev: ${(stdDevMicroseconds / 1000.0).toStringAsFixed(3)}ms');
    debugPrint('P50: ${(p50 / 1000.0).toStringAsFixed(3)}ms');
    debugPrint('P90: ${(p90 / 1000.0).toStringAsFixed(3)}ms');
    debugPrint('P95: ${(p95 / 1000.0).toStringAsFixed(3)}ms');
    debugPrint('P99: ${(p99 / 1000.0).toStringAsFixed(3)}ms');
    debugPrint('');
  }
}

/// Comparison of multiple benchmark summaries
class BenchmarkComparison {
  final List<BenchmarkSummary> summaries;
  
  BenchmarkComparison(this.summaries);
  
  void printComparison() {
    if (summaries.isEmpty) return;
    
    debugPrint('=== Benchmark Comparison ===');
    debugPrint('');
    
    // Find baseline (first summary)
    final baseline = summaries.first;
    
    // Print header
    debugPrint('Database       | Mean (ms) | Ops/s     | vs Baseline');
    debugPrint('---------------|-----------|-----------|------------');
    
    for (final summary in summaries) {
      final speedup = baseline.meanMilliseconds / summary.meanMilliseconds;
      final speedupStr = summary == baseline ? '1.00x' : '${speedup.toStringAsFixed(2)}x';
      
      debugPrint(
        '${summary.name.padRight(14)} | '
        '${summary.meanMilliseconds.toStringAsFixed(3).padLeft(9)} | '
        '${summary.opsPerSecond.toStringAsFixed(0).padLeft(9)} | '
        '$speedupStr'
      );
    }
    
    debugPrint('');
  }
  
  Map<String, dynamic> toJson() => {
    'summaries': summaries.map((s) => s.toJson()).toList(),
    'comparison': summaries.map((s) => {
      'name': s.name,
      'relativeSpeed': summaries.first.meanMilliseconds / s.meanMilliseconds,
    }).toList(),
  };
}

/// Batch operation helper
class BatchOperation<T> {
  final List<T> items;
  final int batchSize;
  
  BatchOperation({
    required this.items,
    this.batchSize = 1000,
  });
  
  Stream<List<T>> get batches async* {
    for (int i = 0; i < items.length; i += batchSize) {
      final end = (i + batchSize).clamp(0, items.length);
      yield items.sublist(i, end);
    }
  }
  
  Future<void> processBatches(Future<void> Function(List<T>) processor) async {
    await for (final batch in batches) {
      await processor(batch);
    }
  }
}