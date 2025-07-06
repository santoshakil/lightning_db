import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:typed_data';
import 'package:meta/meta.dart';
import 'database.dart';
import 'errors.dart';

/// Performance monitoring and diagnostics for Lightning DB
class LightningDbMonitor {
  final LightningDb _db;
  final PerformanceMetrics _metrics = PerformanceMetrics();
  final List<StreamController<PerformanceEvent>> _eventStreams = [];
  final List<PerformanceEvent> _eventHistory = [];
  
  Timer? _metricsTimer;
  bool _isEnabled = true;
  
  LightningDbMonitor(this._db);
  
  /// Enable or disable monitoring
  void setEnabled(bool enabled) {
    _isEnabled = enabled;
    if (enabled) {
      _startMetricsCollection();
    } else {
      _stopMetricsCollection();
    }
  }
  
  /// Get current performance metrics
  PerformanceMetrics get metrics => _metrics;
  
  /// Get performance event stream
  Stream<PerformanceEvent> get events {
    final controller = StreamController<PerformanceEvent>.broadcast();
    _eventStreams.add(controller);
    return controller.stream;
  }
  
  /// Record a performance event
  void recordEvent(PerformanceEvent event) {
    if (!_isEnabled) return;
    
    _eventHistory.add(event);
    if (_eventHistory.length > 10000) {
      _eventHistory.removeAt(0);
    }
    
    _metrics.recordEvent(event);
    
    for (final stream in _eventStreams) {
      if (!stream.isClosed) {
        stream.add(event);
      }
    }
  }
  
  /// Get performance summary
  PerformanceSummary getSummary({Duration? period}) {
    final cutoff = period != null 
        ? DateTime.now().subtract(period)
        : null;
    
    final relevantEvents = cutoff != null
        ? _eventHistory.where((e) => e.timestamp.isAfter(cutoff))
        : _eventHistory;
    
    return PerformanceSummary.fromEvents(relevantEvents.toList());
  }
  
  /// Generate performance report
  Future<PerformanceReport> generateReport() async {
    final dbStats = await _db.getStatistics();
    final systemStats = await _getSystemStatistics();
    
    return PerformanceReport(
      timestamp: DateTime.now(),
      databasePath: _db.path,
      metrics: _metrics,
      summary: getSummary(const Duration(hours: 1)),
      databaseStatistics: dbStats,
      systemStatistics: systemStats,
      recommendations: _generateRecommendations(),
    );
  }
  
  /// Start automatic metrics collection
  void _startMetricsCollection() {
    _metricsTimer?.cancel();
    _metricsTimer = Timer.periodic(const Duration(seconds: 1), (_) {
      _collectSystemMetrics();
    });
  }
  
  /// Stop automatic metrics collection
  void _stopMetricsCollection() {
    _metricsTimer?.cancel();
  }
  
  Future<void> _collectSystemMetrics() async {
    try {
      recordEvent(PerformanceEvent(
        type: EventType.systemMetric,
        operation: 'memory_usage',
        duration: Duration.zero,
        metadata: await _getMemoryStatistics(),
      ));
    } catch (e) {
      // Ignore errors in metrics collection
    }
  }
  
  Future<Map<String, dynamic>> _getSystemStatistics() async {
    return {
      'timestamp': DateTime.now().toIso8601String(),
      'memory': await _getMemoryStatistics(),
      'io': _getIOStatistics(),
    };
  }
  
  Future<Map<String, dynamic>> _getMemoryStatistics() async {
    // Note: In production, use platform-specific APIs
    return {
      'usedBytes': _metrics.totalBytesRead + _metrics.totalBytesWritten,
      'allocatedBytes': _metrics.totalBytesRead + _metrics.totalBytesWritten,
      'collections': 0, // GC collections
    };
  }
  
  Map<String, dynamic> _getIOStatistics() {
    return {
      'readsPerSecond': _metrics.readsPerSecond,
      'writesPerSecond': _metrics.writesPerSecond,
      'totalReads': _metrics.totalReads,
      'totalWrites': _metrics.totalWrites,
      'totalBytesRead': _metrics.totalBytesRead,
      'totalBytesWritten': _metrics.totalBytesWritten,
    };
  }
  
  List<String> _generateRecommendations() {
    final recommendations = <String>[];
    
    // Check read/write ratio
    if (_metrics.totalReads > 0 && _metrics.totalWrites > 0) {
      final ratio = _metrics.totalReads / _metrics.totalWrites;
      if (ratio > 10) {
        recommendations.add('Consider enabling read caching for read-heavy workload');
      } else if (ratio < 0.1) {
        recommendations.add('Consider batch writes for write-heavy workload');
      }
    }
    
    // Check cache hit rate
    if (_metrics.cacheHitRate < 0.8) {
      recommendations.add('Low cache hit rate (${(_metrics.cacheHitRate * 100).toStringAsFixed(1)}%). Consider increasing cache size');
    }
    
    // Check operation latency
    if (_metrics.averageReadLatency.inMicroseconds > 1000) {
      recommendations.add('High read latency detected. Consider adding indexes or optimizing queries');
    }
    
    if (_metrics.averageWriteLatency.inMicroseconds > 5000) {
      recommendations.add('High write latency detected. Consider using batch operations');
    }
    
    // Check transaction conflicts
    if (_metrics.transactionConflicts > 0) {
      recommendations.add('Transaction conflicts detected. Consider optimizing concurrent access patterns');
    }
    
    return recommendations;
  }
  
  /// Dispose of monitoring resources
  void dispose() {
    _stopMetricsCollection();
    for (final stream in _eventStreams) {
      stream.close();
    }
    _eventStreams.clear();
  }
}

/// Performance metrics aggregator
class PerformanceMetrics {
  int totalReads = 0;
  int totalWrites = 0;
  int totalDeletes = 0;
  int totalScans = 0;
  int totalTransactions = 0;
  int transactionConflicts = 0;
  int cacheHits = 0;
  int cacheMisses = 0;
  
  int totalBytesRead = 0;
  int totalBytesWritten = 0;
  
  Duration totalReadTime = Duration.zero;
  Duration totalWriteTime = Duration.zero;
  Duration totalDeleteTime = Duration.zero;
  Duration totalScanTime = Duration.zero;
  Duration totalTransactionTime = Duration.zero;
  
  final Queue<PerformanceEvent> _recentEvents = Queue();
  DateTime _lastMetricsUpdate = DateTime.now();
  
  /// Record a performance event
  void recordEvent(PerformanceEvent event) {
    _recentEvents.add(event);
    if (_recentEvents.length > 1000) {
      _recentEvents.removeFirst();
    }
    
    switch (event.type) {
      case EventType.read:
        totalReads++;
        totalReadTime += event.duration;
        if (event.metadata['bytes'] != null) {
          totalBytesRead += event.metadata['bytes'] as int;
        }
        break;
        
      case EventType.write:
        totalWrites++;
        totalWriteTime += event.duration;
        if (event.metadata['bytes'] != null) {
          totalBytesWritten += event.metadata['bytes'] as int;
        }
        break;
        
      case EventType.delete:
        totalDeletes++;
        totalDeleteTime += event.duration;
        break;
        
      case EventType.scan:
        totalScans++;
        totalScanTime += event.duration;
        break;
        
      case EventType.transaction:
        totalTransactions++;
        totalTransactionTime += event.duration;
        if (event.metadata['conflict'] == true) {
          transactionConflicts++;
        }
        break;
        
      case EventType.cacheHit:
        cacheHits++;
        break;
        
      case EventType.cacheMiss:
        cacheMisses++;
        break;
        
      case EventType.systemMetric:
        // System metrics don't update operation counters
        break;
    }
    
    _lastMetricsUpdate = DateTime.now();
  }
  
  /// Average read latency
  Duration get averageReadLatency => totalReads > 0
      ? Duration(microseconds: totalReadTime.inMicroseconds ~/ totalReads)
      : Duration.zero;
  
  /// Average write latency
  Duration get averageWriteLatency => totalWrites > 0
      ? Duration(microseconds: totalWriteTime.inMicroseconds ~/ totalWrites)
      : Duration.zero;
  
  /// Average transaction time
  Duration get averageTransactionTime => totalTransactions > 0
      ? Duration(microseconds: totalTransactionTime.inMicroseconds ~/ totalTransactions)
      : Duration.zero;
  
  /// Cache hit rate
  double get cacheHitRate {
    final total = cacheHits + cacheMisses;
    return total > 0 ? cacheHits / total : 0.0;
  }
  
  /// Operations per second (reads)
  double get readsPerSecond {
    final events = _recentEvents.where((e) => e.type == EventType.read);
    if (events.isEmpty) return 0.0;
    
    final timeSpan = DateTime.now().difference(events.first.timestamp);
    return timeSpan.inSeconds > 0 ? events.length / timeSpan.inSeconds : 0.0;
  }
  
  /// Operations per second (writes)
  double get writesPerSecond {
    final events = _recentEvents.where((e) => e.type == EventType.write);
    if (events.isEmpty) return 0.0;
    
    final timeSpan = DateTime.now().difference(events.first.timestamp);
    return timeSpan.inSeconds > 0 ? events.length / timeSpan.inSeconds : 0.0;
  }
  
  /// Throughput in bytes per second
  double get bytesPerSecond {
    if (_recentEvents.isEmpty) return 0.0;
    
    final timeSpan = DateTime.now().difference(_recentEvents.first.timestamp);
    final totalBytes = totalBytesRead + totalBytesWritten;
    
    return timeSpan.inSeconds > 0 ? totalBytes / timeSpan.inSeconds : 0.0;
  }
  
  /// Convert to JSON
  Map<String, dynamic> toJson() => {
    'totalReads': totalReads,
    'totalWrites': totalWrites,
    'totalDeletes': totalDeletes,
    'totalScans': totalScans,
    'totalTransactions': totalTransactions,
    'transactionConflicts': transactionConflicts,
    'cacheHits': cacheHits,
    'cacheMisses': cacheMisses,
    'totalBytesRead': totalBytesRead,
    'totalBytesWritten': totalBytesWritten,
    'averageReadLatencyUs': averageReadLatency.inMicroseconds,
    'averageWriteLatencyUs': averageWriteLatency.inMicroseconds,
    'averageTransactionTimeUs': averageTransactionTime.inMicroseconds,
    'cacheHitRate': cacheHitRate,
    'readsPerSecond': readsPerSecond,
    'writesPerSecond': writesPerSecond,
    'bytesPerSecond': bytesPerSecond,
    'lastUpdate': _lastMetricsUpdate.toIso8601String(),
  };
}

/// Performance event
class PerformanceEvent {
  final EventType type;
  final String operation;
  final Duration duration;
  final DateTime timestamp;
  final Map<String, dynamic> metadata;
  
  PerformanceEvent({
    required this.type,
    required this.operation,
    required this.duration,
    Map<String, dynamic>? metadata,
  }) : timestamp = DateTime.now(),
       metadata = metadata ?? {};
  
  Map<String, dynamic> toJson() => {
    'type': type.toString().split('.').last,
    'operation': operation,
    'durationUs': duration.inMicroseconds,
    'timestamp': timestamp.toIso8601String(),
    'metadata': metadata,
  };
}

/// Performance event types
enum EventType {
  read,
  write,
  delete,
  scan,
  transaction,
  cacheHit,
  cacheMiss,
  systemMetric,
}

/// Performance summary
class PerformanceSummary {
  final int totalEvents;
  final Duration timeSpan;
  final Map<EventType, int> eventCounts;
  final Map<EventType, Duration> totalDurations;
  final double averageOpsPerSecond;
  
  PerformanceSummary({
    required this.totalEvents,
    required this.timeSpan,
    required this.eventCounts,
    required this.totalDurations,
    required this.averageOpsPerSecond,
  });
  
  factory PerformanceSummary.fromEvents(List<PerformanceEvent> events) {
    if (events.isEmpty) {
      return PerformanceSummary(
        totalEvents: 0,
        timeSpan: Duration.zero,
        eventCounts: {},
        totalDurations: {},
        averageOpsPerSecond: 0.0,
      );
    }
    
    final eventCounts = <EventType, int>{};
    final totalDurations = <EventType, Duration>{};
    
    for (final event in events) {
      eventCounts[event.type] = (eventCounts[event.type] ?? 0) + 1;
      totalDurations[event.type] = (totalDurations[event.type] ?? Duration.zero) + event.duration;
    }
    
    final timeSpan = events.last.timestamp.difference(events.first.timestamp);
    final opsPerSecond = timeSpan.inSeconds > 0 ? events.length / timeSpan.inSeconds : 0.0;
    
    return PerformanceSummary(
      totalEvents: events.length,
      timeSpan: timeSpan,
      eventCounts: eventCounts,
      totalDurations: totalDurations,
      averageOpsPerSecond: opsPerSecond,
    );
  }
  
  Map<String, dynamic> toJson() => {
    'totalEvents': totalEvents,
    'timeSpanMs': timeSpan.inMilliseconds,
    'eventCounts': eventCounts.map((k, v) => MapEntry(k.toString().split('.').last, v)),
    'totalDurations': totalDurations.map((k, v) => MapEntry(k.toString().split('.').last, v.inMicroseconds)),
    'averageOpsPerSecond': averageOpsPerSecond,
  };
}

/// Comprehensive performance report
class PerformanceReport {
  final DateTime timestamp;
  final String databasePath;
  final PerformanceMetrics metrics;
  final PerformanceSummary summary;
  final Map<String, dynamic> databaseStatistics;
  final Map<String, dynamic> systemStatistics;
  final List<String> recommendations;
  
  PerformanceReport({
    required this.timestamp,
    required this.databasePath,
    required this.metrics,
    required this.summary,
    required this.databaseStatistics,
    required this.systemStatistics,
    required this.recommendations,
  });
  
  Map<String, dynamic> toJson() => {
    'timestamp': timestamp.toIso8601String(),
    'databasePath': databasePath,
    'metrics': metrics.toJson(),
    'summary': summary.toJson(),
    'databaseStatistics': databaseStatistics,
    'systemStatistics': systemStatistics,
    'recommendations': recommendations,
  };
  
  String toFormattedString() {
    final buffer = StringBuffer();
    
    buffer.writeln('Lightning DB Performance Report');
    buffer.writeln('Generated: ${timestamp.toLocal()}');
    buffer.writeln('Database: $databasePath');
    buffer.writeln('');
    
    buffer.writeln('=== Performance Metrics ===');
    buffer.writeln('Total Reads: ${metrics.totalReads}');
    buffer.writeln('Total Writes: ${metrics.totalWrites}');
    buffer.writeln('Total Transactions: ${metrics.totalTransactions}');
    buffer.writeln('Cache Hit Rate: ${(metrics.cacheHitRate * 100).toStringAsFixed(1)}%');
    buffer.writeln('Avg Read Latency: ${metrics.averageReadLatency.inMicroseconds}μs');
    buffer.writeln('Avg Write Latency: ${metrics.averageWriteLatency.inMicroseconds}μs');
    buffer.writeln('Reads/sec: ${metrics.readsPerSecond.toStringAsFixed(1)}');
    buffer.writeln('Writes/sec: ${metrics.writesPerSecond.toStringAsFixed(1)}');
    buffer.writeln('');
    
    buffer.writeln('=== Database Statistics ===');
    for (final entry in databaseStatistics.entries) {
      buffer.writeln('${entry.key}: ${entry.value}');
    }
    buffer.writeln('');
    
    if (recommendations.isNotEmpty) {
      buffer.writeln('=== Recommendations ===');
      for (final rec in recommendations) {
        buffer.writeln('• $rec');
      }
      buffer.writeln('');
    }
    
    return buffer.toString();
  }
}

/// Monitored database wrapper
class MonitoredLightningDb implements LightningDb {
  final LightningDb _delegate;
  final LightningDbMonitor monitor;
  
  MonitoredLightningDb(this._delegate) : monitor = LightningDbMonitor(_delegate);
  
  @override
  String get path => _delegate.path;
  
  @override
  bool get isOpen => _delegate.isOpen;
  
  @override
  Future<void> close() async {
    monitor.dispose();
    return _delegate.close();
  }
  
  @override
  Future<String?> get(String key) async {
    final stopwatch = Stopwatch()..start();
    
    try {
      final result = await _delegate.get(key);
      
      monitor.recordEvent(PerformanceEvent(
        type: EventType.read,
        operation: 'get',
        duration: stopwatch.elapsed,
        metadata: {
          'key': key,
          'found': result != null,
          'bytes': result?.length ?? 0,
        },
      ));
      
      if (result != null) {
        monitor.recordEvent(PerformanceEvent(
          type: EventType.cacheHit,
          operation: 'get',
          duration: Duration.zero,
        ));
      } else {
        monitor.recordEvent(PerformanceEvent(
          type: EventType.cacheMiss,
          operation: 'get',
          duration: Duration.zero,
        ));
      }
      
      return result;
    } catch (e) {
      monitor.recordEvent(PerformanceEvent(
        type: EventType.read,
        operation: 'get_error',
        duration: stopwatch.elapsed,
        metadata: {'error': e.toString()},
      ));
      rethrow;
    }
  }
  
  @override
  Future<void> put(String key, String value) async {
    final stopwatch = Stopwatch()..start();
    
    try {
      await _delegate.put(key, value);
      
      monitor.recordEvent(PerformanceEvent(
        type: EventType.write,
        operation: 'put',
        duration: stopwatch.elapsed,
        metadata: {
          'key': key,
          'bytes': value.length,
        },
      ));
    } catch (e) {
      monitor.recordEvent(PerformanceEvent(
        type: EventType.write,
        operation: 'put_error',
        duration: stopwatch.elapsed,
        metadata: {'error': e.toString()},
      ));
      rethrow;
    }
  }
  
  @override
  Future<void> delete(String key) async {
    final stopwatch = Stopwatch()..start();
    
    try {
      await _delegate.delete(key);
      
      monitor.recordEvent(PerformanceEvent(
        type: EventType.delete,
        operation: 'delete',
        duration: stopwatch.elapsed,
        metadata: {'key': key},
      ));
    } catch (e) {
      monitor.recordEvent(PerformanceEvent(
        type: EventType.delete,
        operation: 'delete_error',
        duration: stopwatch.elapsed,
        metadata: {'error': e.toString()},
      ));
      rethrow;
    }
  }
  
  @override
  Future<R> transaction<R>(TransactionCallback<R> callback) async {
    final stopwatch = Stopwatch()..start();
    
    try {
      final result = await _delegate.transaction(callback);
      
      monitor.recordEvent(PerformanceEvent(
        type: EventType.transaction,
        operation: 'transaction',
        duration: stopwatch.elapsed,
        metadata: {'success': true},
      ));
      
      return result;
    } on TransactionConflictException catch (e) {
      monitor.recordEvent(PerformanceEvent(
        type: EventType.transaction,
        operation: 'transaction_conflict',
        duration: stopwatch.elapsed,
        metadata: {'conflict': true, 'error': e.toString()},
      ));
      rethrow;
    } catch (e) {
      monitor.recordEvent(PerformanceEvent(
        type: EventType.transaction,
        operation: 'transaction_error',
        duration: stopwatch.elapsed,
        metadata: {'error': e.toString()},
      ));
      rethrow;
    }
  }
  
  @override
  Stream<KeyValue> scanStream({String? startKey, String? endKey}) async* {
    final stopwatch = Stopwatch()..start();
    int count = 0;
    
    try {
      await for (final kv in _delegate.scanStream(startKey: startKey, endKey: endKey)) {
        count++;
        yield kv;
      }
      
      monitor.recordEvent(PerformanceEvent(
        type: EventType.scan,
        operation: 'scan',
        duration: stopwatch.elapsed,
        metadata: {
          'startKey': startKey,
          'endKey': endKey,
          'resultCount': count,
        },
      ));
    } catch (e) {
      monitor.recordEvent(PerformanceEvent(
        type: EventType.scan,
        operation: 'scan_error',
        duration: stopwatch.elapsed,
        metadata: {'error': e.toString()},
      ));
      rethrow;
    }
  }
  
  // Delegate other methods
  @override
  Future<Uint8List?> getBytes(String key) => _delegate.getBytes(key);
  
  @override
  Future<void> putBytes(String key, Uint8List value) => _delegate.putBytes(key, value);
  
  @override
  Future<Map<String, dynamic>?> getJson(String key) => _delegate.getJson(key);
  
  @override
  Future<void> putJson(String key, Map<String, dynamic> value) => _delegate.putJson(key, value);
  
  @override
  Future<bool> contains(String key) => _delegate.contains(key);
  
  @override
  FreezedCollection<T> freezedCollection<T>(String name) => _delegate.freezedCollection<T>(name);
  
  @override
  Future<DatabaseStatistics> getStatistics() => _delegate.getStatistics();
  
  @override
  Future<void> compact() => _delegate.compact();
  
  @override
  Future<void> backup(String path) => _delegate.backup(path);
}

/// Performance profiler for detailed analysis
class PerformanceProfiler {
  final Map<String, List<Duration>> _operationTimes = {};
  final Map<String, int> _operationCounts = {};
  
  /// Start profiling an operation
  ProfilerScope startOperation(String operationName) {
    return ProfilerScope(this, operationName);
  }
  
  /// Record operation completion
  void recordOperation(String operationName, Duration duration) {
    _operationTimes.putIfAbsent(operationName, () => []).add(duration);
    _operationCounts[operationName] = (_operationCounts[operationName] ?? 0) + 1;
  }
  
  /// Get profiling results
  Map<String, OperationProfile> getProfiles() {
    final profiles = <String, OperationProfile>{};
    
    for (final operation in _operationTimes.keys) {
      final times = _operationTimes[operation]!;
      profiles[operation] = OperationProfile(
        operationName: operation,
        count: _operationCounts[operation]!,
        times: times,
      );
    }
    
    return profiles;
  }
  
  /// Clear profiling data
  void clear() {
    _operationTimes.clear();
    _operationCounts.clear();
  }
}

/// Profiler scope for automatic timing
class ProfilerScope {
  final PerformanceProfiler _profiler;
  final String _operationName;
  final Stopwatch _stopwatch = Stopwatch()..start();
  
  ProfilerScope(this._profiler, this._operationName);
  
  /// Complete the operation and record timing
  void complete() {
    _stopwatch.stop();
    _profiler.recordOperation(_operationName, _stopwatch.elapsed);
  }
}

/// Operation profile statistics
class OperationProfile {
  final String operationName;
  final int count;
  final List<Duration> times;
  
  OperationProfile({
    required this.operationName,
    required this.count,
    required this.times,
  });
  
  Duration get totalTime => times.fold(Duration.zero, (sum, time) => sum + time);
  
  Duration get averageTime => count > 0 
      ? Duration(microseconds: totalTime.inMicroseconds ~/ count)
      : Duration.zero;
  
  Duration get minTime => times.isEmpty ? Duration.zero : times.reduce((a, b) => a < b ? a : b);
  
  Duration get maxTime => times.isEmpty ? Duration.zero : times.reduce((a, b) => a > b ? a : b);
  
  double get operationsPerSecond => totalTime.inMicroseconds > 0
      ? count / (totalTime.inMicroseconds / 1000000.0)
      : 0.0;
  
  Map<String, dynamic> toJson() => {
    'operationName': operationName,
    'count': count,
    'totalTimeUs': totalTime.inMicroseconds,
    'averageTimeUs': averageTime.inMicroseconds,
    'minTimeUs': minTime.inMicroseconds,
    'maxTimeUs': maxTime.inMicroseconds,
    'operationsPerSecond': operationsPerSecond,
  };
}