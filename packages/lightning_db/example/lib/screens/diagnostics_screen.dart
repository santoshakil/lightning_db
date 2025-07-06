import 'dart:async';
import 'dart:math';
import 'package:flutter/material.dart';
import 'package:fl_chart/fl_chart.dart';
import 'package:lightning_db/lightning_db.dart';
import '../models/user_model.dart';

class DiagnosticsScreen extends StatefulWidget {
  const DiagnosticsScreen({super.key});

  @override
  State<DiagnosticsScreen> createState() => _DiagnosticsScreenState();
}

class _DiagnosticsScreenState extends State<DiagnosticsScreen> {
  late MonitoredLightningDb _monitoredDb;
  late FreezedCollection<User> _users;
  StreamSubscription<PerformanceEvent>? _eventSubscription;
  
  final List<PerformanceEvent> _recentEvents = [];
  PerformanceReport? _lastReport;
  Timer? _updateTimer;
  bool _isInitialized = false;
  bool _isRunningLoad = false;

  @override
  void initState() {
    super.initState();
    _initDatabase();
  }

  Future<void> _initDatabase() async {
    // Open regular database and wrap with monitoring
    final rawDb = await LightningDb.open('diagnostics_demo.db');
    _monitoredDb = MonitoredLightningDb(rawDb);
    _users = _monitoredDb.freezedCollection<User>('users');
    
    // Subscribe to performance events
    _eventSubscription = _monitoredDb.monitor.events.listen((event) {
      setState(() {
        _recentEvents.add(event);
        if (_recentEvents.length > 100) {
          _recentEvents.removeAt(0);
        }
      });
    });
    
    // Update performance report periodically
    _updateTimer = Timer.periodic(const Duration(seconds: 5), (_) {
      _updateReport();
    });
    
    setState(() {
      _isInitialized = true;
    });
    
    // Generate initial report
    await _updateReport();
  }

  Future<void> _updateReport() async {
    try {
      final report = await _monitoredDb.monitor.generateReport();
      setState(() {
        _lastReport = report;
      });
    } catch (e) {
      debugPrint('Error updating report: $e');
    }
  }

  @override
  Widget build(BuildContext context) {
    if (!_isInitialized) {
      return const Scaffold(
        body: Center(child: CircularProgressIndicator()),
      );
    }

    return Scaffold(
      appBar: AppBar(
        title: const Text('Performance Diagnostics'),
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            onPressed: _updateReport,
            tooltip: 'Refresh Report',
          ),
        ],
      ),
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Real-time metrics
            _buildRealTimeMetrics(),
            const SizedBox(height: 16),
            
            // Performance charts
            Row(
              children: [
                Expanded(child: _buildLatencyChart()),
                const SizedBox(width: 16),
                Expanded(child: _buildThroughputChart()),
              ],
            ),
            const SizedBox(height: 16),
            
            // Load testing controls
            _buildLoadTestingControls(),
            const SizedBox(height: 16),
            
            // Performance summary
            if (_lastReport != null) ...[
              _buildPerformanceSummary(),
              const SizedBox(height: 16),
            ],
            
            // Event timeline
            _buildEventTimeline(),
          ],
        ),
      ),
    );
  }

  Widget _buildRealTimeMetrics() {
    final metrics = _monitoredDb.monitor.metrics;
    
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Real-Time Metrics',
              style: Theme.of(context).textTheme.titleLarge,
            ),
            const SizedBox(height: 16),
            Row(
              children: [
                Expanded(
                  child: _buildMetricCard(
                    'Total Reads',
                    metrics.totalReads.toString(),
                    Icons.visibility,
                    Colors.blue,
                  ),
                ),
                const SizedBox(width: 8),
                Expanded(
                  child: _buildMetricCard(
                    'Total Writes',
                    metrics.totalWrites.toString(),
                    Icons.edit,
                    Colors.green,
                  ),
                ),
                const SizedBox(width: 8),
                Expanded(
                  child: _buildMetricCard(
                    'Cache Hit Rate',
                    '${(metrics.cacheHitRate * 100).toStringAsFixed(1)}%',
                    Icons.speed,
                    Colors.orange,
                  ),
                ),
                const SizedBox(width: 8),
                Expanded(
                  child: _buildMetricCard(
                    'Reads/sec',
                    metrics.readsPerSecond.toStringAsFixed(1),
                    Icons.trending_up,
                    Colors.purple,
                  ),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildMetricCard(String title, String value, IconData icon, Color color) {
    return Container(
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: color.withOpacity(0.1),
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: color.withOpacity(0.3)),
      ),
      child: Column(
        children: [
          Icon(icon, color: color, size: 24),
          const SizedBox(height: 8),
          Text(
            value,
            style: TextStyle(
              fontSize: 18,
              fontWeight: FontWeight.bold,
              color: color,
            ),
          ),
          Text(
            title,
            style: TextStyle(
              fontSize: 12,
              color: Colors.grey[600],
            ),
            textAlign: TextAlign.center,
          ),
        ],
      ),
    );
  }

  Widget _buildLatencyChart() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Operation Latency',
              style: Theme.of(context).textTheme.titleMedium,
            ),
            const SizedBox(height: 16),
            SizedBox(
              height: 200,
              child: _buildLatencyLineChart(),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildLatencyLineChart() {
    if (_recentEvents.isEmpty) {
      return const Center(child: Text('No data available'));
    }

    final spots = <FlSpot>[];
    final now = DateTime.now();
    
    for (int i = 0; i < _recentEvents.length; i++) {
      final event = _recentEvents[i];
      final secondsAgo = now.difference(event.timestamp).inSeconds.toDouble();
      final latencyMs = event.duration.inMicroseconds / 1000.0;
      spots.add(FlSpot(-secondsAgo, latencyMs));
    }

    return LineChart(
      LineChartData(
        gridData: const FlGridData(show: true),
        titlesData: FlTitlesData(
          leftTitles: AxisTitles(
            sideTitles: SideTitles(
              showTitles: true,
              reservedSize: 50,
              getTitlesWidget: (value, meta) => Text(
                '${value.toStringAsFixed(1)}ms',
                style: const TextStyle(fontSize: 10),
              ),
            ),
          ),
          bottomTitles: AxisTitles(
            sideTitles: SideTitles(
              showTitles: true,
              getTitlesWidget: (value, meta) => Text(
                '${value.toStringAsFixed(0)}s',
                style: const TextStyle(fontSize: 10),
              ),
            ),
          ),
          rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
          topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
        ),
        borderData: FlBorderData(show: true),
        lineBarsData: [
          LineChartBarData(
            spots: spots,
            isCurved: true,
            color: Colors.blue,
            barWidth: 2,
            dotData: const FlDotData(show: false),
          ),
        ],
      ),
    );
  }

  Widget _buildThroughputChart() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Operations Throughput',
              style: Theme.of(context).textTheme.titleMedium,
            ),
            const SizedBox(height: 16),
            SizedBox(
              height: 200,
              child: _buildThroughputBarChart(),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildThroughputBarChart() {
    final metrics = _monitoredDb.monitor.metrics;
    
    return BarChart(
      BarChartData(
        alignment: BarChartAlignment.spaceAround,
        maxY: max(max(metrics.readsPerSecond, metrics.writesPerSecond), 10).toDouble(),
        barTouchData: BarTouchData(enabled: false),
        titlesData: FlTitlesData(
          show: true,
          bottomTitles: AxisTitles(
            sideTitles: SideTitles(
              showTitles: true,
              getTitlesWidget: (value, meta) {
                switch (value.toInt()) {
                  case 0: return const Text('Reads', style: TextStyle(fontSize: 12));
                  case 1: return const Text('Writes', style: TextStyle(fontSize: 12));
                  case 2: return const Text('Deletes', style: TextStyle(fontSize: 12));
                  default: return const Text('');
                }
              },
            ),
          ),
          leftTitles: AxisTitles(
            sideTitles: SideTitles(
              showTitles: true,
              reservedSize: 50,
              getTitlesWidget: (value, meta) => Text(
                value.toStringAsFixed(0),
                style: const TextStyle(fontSize: 10),
              ),
            ),
          ),
          rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
          topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
        ),
        borderData: FlBorderData(show: false),
        barGroups: [
          BarChartGroupData(
            x: 0,
            barRods: [
              BarChartRodData(
                toY: metrics.readsPerSecond,
                color: Colors.blue,
                width: 40,
              ),
            ],
          ),
          BarChartGroupData(
            x: 1,
            barRods: [
              BarChartRodData(
                toY: metrics.writesPerSecond,
                color: Colors.green,
                width: 40,
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildLoadTestingControls() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Load Testing',
              style: Theme.of(context).textTheme.titleLarge,
            ),
            const SizedBox(height: 16),
            Row(
              children: [
                ElevatedButton.icon(
                  onPressed: _isRunningLoad ? null : () => _runLoadTest(100),
                  icon: const Icon(Icons.play_arrow),
                  label: const Text('Light Load (100 ops)'),
                ),
                const SizedBox(width: 16),
                ElevatedButton.icon(
                  onPressed: _isRunningLoad ? null : () => _runLoadTest(1000),
                  icon: const Icon(Icons.fast_forward),
                  label: const Text('Heavy Load (1000 ops)'),
                ),
                const SizedBox(width: 16),
                ElevatedButton.icon(
                  onPressed: _isRunningLoad ? null : _runConcurrencyTest,
                  icon: const Icon(Icons.multiple_stop),
                  label: const Text('Concurrency Test'),
                ),
                const SizedBox(width: 16),
                if (_isRunningLoad)
                  const SizedBox(
                    width: 20,
                    height: 20,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildPerformanceSummary() {
    final report = _lastReport!;
    
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Text(
                  'Performance Summary',
                  style: Theme.of(context).textTheme.titleLarge,
                ),
                const Spacer(),
                Text(
                  'Generated: ${report.timestamp.toLocal().toString().substring(0, 19)}',
                  style: Theme.of(context).textTheme.bodySmall,
                ),
              ],
            ),
            const SizedBox(height: 16),
            
            // Key metrics
            Row(
              children: [
                Expanded(
                  child: _buildSummaryMetric(
                    'Avg Read Latency',
                    '${report.metrics.averageReadLatency.inMicroseconds}μs',
                    _getLatencyColor(report.metrics.averageReadLatency.inMicroseconds),
                  ),
                ),
                Expanded(
                  child: _buildSummaryMetric(
                    'Avg Write Latency',
                    '${report.metrics.averageWriteLatency.inMicroseconds}μs',
                    _getLatencyColor(report.metrics.averageWriteLatency.inMicroseconds),
                  ),
                ),
                Expanded(
                  child: _buildSummaryMetric(
                    'Cache Hit Rate',
                    '${(report.metrics.cacheHitRate * 100).toStringAsFixed(1)}%',
                    _getCacheHitColor(report.metrics.cacheHitRate),
                  ),
                ),
                Expanded(
                  child: _buildSummaryMetric(
                    'TX Conflicts',
                    '${report.metrics.transactionConflicts}',
                    report.metrics.transactionConflicts > 0 ? Colors.red : Colors.green,
                  ),
                ),
              ],
            ),
            
            // Recommendations
            if (report.recommendations.isNotEmpty) ...[
              const SizedBox(height: 16),
              Text(
                'Recommendations',
                style: Theme.of(context).textTheme.titleMedium,
              ),
              const SizedBox(height: 8),
              ...report.recommendations.map((rec) => Padding(
                padding: const EdgeInsets.symmetric(vertical: 2),
                child: Row(
                  children: [
                    Icon(Icons.lightbulb_outline, size: 16, color: Colors.amber),
                    const SizedBox(width: 8),
                    Expanded(child: Text(rec, style: const TextStyle(fontSize: 12))),
                  ],
                ),
              )),
            ],
          ],
        ),
      ),
    );
  }

  Widget _buildSummaryMetric(String label, String value, Color color) {
    return Column(
      children: [
        Text(
          value,
          style: TextStyle(
            fontSize: 18,
            fontWeight: FontWeight.bold,
            color: color,
          ),
        ),
        Text(
          label,
          style: TextStyle(
            fontSize: 12,
            color: Colors.grey[600],
          ),
          textAlign: TextAlign.center,
        ),
      ],
    );
  }

  Color _getLatencyColor(int microseconds) {
    if (microseconds < 100) return Colors.green;
    if (microseconds < 1000) return Colors.orange;
    return Colors.red;
  }

  Color _getCacheHitColor(double hitRate) {
    if (hitRate > 0.9) return Colors.green;
    if (hitRate > 0.7) return Colors.orange;
    return Colors.red;
  }

  Widget _buildEventTimeline() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Recent Events',
              style: Theme.of(context).textTheme.titleLarge,
            ),
            const SizedBox(height: 16),
            SizedBox(
              height: 300,
              child: ListView.builder(
                itemCount: _recentEvents.length,
                itemBuilder: (context, index) {
                  final event = _recentEvents[_recentEvents.length - 1 - index];
                  return _buildEventItem(event);
                },
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildEventItem(PerformanceEvent event) {
    final icon = _getEventIcon(event.type);
    final color = _getEventColor(event.type);
    
    return ListTile(
      dense: true,
      leading: Icon(icon, color: color, size: 16),
      title: Text(
        event.operation,
        style: const TextStyle(fontSize: 12),
      ),
      subtitle: Text(
        '${event.duration.inMicroseconds}μs',
        style: const TextStyle(fontSize: 10),
      ),
      trailing: Text(
        _formatTimestamp(event.timestamp),
        style: const TextStyle(fontSize: 10),
      ),
    );
  }

  IconData _getEventIcon(EventType type) {
    switch (type) {
      case EventType.read: return Icons.visibility;
      case EventType.write: return Icons.edit;
      case EventType.delete: return Icons.delete;
      case EventType.transaction: return Icons.sync;
      case EventType.scan: return Icons.search;
      case EventType.cacheHit: return Icons.speed;
      case EventType.cacheMiss: return Icons.slow_motion_video;
      case EventType.systemMetric: return Icons.computer;
    }
  }

  Color _getEventColor(EventType type) {
    switch (type) {
      case EventType.read: return Colors.blue;
      case EventType.write: return Colors.green;
      case EventType.delete: return Colors.red;
      case EventType.transaction: return Colors.purple;
      case EventType.scan: return Colors.orange;
      case EventType.cacheHit: return Colors.teal;
      case EventType.cacheMiss: return Colors.grey;
      case EventType.systemMetric: return Colors.indigo;
    }
  }

  String _formatTimestamp(DateTime timestamp) {
    final now = DateTime.now();
    final diff = now.difference(timestamp);
    
    if (diff.inSeconds < 60) return '${diff.inSeconds}s ago';
    if (diff.inMinutes < 60) return '${diff.inMinutes}m ago';
    return '${diff.inHours}h ago';
  }

  Future<void> _runLoadTest(int operations) async {
    setState(() {
      _isRunningLoad = true;
    });

    try {
      for (int i = 0; i < operations; i++) {
        final user = User(
          id: 'load_user_$i',
          name: 'Load Test User $i',
          email: 'load$i@test.com',
          age: 20 + (i % 50),
          createdAt: DateTime.now(),
          metadata: {
            'loadTest': true,
            'iteration': i,
          },
        );

        await _users.add(user);

        // Read back immediately
        await _users.get(user.id);

        // Small delay to see progress
        if (i % 10 == 0) {
          await Future.delayed(const Duration(milliseconds: 1));
        }
      }

      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Load test completed: $operations operations'),
          backgroundColor: Colors.green,
        ),
      );
    } catch (e) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Load test failed: $e'),
          backgroundColor: Colors.red,
        ),
      );
    } finally {
      setState(() {
        _isRunningLoad = false;
      });
    }
  }

  Future<void> _runConcurrencyTest() async {
    setState(() {
      _isRunningLoad = true;
    });

    try {
      // Run multiple operations concurrently
      final futures = List.generate(10, (i) async {
        for (int j = 0; j < 50; j++) {
          final user = User(
            id: 'concurrent_${i}_$j',
            name: 'Concurrent User $i-$j',
            email: 'concurrent${i}_$j@test.com',
            age: 25,
            createdAt: DateTime.now(),
            metadata: {'thread': i, 'iteration': j},
          );

          await _users.add(user);
          await _users.get(user.id);
        }
      });

      await Future.wait(futures);

      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Concurrency test completed: 500 concurrent operations'),
          backgroundColor: Colors.green,
        ),
      );
    } catch (e) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Concurrency test failed: $e'),
          backgroundColor: Colors.red,
        ),
      );
    } finally {
      setState(() {
        _isRunningLoad = false;
      });
    }
  }

  @override
  void dispose() {
    _eventSubscription?.cancel();
    _updateTimer?.cancel();
    _monitoredDb.close();
    super.dispose();
  }
}