import 'package:flutter/material.dart';
import 'package:fl_chart/fl_chart.dart';
import '../benchmarks/benchmark_suite.dart';
import '../benchmarks/benchmark_runner.dart';

class BenchmarkScreen extends StatefulWidget {
  const BenchmarkScreen({super.key});

  @override
  State<BenchmarkScreen> createState() => _BenchmarkScreenState();
}

class _BenchmarkScreenState extends State<BenchmarkScreen> {
  final Map<String, BenchmarkComparison> _results = {};
  bool _isRunning = false;
  String _currentBenchmark = '';
  double _progress = 0;
  
  BenchmarkConfig _config = const BenchmarkConfig(
    recordCount: 1000, // Start with smaller dataset for UI
    iterations: 50,
    warmupIterations: 3,
  );

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Database Benchmarks'),
        actions: [
          IconButton(
            icon: const Icon(Icons.settings),
            onPressed: _showConfigDialog,
          ),
        ],
      ),
      body: Column(
        children: [
          // Control panel
          Card(
            margin: const EdgeInsets.all(16),
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                children: [
                  Row(
                    children: [
                      Expanded(
                        child: ElevatedButton.icon(
                          onPressed: _isRunning ? null : _runBenchmarks,
                          icon: const Icon(Icons.play_arrow),
                          label: const Text('Run Benchmarks'),
                        ),
                      ),
                      const SizedBox(width: 16),
                      Expanded(
                        child: ElevatedButton.icon(
                          onPressed: _results.isEmpty ? null : _exportResults,
                          icon: const Icon(Icons.download),
                          label: const Text('Export Results'),
                        ),
                      ),
                    ],
                  ),
                  if (_isRunning) ...[
                    const SizedBox(height: 16),
                    LinearProgressIndicator(value: _progress),
                    const SizedBox(height: 8),
                    Text(_currentBenchmark),
                  ],
                ],
              ),
            ),
          ),
          
          // Results
          Expanded(
            child: _results.isEmpty
                ? const Center(
                    child: Text(
                      'Run benchmarks to see results',
                      style: TextStyle(fontSize: 18, color: Colors.grey),
                    ),
                  )
                : ListView(
                    padding: const EdgeInsets.all(16),
                    children: [
                      _buildSummaryCard(),
                      const SizedBox(height: 16),
                      ..._results.entries.map((entry) => Padding(
                        padding: const EdgeInsets.only(bottom: 16),
                        child: _buildBenchmarkCard(entry.key, entry.value),
                      )),
                    ],
                  ),
          ),
        ],
      ),
    );
  }

  Widget _buildSummaryCard() {
    // Calculate overall performance
    double lightningDbScore = 0;
    double sqliteScore = 0;
    int count = 0;
    
    for (final comparison in _results.values) {
      for (final summary in comparison.summaries) {
        if (summary.name.contains('Lightning DB')) {
          lightningDbScore += summary.opsPerSecond;
          count++;
        } else if (summary.name.contains('SQLite')) {
          sqliteScore += summary.opsPerSecond;
        }
      }
    }
    
    final avgLightning = count > 0 ? lightningDbScore / count : 0;
    final avgSqlite = count > 0 ? sqliteScore / count : 0;
    final speedup = avgSqlite > 0 ? avgLightning / avgSqlite : 0;
    
    return Card(
      color: Colors.blue[50],
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Overall Performance',
              style: Theme.of(context).textTheme.headlineSmall,
            ),
            const SizedBox(height: 16),
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceAround,
              children: [
                _buildMetric(
                  'Lightning DB',
                  '${avgLightning.toStringAsFixed(0)} ops/s',
                  Colors.blue,
                ),
                _buildMetric(
                  'vs SQLite',
                  '${speedup.toStringAsFixed(1)}x faster',
                  Colors.green,
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildMetric(String label, String value, Color color) {
    return Column(
      children: [
        Text(
          value,
          style: TextStyle(
            fontSize: 24,
            fontWeight: FontWeight.bold,
            color: color,
          ),
        ),
        Text(
          label,
          style: const TextStyle(color: Colors.grey),
        ),
      ],
    );
  }

  Widget _buildBenchmarkCard(String name, BenchmarkComparison comparison) {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              name.replaceAll('_', ' ').toUpperCase(),
              style: Theme.of(context).textTheme.titleLarge,
            ),
            const SizedBox(height: 16),
            
            // Performance chart
            SizedBox(
              height: 200,
              child: _buildPerformanceChart(comparison),
            ),
            
            const SizedBox(height: 16),
            
            // Detailed metrics
            ..._buildDetailedMetrics(comparison),
          ],
        ),
      ),
    );
  }

  Widget _buildPerformanceChart(BenchmarkComparison comparison) {
    final data = comparison.summaries
        .map((s) => BarChartGroupData(
              x: comparison.summaries.indexOf(s),
              barRods: [
                BarChartRodData(
                  toY: s.opsPerSecond,
                  color: _getColorForDb(s.name),
                  width: 40,
                  borderRadius: const BorderRadius.vertical(top: Radius.circular(4)),
                ),
              ],
            ))
        .toList();

    return BarChart(
      BarChartData(
        barGroups: data,
        titlesData: FlTitlesData(
          leftTitles: AxisTitles(
            sideTitles: SideTitles(
              showTitles: true,
              reservedSize: 60,
              getTitlesWidget: (value, meta) => Text(
                '${(value / 1000).toStringAsFixed(1)}k',
                style: const TextStyle(fontSize: 12),
              ),
            ),
          ),
          bottomTitles: AxisTitles(
            sideTitles: SideTitles(
              showTitles: true,
              getTitlesWidget: (value, meta) {
                if (value.toInt() < comparison.summaries.length) {
                  return Padding(
                    padding: const EdgeInsets.only(top: 8),
                    child: Text(
                      comparison.summaries[value.toInt()].name.split(' - ').first,
                      style: const TextStyle(fontSize: 12),
                    ),
                  );
                }
                return const SizedBox();
              },
            ),
          ),
          rightTitles: const AxisTitles(
            sideTitles: SideTitles(showTitles: false),
          ),
          topTitles: const AxisTitles(
            sideTitles: SideTitles(showTitles: false),
          ),
        ),
        gridData: const FlGridData(show: true, horizontalInterval: 1000),
        borderData: FlBorderData(show: false),
      ),
    );
  }

  List<Widget> _buildDetailedMetrics(BenchmarkComparison comparison) {
    return comparison.summaries.map((summary) {
      final baseline = comparison.summaries.first;
      final speedup = baseline.meanMilliseconds / summary.meanMilliseconds;
      
      return Padding(
        padding: const EdgeInsets.only(bottom: 8),
        child: Row(
          children: [
            Container(
              width: 12,
              height: 12,
              decoration: BoxDecoration(
                color: _getColorForDb(summary.name),
                shape: BoxShape.circle,
              ),
            ),
            const SizedBox(width: 8),
            Expanded(
              child: Text(
                summary.name,
                style: const TextStyle(fontWeight: FontWeight.bold),
              ),
            ),
            Text(
              '${summary.meanMilliseconds.toStringAsFixed(3)}ms',
              style: const TextStyle(fontFamily: 'monospace'),
            ),
            const SizedBox(width: 16),
            Text(
              '${summary.opsPerSecond.toStringAsFixed(0)} ops/s',
              style: const TextStyle(fontFamily: 'monospace'),
            ),
            const SizedBox(width: 16),
            Container(
              padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
              decoration: BoxDecoration(
                color: speedup >= 1 ? Colors.green[100] : Colors.red[100],
                borderRadius: BorderRadius.circular(12),
              ),
              child: Text(
                '${speedup.toStringAsFixed(2)}x',
                style: TextStyle(
                  fontSize: 12,
                  color: speedup >= 1 ? Colors.green[700] : Colors.red[700],
                  fontWeight: FontWeight.bold,
                ),
              ),
            ),
          ],
        ),
      );
    }).toList();
  }

  Color _getColorForDb(String dbName) {
    if (dbName.contains('Lightning')) return Colors.blue;
    if (dbName.contains('SQLite')) return Colors.orange;
    if (dbName.contains('Realm')) return Colors.purple;
    return Colors.grey;
  }

  Future<void> _runBenchmarks() async {
    setState(() {
      _isRunning = true;
      _results.clear();
      _progress = 0;
    });

    try {
      final suite = BenchmarkSuite(
        recordCount: _config.recordCount,
        includeRealm: _config.includeRealm,
      );
      
      final benchmarks = ['crud', 'bulk_insert', 'query', 'transaction', 'concurrent'];
      
      for (int i = 0; i < benchmarks.length; i++) {
        setState(() {
          _currentBenchmark = 'Running ${benchmarks[i]} benchmark...';
          _progress = i / benchmarks.length;
        });
        
        // Run individual benchmark
        final results = await suite.runAll(
          warmupIterations: _config.warmupIterations,
          iterations: _config.iterations,
        );
        
        setState(() {
          _results.addAll(results);
        });
        
        // Break after first full run to avoid duplicates
        if (i == benchmarks.length - 1) break;
      }
      
      setState(() {
        _currentBenchmark = 'Benchmarks completed!';
        _progress = 1.0;
      });
      
      // Show completion dialog
      if (mounted) {
        showDialog(
          context: context,
          builder: (context) => AlertDialog(
            title: const Text('Benchmarks Complete'),
            content: Text(
              'All benchmarks completed successfully.\n\n'
              'Lightning DB showed an average ${_calculateAverageSpeedup()}x '
              'performance improvement over SQLite.',
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.pop(context),
                child: const Text('OK'),
              ),
            ],
          ),
        );
      }
    } catch (e) {
      setState(() {
        _currentBenchmark = 'Error: $e';
      });
    } finally {
      setState(() {
        _isRunning = false;
      });
    }
  }

  String _calculateAverageSpeedup() {
    double totalSpeedup = 0;
    int count = 0;
    
    for (final comparison in _results.values) {
      final lightning = comparison.summaries.firstWhere(
        (s) => s.name.contains('Lightning'),
        orElse: () => comparison.summaries.first,
      );
      final sqlite = comparison.summaries.firstWhere(
        (s) => s.name.contains('SQLite'),
        orElse: () => comparison.summaries.first,
      );
      
      if (sqlite.meanMilliseconds > 0) {
        totalSpeedup += sqlite.meanMilliseconds / lightning.meanMilliseconds;
        count++;
      }
    }
    
    return count > 0 ? (totalSpeedup / count).toStringAsFixed(1) : 'N/A';
  }

  Future<void> _exportResults() async {
    // TODO: Implement export functionality
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(content: Text('Export functionality coming soon')),
    );
  }

  void _showConfigDialog() {
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Benchmark Configuration'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            _buildConfigOption(
              'Record Count',
              _config.recordCount.toString(),
              (value) {
                final count = int.tryParse(value);
                if (count != null && count > 0) {
                  setState(() {
                    _config = _config.copyWith(recordCount: count);
                  });
                }
              },
            ),
            _buildConfigOption(
              'Iterations',
              _config.iterations.toString(),
              (value) {
                final iterations = int.tryParse(value);
                if (iterations != null && iterations > 0) {
                  setState(() {
                    _config = _config.copyWith(iterations: iterations);
                  });
                }
              },
            ),
            _buildConfigOption(
              'Warmup Iterations',
              _config.warmupIterations.toString(),
              (value) {
                final warmup = int.tryParse(value);
                if (warmup != null && warmup >= 0) {
                  setState(() {
                    _config = _config.copyWith(warmupIterations: warmup);
                  });
                }
              },
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('Close'),
          ),
        ],
      ),
    );
  }

  Widget _buildConfigOption(String label, String value, ValueChanged<String> onChanged) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8),
      child: TextField(
        decoration: InputDecoration(
          labelText: label,
          border: const OutlineInputBorder(),
        ),
        controller: TextEditingController(text: value),
        keyboardType: TextInputType.number,
        onChanged: onChanged,
      ),
    );
  }
}