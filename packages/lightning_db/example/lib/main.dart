import 'package:flutter/material.dart';
import 'screens/todo_screen.dart';
import 'screens/query_builder_screen.dart';
import 'screens/error_handling_screen.dart';
import 'screens/benchmark_screen.dart';
import 'screens/migration_screen.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Lightning DB Demo',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.blue),
        useMaterial3: true,
      ),
      home: const HomePage(),
    );
  }
}

class HomePage extends StatelessWidget {
  const HomePage({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Lightning DB Demo'),
        backgroundColor: Theme.of(context).colorScheme.inversePrimary,
      ),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: [
          _buildDemoCard(
            context,
            'Todo App',
            'Basic CRUD operations with Freezed models',
            Icons.check_box,
            () => Navigator.push(
              context,
              MaterialPageRoute(builder: (_) => const TodoScreen()),
            ),
          ),
          const SizedBox(height: 16),
          _buildDemoCard(
            context,
            'Query Builder',
            'Advanced queries with filtering, sorting, and aggregations',
            Icons.search,
            () => Navigator.push(
              context,
              MaterialPageRoute(builder: (_) => const QueryBuilderScreen()),
            ),
          ),
          const SizedBox(height: 16),
          _buildDemoCard(
            context,
            'Error Handling',
            'Robust error handling and recovery strategies',
            Icons.error_outline,
            () => Navigator.push(
              context,
              MaterialPageRoute(builder: (_) => const ErrorHandlingScreen()),
            ),
          ),
          const SizedBox(height: 16),
          _buildDemoCard(
            context,
            'Performance Benchmarks',
            'Compare Lightning DB with SQLite and Realm',
            Icons.speed,
            () => Navigator.push(
              context,
              MaterialPageRoute(builder: (_) => const BenchmarkScreen()),
            ),
          ),
          const SizedBox(height: 16),
          _buildDemoCard(
            context,
            'Schema Migrations',
            'Database versioning and schema evolution',
            Icons.schema,
            () => Navigator.push(
              context,
              MaterialPageRoute(builder: (_) => const MigrationScreen()),
            ),
          ),
          const SizedBox(height: 32),
          Card(
            color: Colors.blue[50],
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'About Lightning DB',
                    style: Theme.of(context).textTheme.headlineSmall,
                  ),
                  const SizedBox(height: 8),
                  const Text(
                    'Lightning DB is a high-performance embedded database '
                    'written in Rust with Flutter bindings. It provides:',
                  ),
                  const SizedBox(height: 8),
                  _buildFeature('⚡ 20M+ ops/sec read performance'),
                  _buildFeature('🔒 ACID transactions with MVCC'),
                  _buildFeature('📦 Zero-config installation'),
                  _buildFeature('🧊 Freezed model support'),
                  _buildFeature('🔍 Advanced query builder'),
                  _buildFeature('💾 Automatic compression'),
                  _buildFeature('🚀 10x+ faster than SQLite'),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildDemoCard(
    BuildContext context,
    String title,
    String subtitle,
    IconData icon,
    VoidCallback onTap,
  ) {
    return Card(
      elevation: 2,
      child: ListTile(
        leading: CircleAvatar(
          backgroundColor: Theme.of(context).colorScheme.primary,
          child: Icon(icon, color: Colors.white),
        ),
        title: Text(
          title,
          style: const TextStyle(fontWeight: FontWeight.bold),
        ),
        subtitle: Text(subtitle),
        trailing: const Icon(Icons.arrow_forward_ios),
        onTap: onTap,
      ),
    );
  }

  Widget _buildFeature(String text) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 2),
      child: Text(text),
    );
  }
}