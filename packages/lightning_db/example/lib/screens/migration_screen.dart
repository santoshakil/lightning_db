import 'package:flutter/material.dart';
import 'package:lightning_db/lightning_db.dart';
import '../models/user_model.dart';

class MigrationScreen extends StatefulWidget {
  const MigrationScreen({super.key});

  @override
  State<MigrationScreen> createState() => _MigrationScreenState();
}

class _MigrationScreenState extends State<MigrationScreen> {
  late LightningDb _db;
  late MigrationManager _migrationManager;
  int _currentVersion = 0;
  final List<String> _logs = [];
  bool _isRunning = false;

  // Define migrations
  final List<Migration> _migrations = [
    // V0 -> V1: Initial schema
    _InitialSchemaMigration(),
    
    // V1 -> V2: Add metadata field
    _AddMetadataFieldMigration(),
    
    // V2 -> V3: Rename field
    _RenameFieldMigration(),
    
    // V3 -> V4: Create index
    _CreateIndexMigration(),
    
    // V4 -> V5: Transform data
    _TransformDataMigration(),
  ];

  @override
  void initState() {
    super.initState();
    _initDatabase();
  }

  Future<void> _initDatabase() async {
    _db = await LightningDb.open('migration_demo.db');
    _migrationManager = MigrationManager(_db);
    await _loadCurrentVersion();
  }

  Future<void> _loadCurrentVersion() async {
    final version = await _migrationManager.getCurrentVersion();
    setState(() {
      _currentVersion = version;
    });
    _log('Current schema version: $version');
  }

  void _log(String message) {
    setState(() {
      _logs.add('[${DateTime.now().toIso8601String()}] $message');
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Migration Demo'),
        actions: [
          Chip(
            label: Text('Version: $_currentVersion'),
            backgroundColor: Theme.of(context).colorScheme.primaryContainer,
          ),
          const SizedBox(width: 16),
        ],
      ),
      body: Column(
        children: [
          // Migration controls
          Card(
            margin: const EdgeInsets.all(16),
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'Database Migrations',
                    style: Theme.of(context).textTheme.titleLarge,
                  ),
                  const SizedBox(height: 16),
                  Text(
                    'Current Version: $_currentVersion',
                    style: Theme.of(context).textTheme.titleMedium,
                  ),
                  Text(
                    'Latest Version: ${_migrations.length}',
                    style: Theme.of(context).textTheme.titleMedium,
                  ),
                  const SizedBox(height: 16),
                  Row(
                    children: [
                      ElevatedButton.icon(
                        onPressed: _isRunning || _currentVersion >= _migrations.length
                            ? null
                            : _runMigrations,
                        icon: const Icon(Icons.upgrade),
                        label: const Text('Run Migrations'),
                      ),
                      const SizedBox(width: 16),
                      ElevatedButton.icon(
                        onPressed: _isRunning || _currentVersion == 0
                            ? null
                            : _rollbackMigration,
                        icon: const Icon(Icons.undo),
                        label: const Text('Rollback'),
                        style: ElevatedButton.styleFrom(
                          backgroundColor: Colors.orange,
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ),
          
          // Migration list
          Expanded(
            flex: 1,
            child: Card(
              margin: const EdgeInsets.symmetric(horizontal: 16),
              child: ListView.builder(
                padding: const EdgeInsets.all(16),
                itemCount: _migrations.length,
                itemBuilder: (context, index) {
                  final migration = _migrations[index];
                  final isApplied = migration.version <= _currentVersion;
                  
                  return ListTile(
                    leading: CircleAvatar(
                      backgroundColor: isApplied
                          ? Colors.green
                          : Colors.grey[300],
                      child: Text(
                        migration.version.toString(),
                        style: TextStyle(
                          color: isApplied ? Colors.white : Colors.black,
                        ),
                      ),
                    ),
                    title: Text('Version ${migration.version}'),
                    subtitle: Text(migration.description),
                    trailing: isApplied
                        ? const Icon(Icons.check_circle, color: Colors.green)
                        : const Icon(Icons.circle_outlined),
                  );
                },
              ),
            ),
          ),
          
          // Logs
          Expanded(
            flex: 1,
            child: Container(
              margin: const EdgeInsets.all(16),
              padding: const EdgeInsets.all(16),
              decoration: BoxDecoration(
                color: Colors.grey[900],
                borderRadius: BorderRadius.circular(8),
              ),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    children: [
                      Text(
                        'Migration Logs',
                        style: Theme.of(context).textTheme.titleMedium?.copyWith(
                          color: Colors.white,
                        ),
                      ),
                      const Spacer(),
                      TextButton(
                        onPressed: () {
                          setState(() {
                            _logs.clear();
                          });
                        },
                        child: const Text('Clear'),
                      ),
                    ],
                  ),
                  const SizedBox(height: 8),
                  Expanded(
                    child: ListView.builder(
                      itemCount: _logs.length,
                      itemBuilder: (context, index) {
                        return Text(
                          _logs[index],
                          style: const TextStyle(
                            color: Colors.green,
                            fontFamily: 'monospace',
                            fontSize: 12,
                          ),
                        );
                      },
                    ),
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Future<void> _runMigrations() async {
    setState(() {
      _isRunning = true;
    });

    try {
      _log('Starting migrations...');
      
      final pendingMigrations = _migrations
          .where((m) => m.version > _currentVersion)
          .toList();
      
      for (final migration in pendingMigrations) {
        _log('Running migration ${migration.version}: ${migration.description}');
        
        await _migrationManager.migrate([migration]);
        
        _log('✅ Migration ${migration.version} completed');
      }
      
      await _loadCurrentVersion();
      _log('All migrations completed successfully!');
      
      _showSnackBar('Migrations completed successfully', Colors.green);
    } catch (e) {
      _log('❌ Migration failed: $e');
      _showSnackBar('Migration failed: $e', Colors.red);
    } finally {
      setState(() {
        _isRunning = false;
      });
    }
  }

  Future<void> _rollbackMigration() async {
    setState(() {
      _isRunning = true;
    });

    try {
      _log('Starting rollback...');
      
      final targetVersion = _currentVersion - 1;
      await _migrationManager.rollback(targetVersion, _migrations);
      
      _log('✅ Rolled back to version $targetVersion');
      await _loadCurrentVersion();
      
      _showSnackBar('Rollback completed successfully', Colors.orange);
    } catch (e) {
      _log('❌ Rollback failed: $e');
      _showSnackBar('Rollback failed: $e', Colors.red);
    } finally {
      setState(() {
        _isRunning = false;
      });
    }
  }

  void _showSnackBar(String message, Color color) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(message),
        backgroundColor: color,
      ),
    );
  }

  @override
  void dispose() {
    _db.close();
    super.dispose();
  }
}

// Migration implementations

class _InitialSchemaMigration extends Migration {
  _InitialSchemaMigration() : super(
    version: 1,
    description: 'Create initial schema',
  );

  @override
  Future<void> up(MigrationContext context) async {
    // Create collections metadata
    await context.db.putJson('_collection:users', {
      'version': 1,
      'createdAt': DateTime.now().toIso8601String(),
    });
    
    await context.db.putJson('_collection:posts', {
      'version': 1,
      'createdAt': DateTime.now().toIso8601String(),
    });
    
    // Add sample data
    await context.db.putJson('users:user_1', {
      'id': 'user_1',
      'name': 'John Doe',
      'email': 'john@example.com',
      'age': 30,
      'createdAt': DateTime.now().toIso8601String(),
    });
  }

  @override
  Future<void> down(MigrationContext context) async {
    await context.db.delete('_collection:users');
    await context.db.delete('_collection:posts');
    await context.db.delete('users:user_1');
  }
}

class _AddMetadataFieldMigration extends Migration {
  _AddMetadataFieldMigration() : super(
    version: 2,
    description: 'Add metadata field to users',
  );

  @override
  Future<void> up(MigrationContext context) async {
    await context.addField('users', 'metadata', (doc) => {
      'migrated': true,
      'migratedAt': DateTime.now().toIso8601String(),
    });
  }

  @override
  Future<void> down(MigrationContext context) async {
    await context.removeField('users', 'metadata');
  }
}

class _RenameFieldMigration extends Migration {
  _RenameFieldMigration() : super(
    version: 3,
    description: 'Rename name to displayName',
  );

  @override
  Future<void> up(MigrationContext context) async {
    await context.renameField('users', 'name', 'displayName');
  }

  @override
  Future<void> down(MigrationContext context) async {
    await context.renameField('users', 'displayName', 'name');
  }
}

class _CreateIndexMigration extends Migration {
  _CreateIndexMigration() : super(
    version: 4,
    description: 'Create email index',
  );

  @override
  Future<void> up(MigrationContext context) async {
    await context.createIndex('users', 'email', unique: true);
  }

  @override
  Future<void> down(MigrationContext context) async {
    await context.dropIndex('users', 'email');
  }
}

class _TransformDataMigration extends Migration {
  _TransformDataMigration() : super(
    version: 5,
    description: 'Transform user data structure',
  );

  @override
  Future<void> up(MigrationContext context) async {
    await context.transformCollection('users', (doc) {
      // Add computed fields
      doc['fullName'] = doc['displayName'] ?? doc['name'] ?? 'Unknown';
      doc['isAdult'] = (doc['age'] ?? 0) >= 18;
      
      // Normalize email
      if (doc['email'] != null) {
        doc['email'] = doc['email'].toString().toLowerCase();
      }
      
      // Add version tracking
      doc['schemaVersion'] = 5;
      
      return doc;
    });
  }

  @override
  Future<void> down(MigrationContext context) async {
    await context.transformCollection('users', (doc) {
      // Remove added fields
      doc.remove('fullName');
      doc.remove('isAdult');
      doc.remove('schemaVersion');
      
      return doc;
    });
  }
}