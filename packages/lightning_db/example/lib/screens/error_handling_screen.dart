import 'package:flutter/material.dart';
import 'package:lightning_db/lightning_db.dart';
import '../models/user_model.dart';

class ErrorHandlingScreen extends StatefulWidget {
  const ErrorHandlingScreen({super.key});

  @override
  State<ErrorHandlingScreen> createState() => _ErrorHandlingScreenState();
}

class _ErrorHandlingScreenState extends State<ErrorHandlingScreen> {
  late LightningDb _db;
  late FreezedCollection<User> _users;
  final List<String> _logs = [];
  bool _isRecovering = false;

  @override
  void initState() {
    super.initState();
    _initDatabase();
  }

  Future<void> _initDatabase() async {
    _db = await LightningDb.open('error_handling_demo.db');
    _users = _db.freezedCollection<User>('users');
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
        title: const Text('Error Handling Demo'),
      ),
      body: Column(
        children: [
          Expanded(
            child: SingleChildScrollView(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  _buildSection(
                    'Basic Error Handling',
                    [
                      _buildButton(
                        'Key Not Found',
                        _demonstrateKeyNotFound,
                      ),
                      _buildButton(
                        'Duplicate ID',
                        _demonstrateDuplicateId,
                      ),
                      _buildButton(
                        'Serialization Error',
                        _demonstrateSerializationError,
                      ),
                    ],
                  ),
                  const SizedBox(height: 16),
                  _buildSection(
                    'Retry Logic',
                    [
                      _buildButton(
                        'Transaction Conflict',
                        _demonstrateTransactionConflict,
                      ),
                      _buildButton(
                        'With Retry',
                        _demonstrateWithRetry,
                      ),
                      _buildButton(
                        'With Timeout',
                        _demonstrateWithTimeout,
                      ),
                    ],
                  ),
                  const SizedBox(height: 16),
                  _buildSection(
                    'Recovery',
                    [
                      _buildButton(
                        'Check Database Health',
                        _checkDatabaseHealth,
                      ),
                      _buildButton(
                        'Simulate Corruption',
                        _simulateCorruption,
                      ),
                      _buildButton(
                        'Recover Database',
                        _recoverDatabase,
                        enabled: !_isRecovering,
                      ),
                    ],
                  ),
                  const SizedBox(height: 16),
                  _buildSection(
                    'Error Recovery Patterns',
                    [
                      _buildButton(
                        'Handle Duplicate ID',
                        _demonstrateHandleDuplicate,
                      ),
                      _buildButton(
                        'Concurrent Modification',
                        _demonstrateConcurrentModification,
                      ),
                      _buildButton(
                        'Fallback Strategy',
                        _demonstrateFallback,
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ),
          Container(
            height: 200,
            decoration: BoxDecoration(
              color: Colors.grey[900],
              border: Border(
                top: BorderSide(color: Colors.grey[700]!),
              ),
            ),
            child: Column(
              children: [
                Container(
                  padding: const EdgeInsets.all(8),
                  child: Row(
                    children: [
                      const Text(
                        'Logs',
                        style: TextStyle(
                          color: Colors.white,
                          fontWeight: FontWeight.bold,
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
                ),
                Expanded(
                  child: ListView.builder(
                    padding: const EdgeInsets.all(8),
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
        ],
      ),
    );
  }

  Widget _buildSection(String title, List<Widget> children) {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              title,
              style: Theme.of(context).textTheme.titleMedium,
            ),
            const SizedBox(height: 16),
            ...children,
          ],
        ),
      ),
    );
  }

  Widget _buildButton(
    String label,
    VoidCallback onPressed, {
    bool enabled = true,
  }) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 8),
      child: ElevatedButton(
        onPressed: enabled ? onPressed : null,
        child: Text(label),
      ),
    );
  }

  Future<void> _demonstrateKeyNotFound() async {
    _log('Attempting to get non-existent key...');
    
    try {
      await _users.get('non_existent_id');
      _log('❌ Should have thrown KeyNotFoundException');
    } on KeyNotFoundException catch (e) {
      _log('✅ Caught KeyNotFoundException: ${e.message}');
      _log('   User message: ${e.userMessage}');
    }
  }

  Future<void> _demonstrateDuplicateId() async {
    _log('Attempting to insert duplicate ID...');
    
    final user = User(
      id: 'duplicate_test',
      name: 'Test User',
      email: 'test@example.com',
      createdAt: DateTime.now(),
    );
    
    try {
      await _users.add(user);
      _log('First insert successful');
      
      await _users.add(user);
      _log('❌ Should have thrown DuplicateIdException');
    } on DuplicateIdException catch (e) {
      _log('✅ Caught DuplicateIdException: ${e.message}');
      _log('   Collection: ${e.collectionName}');
      _log('   ID: ${e.id}');
    }
  }

  Future<void> _demonstrateSerializationError() async {
    _log('Simulating serialization error...');
    
    // This would typically happen with invalid data
    try {
      // Force a serialization error by creating invalid metadata
      final user = User(
        id: 'serialization_test',
        name: 'Test User',
        email: 'test@example.com',
        createdAt: DateTime.now(),
        metadata: {
          'invalid_value': double.infinity, // This might cause issues
        },
      );
      
      await _users.add(user);
      _log('✅ Serialization successful');
    } on SerializationException catch (e) {
      _log('✅ Caught SerializationException: ${e.message}');
      _log('   Model type: ${e.modelType}');
      _log('   Field: ${e.fieldName ?? 'unknown'}');
    } catch (e) {
      _log('⚠️ Unexpected error: $e');
    }
  }

  Future<void> _demonstrateTransactionConflict() async {
    _log('Simulating transaction conflict...');
    
    final user = User(
      id: 'tx_conflict_test',
      name: 'TX Test',
      email: 'tx@example.com',
      createdAt: DateTime.now(),
    );
    
    await _users.upsert(user);
    
    // Simulate concurrent modifications
    final futures = List.generate(5, (i) async {
      try {
        await _db.transaction((tx) async {
          final current = await _users.get(user.id);
          if (current != null) {
            await _users.update(current.copyWith(
              name: 'Updated by TX $i',
              metadata: {'version': i},
            ));
          }
        });
        _log('✅ Transaction $i succeeded');
      } on TransactionConflictException catch (e) {
        _log('⚠️ Transaction $i conflict: ${e.message}');
      }
    });
    
    await Future.wait(futures);
  }

  Future<void> _demonstrateWithRetry() async {
    _log('Demonstrating automatic retry...');
    
    var attemptCount = 0;
    
    try {
      await _users.add(
        User(
          id: 'retry_test_${DateTime.now().millisecondsSinceEpoch}',
          name: 'Retry Test',
          email: 'retry@example.com',
          createdAt: DateTime.now(),
        ),
      ).withRetry(
        maxAttempts: 3,
        delay: const Duration(milliseconds: 500),
      );
      
      _log('✅ Operation succeeded after $attemptCount attempts');
    } on LightningDbException catch (e) {
      _log('❌ Operation failed after retries: ${e.message}');
    }
  }

  Future<void> _demonstrateWithTimeout() async {
    _log('Demonstrating operation timeout...');
    
    try {
      await Future.delayed(const Duration(seconds: 2))
          .withDbTimeout(
        const Duration(seconds: 1),
        'Custom timeout message',
      );
      
      _log('❌ Should have timed out');
    } on LightningDbException catch (e) {
      _log('✅ Operation timed out: ${e.message}');
      _log('   Error type: ${e.errorType}');
    }
  }

  Future<void> _checkDatabaseHealth() async {
    _log('Checking database health...');
    
    final status = await DatabaseRecovery.checkDatabase(_db.path);
    
    _log('Database status:');
    _log('  Corrupted: ${status.isCorrupted}');
    _log('  Recoverable: ${status.canRecover}');
    _log('  Message: ${status.message}');
  }

  Future<void> _simulateCorruption() async {
    _log('⚠️ This would simulate database corruption');
    _log('In a real scenario, this is dangerous!');
    
    // In practice, you wouldn't want to actually corrupt your database
    // This is just for demonstration
  }

  Future<void> _recoverDatabase() async {
    setState(() {
      _isRecovering = true;
    });
    
    _log('Starting database recovery...');
    
    try {
      final result = await DatabaseRecovery.recoverDatabase(
        _db.path,
        options: const RecoveryOptions(
          createBackup: true,
          allowDataLoss: false,
          verifyAfterRecovery: true,
        ),
        onProgress: (progress) {
          _log('Recovery progress: ${progress.stage} (${progress.percentComplete}%)');
        },
      );
      
      _log('Recovery result:');
      _log('  Success: ${result.success}');
      _log('  Message: ${result.message}');
      _log('  Recovered records: ${result.recoveredRecords}');
      _log('  Lost records: ${result.lostRecords}');
      if (result.backupPath != null) {
        _log('  Backup saved to: ${result.backupPath}');
      }
    } finally {
      setState(() {
        _isRecovering = false;
      });
    }
  }

  Future<void> _demonstrateHandleDuplicate() async {
    _log('Demonstrating duplicate ID handling...');
    
    final user = User(
      id: 'handle_duplicate_test',
      name: 'Original User',
      email: 'original@example.com',
      createdAt: DateTime.now(),
    );
    
    // First insert
    await _users.add(user);
    _log('First insert successful');
    
    // Try to insert again with handling
    final result = await _users
        .add(user.copyWith(name: 'Duplicate User'))
        .handleDuplicateId((existingId) async {
      _log('⚠️ Duplicate detected for ID: $existingId');
      _log('   Using update instead...');
      
      await _users.update(user.copyWith(
        name: 'Updated User',
        metadata: {'updated': true},
      ));
      
      return null;
    });
    
    _log('✅ Handled duplicate ID successfully');
  }

  Future<void> _demonstrateConcurrentModification() async {
    _log('Demonstrating concurrent modification handling...');
    
    final user = User(
      id: 'concurrent_test',
      name: 'Concurrent Test',
      email: 'concurrent@example.com',
      createdAt: DateTime.now(),
      metadata: {'version': 1},
    );
    
    await _users.upsert(user);
    
    // Simulate concurrent modification
    await _users
        .update(user.copyWith(
      name: 'Modified User',
      metadata: {'version': 2},
    ))
        .handleConcurrentModification(
      retry: () async {
        _log('⚠️ Concurrent modification detected, retrying...');
        
        // Get latest version
        final latest = await _users.get(user.id);
        if (latest != null) {
          final version = (latest.metadata?['version'] ?? 0) as int;
          return _users.update(latest.copyWith(
            name: 'Modified User (Retry)',
            metadata: {'version': version + 1},
          ));
        }
      },
    );
    
    _log('✅ Handled concurrent modification successfully');
  }

  Future<void> _demonstrateFallback() async {
    _log('Demonstrating fallback strategy...');
    
    try {
      // Try primary operation
      await _db
          .get('fallback_test')
          .withDbTimeout(const Duration(milliseconds: 100))
          .recoverFrom<LightningDbException>((error) async {
        _log('⚠️ Primary operation failed: ${error.message}');
        _log('   Trying fallback...');
        
        // Fallback to default value
        return Uint8List.fromList('fallback_value'.codeUnits);
      });
      
      _log('✅ Fallback strategy executed successfully');
    } catch (e) {
      _log('❌ Fallback failed: $e');
    }
  }

  @override
  void dispose() {
    _db.close();
    super.dispose();
  }
}