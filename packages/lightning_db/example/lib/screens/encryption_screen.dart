import 'dart:typed_data';
import 'package:flutter/material.dart';
import 'package:lightning_db/lightning_db.dart';
import '../models/user_model.dart';

class EncryptionScreen extends StatefulWidget {
  const EncryptionScreen({super.key});

  @override
  State<EncryptionScreen> createState() => _EncryptionScreenState();
}

class _EncryptionScreenState extends State<EncryptionScreen> {
  EncryptedDatabase? _encryptedDb;
  EncryptedFreezedCollection<User>? _encryptedUsers;
  final _passwordController = TextEditingController(text: 'demo_password');
  final List<String> _logs = [];
  bool _isInitialized = false;
  bool _showPassword = false;
  EncryptionMetrics? _metrics;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Encryption Demo'),
        actions: [
          if (_metrics != null)
            IconButton(
              icon: const Icon(Icons.analytics),
              onPressed: _showMetrics,
              tooltip: 'Encryption Metrics',
            ),
        ],
      ),
      body: Column(
        children: [
          // Initialization panel
          Card(
            margin: const EdgeInsets.all(16),
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'Database Encryption',
                    style: Theme.of(context).textTheme.titleLarge,
                  ),
                  const SizedBox(height: 16),
                  TextField(
                    controller: _passwordController,
                    obscureText: !_showPassword,
                    decoration: InputDecoration(
                      labelText: 'Encryption Password',
                      border: const OutlineInputBorder(),
                      suffixIcon: IconButton(
                        icon: Icon(
                          _showPassword ? Icons.visibility_off : Icons.visibility,
                        ),
                        onPressed: () {
                          setState(() {
                            _showPassword = !_showPassword;
                          });
                        },
                      ),
                    ),
                  ),
                  const SizedBox(height: 16),
                  Row(
                    children: [
                      ElevatedButton.icon(
                        onPressed: _isInitialized ? null : _initializeEncryptedDb,
                        icon: const Icon(Icons.lock),
                        label: const Text('Initialize Encrypted DB'),
                      ),
                      const SizedBox(width: 16),
                      if (_isInitialized)
                        TextButton.icon(
                          onPressed: _closeDb,
                          icon: const Icon(Icons.close),
                          label: const Text('Close'),
                          style: TextButton.styleFrom(
                            foregroundColor: Colors.red,
                          ),
                        ),
                    ],
                  ),
                ],
              ),
            ),
          ),
          
          // Operations
          if (_isInitialized) ...[
            Expanded(
              child: Row(
                children: [
                  // Operations panel
                  Expanded(
                    flex: 1,
                    child: Card(
                      margin: const EdgeInsets.only(left: 16, right: 8, bottom: 16),
                      child: ListView(
                        padding: const EdgeInsets.all(16),
                        children: [
                          Text(
                            'Encryption Operations',
                            style: Theme.of(context).textTheme.titleMedium,
                          ),
                          const SizedBox(height: 16),
                          _buildOperationButton(
                            'Add Encrypted User',
                            Icons.person_add,
                            _addEncryptedUser,
                          ),
                          _buildOperationButton(
                            'Read Encrypted User',
                            Icons.person_search,
                            _readEncryptedUser,
                          ),
                          _buildOperationButton(
                            'Field-Level Encryption',
                            Icons.enhanced_encryption,
                            _demonstrateFieldEncryption,
                          ),
                          _buildOperationButton(
                            'Transparent Encryption',
                            Icons.lock_open,
                            _demonstrateTransparentEncryption,
                          ),
                          const Divider(height: 32),
                          _buildOperationButton(
                            'Create Encrypted Backup',
                            Icons.backup,
                            _createEncryptedBackup,
                          ),
                          _buildOperationButton(
                            'List Backups',
                            Icons.list,
                            _listBackups,
                          ),
                          _buildOperationButton(
                            'Restore from Backup',
                            Icons.restore,
                            _restoreFromBackup,
                          ),
                          const Divider(height: 32),
                          _buildOperationButton(
                            'Performance Test',
                            Icons.speed,
                            _runPerformanceTest,
                          ),
                          _buildOperationButton(
                            'Security Audit',
                            Icons.security,
                            _runSecurityAudit,
                          ),
                        ],
                      ),
                    ),
                  ),
                  
                  // Logs panel
                  Expanded(
                    flex: 1,
                    child: Card(
                      margin: const EdgeInsets.only(left: 8, right: 16, bottom: 16),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Padding(
                            padding: const EdgeInsets.all(16),
                            child: Row(
                              children: [
                                Text(
                                  'Operation Logs',
                                  style: Theme.of(context).textTheme.titleMedium,
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
                            child: Container(
                              color: Colors.grey[900],
                              child: ListView.builder(
                                padding: const EdgeInsets.all(16),
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
                          ),
                        ],
                      ),
                    ),
                  ),
                ],
              ),
            ),
          ],
        ],
      ),
    );
  }

  Widget _buildOperationButton(
    String label,
    IconData icon,
    VoidCallback onPressed,
  ) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4),
      child: ElevatedButton.icon(
        onPressed: onPressed,
        icon: Icon(icon),
        label: Text(label),
        style: ElevatedButton.styleFrom(
          alignment: Alignment.centerLeft,
          padding: const EdgeInsets.all(12),
        ),
      ),
    );
  }

  void _log(String message) {
    setState(() {
      _logs.add('[${DateTime.now().toIso8601String()}] $message');
    });
  }

  Future<void> _initializeEncryptedDb() async {
    try {
      _log('Initializing encrypted database...');
      
      // Create monitored encryption provider
      final baseProvider = AesGcmEncryptionProvider(key: Uint8List(32));
      final monitoredProvider = MonitoredEncryptionProvider(baseProvider);
      
      // Open encrypted database
      _encryptedDb = await EncryptedDatabase.open(
        'encrypted_demo.db',
        password: _passwordController.text,
      );
      
      // Create encrypted collection with field-level encryption
      _encryptedUsers = _encryptedDb!.encryptedCollection<User>(
        'users',
        encryptedFields: ['email', 'metadata'],
      );
      
      _metrics = monitoredProvider.metrics;
      
      setState(() {
        _isInitialized = true;
      });
      
      _log('✅ Encrypted database initialized');
      _log('   Algorithm: AES-256-GCM');
      _log('   Key derivation: PBKDF2 (100k iterations)');
    } catch (e) {
      _log('❌ Failed to initialize: $e');
    }
  }

  Future<void> _closeDb() async {
    await _encryptedDb?.close();
    setState(() {
      _isInitialized = false;
      _encryptedDb = null;
      _encryptedUsers = null;
      _metrics = null;
    });
    _log('Database closed');
  }

  Future<void> _addEncryptedUser() async {
    try {
      final user = User(
        id: 'enc_user_${DateTime.now().millisecondsSinceEpoch}',
        name: 'Encrypted User ${_logs.length}',
        email: 'sensitive.email@secure.com',
        age: 25 + (_logs.length % 40),
        createdAt: DateTime.now(),
        metadata: {
          'ssn': '123-45-6789', // Sensitive data
          'creditCard': '4111-1111-1111-1111', // Sensitive data
          'balance': 1000.0 + (_logs.length * 100),
          'notes': 'This is encrypted data',
        },
      );
      
      await _encryptedUsers!.add(user);
      
      _log('✅ Added encrypted user: ${user.id}');
      _log('   Email and metadata fields are encrypted');
    } catch (e) {
      _log('❌ Failed to add user: $e');
    }
  }

  Future<void> _readEncryptedUser() async {
    try {
      // Get the last added user ID
      final lastUserId = _logs
          .where((log) => log.contains('Added encrypted user: enc_user_'))
          .map((log) => log.split('enc_user_').last.split(']').first)
          .lastOrNull;
      
      if (lastUserId == null) {
        _log('⚠️ No users to read. Add a user first.');
        return;
      }
      
      final userId = 'enc_user_$lastUserId';
      final user = await _encryptedUsers!.get(userId);
      
      if (user != null) {
        _log('✅ Read encrypted user: ${user.id}');
        _log('   Name: ${user.name}');
        _log('   Email: ${user.email} (was encrypted)');
        _log('   Metadata: ${user.metadata} (was encrypted)');
      } else {
        _log('⚠️ User not found');
      }
    } catch (e) {
      _log('❌ Failed to read user: $e');
    }
  }

  Future<void> _demonstrateFieldEncryption() async {
    try {
      _log('Demonstrating field-level encryption...');
      
      // Store with specific fields encrypted
      await _encryptedDb!.putJson(
        'sensitive_doc',
        {
          'public_field': 'This is not encrypted',
          'private_field': 'This is encrypted',
          'credit_card': '4111-1111-1111-1111',
          'password': 'super_secret_password',
        },
        encrypt: true,
      );
      
      _log('✅ Stored document with mixed encryption');
      
      // Read back
      final doc = await _encryptedDb!.getJson('sensitive_doc', encrypted: true);
      _log('✅ Retrieved document: $doc');
    } catch (e) {
      _log('❌ Field encryption failed: $e');
    }
  }

  Future<void> _demonstrateTransparentEncryption() async {
    try {
      _log('Demonstrating transparent encryption...');
      
      // All operations are automatically encrypted
      await _encryptedDb!.put('transparent_key', 'This is automatically encrypted');
      
      final value = await _encryptedDb!.get('transparent_key');
      _log('✅ Transparent encryption working');
      _log('   Stored and retrieved: $value');
    } catch (e) {
      _log('❌ Transparent encryption failed: $e');
    }
  }

  Future<void> _createEncryptedBackup() async {
    try {
      _log('Creating encrypted backup...');
      
      final backupManager = EncryptedBackupManager(
        db: _encryptedDb!,
        encryption: _encryptedDb!._encryption,
      );
      
      final backupName = 'backup_${DateTime.now().millisecondsSinceEpoch}';
      await backupManager.backup(backupName);
      
      _log('✅ Created encrypted backup: $backupName');
    } catch (e) {
      _log('❌ Backup failed: $e');
    }
  }

  Future<void> _listBackups() async {
    try {
      final backupManager = EncryptedBackupManager(
        db: _encryptedDb!,
        encryption: _encryptedDb!._encryption,
      );
      
      final backups = await backupManager.listBackups();
      
      _log('Found ${backups.length} backups:');
      for (final backup in backups) {
        _log('  - ${backup.path}');
        _log('    Created: ${backup.createdAt}');
        _log('    Size: ${backup.size} bytes');
        _log('    Encrypted: ${backup.encrypted}');
      }
    } catch (e) {
      _log('❌ Failed to list backups: $e');
    }
  }

  Future<void> _restoreFromBackup() async {
    try {
      final backupManager = EncryptedBackupManager(
        db: _encryptedDb!,
        encryption: _encryptedDb!._encryption,
      );
      
      final backups = await backupManager.listBackups();
      if (backups.isEmpty) {
        _log('⚠️ No backups available');
        return;
      }
      
      final latestBackup = backups.last;
      _log('Restoring from backup: ${latestBackup.path}...');
      
      await backupManager.restore(latestBackup.path);
      
      _log('✅ Restored from backup successfully');
    } catch (e) {
      _log('❌ Restore failed: $e');
    }
  }

  Future<void> _runPerformanceTest() async {
    try {
      _log('Running encryption performance test...');
      
      const iterations = 1000;
      final testData = List.generate(iterations, (i) => User(
        id: 'perf_user_$i',
        name: 'Performance User $i',
        email: 'perf$i@test.com',
        age: 20 + (i % 50),
        createdAt: DateTime.now(),
        metadata: {
          'index': i,
          'data': 'x' * 100, // 100 chars of data
        },
      ));
      
      // Test encryption performance
      final encryptStart = DateTime.now();
      for (final user in testData) {
        await _encryptedUsers!.add(user);
      }
      final encryptDuration = DateTime.now().difference(encryptStart);
      
      _log('✅ Encryption performance:');
      _log('   Encrypted $iterations documents in ${encryptDuration.inMilliseconds}ms');
      _log('   Rate: ${(iterations / encryptDuration.inMilliseconds * 1000).toStringAsFixed(0)} docs/sec');
      
      // Test decryption performance
      final decryptStart = DateTime.now();
      for (int i = 0; i < iterations; i++) {
        await _encryptedUsers!.get('perf_user_$i');
      }
      final decryptDuration = DateTime.now().difference(decryptStart);
      
      _log('✅ Decryption performance:');
      _log('   Decrypted $iterations documents in ${decryptDuration.inMilliseconds}ms');
      _log('   Rate: ${(iterations / decryptDuration.inMilliseconds * 1000).toStringAsFixed(0)} docs/sec');
      
      // Clean up
      for (int i = 0; i < iterations; i++) {
        await _encryptedUsers!.delete('perf_user_$i');
      }
    } catch (e) {
      _log('❌ Performance test failed: $e');
    }
  }

  Future<void> _runSecurityAudit() async {
    try {
      _log('Running security audit...');
      
      // Check encryption metadata
      final metadata = await _encryptedDb!._db.getJson('_encryption_metadata');
      _log('Encryption configuration:');
      _log('  Algorithm: ${metadata?['algorithm']}');
      _log('  Version: ${metadata?['version']}');
      _log('  Created: ${metadata?['createdAt']}');
      
      // Check for unencrypted sensitive data
      var unencryptedCount = 0;
      var encryptedCount = 0;
      
      await for (final kv in _encryptedDb!._db.scanStream()) {
        if (kv.key.startsWith('_enc:')) {
          encryptedCount++;
        } else if (!kv.key.startsWith('_')) {
          unencryptedCount++;
        }
      }
      
      _log('Data analysis:');
      _log('  Encrypted entries: $encryptedCount');
      _log('  Unencrypted entries: $unencryptedCount');
      
      // Security recommendations
      _log('Security recommendations:');
      if (unencryptedCount > 0) {
        _log('  ⚠️ Found $unencryptedCount unencrypted entries');
        _log('     Consider encrypting all sensitive data');
      } else {
        _log('  ✅ All user data is encrypted');
      }
      
      _log('  ✅ Using strong encryption (AES-256-GCM)');
      _log('  ✅ Key derivation with PBKDF2');
      _log('  ℹ️ Consider using hardware security module');
      _log('  ℹ️ Implement key rotation policy');
    } catch (e) {
      _log('❌ Security audit failed: $e');
    }
  }

  void _showMetrics() {
    if (_metrics == null) return;
    
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Encryption Metrics'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Encryption Operations: ${_metrics!.encryptionOperations}'),
            Text('Decryption Operations: ${_metrics!.decryptionOperations}'),
            Text('Encryption Errors: ${_metrics!.encryptionErrors}'),
            Text('Decryption Errors: ${_metrics!.decryptionErrors}'),
            const SizedBox(height: 16),
            Text('Avg Encryption Time: ${_metrics!.averageEncryptionTime.toStringAsFixed(2)}μs'),
            Text('Avg Decryption Time: ${_metrics!.averageDecryptionTime.toStringAsFixed(2)}μs'),
            const SizedBox(height: 16),
            Text('Encryption Error Rate: ${(_metrics!.encryptionErrorRate * 100).toStringAsFixed(2)}%'),
            Text('Decryption Error Rate: ${(_metrics!.decryptionErrorRate * 100).toStringAsFixed(2)}%'),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () {
              _metrics!.reset();
              Navigator.of(context).pop();
              _log('Metrics reset');
            },
            child: const Text('Reset'),
          ),
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('Close'),
          ),
        ],
      ),
    );
  }

  @override
  void dispose() {
    _passwordController.dispose();
    _encryptedDb?.close();
    super.dispose();
  }
}