# Lightning DB Encryption Guide

## Overview

Lightning DB provides comprehensive encryption capabilities to protect sensitive data at rest. The encryption system offers:

- **Database-level encryption**: Entire database encryption with password/key
- **Field-level encryption**: Selective encryption of sensitive fields
- **Transparent encryption**: Automatic encryption/decryption operations
- **Performance monitoring**: Built-in metrics for encryption operations
- **Secure key management**: Integration with platform keychains
- **Encrypted backups**: Secure backup and restore functionality

## Quick Start

### Basic Database Encryption

```dart
// Open an encrypted database
final encryptedDb = await EncryptedDatabase.open(
  'secure.db',
  password: 'your_secure_password',
);

// Use like a normal database
await encryptedDb.put('secret_key', 'sensitive_data');
final data = await encryptedDb.get('secret_key');

// Close when done
await encryptedDb.close();
```

### Field-Level Encryption

```dart
// Create collection with specific fields encrypted
final users = encryptedDb.encryptedCollection<User>(
  'users',
  encryptedFields: ['email', 'ssn', 'creditCard'],
);

// Add user - specified fields are automatically encrypted
await users.add(User(
  id: 'user_1',
  name: 'John Doe',        // Not encrypted
  email: 'john@email.com', // Encrypted
  ssn: '123-45-6789',      // Encrypted
));

// Get user - fields are automatically decrypted
final user = await users.get('user_1');
```

## Encryption Algorithms

### AES-256-GCM (Default)

```dart
// Create AES-256-GCM provider
final provider = AesGcmEncryptionProvider(key: encryptionKey);

// Or derive key from password
final salt = provider.generateSalt();
final key = await provider.deriveKey('password', salt);
```

### Custom Encryption Provider

```dart
class CustomEncryptionProvider implements EncryptionProvider {
  @override
  Future<Uint8List> encrypt(Uint8List data) async {
    // Implement your encryption logic
  }
  
  @override
  Future<Uint8List> decrypt(Uint8List data) async {
    // Implement your decryption logic
  }
  
  // Implement other required methods...
}
```

## Advanced Features

### Monitored Encryption

Track encryption performance and errors:

```dart
final baseProvider = AesGcmEncryptionProvider(key: key);
final monitoredProvider = MonitoredEncryptionProvider(baseProvider);

// Use monitored provider
final db = EncryptedDatabase(
  db: rawDb,
  encryption: monitoredProvider,
);

// Check metrics
final metrics = monitoredProvider.metrics;
print('Encryption operations: ${metrics.encryptionOperations}');
print('Average time: ${metrics.averageEncryptionTime}μs');
print('Error rate: ${metrics.encryptionErrorRate}%');
```

### Envelope Encryption

For large datasets or key rotation:

```dart
final keyEncryption = AesGcmEncryptionProvider(key: masterKey);
final envelopeEncryption = EnvelopeEncryption(
  keyEncryption: keyEncryption,
);

// Encrypt large data with generated data key
final enveloped = await envelopeEncryption.encrypt(largeData);

// Decrypt
final decrypted = await envelopeEncryption.decrypt(enveloped);
```

### Field-Level Transformation

Custom field encryption logic:

```dart
final transformer = FieldEncryptionTransformer(
  encryption: provider,
  encryptedFields: {'email', 'phone', 'address'},
);

// Transform before storage
final encrypted = await transformer.encryptDocument(document);
await db.putJson(key, encrypted);

// Transform after retrieval
final retrieved = await db.getJson(key);
final decrypted = await transformer.decryptDocument(retrieved);
```

## Secure Key Management

### Platform Keychain Integration

```dart
// Store encryption key securely
await SecureKeyStorage.storeKey('db_encryption_key', encryptionKey);

// Retrieve key
final key = await SecureKeyStorage.getKey('db_encryption_key');

// Check if key exists
final hasKey = await SecureKeyStorage.hasKey('db_encryption_key');

// Delete key
await SecureKeyStorage.deleteKey('db_encryption_key');
```

### Biometric Authentication

```dart
// Open database with biometric auth
final db = await LightningDb.openEncrypted(
  'secure.db',
  password: 'fallback_password',
  useBiometrics: true,
);
```

## Backup and Recovery

### Encrypted Backups

```dart
final backupManager = EncryptedBackupManager(
  db: encryptedDb,
  encryption: encryptionProvider,
);

// Create encrypted backup
await backupManager.backup('daily_backup_20241201');

// List backups
final backups = await backupManager.listBackups();
for (final backup in backups) {
  print('${backup.path}: ${backup.size} bytes, ${backup.createdAt}');
}

// Restore from backup
await backupManager.restore('daily_backup_20241201');

// Delete backup
await backupManager.deleteBackup('old_backup');
```

### Cross-Platform Backup

```dart
// Export encrypted backup for transfer
final exportData = await encryptedDb.export(encrypted: true);

// Save to file or cloud storage
await File('backup.enc').writeAsBytes(exportData);

// Restore on another device
final importData = await File('backup.enc').readAsBytes();
await encryptedDb.import(importData, encrypted: true);
```

## Configuration Options

### High Security Configuration

```dart
final config = EncryptionConfig.highSecurity();

final db = await EncryptedDatabase.open(
  'ultra_secure.db',
  password: strongPassword,
  config: config,
);
```

### Field-Level Configuration

```dart
final config = EncryptionConfig.fieldLevel(
  fields: ['ssn', 'creditCard', 'password', 'notes'],
);

final users = db.encryptedCollection<User>(
  'users',
  config: config,
);
```

### Custom Configuration

```dart
final config = EncryptionConfig(
  algorithm: EncryptionAlgorithm.aes256gcm,
  keyDerivationIterations: 200000,
  useHardwareSecurity: true,
  transparentEncryption: true,
);
```

## Performance Considerations

### Batch Operations

```dart
// Efficient batch encryption
final users = encryptedDb.encryptedCollection<User>('users');

await encryptedDb.transaction((tx) async {
  for (final user in largeUserList) {
    await users.add(user);
  }
});
```

### Selective Encryption

```dart
// Only encrypt sensitive fields
final user = User(
  id: 'user_1',
  name: 'John Doe',          // Plain text - fast queries
  email: 'john@email.com',   // Encrypted - secure
  profilePicture: largeBlob, // Not encrypted - performance
);
```

### Caching Strategies

```dart
// Cache decrypted data for read-heavy workloads
class CachedEncryptedCollection<T> {
  final EncryptedFreezedCollection<T> _collection;
  final Map<String, T> _cache = {};
  
  Future<T?> get(String id) async {
    if (_cache.containsKey(id)) {
      return _cache[id];
    }
    
    final item = await _collection.get(id);
    if (item != null) {
      _cache[id] = item;
    }
    return item;
  }
}
```

## Security Best Practices

### Key Management

1. **Never hardcode keys** in source code
2. **Use platform keychain** for key storage
3. **Implement key rotation** for long-lived applications
4. **Use strong passwords** for key derivation
5. **Consider hardware security modules** for critical applications

```dart
// Good: Key from secure storage
final key = await SecureKeyStorage.getKey('app_encryption_key');

// Bad: Hardcoded key
final key = Uint8List.fromList([1, 2, 3, 4...]); // DON'T DO THIS
```

### Password Security

```dart
// Strong password requirements
bool isStrongPassword(String password) {
  return password.length >= 12 &&
         password.contains(RegExp(r'[A-Z]')) &&
         password.contains(RegExp(r'[a-z]')) &&
         password.contains(RegExp(r'[0-9]')) &&
         password.contains(RegExp(r'[!@#$%^&*]'));
}

// Key derivation with high iteration count
final key = await provider.deriveKey(
  password,
  salt,
  iterations: 200000, // Higher for better security
);
```

### Data Classification

```dart
// Classify data by sensitivity
enum DataSensitivity { public, internal, confidential, restricted }

class SecureUser {
  final String id;           // Public
  final String name;         // Internal
  final String email;        // Confidential - encrypt
  final String ssn;          // Restricted - encrypt + audit
  
  static List<String> get encryptedFields => ['email', 'ssn'];
}
```

## Compliance and Auditing

### Audit Logging

```dart
class AuditLogger {
  static void logEncryption(String operation, String dataId) {
    print('[AUDIT] $operation on $dataId at ${DateTime.now()}');
  }
}

// Log encryption operations
class AuditedEncryptionProvider implements EncryptionProvider {
  final EncryptionProvider _delegate;
  
  @override
  Future<Uint8List> encrypt(Uint8List data) async {
    AuditLogger.logEncryption('ENCRYPT', data.hashCode.toString());
    return await _delegate.encrypt(data);
  }
  
  @override
  Future<Uint8List> decrypt(Uint8List data) async {
    AuditLogger.logEncryption('DECRYPT', data.hashCode.toString());
    return await _delegate.decrypt(data);
  }
}
```

### Compliance Helpers

```dart
// GDPR right to be forgotten
Future<void> deleteUserData(String userId) async {
  // Delete from all collections
  await users.delete(userId);
  await userPreferences.delete(userId);
  await auditLogs.deleteWhere('userId', userId);
  
  // Securely wipe encryption keys if user-specific
  await SecureKeyStorage.deleteKey('user_key_$userId');
}

// HIPAA audit trail
class HipaaCompliantCollection<T> extends EncryptedFreezedCollection<T> {
  @override
  Future<void> add(T model) async {
    await super.add(model);
    await _logAccess('CREATE', getId(model));
  }
  
  @override
  Future<T?> get(String id) async {
    await _logAccess('READ', id);
    return await super.get(id);
  }
  
  Future<void> _logAccess(String action, String id) async {
    // Log to immutable audit trail
  }
}
```

## Testing Encryption

### Unit Tests

```dart
void main() {
  group('Encryption Tests', () {
    test('should encrypt and decrypt data correctly', () async {
      final provider = AesGcmEncryptionProvider(key: testKey);
      final plaintext = Uint8List.fromList('test data'.codeUnits);
      
      final encrypted = await provider.encrypt(plaintext);
      final decrypted = await provider.decrypt(encrypted);
      
      expect(decrypted, equals(plaintext));
      expect(encrypted, isNot(equals(plaintext)));
    });
    
    test('should fail with wrong password', () async {
      final db1 = await EncryptedDatabase.open('test.db', password: 'password1');
      await db1.put('key', 'value');
      await db1.close();
      
      expect(
        () => EncryptedDatabase.open('test.db', password: 'password2'),
        throwsA(isA<EncryptionException>()),
      );
    });
  });
}
```

### Performance Tests

```dart
void main() {
  test('encryption performance should be acceptable', () async {
    final provider = AesGcmEncryptionProvider(key: testKey);
    final data = Uint8List(1024 * 1024); // 1MB
    
    final stopwatch = Stopwatch()..start();
    
    for (int i = 0; i < 100; i++) {
      await provider.encrypt(data);
    }
    
    stopwatch.stop();
    
    final avgTime = stopwatch.elapsedMicroseconds / 100;
    expect(avgTime, lessThan(10000)); // Less than 10ms per MB
  });
}
```

## Troubleshooting

### Common Issues

1. **"Authentication failed" error**
   - Wrong password or corrupted data
   - Check password and try backup restore

2. **Poor performance**
   - Use batch operations for multiple items
   - Consider caching frequently accessed data
   - Monitor encryption metrics

3. **Key not found**
   - Check platform keychain permissions
   - Verify key identifier spelling
   - Handle first-time setup

```dart
// Robust key retrieval
Future<Uint8List> getOrCreateKey(String identifier) async {
  var key = await SecureKeyStorage.getKey(identifier);
  
  if (key == null) {
    // Generate new key on first run
    final provider = AesGcmEncryptionProvider(key: Uint8List(32));
    key = provider.generateSalt(); // Use as key
    await SecureKeyStorage.storeKey(identifier, key);
  }
  
  return key;
}
```

## Migration and Key Rotation

### Rotating Encryption Keys

```dart
Future<void> rotateEncryptionKey(
  EncryptedDatabase oldDb,
  String newPassword,
) async {
  // 1. Export all data with old key
  final exportData = await oldDb.export(encrypted: false);
  
  // 2. Close old database
  await oldDb.close();
  
  // 3. Create new database with new key
  final newDb = await EncryptedDatabase.open(
    oldDb.path,
    password: newPassword,
  );
  
  // 4. Import data with new key
  await newDb.import(exportData, encrypted: false);
  
  // 5. Verify migration
  await _verifyMigration(newDb);
}
```

### Upgrading Encryption Algorithm

```dart
Future<void> upgradeEncryption() async {
  final migration = EncryptionMigration(
    fromAlgorithm: EncryptionAlgorithm.aes256gcm,
    toAlgorithm: EncryptionAlgorithm.xchacha20poly1305,
  );
  
  await migration.migrate('database.db');
}
```

## Conclusion

Lightning DB's encryption system provides enterprise-grade security while maintaining high performance. By following the patterns and best practices in this guide, you can implement robust data protection for your Flutter applications.

Key takeaways:
- Use database-level encryption for simple use cases
- Use field-level encryption for granular control
- Implement proper key management
- Monitor encryption performance
- Follow security best practices
- Test encryption thoroughly