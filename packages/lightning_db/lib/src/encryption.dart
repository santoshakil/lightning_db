import 'dart:typed_data';
import 'package:flutter/services.dart';
import 'package:lightning_db_dart/lightning_db_dart.dart';

/// Flutter-specific encryption extensions
extension EncryptionExtensions on LightningDb {
  /// Open an encrypted database with biometric authentication
  static Future<EncryptedDatabase> openEncrypted(
    String path, {
    required String password,
    bool useBiometrics = false,
    DatabaseConfig? config,
  }) async {
    String finalPassword = password;
    
    if (useBiometrics) {
      // Use platform channel for biometric authentication
      try {
        const platform = MethodChannel('lightning_db/biometrics');
        final bioPassword = await platform.invokeMethod<String>(
          'authenticate',
          {'reason': 'Authenticate to access database'},
        );
        
        if (bioPassword != null) {
          finalPassword = bioPassword;
        }
      } catch (e) {
        // Fallback to password if biometrics fail
      }
    }
    
    return EncryptedDatabase.open(
      path,
      password: finalPassword,
      config: config,
    );
  }
  
  /// Create encrypted collection with field-level encryption
  EncryptedFreezedCollection<T> encryptedCollection<T>(
    String name, {
    required EncryptionProvider encryption,
    List<String>? encryptedFields,
  }) {
    return EncryptedFreezedCollection<T>(
      db: this,
      name: name,
      encryption: encryption,
      encryptedFields: encryptedFields ?? [],
    );
  }
}

/// Secure key storage using platform keychain/keystore
class SecureKeyStorage {
  static const _channel = MethodChannel('lightning_db/secure_storage');
  
  /// Store encryption key securely
  static Future<void> storeKey(String identifier, Uint8List key) async {
    await _channel.invokeMethod('storeKey', {
      'identifier': identifier,
      'key': key,
    });
  }
  
  /// Retrieve encryption key
  static Future<Uint8List?> getKey(String identifier) async {
    final result = await _channel.invokeMethod<Uint8List>('getKey', {
      'identifier': identifier,
    });
    return result;
  }
  
  /// Delete encryption key
  static Future<void> deleteKey(String identifier) async {
    await _channel.invokeMethod('deleteKey', {
      'identifier': identifier,
    });
  }
  
  /// Check if key exists
  static Future<bool> hasKey(String identifier) async {
    final result = await _channel.invokeMethod<bool>('hasKey', {
      'identifier': identifier,
    });
    return result ?? false;
  }
}

/// Database encryption configuration
class EncryptionConfig {
  /// Encryption algorithm
  final EncryptionAlgorithm algorithm;
  
  /// Key derivation iterations
  final int keyDerivationIterations;
  
  /// Enable field-level encryption
  final bool enableFieldEncryption;
  
  /// Fields to encrypt (if field encryption is enabled)
  final List<String> encryptedFields;
  
  /// Use hardware security module if available
  final bool useHardwareSecurity;
  
  /// Enable transparent encryption
  final bool transparentEncryption;
  
  const EncryptionConfig({
    this.algorithm = EncryptionAlgorithm.aes256gcm,
    this.keyDerivationIterations = 100000,
    this.enableFieldEncryption = false,
    this.encryptedFields = const [],
    this.useHardwareSecurity = true,
    this.transparentEncryption = false,
  });
  
  /// High security configuration
  factory EncryptionConfig.highSecurity() => const EncryptionConfig(
    algorithm: EncryptionAlgorithm.aes256gcm,
    keyDerivationIterations: 200000,
    useHardwareSecurity: true,
  );
  
  /// Field-level encryption configuration
  factory EncryptionConfig.fieldLevel({
    required List<String> fields,
  }) => EncryptionConfig(
    enableFieldEncryption: true,
    encryptedFields: fields,
  );
  
  Map<String, dynamic> toJson() => {
    'algorithm': algorithm.name,
    'keyDerivationIterations': keyDerivationIterations,
    'enableFieldEncryption': enableFieldEncryption,
    'encryptedFields': encryptedFields,
    'useHardwareSecurity': useHardwareSecurity,
    'transparentEncryption': transparentEncryption,
  };
}

/// Supported encryption algorithms
enum EncryptionAlgorithm {
  /// AES-256 in GCM mode
  aes256gcm,
  
  /// ChaCha20-Poly1305
  chacha20poly1305,
  
  /// XChaCha20-Poly1305
  xchacha20poly1305,
}

/// Encrypted backup manager
class EncryptedBackupManager {
  final EncryptedDatabase db;
  final EncryptionProvider encryption;
  
  EncryptedBackupManager({
    required this.db,
    required this.encryption,
  });
  
  /// Create encrypted backup
  Future<void> backup(String backupPath, {String? password}) async {
    // Export database
    final export = await db._db.export();
    
    // Encrypt the export
    final encrypted = await encryption.encrypt(export);
    
    // Save encrypted backup
    await db._db.putBytes('_backup:$backupPath', encrypted);
    
    // Store metadata
    await db._db.putJson('_backup_metadata:$backupPath', {
      'createdAt': DateTime.now().toIso8601String(),
      'size': encrypted.length,
      'encrypted': true,
      'version': 1,
    });
  }
  
  /// Restore from encrypted backup
  Future<void> restore(String backupPath, {String? password}) async {
    // Get encrypted backup
    final encrypted = await db._db.getBytes('_backup:$backupPath');
    if (encrypted == null) {
      throw EncryptionException(
        message: 'Backup not found: $backupPath',
        errorType: 'backup_not_found',
      );
    }
    
    // Decrypt
    final decrypted = await encryption.decrypt(encrypted);
    
    // Import into database
    await db._db.import(decrypted);
  }
  
  /// List available backups
  Future<List<BackupInfo>> listBackups() async {
    final backups = <BackupInfo>[];
    
    await for (final kv in db._db.scanStream(startKey: '_backup_metadata:')) {
      if (!kv.key.startsWith('_backup_metadata:')) break;
      
      final metadata = await db._db.getJson(kv.key);
      if (metadata != null) {
        final path = kv.key.substring('_backup_metadata:'.length);
        backups.add(BackupInfo(
          path: path,
          createdAt: DateTime.parse(metadata['createdAt']),
          size: metadata['size'],
          encrypted: metadata['encrypted'] ?? false,
        ));
      }
    }
    
    return backups;
  }
  
  /// Delete backup
  Future<void> deleteBackup(String backupPath) async {
    await db._db.delete('_backup:$backupPath');
    await db._db.delete('_backup_metadata:$backupPath');
  }
}

/// Backup information
class BackupInfo {
  final String path;
  final DateTime createdAt;
  final int size;
  final bool encrypted;
  
  BackupInfo({
    required this.path,
    required this.createdAt,
    required this.size,
    required this.encrypted,
  });
  
  Map<String, dynamic> toJson() => {
    'path': path,
    'createdAt': createdAt.toIso8601String(),
    'size': size,
    'encrypted': encrypted,
  };
}

/// Encryption metrics for monitoring
class EncryptionMetrics {
  int encryptionOperations = 0;
  int decryptionOperations = 0;
  int encryptionErrors = 0;
  int decryptionErrors = 0;
  Duration totalEncryptionTime = Duration.zero;
  Duration totalDecryptionTime = Duration.zero;
  
  double get averageEncryptionTime {
    if (encryptionOperations == 0) return 0;
    return totalEncryptionTime.inMicroseconds / encryptionOperations;
  }
  
  double get averageDecryptionTime {
    if (decryptionOperations == 0) return 0;
    return totalDecryptionTime.inMicroseconds / decryptionOperations;
  }
  
  double get encryptionErrorRate {
    final total = encryptionOperations + encryptionErrors;
    if (total == 0) return 0;
    return encryptionErrors / total;
  }
  
  double get decryptionErrorRate {
    final total = decryptionOperations + decryptionErrors;
    if (total == 0) return 0;
    return decryptionErrors / total;
  }
  
  Map<String, dynamic> toJson() => {
    'encryptionOperations': encryptionOperations,
    'decryptionOperations': decryptionOperations,
    'encryptionErrors': encryptionErrors,
    'decryptionErrors': decryptionErrors,
    'totalEncryptionTimeMs': totalEncryptionTime.inMilliseconds,
    'totalDecryptionTimeMs': totalDecryptionTime.inMilliseconds,
    'averageEncryptionTimeUs': averageEncryptionTime,
    'averageDecryptionTimeUs': averageDecryptionTime,
    'encryptionErrorRate': encryptionErrorRate,
    'decryptionErrorRate': decryptionErrorRate,
  };
  
  void reset() {
    encryptionOperations = 0;
    decryptionOperations = 0;
    encryptionErrors = 0;
    decryptionErrors = 0;
    totalEncryptionTime = Duration.zero;
    totalDecryptionTime = Duration.zero;
  }
}

/// Monitored encryption provider
class MonitoredEncryptionProvider implements EncryptionProvider {
  final EncryptionProvider _delegate;
  final EncryptionMetrics metrics = EncryptionMetrics();
  
  MonitoredEncryptionProvider(this._delegate);
  
  @override
  Future<Uint8List> encrypt(Uint8List data) async {
    final stopwatch = Stopwatch()..start();
    
    try {
      final result = await _delegate.encrypt(data);
      metrics.encryptionOperations++;
      metrics.totalEncryptionTime += stopwatch.elapsed;
      return result;
    } catch (e) {
      metrics.encryptionErrors++;
      rethrow;
    }
  }
  
  @override
  Future<Uint8List> decrypt(Uint8List data) async {
    final stopwatch = Stopwatch()..start();
    
    try {
      final result = await _delegate.decrypt(data);
      metrics.decryptionOperations++;
      metrics.totalDecryptionTime += stopwatch.elapsed;
      return result;
    } catch (e) {
      metrics.decryptionErrors++;
      rethrow;
    }
  }
  
  @override
  Future<Uint8List> deriveKey(String password, Uint8List salt) => 
      _delegate.deriveKey(password, salt);
  
  @override
  Uint8List generateSalt() => _delegate.generateSalt();
  
  @override
  Uint8List generateIV() => _delegate.generateIV();
}