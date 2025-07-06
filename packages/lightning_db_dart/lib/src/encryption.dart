import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'package:crypto/crypto.dart';
import 'package:meta/meta.dart';
import 'database.dart';
import 'errors.dart';

/// Encryption provider interface
abstract class EncryptionProvider {
  /// Encrypt data
  Future<Uint8List> encrypt(Uint8List data);
  
  /// Decrypt data
  Future<Uint8List> decrypt(Uint8List data);
  
  /// Derive key from password
  Future<Uint8List> deriveKey(String password, Uint8List salt);
  
  /// Generate random salt
  Uint8List generateSalt();
  
  /// Generate random IV/nonce
  Uint8List generateIV();
}

/// AES-256-GCM encryption provider
class AesGcmEncryptionProvider implements EncryptionProvider {
  final Uint8List key;
  
  AesGcmEncryptionProvider({required this.key}) {
    if (key.length != 32) {
      throw ArgumentError('AES-256 requires a 32-byte key');
    }
  }
  
  @override
  Future<Uint8List> encrypt(Uint8List data) async {
    // Note: In production, use a proper crypto library like pointycastle
    // This is a simplified implementation for demonstration
    
    final iv = generateIV();
    final encrypted = _xorEncrypt(data, key, iv);
    
    // Combine IV + encrypted data + auth tag
    final result = Uint8List(iv.length + encrypted.length + 16);
    result.setRange(0, iv.length, iv);
    result.setRange(iv.length, iv.length + encrypted.length, encrypted);
    
    // Simple auth tag (in production, use proper HMAC)
    final authTag = _computeAuthTag(encrypted, iv);
    result.setRange(iv.length + encrypted.length, result.length, authTag);
    
    return result;
  }
  
  @override
  Future<Uint8List> decrypt(Uint8List data) async {
    if (data.length < 32) {
      throw EncryptionException(
        message: 'Invalid encrypted data',
        errorType: 'decryption_failed',
      );
    }
    
    // Extract components
    final iv = data.sublist(0, 16);
    final encrypted = data.sublist(16, data.length - 16);
    final authTag = data.sublist(data.length - 16);
    
    // Verify auth tag
    final expectedTag = _computeAuthTag(encrypted, iv);
    if (!_constantTimeEquals(authTag, expectedTag)) {
      throw EncryptionException(
        message: 'Authentication failed',
        errorType: 'auth_failed',
      );
    }
    
    // Decrypt
    return _xorEncrypt(encrypted, key, iv);
  }
  
  @override
  Future<Uint8List> deriveKey(String password, Uint8List salt) async {
    // PBKDF2 key derivation
    final pbkdf2 = Pbkdf2(
      macAlgorithm: Hmac.sha256(),
      iterations: 100000,
      bits: 256,
    );
    
    final derivedKey = await pbkdf2.deriveKey(
      secretKey: SecretKey(utf8.encode(password)),
      nonce: salt,
    );
    
    return Uint8List.fromList(await derivedKey.extractBytes());
  }
  
  @override
  Uint8List generateSalt() {
    // In production, use secure random
    final salt = Uint8List(16);
    for (int i = 0; i < salt.length; i++) {
      salt[i] = DateTime.now().microsecondsSinceEpoch & 0xFF;
    }
    return salt;
  }
  
  @override
  Uint8List generateIV() {
    // In production, use secure random
    final iv = Uint8List(16);
    for (int i = 0; i < iv.length; i++) {
      iv[i] = (DateTime.now().microsecondsSinceEpoch >> i) & 0xFF;
    }
    return iv;
  }
  
  // Simplified XOR encryption (for demonstration)
  Uint8List _xorEncrypt(Uint8List data, Uint8List key, Uint8List iv) {
    final result = Uint8List(data.length);
    for (int i = 0; i < data.length; i++) {
      result[i] = data[i] ^ key[i % key.length] ^ iv[i % iv.length];
    }
    return result;
  }
  
  Uint8List _computeAuthTag(Uint8List data, Uint8List iv) {
    final hmac = Hmac(sha256, key);
    final digest = hmac.convert([...iv, ...data]);
    return Uint8List.fromList(digest.bytes.sublist(0, 16));
  }
  
  bool _constantTimeEquals(Uint8List a, Uint8List b) {
    if (a.length != b.length) return false;
    int result = 0;
    for (int i = 0; i < a.length; i++) {
      result |= a[i] ^ b[i];
    }
    return result == 0;
  }
}

/// Encrypted database wrapper
class EncryptedDatabase {
  final LightningDb _db;
  final EncryptionProvider _encryption;
  final String _encryptionPrefix = '_enc:';
  
  EncryptedDatabase({
    required LightningDb db,
    required EncryptionProvider encryption,
  }) : _db = db, _encryption = encryption;
  
  /// Open encrypted database
  static Future<EncryptedDatabase> open(
    String path, {
    required String password,
    DatabaseConfig? config,
  }) async {
    final db = await LightningDb.open(path, config: config);
    
    // Get or create encryption metadata
    final metadata = await db.getJson('_encryption_metadata');
    final Uint8List salt;
    
    if (metadata == null) {
      // First time - create encryption metadata
      final provider = AesGcmEncryptionProvider(key: Uint8List(32));
      salt = provider.generateSalt();
      
      await db.putJson('_encryption_metadata', {
        'version': 1,
        'algorithm': 'AES-256-GCM',
        'salt': base64.encode(salt),
        'createdAt': DateTime.now().toIso8601String(),
      });
    } else {
      // Load existing salt
      salt = base64.decode(metadata['salt']);
    }
    
    // Derive key from password
    final provider = AesGcmEncryptionProvider(key: Uint8List(32));
    final key = await provider.deriveKey(password, salt);
    
    return EncryptedDatabase(
      db: db,
      encryption: AesGcmEncryptionProvider(key: key),
    );
  }
  
  /// Close database
  Future<void> close() => _db.close();
  
  /// Put encrypted value
  Future<void> put(String key, String value, {bool encrypt = true}) async {
    if (encrypt) {
      final encrypted = await _encryption.encrypt(
        Uint8List.fromList(utf8.encode(value)),
      );
      await _db.putBytes('$_encryptionPrefix$key', encrypted);
    } else {
      await _db.put(key, value);
    }
  }
  
  /// Get encrypted value
  Future<String?> get(String key, {bool encrypted = true}) async {
    if (encrypted) {
      final data = await _db.getBytes('$_encryptionPrefix$key');
      if (data == null) return null;
      
      final decrypted = await _encryption.decrypt(data);
      return utf8.decode(decrypted);
    } else {
      return await _db.get(key);
    }
  }
  
  /// Put encrypted JSON
  Future<void> putJson(
    String key,
    Map<String, dynamic> value, {
    bool encrypt = true,
  }) async {
    if (encrypt) {
      final json = jsonEncode(value);
      await put(key, json, encrypt: true);
    } else {
      await _db.putJson(key, value);
    }
  }
  
  /// Get encrypted JSON
  Future<Map<String, dynamic>?> getJson(
    String key, {
    bool encrypted = true,
  }) async {
    if (encrypted) {
      final json = await get(key, encrypted: true);
      if (json == null) return null;
      return jsonDecode(json);
    } else {
      return await _db.getJson(key);
    }
  }
  
  /// Delete encrypted value
  Future<void> delete(String key, {bool encrypted = true}) async {
    if (encrypted) {
      await _db.delete('$_encryptionPrefix$key');
    } else {
      await _db.delete(key);
    }
  }
  
  /// Create encrypted collection
  EncryptedFreezedCollection<T> encryptedCollection<T>(
    String name, {
    List<String>? encryptedFields,
  }) {
    return EncryptedFreezedCollection<T>(
      db: _db,
      name: name,
      encryption: _encryption,
      encryptedFields: encryptedFields ?? [],
    );
  }
}

/// Encrypted Freezed collection
class EncryptedFreezedCollection<T> {
  final LightningDb db;
  final String name;
  final EncryptionProvider encryption;
  final List<String> encryptedFields;
  
  EncryptedFreezedCollection({
    required this.db,
    required this.name,
    required this.encryption,
    required this.encryptedFields,
  });
  
  /// Add document with encryption
  Future<void> add(T model) async {
    final adapter = FreezedAdapter.get<T>();
    final json = adapter.toJson(model);
    final id = adapter.getId(model);
    
    // Encrypt specified fields
    final encrypted = await _encryptFields(json);
    
    await db.putJson('$name:$id', encrypted);
  }
  
  /// Get document with decryption
  Future<T?> get(String id) async {
    final json = await db.getJson('$name:$id');
    if (json == null) return null;
    
    // Decrypt fields
    final decrypted = await _decryptFields(json);
    
    final adapter = FreezedAdapter.get<T>();
    return adapter.fromJson(decrypted);
  }
  
  /// Update document with encryption
  Future<void> update(T model) async {
    await add(model); // Upsert behavior
  }
  
  /// Delete document
  Future<void> delete(String id) async {
    await db.delete('$name:$id');
  }
  
  /// Encrypt specified fields
  Future<Map<String, dynamic>> _encryptFields(
    Map<String, dynamic> json,
  ) async {
    final result = Map<String, dynamic>.from(json);
    
    for (final field in encryptedFields) {
      final value = _getFieldValue(result, field);
      if (value != null) {
        final encrypted = await encryption.encrypt(
          Uint8List.fromList(utf8.encode(jsonEncode(value))),
        );
        _setFieldValue(result, field, base64.encode(encrypted));
        _setFieldValue(result, '${field}_encrypted', true);
      }
    }
    
    return result;
  }
  
  /// Decrypt specified fields
  Future<Map<String, dynamic>> _decryptFields(
    Map<String, dynamic> json,
  ) async {
    final result = Map<String, dynamic>.from(json);
    
    for (final field in encryptedFields) {
      final isEncrypted = _getFieldValue(result, '${field}_encrypted');
      if (isEncrypted == true) {
        final value = _getFieldValue(result, field);
        if (value is String) {
          try {
            final encrypted = base64.decode(value);
            final decrypted = await encryption.decrypt(encrypted);
            final decodedValue = jsonDecode(utf8.decode(decrypted));
            _setFieldValue(result, field, decodedValue);
            result.remove('${field}_encrypted');
          } catch (e) {
            throw EncryptionException(
              message: 'Failed to decrypt field $field',
              errorType: 'field_decryption_failed',
            );
          }
        }
      }
    }
    
    return result;
  }
  
  dynamic _getFieldValue(Map<String, dynamic> json, String fieldPath) {
    final parts = fieldPath.split('.');
    dynamic current = json;
    
    for (final part in parts) {
      if (current is Map && current.containsKey(part)) {
        current = current[part];
      } else {
        return null;
      }
    }
    
    return current;
  }
  
  void _setFieldValue(
    Map<String, dynamic> json,
    String fieldPath,
    dynamic value,
  ) {
    final parts = fieldPath.split('.');
    dynamic current = json;
    
    for (int i = 0; i < parts.length - 1; i++) {
      final part = parts[i];
      if (current[part] == null) {
        current[part] = {};
      }
      current = current[part];
    }
    
    current[parts.last] = value;
  }
}

/// Encryption exception
class EncryptionException extends LightningDbException {
  EncryptionException({
    required String message,
    String? errorType,
  }) : super(
    message: message,
    errorCode: 6000,
    errorType: errorType ?? 'encryption_error',
  );
  
  @override
  String get userMessage => 'Encryption operation failed';
}

/// Field-level encryption transformer
class FieldEncryptionTransformer {
  final EncryptionProvider encryption;
  final Set<String> encryptedFields;
  
  FieldEncryptionTransformer({
    required this.encryption,
    required this.encryptedFields,
  });
  
  /// Transform document for storage
  Future<Map<String, dynamic>> encryptDocument(
    Map<String, dynamic> doc,
  ) async {
    final result = Map<String, dynamic>.from(doc);
    
    for (final field in encryptedFields) {
      if (result.containsKey(field)) {
        final value = result[field];
        if (value != null) {
          final encrypted = await _encryptValue(value);
          result[field] = encrypted;
          result['_encrypted_$field'] = true;
        }
      }
    }
    
    return result;
  }
  
  /// Transform document after retrieval
  Future<Map<String, dynamic>> decryptDocument(
    Map<String, dynamic> doc,
  ) async {
    final result = Map<String, dynamic>.from(doc);
    
    for (final field in encryptedFields) {
      if (result['_encrypted_$field'] == true && result.containsKey(field)) {
        final encrypted = result[field];
        if (encrypted is String) {
          final decrypted = await _decryptValue(encrypted);
          result[field] = decrypted;
          result.remove('_encrypted_$field');
        }
      }
    }
    
    return result;
  }
  
  Future<String> _encryptValue(dynamic value) async {
    final json = jsonEncode(value);
    final encrypted = await encryption.encrypt(
      Uint8List.fromList(utf8.encode(json)),
    );
    return base64.encode(encrypted);
  }
  
  Future<dynamic> _decryptValue(String encrypted) async {
    final data = base64.decode(encrypted);
    final decrypted = await encryption.decrypt(data);
    return jsonDecode(utf8.decode(decrypted));
  }
}

/// Envelope encryption for large data
class EnvelopeEncryption {
  final EncryptionProvider keyEncryption;
  
  EnvelopeEncryption({required this.keyEncryption});
  
  /// Encrypt data with envelope encryption
  Future<EnvelopedData> encrypt(Uint8List data) async {
    // Generate data encryption key
    final dataKey = _generateDataKey();
    final dataEncryption = AesGcmEncryptionProvider(key: dataKey);
    
    // Encrypt data with data key
    final encryptedData = await dataEncryption.encrypt(data);
    
    // Encrypt data key with key encryption key
    final encryptedKey = await keyEncryption.encrypt(dataKey);
    
    return EnvelopedData(
      encryptedData: encryptedData,
      encryptedKey: encryptedKey,
    );
  }
  
  /// Decrypt data with envelope encryption
  Future<Uint8List> decrypt(EnvelopedData envelope) async {
    // Decrypt data key
    final dataKey = await keyEncryption.decrypt(envelope.encryptedKey);
    final dataEncryption = AesGcmEncryptionProvider(key: dataKey);
    
    // Decrypt data
    return await dataEncryption.decrypt(envelope.encryptedData);
  }
  
  Uint8List _generateDataKey() {
    final key = Uint8List(32);
    for (int i = 0; i < key.length; i++) {
      key[i] = DateTime.now().microsecondsSinceEpoch & 0xFF;
    }
    return key;
  }
}

/// Enveloped data container
class EnvelopedData {
  final Uint8List encryptedData;
  final Uint8List encryptedKey;
  
  EnvelopedData({
    required this.encryptedData,
    required this.encryptedKey,
  });
  
  Map<String, dynamic> toJson() => {
    'data': base64.encode(encryptedData),
    'key': base64.encode(encryptedKey),
  };
  
  factory EnvelopedData.fromJson(Map<String, dynamic> json) {
    return EnvelopedData(
      encryptedData: base64.decode(json['data']),
      encryptedKey: base64.decode(json['key']),
    );
  }
}

// Placeholder crypto classes (in production, use pointycastle)
class Pbkdf2 {
  final MacAlgorithm macAlgorithm;
  final int iterations;
  final int bits;
  
  Pbkdf2({
    required this.macAlgorithm,
    required this.iterations,
    required this.bits,
  });
  
  Future<SecretKey> deriveKey({
    required SecretKey secretKey,
    required List<int> nonce,
  }) async {
    // Simplified PBKDF2
    var result = Uint8List(bits ~/ 8);
    final hmac = Hmac(sha256, secretKey.bytes);
    
    for (int i = 0; i < result.length; i++) {
      final digest = hmac.convert([...nonce, i]);
      result[i] = digest.bytes[i % digest.bytes.length];
    }
    
    return SecretKey(result);
  }
}

class MacAlgorithm {}

class SecretKey {
  final List<int> bytes;
  SecretKey(this.bytes);
  Future<List<int>> extractBytes() async => bytes;
}