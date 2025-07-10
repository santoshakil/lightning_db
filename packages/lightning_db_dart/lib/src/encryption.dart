import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'package:crypto/crypto.dart';
import 'database.dart';
import 'types.dart';

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
}

/// Simple XOR encryption provider (for demonstration - not secure for production)
class SimpleEncryptionProvider implements EncryptionProvider {
  final Uint8List _key;
  
  SimpleEncryptionProvider({required Uint8List key}) : _key = key;
  
  @override
  Future<Uint8List> encrypt(Uint8List data) async {
    final result = Uint8List(data.length);
    for (int i = 0; i < data.length; i++) {
      result[i] = data[i] ^ _key[i % _key.length];
    }
    return result;
  }
  
  @override
  Future<Uint8List> decrypt(Uint8List data) async {
    // XOR decryption is the same as encryption
    return encrypt(data);
  }
  
  @override
  Future<Uint8List> deriveKey(String password, Uint8List salt) async {
    // Simple key derivation using SHA256
    final bytes = utf8.encode(password) + salt;
    final digest = sha256.convert(bytes);
    return Uint8List.fromList(digest.bytes);
  }
  
  @override
  Uint8List generateSalt() {
    // Generate a simple 16-byte salt
    final salt = Uint8List(16);
    for (int i = 0; i < 16; i++) {
      salt[i] = DateTime.now().millisecondsSinceEpoch % 256;
    }
    return salt;
  }
}

/// Encrypted database wrapper
class EncryptedDatabase {
  final LightningDb _db;
  final EncryptionProvider _encryption;
  
  EncryptedDatabase._({
    required LightningDb db,
    required EncryptionProvider encryption,
  }) : _db = db, _encryption = encryption;
  
  /// Open encrypted database
  static Future<EncryptedDatabase> open(
    String path, {
    required String password,
    DatabaseConfig? config,
  }) async {
    final db = await LightningDb.open(path);
    
    // Simple encryption setup
    final salt = Uint8List.fromList([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    final provider = SimpleEncryptionProvider(key: Uint8List.fromList(utf8.encode(password)));
    final key = await provider.deriveKey(password, salt);
    
    final encryption = SimpleEncryptionProvider(key: key);
    
    return EncryptedDatabase._(
      db: db,
      encryption: encryption,
    );
  }
  
  /// Create encrypted database
  static Future<EncryptedDatabase> create(
    String path, {
    required String password,
    DatabaseConfig? config,
  }) async {
    final db = await LightningDb.create(path, config);
    
    // Simple encryption setup
    final salt = Uint8List.fromList([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    final provider = SimpleEncryptionProvider(key: Uint8List.fromList(utf8.encode(password)));
    final key = await provider.deriveKey(password, salt);
    
    final encryption = SimpleEncryptionProvider(key: key);
    
    return EncryptedDatabase._(
      db: db,
      encryption: encryption,
    );
  }
  
  /// Put encrypted data
  Future<void> put(String key, Uint8List value) async {
    final encryptedValue = await _encryption.encrypt(value);
    await _db.put(key, encryptedValue);
  }
  
  /// Get and decrypt data
  Future<Uint8List?> get(String key) async {
    final encryptedValue = await _db.get(key);
    if (encryptedValue == null) return null;
    
    return await _encryption.decrypt(encryptedValue);
  }
  
  /// Close the database
  Future<void> close() async {
    await _db.close();
  }
  
  /// Check if database is closed
  bool get isClosed => _db.isClosed;
  
  /// Get database path
  String get path => _db.path;
}