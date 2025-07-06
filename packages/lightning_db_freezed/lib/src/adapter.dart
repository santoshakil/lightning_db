import 'dart:typed_data';
import 'package:meta/meta.dart';
import 'serialization.dart';

/// Base adapter for Freezed models
abstract class FreezedAdapter<T> {
  /// Get a unique key for the object
  String getKey(T object);
  
  /// Serialize the object to bytes
  Uint8List serialize(T object);
  
  /// Deserialize bytes back to object
  T deserialize(Uint8List bytes);
  
  /// Get the type name for this adapter
  String get typeName;
  
  /// Create a compound key with type prefix
  String createCompoundKey(T object) {
    return '$typeName:${getKey(object)}';
  }
  
  /// Extract the key from a compound key
  static String extractKey(String compoundKey) {
    final index = compoundKey.indexOf(':');
    if (index == -1) return compoundKey;
    return compoundKey.substring(index + 1);
  }
  
  /// Extract the type from a compound key
  static String extractType(String compoundKey) {
    final index = compoundKey.indexOf(':');
    if (index == -1) return '';
    return compoundKey.substring(0, index);
  }
}

/// Simple adapter using a serializer
class SimpleFreezedAdapter<T> extends FreezedAdapter<T> {
  final Serializer<T> serializer;
  final String Function(T) keyExtractor;
  final String typeName;
  
  SimpleFreezedAdapter({
    required this.serializer,
    required this.keyExtractor,
    required this.typeName,
  });
  
  @override
  String getKey(T object) => keyExtractor(object);
  
  @override
  Uint8List serialize(T object) => serializer.serialize(object);
  
  @override
  T deserialize(Uint8List bytes) => serializer.deserialize(bytes);
}

/// JSON-based adapter for Freezed models
class JsonFreezedAdapter<T> extends FreezedAdapter<T> {
  final Map<String, dynamic> Function(T) toJson;
  final T Function(Map<String, dynamic>) fromJson;
  final String Function(T) keyExtractor;
  final String typeName;
  
  late final JsonSerializer<T> _serializer;
  
  JsonFreezedAdapter({
    required this.toJson,
    required this.fromJson,
    required this.keyExtractor,
    required this.typeName,
  }) {
    _serializer = JsonSerializer(toJson: toJson, fromJson: fromJson);
  }
  
  @override
  String getKey(T object) => keyExtractor(object);
  
  @override
  Uint8List serialize(T object) => _serializer.serialize(object);
  
  @override
  T deserialize(Uint8List bytes) => _serializer.deserialize(bytes);
}

/// Adapter with custom serialization logic
class CustomFreezedAdapter<T> extends FreezedAdapter<T> {
  final String Function(T) keyExtractor;
  final Uint8List Function(T) customSerializer;
  final T Function(Uint8List) customDeserializer;
  final String typeName;
  
  CustomFreezedAdapter({
    required this.keyExtractor,
    required this.customSerializer,
    required this.customDeserializer,
    required this.typeName,
  });
  
  @override
  String getKey(T object) => keyExtractor(object);
  
  @override
  Uint8List serialize(T object) => customSerializer(object);
  
  @override
  T deserialize(Uint8List bytes) => customDeserializer(bytes);
}

/// Mixin for Freezed models to provide adapter functionality
mixin LightningDbModel {
  /// Get the unique key for this model
  String get dbKey;
  
  /// Get the collection name for this model type
  String get collectionName;
}

/// Factory for creating adapters
class AdapterFactory {
  static final Map<Type, FreezedAdapter> _adapters = {};
  
  /// Register an adapter for a type
  static void register<T>(FreezedAdapter<T> adapter) {
    _adapters[T] = adapter;
  }
  
  /// Get an adapter for a type
  static FreezedAdapter<T>? get<T>() {
    return _adapters[T] as FreezedAdapter<T>?;
  }
  
  /// Check if an adapter is registered for a type
  static bool hasAdapter<T>() {
    return _adapters.containsKey(T);
  }
  
  /// Clear all registered adapters
  static void clear() {
    _adapters.clear();
  }
  
  /// Create a JSON adapter with type inference
  static JsonFreezedAdapter<T> createJsonAdapter<T>({
    required Map<String, dynamic> Function(T) toJson,
    required T Function(Map<String, dynamic>) fromJson,
    required String Function(T) keyExtractor,
    String? typeName,
  }) {
    return JsonFreezedAdapter<T>(
      toJson: toJson,
      fromJson: fromJson,
      keyExtractor: keyExtractor,
      typeName: typeName ?? T.toString(),
    );
  }
}