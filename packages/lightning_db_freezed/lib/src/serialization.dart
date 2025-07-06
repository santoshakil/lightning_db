import 'dart:convert';
import 'dart:typed_data';

/// Serialization format for Freezed models
enum SerializationFormat {
  /// JSON serialization (default)
  json,
  
  /// MessagePack serialization (more efficient)
  msgpack,
  
  /// Custom binary serialization
  binary,
}

/// Base serializer interface
abstract class Serializer<T> {
  /// Serialize object to bytes
  Uint8List serialize(T object);
  
  /// Deserialize bytes to object
  T deserialize(Uint8List bytes);
  
  /// Get the serialization format
  SerializationFormat get format;
}

/// JSON-based serializer for Freezed models
class JsonSerializer<T> implements Serializer<T> {
  final Map<String, dynamic> Function(T) toJson;
  final T Function(Map<String, dynamic>) fromJson;
  
  const JsonSerializer({
    required this.toJson,
    required this.fromJson,
  });
  
  @override
  SerializationFormat get format => SerializationFormat.json;
  
  @override
  Uint8List serialize(T object) {
    final json = toJson(object);
    final jsonString = jsonEncode(json);
    return Uint8List.fromList(utf8.encode(jsonString));
  }
  
  @override
  T deserialize(Uint8List bytes) {
    final jsonString = utf8.decode(bytes);
    final json = jsonDecode(jsonString) as Map<String, dynamic>;
    return fromJson(json);
  }
}

/// Optimized serializer that uses compression
class CompressedJsonSerializer<T> extends JsonSerializer<T> {
  CompressedJsonSerializer({
    required super.toJson,
    required super.fromJson,
  });
  
  @override
  Uint8List serialize(T object) {
    final uncompressed = super.serialize(object);
    // In a real implementation, we would use zlib or similar
    // For now, return uncompressed
    return uncompressed;
  }
  
  @override
  T deserialize(Uint8List bytes) {
    // In a real implementation, we would decompress first
    return super.deserialize(bytes);
  }
}

/// Helper class for common serialization tasks
class SerializationHelpers {
  /// Convert a DateTime to milliseconds since epoch
  static int dateTimeToMillis(DateTime dateTime) {
    return dateTime.millisecondsSinceEpoch;
  }
  
  /// Convert milliseconds since epoch to DateTime
  static DateTime millisToDateTime(int millis) {
    return DateTime.fromMillisecondsSinceEpoch(millis);
  }
  
  /// Convert a Duration to microseconds
  static int durationToMicros(Duration duration) {
    return duration.inMicroseconds;
  }
  
  /// Convert microseconds to Duration
  static Duration microsToDuration(int micros) {
    return Duration(microseconds: micros);
  }
  
  /// Encode a list of strings as a single string
  static String encodeStringList(List<String> list) {
    return list.join('\u0000'); // Use null character as separator
  }
  
  /// Decode a string to a list of strings
  static List<String> decodeStringList(String encoded) {
    if (encoded.isEmpty) return [];
    return encoded.split('\u0000');
  }
}