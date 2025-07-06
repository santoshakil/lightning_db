import 'package:test/test.dart';
import 'package:lightning_db_freezed/lightning_db_freezed.dart';
import 'package:freezed_annotation/freezed_annotation.dart';
import 'package:json_annotation/json_annotation.dart';

part 'freezed_integration_test.freezed.dart';
part 'freezed_integration_test.g.dart';

// Example Freezed model
@freezed
class User with _$User {
  const User._();
  
  const factory User({
    required String id,
    required String name,
    required int age,
    @Default(true) bool isActive,
    required DateTime createdAt,
    DateTime? updatedAt,
  }) = _User;
  
  factory User.fromJson(Map<String, dynamic> json) => _$UserFromJson(json);
}

void main() {
  group('Lightning DB Freezed Integration', () {
    test('Exports are available', () {
      // Verify that all types are exported
      expect(FreezedAdapter, isNotNull);
      expect(JsonFreezedAdapter, isNotNull);
      expect(FreezedCollection, isNotNull);
      expect(QueryBuilder, isNotNull);
      expect(ReactiveCollection, isNotNull);
      expect(SerializationFormat, isNotNull);
    });
    
    test('JSON adapter creation', () {
      final adapter = JsonFreezedAdapter<User>(
        toJson: (user) => user.toJson(),
        fromJson: User.fromJson,
        keyExtractor: (user) => user.id,
        typeName: 'users',
      );
      
      expect(adapter.typeName, equals('users'));
      
      final user = User(
        id: '123',
        name: 'Alice',
        age: 30,
        createdAt: DateTime.now(),
      );
      
      expect(adapter.getKey(user), equals('123'));
      expect(adapter.createCompoundKey(user), equals('users:123'));
    });
    
    test('Serialization and deserialization', () {
      final adapter = JsonFreezedAdapter<User>(
        toJson: (user) => user.toJson(),
        fromJson: User.fromJson,
        keyExtractor: (user) => user.id,
        typeName: 'users',
      );
      
      final originalUser = User(
        id: '123',
        name: 'Alice',
        age: 30,
        createdAt: DateTime.now(),
        updatedAt: DateTime.now(),
      );
      
      // Serialize
      final bytes = adapter.serialize(originalUser);
      expect(bytes, isNotEmpty);
      
      // Deserialize
      final deserializedUser = adapter.deserialize(bytes);
      expect(deserializedUser.id, equals(originalUser.id));
      expect(deserializedUser.name, equals(originalUser.name));
      expect(deserializedUser.age, equals(originalUser.age));
      expect(deserializedUser.isActive, equals(originalUser.isActive));
    });
    
    test('Adapter factory', () {
      final adapter = AdapterFactory.createJsonAdapter<User>(
        toJson: (user) => user.toJson(),
        fromJson: User.fromJson,
        keyExtractor: (user) => user.id,
        typeName: 'users',
      );
      
      AdapterFactory.register(adapter);
      
      expect(AdapterFactory.hasAdapter<User>(), isTrue);
      expect(AdapterFactory.get<User>(), equals(adapter));
      
      AdapterFactory.clear();
      expect(AdapterFactory.hasAdapter<User>(), isFalse);
    });
    
    test('Convenience adapter creation', () {
      final jsonAdapter = Adapters.json<User>(
        toJson: (user) => user.toJson(),
        fromJson: User.fromJson,
        keyExtractor: (user) => user.id,
      );
      
      expect(jsonAdapter, isA<JsonFreezedAdapter<User>>());
      
      final numericAdapter = Adapters.withNumericId<User>(
        toJson: (user) => user.toJson(),
        fromJson: User.fromJson,
        idExtractor: (user) => int.parse(user.id),
      );
      
      expect(numericAdapter, isA<JsonFreezedAdapter<User>>());
    });
    
    test('Serialization helpers', () {
      final now = DateTime.now();
      final millis = SerializationHelpers.dateTimeToMillis(now);
      final reconstructed = SerializationHelpers.millisToDateTime(millis);
      
      expect(reconstructed.millisecondsSinceEpoch, equals(now.millisecondsSinceEpoch));
      
      final duration = const Duration(hours: 1, minutes: 30);
      final micros = SerializationHelpers.durationToMicros(duration);
      final reconstructedDuration = SerializationHelpers.microsToDuration(micros);
      
      expect(reconstructedDuration, equals(duration));
      
      final strings = ['apple', 'banana', 'cherry'];
      final encoded = SerializationHelpers.encodeStringList(strings);
      final decoded = SerializationHelpers.decodeStringList(encoded);
      
      expect(decoded, equals(strings));
    });
  });
}