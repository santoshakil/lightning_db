import 'package:test/test.dart';
import 'package:lightning_db_freezed/lightning_db_freezed.dart';

// Simple model for testing without Freezed generation
class TestModel {
  final String id;
  final String name;
  final int value;
  
  TestModel({
    required this.id,
    required this.name,
    required this.value,
  });
  
  Map<String, dynamic> toJson() => {
    'id': id,
    'name': name,
    'value': value,
  };
  
  factory TestModel.fromJson(Map<String, dynamic> json) => TestModel(
    id: json['id'] as String,
    name: json['name'] as String,
    value: json['value'] as int,
  );
  
  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is TestModel &&
          runtimeType == other.runtimeType &&
          id == other.id &&
          name == other.name &&
          value == other.value;
  
  @override
  int get hashCode => Object.hash(id, name, value);
}

void main() {
  group('Lightning DB Freezed Basic Tests', () {
    test('Package exports are available', () {
      // Verify core types
      expect(FreezedAdapter, isNotNull);
      expect(SimpleFreezedAdapter, isNotNull);
      expect(JsonFreezedAdapter, isNotNull);
      expect(CustomFreezedAdapter, isNotNull);
      expect(FreezedCollection, isNotNull);
      
      // Verify query types
      expect(QueryBuilder, isNotNull);
      expect(PaginatedResults, isNotNull);
      
      // Verify reactive types
      expect(ReactiveCollection, isNotNull);
      expect(ReactiveQuery, isNotNull);
      expect(CollectionEvent, isNotNull);
      
      // Verify serialization types
      expect(SerializationFormat, isNotNull);
      expect(Serializer, isNotNull);
      expect(JsonSerializer, isNotNull);
      
      // Verify helper classes
      expect(AdapterFactory, isNotNull);
      expect(Adapters, isNotNull);
      expect(SerializationHelpers, isNotNull);
    });
    
    test('JSON adapter functionality', () {
      final adapter = JsonFreezedAdapter<TestModel>(
        toJson: (model) => model.toJson(),
        fromJson: TestModel.fromJson,
        keyExtractor: (model) => model.id,
        typeName: 'test_models',
      );
      
      final model = TestModel(
        id: 'test-1',
        name: 'Test Model',
        value: 42,
      );
      
      // Test key extraction
      expect(adapter.getKey(model), equals('test-1'));
      expect(adapter.typeName, equals('test_models'));
      expect(adapter.createCompoundKey(model), equals('test_models:test-1'));
      
      // Test serialization
      final serialized = adapter.serialize(model);
      expect(serialized, isNotEmpty);
      
      // Test deserialization
      final deserialized = adapter.deserialize(serialized);
      expect(deserialized, equals(model));
    });
    
    test('Adapter factory registration', () {
      final adapter = Adapters.json<TestModel>(
        toJson: (model) => model.toJson(),
        fromJson: TestModel.fromJson,
        keyExtractor: (model) => model.id,
        typeName: 'test_models',
      );
      
      // Register adapter
      AdapterFactory.register(adapter);
      
      // Verify registration
      expect(AdapterFactory.hasAdapter<TestModel>(), isTrue);
      expect(AdapterFactory.get<TestModel>(), isNotNull);
      expect(AdapterFactory.get<TestModel>(), equals(adapter));
      
      // Clear and verify
      AdapterFactory.clear();
      expect(AdapterFactory.hasAdapter<TestModel>(), isFalse);
      expect(AdapterFactory.get<TestModel>(), isNull);
    });
    
    test('Compound key extraction', () {
      const compoundKey = 'users:123';
      
      expect(FreezedAdapter.extractType(compoundKey), equals('users'));
      expect(FreezedAdapter.extractKey(compoundKey), equals('123'));
      
      // Test with no separator
      const simpleKey = 'noseparator';
      expect(FreezedAdapter.extractType(simpleKey), equals(''));
      expect(FreezedAdapter.extractKey(simpleKey), equals('noseparator'));
    });
    
    test('Serialization helpers', () {
      // DateTime conversion
      final now = DateTime.now();
      final millis = SerializationHelpers.dateTimeToMillis(now);
      final reconstructed = SerializationHelpers.millisToDateTime(millis);
      expect(
        reconstructed.millisecondsSinceEpoch,
        equals(now.millisecondsSinceEpoch),
      );
      
      // Duration conversion
      const duration = Duration(hours: 2, minutes: 30, seconds: 45);
      final micros = SerializationHelpers.durationToMicros(duration);
      final reconstructedDuration = SerializationHelpers.microsToDuration(micros);
      expect(reconstructedDuration, equals(duration));
      
      // String list encoding
      final strings = ['apple', 'banana', 'cherry'];
      final encoded = SerializationHelpers.encodeStringList(strings);
      final decoded = SerializationHelpers.decodeStringList(encoded);
      expect(decoded, equals(strings));
      
      // Empty string list
      final emptyEncoded = SerializationHelpers.encodeStringList([]);
      final emptyDecoded = SerializationHelpers.decodeStringList('');
      expect(emptyDecoded, isEmpty);
    });
    
    test('Change types enum', () {
      expect(ChangeType.values, hasLength(4));
      expect(ChangeType.values, contains(ChangeType.insert));
      expect(ChangeType.values, contains(ChangeType.update));
      expect(ChangeType.values, contains(ChangeType.delete));
      expect(ChangeType.values, contains(ChangeType.clear));
    });
    
    test('Collection event types', () {
      final insertEvent = CollectionEvent<TestModel>.inserted(
        TestModel(id: '1', name: 'Test', value: 1),
      );
      expect(insertEvent.type, equals(EventType.inserted));
      expect(insertEvent.item, isNotNull);
      
      final updateEvent = CollectionEvent<TestModel>.updated(
        TestModel(id: '1', name: 'Old', value: 1),
        TestModel(id: '1', name: 'New', value: 2),
      );
      expect(updateEvent.type, equals(EventType.updated));
      expect(updateEvent.oldItem, isNotNull);
      expect(updateEvent.newItem, isNotNull);
      
      final deleteEvent = CollectionEvent<TestModel>.deleted(
        TestModel(id: '1', name: 'Test', value: 1),
      );
      expect(deleteEvent.type, equals(EventType.deleted));
      expect(deleteEvent.item, isNotNull);
      
      final clearEvent = CollectionEvent<TestModel>.cleared();
      expect(clearEvent.type, equals(EventType.cleared));
    });
  });
}