import 'package:test/test.dart';
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'package:lightning_db_freezed/lightning_db_freezed.dart';
import 'package:freezed_annotation/freezed_annotation.dart';
import 'dart:io';

part 'freezed_collection_test.freezed.dart';
part 'freezed_collection_test.g.dart';

// Test models
@freezed
sealed class TestUser with _$TestUser {
  const factory TestUser({
    required String id,
    required String name,
    required int age,
    DateTime? createdAt,
    Map<String, dynamic>? metadata,
  }) = _TestUser;

  factory TestUser.fromJson(Map<String, dynamic> json) => _$TestUserFromJson(json);
}

@freezed
sealed class TestState with _$TestState {
  const factory TestState.idle() = TestStateIdle;
  const factory TestState.loading(double progress) = TestStateLoading;
  const factory TestState.success(TestUser user) = TestStateSuccess;
  const factory TestState.error(String message, int? code) = TestStateError;

  factory TestState.fromJson(Map<String, dynamic> json) => _$TestStateFromJson(json);
}

void main() {
  group('FreezedCollection Tests', () {
    late String testDbPath;
    late LightningDb db;
    late FreezedCollection<TestUser> users;

    setUp(() async {
      final tempDir = await Directory.systemTemp.createTemp('freezed_test_');
      testDbPath = '${tempDir.path}/test.db';
      LightningDbInit.init();
      db = LightningDb.create(testDbPath);
      users = db.freezedCollection<TestUser>('users');
    });

    tearDown(() async {
      db.close();
      final testDir = Directory(testDbPath).parent;
      if (await testDir.exists()) {
        await testDir.delete(recursive: true);
      }
    });

    test('Basic CRUD operations', () async {
      final user = TestUser(
        id: 'user1',
        name: 'John Doe',
        age: 30,
        createdAt: DateTime.now(),
      );

      // Create
      await users.add(user);
      
      // Read
      final retrieved = await users.get(user.id);
      expect(retrieved, isNotNull);
      expect(retrieved!.id, equals(user.id));
      expect(retrieved.name, equals(user.name));
      expect(retrieved.age, equals(user.age));

      // Update
      final updated = user.copyWith(age: 31);
      await users.update(updated);
      
      final retrievedUpdated = await users.get(user.id);
      expect(retrievedUpdated!.age, equals(31));

      // Delete
      await users.delete(user.id);
      expect(await users.get(user.id), isNull);
    });

    test('Custom ID field', () async {
      // Test with custom ID extractor
      final customUsers = db.freezedCollection<TestUser>(
        'custom_users',
        getId: (user) => '${user.name}_${user.age}',
      );

      final user = TestUser(
        id: 'ignored',
        name: 'Jane',
        age: 25,
        createdAt: DateTime.now(),
      );

      await customUsers.add(user);
      
      // Should use custom ID
      expect(await customUsers.get('ignored'), isNull);
      expect(await customUsers.get('Jane_25'), isNotNull);
    });

    test('Batch operations', () async {
      final batch = List.generate(100, (i) => TestUser(
        id: 'batch_$i',
        name: 'User $i',
        age: 20 + (i % 50),
        createdAt: DateTime.now(),
      ));

      // Add all
      await users.addAll(batch);
      
      // Verify all added
      final all = await users.getAll();
      expect(all.length, equals(100));

      // Update multiple
      final updates = batch.take(50).map((u) => u.copyWith(age: u.age + 1)).toList();
      await users.updateAll(updates);

      // Delete multiple
      final idsToDelete = batch.skip(50).map((u) => u.id).toList();
      await users.deleteAll(idsToDelete);

      // Verify results
      final remaining = await users.getAll();
      expect(remaining.length, equals(50));
    });

    test('Query functionality', () async {
      // Add test data
      final testData = [
        TestUser(id: '1', name: 'Alice', age: 25, metadata: {'city': 'NYC', 'active': true}),
        TestUser(id: '2', name: 'Bob', age: 30, metadata: {'city': 'LA', 'active': true}),
        TestUser(id: '3', name: 'Charlie', age: 25, metadata: {'city': 'NYC', 'active': false}),
        TestUser(id: '4', name: 'David', age: 35, metadata: {'city': 'Chicago', 'active': true}),
        TestUser(id: '5', name: 'Eve', age: 28, metadata: {'city': 'LA', 'active': false}),
      ];

      await users.addAll(testData);

      // Simple equality query
      final age25 = await users.query()
        .where('age', isEqualTo: 25)
        .findAll();
      expect(age25.length, equals(2));

      // Greater than query
      final over28 = await users.query()
        .where('age', isGreaterThan: 28)
        .findAll();
      expect(over28.length, equals(2));

      // Multiple conditions
      final nycActive = await users.query()
        .where('metadata.city', isEqualTo: 'NYC')
        .where('metadata.active', isEqualTo: true)
        .findAll();
      expect(nycActive.length, equals(1));
      expect(nycActive.first.name, equals('Alice'));

      // Ordering
      final orderedByAge = await users.query()
        .orderBy('age')
        .findAll();
      expect(orderedByAge.first.age, equals(25));
      expect(orderedByAge.last.age, equals(35));

      // Limit and offset
      final paginated = await users.query()
        .orderBy('name')
        .limit(2)
        .offset(1)
        .findAll();
      expect(paginated.length, equals(2));
      expect(paginated.first.name, equals('Bob'));
    });

    test('Reactive streams', () async {
      final changes = <CollectionChange<TestUser>>[];
      final subscription = users.changes.listen((change) {
        changes.add(change);
      });

      // Add
      final user = TestUser(id: 'stream1', name: 'Stream User', age: 25);
      await users.add(user);

      // Update
      await users.update(user.copyWith(age: 26));

      // Delete
      await users.delete(user.id);

      // Wait for stream events
      await Future.delayed(const Duration(milliseconds: 100));

      expect(changes.length, equals(3));
      expect(changes[0].type, equals(ChangeType.added));
      expect(changes[1].type, equals(ChangeType.modified));
      expect(changes[2].type, equals(ChangeType.removed));

      await subscription.cancel();
    });

    test('Union type support', () async {
      final states = db.freezedCollection<TestState>('states');

      // Add different union states
      await states.add(const TestState.idle(), 'state1');
      await states.add(const TestState.loading(0.5), 'state2');
      await states.add(
        TestState.success(TestUser(id: 'u1', name: 'Success', age: 20)),
        'state3',
      );
      await states.add(const TestState.error('Failed', 404), 'state4');

      // Retrieve and verify
      final idle = await states.get('state1');
      expect(idle, isA<TestStateIdle>());

      final loading = await states.get('state2');
      loading!.when(
        idle: () => fail('Should not be idle'),
        loading: (progress) => expect(progress, equals(0.5)),
        success: (_) => fail('Should not be success'),
        error: (_, __) => fail('Should not be error'),
      );

      final error = await states.get('state4');
      error!.whenOrNull(
        error: (message, code) {
          expect(message, equals('Failed'));
          expect(code, equals(404));
        },
      );
    });

    test('Null handling', () async {
      final userWithNulls = TestUser(
        id: 'null_test',
        name: 'Null Test',
        age: 30,
        createdAt: null,
        metadata: null,
      );

      await users.add(userWithNulls);
      final retrieved = await users.get(userWithNulls.id);

      expect(retrieved, isNotNull);
      expect(retrieved!.createdAt, isNull);
      expect(retrieved.metadata, isNull);
    });

    test('Complex metadata queries', () async {
      final complexUsers = [
        TestUser(
          id: 'c1',
          name: 'Complex 1',
          age: 25,
          metadata: {
            'profile': {
              'bio': 'Developer',
              'skills': ['dart', 'flutter', 'rust'],
              'experience': 5,
            },
            'preferences': {
              'theme': 'dark',
              'notifications': true,
            },
          },
        ),
        TestUser(
          id: 'c2',
          name: 'Complex 2',
          age: 30,
          metadata: {
            'profile': {
              'bio': 'Designer',
              'skills': ['figma', 'sketch'],
              'experience': 8,
            },
            'preferences': {
              'theme': 'light',
              'notifications': false,
            },
          },
        ),
      ];

      await users.addAll(complexUsers);

      // Nested field query
      final darkTheme = await users.query()
        .where('metadata.preferences.theme', isEqualTo: 'dark')
        .findAll();
      expect(darkTheme.length, equals(1));
      expect(darkTheme.first.id, equals('c1'));

      // Array contains query
      final flutterDevs = await users.query()
        .where('metadata.profile.skills', contains: 'flutter')
        .findAll();
      expect(flutterDevs.length, equals(1));

      // Nested numeric comparison
      final experienced = await users.query()
        .where('metadata.profile.experience', isGreaterThan: 5)
        .findAll();
      expect(experienced.length, equals(1));
      expect(experienced.first.id, equals('c2'));
    });

    test('Transaction support', () async {
      // Successful transaction
      await db.runInTransaction(() async {
        await users.add(TestUser(id: 't1', name: 'TX User 1', age: 25));
        await users.add(TestUser(id: 't2', name: 'TX User 2', age: 30));
      });

      expect(await users.get('t1'), isNotNull);
      expect(await users.get('t2'), isNotNull);

      // Failed transaction
      try {
        await db.runInTransaction(() async {
          await users.add(TestUser(id: 't3', name: 'TX User 3', age: 35));
          throw Exception('Rollback');
        });
      } catch (_) {
        // Expected
      }

      expect(await users.get('t3'), isNull);
    });

    test('Performance with large collections', () async {
      const size = 10000;
      final stopwatch = Stopwatch()..start();

      // Bulk insert
      final largeDataset = List.generate(size, (i) => TestUser(
        id: 'perf_$i',
        name: 'User $i',
        age: 20 + (i % 60),
        createdAt: DateTime.now(),
        metadata: {'index': i, 'even': i % 2 == 0},
      ));

      await users.addAll(largeDataset);
      final insertTime = stopwatch.elapsedMilliseconds;
      print('Inserted $size records in ${insertTime}ms');

      // Query performance
      stopwatch.reset();
      final queryResult = await users.query()
        .where('age', isGreaterThan: 50)
        .where('metadata.even', isEqualTo: true)
        .findAll();
      final queryTime = stopwatch.elapsedMilliseconds;
      print('Query returned ${queryResult.length} results in ${queryTime}ms');

      expect(insertTime, lessThan(30000)); // 30 seconds max
      expect(queryTime, lessThan(1000)); // 1 second max
    });

    test('Custom adapter', () async {
      var serializeCalls = 0;
      var deserializeCalls = 0;

      final customAdapter = _TestUserAdapter(
        onSerialize: () => serializeCalls++,
        onDeserialize: () => deserializeCalls++,
      );

      final customUsers = db.freezedCollection<TestUser>(
        'custom_adapter_users',
        adapter: customAdapter,
      );

      final user = TestUser(id: 'custom1', name: 'Custom', age: 25);
      await customUsers.add(user);
      await customUsers.get(user.id);

      expect(serializeCalls, equals(1));
      expect(deserializeCalls, equals(1));
    });

    test('Collection naming', () async {
      // Different collections should be isolated
      final users1 = db.freezedCollection<TestUser>('users1');
      final users2 = db.freezedCollection<TestUser>('users2');

      final user1 = TestUser(id: 'same_id', name: 'User 1', age: 25);
      final user2 = TestUser(id: 'same_id', name: 'User 2', age: 30);

      await users1.add(user1);
      await users2.add(user2);

      final retrieved1 = await users1.get('same_id');
      final retrieved2 = await users2.get('same_id');

      expect(retrieved1!.name, equals('User 1'));
      expect(retrieved2!.name, equals('User 2'));
    });
  });
}

class _TestUserAdapter extends FreezedAdapter<TestUser> {
  final VoidCallback onSerialize;
  final VoidCallback onDeserialize;

  _TestUserAdapter({
    required this.onSerialize,
    required this.onDeserialize,
  });

  @override
  Map<String, dynamic> toJson(TestUser model) {
    onSerialize();
    return model.toJson();
  }

  @override
  TestUser fromJson(Map<String, dynamic> json) {
    onDeserialize();
    return TestUser.fromJson(json);
  }

  @override
  String getId(TestUser model) => model.id;
}