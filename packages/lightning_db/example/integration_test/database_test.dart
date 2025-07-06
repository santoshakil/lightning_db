import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:lightning_db/lightning_db.dart';
import 'package:lightning_db_example/models/user_model.dart';
import 'package:lightning_db_example/models/post_model.dart';
import 'dart:io';
import 'dart:math';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Lightning DB Integration Tests', () {
    late String testDbPath;
    late LightningDb db;

    setUp(() async {
      // Create unique test database path
      final tempDir = await Directory.systemTemp.createTemp('lightning_test_');
      testDbPath = '${tempDir.path}/test.db';
    });

    tearDown(() async {
      // Close database if open
      if (db.isOpen) {
        await db.close();
      }
      
      // Clean up test directory
      final testDir = Directory(testDbPath).parent;
      if (await testDir.exists()) {
        await testDir.delete(recursive: true);
      }
    });

    test('Database creation and lifecycle', () async {
      // Create database
      db = await LightningDb.open(testDbPath);
      expect(db.isOpen, isTrue);
      expect(db.path, equals(testDbPath));

      // Close database
      await db.close();
      expect(db.isOpen, isFalse);

      // Reopen database
      db = await LightningDb.open(testDbPath);
      expect(db.isOpen, isTrue);

      // Verify database file exists
      final dbFile = File(testDbPath);
      expect(await dbFile.exists(), isTrue);
    });

    test('Basic CRUD operations', () async {
      db = await LightningDb.open(testDbPath);

      // Create
      const key = 'test_key';
      const value = 'test_value';
      await db.put(key, value);

      // Read
      final retrieved = await db.get(key);
      expect(retrieved, equals(value));

      // Update
      const newValue = 'updated_value';
      await db.put(key, newValue);
      final updated = await db.get(key);
      expect(updated, equals(newValue));

      // Delete
      await db.delete(key);
      final deleted = await db.get(key);
      expect(deleted, isNull);
    });

    test('Freezed model integration', () async {
      db = await LightningDb.open(testDbPath);
      final users = db.freezedCollection<User>('users');

      // Create user
      final user = User(
        id: 'user_1',
        name: 'John Doe',
        email: 'john@example.com',
        age: 30,
        createdAt: DateTime.now(),
      );

      // Add user
      await users.add(user);

      // Get user
      final retrieved = await users.get(user.id);
      expect(retrieved, isNotNull);
      expect(retrieved!.id, equals(user.id));
      expect(retrieved.name, equals(user.name));
      expect(retrieved.email, equals(user.email));
      expect(retrieved.age, equals(user.age));

      // Update user
      final updated = user.copyWith(age: 31);
      await users.update(updated);
      
      final retrievedUpdated = await users.get(user.id);
      expect(retrievedUpdated!.age, equals(31));

      // Delete user
      await users.delete(user.id);
      final deleted = await users.get(user.id);
      expect(deleted, isNull);
    });

    test('Transaction support', () async {
      db = await LightningDb.open(testDbPath);
      final users = db.freezedCollection<User>('users');

      // Create test users
      final user1 = User(
        id: 'tx_user_1',
        name: 'Alice',
        email: 'alice@example.com',
        age: 25,
        createdAt: DateTime.now(),
      );

      final user2 = User(
        id: 'tx_user_2',
        name: 'Bob',
        email: 'bob@example.com',
        age: 28,
        createdAt: DateTime.now(),
      );

      // Successful transaction
      await db.transaction((tx) async {
        await tx.freezedCollection<User>('users').add(user1);
        await tx.freezedCollection<User>('users').add(user2);
      });

      // Verify both users were added
      expect(await users.get(user1.id), isNotNull);
      expect(await users.get(user2.id), isNotNull);

      // Failed transaction (should rollback)
      try {
        await db.transaction((tx) async {
          final user3 = User(
            id: 'tx_user_3',
            name: 'Charlie',
            email: 'charlie@example.com',
            age: 30,
            createdAt: DateTime.now(),
          );
          
          await tx.freezedCollection<User>('users').add(user3);
          
          // Simulate error
          throw Exception('Transaction error');
        });
      } catch (e) {
        // Expected error
      }

      // Verify user3 was not added (transaction rolled back)
      expect(await users.get('tx_user_3'), isNull);
    });

    test('Query capabilities', () async {
      db = await LightningDb.open(testDbPath);
      final users = db.freezedCollection<User>('users');

      // Add multiple users
      final testUsers = List.generate(10, (i) => User(
        id: 'query_user_$i',
        name: 'User $i',
        email: 'user$i@example.com',
        age: 20 + i,
        createdAt: DateTime.now().subtract(Duration(days: i)),
      ));

      for (final user in testUsers) {
        await users.add(user);
      }

      // Query all users
      final allUsers = await users.getAll();
      expect(allUsers.length, equals(10));

      // Query with filter
      final adults = await users.query()
        .where('age', isGreaterThan: 25)
        .findAll();
      expect(adults.length, equals(5));

      // Query with ordering
      final sortedByAge = await users.query()
        .orderBy('age', descending: true)
        .findAll();
      expect(sortedByAge.first.age, equals(29));
      expect(sortedByAge.last.age, equals(20));

      // Query with limit
      final topThree = await users.query()
        .orderBy('age', descending: true)
        .limit(3)
        .findAll();
      expect(topThree.length, equals(3));
    });

    test('Performance characteristics', () async {
      db = await LightningDb.open(testDbPath);
      final users = db.freezedCollection<User>('users');

      const recordCount = 1000;
      final random = Random();

      // Measure write performance
      final writeStart = DateTime.now();
      
      for (int i = 0; i < recordCount; i++) {
        final user = User(
          id: 'perf_user_$i',
          name: 'User $i',
          email: 'user$i@example.com',
          age: 20 + random.nextInt(50),
          createdAt: DateTime.now(),
        );
        await users.add(user);
      }
      
      final writeEnd = DateTime.now();
      final writeDuration = writeEnd.difference(writeStart);
      final writesPerSecond = recordCount / writeDuration.inMilliseconds * 1000;
      
      print('Write performance: ${writesPerSecond.toStringAsFixed(2)} records/second');
      expect(writesPerSecond, greaterThan(100)); // At least 100 writes/second

      // Measure read performance
      final readStart = DateTime.now();
      
      for (int i = 0; i < recordCount; i++) {
        await users.get('perf_user_$i');
      }
      
      final readEnd = DateTime.now();
      final readDuration = readEnd.difference(readStart);
      final readsPerSecond = recordCount / readDuration.inMilliseconds * 1000;
      
      print('Read performance: ${readsPerSecond.toStringAsFixed(2)} records/second');
      expect(readsPerSecond, greaterThan(1000)); // At least 1000 reads/second
    });

    test('Error handling', () async {
      db = await LightningDb.open(testDbPath);

      // Test invalid key
      expect(() async => await db.put('', 'value'), throwsA(isA<ArgumentError>()));

      // Test database closed error
      await db.close();
      expect(() async => await db.get('key'), throwsA(isA<StateError>()));

      // Test invalid path
      expect(
        () async => await LightningDb.open('/invalid/path/to/db'),
        throwsA(isA<FileSystemException>()),
      );
    });

    test('Multi-threading safety', () async {
      db = await LightningDb.open(testDbPath);
      
      const concurrentOperations = 100;
      final futures = <Future>[];

      // Concurrent writes
      for (int i = 0; i < concurrentOperations; i++) {
        futures.add(
          db.put('concurrent_key_$i', 'value_$i'),
        );
      }

      await Future.wait(futures);

      // Verify all writes succeeded
      for (int i = 0; i < concurrentOperations; i++) {
        final value = await db.get('concurrent_key_$i');
        expect(value, equals('value_$i'));
      }

      // Concurrent reads
      futures.clear();
      for (int i = 0; i < concurrentOperations; i++) {
        futures.add(
          db.get('concurrent_key_$i').then((value) {
            expect(value, equals('value_$i'));
          }),
        );
      }

      await Future.wait(futures);
    });

    test('Large data handling', () async {
      db = await LightningDb.open(testDbPath);

      // Create large value (1MB)
      final largeValue = List.generate(1024 * 1024, (i) => i % 256).join();
      const key = 'large_data_key';

      // Write large value
      await db.put(key, largeValue);

      // Read large value
      final retrieved = await db.get(key);
      expect(retrieved, equals(largeValue));

      // Update with even larger value (5MB)
      final veryLargeValue = List.generate(5 * 1024 * 1024, (i) => i % 256).join();
      await db.put(key, veryLargeValue);

      final retrievedLarge = await db.get(key);
      expect(retrievedLarge, equals(veryLargeValue));
    });

    test('Complex model relationships', () async {
      db = await LightningDb.open(testDbPath);
      
      final users = db.freezedCollection<User>('users');
      final posts = db.freezedCollection<Post>('posts');

      // Create user
      final user = User(
        id: 'rel_user_1',
        name: 'Author',
        email: 'author@example.com',
        age: 30,
        createdAt: DateTime.now(),
      );
      await users.add(user);

      // Create posts for user
      final userPosts = List.generate(5, (i) => Post(
        id: 'rel_post_$i',
        title: 'Post $i',
        content: 'Content for post $i',
        authorId: user.id,
        createdAt: DateTime.now(),
        tags: ['tag$i', 'common'],
      ));

      for (final post in userPosts) {
        await posts.add(post);
      }

      // Query posts by author
      final authorPosts = await posts.query()
        .where('authorId', isEqualTo: user.id)
        .findAll();
      expect(authorPosts.length, equals(5));

      // Query posts by tag
      final commonTagPosts = await posts.query()
        .where('tags', contains: 'common')
        .findAll();
      expect(commonTagPosts.length, equals(5));
    });

    test('Database statistics', () async {
      db = await LightningDb.open(testDbPath);
      
      // Add test data
      for (int i = 0; i < 100; i++) {
        await db.put('stat_key_$i', 'value_$i');
      }

      // Get statistics
      final stats = await db.getStatistics();
      expect(stats.keyCount, equals(100));
      expect(stats.diskSize, greaterThan(0));
      expect(stats.memoryUsage, greaterThan(0));
    });

    test('Backup and restore', () async {
      db = await LightningDb.open(testDbPath);
      
      // Add test data
      await db.put('backup_key', 'backup_value');
      
      // Create backup
      final backupDir = await Directory.systemTemp.createTemp('backup_');
      final backupPath = '${backupDir.path}/backup.db';
      await db.backup(backupPath);
      
      // Close original database
      await db.close();
      
      // Open backup
      final backupDb = await LightningDb.open(backupPath);
      final value = await backupDb.get('backup_key');
      expect(value, equals('backup_value'));
      
      await backupDb.close();
      await backupDir.delete(recursive: true);
    });
  });
}