import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:lightning_db/lightning_db.dart';
import 'package:lightning_db_example/models/user_model.dart';
import 'package:lightning_db_example/models/post_model.dart';
import 'package:lightning_db_example/models/comment_model.dart';
import 'dart:io';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Freezed Integration Tests', () {
    late String testDbPath;
    late LightningDb db;

    setUp(() async {
      final tempDir = await Directory.systemTemp.createTemp('freezed_test_');
      testDbPath = '${tempDir.path}/test.db';
      db = await LightningDb.open(testDbPath);
    });

    tearDown(() async {
      if (db.isOpen) {
        await db.close();
      }
      final testDir = Directory(testDbPath).parent;
      if (await testDir.exists()) {
        await testDir.delete(recursive: true);
      }
    });

    test('Freezed model serialization and deserialization', () async {
      final users = db.freezedCollection<User>('users');

      final user = User(
        id: 'freezed_user_1',
        name: 'John Doe',
        email: 'john@example.com',
        age: 30,
        createdAt: DateTime.now(),
        metadata: {
          'role': 'admin',
          'verified': true,
          'score': 95.5,
        },
      );

      // Save user
      await users.add(user);

      // Retrieve user
      final retrieved = await users.get(user.id);
      expect(retrieved, isNotNull);
      expect(retrieved!.id, equals(user.id));
      expect(retrieved.name, equals(user.name));
      expect(retrieved.metadata?['role'], equals('admin'));
      expect(retrieved.metadata?['verified'], equals(true));
      expect(retrieved.metadata?['score'], equals(95.5));
    });

    test('Freezed union types', () async {
      final states = db.freezedCollection<UserState>('user_states');

      // Test different union states
      final loadingState = const UserState.loading();
      final dataState = UserState.data(
        User(
          id: 'union_user',
          name: 'Union User',
          email: 'union@example.com',
          age: 25,
          createdAt: DateTime.now(),
        ),
      );
      const errorState = UserState.error('Connection failed');

      // Save states
      await states.add(loadingState, 'state_1');
      await states.add(dataState, 'state_2');
      await states.add(errorState, 'state_3');

      // Retrieve and verify states
      final retrievedLoading = await states.get('state_1');
      expect(retrievedLoading, equals(loadingState));

      final retrievedData = await states.get('state_2');
      expect(retrievedData, isA<UserStateData>());
      retrievedData!.when(
        loading: () => fail('Should not be loading'),
        data: (user) => expect(user.name, equals('Union User')),
        error: (_) => fail('Should not be error'),
      );

      final retrievedError = await states.get('state_3');
      expect(retrievedError, equals(errorState));
    });

    test('Freezed nullable fields', () async {
      final users = db.freezedCollection<User>('users');

      // User with null fields
      final userWithNulls = User(
        id: 'null_user',
        name: 'Null Test',
        email: 'null@example.com',
        age: null,
        createdAt: DateTime.now(),
        metadata: null,
      );

      await users.add(userWithNulls);
      final retrieved = await users.get(userWithNulls.id);

      expect(retrieved, isNotNull);
      expect(retrieved!.age, isNull);
      expect(retrieved.metadata, isNull);
    });

    test('Freezed nested models', () async {
      final posts = db.freezedCollection<Post>('posts');
      final comments = db.freezedCollection<Comment>('comments');

      // Create post with nested data
      final post = Post(
        id: 'nested_post',
        title: 'Nested Models Test',
        content: 'Testing nested freezed models',
        authorId: 'author_1',
        createdAt: DateTime.now(),
        tags: ['test', 'freezed', 'nested'],
        metadata: {
          'views': 100,
          'likes': 50,
          'shares': 10,
        },
      );

      await posts.add(post);

      // Create comments for the post
      final comment1 = Comment(
        id: 'comment_1',
        postId: post.id,
        authorId: 'commenter_1',
        content: 'Great post!',
        createdAt: DateTime.now(),
        replies: [],
      );

      final comment2 = Comment(
        id: 'comment_2',
        postId: post.id,
        authorId: 'commenter_2',
        content: 'Thanks for sharing',
        createdAt: DateTime.now(),
        replies: [
          Comment(
            id: 'reply_1',
            postId: post.id,
            authorId: post.authorId,
            content: 'You\'re welcome!',
            createdAt: DateTime.now(),
            replies: [],
          ),
        ],
      );

      await comments.add(comment1);
      await comments.add(comment2);

      // Retrieve and verify
      final retrievedPost = await posts.get(post.id);
      expect(retrievedPost!.tags, equals(['test', 'freezed', 'nested']));
      expect(retrievedPost.metadata?['views'], equals(100));

      final retrievedComment2 = await comments.get(comment2.id);
      expect(retrievedComment2!.replies.length, equals(1));
      expect(retrievedComment2.replies.first.content, equals('You\'re welcome!'));
    });

    test('Freezed copyWith functionality', () async {
      final users = db.freezedCollection<User>('users');

      final original = User(
        id: 'copy_user',
        name: 'Original Name',
        email: 'original@example.com',
        age: 25,
        createdAt: DateTime.now(),
      );

      await users.add(original);

      // Update using copyWith
      final updated = original.copyWith(
        name: 'Updated Name',
        age: 26,
        metadata: {'updated': true},
      );

      await users.update(updated);

      final retrieved = await users.get(original.id);
      expect(retrieved!.name, equals('Updated Name'));
      expect(retrieved.age, equals(26));
      expect(retrieved.email, equals('original@example.com')); // Unchanged
      expect(retrieved.metadata?['updated'], equals(true));
    });

    test('Freezed list operations', () async {
      final users = db.freezedCollection<User>('users');

      // Create multiple users
      final userList = List.generate(50, (i) => User(
        id: 'list_user_$i',
        name: 'User $i',
        email: 'user$i@example.com',
        age: 20 + (i % 30),
        createdAt: DateTime.now().subtract(Duration(days: i)),
        metadata: {
          'group': i < 25 ? 'A' : 'B',
          'score': i * 2.0,
        },
      ));

      // Batch add
      await Future.wait(userList.map((user) => users.add(user)));

      // Query with complex conditions
      final groupA = await users.query()
        .where('metadata.group', isEqualTo: 'A')
        .findAll();
      expect(groupA.length, equals(25));

      final highScorers = await users.query()
        .where('metadata.score', isGreaterThan: 50)
        .orderBy('metadata.score', descending: true)
        .limit(10)
        .findAll();
      expect(highScorers.length, equals(10));
      expect(highScorers.first.metadata?['score'], greaterThan(50));
    });

    test('Freezed reactive streams', () async {
      final users = db.freezedCollection<User>('users');

      // Set up stream listener
      final changes = <CollectionChange<User>>[];
      final subscription = users.changes.listen((change) {
        changes.add(change);
      });

      // Add user
      final user1 = User(
        id: 'stream_user_1',
        name: 'Stream User 1',
        email: 'stream1@example.com',
        age: 25,
        createdAt: DateTime.now(),
      );
      await users.add(user1);

      // Update user
      final updated = user1.copyWith(age: 26);
      await users.update(updated);

      // Delete user
      await users.delete(user1.id);

      // Allow time for stream events
      await Future.delayed(const Duration(milliseconds: 100));

      // Verify stream events
      expect(changes.length, equals(3));
      expect(changes[0].type, equals(ChangeType.added));
      expect(changes[0].document.id, equals(user1.id));
      expect(changes[1].type, equals(ChangeType.modified));
      expect(changes[1].document.age, equals(26));
      expect(changes[2].type, equals(ChangeType.removed));
      expect(changes[2].document.id, equals(user1.id));

      await subscription.cancel();
    });

    test('Freezed transaction atomicity', () async {
      final users = db.freezedCollection<User>('users');
      final posts = db.freezedCollection<Post>('posts');

      // Create user and posts in transaction
      final user = User(
        id: 'tx_user',
        name: 'Transaction User',
        email: 'tx@example.com',
        age: 30,
        createdAt: DateTime.now(),
      );

      final userPosts = List.generate(3, (i) => Post(
        id: 'tx_post_$i',
        title: 'Post $i',
        content: 'Transaction post content',
        authorId: user.id,
        createdAt: DateTime.now(),
        tags: ['transaction'],
      ));

      // Successful transaction
      await db.transaction((tx) async {
        await tx.freezedCollection<User>('users').add(user);
        for (final post in userPosts) {
          await tx.freezedCollection<Post>('posts').add(post);
        }
      });

      // Verify all data was added
      expect(await users.get(user.id), isNotNull);
      expect((await posts.getAll()).length, equals(3));

      // Failed transaction with partial operations
      try {
        await db.transaction((tx) async {
          // Try to add duplicate user (should fail)
          await tx.freezedCollection<User>('users').add(user);
          
          // This shouldn't be committed
          await tx.freezedCollection<Post>('posts').add(Post(
            id: 'failed_post',
            title: 'Should not exist',
            content: 'This post should be rolled back',
            authorId: user.id,
            createdAt: DateTime.now(),
            tags: [],
          ));
        });
      } catch (e) {
        // Expected failure
      }

      // Verify failed transaction didn't add the post
      expect(await posts.get('failed_post'), isNull);
      expect((await posts.getAll()).length, equals(3)); // Still only 3 posts
    });

    test('Freezed performance with large datasets', () async {
      final users = db.freezedCollection<User>('users');

      const batchSize = 1000;
      final stopwatch = Stopwatch()..start();

      // Batch insert
      final batch = List.generate(batchSize, (i) => User(
        id: 'perf_user_$i',
        name: 'Performance User $i',
        email: 'perf$i@example.com',
        age: 20 + (i % 50),
        createdAt: DateTime.now(),
        metadata: {
          'index': i,
          'even': i % 2 == 0,
          'group': String.fromCharCode(65 + (i % 26)), // A-Z
        },
      ));

      await Future.wait(batch.map((user) => users.add(user)));
      
      final insertTime = stopwatch.elapsedMilliseconds;
      print('Inserted $batchSize Freezed models in ${insertTime}ms');
      expect(insertTime, lessThan(10000)); // Should complete within 10 seconds

      // Complex query performance
      stopwatch.reset();
      
      final results = await users.query()
        .where('metadata.even', isEqualTo: true)
        .where('age', isGreaterThan: 30)
        .orderBy('metadata.group')
        .findAll();
      
      final queryTime = stopwatch.elapsedMilliseconds;
      print('Complex query returned ${results.length} results in ${queryTime}ms');
      expect(queryTime, lessThan(1000)); // Query should complete within 1 second
    });

    test('Freezed custom adapter', () async {
      // Test custom adapter functionality
      final customAdapter = CustomUserAdapter();
      final users = db.freezedCollection<User>('custom_users', adapter: customAdapter);

      final user = User(
        id: 'custom_user',
        name: 'Custom Adapter Test',
        email: 'custom@example.com',
        age: 25,
        createdAt: DateTime.now(),
      );

      await users.add(user);
      
      final retrieved = await users.get(user.id);
      expect(retrieved, isNotNull);
      expect(customAdapter.serializeCallCount, equals(1));
      expect(customAdapter.deserializeCallCount, equals(1));
    });
  });
}

// Custom adapter for testing
class CustomUserAdapter extends FreezedAdapter<User> {
  int serializeCallCount = 0;
  int deserializeCallCount = 0;

  @override
  Map<String, dynamic> toJson(User model) {
    serializeCallCount++;
    return model.toJson();
  }

  @override
  User fromJson(Map<String, dynamic> json) {
    deserializeCallCount++;
    return User.fromJson(json);
  }

  @override
  String getId(User model) => model.id;
}