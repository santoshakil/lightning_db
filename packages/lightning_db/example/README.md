# Lightning DB Example App

A comprehensive example demonstrating all features of Lightning DB for Flutter.

## Overview

This example app showcases:
- Basic CRUD operations
- Freezed model integration
- Complex queries
- Real-time updates
- Transactions
- Performance benchmarks
- Platform-specific features

## Getting Started

### 1. Install Dependencies

```bash
flutter pub get
```

### 2. Generate Freezed Models

```bash
dart run build_runner build --delete-conflicting-outputs
```

### 3. Run the App

```bash
# iOS
flutter run -d ios

# Android
flutter run -d android

# macOS
flutter run -d macos

# Linux
flutter run -d linux

# Windows
flutter run -d windows
```

## Features Demonstrated

### 1. User Management

```dart
// Models
@freezed
class User with _$User {
  const factory User({
    required String id,
    required String name,
    required String email,
    int? age,
    required DateTime createdAt,
    Map<String, dynamic>? metadata,
  }) = _User;
}

// Usage
final users = db.freezedCollection<User>('users');
await users.add(User(...));
```

### 2. Post Management with Relationships

```dart
@freezed
class Post with _$Post {
  const factory Post({
    required String id,
    required String title,
    required String content,
    required String authorId,
    required DateTime createdAt,
    required List<String> tags,
    Map<String, dynamic>? metadata,
  }) = _Post;
}

// Query posts by author
final userPosts = await posts.query()
  .where('authorId', isEqualTo: userId)
  .orderBy('createdAt', descending: true)
  .findAll();
```

### 3. Real-time Comments

```dart
@freezed
class Comment with _$Comment {
  const factory Comment({
    required String id,
    required String postId,
    required String authorId,
    required String content,
    required DateTime createdAt,
    required List<Comment> replies,
  }) = _Comment;
}

// Listen to comments on a post
comments.query()
  .where('postId', isEqualTo: postId)
  .snapshots()
  .listen((comments) {
    setState(() {
      _comments = comments;
    });
  });
```

### 4. State Management

```dart
@freezed
class UserState with _$UserState {
  const factory UserState.loading() = UserStateLoading;
  const factory UserState.data(User user) = UserStateData;
  const factory UserState.error(String message) = UserStateError;
}

// Handle different states
userState.when(
  loading: () => CircularProgressIndicator(),
  data: (user) => UserProfile(user: user),
  error: (message) => ErrorWidget(message: message),
);
```

## Code Examples

### Basic Operations Screen

Shows basic CRUD operations with a simple key-value interface.

```dart
class BasicOperationsScreen extends StatelessWidget {
  Future<void> _demonstrateBasicOps() async {
    final db = await LightningDb.open('example.db');
    
    // String operations
    await db.put('greeting', 'Hello, Lightning DB!');
    final greeting = await db.get('greeting');
    
    // JSON operations
    await db.putJson('config', {
      'theme': 'dark',
      'language': 'en',
      'notifications': true,
    });
    final config = await db.getJson('config');
    
    // Binary operations
    final imageBytes = await rootBundle.load('assets/image.png');
    await db.putBytes('image', imageBytes.buffer.asUint8List());
    final retrieved = await db.getBytes('image');
  }
}
```

### User Management Screen

Demonstrates Freezed collection usage with full CRUD operations.

```dart
class UserManagementScreen extends StatefulWidget {
  @override
  _UserManagementScreenState createState() => _UserManagementScreenState();
}

class _UserManagementScreenState extends State<UserManagementScreen> {
  late FreezedCollection<User> users;
  List<User> _users = [];
  StreamSubscription? _subscription;

  @override
  void initState() {
    super.initState();
    _initializeCollection();
  }

  Future<void> _initializeCollection() async {
    final db = await LightningDb.open('example.db');
    users = db.freezedCollection<User>('users');
    
    // Listen to changes
    _subscription = users.changes.listen((change) {
      _loadUsers();
    });
    
    _loadUsers();
  }

  Future<void> _loadUsers() async {
    final allUsers = await users.getAll();
    setState(() {
      _users = allUsers;
    });
  }

  Future<void> _addUser() async {
    final user = User(
      id: DateTime.now().millisecondsSinceEpoch.toString(),
      name: _nameController.text,
      email: _emailController.text,
      age: int.tryParse(_ageController.text),
      createdAt: DateTime.now(),
    );
    
    await users.add(user);
  }

  @override
  void dispose() {
    _subscription?.cancel();
    super.dispose();
  }
}
```

### Query Builder Screen

Shows advanced query capabilities.

```dart
class QueryBuilderScreen extends StatelessWidget {
  Future<List<User>> _executeQuery() async {
    return await users.query()
      .where('age', isGreaterThan: _minAge)
      .where('age', isLessThan: _maxAge)
      .where('metadata.active', isEqualTo: true)
      .where('email', contains: _emailFilter)
      .orderBy(_sortField, descending: _sortDescending)
      .limit(_limit)
      .offset(_offset)
      .findAll();
  }
}
```

### Performance Benchmark Screen

Demonstrates performance testing.

```dart
class BenchmarkScreen extends StatelessWidget {
  Future<BenchmarkResult> _runBenchmark() async {
    final db = await LightningDb.open('benchmark.db');
    final collection = db.freezedCollection<User>('benchmark_users');
    
    // Write benchmark
    final writeStart = DateTime.now();
    final users = List.generate(10000, (i) => User(
      id: 'user_$i',
      name: 'User $i',
      email: 'user$i@example.com',
      age: 20 + (i % 60),
      createdAt: DateTime.now(),
    ));
    
    await collection.addAll(users);
    final writeEnd = DateTime.now();
    final writeDuration = writeEnd.difference(writeStart);
    
    // Read benchmark
    final readStart = DateTime.now();
    for (int i = 0; i < 10000; i++) {
      await collection.get('user_$i');
    }
    final readEnd = DateTime.now();
    final readDuration = readEnd.difference(readStart);
    
    // Query benchmark
    final queryStart = DateTime.now();
    final results = await collection.query()
      .where('age', isGreaterThan: 40)
      .where('email', contains: '@example.com')
      .findAll();
    final queryEnd = DateTime.now();
    final queryDuration = queryEnd.difference(queryStart);
    
    return BenchmarkResult(
      writeOpsPerSecond: 10000 / writeDuration.inMilliseconds * 1000,
      readOpsPerSecond: 10000 / readDuration.inMilliseconds * 1000,
      queryTimeMs: queryDuration.inMilliseconds,
      resultCount: results.length,
    );
  }
}
```

### Transaction Demo Screen

Shows transaction usage for data consistency.

```dart
class TransactionDemoScreen extends StatelessWidget {
  Future<void> _transferCredits(String fromUserId, String toUserId, int amount) async {
    await db.transaction((tx) async {
      final users = tx.freezedCollection<User>('users');
      final transactions = tx.freezedCollection<Transaction>('transactions');
      
      // Get both users
      final fromUser = await users.get(fromUserId);
      final toUser = await users.get(toUserId);
      
      if (fromUser == null || toUser == null) {
        throw Exception('User not found');
      }
      
      final fromCredits = fromUser.metadata?['credits'] ?? 0;
      if (fromCredits < amount) {
        throw Exception('Insufficient credits');
      }
      
      // Update users
      await users.update(fromUser.copyWith(
        metadata: {...?fromUser.metadata, 'credits': fromCredits - amount},
      ));
      
      await users.update(toUser.copyWith(
        metadata: {...?toUser.metadata, 'credits': (toUser.metadata?['credits'] ?? 0) + amount},
      ));
      
      // Record transaction
      await transactions.add(Transaction(
        id: DateTime.now().millisecondsSinceEpoch.toString(),
        fromUserId: fromUserId,
        toUserId: toUserId,
        amount: amount,
        timestamp: DateTime.now(),
      ));
    });
  }
}
```

## Running Tests

### Unit Tests

```bash
flutter test
```

### Integration Tests

```bash
# Run all integration tests
flutter test integration_test/

# Run specific test
flutter test integration_test/database_test.dart

# Run on specific device
flutter test integration_test/ -d macos
```

### Performance Tests

```bash
flutter test integration_test/performance_test.dart
```

## Project Structure

```
example/
├── lib/
│   ├── main.dart                 # App entry point
│   ├── models/                   # Freezed models
│   │   ├── user_model.dart
│   │   ├── post_model.dart
│   │   └── comment_model.dart
│   ├── screens/                  # Feature screens
│   │   ├── home_screen.dart
│   │   ├── basic_operations_screen.dart
│   │   ├── user_management_screen.dart
│   │   ├── query_builder_screen.dart
│   │   ├── realtime_updates_screen.dart
│   │   ├── transaction_demo_screen.dart
│   │   └── benchmark_screen.dart
│   └── widgets/                  # Reusable widgets
│       ├── user_card.dart
│       ├── query_builder.dart
│       └── benchmark_chart.dart
├── integration_test/             # Integration tests
│   ├── database_test.dart
│   ├── freezed_integration_test.dart
│   └── platform_test.dart
└── test/                        # Unit tests
    └── widget_test.dart
```

## Platform-Specific Notes

### iOS
- Minimum deployment target: iOS 11.0
- Uses static library (.a) without zstd compression

### Android
- Minimum SDK: 21
- Supports all architectures (arm64, arm, x86, x64)

### macOS
- Requires network entitlement for binary installer
- Supports both Intel and Apple Silicon

### Windows
- Requires Visual C++ Redistributables
- 64-bit only

### Linux
- Requires glibc 2.17+
- 64-bit only

## Troubleshooting

### Build Issues

```bash
# Clean build
flutter clean
flutter pub get
dart run build_runner build --delete-conflicting-outputs

# Reset iOS pods
cd ios && pod deintegrate && pod install
```

### Performance Issues

1. Check cache configuration
2. Enable appropriate compression
3. Use batch operations
4. Create indexes for queries

### Platform-Specific Issues

See platform setup in main README.

## Contributing

Feel free to submit issues and enhancement requests!

## License

MIT License - see LICENSE file for details.