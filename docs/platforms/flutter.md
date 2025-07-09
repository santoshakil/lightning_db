# Flutter Integration Guide

## Overview

Lightning DB provides seamless Flutter integration with native performance across all platforms. This guide covers setup, usage patterns, and best practices for Flutter applications.

## Prerequisites

- Flutter 3.0+
- Dart 2.17+
- Platform-specific requirements:
  - Android: API level 21+
  - iOS: iOS 13.0+
  - macOS: macOS 10.15+
  - Windows: Windows 10+
  - Linux: Ubuntu 18.04+

## Installation

### pubspec.yaml

```yaml
dependencies:
  lightning_db: ^0.1.0
  
dev_dependencies:
  flutter_test:
    sdk: flutter
  integration_test:
    sdk: flutter
```

### Platform Setup

The package automatically handles platform-specific setup, but you may need to configure:

#### Android (android/app/build.gradle)
```gradle
android {
    compileSdkVersion 34
    
    defaultConfig {
        minSdkVersion 21
        targetSdkVersion 34
    }
}
```

#### iOS (ios/Podfile)
```ruby
platform :ios, '13.0'
```

## Quick Start

### Basic Usage

```dart
import 'package:lightning_db/lightning_db.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  
  // Initialize Lightning DB
  await LightningDB.initialize();
  
  runApp(MyApp());
}

class MyApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: DatabaseExample(),
    );
  }
}

class DatabaseExample extends StatefulWidget {
  @override
  _DatabaseExampleState createState() => _DatabaseExampleState();
}

class _DatabaseExampleState extends State<DatabaseExample> {
  late Database db;
  String? retrievedValue;
  
  @override
  void initState() {
    super.initState();
    initializeDatabase();
  }
  
  Future<void> initializeDatabase() async {
    db = await Database.open('my_database');
    
    // Basic operations
    await db.put('greeting', 'Hello, Lightning DB!');
    final value = await db.get('greeting');
    
    setState(() {
      retrievedValue = value;
    });
  }
  
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text('Lightning DB Demo')),
      body: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Text('Retrieved: ${retrievedValue ?? 'Loading...'}'),
            ElevatedButton(
              onPressed: () async {
                await db.put('timestamp', DateTime.now().toString());
                final value = await db.get('timestamp');
                setState(() {
                  retrievedValue = value;
                });
              },
              child: Text('Update Timestamp'),
            ),
          ],
        ),
      ),
    );
  }
  
  @override
  void dispose() {
    db.close();
    super.dispose();
  }
}
```

## State Management Integration

### Provider Pattern

```dart
import 'package:provider/provider.dart';
import 'package:lightning_db/lightning_db.dart';

class DatabaseProvider extends ChangeNotifier {
  Database? _db;
  bool _isInitialized = false;
  
  Database? get database => _db;
  bool get isInitialized => _isInitialized;
  
  Future<void> initialize() async {
    if (_isInitialized) return;
    
    try {
      _db = await Database.open('app_database');
      _isInitialized = true;
      notifyListeners();
    } catch (e) {
      print('Failed to initialize database: $e');
    }
  }
  
  Future<void> put(String key, String value) async {
    if (_db == null) throw Exception('Database not initialized');
    await _db!.put(key, value);
    notifyListeners();
  }
  
  Future<String?> get(String key) async {
    if (_db == null) throw Exception('Database not initialized');
    return await _db!.get(key);
  }
  
  @override
  void dispose() {
    _db?.close();
    super.dispose();
  }
}

// Usage
void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await LightningDB.initialize();
  
  runApp(
    ChangeNotifierProvider(
      create: (context) => DatabaseProvider()..initialize(),
      child: MyApp(),
    ),
  );
}

class MyApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Consumer<DatabaseProvider>(
        builder: (context, dbProvider, child) {
          if (!dbProvider.isInitialized) {
            return Scaffold(
              body: Center(child: CircularProgressIndicator()),
            );
          }
          
          return DatabaseScreen();
        },
      ),
    );
  }
}
```

### Riverpod Integration

```dart
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:lightning_db/lightning_db.dart';

final databaseProvider = FutureProvider<Database>((ref) async {
  return await Database.open('app_database');
});

final userProvider = StateNotifierProvider<UserNotifier, AsyncValue<User?>>((ref) {
  return UserNotifier(ref.watch(databaseProvider));
});

class UserNotifier extends StateNotifier<AsyncValue<User?>> {
  final AsyncValue<Database> _database;
  
  UserNotifier(this._database) : super(const AsyncValue.loading()) {
    loadUser();
  }
  
  Future<void> loadUser() async {
    try {
      final db = await _database.future;
      final userJson = await db.get('current_user');
      
      if (userJson != null) {
        final user = User.fromJson(jsonDecode(userJson));
        state = AsyncValue.data(user);
      } else {
        state = const AsyncValue.data(null);
      }
    } catch (e) {
      state = AsyncValue.error(e, StackTrace.current);
    }
  }
  
  Future<void> saveUser(User user) async {
    try {
      final db = await _database.future;
      await db.put('current_user', jsonEncode(user.toJson()));
      state = AsyncValue.data(user);
    } catch (e) {
      state = AsyncValue.error(e, StackTrace.current);
    }
  }
}

// Usage
class UserScreen extends ConsumerWidget {
  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final userAsyncValue = ref.watch(userProvider);
    
    return Scaffold(
      appBar: AppBar(title: Text('User Profile')),
      body: userAsyncValue.when(
        loading: () => Center(child: CircularProgressIndicator()),
        error: (error, stack) => Center(child: Text('Error: $error')),
        data: (user) => user != null
            ? UserProfile(user: user)
            : Center(child: Text('No user found')),
      ),
    );
  }
}
```

### Bloc Pattern

```dart
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:lightning_db/lightning_db.dart';

// Events
abstract class DatabaseEvent {}
class LoadData extends DatabaseEvent {
  final String key;
  LoadData(this.key);
}
class SaveData extends DatabaseEvent {
  final String key;
  final String value;
  SaveData(this.key, this.value);
}

// States
abstract class DatabaseState {}
class DatabaseInitial extends DatabaseState {}
class DatabaseLoading extends DatabaseState {}
class DatabaseLoaded extends DatabaseState {
  final String? value;
  DatabaseLoaded(this.value);
}
class DatabaseError extends DatabaseState {
  final String message;
  DatabaseError(this.message);
}

// Bloc
class DatabaseBloc extends Bloc<DatabaseEvent, DatabaseState> {
  final Database database;
  
  DatabaseBloc(this.database) : super(DatabaseInitial()) {
    on<LoadData>(_onLoadData);
    on<SaveData>(_onSaveData);
  }
  
  Future<void> _onLoadData(LoadData event, Emitter<DatabaseState> emit) async {
    emit(DatabaseLoading());
    try {
      final value = await database.get(event.key);
      emit(DatabaseLoaded(value));
    } catch (e) {
      emit(DatabaseError(e.toString()));
    }
  }
  
  Future<void> _onSaveData(SaveData event, Emitter<DatabaseState> emit) async {
    try {
      await database.put(event.key, event.value);
      emit(DatabaseLoaded(event.value));
    } catch (e) {
      emit(DatabaseError(e.toString()));
    }
  }
}

// Usage
class DatabaseScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return BlocProvider(
      create: (context) => DatabaseBloc(context.read<Database>()),
      child: BlocBuilder<DatabaseBloc, DatabaseState>(
        builder: (context, state) {
          if (state is DatabaseLoading) {
            return Center(child: CircularProgressIndicator());
          } else if (state is DatabaseLoaded) {
            return Center(child: Text('Value: ${state.value}'));
          } else if (state is DatabaseError) {
            return Center(child: Text('Error: ${state.message}'));
          }
          return Center(child: Text('No data'));
        },
      ),
    );
  }
}
```

## Data Models with Freezed

### User Model

```dart
import 'package:freezed_annotation/freezed_annotation.dart';
import 'package:json_annotation/json_annotation.dart';

part 'user.freezed.dart';
part 'user.g.dart';

@freezed
class User with _$User {
  const factory User({
    required String id,
    required String name,
    required String email,
    DateTime? createdAt,
    @Default(false) bool isActive,
  }) = _User;
  
  factory User.fromJson(Map<String, dynamic> json) => _$UserFromJson(json);
}

// Repository
class UserRepository {
  final Database _db;
  
  UserRepository(this._db);
  
  Future<void> saveUser(User user) async {
    await _db.put('user:${user.id}', jsonEncode(user.toJson()));
  }
  
  Future<User?> getUser(String id) async {
    final json = await _db.get('user:$id');
    if (json == null) return null;
    
    return User.fromJson(jsonDecode(json));
  }
  
  Future<List<User>> getAllUsers() async {
    final userIds = await _db.get('user_ids');
    if (userIds == null) return [];
    
    final ids = List<String>.from(jsonDecode(userIds));
    final users = <User>[];
    
    for (final id in ids) {
      final user = await getUser(id);
      if (user != null) users.add(user);
    }
    
    return users;
  }
  
  Future<void> deleteUser(String id) async {
    await _db.delete('user:$id');
    
    // Update user_ids list
    final userIds = await _db.get('user_ids');
    if (userIds != null) {
      final ids = List<String>.from(jsonDecode(userIds));
      ids.remove(id);
      await _db.put('user_ids', jsonEncode(ids));
    }
  }
}
```

### Todo App Example

```dart
@freezed
class Todo with _$Todo {
  const factory Todo({
    required String id,
    required String title,
    required String description,
    required DateTime createdAt,
    DateTime? completedAt,
    @Default(false) bool isCompleted,
    @Default(TodoPriority.medium) TodoPriority priority,
  }) = _Todo;
  
  factory Todo.fromJson(Map<String, dynamic> json) => _$TodoFromJson(json);
}

enum TodoPriority { low, medium, high }

class TodoRepository {
  final Database _db;
  
  TodoRepository(this._db);
  
  Future<void> saveTodo(Todo todo) async {
    await _db.put('todo:${todo.id}', jsonEncode(todo.toJson()));
    
    // Update todo list
    final todos = await getAllTodos();
    final todoIds = todos.map((t) => t.id).toList();
    await _db.put('todo_ids', jsonEncode(todoIds));
  }
  
  Future<List<Todo>> getAllTodos() async {
    final todoIds = await _db.get('todo_ids');
    if (todoIds == null) return [];
    
    final ids = List<String>.from(jsonDecode(todoIds));
    final todos = <Todo>[];
    
    for (final id in ids) {
      final todo = await getTodo(id);
      if (todo != null) todos.add(todo);
    }
    
    return todos;
  }
  
  Future<Todo?> getTodo(String id) async {
    final json = await _db.get('todo:$id');
    if (json == null) return null;
    
    return Todo.fromJson(jsonDecode(json));
  }
  
  Future<void> completeTodo(String id) async {
    final todo = await getTodo(id);
    if (todo != null) {
      final completed = todo.copyWith(
        isCompleted: true,
        completedAt: DateTime.now(),
      );
      await saveTodo(completed);
    }
  }
}

// Todo Screen
class TodoScreen extends StatefulWidget {
  @override
  _TodoScreenState createState() => _TodoScreenState();
}

class _TodoScreenState extends State<TodoScreen> {
  late TodoRepository _todoRepository;
  List<Todo> _todos = [];
  bool _isLoading = true;
  
  @override
  void initState() {
    super.initState();
    _initializeRepository();
  }
  
  Future<void> _initializeRepository() async {
    final db = await Database.open('todos');
    _todoRepository = TodoRepository(db);
    await _loadTodos();
  }
  
  Future<void> _loadTodos() async {
    setState(() => _isLoading = true);
    
    try {
      final todos = await _todoRepository.getAllTodos();
      setState(() {
        _todos = todos;
        _isLoading = false;
      });
    } catch (e) {
      setState(() => _isLoading = false);
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Failed to load todos: $e')),
      );
    }
  }
  
  Future<void> _addTodo(String title, String description) async {
    final todo = Todo(
      id: DateTime.now().millisecondsSinceEpoch.toString(),
      title: title,
      description: description,
      createdAt: DateTime.now(),
    );
    
    await _todoRepository.saveTodo(todo);
    await _loadTodos();
  }
  
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('Lightning DB Todos'),
        actions: [
          IconButton(
            icon: Icon(Icons.add),
            onPressed: () => _showAddTodoDialog(),
          ),
        ],
      ),
      body: _isLoading
          ? Center(child: CircularProgressIndicator())
          : ListView.builder(
              itemCount: _todos.length,
              itemBuilder: (context, index) {
                final todo = _todos[index];
                return TodoItem(
                  todo: todo,
                  onCompleted: () async {
                    await _todoRepository.completeTodo(todo.id);
                    await _loadTodos();
                  },
                );
              },
            ),
    );
  }
  
  void _showAddTodoDialog() {
    final titleController = TextEditingController();
    final descriptionController = TextEditingController();
    
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: Text('Add Todo'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(
              controller: titleController,
              decoration: InputDecoration(labelText: 'Title'),
            ),
            TextField(
              controller: descriptionController,
              decoration: InputDecoration(labelText: 'Description'),
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: Text('Cancel'),
          ),
          TextButton(
            onPressed: () async {
              await _addTodo(titleController.text, descriptionController.text);
              Navigator.pop(context);
            },
            child: Text('Add'),
          ),
        ],
      ),
    );
  }
}
```

## Transaction Management

```dart
class TransactionService {
  final Database _db;
  
  TransactionService(this._db);
  
  Future<T> executeTransaction<T>(Future<T> Function(Transaction) operation) async {
    final transaction = await _db.beginTransaction();
    
    try {
      final result = await operation(transaction);
      await transaction.commit();
      return result;
    } catch (e) {
      await transaction.rollback();
      rethrow;
    }
  }
  
  Future<void> transferData(String fromKey, String toKey, String value) async {
    await executeTransaction((tx) async {
      // Check if source exists
      final sourceValue = await tx.get(fromKey);
      if (sourceValue == null) {
        throw Exception('Source key not found');
      }
      
      // Perform transfer
      await tx.delete(fromKey);
      await tx.put(toKey, value);
    });
  }
  
  Future<void> batchUpdate(Map<String, String> updates) async {
    await executeTransaction((tx) async {
      for (final entry in updates.entries) {
        await tx.put(entry.key, entry.value);
      }
    });
  }
}
```

## Background Processing

```dart
import 'package:workmanager/workmanager.dart';

class DatabaseSyncService {
  static const String syncTaskName = 'database_sync';
  
  static void initialize() {
    Workmanager().initialize(
      callbackDispatcher,
      isInDebugMode: false,
    );
  }
  
  static void scheduleSyncTask() {
    Workmanager().registerPeriodicTask(
      syncTaskName,
      syncTaskName,
      frequency: Duration(minutes: 15),
      constraints: Constraints(
        networkType: NetworkType.connected,
      ),
    );
  }
  
  static Future<void> performSync() async {
    try {
      final db = await Database.open('app_database');
      
      // Perform database maintenance
      await db.checkpoint();
      await db.compact();
      
      // Sync with remote server
      await _syncWithServer(db);
      
      db.close();
    } catch (e) {
      print('Sync failed: $e');
    }
  }
  
  static Future<void> _syncWithServer(Database db) async {
    // Implementation depends on your sync strategy
  }
}

@pragma('vm:entry-point')
void callbackDispatcher() {
  Workmanager().executeTask((task, inputData) async {
    switch (task) {
      case DatabaseSyncService.syncTaskName:
        await DatabaseSyncService.performSync();
        break;
    }
    return Future.value(true);
  });
}
```

## Testing

### Unit Tests

```dart
import 'package:flutter_test/flutter_test.dart';
import 'package:lightning_db/lightning_db.dart';

void main() {
  group('Database Tests', () {
    late Database db;
    
    setUp(() async {
      await LightningDB.initialize();
      db = await Database.open('test_db');
    });
    
    tearDown(() async {
      await db.close();
      await Database.destroy('test_db');
    });
    
    test('should store and retrieve data', () async {
      await db.put('test_key', 'test_value');
      final value = await db.get('test_key');
      expect(value, 'test_value');
    });
    
    test('should handle transactions', () async {
      final tx = await db.beginTransaction();
      
      await tx.put('tx_key', 'tx_value');
      
      // Value not visible before commit
      expect(await db.get('tx_key'), isNull);
      
      await tx.commit();
      
      // Value visible after commit
      expect(await db.get('tx_key'), 'tx_value');
    });
    
    test('should handle rollback', () async {
      final tx = await db.beginTransaction();
      
      await tx.put('rollback_key', 'rollback_value');
      await tx.rollback();
      
      // Value should not be visible after rollback
      expect(await db.get('rollback_key'), isNull);
    });
  });
}
```

### Integration Tests

```dart
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:my_app/main.dart' as app;

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  
  group('Database Integration Tests', () {
    testWidgets('should persist data across app restarts', (tester) async {
      // First app launch
      app.main();
      await tester.pumpAndSettle();
      
      // Add some data
      await tester.enterText(find.byKey(Key('key_input')), 'test_key');
      await tester.enterText(find.byKey(Key('value_input')), 'test_value');
      await tester.tap(find.byKey(Key('save_button')));
      await tester.pumpAndSettle();
      
      // Restart app
      await tester.binding.defaultBinaryMessenger.handlePlatformMessage(
        'flutter/platform',
        const StandardMethodCodec().encodeMethodCall(
          const MethodCall('SystemNavigator.pop'),
        ),
        (data) {},
      );
      
      app.main();
      await tester.pumpAndSettle();
      
      // Verify data persisted
      await tester.enterText(find.byKey(Key('key_input')), 'test_key');
      await tester.tap(find.byKey(Key('load_button')));
      await tester.pumpAndSettle();
      
      expect(find.text('test_value'), findsOneWidget);
    });
    
    testWidgets('should handle concurrent operations', (tester) async {
      app.main();
      await tester.pumpAndSettle();
      
      // Simulate concurrent writes
      await tester.tap(find.byKey(Key('concurrent_test_button')));
      await tester.pumpAndSettle();
      
      // Verify all operations completed successfully
      expect(find.text('All operations completed'), findsOneWidget);
    });
  });
}
```

### Performance Tests

```dart
import 'package:flutter_test/flutter_test.dart';
import 'package:lightning_db/lightning_db.dart';

void main() {
  group('Performance Tests', () {
    late Database db;
    
    setUp(() async {
      await LightningDB.initialize();
      db = await Database.open('performance_test_db');
    });
    
    tearDown(() async {
      await db.close();
      await Database.destroy('performance_test_db');
    });
    
    test('should handle large datasets efficiently', () async {
      const itemCount = 10000;
      final stopwatch = Stopwatch()..start();
      
      // Batch insert
      final tx = await db.beginTransaction();
      for (int i = 0; i < itemCount; i++) {
        await tx.put('item_$i', 'value_$i');
      }
      await tx.commit();
      
      stopwatch.stop();
      print('Inserted $itemCount items in ${stopwatch.elapsedMilliseconds}ms');
      
      // Verify performance is acceptable (adjust threshold as needed)
      expect(stopwatch.elapsedMilliseconds, lessThan(5000));
    });
    
    test('should handle concurrent access', () async {
      final futures = <Future>[];
      
      for (int i = 0; i < 100; i++) {
        futures.add(db.put('concurrent_$i', 'value_$i'));
      }
      
      await Future.wait(futures);
      
      // Verify all items were stored
      for (int i = 0; i < 100; i++) {
        final value = await db.get('concurrent_$i');
        expect(value, 'value_$i');
      }
    });
  });
}
```

## Best Practices

### 1. Database Lifecycle Management

```dart
class DatabaseManager {
  static DatabaseManager? _instance;
  Database? _database;
  
  DatabaseManager._internal();
  
  static DatabaseManager get instance {
    _instance ??= DatabaseManager._internal();
    return _instance!;
  }
  
  Future<Database> get database async {
    if (_database == null) {
      _database = await Database.open('app_database');
    }
    return _database!;
  }
  
  Future<void> close() async {
    if (_database != null) {
      await _database!.close();
      _database = null;
    }
  }
}
```

### 2. Error Handling

```dart
class DatabaseService {
  final Database _db;
  
  DatabaseService(this._db);
  
  Future<String?> safeGet(String key) async {
    try {
      return await _db.get(key);
    } catch (e) {
      print('Error getting key $key: $e');
      return null;
    }
  }
  
  Future<bool> safePut(String key, String value) async {
    try {
      await _db.put(key, value);
      return true;
    } catch (e) {
      print('Error putting key $key: $e');
      return false;
    }
  }
}
```

### 3. Configuration Management

```dart
class DatabaseConfig {
  static const String _configKey = 'database_config';
  
  static Future<void> saveConfig(Map<String, dynamic> config) async {
    final db = await DatabaseManager.instance.database;
    await db.put(_configKey, jsonEncode(config));
  }
  
  static Future<Map<String, dynamic>> loadConfig() async {
    final db = await DatabaseManager.instance.database;
    final configJson = await db.get(_configKey);
    
    if (configJson == null) {
      return <String, dynamic>{};
    }
    
    return Map<String, dynamic>.from(jsonDecode(configJson));
  }
}
```

This comprehensive Flutter guide provides everything needed to integrate Lightning DB into Flutter applications effectively. For more advanced usage patterns, see the [API Reference](../api/README.md).