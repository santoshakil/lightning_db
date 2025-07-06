import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'package:lightning_db_freezed/lightning_db_freezed.dart';
import 'package:freezed_annotation/freezed_annotation.dart';

part 'freezed_example.freezed.dart';
part 'freezed_example.g.dart';

// Define a Freezed model
@freezed
class Todo with _$Todo {
  const Todo._();
  
  const factory Todo({
    required String id,
    required String title,
    required String description,
    @Default(false) bool completed,
    required DateTime createdAt,
    DateTime? completedAt,
    @Default([]) List<String> tags,
  }) = _Todo;
  
  factory Todo.fromJson(Map<String, dynamic> json) => _$TodoFromJson(json);
}

// Example usage
Future<void> main() async {
  // Open database
  final db = await LightningDb.open('todo_app.db');
  
  // Create a collection for todos
  final todos = db.jsonCollection<Todo>(
    name: 'todos',
    toJson: (todo) => todo.toJson(),
    fromJson: Todo.fromJson,
    keyExtractor: (todo) => todo.id,
  );
  
  // Insert a new todo
  final newTodo = Todo(
    id: DateTime.now().millisecondsSinceEpoch.toString(),
    title: 'Learn Lightning DB',
    description: 'Explore the Freezed integration with Lightning DB',
    createdAt: DateTime.now(),
    tags: ['learning', 'database'],
  );
  
  await todos.insert(newTodo);
  print('Inserted todo: ${newTodo.title}');
  
  // Update the todo using copyWith
  final updatedTodo = newTodo.copyWith(
    completed: true,
    completedAt: DateTime.now(),
  );
  
  await todos.update(updatedTodo);
  print('Updated todo: ${updatedTodo.title} - Completed: ${updatedTodo.completed}');
  
  // Query todos
  final activeTodos = await todos
      .query()
      .where((todo) => !todo.completed)
      .orderBy((todo) => todo.createdAt)
      .execute();
  
  print('Active todos: ${activeTodos.length}');
  
  // Use reactive features
  final reactiveTodos = todos.reactive();
  
  // Watch for changes
  reactiveTodos.events.listen((event) {
    switch (event.type) {
      case EventType.inserted:
        print('New todo added: ${event.item?.title}');
        break;
      case EventType.updated:
        print('Todo updated: ${event.newItem?.title}');
        break;
      case EventType.deleted:
        print('Todo deleted: ${event.item?.title}');
        break;
      case EventType.cleared:
        print('All todos cleared');
        break;
    }
  });
  
  // Reactive queries
  final completedCount = reactiveTodos
      .where((todo) => todo.completed)
      .count;
  
  completedCount.listen((count) {
    print('Completed todos: $count');
  });
  
  // Group by tags
  final todosByTag = reactiveTodos.groupBy((todo) => todo.tags.isNotEmpty ? todo.tags.first : 'untagged');
  
  todosByTag.listen((groups) {
    groups.forEach((tag, todos) {
      print('Tag "$tag": ${todos.length} todos');
    });
  });
  
  // Pagination
  final page1 = await todos.paginate(
    page: 1,
    pageSize: 10,
  );
  
  print('Page 1 of ${page1.totalPages}: ${page1.items.length} items');
  
  // Advanced query with multiple filters
  final urgentTodos = await todos
      .query()
      .where((todo) => !todo.completed)
      .where((todo) => todo.tags.contains('urgent'))
      .where((todo) => todo.createdAt.isAfter(DateTime.now().subtract(Duration(days: 7))))
      .orderByDescending((todo) => todo.createdAt)
      .limit(5)
      .execute();
  
  print('Urgent todos from last week: ${urgentTodos.length}');
  
  // Batch operations
  final batch = db.batch();
  
  for (int i = 0; i < 5; i++) {
    final todo = Todo(
      id: 'batch-$i',
      title: 'Batch Todo $i',
      description: 'Created in batch',
      createdAt: DateTime.now(),
    );
    
    batch.putObject(todo, todos.adapter);
  }
  
  await batch.commit();
  print('Batch inserted 5 todos');
  
  // Transaction with multiple operations
  final tx = await db.beginTransaction();
  
  try {
    // Move all completed todos to an archive collection
    final archives = db.jsonCollection<Todo>(
      name: 'archived_todos',
      toJson: (todo) => todo.toJson(),
      fromJson: Todo.fromJson,
      keyExtractor: (todo) => todo.id,
    );
    
    final completedTodos = await todos
        .query()
        .where((todo) => todo.completed)
        .execute();
    
    for (final todo in completedTodos) {
      await tx.putObject(todo, archives.adapter);
      await tx.deleteObject(todo, todos.adapter);
    }
    
    await tx.commit();
    print('Archived ${completedTodos.length} completed todos');
  } catch (e) {
    await tx.rollback();
    print('Transaction failed: $e');
  }
  
  // Clean up
  reactiveTodos.dispose();
  await db.close();
}

// Example of using pattern matching with union types
@freezed
class TodoState with _$TodoState {
  const factory TodoState.initial() = Initial;
  const factory TodoState.loading() = Loading;
  const factory TodoState.loaded(List<Todo> todos) = Loaded;
  const factory TodoState.error(String message) = Error;
}

// State management example
class TodoController {
  final FreezedCollection<Todo> _todos;
  final stateCollection = AdapterFactory.createJsonAdapter<TodoState>(
    toJson: (state) => state.toJson(),
    fromJson: TodoState.fromJson,
    keyExtractor: (_) => 'current_state',
  );
  
  TodoController(this._todos);
  
  Stream<TodoState> get state async* {
    yield TodoState.loading();
    
    try {
      final todos = await _todos.getAll();
      yield TodoState.loaded(todos);
    } catch (e) {
      yield TodoState.error(e.toString());
    }
  }
  
  void handleState(TodoState state) {
    switch (state) {
      case Initial():
        print('Initializing...');
      case Loading():
        print('Loading todos...');
      case Loaded(:final todos):
        print('Loaded ${todos.length} todos');
      case Error(:final message):
        print('Error: $message');
    }
  }
}