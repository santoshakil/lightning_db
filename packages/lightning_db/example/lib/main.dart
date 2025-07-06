import 'package:flutter/material.dart';
import 'package:lightning_db/lightning_db.dart';
import 'package:rxdart/rxdart.dart';
import 'models/todo.dart';
import 'models/user.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Lightning DB Example',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.deepPurple),
        useMaterial3: true,
      ),
      home: const MyHomePage(title: 'Lightning DB Example'),
    );
  }
}

class MyHomePage extends StatefulWidget {
  const MyHomePage({super.key, required this.title});

  final String title;

  @override
  State<MyHomePage> createState() => _MyHomePageState();
}

class _MyHomePageState extends State<MyHomePage> {
  late LightningDb _db;
  late FreezedCollection<Todo> _todoCollection;
  late FreezedCollection<User> _userCollection;
  
  final _todos = BehaviorSubject<List<Todo>>.seeded([]);
  User? _currentUser;
  
  @override
  void initState() {
    super.initState();
    _initDatabase();
  }
  
  Future<void> _initDatabase() async {
    // Initialize database
    _db = await LightningDb.create(
      'example_app.db',
      DatabaseConfig(
        pageSize: 4096,
        cacheSize: 10 * 1024 * 1024, // 10MB
        syncMode: SyncMode.normal,
      ),
    );
    
    // Create collections with Freezed adapters
    _todoCollection = FreezedCollection<Todo>(
      _db,
      'todos',
      FreezedAdapter<Todo>(
        fromJson: Todo.fromJson,
        toJson: (todo) => todo.toJson(),
      ),
    );
    
    _userCollection = FreezedCollection<User>(
      _db,
      'users',
      FreezedAdapter<User>(
        fromJson: User.fromJson,
        toJson: (user) => user.toJson(),
      ),
    );
    
    // Load initial data
    await _loadData();
    
    // Subscribe to todo changes
    _todoCollection.watch().listen((todos) {
      _todos.add(todos);
    });
  }
  
  Future<void> _loadData() async {
    // Check if we have a user
    final users = await _userCollection.getAll();
    if (users.isEmpty) {
      // Create default user
      final user = User(
        id: DateTime.now().millisecondsSinceEpoch.toString(),
        name: 'Test User',
        email: 'test@example.com',
        createdAt: DateTime.now(),
      );
      await _userCollection.add(user);
      _currentUser = user;
    } else {
      _currentUser = users.first;
    }
    
    // Load todos
    final todos = await _todoCollection.getAll();
    _todos.add(todos);
  }
  
  Future<void> _addTodo(String title, String? description) async {
    final todo = Todo(
      id: DateTime.now().millisecondsSinceEpoch.toString(),
      title: title,
      description: description,
      completed: false,
      createdAt: DateTime.now(),
    );
    
    await _todoCollection.add(todo);
  }
  
  Future<void> _toggleTodo(Todo todo) async {
    final updated = todo.copyWith(
      completed: !todo.completed,
      completedAt: !todo.completed ? DateTime.now() : null,
    );
    
    await _todoCollection.update(todo.id, updated);
  }
  
  Future<void> _deleteTodo(String id) async {
    await _todoCollection.delete(id);
  }
  
  void _showAddTodoDialog() {
    final titleController = TextEditingController();
    final descriptionController = TextEditingController();
    
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Add Todo'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(
              controller: titleController,
              decoration: const InputDecoration(labelText: 'Title'),
            ),
            TextField(
              controller: descriptionController,
              decoration: const InputDecoration(labelText: 'Description'),
              maxLines: 3,
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('Cancel'),
          ),
          TextButton(
            onPressed: () async {
              await _addTodo(
                titleController.text,
                descriptionController.text.isEmpty ? null : descriptionController.text,
              );
              if (context.mounted) {
                Navigator.of(context).pop();
              }
            },
            child: const Text('Add'),
          ),
        ],
      ),
    );
  }
  
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        backgroundColor: Theme.of(context).colorScheme.inversePrimary,
        title: Text(widget.title),
        actions: [
          if (_currentUser != null)
            Padding(
              padding: const EdgeInsets.all(8.0),
              child: Center(
                child: Text('Hello, ${_currentUser!.name}'),
              ),
            ),
        ],
      ),
      body: StreamBuilder<List<Todo>>(
        stream: _todos,
        builder: (context, snapshot) {
          final todos = snapshot.data ?? [];
          
          if (todos.isEmpty) {
            return const Center(
              child: Text('No todos yet. Add one!'),
            );
          }
          
          return ListView.builder(
            itemCount: todos.length,
            itemBuilder: (context, index) {
              final todo = todos[index];
              
              return ListTile(
                leading: Checkbox(
                  value: todo.completed,
                  onChanged: (_) => _toggleTodo(todo),
                ),
                title: Text(
                  todo.title,
                  style: TextStyle(
                    decoration: todo.completed
                        ? TextDecoration.lineThrough
                        : TextDecoration.none,
                  ),
                ),
                subtitle: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    if (todo.description != null)
                      Text(todo.description!),
                    Text(
                      'Created: ${todo.createdAt.toLocal()}',
                      style: Theme.of(context).textTheme.bodySmall,
                    ),
                    if (todo.completedAt != null)
                      Text(
                        'Completed: ${todo.completedAt!.toLocal()}',
                        style: Theme.of(context).textTheme.bodySmall,
                      ),
                  ],
                ),
                trailing: IconButton(
                  icon: const Icon(Icons.delete),
                  onPressed: () => _deleteTodo(todo.id),
                ),
              );
            },
          );
        },
      ),
      floatingActionButton: FloatingActionButton(
        onPressed: _showAddTodoDialog,
        tooltip: 'Add Todo',
        child: const Icon(Icons.add),
      ),
    );
  }
  
  @override
  void dispose() {
    _todos.close();
    _db.close();
    super.dispose();
  }
}