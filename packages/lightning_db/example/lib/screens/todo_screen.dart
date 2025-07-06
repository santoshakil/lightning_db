import 'package:flutter/material.dart';
import 'package:lightning_db/lightning_db.dart';
import 'package:rxdart/rxdart.dart';
import '../models/todo_model.dart';
import '../models/user_model.dart';

class TodoScreen extends StatefulWidget {
  const TodoScreen({super.key});

  @override
  State<TodoScreen> createState() => _TodoScreenState();
}

class _TodoScreenState extends State<TodoScreen> {
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
    _db = await LightningDb.open('todo_demo.db');
    
    // Create collections
    _todoCollection = _db.freezedCollection<Todo>('todos');
    _userCollection = _db.freezedCollection<User>('users');
    
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
    
    await _todoCollection.update(updated);
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
              autofocus: true,
            ),
            const SizedBox(height: 16),
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
              if (titleController.text.isNotEmpty) {
                await _addTodo(
                  titleController.text,
                  descriptionController.text.isEmpty ? null : descriptionController.text,
                );
                if (context.mounted) {
                  Navigator.of(context).pop();
                }
              }
            },
            child: const Text('Add'),
          ),
        ],
      ),
    );
  }
  
  void _showStatsDialog() {
    final totalTodos = _todos.value.length;
    final completedTodos = _todos.value.where((t) => t.completed).length;
    final pendingTodos = totalTodos - completedTodos;
    
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Todo Statistics'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Total todos: $totalTodos'),
            Text('Completed: $completedTodos'),
            Text('Pending: $pendingTodos'),
            if (totalTodos > 0)
              Text('Completion rate: ${(completedTodos / totalTodos * 100).toStringAsFixed(1)}%'),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('Close'),
          ),
        ],
      ),
    );
  }
  
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Todo App'),
        backgroundColor: Theme.of(context).colorScheme.inversePrimary,
        actions: [
          IconButton(
            icon: const Icon(Icons.bar_chart),
            onPressed: _showStatsDialog,
            tooltip: 'Statistics',
          ),
          if (_currentUser != null)
            Padding(
              padding: const EdgeInsets.all(8.0),
              child: Center(
                child: Text('${_currentUser!.name}'),
              ),
            ),
        ],
      ),
      body: StreamBuilder<List<Todo>>(
        stream: _todos,
        builder: (context, snapshot) {
          final todos = snapshot.data ?? [];
          
          if (todos.isEmpty) {
            return Center(
              child: Column(
                mainAxisAlignment: MainAxisAlignment.center,
                children: [
                  Icon(
                    Icons.checklist,
                    size: 64,
                    color: Colors.grey[400],
                  ),
                  const SizedBox(height: 16),
                  Text(
                    'No todos yet',
                    style: TextStyle(
                      fontSize: 18,
                      color: Colors.grey[600],
                    ),
                  ),
                  const SizedBox(height: 8),
                  ElevatedButton.icon(
                    onPressed: _showAddTodoDialog,
                    icon: const Icon(Icons.add),
                    label: const Text('Add your first todo'),
                  ),
                ],
              ),
            );
          }
          
          // Group todos by completion status
          final pendingTodos = todos.where((t) => !t.completed).toList();
          final completedTodos = todos.where((t) => t.completed).toList();
          
          return ListView(
            children: [
              if (pendingTodos.isNotEmpty) ...[
                const ListTile(
                  title: Text(
                    'Pending',
                    style: TextStyle(fontWeight: FontWeight.bold),
                  ),
                ),
                ...pendingTodos.map((todo) => _buildTodoTile(todo)),
              ],
              if (completedTodos.isNotEmpty) ...[
                const ListTile(
                  title: Text(
                    'Completed',
                    style: TextStyle(fontWeight: FontWeight.bold),
                  ),
                ),
                ...completedTodos.map((todo) => _buildTodoTile(todo)),
              ],
            ],
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
  
  Widget _buildTodoTile(Todo todo) {
    return Dismissible(
      key: Key(todo.id),
      background: Container(
        color: Colors.red,
        alignment: Alignment.centerRight,
        padding: const EdgeInsets.only(right: 16),
        child: const Icon(Icons.delete, color: Colors.white),
      ),
      direction: DismissDirection.endToStart,
      onDismissed: (_) => _deleteTodo(todo.id),
      child: ListTile(
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
            color: todo.completed
                ? Colors.grey
                : null,
          ),
        ),
        subtitle: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            if (todo.description != null)
              Text(
                todo.description!,
                style: TextStyle(
                  color: todo.completed
                      ? Colors.grey[600]
                      : null,
                ),
              ),
            const SizedBox(height: 4),
            Text(
              _formatDate(todo.createdAt),
              style: Theme.of(context).textTheme.bodySmall?.copyWith(
                color: Colors.grey[600],
              ),
            ),
            if (todo.completedAt != null)
              Text(
                'Completed: ${_formatDate(todo.completedAt!)}',
                style: Theme.of(context).textTheme.bodySmall?.copyWith(
                  color: Colors.green[600],
                ),
              ),
          ],
        ),
        trailing: PopupMenuButton<String>(
          onSelected: (value) {
            switch (value) {
              case 'edit':
                _showEditTodoDialog(todo);
                break;
              case 'delete':
                _deleteTodo(todo.id);
                break;
            }
          },
          itemBuilder: (context) => [
            const PopupMenuItem(
              value: 'edit',
              child: Text('Edit'),
            ),
            const PopupMenuItem(
              value: 'delete',
              child: Text('Delete'),
            ),
          ],
        ),
      ),
    );
  }
  
  void _showEditTodoDialog(Todo todo) {
    final titleController = TextEditingController(text: todo.title);
    final descriptionController = TextEditingController(text: todo.description);
    
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Edit Todo'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(
              controller: titleController,
              decoration: const InputDecoration(labelText: 'Title'),
              autofocus: true,
            ),
            const SizedBox(height: 16),
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
              if (titleController.text.isNotEmpty) {
                await _todoCollection.update(todo.copyWith(
                  title: titleController.text,
                  description: descriptionController.text.isEmpty 
                      ? null 
                      : descriptionController.text,
                ));
                if (context.mounted) {
                  Navigator.of(context).pop();
                }
              }
            },
            child: const Text('Save'),
          ),
        ],
      ),
    );
  }
  
  String _formatDate(DateTime date) {
    final now = DateTime.now();
    final difference = now.difference(date);
    
    if (difference.inDays == 0) {
      if (difference.inHours == 0) {
        if (difference.inMinutes == 0) {
          return 'Just now';
        }
        return '${difference.inMinutes}m ago';
      }
      return '${difference.inHours}h ago';
    } else if (difference.inDays == 1) {
      return 'Yesterday';
    } else if (difference.inDays < 7) {
      return '${difference.inDays} days ago';
    } else {
      return '${date.day}/${date.month}/${date.year}';
    }
  }
  
  @override
  void dispose() {
    _todos.close();
    _db.close();
    super.dispose();
  }
}