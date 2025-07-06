import 'package:flutter/material.dart';
import 'dart:async';
import 'dart:typed_data';
import 'dart:io';
import 'package:lightning_db/lightning_db.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Lightning DB Demo',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.blue),
        useMaterial3: true,
      ),
      home: const TodoListScreen(),
    );
  }
}

class TodoListScreen extends StatefulWidget {
  const TodoListScreen({super.key});

  @override
  State<TodoListScreen> createState() => _TodoListScreenState();
}

class _TodoListScreenState extends State<TodoListScreen> {
  LightningDb? _db;
  final List<Todo> _todos = [];
  final _textController = TextEditingController();
  bool _isLoading = true;

  @override
  void initState() {
    super.initState();
    _initDatabase();
  }

  Future<void> _initDatabase() async {
    try {
      // Initialize database
      _db = await LightningDb.create('${Directory.systemTemp.path}/todo_database');
      await _loadTodos();
    } catch (e) {
      debugPrint('Failed to initialize database: $e');
    } finally {
      setState(() => _isLoading = false);
    }
  }

  Future<void> _loadTodos() async {
    if (_db == null) return;
    
    _todos.clear();
    
    // Scan all todos
    final iter = await _db!.scan(
      start: Uint8List.fromList('todo:'.codeUnits),
      end: Uint8List.fromList('todo:~'.codeUnits),
    );
    
    await for (final (keyBytes, valueBytes) in iter.toStream()) {
      final key = String.fromCharCodes(keyBytes);
      final id = key.split(':')[1];
      final text = String.fromCharCodes(valueBytes);
      _todos.add(Todo(id: id, text: text));
    }
    
    setState(() {});
  }

  Future<void> _addTodo(String text) async {
    if (_db == null || text.isEmpty) return;
    
    final id = DateTime.now().millisecondsSinceEpoch.toString();
    final key = Uint8List.fromList('todo:$id'.codeUnits);
    final value = Uint8List.fromList(text.codeUnits);
    
    await _db!.put(key, value);
    await _loadTodos();
  }

  Future<void> _deleteTodo(String id) async {
    if (_db == null) return;
    
    final key = Uint8List.fromList('todo:$id'.codeUnits);
    await _db!.delete(key);
    await _loadTodos();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        backgroundColor: Theme.of(context).colorScheme.inversePrimary,
        title: const Text('Lightning DB Todo List'),
      ),
      body: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : Column(
              children: [
                Padding(
                  padding: const EdgeInsets.all(16.0),
                  child: Row(
                    children: [
                      Expanded(
                        child: TextField(
                          controller: _textController,
                          decoration: const InputDecoration(
                            hintText: 'Enter a todo item',
                            border: OutlineInputBorder(),
                          ),
                          onSubmitted: (text) {
                            _addTodo(text);
                            _textController.clear();
                          },
                        ),
                      ),
                      const SizedBox(width: 8),
                      ElevatedButton(
                        onPressed: () {
                          _addTodo(_textController.text);
                          _textController.clear();
                        },
                        child: const Text('Add'),
                      ),
                    ],
                  ),
                ),
                Expanded(
                  child: _todos.isEmpty
                      ? const Center(
                          child: Text('No todos yet. Add one above!'),
                        )
                      : ListView.builder(
                          itemCount: _todos.length,
                          itemBuilder: (context, index) {
                            final todo = _todos[index];
                            return ListTile(
                              title: Text(todo.text),
                              trailing: IconButton(
                                icon: const Icon(Icons.delete),
                                onPressed: () => _deleteTodo(todo.id),
                              ),
                            );
                          },
                        ),
                ),
                Padding(
                  padding: const EdgeInsets.all(16.0),
                  child: Text(
                    'Total items: ${_todos.length}',
                    style: Theme.of(context).textTheme.bodySmall,
                  ),
                ),
              ],
            ),
    );
  }

  @override
  void dispose() {
    _textController.dispose();
    _db?.close();
    super.dispose();
  }
}

class Todo {
  final String id;
  final String text;

  Todo({required this.id, required this.text});
}
