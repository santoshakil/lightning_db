# Lightning DB Example

This example demonstrates how to use Lightning DB Flutter plugin with Freezed models.

## Features Demonstrated

- Database initialization and configuration
- Creating type-safe collections with Freezed models
- CRUD operations (Create, Read, Update, Delete)
- Reactive programming with RxDart
- Real-time data synchronization
- Transaction support

## Running the Example

1. Generate Freezed models:
   ```bash
   dart run build_runner build --delete-conflicting-outputs
   ```

2. Run the app:
   ```bash
   flutter run
   ```

## Key Concepts

### Database Initialization
```dart
final db = await LightningDb.create(
  'example_app.db',
  DatabaseConfig(
    pageSize: 4096,
    cacheSize: 10 * 1024 * 1024, // 10MB
    syncMode: SyncMode.normal,
  ),
);
```

### Creating Collections
```dart
final todoCollection = FreezedCollection<Todo>(
  db,
  'todos',
  FreezedAdapter<Todo>(
    fromJson: Todo.fromJson,
    toJson: (todo) => todo.toJson(),
  ),
);
```

### Reactive Queries
```dart
todoCollection.watch().listen((todos) {
  // React to changes
});
```

### CRUD Operations
```dart
// Create
await todoCollection.add(todo);

// Read
final todo = await todoCollection.get(id);
final todos = await todoCollection.getAll();

// Update
await todoCollection.update(id, updatedTodo);

// Delete
await todoCollection.delete(id);
```