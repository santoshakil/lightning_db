import 'dart:io';
import 'dart:convert';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'benchmark_runner.dart';
import 'benchmark_models.dart';

/// SQLite benchmark implementation
class SqliteBenchmark extends BenchmarkRunner {
  late Database db;
  final String dbPath;
  final int recordCount;
  
  SqliteBenchmark({
    required this.dbPath,
    required this.recordCount,
  }) : super('SQLite');
  
  @override
  Future<void> setup() async {
    // Initialize FFI for desktop
    sqfliteFfiInit();
    databaseFactory = databaseFactoryFfi;
    
    // Delete existing database
    final dbFile = File(dbPath);
    if (await dbFile.exists()) {
      await dbFile.delete();
    }
    
    // Open database
    db = await openDatabase(
      dbPath,
      version: 1,
      onCreate: (db, version) async {
        // Create users table
        await db.execute('''
          CREATE TABLE users (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            metadata TEXT
          )
        ''');
        
        // Create indexes
        await db.execute('CREATE INDEX idx_users_age ON users(age)');
        await db.execute('CREATE INDEX idx_users_email ON users(email)');
        
        // Create posts table
        await db.execute('''
          CREATE TABLE posts (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            author_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER,
            tags TEXT,
            metadata TEXT,
            FOREIGN KEY (author_id) REFERENCES users(id)
          )
        ''');
        
        await db.execute('CREATE INDEX idx_posts_author ON posts(author_id)');
        
        // Create comments table
        await db.execute('''
          CREATE TABLE comments (
            id TEXT PRIMARY KEY,
            post_id TEXT NOT NULL,
            author_id TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            edited_at INTEGER,
            likes INTEGER DEFAULT 0,
            metadata TEXT,
            FOREIGN KEY (post_id) REFERENCES posts(id),
            FOREIGN KEY (author_id) REFERENCES users(id)
          )
        ''');
        
        await db.execute('CREATE INDEX idx_comments_post ON comments(post_id)');
      },
    );
    
    // Enable WAL mode for better concurrent performance
    await db.execute('PRAGMA journal_mode=WAL');
  }
  
  @override
  Future<void> cleanup() async {
    await db.close();
  }
  
  @override
  Future<void> runIteration(int iteration) async {
    // This will be overridden by specific benchmark types
  }
  
  Map<String, dynamic> _userToMap(BenchmarkUser user) => {
    'id': user.id,
    'name': user.name,
    'email': user.email,
    'age': user.age,
    'created_at': user.createdAt.millisecondsSinceEpoch,
    'metadata': jsonEncode(user.metadata),
  };
  
  BenchmarkUser _userFromMap(Map<String, dynamic> map) => BenchmarkUser(
    id: map['id'],
    name: map['name'],
    email: map['email'],
    age: map['age'],
    createdAt: DateTime.fromMillisecondsSinceEpoch(map['created_at']),
    metadata: map['metadata'] != null 
        ? Map<String, dynamic>.from(jsonDecode(map['metadata']))
        : {},
  );
}

/// CRUD operations benchmark
class SqliteCrudBenchmark extends SqliteBenchmark {
  SqliteCrudBenchmark({
    required String dbPath,
    required int recordCount,
  }) : super(dbPath: dbPath, recordCount: recordCount);
  
  @override
  String get name => 'SQLite - CRUD';
  
  @override
  Future<void> runIteration(int iteration) async {
    final user = BenchmarkUser(
      id: 'user_$iteration',
      name: 'User $iteration',
      email: 'user$iteration@example.com',
      age: 20 + (iteration % 50),
      createdAt: DateTime.now(),
      metadata: {
        'iteration': iteration,
        'active': iteration % 2 == 0,
        'score': iteration * 1.5,
      },
    );
    
    // Create
    await db.insert('users', _userToMap(user));
    
    // Read
    final result = await db.query(
      'users',
      where: 'id = ?',
      whereArgs: [user.id],
    );
    
    // Update
    if (result.isNotEmpty) {
      final retrieved = _userFromMap(result.first);
      await db.update(
        'users',
        _userToMap(retrieved.copyWith(
          name: 'Updated ${retrieved.name}',
          metadata: {...retrieved.metadata, 'updated': true},
        )),
        where: 'id = ?',
        whereArgs: [user.id],
      );
    }
    
    // Read again
    await db.query(
      'users',
      where: 'id = ?',
      whereArgs: [user.id],
    );
    
    // Delete
    await db.delete(
      'users',
      where: 'id = ?',
      whereArgs: [user.id],
    );
  }
}

/// Bulk insert benchmark
class SqliteBulkInsertBenchmark extends SqliteBenchmark {
  SqliteBulkInsertBenchmark({
    required String dbPath,
    required int recordCount,
  }) : super(dbPath: dbPath, recordCount: recordCount);
  
  @override
  String get name => 'SQLite - Bulk Insert';
  
  @override
  Future<void> setup() async {
    await super.setup();
    results.clear();
  }
  
  @override
  Future<void> runIteration(int iteration) async {
    if (iteration > 0) return; // Run only once
    
    final batch = List.generate(recordCount, (i) => BenchmarkUser(
      id: 'bulk_user_$i',
      name: 'Bulk User $i',
      email: 'bulk$i@example.com',
      age: 20 + (i % 50),
      createdAt: DateTime.now(),
      metadata: {
        'index': i,
        'batch': true,
        'score': i * 0.1,
      },
    ));
    
    // Insert in batches using transaction
    final batchOp = BatchOperation(items: batch, batchSize: 1000);
    await batchOp.processBatches((items) async {
      await db.transaction((txn) async {
        for (final item in items) {
          await txn.insert('users', _userToMap(item));
        }
      });
    });
  }
}

/// Query benchmark
class SqliteQueryBenchmark extends SqliteBenchmark {
  SqliteQueryBenchmark({
    required String dbPath,
    required int recordCount,
  }) : super(dbPath: dbPath, recordCount: recordCount);
  
  @override
  String get name => 'SQLite - Query';
  
  @override
  Future<void> setup() async {
    await super.setup();
    
    // Insert test data
    final batch = List.generate(recordCount, (i) => BenchmarkUser(
      id: 'query_user_$i',
      name: 'Query User $i',
      email: 'query$i@example.com',
      age: 20 + (i % 50),
      createdAt: DateTime.now().subtract(Duration(days: i % 365)),
      metadata: {
        'city': ['New York', 'London', 'Tokyo', 'Paris'][i % 4],
        'active': i % 2 == 0,
        'score': (i % 100) * 1.0,
        'tags': ['developer', 'designer', 'manager', 'analyst'].sublist(0, (i % 4) + 1),
      },
    ));
    
    await BatchOperation(items: batch, batchSize: 1000).processBatches((items) async {
      await db.transaction((txn) async {
        for (final item in items) {
          await txn.insert('users', _userToMap(item));
        }
      });
    });
  }
  
  @override
  Future<void> runIteration(int iteration) async {
    // Run different query types
    switch (iteration % 5) {
      case 0:
        // Simple equality query
        await db.rawQuery('''
          SELECT * FROM users 
          WHERE json_extract(metadata, '\$.active') = 1
          LIMIT 100
        ''');
        break;
        
      case 1:
        // Range query
        await db.query(
          'users',
          where: 'age > ? AND age < ?',
          whereArgs: [30, 40],
          orderBy: 'age',
        );
        break;
        
      case 2:
        // Complex query
        await db.rawQuery('''
          SELECT * FROM users 
          WHERE json_extract(metadata, '\$.city') IN (?, ?)
          AND json_extract(metadata, '\$.score') > ?
          ORDER BY json_extract(metadata, '\$.score') DESC
          LIMIT 50
        ''', ['New York', 'London', 50]);
        break;
        
      case 3:
        // Text search
        await db.query(
          'users',
          where: "name LIKE ?",
          whereArgs: ['%5%'],
        );
        break;
        
      case 4:
        // Aggregation
        await db.rawQuery('''
          SELECT 
            COUNT(*) as total,
            AVG(age) as avgAge,
            MAX(json_extract(metadata, '\$.score')) as maxScore
          FROM users
        ''');
        break;
    }
  }
}

/// Transaction benchmark
class SqliteTransactionBenchmark extends SqliteBenchmark {
  SqliteTransactionBenchmark({
    required String dbPath,
    required int recordCount,
  }) : super(dbPath: dbPath, recordCount: recordCount);
  
  @override
  String get name => 'SQLite - Transaction';
  
  @override
  Future<void> setup() async {
    await super.setup();
    
    // Create initial accounts
    for (int i = 0; i < 10; i++) {
      await db.insert('users', _userToMap(BenchmarkUser(
        id: 'account_$i',
        name: 'Account $i',
        email: 'account$i@example.com',
        age: 30,
        createdAt: DateTime.now(),
        metadata: {
          'balance': 1000.0,
        },
      )));
    }
  }
  
  @override
  Future<void> runIteration(int iteration) async {
    final fromId = 'account_${iteration % 10}';
    final toId = 'account_${(iteration + 1) % 10}';
    final amount = 10.0;
    
    await db.transaction((txn) async {
      // Read both accounts
      final fromResult = await txn.query(
        'users',
        where: 'id = ?',
        whereArgs: [fromId],
      );
      final toResult = await txn.query(
        'users',
        where: 'id = ?',
        whereArgs: [toId],
      );
      
      if (fromResult.isNotEmpty && toResult.isNotEmpty) {
        final from = _userFromMap(fromResult.first);
        final to = _userFromMap(toResult.first);
        
        final fromBalance = (from.metadata['balance'] as num).toDouble();
        final toBalance = (to.metadata['balance'] as num).toDouble();
        
        // Update balances
        await txn.update(
          'users',
          _userToMap(from.copyWith(
            metadata: {...from.metadata, 'balance': fromBalance - amount},
          )),
          where: 'id = ?',
          whereArgs: [fromId],
        );
        
        await txn.update(
          'users',
          _userToMap(to.copyWith(
            metadata: {...to.metadata, 'balance': toBalance + amount},
          )),
          where: 'id = ?',
          whereArgs: [toId],
        );
      }
    });
  }
}

/// Concurrent access benchmark
class SqliteConcurrentBenchmark extends SqliteBenchmark {
  SqliteConcurrentBenchmark({
    required String dbPath,
    required int recordCount,
  }) : super(dbPath: dbPath, recordCount: recordCount);
  
  @override
  String get name => 'SQLite - Concurrent';
  
  @override
  Future<void> runIteration(int iteration) async {
    // Run multiple operations concurrently
    await Future.wait([
      // Writer 1
      db.insert('users', _userToMap(BenchmarkUser(
        id: 'concurrent_${iteration}_1',
        name: 'Concurrent User 1',
        email: 'concurrent1@example.com',
        age: 25,
        createdAt: DateTime.now(),
        metadata: {},
      ))),
      
      // Writer 2
      db.insert('users', _userToMap(BenchmarkUser(
        id: 'concurrent_${iteration}_2',
        name: 'Concurrent User 2',
        email: 'concurrent2@example.com',
        age: 30,
        createdAt: DateTime.now(),
        metadata: {},
      ))),
      
      // Reader 1
      db.query('users', limit: 10),
      
      // Reader 2
      db.query('users', where: 'age > ?', whereArgs: [25]),
    ]);
  }
}