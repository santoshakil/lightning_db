import 'dart:io';
import 'dart:typed_data';
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'package:lightning_db_freezed/lightning_db_freezed.dart';
import 'benchmark_runner.dart';
import 'benchmark_models.dart';

/// Lightning DB benchmark implementation
class LightningDbBenchmark extends BenchmarkRunner {
  late LightningDb db;
  late FreezedCollection<BenchmarkUser> users;
  late FreezedCollection<BenchmarkPost> posts;
  late FreezedCollection<BenchmarkComment> comments;
  
  final String dbPath;
  final int recordCount;
  
  LightningDbBenchmark({
    required this.dbPath,
    required this.recordCount,
  }) : super('Lightning DB');
  
  @override
  Future<void> setup() async {
    // Delete existing database
    final dbFile = File(dbPath);
    if (await dbFile.exists()) {
      await dbFile.delete();
    }
    
    // Open database
    db = await LightningDb.open(dbPath);
    
    // Register adapters
    FreezedAdapter.register(BenchmarkUser.fromJson, (u) => u.toJson());
    FreezedAdapter.register(BenchmarkPost.fromJson, (p) => p.toJson());
    FreezedAdapter.register(BenchmarkComment.fromJson, (c) => c.toJson());
    
    // Create collections
    users = db.freezedCollection<BenchmarkUser>('users');
    posts = db.freezedCollection<BenchmarkPost>('posts');
    comments = db.freezedCollection<BenchmarkComment>('comments');
  }
  
  @override
  Future<void> cleanup() async {
    await db.close();
  }
  
  @override
  Future<void> runIteration(int iteration) async {
    // This will be overridden by specific benchmark types
  }
}

/// CRUD operations benchmark
class LightningDbCrudBenchmark extends LightningDbBenchmark {
  LightningDbCrudBenchmark({
    required String dbPath,
    required int recordCount,
  }) : super(dbPath: dbPath, recordCount: recordCount);
  
  @override
  String get name => 'Lightning DB - CRUD';
  
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
    await users.add(user);
    
    // Read
    final retrieved = await users.get(user.id);
    
    // Update
    if (retrieved != null) {
      await users.update(retrieved.copyWith(
        name: 'Updated ${retrieved.name}',
        metadata: {...retrieved.metadata, 'updated': true},
      ));
    }
    
    // Read again
    await users.get(user.id);
    
    // Delete
    await users.delete(user.id);
  }
}

/// Bulk insert benchmark
class LightningDbBulkInsertBenchmark extends LightningDbBenchmark {
  LightningDbBulkInsertBenchmark({
    required String dbPath,
    required int recordCount,
  }) : super(dbPath: dbPath, recordCount: recordCount);
  
  @override
  String get name => 'Lightning DB - Bulk Insert';
  
  @override
  Future<void> setup() async {
    await super.setup();
    results.clear(); // Clear results from parent
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
    
    // Insert in batches of 1000
    final batchOp = BatchOperation(items: batch, batchSize: 1000);
    await batchOp.processBatches((items) async {
      await db.transaction((tx) async {
        for (final item in items) {
          await users.add(item);
        }
      });
    });
  }
}

/// Query benchmark
class LightningDbQueryBenchmark extends LightningDbBenchmark {
  LightningDbQueryBenchmark({
    required String dbPath,
    required int recordCount,
  }) : super(dbPath: dbPath, recordCount: recordCount);
  
  @override
  String get name => 'Lightning DB - Query';
  
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
      await db.transaction((tx) async {
        for (final item in items) {
          await users.add(item);
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
        await users.query()
          .where('metadata.active', isEqualTo: true)
          .limit(100)
          .findAll();
        break;
        
      case 1:
        // Range query
        await users.query()
          .where('age', isGreaterThan: 30)
          .where('age', isLessThan: 40)
          .orderBy('age')
          .findAll();
        break;
        
      case 2:
        // Complex query
        await users.query()
          .where('metadata.city', whereIn: ['New York', 'London'])
          .where('metadata.score', isGreaterThan: 50)
          .orderBy('metadata.score', descending: true)
          .limit(50)
          .findAll();
        break;
        
      case 3:
        // Text search
        await users.query()
          .where('name', contains: '5')
          .findAll();
        break;
        
      case 4:
        // Aggregation
        await users.query()
          .aggregate()
          .count('total')
          .average('age', 'avgAge')
          .max('metadata.score', 'maxScore')
          .execute();
        break;
    }
  }
}

/// Transaction benchmark
class LightningDbTransactionBenchmark extends LightningDbBenchmark {
  LightningDbTransactionBenchmark({
    required String dbPath,
    required int recordCount,
  }) : super(dbPath: dbPath, recordCount: recordCount);
  
  @override
  String get name => 'Lightning DB - Transaction';
  
  @override
  Future<void> setup() async {
    await super.setup();
    
    // Create initial accounts
    for (int i = 0; i < 10; i++) {
      await users.add(BenchmarkUser(
        id: 'account_$i',
        name: 'Account $i',
        email: 'account$i@example.com',
        age: 30,
        createdAt: DateTime.now(),
        metadata: {
          'balance': 1000.0,
        },
      ));
    }
  }
  
  @override
  Future<void> runIteration(int iteration) async {
    final fromId = 'account_${iteration % 10}';
    final toId = 'account_${(iteration + 1) % 10}';
    final amount = 10.0;
    
    await db.transaction((tx) async {
      // Read both accounts
      final from = await users.get(fromId);
      final to = await users.get(toId);
      
      if (from != null && to != null) {
        final fromBalance = (from.metadata['balance'] as num).toDouble();
        final toBalance = (to.metadata['balance'] as num).toDouble();
        
        // Update balances
        await users.update(from.copyWith(
          metadata: {...from.metadata, 'balance': fromBalance - amount},
        ));
        
        await users.update(to.copyWith(
          metadata: {...to.metadata, 'balance': toBalance + amount},
        ));
      }
    });
  }
}

/// Concurrent access benchmark
class LightningDbConcurrentBenchmark extends LightningDbBenchmark {
  LightningDbConcurrentBenchmark({
    required String dbPath,
    required int recordCount,
  }) : super(dbPath: dbPath, recordCount: recordCount);
  
  @override
  String get name => 'Lightning DB - Concurrent';
  
  @override
  Future<void> runIteration(int iteration) async {
    // Run multiple operations concurrently
    await Future.wait([
      // Writer 1
      users.add(BenchmarkUser(
        id: 'concurrent_${iteration}_1',
        name: 'Concurrent User 1',
        email: 'concurrent1@example.com',
        age: 25,
        createdAt: DateTime.now(),
        metadata: {},
      )),
      
      // Writer 2
      users.add(BenchmarkUser(
        id: 'concurrent_${iteration}_2',
        name: 'Concurrent User 2',
        email: 'concurrent2@example.com',
        age: 30,
        createdAt: DateTime.now(),
        metadata: {},
      )),
      
      // Reader 1
      users.query().limit(10).findAll(),
      
      // Reader 2
      users.query().where('age', isGreaterThan: 25).findAll(),
    ]);
  }
}

/// Raw key-value benchmark (bypassing Freezed)
class LightningDbRawBenchmark extends BenchmarkRunner {
  late LightningDb db;
  final String dbPath;
  final int recordCount;
  
  LightningDbRawBenchmark({
    required this.dbPath,
    required this.recordCount,
  }) : super('Lightning DB - Raw KV');
  
  @override
  Future<void> setup() async {
    // Delete existing database
    final dbFile = File(dbPath);
    if (await dbFile.exists()) {
      await dbFile.delete();
    }
    
    // Open database
    db = await LightningDb.open(dbPath);
  }
  
  @override
  Future<void> cleanup() async {
    await db.close();
  }
  
  @override
  Future<void> runIteration(int iteration) async {
    final key = 'raw_key_$iteration';
    final value = Uint8List.fromList('value_$iteration'.codeUnits);
    
    // Put
    await db.put(key, value);
    
    // Get
    await db.get(key);
    
    // Delete
    await db.delete(key);
  }
}