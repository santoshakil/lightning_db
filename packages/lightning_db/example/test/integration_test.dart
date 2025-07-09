import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:lightning_db/lightning_db.dart';
import 'package:example/main.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Lightning DB Integration Tests', () {
    late Database db;
    
    setUpAll(() async {
      // Initialize Lightning DB
      await LightningDB.initialize();
      db = await Database.open('test_db');
    });
    
    tearDownAll(() async {
      await db.close();
      await Database.destroy('test_db');
    });
    
    tearDown(() async {
      // Clear all data between tests
      await db.clear();
    });

    testWidgets('App launches successfully', (WidgetTester tester) async {
      await tester.pumpWidget(const MyApp());
      await tester.pumpAndSettle();
      
      expect(find.text('Lightning DB Demo'), findsOneWidget);
      expect(find.text('Todo App'), findsOneWidget);
    });

    test('Basic CRUD operations', () async {
      // Create
      await db.put('key1', 'value1');
      await db.put('key2', 'value2');
      
      // Read
      final value1 = await db.get('key1');
      expect(value1, equals('value1'));
      
      // Update
      await db.put('key1', 'updated_value1');
      final updatedValue = await db.get('key1');
      expect(updatedValue, equals('updated_value1'));
      
      // Delete
      await db.delete('key1');
      final deletedValue = await db.get('key1');
      expect(deletedValue, isNull);
    });

    test('Transaction operations', () async {
      final tx = await db.beginTransaction();
      
      try {
        await tx.put('tx_key1', 'tx_value1');
        await tx.put('tx_key2', 'tx_value2');
        await tx.commit();
        
        final value = await db.get('tx_key1');
        expect(value, equals('tx_value1'));
      } catch (e) {
        await tx.rollback();
        rethrow;
      }
    });

    test('Batch operations', () async {
      final batch = db.batch();
      
      for (int i = 0; i < 100; i++) {
        batch.put('batch_key_$i', 'batch_value_$i');
      }
      
      await batch.commit();
      
      // Verify all values
      for (int i = 0; i < 100; i++) {
        final value = await db.get('batch_key_$i');
        expect(value, equals('batch_value_$i'));
      }
    });

    test('Range queries', () async {
      // Insert sorted data
      for (int i = 0; i < 10; i++) {
        await db.put('range_${i.toString().padLeft(2, '0')}', 'value_$i');
      }
      
      // Range query
      final results = await db.range(
        start: 'range_03',
        end: 'range_07',
      );
      
      expect(results.length, equals(4));
      expect(results.first.key, equals('range_03'));
      expect(results.last.key, equals('range_06'));
    });

    test('Performance benchmark', () async {
      final stopwatch = Stopwatch()..start();
      
      // Write 1000 items
      for (int i = 0; i < 1000; i++) {
        await db.put('perf_key_$i', 'perf_value_$i');
      }
      
      final writeTime = stopwatch.elapsedMilliseconds;
      stopwatch.reset();
      
      // Read 1000 items
      for (int i = 0; i < 1000; i++) {
        await db.get('perf_key_$i');
      }
      
      final readTime = stopwatch.elapsedMilliseconds;
      
      print('Write 1000 items: ${writeTime}ms');
      print('Read 1000 items: ${readTime}ms');
      
      // Ensure reasonable performance
      expect(writeTime, lessThan(5000)); // Less than 5 seconds
      expect(readTime, lessThan(1000)); // Less than 1 second
    });

    test('Concurrent operations', () async {
      final futures = <Future>[];
      
      // Spawn 10 concurrent writers
      for (int i = 0; i < 10; i++) {
        futures.add(() async {
          for (int j = 0; j < 100; j++) {
            await db.put('concurrent_${i}_$j', 'value_${i}_$j');
          }
        }());
      }
      
      await Future.wait(futures);
      
      // Verify all writes succeeded
      int count = 0;
      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 100; j++) {
          final value = await db.get('concurrent_${i}_$j');
          if (value != null) count++;
        }
      }
      
      expect(count, equals(1000));
    });

    test('Error handling', () async {
      // Test invalid key
      expect(
        () async => await db.put('', 'value'),
        throwsA(isA<DatabaseException>()),
      );
      
      // Test transaction after close
      final tx = await db.beginTransaction();
      await tx.rollback();
      
      expect(
        () async => await tx.put('key', 'value'),
        throwsA(isA<DatabaseException>()),
      );
    });

    test('Data persistence', () async {
      // Write data
      await db.put('persist_key', 'persist_value');
      
      // Close and reopen
      await db.close();
      db = await Database.open('test_db');
      
      // Verify data persists
      final value = await db.get('persist_key');
      expect(value, equals('persist_value'));
    });

    testWidgets('Todo screen functionality', (WidgetTester tester) async {
      await tester.pumpWidget(const MyApp());
      await tester.pumpAndSettle();
      
      // Navigate to Todo screen
      await tester.tap(find.text('Todo App'));
      await tester.pumpAndSettle();
      
      // Add a todo
      await tester.enterText(find.byType(TextField).first, 'Test Todo');
      await tester.enterText(find.byType(TextField).last, 'Test Description');
      await tester.tap(find.text('Add Todo'));
      await tester.pumpAndSettle();
      
      // Verify todo appears
      expect(find.text('Test Todo'), findsOneWidget);
      expect(find.text('Test Description'), findsOneWidget);
    });
  });
}