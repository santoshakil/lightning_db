import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:lightning_db_example/main.dart';

void main() {
  testWidgets('Example app smoke test', (WidgetTester tester) async {
    // Build our app and trigger a frame.
    await tester.pumpWidget(const MyApp());

    // Verify that app starts with no todos
    expect(find.text('No todos yet. Add one!'), findsOneWidget);
    
    // Verify that we have an add button
    expect(find.byIcon(Icons.add), findsOneWidget);
  });
}