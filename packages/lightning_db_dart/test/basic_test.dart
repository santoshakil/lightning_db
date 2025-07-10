import 'package:test/test.dart';
import 'package:lightning_db_dart/lightning_db_dart.dart';

void main() {
  test('basic import test', () {
    // Test that we can import the library without errors
    expect(CompressionType.none, isNotNull);
    expect(ConsistencyLevel.eventual, isNotNull);
  });
}