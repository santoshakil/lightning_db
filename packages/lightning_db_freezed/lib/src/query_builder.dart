import 'dart:async';
import 'dart:convert';
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'package:meta/meta.dart';
import 'adapter.dart';
import 'collection.dart';
import 'error_handling.dart';

/// Advanced query builder for Freezed collections
class AdvancedQueryBuilder<T> {
  final FreezedCollection<T> collection;
  final LightningDb db;
  final FreezedAdapter<T> adapter;
  final List<QueryCondition> _conditions = [];
  final List<SortSpecification> _sorts = [];
  final List<String> _projections = [];
  final List<String> _indexHints = [];
  int? _limit;
  int? _offset;
  bool _distinct = false;
  bool _explain = false;

  AdvancedQueryBuilder({
    required this.collection,
    required this.db,
    required this.adapter,
  });

  /// Add a field-based condition
  AdvancedQueryBuilder<T> where(
    String field, {
    dynamic isEqualTo,
    dynamic isNotEqualTo,
    dynamic isGreaterThan,
    dynamic isGreaterThanOrEqualTo,
    dynamic isLessThan,
    dynamic isLessThanOrEqualTo,
    bool? isNull,
    dynamic contains,
    dynamic containsAny,
    dynamic containsAll,
    String? startsWith,
    String? endsWith,
    String? matches,
    List<dynamic>? whereIn,
    List<dynamic>? whereNotIn,
  }) {
    _conditions.add(FieldCondition(
      field: field,
      isEqualTo: isEqualTo,
      isNotEqualTo: isNotEqualTo,
      isGreaterThan: isGreaterThan,
      isGreaterThanOrEqualTo: isGreaterThanOrEqualTo,
      isLessThan: isLessThan,
      isLessThanOrEqualTo: isLessThanOrEqualTo,
      isNull: isNull,
      contains: contains,
      containsAny: containsAny,
      containsAll: containsAll,
      startsWith: startsWith,
      endsWith: endsWith,
      matches: matches,
      whereIn: whereIn,
      whereNotIn: whereNotIn,
    ));
    return this;
  }

  /// Add a custom predicate condition
  AdvancedQueryBuilder<T> wherePredicate(bool Function(T) predicate, [String? description]) {
    _conditions.add(PredicateCondition(predicate, description));
    return this;
  }

  /// Add an OR condition group
  AdvancedQueryBuilder<T> orWhere(List<QueryCondition> conditions) {
    _conditions.add(OrCondition(conditions));
    return this;
  }

  /// Add an AND condition group (default behavior)
  AdvancedQueryBuilder<T> andWhere(List<QueryCondition> conditions) {
    _conditions.add(AndCondition(conditions));
    return this;
  }

  /// Negate a condition
  AdvancedQueryBuilder<T> not(QueryCondition condition) {
    _conditions.add(NotCondition(condition));
    return this;
  }

  /// Order by a field
  AdvancedQueryBuilder<T> orderBy(String field, {bool descending = false}) {
    _sorts.add(SortSpecification(field: field, descending: descending));
    return this;
  }

  /// Select specific fields (projection)
  AdvancedQueryBuilder<T> select(List<String> fields) {
    _projections.addAll(fields);
    return this;
  }

  /// Add index hint
  AdvancedQueryBuilder<T> useIndex(String indexName) {
    _indexHints.add(indexName);
    return this;
  }

  /// Limit results
  AdvancedQueryBuilder<T> limit(int count) {
    _limit = count;
    return this;
  }

  /// Skip results
  AdvancedQueryBuilder<T> offset(int count) {
    _offset = count;
    return this;
  }

  /// Get distinct results
  AdvancedQueryBuilder<T> distinct() {
    _distinct = true;
    return this;
  }

  /// Enable query explanation
  AdvancedQueryBuilder<T> explain() {
    _explain = true;
    return this;
  }

  /// Execute the query
  Future<List<T>> findAll() async {
    final queryPlan = _buildQueryPlan();
    
    if (_explain) {
      print('Query Explanation:');
      print(queryPlan.explain());
    }

    return await _executeQuery(queryPlan);
  }

  /// Find first matching document
  Future<T?> findFirst() async {
    final results = await limit(1).findAll();
    return results.isEmpty ? null : results.first;
  }

  /// Count matching documents
  Future<int> count() async {
    final results = await findAll();
    return results.length;
  }

  /// Check if any documents match
  Future<bool> exists() async {
    final result = await findFirst();
    return result != null;
  }

  /// Get a stream of results
  Stream<T> snapshots() async* {
    final results = await findAll();
    for (final result in results) {
      yield result;
    }

    // Listen for changes
    await for (final change in collection.changes) {
      if (_matchesConditions(change.newValue ?? change.oldValue!)) {
        yield* Stream.fromIterable(await findAll());
      }
    }
  }

  /// Delete matching documents
  Future<int> delete() async {
    final items = await findAll();
    var count = 0;
    
    for (final item in items) {
      await collection.delete(adapter.getId(item));
      count++;
    }
    
    return count;
  }

  /// Update matching documents
  Future<int> update(T Function(T) updater) async {
    final items = await findAll();
    var count = 0;
    
    for (final item in items) {
      await collection.update(updater(item));
      count++;
    }
    
    return count;
  }

  /// Aggregate results
  Future<AggregationResult> aggregate(List<Aggregation> aggregations) async {
    final items = await findAll();
    final results = <String, dynamic>{};
    
    for (final agg in aggregations) {
      results[agg.name] = await agg.compute(items);
    }
    
    return AggregationResult(results);
  }

  /// Build query plan
  QueryPlan _buildQueryPlan() {
    return QueryPlan(
      collection: collection.name,
      conditions: _conditions,
      sorts: _sorts,
      projections: _projections,
      indexHints: _indexHints,
      limit: _limit,
      offset: _offset,
      distinct: _distinct,
    );
  }

  /// Execute query with plan
  Future<List<T>> _executeQuery(QueryPlan plan) async {
    // Start with all documents or use index
    var candidates = await _getCandidates(plan);
    
    // Apply conditions
    candidates = _applyConditions(candidates, plan.conditions);
    
    // Apply distinct
    if (plan.distinct) {
      candidates = _applyDistinct(candidates);
    }
    
    // Apply sorting
    if (plan.sorts.isNotEmpty) {
      candidates = _applySort(candidates, plan.sorts);
    }
    
    // Apply offset
    if (plan.offset != null) {
      candidates = candidates.skip(plan.offset!).toList();
    }
    
    // Apply limit
    if (plan.limit != null) {
      candidates = candidates.take(plan.limit!).toList();
    }
    
    // Apply projections if needed
    if (plan.projections.isNotEmpty) {
      // Note: Projections would require partial deserialization
      // For now, we return full objects
    }
    
    return candidates;
  }

  /// Get initial candidates
  Future<List<T>> _getCandidates(QueryPlan plan) async {
    // Check if we can use an index
    final indexCondition = _findIndexableCondition(plan.conditions);
    
    if (indexCondition != null && plan.indexHints.isNotEmpty) {
      // Use index scan
      return await _indexScan(indexCondition, plan.indexHints.first);
    }
    
    // Full collection scan
    return await collection.getAll();
  }

  /// Find condition that can use an index
  FieldCondition? _findIndexableCondition(List<QueryCondition> conditions) {
    for (final condition in conditions) {
      if (condition is FieldCondition) {
        return condition;
      }
    }
    return null;
  }

  /// Perform index scan
  Future<List<T>> _indexScan(FieldCondition condition, String indexName) async {
    // This would use an actual index if implemented
    // For now, fallback to full scan with early filtering
    return await collection.getAll();
  }

  /// Apply conditions to candidates
  List<T> _applyConditions(List<T> candidates, List<QueryCondition> conditions) {
    if (conditions.isEmpty) return candidates;
    
    return candidates.where((item) {
      for (final condition in conditions) {
        if (!_evaluateCondition(item, condition)) {
          return false;
        }
      }
      return true;
    }).toList();
  }

  /// Evaluate a single condition
  bool _evaluateCondition(T item, QueryCondition condition) {
    if (condition is FieldCondition) {
      return _evaluateFieldCondition(item, condition);
    } else if (condition is PredicateCondition<T>) {
      return condition.predicate(item);
    } else if (condition is OrCondition) {
      return condition.conditions.any((c) => _evaluateCondition(item, c));
    } else if (condition is AndCondition) {
      return condition.conditions.every((c) => _evaluateCondition(item, c));
    } else if (condition is NotCondition) {
      return !_evaluateCondition(item, condition.condition);
    }
    return true;
  }

  /// Evaluate field-based condition
  bool _evaluateFieldCondition(T item, FieldCondition condition) {
    final value = _getFieldValue(item, condition.field);
    
    // Null check
    if (condition.isNull != null) {
      return (value == null) == condition.isNull!;
    }
    
    // Skip if value is null and not checking for null
    if (value == null) return false;
    
    // Equality checks
    if (condition.isEqualTo != null) {
      return value == condition.isEqualTo;
    }
    
    if (condition.isNotEqualTo != null) {
      return value != condition.isNotEqualTo;
    }
    
    // Comparison checks
    if (value is Comparable) {
      if (condition.isGreaterThan != null && condition.isGreaterThan is Comparable) {
        if (value.compareTo(condition.isGreaterThan) <= 0) return false;
      }
      
      if (condition.isGreaterThanOrEqualTo != null && condition.isGreaterThanOrEqualTo is Comparable) {
        if (value.compareTo(condition.isGreaterThanOrEqualTo) < 0) return false;
      }
      
      if (condition.isLessThan != null && condition.isLessThan is Comparable) {
        if (value.compareTo(condition.isLessThan) >= 0) return false;
      }
      
      if (condition.isLessThanOrEqualTo != null && condition.isLessThanOrEqualTo is Comparable) {
        if (value.compareTo(condition.isLessThanOrEqualTo) > 0) return false;
      }
    }
    
    // String checks
    if (value is String) {
      if (condition.startsWith != null && !value.startsWith(condition.startsWith!)) {
        return false;
      }
      
      if (condition.endsWith != null && !value.endsWith(condition.endsWith!)) {
        return false;
      }
      
      if (condition.contains != null && !value.contains(condition.contains.toString())) {
        return false;
      }
      
      if (condition.matches != null) {
        final regex = RegExp(condition.matches!);
        if (!regex.hasMatch(value)) return false;
      }
    }
    
    // List checks
    if (value is List) {
      if (condition.contains != null && !value.contains(condition.contains)) {
        return false;
      }
      
      if (condition.containsAny != null && condition.containsAny is List) {
        final anyList = condition.containsAny as List;
        if (!anyList.any((item) => value.contains(item))) return false;
      }
      
      if (condition.containsAll != null && condition.containsAll is List) {
        final allList = condition.containsAll as List;
        if (!allList.every((item) => value.contains(item))) return false;
      }
    }
    
    // In/NotIn checks
    if (condition.whereIn != null && !condition.whereIn!.contains(value)) {
      return false;
    }
    
    if (condition.whereNotIn != null && condition.whereNotIn!.contains(value)) {
      return false;
    }
    
    return true;
  }

  /// Get field value from object
  dynamic _getFieldValue(T item, String field) {
    final json = adapter.toJson(item);
    final parts = field.split('.');
    
    dynamic current = json;
    for (final part in parts) {
      if (current is Map) {
        current = current[part];
      } else {
        return null;
      }
    }
    
    return current;
  }

  /// Apply distinct filter
  List<T> _applyDistinct(List<T> items) {
    final seen = <String>{};
    final distinct = <T>[];
    
    for (final item in items) {
      final key = jsonEncode(adapter.toJson(item));
      if (!seen.contains(key)) {
        seen.add(key);
        distinct.add(item);
      }
    }
    
    return distinct;
  }

  /// Apply sorting
  List<T> _applySort(List<T> items, List<SortSpecification> sorts) {
    final sorted = List<T>.from(items);
    
    sorted.sort((a, b) {
      for (final sort in sorts) {
        final aValue = _getFieldValue(a, sort.field);
        final bValue = _getFieldValue(b, sort.field);
        
        if (aValue == null && bValue == null) continue;
        if (aValue == null) return sort.descending ? 1 : -1;
        if (bValue == null) return sort.descending ? -1 : 1;
        
        if (aValue is Comparable && bValue is Comparable) {
          final comparison = aValue.compareTo(bValue);
          if (comparison != 0) {
            return sort.descending ? -comparison : comparison;
          }
        }
      }
      return 0;
    });
    
    return sorted;
  }

  /// Check if item matches conditions
  bool _matchesConditions(T item) {
    return _conditions.every((condition) => _evaluateCondition(item, condition));
  }
}

/// Base class for query conditions
abstract class QueryCondition {}

/// Field-based condition
class FieldCondition extends QueryCondition {
  final String field;
  final dynamic isEqualTo;
  final dynamic isNotEqualTo;
  final dynamic isGreaterThan;
  final dynamic isGreaterThanOrEqualTo;
  final dynamic isLessThan;
  final dynamic isLessThanOrEqualTo;
  final bool? isNull;
  final dynamic contains;
  final dynamic containsAny;
  final dynamic containsAll;
  final String? startsWith;
  final String? endsWith;
  final String? matches;
  final List<dynamic>? whereIn;
  final List<dynamic>? whereNotIn;

  FieldCondition({
    required this.field,
    this.isEqualTo,
    this.isNotEqualTo,
    this.isGreaterThan,
    this.isGreaterThanOrEqualTo,
    this.isLessThan,
    this.isLessThanOrEqualTo,
    this.isNull,
    this.contains,
    this.containsAny,
    this.containsAll,
    this.startsWith,
    this.endsWith,
    this.matches,
    this.whereIn,
    this.whereNotIn,
  });
}

/// Predicate-based condition
class PredicateCondition<T> extends QueryCondition {
  final bool Function(T) predicate;
  final String? description;

  PredicateCondition(this.predicate, this.description);
}

/// OR condition group
class OrCondition extends QueryCondition {
  final List<QueryCondition> conditions;

  OrCondition(this.conditions);
}

/// AND condition group
class AndCondition extends QueryCondition {
  final List<QueryCondition> conditions;

  AndCondition(this.conditions);
}

/// NOT condition
class NotCondition extends QueryCondition {
  final QueryCondition condition;

  NotCondition(this.condition);
}

/// Sort specification
class SortSpecification {
  final String field;
  final bool descending;

  SortSpecification({
    required this.field,
    this.descending = false,
  });
}

/// Query plan for execution
class QueryPlan {
  final String collection;
  final List<QueryCondition> conditions;
  final List<SortSpecification> sorts;
  final List<String> projections;
  final List<String> indexHints;
  final int? limit;
  final int? offset;
  final bool distinct;

  QueryPlan({
    required this.collection,
    required this.conditions,
    required this.sorts,
    required this.projections,
    required this.indexHints,
    this.limit,
    this.offset,
    this.distinct = false,
  });

  /// Explain the query plan
  String explain() {
    final buffer = StringBuffer();
    buffer.writeln('Query Plan for collection: $collection');
    buffer.writeln('----------------------------------------');
    
    if (indexHints.isNotEmpty) {
      buffer.writeln('Index hints: ${indexHints.join(', ')}');
    }
    
    if (conditions.isNotEmpty) {
      buffer.writeln('Conditions: ${conditions.length}');
      for (final condition in conditions) {
        buffer.writeln('  - ${_explainCondition(condition)}');
      }
    }
    
    if (sorts.isNotEmpty) {
      buffer.writeln('Sort by:');
      for (final sort in sorts) {
        buffer.writeln('  - ${sort.field} ${sort.descending ? 'DESC' : 'ASC'}');
      }
    }
    
    if (projections.isNotEmpty) {
      buffer.writeln('Select fields: ${projections.join(', ')}');
    }
    
    if (distinct) {
      buffer.writeln('Distinct: true');
    }
    
    if (offset != null) {
      buffer.writeln('Offset: $offset');
    }
    
    if (limit != null) {
      buffer.writeln('Limit: $limit');
    }
    
    return buffer.toString();
  }

  String _explainCondition(QueryCondition condition) {
    if (condition is FieldCondition) {
      final parts = <String>[];
      if (condition.isEqualTo != null) parts.add('${condition.field} = ${condition.isEqualTo}');
      if (condition.isNotEqualTo != null) parts.add('${condition.field} != ${condition.isNotEqualTo}');
      if (condition.isGreaterThan != null) parts.add('${condition.field} > ${condition.isGreaterThan}');
      if (condition.isLessThan != null) parts.add('${condition.field} < ${condition.isLessThan}');
      if (condition.isNull != null) parts.add('${condition.field} IS ${condition.isNull! ? 'NULL' : 'NOT NULL'}');
      if (condition.startsWith != null) parts.add('${condition.field} STARTS WITH "${condition.startsWith}"');
      if (condition.contains != null) parts.add('${condition.field} CONTAINS ${condition.contains}');
      return parts.join(' AND ');
    } else if (condition is PredicateCondition) {
      return condition.description ?? 'Custom predicate';
    } else if (condition is OrCondition) {
      return 'OR(${condition.conditions.map(_explainCondition).join(', ')})';
    } else if (condition is AndCondition) {
      return 'AND(${condition.conditions.map(_explainCondition).join(', ')})';
    } else if (condition is NotCondition) {
      return 'NOT(${_explainCondition(condition.condition)})';
    }
    return 'Unknown condition';
  }
}

/// Aggregation operation
abstract class Aggregation {
  final String name;
  final String field;

  Aggregation(this.name, this.field);

  Future<dynamic> compute<T>(List<T> items);
}

/// Count aggregation
class CountAggregation extends Aggregation {
  CountAggregation(String name) : super(name, '');

  @override
  Future<int> compute<T>(List<T> items) async {
    return items.length;
  }
}

/// Sum aggregation
class SumAggregation extends Aggregation {
  SumAggregation(String name, String field) : super(name, field);

  @override
  Future<num> compute<T>(List<T> items) async {
    num sum = 0;
    for (final item in items) {
      final value = _getFieldValue(item, field);
      if (value is num) {
        sum += value;
      }
    }
    return sum;
  }

  dynamic _getFieldValue(dynamic item, String field) {
    // This would need access to the adapter
    // For now, simplified implementation
    return null;
  }
}

/// Average aggregation
class AverageAggregation extends Aggregation {
  AverageAggregation(String name, String field) : super(name, field);

  @override
  Future<double> compute<T>(List<T> items) async {
    if (items.isEmpty) return 0;
    
    final sum = await SumAggregation('', field).compute(items);
    return sum / items.length;
  }
}

/// Min aggregation
class MinAggregation extends Aggregation {
  MinAggregation(String name, String field) : super(name, field);

  @override
  Future<dynamic> compute<T>(List<T> items) async {
    dynamic min;
    for (final item in items) {
      final value = _getFieldValue(item, field);
      if (value != null && value is Comparable) {
        if (min == null || value.compareTo(min) < 0) {
          min = value;
        }
      }
    }
    return min;
  }

  dynamic _getFieldValue(dynamic item, String field) {
    // This would need access to the adapter
    return null;
  }
}

/// Max aggregation
class MaxAggregation extends Aggregation {
  MaxAggregation(String name, String field) : super(name, field);

  @override
  Future<dynamic> compute<T>(List<T> items) async {
    dynamic max;
    for (final item in items) {
      final value = _getFieldValue(item, field);
      if (value != null && value is Comparable) {
        if (max == null || value.compareTo(max) > 0) {
          max = value;
        }
      }
    }
    return max;
  }

  dynamic _getFieldValue(dynamic item, String field) {
    // This would need access to the adapter
    return null;
  }
}

/// Aggregation result
class AggregationResult {
  final Map<String, dynamic> results;

  AggregationResult(this.results);

  T? get<T>(String name) => results[name] as T?;
}

/// Extension to add advanced query to collection
extension AdvancedQueryExtension<T> on FreezedCollection<T> {
  /// Create an advanced query builder
  AdvancedQueryBuilder<T> advancedQuery() {
    return AdvancedQueryBuilder<T>(
      collection: this,
      db: _db,
      adapter: adapter,
    );
  }
}

// Helper to access private db field
extension on FreezedCollection {
  LightningDb get _db => (this as dynamic)._db as LightningDb;
}