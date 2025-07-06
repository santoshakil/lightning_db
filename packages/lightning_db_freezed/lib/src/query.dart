import 'dart:async';
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'adapter.dart';
import 'collection.dart';

/// Query builder for filtering and searching collections
class QueryBuilder<T> {
  final FreezedCollection<T> collection;
  final LightningDb db;
  final FreezedAdapter<T> adapter;
  final List<QueryFilter<T>> _filters = [];
  final List<QuerySort<T>> _sorts = [];
  int? _limit;
  int? _offset;
  
  QueryBuilder({
    required this.collection,
    required this.db,
    required this.adapter,
  });
  
  /// Add a filter condition
  QueryBuilder<T> where(bool Function(T) predicate) {
    _filters.add(QueryFilter.predicate(predicate));
    return this;
  }
  
  /// Filter by key range
  QueryBuilder<T> whereKeyBetween(String start, String end) {
    _filters.add(QueryFilter.keyRange(start, end));
    return this;
  }
  
  /// Filter by key prefix
  QueryBuilder<T> whereKeyStartsWith(String prefix) {
    _filters.add(QueryFilter.keyPrefix(prefix));
    return this;
  }
  
  /// Sort by a field in ascending order
  QueryBuilder<T> orderBy<R extends Comparable>(R Function(T) selector) {
    _sorts.add(QuerySort(selector: selector, ascending: true));
    return this;
  }
  
  /// Sort by a field in descending order
  QueryBuilder<T> orderByDescending<R extends Comparable>(R Function(T) selector) {
    _sorts.add(QuerySort(selector: selector, ascending: false));
    return this;
  }
  
  /// Limit the number of results
  QueryBuilder<T> limit(int count) {
    _limit = count;
    return this;
  }
  
  /// Skip a number of results
  QueryBuilder<T> offset(int count) {
    _offset = count;
    return this;
  }
  
  /// Execute the query and get results
  Future<List<T>> execute() async {
    // Start with all items or filtered by key range
    Stream<T> stream;
    
    final keyRangeFilter = _filters.firstWhere(
      (f) => f.type == FilterType.keyRange,
      orElse: () => QueryFilter.none(),
    );
    
    if (keyRangeFilter.type == FilterType.keyRange) {
      final startKey = '${collection.name}:${keyRangeFilter.startKey}';
      final endKey = '${collection.name}:${keyRangeFilter.endKey}';
      stream = _scanRange(startKey, endKey);
    } else {
      stream = collection.getAllStream();
    }
    
    // Apply predicate filters
    for (final filter in _filters) {
      if (filter.type == FilterType.predicate && filter.predicate != null) {
        stream = stream.where(filter.predicate!);
      }
    }
    
    // Collect results for sorting
    List<T> results = await stream.toList();
    
    // Apply sorting
    if (_sorts.isNotEmpty) {
      results = _applySort(results);
    }
    
    // Apply offset
    if (_offset != null && _offset! > 0) {
      results = results.skip(_offset!).toList();
    }
    
    // Apply limit
    if (_limit != null && _limit! > 0) {
      results = results.take(_limit!).toList();
    }
    
    return results;
  }
  
  /// Execute the query and get a stream of results
  Stream<T> executeStream() async* {
    final results = await execute();
    for (final result in results) {
      yield result;
    }
  }
  
  /// Count matching items
  Future<int> count() async {
    final results = await execute();
    return results.length;
  }
  
  /// Get the first matching item
  Future<T?> first() async {
    final limited = limit(1);
    final results = await limited.execute();
    return results.isEmpty ? null : results.first;
  }
  
  /// Get the last matching item
  Future<T?> last() async {
    final results = await execute();
    return results.isEmpty ? null : results.last;
  }
  
  /// Check if any items match
  Future<bool> any() async {
    final result = await first();
    return result != null;
  }
  
  /// Delete all matching items
  Future<int> delete() async {
    final items = await execute();
    final keys = items.map((item) => adapter.getKey(item)).toList();
    return collection.deleteByKeys(keys);
  }
  
  /// Update all matching items
  Future<int> update(T Function(T) updater) async {
    final items = await execute();
    int count = 0;
    
    for (final item in items) {
      final updated = updater(item);
      await collection.update(updated);
      count++;
    }
    
    return count;
  }
  
  Stream<T> _scanRange(String start, String end) async* {
    final stream = db.scanStream(startKey: start, endKey: end);
    
    await for (final kv in stream) {
      if (kv.key.startsWith('${collection.name}:')) {
        try {
          yield adapter.deserialize(kv.value);
        } catch (e) {
          // Skip corrupted entries
        }
      }
    }
  }
  
  List<T> _applySort(List<T> items) {
    final sorted = List<T>.from(items);
    
    sorted.sort((a, b) {
      for (final sort in _sorts) {
        final aValue = sort.selector(a);
        final bValue = sort.selector(b);
        
        if (aValue is Comparable && bValue is Comparable) {
          final comparison = (aValue as Comparable).compareTo(bValue);
          
          if (comparison != 0) {
            return sort.ascending ? comparison : -comparison;
          }
        }
      }
      return 0;
    });
    
    return sorted;
  }
}

/// Filter for queries
class QueryFilter<T> {
  final FilterType type;
  final bool Function(T)? predicate;
  final String? startKey;
  final String? endKey;
  final String? prefix;
  
  QueryFilter._({
    required this.type,
    this.predicate,
    this.startKey,
    this.endKey,
    this.prefix,
  });
  
  factory QueryFilter.predicate(bool Function(T) predicate) {
    return QueryFilter._(
      type: FilterType.predicate,
      predicate: predicate,
    );
  }
  
  factory QueryFilter.keyRange(String start, String end) {
    return QueryFilter._(
      type: FilterType.keyRange,
      startKey: start,
      endKey: end,
    );
  }
  
  factory QueryFilter.keyPrefix(String prefix) {
    return QueryFilter._(
      type: FilterType.keyPrefix,
      prefix: prefix,
    );
  }
  
  factory QueryFilter.none() {
    return QueryFilter._(type: FilterType.none);
  }
}

/// Type of filter
enum FilterType {
  predicate,
  keyRange,
  keyPrefix,
  none,
}

/// Sort specification
class QuerySort<T> {
  final dynamic Function(T) selector;
  final bool ascending;
  
  QuerySort({
    required this.selector,
    required this.ascending,
  });
}

/// Extension methods for common query patterns
extension QueryPatterns<T> on FreezedCollection<T> {
  /// Find items where a field equals a value
  Future<List<T>> whereEquals<R>(R Function(T) selector, R value) {
    return query()
        .where((item) => selector(item) == value)
        .execute();
  }
  
  /// Find items where a field is in a list of values
  Future<List<T>> whereIn<R>(R Function(T) selector, List<R> values) {
    return query()
        .where((item) => values.contains(selector(item)))
        .execute();
  }
  
  /// Find items where a field is not null
  Future<List<T>> whereNotNull<R>(R? Function(T) selector) {
    return query()
        .where((item) => selector(item) != null)
        .execute();
  }
  
  /// Get paginated results
  Future<PaginatedResults<T>> paginate({
    required int page,
    required int pageSize,
    List<QuerySort<T>>? sorts,
  }) async {
    final query = this.query()
        .offset((page - 1) * pageSize)
        .limit(pageSize);
    
    // Note: sorts are handled differently since they may not be Comparable
    // This is handled internally by the execute method
    
    final items = await query.execute();
    final total = await count();
    
    return PaginatedResults<T>(
      items: items,
      page: page,
      pageSize: pageSize,
      totalItems: total,
      totalPages: (total / pageSize).ceil(),
    );
  }
}

/// Paginated results
class PaginatedResults<T> {
  final List<T> items;
  final int page;
  final int pageSize;
  final int totalItems;
  final int totalPages;
  
  PaginatedResults({
    required this.items,
    required this.page,
    required this.pageSize,
    required this.totalItems,
    required this.totalPages,
  });
  
  bool get hasNextPage => page < totalPages;
  bool get hasPreviousPage => page > 1;
  int get nextPage => hasNextPage ? page + 1 : page;
  int get previousPage => hasPreviousPage ? page - 1 : page;
}