import 'dart:async';
import 'package:meta/meta.dart';
import 'collection.dart';
import 'query_builder.dart';

/// Simplified query API for common use cases
class CollectionQuery<T> {
  final AdvancedQueryBuilder<T> _builder;
  
  CollectionQuery(this._builder);

  /// Add a where condition
  CollectionQuery<T> where(
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
    _builder.where(
      field,
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
    );
    return this;
  }

  /// Order by field
  CollectionQuery<T> orderBy(String field, {bool descending = false}) {
    _builder.orderBy(field, descending: descending);
    return this;
  }

  /// Limit results
  CollectionQuery<T> limit(int count) {
    _builder.limit(count);
    return this;
  }

  /// Skip results
  CollectionQuery<T> offset(int count) {
    _builder.offset(count);
    return this;
  }

  /// Select specific fields
  CollectionQuery<T> select(List<String> fields) {
    _builder.select(fields);
    return this;
  }

  /// Get distinct results
  CollectionQuery<T> distinct() {
    _builder.distinct();
    return this;
  }

  /// Find all matching documents
  Future<List<T>> findAll() => _builder.findAll();

  /// Find first matching document
  Future<T?> findFirst() => _builder.findFirst();

  /// Count matching documents
  Future<int> count() => _builder.count();

  /// Check if any documents match
  Future<bool> exists() => _builder.exists();

  /// Get real-time updates
  Stream<List<T>> snapshots() => _builder.snapshots();

  /// Delete matching documents
  Future<int> delete() => _builder.delete();

  /// Update matching documents
  Future<int> update(T Function(T) updater) => _builder.update(updater);
}

/// Query builder extensions for common patterns
extension QueryPatterns<T> on CollectionQuery<T> {
  /// Find by ID
  Future<T?> findById(String id) {
    return where('id', isEqualTo: id).findFirst();
  }

  /// Find by multiple IDs
  Future<List<T>> findByIds(List<String> ids) {
    return where('id', whereIn: ids).findAll();
  }

  /// Find documents created after a date
  CollectionQuery<T> createdAfter(DateTime date) {
    return where('createdAt', isGreaterThan: date.toIso8601String());
  }

  /// Find documents created before a date
  CollectionQuery<T> createdBefore(DateTime date) {
    return where('createdAt', isLessThan: date.toIso8601String());
  }

  /// Find documents created between dates
  CollectionQuery<T> createdBetween(DateTime start, DateTime end) {
    return where('createdAt', isGreaterThanOrEqualTo: start.toIso8601String())
        .where('createdAt', isLessThanOrEqualTo: end.toIso8601String());
  }

  /// Find active documents
  CollectionQuery<T> active() {
    return where('active', isEqualTo: true)
        .where('deletedAt', isNull: true);
  }

  /// Find deleted documents (soft delete)
  CollectionQuery<T> deleted() {
    return where('deletedAt', isNull: false);
  }

  /// Paginate results
  Future<PagedResult<T>> page(int pageNumber, {int pageSize = 20}) async {
    final totalCount = await count();
    final items = await offset((pageNumber - 1) * pageSize)
        .limit(pageSize)
        .findAll();
    
    return PagedResult<T>(
      items: items,
      page: pageNumber,
      pageSize: pageSize,
      totalItems: totalCount,
      totalPages: (totalCount / pageSize).ceil(),
    );
  }
}

/// Paged result container
class PagedResult<T> {
  final List<T> items;
  final int page;
  final int pageSize;
  final int totalItems;
  final int totalPages;

  PagedResult({
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
  
  Map<String, dynamic> toJson() => {
    'items': items,
    'page': page,
    'pageSize': pageSize,
    'totalItems': totalItems,
    'totalPages': totalPages,
    'hasNextPage': hasNextPage,
    'hasPreviousPage': hasPreviousPage,
  };
}

/// Text search query builder
class TextSearchQuery<T> {
  final CollectionQuery<T> _query;
  final List<String> _searchFields;
  final String _searchTerm;
  
  TextSearchQuery(this._query, this._searchFields, this._searchTerm);

  /// Search in all specified fields
  CollectionQuery<T> search() {
    if (_searchFields.isEmpty || _searchTerm.isEmpty) {
      return _query;
    }

    // Create OR conditions for each field
    final conditions = <FieldCondition>[];
    for (final field in _searchFields) {
      conditions.add(FieldCondition(
        field: field,
        contains: _searchTerm,
      ));
    }

    (_query as dynamic)._builder.orWhere(conditions);
    return _query;
  }

  /// Search with exact match
  CollectionQuery<T> exact() {
    if (_searchFields.isEmpty || _searchTerm.isEmpty) {
      return _query;
    }

    final conditions = <FieldCondition>[];
    for (final field in _searchFields) {
      conditions.add(FieldCondition(
        field: field,
        isEqualTo: _searchTerm,
      ));
    }

    (_query as dynamic)._builder.orWhere(conditions);
    return _query;
  }

  /// Search with prefix match
  CollectionQuery<T> prefix() {
    if (_searchFields.isEmpty || _searchTerm.isEmpty) {
      return _query;
    }

    final conditions = <FieldCondition>[];
    for (final field in _searchFields) {
      conditions.add(FieldCondition(
        field: field,
        startsWith: _searchTerm,
      ));
    }

    (_query as dynamic)._builder.orWhere(conditions);
    return _query;
  }

  /// Search with regex pattern
  CollectionQuery<T> pattern(String regexPattern) {
    if (_searchFields.isEmpty) {
      return _query;
    }

    final conditions = <FieldCondition>[];
    for (final field in _searchFields) {
      conditions.add(FieldCondition(
        field: field,
        matches: regexPattern,
      ));
    }

    (_query as dynamic)._builder.orWhere(conditions);
    return _query;
  }
}

/// Extension to add text search to queries
extension TextSearchExtension<T> on CollectionQuery<T> {
  /// Create a text search query
  TextSearchQuery<T> textSearch(List<String> fields, String term) {
    return TextSearchQuery<T>(this, fields, term);
  }
}

/// Geo query builder (for location-based queries)
class GeoQuery<T> {
  final CollectionQuery<T> _query;
  final String _latField;
  final String _lngField;
  
  GeoQuery(this._query, this._latField, this._lngField);

  /// Find within bounding box
  CollectionQuery<T> withinBounds({
    required double northEast_lat,
    required double northEast_lng,
    required double southWest_lat,
    required double southWest_lng,
  }) {
    return _query
        .where(_latField, isLessThanOrEqualTo: northEast_lat)
        .where(_latField, isGreaterThanOrEqualTo: southWest_lat)
        .where(_lngField, isLessThanOrEqualTo: northEast_lng)
        .where(_lngField, isGreaterThanOrEqualTo: southWest_lng);
  }

  /// Find near a point (simplified, actual implementation would use geohashing)
  CollectionQuery<T> near({
    required double latitude,
    required double longitude,
    required double radiusInKm,
  }) {
    // Approximate degrees per km
    const double kmPerDegree = 111.0;
    final latDelta = radiusInKm / kmPerDegree;
    final lngDelta = radiusInKm / (kmPerDegree * cos(latitude * pi / 180));
    
    return withinBounds(
      northEast_lat: latitude + latDelta,
      northEast_lng: longitude + lngDelta,
      southWest_lat: latitude - latDelta,
      southWest_lng: longitude - lngDelta,
    );
  }
}

/// Extension to add geo queries
extension GeoQueryExtension<T> on CollectionQuery<T> {
  /// Create a geo query
  GeoQuery<T> geo(String latField, String lngField) {
    return GeoQuery<T>(this, latField, lngField);
  }
}

// Math functions
double cos(double radians) => (radians == 0) ? 1 : (radians == pi / 2) ? 0 : 0.5; // Simplified
const double pi = 3.14159265359;

/// Aggregation query builder
class AggregationQuery<T> {
  final AdvancedQueryBuilder<T> _builder;
  final List<Aggregation> _aggregations = [];
  
  AggregationQuery(this._builder);

  /// Count documents
  AggregationQuery<T> count([String name = 'count']) {
    _aggregations.add(CountAggregation(name));
    return this;
  }

  /// Sum a field
  AggregationQuery<T> sum(String field, [String? name]) {
    _aggregations.add(SumAggregation(name ?? 'sum_$field', field));
    return this;
  }

  /// Average a field
  AggregationQuery<T> average(String field, [String? name]) {
    _aggregations.add(AverageAggregation(name ?? 'avg_$field', field));
    return this;
  }

  /// Get minimum value
  AggregationQuery<T> min(String field, [String? name]) {
    _aggregations.add(MinAggregation(name ?? 'min_$field', field));
    return this;
  }

  /// Get maximum value
  AggregationQuery<T> max(String field, [String? name]) {
    _aggregations.add(MaxAggregation(name ?? 'max_$field', field));
    return this;
  }

  /// Execute aggregation
  Future<Map<String, dynamic>> execute() async {
    final result = await _builder.aggregate(_aggregations);
    return result.results;
  }
}

/// Extension to add aggregation to queries
extension AggregationExtension<T> on CollectionQuery<T> {
  /// Create an aggregation query
  AggregationQuery<T> aggregate() {
    return AggregationQuery<T>((_this as dynamic)._builder);
  }
  
  CollectionQuery<T> get _this => this;
}

/// Batch query operations
class BatchQuery<T> {
  final List<CollectionQuery<T>> _queries = [];
  
  /// Add a query to the batch
  BatchQuery<T> add(CollectionQuery<T> query) {
    _queries.add(query);
    return this;
  }

  /// Execute all queries
  Future<List<List<T>>> execute() async {
    return Future.wait(_queries.map((q) => q.findAll()));
  }

  /// Execute with error handling
  Future<List<QueryResult<T>>> executeWithResults() async {
    final results = <QueryResult<T>>[];
    
    for (final query in _queries) {
      try {
        final items = await query.findAll();
        results.add(QueryResult<T>.success(items));
      } catch (e) {
        results.add(QueryResult<T>.error(e));
      }
    }
    
    return results;
  }
}

/// Query result with error handling
class QueryResult<T> {
  final List<T>? items;
  final dynamic error;
  final bool success;
  
  QueryResult.success(this.items) : error = null, success = true;
  QueryResult.error(this.error) : items = null, success = false;
}

/// Extension to update the collection with query method
extension CollectionQueryExtension<T> on FreezedCollection<T> {
  /// Create a query
  CollectionQuery<T> query() {
    final builder = advancedQuery();
    return CollectionQuery<T>(builder);
  }
  
  /// Create a batch query
  BatchQuery<T> batchQuery() {
    return BatchQuery<T>();
  }
}