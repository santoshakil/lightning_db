import 'dart:async';
import 'package:rxdart/rxdart.dart';
import 'collection.dart';
import 'query.dart';

/// Reactive wrapper for collections
class ReactiveCollection<T> {
  final FreezedCollection<T> _collection;
  final BehaviorSubject<List<T>> _itemsSubject;
  final PublishSubject<CollectionEvent<T>> _eventSubject;
  StreamSubscription<CollectionChange<T>>? _changeSubscription;
  bool _initialized = false;
  
  ReactiveCollection(this._collection) 
    : _itemsSubject = BehaviorSubject<List<T>>(),
      _eventSubject = PublishSubject<CollectionEvent<T>>() {
    _initialize();
  }
  
  /// Stream of all items in the collection
  ValueStream<List<T>> get items => _itemsSubject.stream;
  
  /// Stream of collection events
  Stream<CollectionEvent<T>> get events => _eventSubject.stream;
  
  /// Current items (synchronous access)
  List<T> get currentItems => _itemsSubject.value ?? [];
  
  /// Stream of item count
  Stream<int> get count => items.map((list) => list.length);
  
  /// Stream indicating if collection is empty
  Stream<bool> get isEmpty => count.map((c) => c == 0);
  
  /// Stream indicating if collection is not empty
  Stream<bool> get isNotEmpty => count.map((c) => c > 0);
  
  void _initialize() async {
    if (_initialized) return;
    _initialized = true;
    
    // Load initial data
    final initialItems = await _collection.getAll();
    _itemsSubject.add(initialItems);
    
    // Subscribe to changes
    _changeSubscription = _collection.changes.listen(_handleChange);
  }
  
  void _handleChange(CollectionChange<T> change) async {
    final currentList = List<T>.from(currentItems);
    
    switch (change.type) {
      case ChangeType.insert:
        if (change.newValue != null) {
          currentList.add(change.newValue!);
          _eventSubject.add(CollectionEvent.inserted(change.newValue!));
        }
        break;
        
      case ChangeType.update:
        if (change.newValue != null) {
          final index = currentList.indexWhere(
            (item) => _collection.adapter.getKey(item) == change.key
          );
          if (index != -1) {
            currentList[index] = change.newValue!;
            _eventSubject.add(CollectionEvent.updated(
              change.oldValue!,
              change.newValue!,
            ));
          }
        }
        break;
        
      case ChangeType.delete:
        if (change.oldValue != null) {
          currentList.removeWhere(
            (item) => _collection.adapter.getKey(item) == change.key
          );
          _eventSubject.add(CollectionEvent.deleted(change.oldValue!));
        }
        break;
        
      case ChangeType.clear:
        currentList.clear();
        _eventSubject.add(CollectionEvent.cleared());
        break;
    }
    
    _itemsSubject.add(currentList);
  }
  
  /// Find an item reactively
  ValueStream<T?> findByKey(String key) {
    return items.map((list) {
      try {
        return list.firstWhere(
          (item) => _collection.adapter.getKey(item) == key,
        );
      } catch (_) {
        return null;
      }
    }).shareValueSeeded(null);
  }
  
  /// Filter items reactively
  ValueStream<List<T>> where(bool Function(T) test) {
    return items.map((list) => list.where(test).toList()).shareValue();
  }
  
  /// Map items reactively
  ValueStream<List<R>> map<R>(R Function(T) mapper) {
    return items.map((list) => list.map(mapper).toList()).shareValue();
  }
  
  /// Get first item reactively
  ValueStream<T?> get first {
    return items.map((list) => list.isEmpty ? null : list.first).shareValue();
  }
  
  /// Get last item reactively
  ValueStream<T?> get last {
    return items.map((list) => list.isEmpty ? null : list.last).shareValue();
  }
  
  /// Combine with another reactive collection
  ValueStream<R> combineWith<U, R>(
    ReactiveCollection<U> other,
    R Function(List<T>, List<U>) combiner,
  ) {
    return Rx.combineLatest2(
      items,
      other.items,
      combiner,
    ).shareValue();
  }
  
  /// Create a computed property
  ValueStream<R> computed<R>(R Function(List<T>) compute) {
    return items.map(compute).shareValue();
  }
  
  /// Watch for specific events
  Stream<T> get insertions => events
      .where((e) => e.type == EventType.inserted)
      .map((e) => e.item!);
  
  Stream<T> get updates => events
      .where((e) => e.type == EventType.updated)
      .map((e) => e.newItem!);
  
  Stream<T> get deletions => events
      .where((e) => e.type == EventType.deleted)
      .map((e) => e.item!);
  
  /// Dispose of resources
  void dispose() {
    _changeSubscription?.cancel();
    _itemsSubject.close();
    _eventSubject.close();
  }
}

/// Collection event types
enum EventType {
  inserted,
  updated,
  deleted,
  cleared,
}

/// Event emitted by reactive collections
class CollectionEvent<T> {
  final EventType type;
  final T? item;
  final T? oldItem;
  final T? newItem;
  
  CollectionEvent._({
    required this.type,
    this.item,
    this.oldItem,
    this.newItem,
  });
  
  factory CollectionEvent.inserted(T item) {
    return CollectionEvent._(
      type: EventType.inserted,
      item: item,
    );
  }
  
  factory CollectionEvent.updated(T oldItem, T newItem) {
    return CollectionEvent._(
      type: EventType.updated,
      oldItem: oldItem,
      newItem: newItem,
    );
  }
  
  factory CollectionEvent.deleted(T item) {
    return CollectionEvent._(
      type: EventType.deleted,
      item: item,
    );
  }
  
  factory CollectionEvent.cleared() {
    return CollectionEvent._(type: EventType.cleared);
  }
}

/// Reactive query builder
class ReactiveQuery<T> {
  final ReactiveCollection<T> _collection;
  final List<bool Function(T)> _predicates = [];
  final List<QuerySort<T>> _sorts = [];
  
  ReactiveQuery(this._collection);
  
  /// Add a filter
  ReactiveQuery<T> where(bool Function(T) predicate) {
    _predicates.add(predicate);
    return this;
  }
  
  /// Sort by a field
  ReactiveQuery<T> orderBy<R extends Comparable>(R Function(T) selector, {bool ascending = true}) {
    _sorts.add(QuerySort(selector: selector, ascending: ascending));
    return this;
  }
  
  /// Execute and get reactive results
  ValueStream<List<T>> execute() {
    return _collection.items.map((items) {
      var filtered = items.where((item) {
        return _predicates.every((predicate) => predicate(item));
      }).toList();
      
      if (_sorts.isNotEmpty) {
        filtered.sort((a, b) {
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
      }
      
      return filtered;
    }).shareValue();
  }
}

/// Extension methods for reactive patterns
extension ReactivePatterns<T> on ReactiveCollection<T> {
  /// Create a reactive query
  ReactiveQuery<T> query() => ReactiveQuery<T>(this);
  
  /// Group items by a key
  ValueStream<Map<K, List<T>>> groupBy<K>(K Function(T) keySelector) {
    return items.map((list) {
      final Map<K, List<T>> groups = {};
      for (final item in list) {
        final key = keySelector(item);
        groups.putIfAbsent(key, () => []).add(item);
      }
      return groups;
    }).shareValue();
  }
  
  /// Get distinct values for a field
  ValueStream<Set<R>> distinct<R>(R Function(T) selector) {
    return items.map((list) {
      return list.map(selector).toSet();
    }).shareValue();
  }
  
  /// Calculate sum for a numeric field
  ValueStream<num> sum(num Function(T) selector) {
    return items.map((list) {
      return list.fold<num>(0, (sum, item) => sum + selector(item));
    }).shareValue();
  }
  
  /// Calculate average for a numeric field
  ValueStream<double> average(num Function(T) selector) {
    return items.map((list) {
      if (list.isEmpty) return 0.0;
      final sum = list.fold<num>(0, (sum, item) => sum + selector(item));
      return sum / list.length;
    }).shareValue();
  }
  
  /// Get min value for a comparable field
  ValueStream<R?> min<R extends Comparable>(R Function(T) selector) {
    return items.map((list) {
      if (list.isEmpty) return null;
      return list.map(selector).reduce((a, b) => a.compareTo(b) < 0 ? a : b);
    }).shareValue();
  }
  
  /// Get max value for a comparable field
  ValueStream<R?> max<R extends Comparable>(R Function(T) selector) {
    return items.map((list) {
      if (list.isEmpty) return null;
      return list.map(selector).reduce((a, b) => a.compareTo(b) > 0 ? a : b);
    }).shareValue();
  }
}