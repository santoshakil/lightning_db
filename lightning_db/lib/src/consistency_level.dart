/// Database consistency level
enum ConsistencyLevel {
  /// Eventual consistency - faster but may return stale data
  eventual(0),
  /// Strong consistency - slower but always returns latest data
  strong(1);

  final int value;
  const ConsistencyLevel(this.value);
}