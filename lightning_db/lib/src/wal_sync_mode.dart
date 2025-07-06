/// Write-ahead log synchronization mode
enum WalSyncMode {
  /// Sync on every write - safest but slowest
  sync(0),
  /// Periodic sync - balanced safety and performance
  periodic(1),
  /// Async sync - fastest but least safe
  async(2);

  final int value;
  const WalSyncMode(this.value);
}