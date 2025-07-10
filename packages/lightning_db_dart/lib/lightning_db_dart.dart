/// Lightning DB - High-performance embedded database for Dart
/// 
/// This library provides Dart bindings for Lightning DB, offering:
/// - High-performance key-value storage
/// - ACID transactions
/// - Cross-platform support
/// - Zero-copy operations where possible
library lightning_db_dart;

export 'src/database.dart';
export 'src/errors.dart';
export 'src/types.dart';
export 'src/batch.dart';
export 'src/encryption.dart';
// Temporarily disabled due to API mismatches - need to be fixed:
// export 'src/transaction.dart';
// export 'src/iterator.dart'; 
// export 'src/recovery.dart';
// export 'src/migration.dart';
// export 'src/monitoring.dart';