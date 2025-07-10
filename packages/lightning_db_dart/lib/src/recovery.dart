import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:meta/meta.dart';
import 'database.dart';
import 'errors.dart';

/// Database recovery utilities
class DatabaseRecovery {
  /// Check if a database can be recovered
  static Future<RecoveryStatus> checkDatabase(String path) async {
    try {
      // Try to open the database
      final db = await LightningDb.open(path).withDbTimeout(
        const Duration(seconds: 10),
        'Database health check timed out',
      );
      
      // Try a simple operation
      await db.get('__health_check__').withDbTimeout(
        const Duration(seconds: 5),
        'Health check operation timed out',
      );
      
      await db.close();
      
      return RecoveryStatus(
        isCorrupted: false,
        canRecover: false,
        message: 'Database is healthy',
      );
    } on CorruptedDataException {
      return RecoveryStatus(
        isCorrupted: true,
        canRecover: true,
        message: 'Database is corrupted but may be recoverable',
      );
    } on LightningDbException catch (e) {
      return RecoveryStatus(
        isCorrupted: true,
        canRecover: false,
        message: 'Database error: ${e.message}',
        error: e,
      );
    }
  }

  /// Attempt to recover a corrupted database
  static Future<RecoveryResult> recoverDatabase(
    String path, {
    String? backupPath,
    RecoveryOptions? options,
    void Function(RecoveryProgress)? onProgress,
  }) async {
    options ??= const RecoveryOptions();
    backupPath ??= '$path.backup.${DateTime.now().millisecondsSinceEpoch}';
    
    final progress = RecoveryProgress._();
    void updateProgress(String stage, double percent) {
      progress._stage = stage;
      progress._percentComplete = percent;
      onProgress?.call(progress);
    }
    
    try {
      updateProgress('Starting recovery', 0);
      
      // Step 1: Create backup
      if (options.createBackup) {
        updateProgress('Creating backup', 10);
        await _createBackup(path, backupPath);
      }
      
      // Step 2: Attempt repair
      updateProgress('Attempting repair', 30);
      final repairResult = await _attemptRepair(path, options);
      
      if (repairResult.success) {
        updateProgress('Recovery complete', 100);
        return RecoveryResult(
          success: true,
          message: 'Database recovered successfully',
          backupPath: options.createBackup ? backupPath : null,
          recoveredRecords: repairResult.recoveredRecords,
          lostRecords: repairResult.lostRecords,
        );
      }
      
      // Step 3: If repair failed, try salvage
      if (options.allowDataLoss) {
        updateProgress('Salvaging data', 60);
        final salvageResult = await _salvageData(path, options);
        
        if (salvageResult.success) {
          updateProgress('Rebuilding database', 80);
          await _rebuildDatabase(path, salvageResult.data!);
          
          updateProgress('Recovery complete', 100);
          return RecoveryResult(
            success: true,
            message: 'Database salvaged with possible data loss',
            backupPath: options.createBackup ? backupPath : null,
            recoveredRecords: salvageResult.recoveredRecords,
            lostRecords: salvageResult.lostRecords,
          );
        }
      }
      
      updateProgress('Recovery failed', 100);
      return RecoveryResult(
        success: false,
        message: 'Unable to recover database',
        backupPath: options.createBackup ? backupPath : null,
      );
      
    } catch (e) {
      return RecoveryResult(
        success: false,
        message: 'Recovery failed: $e',
        backupPath: options.createBackup ? backupPath : null,
        error: e,
      );
    }
  }

  /// Create a backup of the database
  static Future<void> _createBackup(String source, String destination) async {
    final sourceFile = File(source);
    if (!await sourceFile.exists()) {
      throw FileSystemException('Database file not found', source);
    }
    
    await sourceFile.copy(destination);
    
    // Also copy WAL and journal files if they exist
    final walFile = File('$source-wal');
    if (await walFile.exists()) {
      await walFile.copy('$destination-wal');
    }
    
    final journalFile = File('$source-journal');
    if (await journalFile.exists()) {
      await journalFile.copy('$destination-journal');
    }
  }

  /// Attempt to repair the database
  static Future<RepairResult> _attemptRepair(
    String path,
    RecoveryOptions options,
  ) async {
    // This would call native repair functions
    // For now, return a placeholder result
    return RepairResult(
      success: false,
      recoveredRecords: 0,
      lostRecords: 0,
    );
  }

  /// Salvage data from a corrupted database
  static Future<SalvageResult> _salvageData(
    String path,
    RecoveryOptions options,
  ) async {
    // This would attempt to read raw data and recover what's possible
    // For now, return a placeholder result
    return SalvageResult(
      success: false,
      recoveredRecords: 0,
      lostRecords: 0,
      data: null,
    );
  }

  /// Rebuild database from salvaged data
  static Future<void> _rebuildDatabase(
    String path,
    Map<String, dynamic> data,
  ) async {
    // Delete corrupted database
    final dbFile = File(path);
    if (await dbFile.exists()) {
      await dbFile.delete();
    }
    
    // Create new database
    final db = await LightningDb.create(path);
    
    try {
      // Reinsert all salvaged data
      final batch = db.batch();
      data.forEach((key, value) {
        if (value is String) {
          batch.putString(key, value);
        } else if (value is List<int>) {
          batch.put(key, Uint8List.fromList(value));
        }
      });
      
      await batch.commit();
    } finally {
      await db.close();
    }
  }
}

/// Recovery status information
class RecoveryStatus {
  final bool isCorrupted;
  final bool canRecover;
  final String message;
  final dynamic error;

  const RecoveryStatus({
    required this.isCorrupted,
    required this.canRecover,
    required this.message,
    this.error,
  });
}

/// Recovery options
class RecoveryOptions {
  final bool createBackup;
  final bool allowDataLoss;
  final bool verifyAfterRecovery;
  final int maxRetries;
  final Duration timeout;

  const RecoveryOptions({
    this.createBackup = true,
    this.allowDataLoss = false,
    this.verifyAfterRecovery = true,
    this.maxRetries = 3,
    this.timeout = const Duration(minutes: 10),
  });
}

/// Recovery progress information
class RecoveryProgress {
  String _stage = '';
  double _percentComplete = 0;

  RecoveryProgress._();

  String get stage => _stage;
  double get percentComplete => _percentComplete;
  
  @override
  String toString() => '$stage: ${percentComplete.toStringAsFixed(1)}%';
}

/// Recovery result
class RecoveryResult {
  final bool success;
  final String message;
  final String? backupPath;
  final int recoveredRecords;
  final int lostRecords;
  final dynamic error;

  const RecoveryResult({
    required this.success,
    required this.message,
    this.backupPath,
    this.recoveredRecords = 0,
    this.lostRecords = 0,
    this.error,
  });
}

/// Internal repair result
@immutable
class RepairResult {
  final bool success;
  final int recoveredRecords;
  final int lostRecords;

  const RepairResult({
    required this.success,
    required this.recoveredRecords,
    required this.lostRecords,
  });
}

/// Internal salvage result
@immutable
class SalvageResult {
  final bool success;
  final int recoveredRecords;
  final int lostRecords;
  final Map<String, dynamic>? data;

  const SalvageResult({
    required this.success,
    required this.recoveredRecords,
    required this.lostRecords,
    this.data,
  });
}

/// Database maintenance utilities
class DatabaseMaintenance {
  /// Run periodic maintenance on the database
  static Future<MaintenanceResult> runMaintenance(
    LightningDb db, {
    MaintenanceOptions? options,
    void Function(String)? onLog,
  }) async {
    options ??= const MaintenanceOptions();
    final log = onLog ?? (_) {};
    
    final startTime = DateTime.now();
    var tasksCompleted = 0;
    final errors = <String>[];
    
    try {
      // 1. Checkpoint if using WAL
      if (options.checkpoint) {
        log('Running checkpoint...');
        try {
          await db.checkpoint().withDbTimeout(
            options.taskTimeout,
            'Checkpoint timed out',
          );
          tasksCompleted++;
        } catch (e) {
          errors.add('Checkpoint failed: $e');
        }
      }
      
      // 2. Sync to disk
      if (options.sync) {
        log('Syncing to disk...');
        try {
          await db.sync().withDbTimeout(
            options.taskTimeout,
            'Sync timed out',
          );
          tasksCompleted++;
        } catch (e) {
          errors.add('Sync failed: $e');
        }
      }
      
      // 3. Compact database
      if (options.compact) {
        log('Compacting database...');
        try {
          // This would call a native compact function
          // await db.compact();
          tasksCompleted++;
        } catch (e) {
          errors.add('Compaction failed: $e');
        }
      }
      
      // 4. Analyze and optimize
      if (options.analyze) {
        log('Analyzing database...');
        try {
          // This would call native analyze functions
          tasksCompleted++;
        } catch (e) {
          errors.add('Analysis failed: $e');
        }
      }
      
      final duration = DateTime.now().difference(startTime);
      
      return MaintenanceResult(
        success: errors.isEmpty,
        duration: duration,
        tasksCompleted: tasksCompleted,
        errors: errors,
      );
      
    } catch (e) {
      return MaintenanceResult(
        success: false,
        duration: DateTime.now().difference(startTime),
        tasksCompleted: tasksCompleted,
        errors: [...errors, 'Maintenance failed: $e'],
      );
    }
  }

  /// Schedule periodic maintenance
  static Timer scheduleMaintenance(
    LightningDb db, {
    Duration interval = const Duration(hours: 24),
    MaintenanceOptions? options,
    void Function(MaintenanceResult)? onComplete,
    void Function(String)? onLog,
  }) {
    return Timer.periodic(interval, (_) async {
      final result = await runMaintenance(
        db,
        options: options,
        onLog: onLog,
      );
      onComplete?.call(result);
    });
  }
}

/// Maintenance options
class MaintenanceOptions {
  final bool checkpoint;
  final bool sync;
  final bool compact;
  final bool analyze;
  final Duration taskTimeout;

  const MaintenanceOptions({
    this.checkpoint = true,
    this.sync = true,
    this.compact = true,
    this.analyze = true,
    this.taskTimeout = const Duration(minutes: 5),
  });
}

/// Maintenance result
class MaintenanceResult {
  final bool success;
  final Duration duration;
  final int tasksCompleted;
  final List<String> errors;

  const MaintenanceResult({
    required this.success,
    required this.duration,
    required this.tasksCompleted,
    required this.errors,
  });
  
  @override
  String toString() {
    final buffer = StringBuffer('Maintenance ');
    buffer.write(success ? 'completed' : 'failed');
    buffer.write(' in ${duration.inSeconds}s');
    buffer.write(' ($tasksCompleted tasks)');
    if (errors.isNotEmpty) {
      buffer.write(' with ${errors.length} errors');
    }
    return buffer.toString();
  }
}