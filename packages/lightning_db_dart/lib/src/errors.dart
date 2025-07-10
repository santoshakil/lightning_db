import 'dart:ffi';
import 'dart:async';
import 'package:ffi/ffi.dart';
import 'init.dart';
import 'native/lightning_db_bindings.dart';

/// Base exception for all Lightning DB errors
class LightningDbException implements Exception {
  final String message;
  final int? errorCode;
  final String? errorType;
  final StackTrace? stackTrace;
  final Map<String, dynamic>? metadata;

  const LightningDbException(
    this.message, [
    this.errorCode,
    this.errorType,
    this.stackTrace,
    this.metadata,
  ]);

  factory LightningDbException.fromNative(int errorCode, [String? details]) {
    final message = details ?? ErrorHandler.getLastError();
    
    switch (errorCode) {
      case ErrorCode.ErrorCodeKeyNotFound:
        return KeyNotFoundException(message);
      case ErrorCode.ErrorCodeTransactionConflict:
        return TransactionConflictException(message);
      case ErrorCode.ErrorCodeIoError:
        return IoException(message);
      case ErrorCode.ErrorCodeCorruptedData:
        return CorruptedDataException(message);
      case ErrorCode.ErrorCodeDatabaseNotFound:
        return DatabaseNotFoundException(message);
      case ErrorCode.ErrorCodeDatabaseExists:
        return DatabaseExistsException(message);
      case ErrorCode.ErrorCodeTransactionNotFound:
        return TransactionNotFoundException(message);
      case ErrorCode.ErrorCodeOutOfMemory:
        return OutOfMemoryException(message);
      case ErrorCode.ErrorCodeInvalidArgument:
        return InvalidArgumentException(message);
      case ErrorCode.ErrorCodeUnknown:
        return DiskFullException(message);
      default:
        return LightningDbException(message, errorCode);
    }
  }

  /// Whether this error is retryable
  bool get isRetryable {
    if (errorCode == null) return false;
    switch (errorCode!) {
      case ErrorCode.ErrorCodeTransactionConflict:
      case ErrorCode.ErrorCodeIoError:
      case ErrorCode.ErrorCodeTransactionNotFound:
        return true;
      default:
        return false;
    }
  }

  /// Convert to a user-friendly message
  String get userMessage {
    switch (errorType) {
      case 'KeyNotFoundException':
        return 'The requested data was not found';
      case 'TransactionConflictException':
        return 'Operation failed due to concurrent access. Please try again.';
      case 'IoException':
        return 'Storage operation failed. Please check disk space and permissions.';
      case 'CorruptedDataException':
        return 'Data integrity error detected. Database may need recovery.';
      case 'DatabaseClosedException':
        return 'Database connection is closed';
      case 'DiskFullException':
        return 'Insufficient disk space';
      case 'OutOfMemoryException':
        return 'Insufficient memory available';
      default:
        return 'An error occurred: $message';
    }
  }

  @override
  String toString() {
    final buffer = StringBuffer();
    buffer.write('LightningDbException');
    if (errorType != null) {
      buffer.write(' ($errorType)');
    }
    buffer.write(': $message');
    if (errorCode != null) {
      buffer.write(' [code: $errorCode]');
    }
    if (metadata != null && metadata!.isNotEmpty) {
      buffer.write(' {');
      metadata!.forEach((key, value) {
        buffer.write('$key: $value, ');
      });
      buffer.write('}');
    }
    return buffer.toString();
  }
}

/// Exception thrown when a key is not found
class KeyNotFoundException extends LightningDbException {
  KeyNotFoundException(String key) 
    : super(
        'Key not found: $key', 
        ErrorCode.ErrorCodeKeyNotFound,
        'KeyNotFoundException',
      );
}

/// Exception thrown for transaction conflicts
class TransactionConflictException extends LightningDbException {
  final int? attemptNumber;
  
  TransactionConflictException([String? details, this.attemptNumber]) 
    : super(
        'Transaction conflict detected${details != null ? ': $details' : ''}', 
        ErrorCode.ErrorCodeTransactionConflict,
        'TransactionConflictException',
        null,
        attemptNumber != null ? {'attempt': attemptNumber} : null,
      );
}

/// Exception thrown for I/O errors
class IoException extends LightningDbException {
  IoException(String details) 
    : super(
        'I/O error: $details', 
        ErrorCode.ErrorCodeIoError,
        'IoException',
      );
}

/// Exception thrown for corrupted data
class CorruptedDataException extends LightningDbException {
  CorruptedDataException([String? details]) 
    : super(
        'Data corruption detected${details != null ? ': $details' : ''}', 
        ErrorCode.ErrorCodeCorruptedData,
        'CorruptedDataException',
      );
}

/// Exception thrown when database is not found
class DatabaseNotFoundException extends LightningDbException {
  DatabaseNotFoundException(String path)
    : super(
        'Database not found: $path',
        ErrorCode.ErrorCodeDatabaseNotFound,
        'DatabaseNotFoundException',
      );
}

/// Exception thrown when database already exists
class DatabaseExistsException extends LightningDbException {
  DatabaseExistsException(String path)
    : super(
        'Database already exists: $path',
        ErrorCode.ErrorCodeDatabaseExists,
        'DatabaseExistsException',
      );
}

/// Exception thrown when transaction is not found
class TransactionNotFoundException extends LightningDbException {
  TransactionNotFoundException(String id)
    : super(
        'Transaction not found: $id',
        ErrorCode.ErrorCodeTransactionNotFound,
        'TransactionNotFoundException',
      );
}

/// Exception thrown when database is closed
class DatabaseClosedException extends LightningDbException {
  DatabaseClosedException()
    : super(
        'Database is closed',
        ErrorCode.ErrorCodeCorruptedData,
        'DatabaseClosedException',
      );
}

/// Exception thrown when disk is full
class DiskFullException extends LightningDbException {
  DiskFullException([String? details])
    : super(
        'Disk full${details != null ? ': $details' : ''}',
        ErrorCode.ErrorCodeOutOfMemory,
        'DiskFullException',
      );
}

/// Exception thrown for out of memory errors
class OutOfMemoryException extends LightningDbException {
  OutOfMemoryException([String? details])
    : super(
        'Out of memory${details != null ? ': $details' : ''}',
        ErrorCode.ErrorCodeOutOfMemory,
        'OutOfMemoryException',
      );
}

/// Exception thrown for invalid arguments
class InvalidArgumentException extends LightningDbException {
  InvalidArgumentException(String details)
    : super(
        'Invalid argument: $details',
        ErrorCode.ErrorCodeInvalidArgument,
        'InvalidArgumentException',
      );
}

/// Additional error codes not in native bindings  
class ErrorCodeExtensions {
  static const int ErrorCodeDatabaseClosed = -100;
  static const int ErrorCodeDiskFull = -101;
  static const int ErrorCodeDatabaseLocked = -102;
}

/// Utility class for error handling
class ErrorHandler {
  static Pointer<Pointer<Utf8>>? _errorBuffer;
  
  /// Initialize error handler
  static void init() {
    _errorBuffer ??= calloc<Pointer<Utf8>>();
  }
  
  /// Check the result code and throw appropriate exception if needed
  static void checkResult(int result, [String? operation]) {
    if (result == ErrorCode.ErrorCodeSuccess) {
      return;
    }
    
    final exception = LightningDbException.fromNative(result, operation);
    
    // Clear the error after reading it
    clearError();
    
    throw exception;
  }
  
  /// Get the last error message from native library
  static String getLastError() {
    init();
    
    final errorPtr = LightningDbInit.bindings.lightning_db_get_last_error();
    if (errorPtr == nullptr) {
      return 'Unknown error';
    }
    
    // Convert C string to Dart string
    final message = errorPtr.cast<Utf8>().toDartString();
    
    // Don't free the error pointer as it's managed by the native library
    
    return message;
  }
  
  /// Clear the last error in native library
  static void clearError() {
    LightningDbInit.bindings.lightning_db_clear_error();
  }
  
  /// Execute operation with retry logic
  static Future<T> withRetry<T>({
    required Future<T> Function() operation,
    int maxAttempts = 3,
    Duration delay = const Duration(milliseconds: 100),
    Duration maxDelay = const Duration(seconds: 5),
    double backoffFactor = 2.0,
    bool Function(LightningDbException)? shouldRetry,
    void Function(LightningDbException, int)? onRetry,
  }) async {
    assert(maxAttempts > 0);
    assert(backoffFactor >= 1.0);
    
    LightningDbException? lastException;
    
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await operation();
      } on LightningDbException catch (e) {
        lastException = e;
        
        // Check if we should retry
        final canRetry = shouldRetry?.call(e) ?? e.isRetryable;
        
        if (!canRetry || attempt == maxAttempts) {
          throw e;
        }
        
        // Notify retry listener
        onRetry?.call(e, attempt);
        
        // Calculate delay with exponential backoff
        final attemptDelay = Duration(
          milliseconds: (delay.inMilliseconds * pow(backoffFactor, attempt - 1)).round(),
        );
        final waitTime = attemptDelay > maxDelay ? maxDelay : attemptDelay;
        
        await Future.delayed(waitTime);
      }
    }
    
    throw lastException!;
  }
  
  /// Execute operation with timeout
  static Future<T> withTimeout<T>({
    required Future<T> Function() operation,
    required Duration timeout,
    String? timeoutMessage,
  }) async {
    try {
      return await operation().timeout(
        timeout,
        onTimeout: () => throw LightningDbException(
          timeoutMessage ?? 'Operation timed out after ${timeout.inSeconds}s',
          ErrorCode.ErrorCodeIoError,
          'TimeoutException',
        ),
      );
    } on TimeoutException {
      throw LightningDbException(
        timeoutMessage ?? 'Operation timed out after ${timeout.inSeconds}s',
        ErrorCode.ErrorCodeIoError,
        'TimeoutException',
      );
    }
  }
  
  /// Cleanup error handler resources
  static void cleanup() {
    if (_errorBuffer != null) {
      if (_errorBuffer!.value != nullptr) {
        calloc.free(_errorBuffer!.value);
      }
      calloc.free(_errorBuffer!);
      _errorBuffer = null;
    }
  }
}

/// Extension for error recovery on Futures
extension ErrorRecovery<T> on Future<T> {
  /// Add retry logic to any Future
  Future<T> withRetry({
    int maxAttempts = 3,
    Duration delay = const Duration(milliseconds: 100),
    Duration maxDelay = const Duration(seconds: 5),
    double backoffFactor = 2.0,
  }) {
    return ErrorHandler.withRetry(
      operation: () => this,
      maxAttempts: maxAttempts,
      delay: delay,
      maxDelay: maxDelay,
      backoffFactor: backoffFactor,
    );
  }
  
  /// Add timeout to any Future
  Future<T> withDbTimeout(Duration timeout, [String? message]) {
    return ErrorHandler.withTimeout(
      operation: () => this,
      timeout: timeout,
      timeoutMessage: message,
    );
  }
  
  /// Map database errors to custom exceptions
  Future<T> mapDbError(LightningDbException Function(dynamic error) mapper) {
    return catchError((error) {
      if (error is LightningDbException) {
        throw mapper(error);
      }
      throw error;
    });
  }
  
  /// Recover from specific error types
  Future<T> recoverFrom<E extends LightningDbException>(
    FutureOr<T> Function(E error) recovery,
  ) {
    return catchError((error) async {
      if (error is E) {
        return await recovery(error);
      }
      throw error;
    });
  }
}

// Helper function for pow
num pow(num x, num exponent) {
  if (exponent == 0) return 1;
  num result = x;
  for (int i = 1; i < exponent; i++) {
    result *= x;
  }
  return result;
}