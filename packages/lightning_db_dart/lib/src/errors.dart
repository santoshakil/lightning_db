import 'dart:ffi';
import 'package:ffi/ffi.dart';
import 'init.dart';
import 'native/lightning_db_bindings.dart';

/// Base exception for Lightning DB errors
class LightningDbException implements Exception {
  final String message;
  final int? errorCode;
  
  const LightningDbException(this.message, [this.errorCode]);
  
  @override
  String toString() {
    if (errorCode != null) {
      return 'LightningDbException: $message (error code: $errorCode)';
    }
    return 'LightningDbException: $message';
  }
}

/// Exception thrown when a key is not found
class KeyNotFoundException extends LightningDbException {
  const KeyNotFoundException(String key) 
    : super('Key not found: $key', ErrorCode.ErrorCodeKeyNotFound);
}

/// Exception thrown for transaction conflicts
class TransactionConflictException extends LightningDbException {
  const TransactionConflictException() 
    : super('Transaction conflict detected', ErrorCode.ErrorCodeTransactionConflict);
}

/// Exception thrown for I/O errors
class IoException extends LightningDbException {
  const IoException(String details) 
    : super('I/O error: $details', ErrorCode.ErrorCodeIoError);
}

/// Exception thrown for corrupted data
class CorruptedDataException extends LightningDbException {
  const CorruptedDataException() 
    : super('Data corruption detected', ErrorCode.ErrorCodeCorruptedData);
}

/// Utility class for error handling
class ErrorHandler {
  /// Check the result code and throw appropriate exception if needed
  static void checkResult(int result, [String? operation]) {
    if (result == ErrorCode.ErrorCodeSuccess) {
      return;
    }
    
    // Get detailed error message from native library
    final errorMessage = getLastError();
    final fullMessage = operation != null 
      ? '$operation failed: $errorMessage' 
      : errorMessage;
    
    switch (result) {
      case ErrorCode.ErrorCodeInvalidArgument:
        throw ArgumentError(fullMessage);
      
      case ErrorCode.ErrorCodeDatabaseNotFound:
        throw LightningDbException('Database not found: $fullMessage', result);
      
      case ErrorCode.ErrorCodeDatabaseExists:
        throw LightningDbException('Database already exists: $fullMessage', result);
      
      case ErrorCode.ErrorCodeTransactionNotFound:
        throw LightningDbException('Transaction not found: $fullMessage', result);
      
      case ErrorCode.ErrorCodeTransactionConflict:
        throw TransactionConflictException();
      
      case ErrorCode.ErrorCodeKeyNotFound:
        throw KeyNotFoundException(errorMessage);
      
      case ErrorCode.ErrorCodeIoError:
        throw IoException(errorMessage);
      
      case ErrorCode.ErrorCodeCorruptedData:
        throw CorruptedDataException();
      
      case ErrorCode.ErrorCodeOutOfMemory:
        throw OutOfMemoryError();
      
      default:
        throw LightningDbException(fullMessage, result);
    }
  }
  
  /// Get the last error message from native library
  static String getLastError() {
    final errorPtr = LightningDbInit.bindings.lightning_db_get_last_error();
    if (errorPtr == nullptr) {
      return 'Unknown error';
    }
    
    // Convert C string to Dart string
    return errorPtr.cast<Utf8>().toDartString();
  }
  
  /// Clear the last error in native library
  static void clearError() {
    LightningDbInit.bindings.lightning_db_clear_error();
  }
}