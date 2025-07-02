/// Base exception for Lightning DB errors
class LightningDBException implements Exception {
  final int errorCode;
  final String message;
  
  const LightningDBException(this.errorCode, this.message);
  
  @override
  String toString() => 'LightningDBException: $message (code: $errorCode)';
  
  /// Create exception from error code
  factory LightningDBException.fromCode(int code, String message) {
    switch (code) {
      case -1:
        return NullPointerException(message);
      case -2:
        return InvalidUtf8Exception(message);
      case -3:
        return IoException(message);
      case -4:
        return CorruptedDataException(message);
      case -5:
        return InvalidArgumentException(message);
      case -6:
        return OutOfMemoryException(message);
      case -7:
        return DatabaseLockedException(message);
      case -8:
        return TransactionConflictException(message);
      default:
        return LightningDBException(code, message);
    }
  }
}

/// Thrown when a null pointer is encountered
class NullPointerException extends LightningDBException {
  const NullPointerException(String message) : super(-1, message);
}

/// Thrown when invalid UTF-8 is encountered
class InvalidUtf8Exception extends LightningDBException {
  const InvalidUtf8Exception(String message) : super(-2, message);
}

/// Thrown when an I/O error occurs
class IoException extends LightningDBException {
  const IoException(String message) : super(-3, message);
}

/// Thrown when corrupted data is detected
class CorruptedDataException extends LightningDBException {
  const CorruptedDataException(String message) : super(-4, message);
}

/// Thrown when an invalid argument is provided
class InvalidArgumentException extends LightningDBException {
  const InvalidArgumentException(String message) : super(-5, message);
}

/// Thrown when out of memory
class OutOfMemoryException extends LightningDBException {
  const OutOfMemoryException(String message) : super(-6, message);
}

/// Thrown when the database is locked
class DatabaseLockedException extends LightningDBException {
  const DatabaseLockedException(String message) : super(-7, message);
}

/// Thrown when a transaction conflict occurs
class TransactionConflictException extends LightningDBException {
  const TransactionConflictException(String message) : super(-8, message);
}