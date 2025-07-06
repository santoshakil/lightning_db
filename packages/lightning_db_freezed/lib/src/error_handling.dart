import 'dart:async';
import 'package:lightning_db_dart/lightning_db_dart.dart';
import 'package:meta/meta.dart';

/// Exception specific to Freezed collections
class CollectionException extends LightningDbException {
  final String collectionName;
  final CollectionErrorType type;

  CollectionException({
    required this.collectionName,
    required this.type,
    required String message,
    int? errorCode,
    Map<String, dynamic>? metadata,
  }) : super(
          message,
          errorCode,
          'CollectionException',
          null,
          {
            'collection': collectionName,
            'type': type.name,
            ...?metadata,
          },
        );

  @override
  bool get isRetryable {
    switch (type) {
      case CollectionErrorType.concurrentModification:
      case CollectionErrorType.temporaryFailure:
        return true;
      case CollectionErrorType.serializationError:
      case CollectionErrorType.invalidQuery:
      case CollectionErrorType.duplicateId:
      case CollectionErrorType.schemaViolation:
        return false;
    }
  }
}

/// Types of collection-specific errors
enum CollectionErrorType {
  serializationError,
  invalidQuery,
  duplicateId,
  concurrentModification,
  schemaViolation,
  temporaryFailure,
}

/// Exception thrown during serialization/deserialization
class SerializationException extends CollectionException {
  final Type modelType;
  final dynamic data;
  final String? fieldName;

  SerializationException({
    required String collectionName,
    required this.modelType,
    required this.data,
    this.fieldName,
    required String message,
  }) : super(
          collectionName: collectionName,
          type: CollectionErrorType.serializationError,
          message: 'Failed to serialize $modelType${fieldName != null ? ' field "$fieldName"' : ''}: $message',
          metadata: {
            'modelType': modelType.toString(),
            if (fieldName != null) 'field': fieldName,
          },
        );
}

/// Exception thrown for invalid queries
class InvalidQueryException extends CollectionException {
  final String field;
  final String operator;
  final dynamic value;

  InvalidQueryException({
    required String collectionName,
    required this.field,
    required this.operator,
    required this.value,
    required String message,
  }) : super(
          collectionName: collectionName,
          type: CollectionErrorType.invalidQuery,
          message: 'Invalid query on field "$field" with operator "$operator": $message',
          metadata: {
            'field': field,
            'operator': operator,
            'value': value?.toString(),
          },
        );
}

/// Exception thrown when ID already exists
class DuplicateIdException extends CollectionException {
  final String id;

  DuplicateIdException({
    required String collectionName,
    required this.id,
  }) : super(
          collectionName: collectionName,
          type: CollectionErrorType.duplicateId,
          message: 'Document with ID "$id" already exists in collection "$collectionName"',
          metadata: {'id': id},
        );
}

/// Exception thrown for concurrent modifications
class ConcurrentModificationException extends CollectionException {
  final String documentId;
  final int expectedVersion;
  final int actualVersion;

  ConcurrentModificationException({
    required String collectionName,
    required this.documentId,
    required this.expectedVersion,
    required this.actualVersion,
  }) : super(
          collectionName: collectionName,
          type: CollectionErrorType.concurrentModification,
          message: 'Document "$documentId" was modified concurrently. Expected version $expectedVersion but found $actualVersion',
          metadata: {
            'documentId': documentId,
            'expectedVersion': expectedVersion,
            'actualVersion': actualVersion,
          },
        );
}

/// Error handler specific to collections
class CollectionErrorHandler {
  final String collectionName;
  final void Function(CollectionException)? onError;
  final bool Function(CollectionException)? shouldRetry;

  const CollectionErrorHandler({
    required this.collectionName,
    this.onError,
    this.shouldRetry,
  });

  /// Wrap an operation with collection-specific error handling
  Future<T> wrapOperation<T>({
    required Future<T> Function() operation,
    required String operationName,
    Map<String, dynamic>? context,
  }) async {
    try {
      return await operation();
    } on CollectionException {
      rethrow;
    } on LightningDbException catch (e) {
      // Convert database exceptions to collection exceptions
      final collectionError = CollectionException(
        collectionName: collectionName,
        type: _mapErrorType(e),
        message: '$operationName failed: ${e.message}',
        errorCode: e.errorCode,
        metadata: context,
      );
      
      onError?.call(collectionError);
      throw collectionError;
    } catch (e) {
      // Wrap unknown errors
      final collectionError = CollectionException(
        collectionName: collectionName,
        type: CollectionErrorType.temporaryFailure,
        message: '$operationName failed: $e',
        metadata: context,
      );
      
      onError?.call(collectionError);
      throw collectionError;
    }
  }

  /// Wrap serialization operations
  Future<T> wrapSerialization<T>({
    required T Function() operation,
    required Type modelType,
    String? fieldName,
    dynamic data,
  }) async {
    try {
      return operation();
    } catch (e) {
      throw SerializationException(
        collectionName: collectionName,
        modelType: modelType,
        data: data,
        fieldName: fieldName,
        message: e.toString(),
      );
    }
  }

  /// Execute with retry logic
  Future<T> withRetry<T>({
    required Future<T> Function() operation,
    required String operationName,
    int maxAttempts = 3,
    Duration delay = const Duration(milliseconds: 100),
    Map<String, dynamic>? context,
  }) async {
    return ErrorHandler.withRetry(
      operation: () => wrapOperation(
        operation: operation,
        operationName: operationName,
        context: context,
      ),
      maxAttempts: maxAttempts,
      delay: delay,
      shouldRetry: (e) {
        if (e is CollectionException) {
          return shouldRetry?.call(e) ?? e.isRetryable;
        }
        return false;
      },
      onRetry: (e, attempt) {
        if (e is CollectionException) {
          onError?.call(CollectionException(
            collectionName: collectionName,
            type: e.type,
            message: '${e.message} (retry attempt $attempt)',
            errorCode: e.errorCode,
            metadata: {
              ...?e.metadata,
              'retryAttempt': attempt,
            },
          ));
        }
      },
    );
  }

  /// Map database error types to collection error types
  CollectionErrorType _mapErrorType(LightningDbException e) {
    if (e is TransactionConflictException) {
      return CollectionErrorType.concurrentModification;
    } else if (e is IoException || e is DatabaseClosedException) {
      return CollectionErrorType.temporaryFailure;
    } else {
      return CollectionErrorType.temporaryFailure;
    }
  }
}

/// Extension for error recovery on collection operations
extension CollectionErrorRecovery<T> on Future<T> {
  /// Add collection-specific error handling
  Future<T> withCollectionErrorHandling({
    required String collectionName,
    required String operationName,
    void Function(CollectionException)? onError,
    bool Function(CollectionException)? shouldRetry,
  }) {
    final handler = CollectionErrorHandler(
      collectionName: collectionName,
      onError: onError,
      shouldRetry: shouldRetry,
    );

    return handler.wrapOperation(
      operation: () => this,
      operationName: operationName,
    );
  }

  /// Handle duplicate ID errors
  Future<T> handleDuplicateId(T Function(String existingId) handler) {
    return catchError((error) {
      if (error is DuplicateIdException) {
        return handler(error.id);
      }
      throw error;
    });
  }

  /// Handle serialization errors
  Future<T> handleSerializationError(T Function(SerializationException) handler) {
    return catchError((error) {
      if (error is SerializationException) {
        return handler(error);
      }
      throw error;
    });
  }

  /// Handle concurrent modifications
  Future<T> handleConcurrentModification({
    required Future<T> Function() retry,
    int maxRetries = 3,
  }) {
    return catchError((error) async {
      if (error is ConcurrentModificationException && maxRetries > 0) {
        // Wait a bit before retrying
        await Future.delayed(Duration(milliseconds: 100 * (4 - maxRetries)));
        return retry().handleConcurrentModification(
          retry: retry,
          maxRetries: maxRetries - 1,
        );
      }
      throw error;
    });
  }
}

/// Mixin for collections to add error handling capabilities
mixin CollectionErrorHandlingMixin {
  String get collectionName;

  /// Create an error handler for this collection
  @protected
  CollectionErrorHandler get errorHandler => CollectionErrorHandler(
    collectionName: collectionName,
  );

  /// Wrap an operation with error handling
  @protected
  Future<T> wrapOperation<T>({
    required Future<T> Function() operation,
    required String operationName,
    Map<String, dynamic>? context,
    bool retry = false,
  }) {
    if (retry) {
      return errorHandler.withRetry(
        operation: operation,
        operationName: operationName,
        context: context,
      );
    }
    return errorHandler.wrapOperation(
      operation: operation,
      operationName: operationName,
      context: context,
    );
  }

  /// Wrap a synchronous operation
  @protected
  T wrapSync<T>({
    required T Function() operation,
    required String operationName,
    Map<String, dynamic>? context,
  }) {
    try {
      return operation();
    } catch (e) {
      throw CollectionException(
        collectionName: collectionName,
        type: CollectionErrorType.temporaryFailure,
        message: '$operationName failed: $e',
        metadata: context,
      );
    }
  }
}