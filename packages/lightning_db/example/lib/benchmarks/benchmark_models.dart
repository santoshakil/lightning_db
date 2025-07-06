import 'package:freezed_annotation/freezed_annotation.dart';

part 'benchmark_models.freezed.dart';
part 'benchmark_models.g.dart';

/// User model for benchmarks
@freezed
sealed class BenchmarkUser with _$BenchmarkUser {
  const factory BenchmarkUser({
    required String id,
    required String name,
    required String email,
    required int age,
    required DateTime createdAt,
    @Default({}) Map<String, dynamic> metadata,
  }) = _BenchmarkUser;
  
  factory BenchmarkUser.fromJson(Map<String, dynamic> json) =>
      _$BenchmarkUserFromJson(json);
}

/// Post model for benchmarks
@freezed
sealed class BenchmarkPost with _$BenchmarkPost {
  const factory BenchmarkPost({
    required String id,
    required String title,
    required String content,
    required String authorId,
    required DateTime createdAt,
    DateTime? updatedAt,
    @Default([]) List<String> tags,
    @Default({}) Map<String, dynamic> metadata,
  }) = _BenchmarkPost;
  
  factory BenchmarkPost.fromJson(Map<String, dynamic> json) =>
      _$BenchmarkPostFromJson(json);
}

/// Comment model for benchmarks
@freezed
sealed class BenchmarkComment with _$BenchmarkComment {
  const factory BenchmarkComment({
    required String id,
    required String postId,
    required String authorId,
    required String content,
    required DateTime createdAt,
    DateTime? editedAt,
    @Default(0) int likes,
    @Default({}) Map<String, dynamic> metadata,
  }) = _BenchmarkComment;
  
  factory BenchmarkComment.fromJson(Map<String, dynamic> json) =>
      _$BenchmarkCommentFromJson(json);
}