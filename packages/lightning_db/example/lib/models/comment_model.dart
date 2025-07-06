import 'package:freezed_annotation/freezed_annotation.dart';

part 'comment_model.freezed.dart';
part 'comment_model.g.dart';

@freezed
sealed class Comment with _$Comment {
  const factory Comment({
    required String id,
    required String postId,
    required String authorId,
    required String content,
    required DateTime createdAt,
    required List<Comment> replies,
    Map<String, dynamic>? metadata,
  }) = _Comment;

  factory Comment.fromJson(Map<String, dynamic> json) => _$CommentFromJson(json);
}