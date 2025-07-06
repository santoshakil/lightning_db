import 'package:freezed_annotation/freezed_annotation.dart';

part 'post_model.freezed.dart';
part 'post_model.g.dart';

@freezed
sealed class Post with _$Post {
  const factory Post({
    required String id,
    required String title,
    required String content,
    required String authorId,
    required DateTime createdAt,
    required List<String> tags,
    Map<String, dynamic>? metadata,
  }) = _Post;

  factory Post.fromJson(Map<String, dynamic> json) => _$PostFromJson(json);
}