// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'benchmark_models.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

_$BenchmarkUserImpl _$$BenchmarkUserImplFromJson(Map<String, dynamic> json) =>
    _$BenchmarkUserImpl(
      id: json['id'] as String,
      name: json['name'] as String,
      email: json['email'] as String,
      age: (json['age'] as num).toInt(),
      createdAt: DateTime.parse(json['createdAt'] as String),
      metadata: json['metadata'] as Map<String, dynamic>? ?? const {},
    );

Map<String, dynamic> _$$BenchmarkUserImplToJson(_$BenchmarkUserImpl instance) =>
    <String, dynamic>{
      'id': instance.id,
      'name': instance.name,
      'email': instance.email,
      'age': instance.age,
      'createdAt': instance.createdAt.toIso8601String(),
      'metadata': instance.metadata,
    };

_$BenchmarkPostImpl _$$BenchmarkPostImplFromJson(Map<String, dynamic> json) =>
    _$BenchmarkPostImpl(
      id: json['id'] as String,
      title: json['title'] as String,
      content: json['content'] as String,
      authorId: json['authorId'] as String,
      createdAt: DateTime.parse(json['createdAt'] as String),
      updatedAt: json['updatedAt'] == null
          ? null
          : DateTime.parse(json['updatedAt'] as String),
      tags:
          (json['tags'] as List<dynamic>?)?.map((e) => e as String).toList() ??
              const [],
      metadata: json['metadata'] as Map<String, dynamic>? ?? const {},
    );

Map<String, dynamic> _$$BenchmarkPostImplToJson(_$BenchmarkPostImpl instance) =>
    <String, dynamic>{
      'id': instance.id,
      'title': instance.title,
      'content': instance.content,
      'authorId': instance.authorId,
      'createdAt': instance.createdAt.toIso8601String(),
      'updatedAt': instance.updatedAt?.toIso8601String(),
      'tags': instance.tags,
      'metadata': instance.metadata,
    };

_$BenchmarkCommentImpl _$$BenchmarkCommentImplFromJson(
        Map<String, dynamic> json) =>
    _$BenchmarkCommentImpl(
      id: json['id'] as String,
      postId: json['postId'] as String,
      authorId: json['authorId'] as String,
      content: json['content'] as String,
      createdAt: DateTime.parse(json['createdAt'] as String),
      editedAt: json['editedAt'] == null
          ? null
          : DateTime.parse(json['editedAt'] as String),
      likes: (json['likes'] as num?)?.toInt() ?? 0,
      metadata: json['metadata'] as Map<String, dynamic>? ?? const {},
    );

Map<String, dynamic> _$$BenchmarkCommentImplToJson(
        _$BenchmarkCommentImpl instance) =>
    <String, dynamic>{
      'id': instance.id,
      'postId': instance.postId,
      'authorId': instance.authorId,
      'content': instance.content,
      'createdAt': instance.createdAt.toIso8601String(),
      'editedAt': instance.editedAt?.toIso8601String(),
      'likes': instance.likes,
      'metadata': instance.metadata,
    };
