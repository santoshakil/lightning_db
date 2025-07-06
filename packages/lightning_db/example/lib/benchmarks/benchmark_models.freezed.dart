// coverage:ignore-file
// GENERATED CODE - DO NOT MODIFY BY HAND
// ignore_for_file: type=lint
// ignore_for_file: unused_element, deprecated_member_use, deprecated_member_use_from_same_package, use_function_type_syntax_for_parameters, unnecessary_const, avoid_init_to_null, invalid_override_different_default_values_named, prefer_expression_function_bodies, annotate_overrides, invalid_annotation_target, unnecessary_question_mark

part of 'benchmark_models.dart';

// **************************************************************************
// FreezedGenerator
// **************************************************************************

T _$identity<T>(T value) => value;

final _privateConstructorUsedError = UnsupportedError(
    'It seems like you constructed your class using `MyClass._()`. This constructor is only meant to be used by freezed and you are not supposed to need it nor use it.\nPlease check the documentation here for more information: https://github.com/rrousselGit/freezed#adding-getters-and-methods-to-our-models');

BenchmarkUser _$BenchmarkUserFromJson(Map<String, dynamic> json) {
  return _BenchmarkUser.fromJson(json);
}

/// @nodoc
mixin _$BenchmarkUser {
  String get id => throw _privateConstructorUsedError;
  String get name => throw _privateConstructorUsedError;
  String get email => throw _privateConstructorUsedError;
  int get age => throw _privateConstructorUsedError;
  DateTime get createdAt => throw _privateConstructorUsedError;
  Map<String, dynamic> get metadata => throw _privateConstructorUsedError;

  /// Serializes this BenchmarkUser to a JSON map.
  Map<String, dynamic> toJson() => throw _privateConstructorUsedError;

  /// Create a copy of BenchmarkUser
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  $BenchmarkUserCopyWith<BenchmarkUser> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $BenchmarkUserCopyWith<$Res> {
  factory $BenchmarkUserCopyWith(
          BenchmarkUser value, $Res Function(BenchmarkUser) then) =
      _$BenchmarkUserCopyWithImpl<$Res, BenchmarkUser>;
  @useResult
  $Res call(
      {String id,
      String name,
      String email,
      int age,
      DateTime createdAt,
      Map<String, dynamic> metadata});
}

/// @nodoc
class _$BenchmarkUserCopyWithImpl<$Res, $Val extends BenchmarkUser>
    implements $BenchmarkUserCopyWith<$Res> {
  _$BenchmarkUserCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of BenchmarkUser
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? id = null,
    Object? name = null,
    Object? email = null,
    Object? age = null,
    Object? createdAt = null,
    Object? metadata = null,
  }) {
    return _then(_value.copyWith(
      id: null == id
          ? _value.id
          : id // ignore: cast_nullable_to_non_nullable
              as String,
      name: null == name
          ? _value.name
          : name // ignore: cast_nullable_to_non_nullable
              as String,
      email: null == email
          ? _value.email
          : email // ignore: cast_nullable_to_non_nullable
              as String,
      age: null == age
          ? _value.age
          : age // ignore: cast_nullable_to_non_nullable
              as int,
      createdAt: null == createdAt
          ? _value.createdAt
          : createdAt // ignore: cast_nullable_to_non_nullable
              as DateTime,
      metadata: null == metadata
          ? _value.metadata
          : metadata // ignore: cast_nullable_to_non_nullable
              as Map<String, dynamic>,
    ) as $Val);
  }
}

/// @nodoc
abstract class _$$BenchmarkUserImplCopyWith<$Res>
    implements $BenchmarkUserCopyWith<$Res> {
  factory _$$BenchmarkUserImplCopyWith(
          _$BenchmarkUserImpl value, $Res Function(_$BenchmarkUserImpl) then) =
      __$$BenchmarkUserImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call(
      {String id,
      String name,
      String email,
      int age,
      DateTime createdAt,
      Map<String, dynamic> metadata});
}

/// @nodoc
class __$$BenchmarkUserImplCopyWithImpl<$Res>
    extends _$BenchmarkUserCopyWithImpl<$Res, _$BenchmarkUserImpl>
    implements _$$BenchmarkUserImplCopyWith<$Res> {
  __$$BenchmarkUserImplCopyWithImpl(
      _$BenchmarkUserImpl _value, $Res Function(_$BenchmarkUserImpl) _then)
      : super(_value, _then);

  /// Create a copy of BenchmarkUser
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? id = null,
    Object? name = null,
    Object? email = null,
    Object? age = null,
    Object? createdAt = null,
    Object? metadata = null,
  }) {
    return _then(_$BenchmarkUserImpl(
      id: null == id
          ? _value.id
          : id // ignore: cast_nullable_to_non_nullable
              as String,
      name: null == name
          ? _value.name
          : name // ignore: cast_nullable_to_non_nullable
              as String,
      email: null == email
          ? _value.email
          : email // ignore: cast_nullable_to_non_nullable
              as String,
      age: null == age
          ? _value.age
          : age // ignore: cast_nullable_to_non_nullable
              as int,
      createdAt: null == createdAt
          ? _value.createdAt
          : createdAt // ignore: cast_nullable_to_non_nullable
              as DateTime,
      metadata: null == metadata
          ? _value._metadata
          : metadata // ignore: cast_nullable_to_non_nullable
              as Map<String, dynamic>,
    ));
  }
}

/// @nodoc
@JsonSerializable()
class _$BenchmarkUserImpl implements _BenchmarkUser {
  const _$BenchmarkUserImpl(
      {required this.id,
      required this.name,
      required this.email,
      required this.age,
      required this.createdAt,
      final Map<String, dynamic> metadata = const {}})
      : _metadata = metadata;

  factory _$BenchmarkUserImpl.fromJson(Map<String, dynamic> json) =>
      _$$BenchmarkUserImplFromJson(json);

  @override
  final String id;
  @override
  final String name;
  @override
  final String email;
  @override
  final int age;
  @override
  final DateTime createdAt;
  final Map<String, dynamic> _metadata;
  @override
  @JsonKey()
  Map<String, dynamic> get metadata {
    if (_metadata is EqualUnmodifiableMapView) return _metadata;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableMapView(_metadata);
  }

  @override
  String toString() {
    return 'BenchmarkUser(id: $id, name: $name, email: $email, age: $age, createdAt: $createdAt, metadata: $metadata)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$BenchmarkUserImpl &&
            (identical(other.id, id) || other.id == id) &&
            (identical(other.name, name) || other.name == name) &&
            (identical(other.email, email) || other.email == email) &&
            (identical(other.age, age) || other.age == age) &&
            (identical(other.createdAt, createdAt) ||
                other.createdAt == createdAt) &&
            const DeepCollectionEquality().equals(other._metadata, _metadata));
  }

  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  int get hashCode => Object.hash(runtimeType, id, name, email, age, createdAt,
      const DeepCollectionEquality().hash(_metadata));

  /// Create a copy of BenchmarkUser
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$BenchmarkUserImplCopyWith<_$BenchmarkUserImpl> get copyWith =>
      __$$BenchmarkUserImplCopyWithImpl<_$BenchmarkUserImpl>(this, _$identity);

  @override
  Map<String, dynamic> toJson() {
    return _$$BenchmarkUserImplToJson(
      this,
    );
  }
}

abstract class _BenchmarkUser implements BenchmarkUser {
  const factory _BenchmarkUser(
      {required final String id,
      required final String name,
      required final String email,
      required final int age,
      required final DateTime createdAt,
      final Map<String, dynamic> metadata}) = _$BenchmarkUserImpl;

  factory _BenchmarkUser.fromJson(Map<String, dynamic> json) =
      _$BenchmarkUserImpl.fromJson;

  @override
  String get id;
  @override
  String get name;
  @override
  String get email;
  @override
  int get age;
  @override
  DateTime get createdAt;
  @override
  Map<String, dynamic> get metadata;

  /// Create a copy of BenchmarkUser
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$BenchmarkUserImplCopyWith<_$BenchmarkUserImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

BenchmarkPost _$BenchmarkPostFromJson(Map<String, dynamic> json) {
  return _BenchmarkPost.fromJson(json);
}

/// @nodoc
mixin _$BenchmarkPost {
  String get id => throw _privateConstructorUsedError;
  String get title => throw _privateConstructorUsedError;
  String get content => throw _privateConstructorUsedError;
  String get authorId => throw _privateConstructorUsedError;
  DateTime get createdAt => throw _privateConstructorUsedError;
  DateTime? get updatedAt => throw _privateConstructorUsedError;
  List<String> get tags => throw _privateConstructorUsedError;
  Map<String, dynamic> get metadata => throw _privateConstructorUsedError;

  /// Serializes this BenchmarkPost to a JSON map.
  Map<String, dynamic> toJson() => throw _privateConstructorUsedError;

  /// Create a copy of BenchmarkPost
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  $BenchmarkPostCopyWith<BenchmarkPost> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $BenchmarkPostCopyWith<$Res> {
  factory $BenchmarkPostCopyWith(
          BenchmarkPost value, $Res Function(BenchmarkPost) then) =
      _$BenchmarkPostCopyWithImpl<$Res, BenchmarkPost>;
  @useResult
  $Res call(
      {String id,
      String title,
      String content,
      String authorId,
      DateTime createdAt,
      DateTime? updatedAt,
      List<String> tags,
      Map<String, dynamic> metadata});
}

/// @nodoc
class _$BenchmarkPostCopyWithImpl<$Res, $Val extends BenchmarkPost>
    implements $BenchmarkPostCopyWith<$Res> {
  _$BenchmarkPostCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of BenchmarkPost
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? id = null,
    Object? title = null,
    Object? content = null,
    Object? authorId = null,
    Object? createdAt = null,
    Object? updatedAt = freezed,
    Object? tags = null,
    Object? metadata = null,
  }) {
    return _then(_value.copyWith(
      id: null == id
          ? _value.id
          : id // ignore: cast_nullable_to_non_nullable
              as String,
      title: null == title
          ? _value.title
          : title // ignore: cast_nullable_to_non_nullable
              as String,
      content: null == content
          ? _value.content
          : content // ignore: cast_nullable_to_non_nullable
              as String,
      authorId: null == authorId
          ? _value.authorId
          : authorId // ignore: cast_nullable_to_non_nullable
              as String,
      createdAt: null == createdAt
          ? _value.createdAt
          : createdAt // ignore: cast_nullable_to_non_nullable
              as DateTime,
      updatedAt: freezed == updatedAt
          ? _value.updatedAt
          : updatedAt // ignore: cast_nullable_to_non_nullable
              as DateTime?,
      tags: null == tags
          ? _value.tags
          : tags // ignore: cast_nullable_to_non_nullable
              as List<String>,
      metadata: null == metadata
          ? _value.metadata
          : metadata // ignore: cast_nullable_to_non_nullable
              as Map<String, dynamic>,
    ) as $Val);
  }
}

/// @nodoc
abstract class _$$BenchmarkPostImplCopyWith<$Res>
    implements $BenchmarkPostCopyWith<$Res> {
  factory _$$BenchmarkPostImplCopyWith(
          _$BenchmarkPostImpl value, $Res Function(_$BenchmarkPostImpl) then) =
      __$$BenchmarkPostImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call(
      {String id,
      String title,
      String content,
      String authorId,
      DateTime createdAt,
      DateTime? updatedAt,
      List<String> tags,
      Map<String, dynamic> metadata});
}

/// @nodoc
class __$$BenchmarkPostImplCopyWithImpl<$Res>
    extends _$BenchmarkPostCopyWithImpl<$Res, _$BenchmarkPostImpl>
    implements _$$BenchmarkPostImplCopyWith<$Res> {
  __$$BenchmarkPostImplCopyWithImpl(
      _$BenchmarkPostImpl _value, $Res Function(_$BenchmarkPostImpl) _then)
      : super(_value, _then);

  /// Create a copy of BenchmarkPost
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? id = null,
    Object? title = null,
    Object? content = null,
    Object? authorId = null,
    Object? createdAt = null,
    Object? updatedAt = freezed,
    Object? tags = null,
    Object? metadata = null,
  }) {
    return _then(_$BenchmarkPostImpl(
      id: null == id
          ? _value.id
          : id // ignore: cast_nullable_to_non_nullable
              as String,
      title: null == title
          ? _value.title
          : title // ignore: cast_nullable_to_non_nullable
              as String,
      content: null == content
          ? _value.content
          : content // ignore: cast_nullable_to_non_nullable
              as String,
      authorId: null == authorId
          ? _value.authorId
          : authorId // ignore: cast_nullable_to_non_nullable
              as String,
      createdAt: null == createdAt
          ? _value.createdAt
          : createdAt // ignore: cast_nullable_to_non_nullable
              as DateTime,
      updatedAt: freezed == updatedAt
          ? _value.updatedAt
          : updatedAt // ignore: cast_nullable_to_non_nullable
              as DateTime?,
      tags: null == tags
          ? _value._tags
          : tags // ignore: cast_nullable_to_non_nullable
              as List<String>,
      metadata: null == metadata
          ? _value._metadata
          : metadata // ignore: cast_nullable_to_non_nullable
              as Map<String, dynamic>,
    ));
  }
}

/// @nodoc
@JsonSerializable()
class _$BenchmarkPostImpl implements _BenchmarkPost {
  const _$BenchmarkPostImpl(
      {required this.id,
      required this.title,
      required this.content,
      required this.authorId,
      required this.createdAt,
      this.updatedAt,
      final List<String> tags = const [],
      final Map<String, dynamic> metadata = const {}})
      : _tags = tags,
        _metadata = metadata;

  factory _$BenchmarkPostImpl.fromJson(Map<String, dynamic> json) =>
      _$$BenchmarkPostImplFromJson(json);

  @override
  final String id;
  @override
  final String title;
  @override
  final String content;
  @override
  final String authorId;
  @override
  final DateTime createdAt;
  @override
  final DateTime? updatedAt;
  final List<String> _tags;
  @override
  @JsonKey()
  List<String> get tags {
    if (_tags is EqualUnmodifiableListView) return _tags;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableListView(_tags);
  }

  final Map<String, dynamic> _metadata;
  @override
  @JsonKey()
  Map<String, dynamic> get metadata {
    if (_metadata is EqualUnmodifiableMapView) return _metadata;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableMapView(_metadata);
  }

  @override
  String toString() {
    return 'BenchmarkPost(id: $id, title: $title, content: $content, authorId: $authorId, createdAt: $createdAt, updatedAt: $updatedAt, tags: $tags, metadata: $metadata)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$BenchmarkPostImpl &&
            (identical(other.id, id) || other.id == id) &&
            (identical(other.title, title) || other.title == title) &&
            (identical(other.content, content) || other.content == content) &&
            (identical(other.authorId, authorId) ||
                other.authorId == authorId) &&
            (identical(other.createdAt, createdAt) ||
                other.createdAt == createdAt) &&
            (identical(other.updatedAt, updatedAt) ||
                other.updatedAt == updatedAt) &&
            const DeepCollectionEquality().equals(other._tags, _tags) &&
            const DeepCollectionEquality().equals(other._metadata, _metadata));
  }

  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  int get hashCode => Object.hash(
      runtimeType,
      id,
      title,
      content,
      authorId,
      createdAt,
      updatedAt,
      const DeepCollectionEquality().hash(_tags),
      const DeepCollectionEquality().hash(_metadata));

  /// Create a copy of BenchmarkPost
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$BenchmarkPostImplCopyWith<_$BenchmarkPostImpl> get copyWith =>
      __$$BenchmarkPostImplCopyWithImpl<_$BenchmarkPostImpl>(this, _$identity);

  @override
  Map<String, dynamic> toJson() {
    return _$$BenchmarkPostImplToJson(
      this,
    );
  }
}

abstract class _BenchmarkPost implements BenchmarkPost {
  const factory _BenchmarkPost(
      {required final String id,
      required final String title,
      required final String content,
      required final String authorId,
      required final DateTime createdAt,
      final DateTime? updatedAt,
      final List<String> tags,
      final Map<String, dynamic> metadata}) = _$BenchmarkPostImpl;

  factory _BenchmarkPost.fromJson(Map<String, dynamic> json) =
      _$BenchmarkPostImpl.fromJson;

  @override
  String get id;
  @override
  String get title;
  @override
  String get content;
  @override
  String get authorId;
  @override
  DateTime get createdAt;
  @override
  DateTime? get updatedAt;
  @override
  List<String> get tags;
  @override
  Map<String, dynamic> get metadata;

  /// Create a copy of BenchmarkPost
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$BenchmarkPostImplCopyWith<_$BenchmarkPostImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

BenchmarkComment _$BenchmarkCommentFromJson(Map<String, dynamic> json) {
  return _BenchmarkComment.fromJson(json);
}

/// @nodoc
mixin _$BenchmarkComment {
  String get id => throw _privateConstructorUsedError;
  String get postId => throw _privateConstructorUsedError;
  String get authorId => throw _privateConstructorUsedError;
  String get content => throw _privateConstructorUsedError;
  DateTime get createdAt => throw _privateConstructorUsedError;
  DateTime? get editedAt => throw _privateConstructorUsedError;
  int get likes => throw _privateConstructorUsedError;
  Map<String, dynamic> get metadata => throw _privateConstructorUsedError;

  /// Serializes this BenchmarkComment to a JSON map.
  Map<String, dynamic> toJson() => throw _privateConstructorUsedError;

  /// Create a copy of BenchmarkComment
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  $BenchmarkCommentCopyWith<BenchmarkComment> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $BenchmarkCommentCopyWith<$Res> {
  factory $BenchmarkCommentCopyWith(
          BenchmarkComment value, $Res Function(BenchmarkComment) then) =
      _$BenchmarkCommentCopyWithImpl<$Res, BenchmarkComment>;
  @useResult
  $Res call(
      {String id,
      String postId,
      String authorId,
      String content,
      DateTime createdAt,
      DateTime? editedAt,
      int likes,
      Map<String, dynamic> metadata});
}

/// @nodoc
class _$BenchmarkCommentCopyWithImpl<$Res, $Val extends BenchmarkComment>
    implements $BenchmarkCommentCopyWith<$Res> {
  _$BenchmarkCommentCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of BenchmarkComment
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? id = null,
    Object? postId = null,
    Object? authorId = null,
    Object? content = null,
    Object? createdAt = null,
    Object? editedAt = freezed,
    Object? likes = null,
    Object? metadata = null,
  }) {
    return _then(_value.copyWith(
      id: null == id
          ? _value.id
          : id // ignore: cast_nullable_to_non_nullable
              as String,
      postId: null == postId
          ? _value.postId
          : postId // ignore: cast_nullable_to_non_nullable
              as String,
      authorId: null == authorId
          ? _value.authorId
          : authorId // ignore: cast_nullable_to_non_nullable
              as String,
      content: null == content
          ? _value.content
          : content // ignore: cast_nullable_to_non_nullable
              as String,
      createdAt: null == createdAt
          ? _value.createdAt
          : createdAt // ignore: cast_nullable_to_non_nullable
              as DateTime,
      editedAt: freezed == editedAt
          ? _value.editedAt
          : editedAt // ignore: cast_nullable_to_non_nullable
              as DateTime?,
      likes: null == likes
          ? _value.likes
          : likes // ignore: cast_nullable_to_non_nullable
              as int,
      metadata: null == metadata
          ? _value.metadata
          : metadata // ignore: cast_nullable_to_non_nullable
              as Map<String, dynamic>,
    ) as $Val);
  }
}

/// @nodoc
abstract class _$$BenchmarkCommentImplCopyWith<$Res>
    implements $BenchmarkCommentCopyWith<$Res> {
  factory _$$BenchmarkCommentImplCopyWith(_$BenchmarkCommentImpl value,
          $Res Function(_$BenchmarkCommentImpl) then) =
      __$$BenchmarkCommentImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call(
      {String id,
      String postId,
      String authorId,
      String content,
      DateTime createdAt,
      DateTime? editedAt,
      int likes,
      Map<String, dynamic> metadata});
}

/// @nodoc
class __$$BenchmarkCommentImplCopyWithImpl<$Res>
    extends _$BenchmarkCommentCopyWithImpl<$Res, _$BenchmarkCommentImpl>
    implements _$$BenchmarkCommentImplCopyWith<$Res> {
  __$$BenchmarkCommentImplCopyWithImpl(_$BenchmarkCommentImpl _value,
      $Res Function(_$BenchmarkCommentImpl) _then)
      : super(_value, _then);

  /// Create a copy of BenchmarkComment
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? id = null,
    Object? postId = null,
    Object? authorId = null,
    Object? content = null,
    Object? createdAt = null,
    Object? editedAt = freezed,
    Object? likes = null,
    Object? metadata = null,
  }) {
    return _then(_$BenchmarkCommentImpl(
      id: null == id
          ? _value.id
          : id // ignore: cast_nullable_to_non_nullable
              as String,
      postId: null == postId
          ? _value.postId
          : postId // ignore: cast_nullable_to_non_nullable
              as String,
      authorId: null == authorId
          ? _value.authorId
          : authorId // ignore: cast_nullable_to_non_nullable
              as String,
      content: null == content
          ? _value.content
          : content // ignore: cast_nullable_to_non_nullable
              as String,
      createdAt: null == createdAt
          ? _value.createdAt
          : createdAt // ignore: cast_nullable_to_non_nullable
              as DateTime,
      editedAt: freezed == editedAt
          ? _value.editedAt
          : editedAt // ignore: cast_nullable_to_non_nullable
              as DateTime?,
      likes: null == likes
          ? _value.likes
          : likes // ignore: cast_nullable_to_non_nullable
              as int,
      metadata: null == metadata
          ? _value._metadata
          : metadata // ignore: cast_nullable_to_non_nullable
              as Map<String, dynamic>,
    ));
  }
}

/// @nodoc
@JsonSerializable()
class _$BenchmarkCommentImpl implements _BenchmarkComment {
  const _$BenchmarkCommentImpl(
      {required this.id,
      required this.postId,
      required this.authorId,
      required this.content,
      required this.createdAt,
      this.editedAt,
      this.likes = 0,
      final Map<String, dynamic> metadata = const {}})
      : _metadata = metadata;

  factory _$BenchmarkCommentImpl.fromJson(Map<String, dynamic> json) =>
      _$$BenchmarkCommentImplFromJson(json);

  @override
  final String id;
  @override
  final String postId;
  @override
  final String authorId;
  @override
  final String content;
  @override
  final DateTime createdAt;
  @override
  final DateTime? editedAt;
  @override
  @JsonKey()
  final int likes;
  final Map<String, dynamic> _metadata;
  @override
  @JsonKey()
  Map<String, dynamic> get metadata {
    if (_metadata is EqualUnmodifiableMapView) return _metadata;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableMapView(_metadata);
  }

  @override
  String toString() {
    return 'BenchmarkComment(id: $id, postId: $postId, authorId: $authorId, content: $content, createdAt: $createdAt, editedAt: $editedAt, likes: $likes, metadata: $metadata)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$BenchmarkCommentImpl &&
            (identical(other.id, id) || other.id == id) &&
            (identical(other.postId, postId) || other.postId == postId) &&
            (identical(other.authorId, authorId) ||
                other.authorId == authorId) &&
            (identical(other.content, content) || other.content == content) &&
            (identical(other.createdAt, createdAt) ||
                other.createdAt == createdAt) &&
            (identical(other.editedAt, editedAt) ||
                other.editedAt == editedAt) &&
            (identical(other.likes, likes) || other.likes == likes) &&
            const DeepCollectionEquality().equals(other._metadata, _metadata));
  }

  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  int get hashCode => Object.hash(
      runtimeType,
      id,
      postId,
      authorId,
      content,
      createdAt,
      editedAt,
      likes,
      const DeepCollectionEquality().hash(_metadata));

  /// Create a copy of BenchmarkComment
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$BenchmarkCommentImplCopyWith<_$BenchmarkCommentImpl> get copyWith =>
      __$$BenchmarkCommentImplCopyWithImpl<_$BenchmarkCommentImpl>(
          this, _$identity);

  @override
  Map<String, dynamic> toJson() {
    return _$$BenchmarkCommentImplToJson(
      this,
    );
  }
}

abstract class _BenchmarkComment implements BenchmarkComment {
  const factory _BenchmarkComment(
      {required final String id,
      required final String postId,
      required final String authorId,
      required final String content,
      required final DateTime createdAt,
      final DateTime? editedAt,
      final int likes,
      final Map<String, dynamic> metadata}) = _$BenchmarkCommentImpl;

  factory _BenchmarkComment.fromJson(Map<String, dynamic> json) =
      _$BenchmarkCommentImpl.fromJson;

  @override
  String get id;
  @override
  String get postId;
  @override
  String get authorId;
  @override
  String get content;
  @override
  DateTime get createdAt;
  @override
  DateTime? get editedAt;
  @override
  int get likes;
  @override
  Map<String, dynamic> get metadata;

  /// Create a copy of BenchmarkComment
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$BenchmarkCommentImplCopyWith<_$BenchmarkCommentImpl> get copyWith =>
      throw _privateConstructorUsedError;
}
