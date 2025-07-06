// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'user_model.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

_$UserImpl _$$UserImplFromJson(Map<String, dynamic> json) => _$UserImpl(
      id: json['id'] as String,
      name: json['name'] as String,
      email: json['email'] as String,
      age: (json['age'] as num?)?.toInt(),
      createdAt: DateTime.parse(json['createdAt'] as String),
      metadata: json['metadata'] as Map<String, dynamic>?,
    );

Map<String, dynamic> _$$UserImplToJson(_$UserImpl instance) =>
    <String, dynamic>{
      'id': instance.id,
      'name': instance.name,
      'email': instance.email,
      'age': instance.age,
      'createdAt': instance.createdAt.toIso8601String(),
      'metadata': instance.metadata,
    };

_$UserStateLoadingImpl _$$UserStateLoadingImplFromJson(
        Map<String, dynamic> json) =>
    _$UserStateLoadingImpl(
      $type: json['runtimeType'] as String?,
    );

Map<String, dynamic> _$$UserStateLoadingImplToJson(
        _$UserStateLoadingImpl instance) =>
    <String, dynamic>{
      'runtimeType': instance.$type,
    };

_$UserStateDataImpl _$$UserStateDataImplFromJson(Map<String, dynamic> json) =>
    _$UserStateDataImpl(
      User.fromJson(json['user'] as Map<String, dynamic>),
      $type: json['runtimeType'] as String?,
    );

Map<String, dynamic> _$$UserStateDataImplToJson(_$UserStateDataImpl instance) =>
    <String, dynamic>{
      'user': instance.user,
      'runtimeType': instance.$type,
    };

_$UserStateErrorImpl _$$UserStateErrorImplFromJson(Map<String, dynamic> json) =>
    _$UserStateErrorImpl(
      json['message'] as String,
      $type: json['runtimeType'] as String?,
    );

Map<String, dynamic> _$$UserStateErrorImplToJson(
        _$UserStateErrorImpl instance) =>
    <String, dynamic>{
      'message': instance.message,
      'runtimeType': instance.$type,
    };
