import 'package:freezed_annotation/freezed_annotation.dart';

part 'user_model.freezed.dart';
part 'user_model.g.dart';

@freezed
sealed class User with _$User {
  const factory User({
    required String id,
    required String name,
    required String email,
    int? age,
    required DateTime createdAt,
    Map<String, dynamic>? metadata,
  }) = _User;

  factory User.fromJson(Map<String, dynamic> json) => _$UserFromJson(json);
}

@freezed
sealed class UserState with _$UserState {
  const factory UserState.loading() = UserStateLoading;
  const factory UserState.data(User user) = UserStateData;
  const factory UserState.error(String message) = UserStateError;

  factory UserState.fromJson(Map<String, dynamic> json) => _$UserStateFromJson(json);
}