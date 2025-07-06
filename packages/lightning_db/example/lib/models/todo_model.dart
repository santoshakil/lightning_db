import 'package:freezed_annotation/freezed_annotation.dart';

part 'todo_model.freezed.dart';
part 'todo_model.g.dart';

@freezed
sealed class Todo with _$Todo {
  const factory Todo({
    required String id,
    required String title,
    String? description,
    required bool completed,
    required DateTime createdAt,
    DateTime? completedAt,
    @Default({}) Map<String, dynamic> metadata,
  }) = _Todo;
  
  factory Todo.fromJson(Map<String, dynamic> json) =>
      _$TodoFromJson(json);
}