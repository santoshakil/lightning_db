import 'dart:io';

void main() {
  // Run ffigen using the Dart VM
  final result = Process.runSync(
    'dart',
    ['run', 'ffigen:generate', '--config', 'ffigen.yaml'],
    runInShell: true,
  );
  
  print(result.stdout);
  if (result.stderr.toString().isNotEmpty) {
    print('Error: ${result.stderr}');
  }
  
  exit(result.exitCode);
}