import 'dart:io';

void main() async {
  print('Generating FFI bindings...');
  
  // Run ffigen
  final result = await Process.run(
    'dart',
    ['run', 'ffigen'],
    workingDirectory: Directory.current.path,
  );
  
  if (result.exitCode != 0) {
    print('Error generating bindings:');
    print(result.stderr);
    exit(1);
  }
  
  print('Bindings generated successfully!');
  print(result.stdout);
}