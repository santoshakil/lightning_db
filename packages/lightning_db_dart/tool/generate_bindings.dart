import 'package:ffigen/ffigen.dart';
import 'dart:io';

void main() {
  final config = Config.fromFile(File('ffigen.yaml'));
  final library = parse(config);
  
  // Create output directory if it doesn't exist
  final outputFile = File(config.output);
  outputFile.parent.createSync(recursive: true);
  
  // Generate bindings
  outputFile.writeAsStringSync(library.generate());
  
  print('Generated bindings at: ${config.output}');
}