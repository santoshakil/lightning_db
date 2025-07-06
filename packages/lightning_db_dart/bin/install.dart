#!/usr/bin/env dart

import 'dart:io';
import 'package:args/args.dart';
import 'package:http/http.dart' as http;
import 'package:path/path.dart' as path;
import 'package:archive/archive.dart';

const String baseUrl = 'https://github.com/santoshakil/lightning_db/releases/download';
const String version = 'v0.0.1';

/// Maps OS types to binary names
final Map<String, String> binaryNames = {
  'android': 'liblightning_db.so',
  'ios': 'LightningDB.xcframework',
  'macos': 'liblightning_db.dylib',
  'windows': 'lightning_db.dll',
  'linux': 'liblightning_db.so',
};

/// Maps OS types to architectures
final Map<String, List<String>> architectures = {
  'android': ['arm64-v8a', 'armeabi-v7a', 'x86', 'x86_64'],
  'ios': ['arm64', 'x86_64'],
  'macos': ['arm64', 'x86_64'],
  'windows': ['x64'],
  'linux': ['x64'],
};

Future<void> main(List<String> arguments) async {
  final parser = ArgParser()
    ..addOption(
      'target-os-type',
      abbr: 't',
      help: 'Target OS type',
      allowed: ['android', 'ios', 'macos', 'windows', 'linux'],
      mandatory: true,
    )
    ..addFlag(
      'help',
      abbr: 'h',
      help: 'Show usage information',
      negatable: false,
    );

  try {
    final results = parser.parse(arguments);
    
    if (results['help'] as bool) {
      print('Lightning DB Binary Installer\n');
      print(parser.usage);
      exit(0);
    }

    final targetOs = results['target-os-type'] as String;
    await installBinaries(targetOs);
    
  } catch (e) {
    print('Error: $e\n');
    print(parser.usage);
    exit(1);
  }
}

Future<void> installBinaries(String targetOs) async {
  print('Installing Lightning DB binaries for $targetOs...\n');
  
  final outputDir = path.join(Directory.current.path, targetOs);
  await Directory(outputDir).create(recursive: true);
  
  if (targetOs == 'android') {
    // For Android, download binaries for each architecture
    for (final arch in architectures[targetOs]!) {
      await downloadAndExtract(
        '$baseUrl/$version/lightning_db-android-$arch.zip',
        path.join(outputDir, arch),
      );
    }
  } else if (targetOs == 'ios') {
    // For iOS, download the XCFramework
    await downloadAndExtract(
      '$baseUrl/$version/lightning_db-ios-xcframework.zip',
      outputDir,
    );
  } else if (targetOs == 'macos') {
    // For macOS, download the zip and extract
    await downloadAndExtract(
      '$baseUrl/$version/lightning_db-macos.zip',
      outputDir,
    );
  } else {
    // For other desktop platforms, download single binary
    final binaryName = binaryNames[targetOs]!;
    await downloadFile(
      '$baseUrl/$version/$binaryName',
      path.join(outputDir, binaryName),
    );
  }
  
  print('\n✅ Installation complete!');
  print('Binaries installed to: $outputDir');
}

Future<void> downloadFile(String url, String outputPath) async {
  print('Downloading $url...');
  
  try {
    final response = await http.get(Uri.parse(url));
    
    if (response.statusCode != 200) {
      throw Exception('Failed to download $url: ${response.statusCode}');
    }
    
    final file = File(outputPath);
    await file.create(recursive: true);
    await file.writeAsBytes(response.bodyBytes);
    
    print('✓ Downloaded ${path.basename(outputPath)} (${response.bodyBytes.length} bytes)');
  } catch (e) {
    print('Error downloading $url: $e');
    
    // Fallback to local development binary if available
    final localPath = _getLocalBinaryPath(outputPath);
    if (localPath != null && await File(localPath).exists()) {
      print('Using local development binary from $localPath');
      await File(localPath).copy(outputPath);
    } else {
      rethrow;
    }
  }
}

String? _getLocalBinaryPath(String outputPath) {
  final projectRoot = path.dirname(path.dirname(Directory.current.path));
  final filename = path.basename(outputPath);
  
  if (Platform.isMacOS && filename.endsWith('.dylib')) {
    return path.join(projectRoot, 'target', 'release', 'liblightning_db_ffi.dylib');
  } else if (Platform.isLinux && filename.endsWith('.so')) {
    return path.join(projectRoot, 'target', 'release', 'liblightning_db_ffi.so');
  } else if (Platform.isWindows && filename.endsWith('.dll')) {
    return path.join(projectRoot, 'target', 'release', 'lightning_db_ffi.dll');
  }
  
  return null;
}

Future<void> downloadAndExtract(String url, String outputDir) async {
  print('Downloading and extracting $url...');
  
  await Directory(outputDir).create(recursive: true);
  
  try {
    // Download the file
    final response = await http.get(Uri.parse(url));
    
    if (response.statusCode != 200) {
      throw Exception('Failed to download $url: ${response.statusCode}');
    }
    
    print('Downloaded ${response.bodyBytes.length} bytes');
    
    // Extract the ZIP
    final archive = ZipDecoder().decodeBytes(response.bodyBytes);
    print('Extracting ${archive.files.length} files...');
    
    for (final file in archive) {
      final filename = file.name;
      if (file.isFile) {
        final data = file.content as List<int>;
        final outputFile = File(path.join(outputDir, filename));
        await outputFile.create(recursive: true);
        await outputFile.writeAsBytes(data);
        print('  ✓ ${filename} (${data.length} bytes)');
      } else {
        await Directory(path.join(outputDir, filename)).create(recursive: true);
      }
    }
    
    print('✓ Extracted to $outputDir');
  } catch (e) {
    print('Error downloading/extracting $url: $e');
    
    // Fallback for development - try to use local builds
    await _fallbackToLocalBuild(url, outputDir);
  }
}

Future<void> _fallbackToLocalBuild(String url, String outputDir) async {
  print('Attempting to use local development build...');
  
  final projectRoot = _findProjectRoot();
  if (projectRoot == null) {
    throw Exception('Could not find project root for local development fallback');
  }
  
  if (url.contains('macos')) {
    await _copyMacOSLocalBuild(projectRoot, outputDir);
  } else if (url.contains('ios')) {
    await _copyIOSLocalBuild(projectRoot, outputDir);
  } else if (url.contains('android')) {
    await _copyAndroidLocalBuild(projectRoot, outputDir);
  } else {
    throw Exception('No local development fallback available for $url');
  }
}

String? _findProjectRoot() {
  // Try to find the project root by looking for Cargo.toml
  var dir = Directory.current;
  while (dir.path != dir.parent.path) {
    if (File(path.join(dir.path, 'Cargo.toml')).existsSync()) {
      return dir.path;
    }
    dir = dir.parent;
  }
  return null;
}

Future<void> _copyMacOSLocalBuild(String projectRoot, String outputDir) async {
  final sources = [
    path.join(projectRoot, 'target', 'release', 'liblightning_db_ffi.dylib'),
    path.join(projectRoot, 'target', 'debug', 'liblightning_db_ffi.dylib'),
  ];
  
  for (final source in sources) {
    if (await File(source).exists()) {
      final dest = File(path.join(outputDir, 'macos', 'liblightning_db.dylib'));
      await dest.create(recursive: true);
      await File(source).copy(dest.path);
      print('✓ Copied local macOS build from $source');
      return;
    }
  }
  
  throw Exception('No local macOS build found');
}

Future<void> _copyIOSLocalBuild(String projectRoot, String outputDir) async {
  // For iOS, we need the static libraries
  final deviceLib = path.join(projectRoot, 'target', 'aarch64-apple-ios', 'release', 'liblightning_db_ffi.a');
  final simLib = path.join(projectRoot, 'target', 'x86_64-apple-ios', 'release', 'liblightning_db_ffi.a');
  
  if (await File(deviceLib).exists() && await File(simLib).exists()) {
    // Create a basic structure that mimics XCFramework
    final iosDir = Directory(path.join(outputDir, 'ios'));
    await iosDir.create(recursive: true);
    
    final deviceDest = File(path.join(iosDir.path, 'device', 'liblightning_db_ffi.a'));
    await deviceDest.create(recursive: true);
    await File(deviceLib).copy(deviceDest.path);
    
    final simDest = File(path.join(iosDir.path, 'simulator', 'liblightning_db_ffi.a'));
    await simDest.create(recursive: true);
    await File(simLib).copy(simDest.path);
    
    print('✓ Copied local iOS builds');
    return;
  }
  
  throw Exception('No local iOS builds found');
}

Future<void> _copyAndroidLocalBuild(String projectRoot, String outputDir) async {
  final androidArches = {
    'arm64-v8a': 'aarch64-linux-android',
    'armeabi-v7a': 'armv7-linux-androideabi',
    'x86': 'i686-linux-android',
    'x86_64': 'x86_64-linux-android',
  };
  
  var copiedAny = false;
  for (final entry in androidArches.entries) {
    final androidArch = entry.key;
    final rustTarget = entry.value;
    
    final source = path.join(projectRoot, 'target', rustTarget, 'release', 'liblightning_db_ffi.so');
    if (await File(source).exists()) {
      final dest = File(path.join(outputDir, 'android', androidArch, 'liblightning_db.so'));
      await dest.create(recursive: true);
      await File(source).copy(dest.path);
      print('✓ Copied $androidArch from local build');
      copiedAny = true;
    }
  }
  
  if (!copiedAny) {
    throw Exception('No local Android builds found');
  }
}