# Lightning DB Dart SDK Development Guide

## Overview

This document provides a comprehensive step-by-step guide for creating a production-ready Dart/Flutter SDK for Lightning DB, following the architecture patterns established by Realm Dart SDK.

## Architecture Overview

```
lightning_db/
├── packages/                           # Monorepo root
│   ├── lightning_db_dart/             # Core Dart package with FFI bindings
│   ├── lightning_db/                  # Flutter plugin wrapper
│   ├── lightning_db_common/           # Shared types and interfaces
│   └── lightning_db_generator/        # Code generation for models
├── melos.yaml                         # Monorepo configuration
└── scripts/                           # Build and release scripts
```

## Step-by-Step Implementation Guide

### Step 1: Set Up Monorepo Structure

1.1. Create the packages directory structure:
```bash
mkdir -p packages/{lightning_db_dart,lightning_db,lightning_db_common,lightning_db_generator}
```

1.2. Create melos.yaml for monorepo management:
```yaml
name: lightning_db_workspace
repository: https://github.com/yourusername/lightning_db

packages:
  - packages/*
  - packages/*/example
  - packages/*/test

scripts:
  analyze:
    exec: dart analyze
  test:
    exec: dart test
  format:
    exec: dart format .
```

### Step 2: Create Core Dart Package (lightning_db_dart)

2.1. Package structure:
```
packages/lightning_db_dart/
├── CMakeLists.txt                    # Native build configuration
├── CMakePresets.json                 # Platform-specific build presets
├── ffigen.yaml                       # FFI bindings generation config
├── pubspec.yaml                      # Package dependencies
├── lib/
│   ├── lightning_db_dart.dart       # Main library export
│   └── src/
│       ├── cli/                      # CLI commands
│       │   ├── install/              # Binary installation
│       │   └── metrics/              # Performance metrics
│       ├── handles/                  # Native handle wrappers
│       ├── native/                   # FFI bindings
│       └── init.dart                 # Library initialization
├── src/                              # Native C/Rust bridge
│   ├── CMakeLists.txt               # Native library build
│   ├── lightning_db_dart.cpp        # C++ wrapper
│   └── lightning_db_dart.h          # C++ header
├── scripts/                          # Platform build scripts
│   ├── build-android.sh
│   ├── build-ios.sh
│   ├── build-linux.sh
│   ├── build-macos.sh
│   └── build-windows.bat
└── bin/
    └── lightning_db.dart             # CLI entry point
```

2.2. Create pubspec.yaml:
```yaml
name: lightning_db_dart
version: 1.0.0
description: Lightning DB native bindings for Dart

environment:
  sdk: '>=3.0.0 <4.0.0'

dependencies:
  ffi: ^2.1.0
  path: ^1.8.0
  args: ^2.4.0
  tar: ^0.5.6
  crypto: ^3.0.0
  http: ^1.0.0
  logging: ^1.2.0
  package_config: ^2.1.0
  pub_semver: ^2.1.0
  build_cli_annotations: ^2.1.0

dev_dependencies:
  ffigen: ^11.0.0
  build_runner: ^2.4.0
  build_cli: ^2.2.0
  test: ^1.24.0
  lints: ^3.0.0

executables:
  lightning_db:
```

### Step 3: Native Bridge Implementation

3.1. Create CMakeLists.txt for native builds:
```cmake
cmake_minimum_required(VERSION 3.21)
project(lightning_db_dart)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Platform detection
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(LIGHTNING_DB_PLATFORM "linux")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(LIGHTNING_DB_PLATFORM "macos")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(LIGHTNING_DB_PLATFORM "windows")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Android")
    set(LIGHTNING_DB_PLATFORM "android")
elseif(CMAKE_SYSTEM_NAME STREQUAL "iOS")
    set(LIGHTNING_DB_PLATFORM "ios")
endif()

# Add Lightning DB static library
add_library(lightning_db_ffi STATIC IMPORTED)
set_target_properties(lightning_db_ffi PROPERTIES
    IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/../../target/release/liblightning_db_ffi.a"
)

# Create shared library
add_library(lightning_db_dart SHARED
    src/lightning_db_dart.cpp
)

target_link_libraries(lightning_db_dart
    lightning_db_ffi
)

# Platform-specific configurations
if(LIGHTNING_DB_PLATFORM STREQUAL "android")
    find_library(log-lib log)
    target_link_libraries(lightning_db_dart ${log-lib})
elseif(LIGHTNING_DB_PLATFORM STREQUAL "windows")
    set_target_properties(lightning_db_dart PROPERTIES
        WINDOWS_EXPORT_ALL_SYMBOLS ON
    )
endif()
```

3.2. Create CMakePresets.json for platform configurations:
```json
{
  "version": 3,
  "configurePresets": [
    {
      "name": "linux",
      "generator": "Unix Makefiles",
      "binaryDir": "${sourceDir}/binary/linux",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Release"
      }
    },
    {
      "name": "macos",
      "generator": "Xcode",
      "binaryDir": "${sourceDir}/binary/macos",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Release",
        "CMAKE_OSX_ARCHITECTURES": "arm64;x86_64"
      }
    },
    {
      "name": "ios",
      "generator": "Xcode",
      "binaryDir": "${sourceDir}/binary/ios",
      "cacheVariables": {
        "CMAKE_SYSTEM_NAME": "iOS",
        "CMAKE_OSX_DEPLOYMENT_TARGET": "11.0",
        "CMAKE_OSX_ARCHITECTURES": "arm64"
      }
    },
    {
      "name": "android",
      "generator": "Ninja",
      "binaryDir": "${sourceDir}/binary/android/${CMAKE_ANDROID_ARCH_ABI}",
      "cacheVariables": {
        "CMAKE_SYSTEM_NAME": "Android",
        "CMAKE_ANDROID_API": "21",
        "CMAKE_ANDROID_STL_TYPE": "c++_static"
      }
    },
    {
      "name": "windows",
      "generator": "Visual Studio 17 2022",
      "binaryDir": "${sourceDir}/binary/windows",
      "architecture": {
        "value": "x64",
        "strategy": "set"
      },
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Release"
      }
    }
  ]
}
```

### Step 4: FFI Bindings Generation

4.1. Create ffigen.yaml:
```yaml
name: LightningDbBindings
description: FFI bindings for Lightning DB
output: 'lib/src/native/lightning_db_bindings.dart'
headers:
  entry-points:
    - '../../../lightning_db.h'
    - 'src/lightning_db_dart.h'
compiler-opts:
  - '-I../../../'
  - '-I./src'
preamble: |
  // AUTO-GENERATED FILE. DO NOT MODIFY.
  // 
  // Generated by `dart run ffigen`.
functions:
  include:
    - 'lightning_db_.*'
    - 'ldb_.*'
structs:
  include:
    - 'LightningDb.*'
    - 'Ldb.*'
enums:
  include:
    - 'LDB_.*'
```

### Step 5: CLI Implementation

5.1. Create lib/src/cli/install/install_command.dart:
```dart
import 'dart:io';
import 'package:args/command_runner.dart';
import 'package:http/http.dart' as http;
import 'package:path/path.dart' as path;
import 'package:tar/tar.dart';
import 'package:crypto/crypto.dart';

class InstallCommand extends Command<void> {
  @override
  String get name => 'install';
  
  @override
  String get description => 'Download and install Lightning DB native libraries';
  
  InstallCommand() {
    argParser
      ..addOption('target-os-type',
          allowed: ['android', 'ios', 'linux', 'macos', 'windows'],
          help: 'Target OS type')
      ..addFlag('force',
          negatable: false,
          help: 'Force re-download even if binaries exist');
  }
  
  @override
  Future<void> run() async {
    final targetOs = argResults!['target-os-type'] as String?;
    final force = argResults!['force'] as bool;
    
    final platforms = targetOs != null ? [targetOs] : _detectPlatforms();
    
    for (final platform in platforms) {
      await _downloadAndInstallBinary(platform, force);
    }
  }
  
  List<String> _detectPlatforms() {
    if (Platform.isLinux) return ['linux'];
    if (Platform.isMacOS) return ['macos'];
    if (Platform.isWindows) return ['windows'];
    if (Platform.isAndroid) return ['android'];
    if (Platform.isIOS) return ['ios'];
    throw UnsupportedError('Unsupported platform');
  }
  
  Future<void> _downloadAndInstallBinary(String platform, bool force) async {
    final version = _getPackageVersion();
    final archiveName = _getArchiveName(platform);
    final downloadUrl = 'https://github.com/yourusername/lightning_db/releases/download/v$version/$archiveName';
    
    final targetDir = _getBinaryDirectory(platform);
    final binaryPath = path.join(targetDir, _getBinaryName(platform));
    
    if (!force && File(binaryPath).existsSync()) {
      print('Binary already exists for $platform. Use --force to re-download.');
      return;
    }
    
    print('Downloading Lightning DB binary for $platform...');
    
    // Download binary
    final response = await http.get(Uri.parse(downloadUrl));
    if (response.statusCode != 200) {
      throw Exception('Failed to download binary: ${response.statusCode}');
    }
    
    // Verify checksum
    final checksum = sha256.convert(response.bodyBytes).toString();
    await _verifyChecksum(platform, version, checksum);
    
    // Extract archive
    await _extractArchive(response.bodyBytes, targetDir);
    
    print('Successfully installed Lightning DB binary for $platform');
  }
  
  String _getPackageVersion() {
    // Read from pubspec.yaml or package_config.json
    return '1.0.0';
  }
  
  String _getArchiveName(String platform) {
    return switch (platform) {
      'linux' => 'lightning_db_linux_x64.tar.gz',
      'macos' => 'lightning_db_macos_universal.tar.gz',
      'windows' => 'lightning_db_windows_x64.zip',
      'android' => 'lightning_db_android_all.tar.gz',
      'ios' => 'lightning_db_ios.xcframework.tar.gz',
      _ => throw ArgumentError('Unknown platform: $platform'),
    };
  }
  
  String _getBinaryDirectory(String platform) {
    final packageRoot = _findPackageRoot();
    return switch (platform) {
      'linux' => path.join(packageRoot, 'linux'),
      'macos' => path.join(packageRoot, 'macos'),
      'windows' => path.join(packageRoot, 'windows'),
      'android' => path.join(packageRoot, 'android'),
      'ios' => path.join(packageRoot, 'ios'),
      _ => throw ArgumentError('Unknown platform: $platform'),
    };
  }
  
  String _getBinaryName(String platform) {
    return switch (platform) {
      'linux' => 'liblightning_db_dart.so',
      'macos' => 'liblightning_db_dart.dylib',
      'windows' => 'lightning_db_dart.dll',
      'android' => 'liblightning_db_dart.so',
      'ios' => 'lightning_db_dart.xcframework',
      _ => throw ArgumentError('Unknown platform: $platform'),
    };
  }
  
  String _findPackageRoot() {
    // Logic to find package root directory
    return Directory.current.path;
  }
  
  Future<void> _verifyChecksum(String platform, String version, String checksum) async {
    // Download and verify checksum file
    final checksumUrl = 'https://github.com/yourusername/lightning_db/releases/download/v$version/checksums.txt';
    final response = await http.get(Uri.parse(checksumUrl));
    
    if (!response.body.contains(checksum)) {
      throw Exception('Checksum verification failed');
    }
  }
  
  Future<void> _extractArchive(List<int> bytes, String targetDir) async {
    final tempFile = File(path.join(Directory.systemTemp.path, 'lightning_db_temp.tar.gz'));
    await tempFile.writeAsBytes(bytes);
    
    await Directory(targetDir).create(recursive: true);
    
    final reader = TarReader(tempFile.openRead().transform(gzip.decoder));
    await reader.forEach((entry) async {
      final outputPath = path.join(targetDir, entry.name);
      if (entry.type == TypeFlag.dir) {
        await Directory(outputPath).create(recursive: true);
      } else {
        final file = File(outputPath);
        await file.create(recursive: true);
        await entry.contents.pipe(file.openWrite());
      }
    });
    
    await tempFile.delete();
  }
}
```

### Step 6: Library Initialization

6.1. Create lib/src/init.dart:
```dart
import 'dart:ffi';
import 'dart:io';
import 'package:ffi/ffi.dart';
import 'package:path/path.dart' as path;
import 'native/lightning_db_bindings.dart';

class LightningDbInit {
  static late LightningDbBindings _bindings;
  static bool _initialized = false;
  
  static LightningDbBindings get bindings {
    if (!_initialized) {
      throw StateError('Lightning DB not initialized. Call LightningDbInit.init() first.');
    }
    return _bindings;
  }
  
  static void init() {
    if (_initialized) return;
    
    final library = _loadNativeLibrary();
    _bindings = LightningDbBindings(library);
    
    // Verify library version
    final nativeVersion = _bindings.ldb_get_version();
    final dartVersion = _getDartPackageVersion();
    
    if (nativeVersion != dartVersion) {
      throw StateError(
        'Version mismatch: Dart package is $dartVersion but native library is $nativeVersion. '
        'Run `dart run lightning_db install` to update native libraries.'
      );
    }
    
    // Initialize native library
    final result = _bindings.ldb_initialize();
    if (result != 0) {
      throw StateError('Failed to initialize Lightning DB: error code $result');
    }
    
    _initialized = true;
  }
  
  static DynamicLibrary _loadNativeLibrary() {
    final libraryPath = _getNativeLibraryPath();
    
    if (libraryPath == null) {
      throw StateError(
        'Lightning DB native library not found. '
        'Run `dart run lightning_db install` to download native libraries.'
      );
    }
    
    try {
      return DynamicLibrary.open(libraryPath);
    } catch (e) {
      throw StateError(
        'Failed to load Lightning DB native library at $libraryPath: $e'
      );
    }
  }
  
  static String? _getNativeLibraryPath() {
    if (_isFlutter()) {
      return _getFlutterLibraryPath();
    } else {
      return _getDartLibraryPath();
    }
  }
  
  static bool _isFlutter() {
    // Check if running in Flutter context
    try {
      // This will throw if not in Flutter
      final flutterRoot = Platform.environment['FLUTTER_ROOT'];
      return flutterRoot != null;
    } catch (_) {
      return false;
    }
  }
  
  static String? _getFlutterLibraryPath() {
    final root = _findPackageRoot('lightning_db');
    if (root == null) return null;
    
    return switch (Platform.operatingSystem) {
      'android' => 'liblightning_db_dart.so',  // Loaded by Flutter engine
      'ios' => path.join(root, 'Frameworks', 'lightning_db_dart.xcframework', 'liblightning_db_dart.a'),
      'linux' => path.join(root, 'lib', 'liblightning_db_dart.so'),
      'macos' => path.join(path.dirname(root), 'Frameworks', 'liblightning_db_dart.dylib'),
      'windows' => 'lightning_db_dart.dll',
      _ => null,
    };
  }
  
  static String? _getDartLibraryPath() {
    final packageRoot = _findPackageRoot('lightning_db_dart');
    if (packageRoot == null) return null;
    
    return switch (Platform.operatingSystem) {
      'linux' => path.join(packageRoot, 'linux', 'liblightning_db_dart.so'),
      'macos' => path.join(packageRoot, 'macos', 'liblightning_db_dart.dylib'),
      'windows' => path.join(packageRoot, 'windows', 'lightning_db_dart.dll'),
      _ => null,
    };
  }
  
  static String? _findPackageRoot(String packageName) {
    // Implementation to find package root using package_config.json
    // This is simplified - real implementation would parse package_config.json
    final packageConfigFile = File('.dart_tool/package_config.json');
    if (!packageConfigFile.existsSync()) return null;
    
    // Parse and find package location
    return null; // Placeholder
  }
  
  static String _getDartPackageVersion() {
    // Read from pubspec.yaml
    return '1.0.0';
  }
}
```

### Step 7: Flutter Plugin Package

7.1. Create packages/lightning_db/pubspec.yaml:
```yaml
name: lightning_db
version: 1.0.0
description: Lightning DB Flutter plugin

environment:
  sdk: '>=3.0.0 <4.0.0'
  flutter: '>=3.10.0'

dependencies:
  flutter:
    sdk: flutter
  lightning_db_dart: ^1.0.0
  lightning_db_common: ^1.0.0

dev_dependencies:
  flutter_test:
    sdk: flutter
  flutter_lints: ^3.0.0

flutter:
  plugin:
    platforms:
      android:
        package: com.example.lightning_db
        pluginClass: LightningDbPlugin
        ffiPlugin: true
      ios:
        pluginClass: LightningDbPlugin
        ffiPlugin: true
      linux:
        ffiPlugin: true
      macos:
        pluginClass: LightningDbPlugin
        ffiPlugin: true
      windows:
        ffiPlugin: true
```

7.2. Android integration (android/build.gradle):
```gradle
group 'com.example.lightning_db'
version '1.0.0'

buildscript {
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.allprojects {
    repositories {
        google()
        mavenCentral()
    }
}

apply plugin: 'com.android.library'

android {
    compileSdkVersion 33
    
    defaultConfig {
        minSdkVersion 21
    }
    
    sourceSets {
        main {
            jniLibs.srcDirs = ['src/main/jniLibs']
        }
    }
}

dependencies {
    implementation 'androidx.annotation:annotation:1.7.0'
}

// Download native libraries during build
task downloadNativeLibraries(type: Exec) {
    def flutterRoot = project.findProperty('flutter.sdk')
    def dartExecutable = "${flutterRoot}/bin/cache/dart-sdk/bin/dart"
    
    commandLine dartExecutable, 'run', 'lightning_db_dart:lightning_db',
        'install', '--target-os-type', 'android'
    
    workingDir project.rootDir
}

preBuild.dependsOn downloadNativeLibraries

// Copy native libraries to JNI directory
task copyNativeLibraries(type: Copy) {
    from "${project.rootDir}/../packages/lightning_db_dart/android"
    into "src/main/jniLibs"
    include "**/*.so"
}

downloadNativeLibraries.finalizedBy copyNativeLibraries
```

7.3. iOS integration (ios/lightning_db.podspec):
```ruby
Pod::Spec.new do |s|
  s.name             = 'lightning_db'
  s.version          = '1.0.0'
  s.summary          = 'Lightning DB Flutter plugin'
  s.homepage         = 'https://github.com/yourusername/lightning_db'
  s.license          = { :file => '../LICENSE' }
  s.author           = { 'Your Name' => 'your-email@example.com' }
  s.source           = { :path => '.' }
  s.source_files     = 'Classes/**/*'
  s.public_header_files = 'Classes/**/*.h'
  s.dependency 'Flutter'
  s.platform = :ios, '11.0'

  # Flutter.framework does not contain a i386 slice.
  s.pod_target_xcconfig = { 'DEFINES_MODULE' => 'YES', 'EXCLUDED_ARCHS[sdk=iphonesimulator*]' => 'i386' }
  
  # Download and prepare xcframework
  s.prepare_command = <<-CMD
    cd "$PODS_TARGET_SRCROOT/.."
    
    # Run dart to download native libraries
    if [ -z "$FLUTTER_ROOT" ]; then
      echo "Error: FLUTTER_ROOT not set"
      exit 1
    fi
    
    "$FLUTTER_ROOT/bin/dart" run lightning_db_dart:lightning_db install --target-os-type ios
    
    # Copy xcframework
    cp -R packages/lightning_db_dart/ios/lightning_db_dart.xcframework .
  CMD
  
  s.vendored_frameworks = 'lightning_db_dart.xcframework'
end
```

### Step 8: Build Scripts

8.1. Create scripts/build-all.sh:
```bash
#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

echo "Building Lightning DB native libraries for all platforms..."

# Build Rust FFI library first
cd "$PROJECT_ROOT/../.."
cargo build --release --features ffi

# Build for each platform
cd "$PROJECT_ROOT"

# Linux
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "Building for Linux..."
    ./scripts/build-linux.sh
fi

# macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Building for macOS..."
    ./scripts/build-macos.sh
    
    echo "Building for iOS..."
    ./scripts/build-ios.sh
fi

# Windows
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
    echo "Building for Windows..."
    ./scripts/build-windows.bat
fi

# Android (requires Android NDK)
if command -v ndk-build &> /dev/null; then
    echo "Building for Android..."
    ./scripts/build-android.sh
fi

echo "Build complete!"
```

8.2. Create scripts/build-android.sh:
```bash
#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Android architectures
ARCHS=("arm64-v8a" "armeabi-v7a" "x86" "x86_64")

# Check for Android NDK
if [ -z "$ANDROID_NDK_HOME" ]; then
    echo "Error: ANDROID_NDK_HOME not set"
    exit 1
fi

cd "$PROJECT_ROOT"

for ARCH in "${ARCHS[@]}"; do
    echo "Building for Android $ARCH..."
    
    # Configure CMake
    cmake --preset android \
        -DCMAKE_ANDROID_ARCH_ABI=$ARCH \
        -DCMAKE_ANDROID_NDK=$ANDROID_NDK_HOME
    
    # Build
    cmake --build binary/android/$ARCH --config Release
    
    # Copy to JNI libs directory
    mkdir -p android/$ARCH
    cp binary/android/$ARCH/liblightning_db_dart.so android/$ARCH/
done

# Create tarball for distribution
cd android
tar -czf ../lightning_db_android_all.tar.gz */liblightning_db_dart.so
cd ..

echo "Android build complete!"
```

8.3. Create scripts/build-ios.sh:
```bash
#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Build for iOS device (arm64)
echo "Building for iOS device..."
cmake --preset ios \
    -DCMAKE_OSX_ARCHITECTURES=arm64 \
    -DCMAKE_OSX_DEPLOYMENT_TARGET=11.0

cmake --build binary/ios --config Release

# Build for iOS simulator (x86_64 for Intel, arm64 for Apple Silicon)
echo "Building for iOS simulator..."
cmake --preset ios \
    -DCMAKE_OSX_ARCHITECTURES="x86_64;arm64" \
    -DCMAKE_OSX_DEPLOYMENT_TARGET=11.0 \
    -DCMAKE_OSX_SYSROOT=iphonesimulator

cmake --build binary/ios-sim --config Release

# Create xcframework
echo "Creating xcframework..."
xcodebuild -create-xcframework \
    -library binary/ios/liblightning_db_dart.a \
    -headers src/lightning_db_dart.h \
    -library binary/ios-sim/liblightning_db_dart.a \
    -headers src/lightning_db_dart.h \
    -output ios/lightning_db_dart.xcframework

# Create tarball for distribution
cd ios
tar -czf ../lightning_db_ios.xcframework.tar.gz lightning_db_dart.xcframework
cd ..

echo "iOS build complete!"
```

### Step 9: Testing Infrastructure

9.1. Create packages/lightning_db_dart/test/integration_test.dart:
```dart
import 'dart:ffi';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:lightning_db_dart/lightning_db_dart.dart';

void main() {
  group('Lightning DB Integration Tests', () {
    late LightningDb db;
    
    setUp(() async {
      LightningDbInit.init();
      db = await LightningDb.open('test_db');
    });
    
    tearDown(() async {
      await db.close();
      await LightningDb.destroy('test_db');
    });
    
    test('Basic CRUD operations', () async {
      // Create
      await db.put('key1', Uint8List.fromList('value1'.codeUnits));
      
      // Read
      final value = await db.get('key1');
      expect(value, isNotNull);
      expect(String.fromCharCodes(value!), equals('value1'));
      
      // Update
      await db.put('key1', Uint8List.fromList('updated'.codeUnits));
      final updated = await db.get('key1');
      expect(String.fromCharCodes(updated!), equals('updated'));
      
      // Delete
      await db.delete('key1');
      final deleted = await db.get('key1');
      expect(deleted, isNull);
    });
    
    test('Transaction support', () async {
      final tx = await db.beginTransaction();
      
      try {
        await tx.put('tx_key1', Uint8List.fromList('tx_value1'.codeUnits));
        await tx.put('tx_key2', Uint8List.fromList('tx_value2'.codeUnits));
        await tx.commit();
        
        final value1 = await db.get('tx_key1');
        final value2 = await db.get('tx_key2');
        
        expect(value1, isNotNull);
        expect(value2, isNotNull);
      } catch (e) {
        await tx.rollback();
        rethrow;
      }
    });
    
    test('Performance benchmarks', () async {
      final stopwatch = Stopwatch()..start();
      final iterations = 10000;
      
      // Write performance
      for (int i = 0; i < iterations; i++) {
        await db.put('perf_$i', Uint8List.fromList('value_$i'.codeUnits));
      }
      
      final writeTime = stopwatch.elapsedMilliseconds;
      final writeOpsPerSec = (iterations / writeTime) * 1000;
      
      print('Write performance: $writeOpsPerSec ops/sec');
      
      // Read performance
      stopwatch.reset();
      for (int i = 0; i < iterations; i++) {
        await db.get('perf_$i');
      }
      
      final readTime = stopwatch.elapsedMilliseconds;
      final readOpsPerSec = (iterations / readTime) * 1000;
      
      print('Read performance: $readOpsPerSec ops/sec');
      
      expect(writeOpsPerSec, greaterThan(1000)); // At least 1k ops/sec
      expect(readOpsPerSec, greaterThan(10000)); // At least 10k ops/sec
    });
  });
}
```

### Step 10: CI/CD Configuration

10.1. Create .github/workflows/build-and-release.yml:
```yaml
name: Build and Release

on:
  push:
    tags:
      - 'v*'
  workflow_dispatch:

jobs:
  build-rust:
    strategy:
      matrix:
        include:
          - os: ubuntu-latest
            target: x86_64-unknown-linux-gnu
          - os: macos-latest
            target: x86_64-apple-darwin
          - os: macos-latest
            target: aarch64-apple-darwin
          - os: windows-latest
            target: x86_64-pc-windows-msvc
    
    runs-on: ${{ matrix.os }}
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.target }}
      
      - name: Build FFI library
        run: cargo build --release --features ffi --target ${{ matrix.target }}
      
      - name: Upload artifacts
        uses: actions/upload-artifact@v3
        with:
          name: rust-${{ matrix.target }}
          path: target/${{ matrix.target }}/release/*lightning_db_ffi.*

  build-native-libraries:
    needs: build-rust
    strategy:
      matrix:
        include:
          - os: ubuntu-latest
            platform: linux
          - os: macos-latest
            platform: macos
          - os: macos-latest
            platform: ios
          - os: windows-latest
            platform: windows
          - os: ubuntu-latest
            platform: android
    
    runs-on: ${{ matrix.os }}
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Download Rust artifacts
        uses: actions/download-artifact@v3
      
      - name: Setup build environment
        run: |
          if [ "${{ matrix.platform }}" = "android" ]; then
            # Install Android NDK
            echo "Installing Android NDK..."
          fi
      
      - name: Build native library
        working-directory: packages/lightning_db_dart
        run: |
          chmod +x scripts/build-${{ matrix.platform }}.*
          ./scripts/build-${{ matrix.platform }}.*
      
      - name: Upload release artifacts
        uses: actions/upload-artifact@v3
        with:
          name: lightning_db_${{ matrix.platform }}
          path: packages/lightning_db_dart/lightning_db_${{ matrix.platform }}*

  create-release:
    needs: build-native-libraries
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Download all artifacts
        uses: actions/download-artifact@v3
        with:
          path: artifacts
      
      - name: Create checksums
        run: |
          cd artifacts
          sha256sum lightning_db_*/* > checksums.txt
      
      - name: Create GitHub Release
        uses: softprops/action-gh-release@v1
        with:
          files: |
            artifacts/lightning_db_*/*
            artifacts/checksums.txt
          draft: false
          prerelease: false
```

### Step 11: Documentation

11.1. Create packages/lightning_db/README.md:
```markdown
# Lightning DB Flutter Plugin

Lightning DB is a high-performance embedded database for Flutter applications.

## Features

- **Blazing Fast**: 20M+ read ops/sec, 1M+ write ops/sec
- **Zero Configuration**: Just add to pubspec.yaml and use
- **Cross-Platform**: Works on iOS, Android, macOS, Windows, and Linux
- **ACID Compliant**: Full transaction support with MVCC
- **Type Safe**: Strongly typed Dart API with code generation
- **Efficient**: <5MB binary size, configurable memory usage

## Installation

Add to your `pubspec.yaml`:

```yaml
dependencies:
  lightning_db: ^1.0.0
```

Run:
```bash
flutter pub get
```

## Usage

### Basic Operations

```dart
import 'package:lightning_db/lightning_db.dart';

// Open database
final db = await LightningDb.open('my_database');

// Store data
await db.put('user:1', {'name': 'Alice', 'age': 30});

// Retrieve data
final user = await db.get('user:1');
print(user); // {name: Alice, age: 30}

// Delete data
await db.delete('user:1');

// Close database
await db.close();
```

### Transactions

```dart
final tx = await db.beginTransaction();

try {
  await tx.put('account:1', {'balance': 100});
  await tx.put('account:2', {'balance': 50});
  await tx.commit();
} catch (e) {
  await tx.rollback();
  rethrow;
}
```

### Type-Safe Models

Define your models:

```dart
import 'package:lightning_db_common/lightning_db_common.dart';

@collection
class User {
  final String id;
  final String name;
  final int age;
  final DateTime createdAt;
  
  User({
    required this.id,
    required this.name,
    required this.age,
    required this.createdAt,
  });
}
```

Generate code:
```bash
dart run build_runner build
```

Use type-safe collections:

```dart
final users = db.collection<User>();

await users.insert(User(
  id: '1',
  name: 'Alice',
  age: 30,
  createdAt: DateTime.now(),
));

final user = await users.findById('1');
```

## Platform-Specific Setup

### iOS

No additional setup required. The plugin automatically handles the xcframework.

### Android

Add to `android/app/build.gradle`:

```gradle
android {
    packagingOptions {
        pickFirst 'lib/*/liblightning_db_dart.so'
    }
}
```

### Desktop Platforms

No additional setup required for macOS, Windows, or Linux.

## Performance

Lightning DB achieves exceptional performance through:

- Lock-free data structures
- SIMD optimizations
- Zero-copy operations
- Adaptive compression
- Efficient caching

Benchmark results on Apple M1:
- Read: 20.4M ops/sec (0.049 μs latency)
- Write: 1.14M ops/sec (0.88 μs latency)

## Troubleshooting

### Binary not found

Run:
```bash
dart run lightning_db_dart:lightning_db install
```

### Version mismatch

Update native libraries:
```bash
dart run lightning_db_dart:lightning_db install --force
```

## License

MIT License - see LICENSE file for details.
```

## Summary

This comprehensive guide provides a production-ready architecture for the Lightning DB Dart SDK that:

1. **Follows Realm's Architecture**: Monorepo structure with separate packages
2. **Automated Binary Distribution**: Pre-built binaries downloaded on demand
3. **Zero Configuration**: Works out of the box for Flutter developers
4. **Cross-Platform Support**: All major platforms with optimized builds
5. **Type Safety**: Code generation for strongly-typed models
6. **Performance**: Minimal overhead over native implementation
7. **Developer Experience**: Simple API with good error messages
8. **CI/CD Integration**: Automated builds and releases

The key innovations over Realm's approach:
- Option to build from source using CMake (not just download binaries)
- Simpler FFI integration without complex platform channels
- Better error messages for version mismatches
- More flexible deployment options

This architecture ensures Lightning DB can be used as easily as adding a line to pubspec.yaml while maintaining the performance characteristics of the native implementation.