import 'dart:ffi';
import 'dart:io';
import 'package:path/path.dart' as path;
import 'native/lightning_db_bindings.dart';

/// Manages Lightning DB library initialization
class LightningDbInit {
  static LightningDbBindings? _bindings;
  static bool _initialized = false;
  
  /// Get the initialized bindings instance
  static LightningDbBindings get bindings {
    if (!_initialized) {
      throw StateError(
        'Lightning DB not initialized. Call LightningDbInit.init() first.'
      );
    }
    return _bindings!;
  }
  
  /// Check if Lightning DB is initialized
  static bool get isInitialized => _initialized;
  
  /// Initialize Lightning DB
  static void init() {
    if (_initialized) return;
    
    final library = _loadNativeLibrary();
    _bindings = LightningDbBindings(library);
    
    // Initialize the native library
    final result = _bindings!.lightning_db_init();
    if (result != 0) {
      throw StateError('Failed to initialize Lightning DB: error code $result');
    }
    
    _initialized = true;
  }
  
  /// Load the native library for the current platform
  static DynamicLibrary _loadNativeLibrary() {
    final libraryPath = _getNativeLibraryPath();
    
    if (libraryPath == null) {
      throw StateError(
        'Lightning DB native library not found for platform: ${Platform.operatingSystem}'
      );
    }
    
    try {
      if (libraryPath == 'process') {
        // iOS and macOS with static linking
        return DynamicLibrary.process();
      } else if (libraryPath == 'executable') {
        // Fallback for embedded scenarios
        return DynamicLibrary.executable();
      } else {
        // Dynamic library loading
        return DynamicLibrary.open(libraryPath);
      }
    } catch (e) {
      throw StateError(
        'Failed to load Lightning DB native library at $libraryPath: $e'
      );
    }
  }
  
  /// Get the native library path for the current platform
  static String? _getNativeLibraryPath() {
    // Check if we're in a Flutter context
    final isFlutter = _isFlutter();
    
    if (isFlutter) {
      return _getFlutterLibraryPath();
    } else {
      return _getDartLibraryPath();
    }
  }
  
  /// Check if running in Flutter context
  static bool _isFlutter() {
    try {
      // In Flutter, Platform will have additional properties
      return Platform.environment.containsKey('FLUTTER_ROOT') ||
             Platform.resolvedExecutable.contains('flutter');
    } catch (_) {
      return false;
    }
  }
  
  /// Get library path for Flutter applications
  static String? _getFlutterLibraryPath() {
    final os = Platform.operatingSystem;
    
    switch (os) {
      case 'android':
        // Android loads from the APK automatically
        return 'liblightning_db_ffi.so';
      case 'ios':
        // iOS uses static linking with the app binary
        return 'process';
      case 'macos':
        // macOS can use either dynamic or static linking
        if (_hasStaticLink()) {
          return 'process';
        }
        // For dynamic linking in Flutter macOS apps
        return 'liblightning_db_ffi.dylib';
      case 'windows':
        // Windows DLL in the same directory as the executable
        return 'lightning_db_ffi.dll';
      case 'linux':
        // Linux shared object
        return 'liblightning_db_ffi.so';
      default:
        return null;
    }
  }
  
  /// Get library path for pure Dart applications
  static String? _getDartLibraryPath() {
    final os = Platform.operatingSystem;
    
    // First, try to find the library relative to the package
    final packageRoot = _findPackageRoot();
    if (packageRoot != null) {
      final platformDir = _getPlatformDirectory(os);
      if (platformDir != null) {
        final libraryName = _getLibraryName(os);
        final libraryPath = path.join(packageRoot, platformDir, libraryName);
        if (File(libraryPath).existsSync()) {
          return libraryPath;
        }
      }
    }
    
    // Fallback to system library search
    return _getLibraryName(os);
  }
  
  /// Check if the library is statically linked
  static bool _hasStaticLink() {
    // Check if symbols are available in the process
    try {
      final process = DynamicLibrary.process();
      process.lookup<NativeFunction<Int32 Function()>>('lightning_db_init');
      return true;
    } catch (_) {
      return false;
    }
  }
  
  /// Find the package root directory
  static String? _findPackageRoot() {
    // This is a simplified version - in production, you would
    // parse .dart_tool/package_config.json to find the exact path
    final script = Platform.script;
    if (script.scheme == 'file') {
      var dir = Directory(script.toFilePath()).parent;
      while (dir.path != dir.parent.path) {
        if (File(path.join(dir.path, 'pubspec.yaml')).existsSync()) {
          return dir.path;
        }
        dir = dir.parent;
      }
    }
    return null;
  }
  
  /// Get platform-specific directory name
  static String? _getPlatformDirectory(String os) {
    switch (os) {
      case 'linux':
        return 'linux';
      case 'macos':
        return 'macos';
      case 'windows':
        return 'windows';
      default:
        return null;
    }
  }
  
  /// Get platform-specific library name
  static String _getLibraryName(String os) {
    switch (os) {
      case 'linux':
      case 'android':
        return 'liblightning_db_ffi.so';
      case 'macos':
      case 'ios':
        return 'liblightning_db_ffi.dylib';
      case 'windows':
        return 'lightning_db_ffi.dll';
      default:
        throw UnsupportedError('Unsupported platform: $os');
    }
  }
}