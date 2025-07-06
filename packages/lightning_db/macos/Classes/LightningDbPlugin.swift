import Cocoa
import FlutterMacOS

/**
 * Lightning DB Flutter Plugin for macOS
 * 
 * This is an FFI plugin, so most functionality is handled through
 * the Dart FFI bindings. This class is mainly for plugin registration.
 */
public class LightningDbPlugin: NSObject, FlutterPlugin {
  public static func register(with registrar: FlutterPluginRegistrar) {
    // FFI plugins don't need to register method channels
    // The native library will be loaded automatically by Flutter
  }
}