package com.lightningdb.flutter

import io.flutter.embedding.engine.plugins.FlutterPlugin

/**
 * Lightning DB Flutter Plugin for Android
 * 
 * This is an FFI plugin, so most functionality is handled through
 * the Dart FFI bindings. This class is mainly for plugin registration.
 */
class LightningDbPlugin : FlutterPlugin {
    override fun onAttachedToEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        // FFI plugins don't need to register method channels
        // The native library will be loaded automatically by Flutter
    }

    override fun onDetachedFromEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        // Nothing to clean up for FFI plugins
    }
}