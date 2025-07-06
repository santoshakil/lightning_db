#
# To learn more about a Podspec see http://guides.cocoapods.org/syntax/podspec.html.
# Run `pod lib lint lightning_db.podspec` to validate before publishing.
#
Pod::Spec.new do |s|
  s.name             = 'lightning_db'
  s.version          = '0.0.1'
  s.summary          = 'Lightning DB Flutter plugin for macOS'
  s.description      = <<-DESC
A high-performance embedded database for Flutter applications with full support for Freezed immutable data models.
                       DESC
  s.homepage         = 'https://github.com/yourusername/lightning_db'
  s.license          = { :file => '../LICENSE' }
  s.author           = { 'Your Company' => 'email@example.com' }
  s.source           = { :path => '.' }
  s.source_files = 'Classes/**/*'
  s.dependency 'FlutterMacOS'
  s.platform = :osx, '10.14'
  s.pod_target_xcconfig = { 'DEFINES_MODULE' => 'YES' }
  s.swift_version = '5.0'

  # Use the pre-built binary
  s.vendored_libraries = 'Libraries/liblightning_db.dylib'
  
  # Download script for the binary
  s.prepare_command = <<-CMD
    cd "$PODS_TARGET_SRCROOT/../.."
    dart run lightning_db_dart:install --target-os-type macos
    mkdir -p "$PODS_TARGET_SRCROOT/Libraries"
    cp packages/lightning_db_dart/macos/liblightning_db.dylib "$PODS_TARGET_SRCROOT/Libraries/"
  CMD
end