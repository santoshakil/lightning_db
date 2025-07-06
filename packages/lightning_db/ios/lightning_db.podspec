#
# To learn more about a Podspec see http://guides.cocoapods.org/syntax/podspec.html.
# Run `pod lib lint lightning_db.podspec` to validate before publishing.
#
Pod::Spec.new do |s|
  s.name             = 'lightning_db'
  s.version          = '0.0.1'
  s.summary          = 'Lightning DB Flutter plugin for iOS'
  s.description      = <<-DESC
A high-performance embedded database for Flutter applications with full support for Freezed immutable data models.
                       DESC
  s.homepage         = 'https://github.com/yourusername/lightning_db'
  s.license          = { :file => '../LICENSE' }
  s.author           = { 'Your Company' => 'email@example.com' }
  s.source           = { :path => '.' }
  s.source_files = 'Classes/**/*'
  s.public_header_files = 'Classes/**/*.h'
  s.dependency 'Flutter'
  s.platform = :ios, '11.0'

  # Flutter.framework does not contain a i386 slice.
  s.pod_target_xcconfig = { 'DEFINES_MODULE' => 'YES', 'EXCLUDED_ARCHS[sdk=iphonesimulator*]' => 'i386' }
  s.swift_version = '5.0'

  # Download pre-built framework
  s.preserve_paths = 'Frameworks/LightningDB.xcframework'
  s.vendored_frameworks = 'Frameworks/LightningDB.xcframework'
  
  # Download script for the binary
  s.prepare_command = <<-CMD
    cd "$PODS_TARGET_SRCROOT/../.."
    dart run lightning_db_dart:install --target-os-type ios
    mkdir -p "$PODS_TARGET_SRCROOT/Frameworks"
    cp -r packages/lightning_db_dart/ios/LightningDB.xcframework "$PODS_TARGET_SRCROOT/Frameworks/"
  CMD
end