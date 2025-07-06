#
# To learn more about a Podspec see http://guides.cocoapods.org/syntax/podspec.html.
# Run `pod lib lint lightning_db_dart.podspec` to validate before publishing.
#
Pod::Spec.new do |s|
  s.name             = 'lightning_db_dart'
  s.version          = '0.1.0'
  s.summary          = 'Dart FFI bindings for Lightning DB'
  s.description      = <<-DESC
Dart FFI bindings for Lightning DB - a high-performance embedded database
                       DESC
  s.homepage         = 'https://github.com/santoshakil/lightning_db'
  s.license          = { :file => '../LICENSE' }
  s.author           = { 'Lightning DB Team' => 'contact@lightningdb.dev' }

  s.source           = { :path => '.' }
  s.source_files = 'Classes/**/*'
  s.dependency 'Flutter'
  s.platform = :ios, '12.0'

  s.pod_target_xcconfig = { 
    'DEFINES_MODULE' => 'YES', 
    'EXCLUDED_ARCHS[sdk=iphonesimulator*]' => 'i386',
    'OTHER_LDFLAGS' => '-force_load ${BUILT_PRODUCTS_DIR}/liblightning_db_ffi.a'
  }
  s.swift_version = '5.0'

  s.script_phase = {
    :name => 'Build Rust library',
    :script => 'sh "$PODS_TARGET_SRCROOT/../cargokit/build_pod.sh" ../../lightning_db_ffi lightning_db_ffi',
    :execution_position => :before_compile,
    :output_files => ['${BUILT_PRODUCTS_DIR}/liblightning_db_ffi.a']
  }
end