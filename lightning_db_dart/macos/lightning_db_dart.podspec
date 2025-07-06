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

  s.dependency 'FlutterMacOS'

  s.platform = :osx, '10.11'
  s.pod_target_xcconfig = { 
    'DEFINES_MODULE' => 'YES',
    'EXCLUDED_ARCHS[sdk=macosx*]' => 'i386',
    'OTHER_LDFLAGS' => '-force_load ${BUILT_PRODUCTS_DIR}/liblightning_db_ffi.a -framework CoreFoundation -framework Security -framework SystemConfiguration'
  }
  s.swift_version = '5.0'

  # For now, just link the pre-built library
  s.vendored_libraries = 'liblightning_db_ffi.a'
  
end