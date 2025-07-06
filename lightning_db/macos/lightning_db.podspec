#
# To learn more about a Podspec see http://guides.cocoapods.org/syntax/podspec.html.
# Run `pod lib lint lightning_db.podspec` to validate before publishing.
#
Pod::Spec.new do |s|
  s.name             = 'lightning_db'
  s.version          = '0.1.0'
  s.summary          = 'High-performance embedded database for Flutter'
  s.description      = <<-DESC
High-performance embedded database for Dart and Flutter applications
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
    'OTHER_LDFLAGS' => '-force_load ${BUILT_PRODUCTS_DIR}/liblightning_db_ffi.a'
  }
  s.swift_version = '5.0'

  s.script_phase = {
    :name => 'Build Rust library',
    :script => 'sh "$PODS_TARGET_SRCROOT/../cargokit/build_pod.sh"',
    :execution_position => :before_compile,
    :output_files => ['${BUILT_PRODUCTS_DIR}/liblightning_db_ffi.a']
  }
end
