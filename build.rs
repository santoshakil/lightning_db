// use std::env;
// use std::path::PathBuf;

fn main() {
    // Generate protobuf code if protoc is available
    if let Err(e) = prost_build::compile_protos(&["proto/database.proto"], &["proto/"]) {
        println!("cargo:warning=Failed to compile protobuf files: {e}. Protobuf support will be disabled.");
        println!("cargo:warning=To enable protobuf support, install protoc (protobuf compiler).");
    }

    // C headers are generated in the lightning-db-ffi subdirectory via cbindgen
    // FFI implementation is complete in lightning-db-ffi/
    /*
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let output_file = PathBuf::from(&crate_dir)
        .join("include")
        .join("lightning_db.h");

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_language(cbindgen::Language::C)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(output_file);
    */
}
