use std::env;
use std::path::PathBuf;

fn main() {
    // Generate protobuf code
    prost_build::compile_protos(&["proto/database.proto"], &["proto/"]).unwrap();

    // Generate C headers
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
}
