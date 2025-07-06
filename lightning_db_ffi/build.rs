use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let target_dir = PathBuf::from(&crate_dir)
        .parent()
        .unwrap()
        .join("packages")
        .join("lightning_db_dart")
        .join("src");

    // Create the target directory if it doesn't exist
    std::fs::create_dir_all(&target_dir).unwrap();

    cbindgen::generate(&crate_dir)
        .expect("Unable to generate bindings")
        .write_to_file(target_dir.join("lightning_db_ffi.h"));
}