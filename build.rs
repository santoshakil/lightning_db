fn main() {
    // Generate protobuf code if protoc is available
    if let Err(e) = prost_build::compile_protos(&["proto/database.proto"], &["proto/"]) {
        println!("cargo:warning=Failed to compile protobuf files: {e}. Protobuf support will be disabled.");
        println!("cargo:warning=To enable protobuf support, install protoc (protobuf compiler).");
    }

    // Generate build-time environment variables
    generate_build_info();
}

fn generate_build_info() {
    use std::process::Command;

    // Get Rust compiler version
    let rustc_version = match Command::new("rustc").arg("--version").output() {
        Ok(output) => String::from_utf8_lossy(&output.stdout).trim().to_string(),
        Err(_) => "unknown".to_string(),
    };
    println!("cargo:rustc-env=RUSTC_VERSION={rustc_version}");

    // Get build time
    let build_time = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string();
    println!("cargo:rustc-env=BUILD_TIME={build_time}");

    // Get git commit hash
    let git_commit = match Command::new("git").args(["rev-parse", "HEAD"]).output() {
        Ok(output) => {
            let hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if hash.is_empty() {
                "unknown".to_string()
            } else {
                hash
            }
        }
        Err(_) => "unknown".to_string(),
    };
    println!("cargo:rustc-env=GIT_COMMIT={git_commit}");
}
