use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let output_dir = PathBuf::from(&crate_dir).join("include");

    // Create include directory if it doesn't exist
    std::fs::create_dir_all(&output_dir).expect("Failed to create include directory");

    // Generate C header with cbindgen (for legacy FFI)
    let config = cbindgen::Config::from_file("cbindgen.toml")
        .expect("Failed to read cbindgen.toml");

    cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
        .expect("Failed to generate C bindings")
        .write_to_file(output_dir.join("basis_rs.h"));

    // Build CXX bridge
    cxx_build::bridge("src/cxx_bridge.rs")
        .flag_if_supported("-std=c++17")
        .compile("basis_rs_cxx");

    println!("cargo:rerun-if-changed=src/ffi.rs");
    println!("cargo:rerun-if-changed=src/cxx_bridge.rs");
    println!("cargo:rerun-if-changed=cbindgen.toml");
}
