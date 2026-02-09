fn main() {
    // Build CXX bridge
    cxx_build::bridge("src/cxx_bridge.rs")
        .flag_if_supported("-std=c++17")
        .compile("basis_rs_cxx");

    println!("cargo:rerun-if-changed=src/cxx_bridge.rs");
}
