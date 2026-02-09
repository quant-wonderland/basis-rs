{
  description = "basis of rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    utils.url = "github:numtide/flake-utils";
    devshell.url = "github:numtide/devshell";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, utils, devshell, rust-overlay, ... }@inputs:
    utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            rust-overlay.overlays.default
            devshell.overlays.default
          ];
        };
        
        rust-toolchain = (pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default)).override {
          extensions = [ "rust-src" "rust-analyzer" ];
        };
        
        # 构建Rust库
        rust-package = pkgs.rustPlatform.buildRustPackage {
          pname = "basis-rs";
          version = "0.1.0";
          
          src = ./.;
          
          cargoHash = "sha256-0000000000000000000000000000000000000000000000000000"; # 首次构建时替换
          # 或使用 cargoSha256 = "";  # 对于旧版本nixpkgs
          
          nativeBuildInputs = with pkgs; [
            pkg-config
          ];
          
          buildInputs = with pkgs; [
            # 添加你需要的系统依赖
            # openssl
          ];
          
          # 构建类型
          buildType = "release";
          
          # 测试设置
          doCheck = true;
          checkType = "release";
        };
        
      in
      {
        # 开发环境
        devShells.default = pkgs.devshell.mkShell {
          name = "basis-rs-dev";
          
          packages = [
            rust-toolchain
            # rustfmt and clippy are included in rust-toolchain
            pkgs.pkg-config
            pkgs.cargo-audit
            pkgs.cargo-outdated
            pkgs.cargo-edit
            # C++ development tools
            pkgs.cmake
            pkgs.gtest
            pkgs.gcc
          ];

          env = [
            {
              name = "RUST_SRC_PATH";
              value = "${rust-toolchain}/lib/rustlib/src/rust/library";
            }
            {
              name = "RUST_BACKTRACE";
              value = "full";
            }
            {
              name = "CARGO_TERM_COLOR";
              value = "always";
            }
          ];

          commands = [
            {
              name = "build";
              help = "Build Rust library";
              command = "cargo build --release";
            }
            {
              name = "test";
              help = "Run Rust tests";
              command = "cargo test";
            }
            {
              name = "check";
              help = "Run clippy checks";
              command = "cargo clippy";
            }
            {
              name = "fmt";
              help = "Format code";
              command = "cargo fmt";
            }
            {
              name = "doc";
              help = "Generate documentation";
              command = "cargo doc --open";
            }
            {
              name = "cpp-build";
              help = "Build C++ tests";
              command = "mkdir -p cpp/build && cd cpp/build && cmake .. && make";
            }
            {
              name = "cpp-test";
              help = "Run C++ tests";
              command = "cd cpp/build && ctest --output-on-failure";
            }
            {
              name = "build-all";
              help = "Build Rust and C++ (release)";
              command = "cargo build --release && mkdir -p cpp/build && cd cpp/build && cmake .. && make";
            }
            {
              name = "test-all";
              help = "Run all tests (Rust and C++)";
              command = "cargo test && cd cpp/build && ctest --output-on-failure";
            }
          ];
        };

        # 构建包
        packages = {
          default = rust-package;
          rustPackage = rust-package;
        };

        # 默认应用（如果需要可执行文件）
        apps.default = {
          type = "app";
          program = "${rust-package}/bin/${rust-package.pname}";
        };
      }
    );
}