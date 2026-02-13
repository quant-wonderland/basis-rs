{
  description = "basis of rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    utils.url = "github:numtide/flake-utils";
    devshell.url = "github:numtide/devshell";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, utils, devshell, rust-overlay, ... }:
    let
      mkBasisRs = pkgs:
        let
          rustToolchain = pkgs.rust-bin.stable.latest.default;
          customRustPlatform = pkgs.makeRustPlatform {
            cargo = rustToolchain;
            rustc = rustToolchain;
          };
        in customRustPlatform.buildRustPackage {
        pname = "basis-rs";
        version = "0.1.0";

        src = pkgs.lib.cleanSource ./.;

        cargoLock = {
          lockFile = ./Cargo.lock;
        };

        nativeBuildInputs = with pkgs; [ cmake ];

        buildType = "release";
        doCheck = true;

        # After cargo build, run CMake to build the C++ bridge library
        postBuild = ''
          cmake -S . -B cmake-build \
            -DCMAKE_INSTALL_PREFIX=$out \
            -DCMAKE_BUILD_TYPE=Release \
            -DBASIS_RS_BUILD_TESTS=OFF \
            -DRUST_TARGET_TRIPLE=${pkgs.stdenv.hostPlatform.rust.rustcTarget}
          cmake --build cmake-build
        '';

        # Override default install (cargo install is for binaries, not libraries)
        installPhase = ''
          runHook preInstall
          cmake --install cmake-build
          runHook postInstall
        '';
      };
    in
    {
      overlays.default = nixpkgs.lib.composeManyExtensions [
        rust-overlay.overlays.default
        (final: prev: {
          basis-rs = mkBasisRs final;
        })
      ];
    } // utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (system:
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

      in
      {
        packages = {
          default = mkBasisRs pkgs;
        };

        devShells.default = pkgs.devshell.mkShell {
          name = "basis-rs-dev";

          devshell.startup.cargo-build = {
            text = ''
              echo "Synchronizing Rust build..."
              cargo build --release 2>&1 | tail -1
            '';
          };

          packages = [
            rust-toolchain
            pkgs.pkg-config
            pkgs.cargo-audit
            pkgs.cargo-outdated
            pkgs.cargo-edit
            pkgs.cmake
            pkgs.gtest
            pkgs.gcc
            pkgs.abseil-cpp
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
              help = "Build Rust library (release)";
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
              help = "Build C++ library and tests";
              command = "cargo build --release && cmake -S . -B build -DBASIS_RS_BUILD_TESTS=ON && cmake --build build";
            }
            {
              name = "cpp-test";
              help = "Run C++ tests";
              command = "cd build && ctest --output-on-failure";
            }
            {
              name = "build-all";
              help = "Build Rust + C++ (release)";
              command = "cargo build --release && cmake -S . -B build -DBASIS_RS_BUILD_TESTS=ON && cmake --build build";
            }
            {
              name = "test-all";
              help = "Run all tests (Rust and C++)";
              command = "cargo test && cd build && ctest --output-on-failure";
            }
          ];
        };
      }
    );
}
