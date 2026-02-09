# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

basis-rs is a Rust utility library providing data processing modules with C++ bindings. Rust implements the core logic; CXX bridge exposes it to C++; a header-only C++ layer provides the user-facing API. Consumed by other projects via Nix flake + CMake `find_package(basis-rs)`.

## Development Environment

```bash
# Enter dev shell (automatic with direnv, or manually)
# Runs `cargo build --release` on entry to keep Rust artifacts in sync
nix develop

# Build Rust library (release mode, generates static lib + CXX bridge)
cargo build --release

# Run Rust tests
cargo test

# Run a single test
cargo test test_roundtrip

# Check with clippy
cargo clippy

# Format code
cargo fmt

# Build C++ library and tests (requires Rust to be built first)
cmake -S . -B build -DBASIS_RS_BUILD_TESTS=ON && cmake --build build

# Run C++ tests
cd build && ctest --output-on-failure

# Build everything (Rust + C++)
cargo build --release && cmake -S . -B build -DBASIS_RS_BUILD_TESTS=ON && cmake --build build

# Run all tests
cargo test && cd build && ctest --output-on-failure

# Install to a prefix (for verification)
cmake --install build --prefix /tmp/basis-rs-install
```

## Architecture

- `src/` - Rust source (standard layout)
  - `lib.rs` - Crate entry point, re-exports public API
  - `parquet.rs` - Parquet file I/O using Polars with builder pattern API
  - `cxx_bridge.rs` - CXX bridge for type-safe Rust-C++ interop
- `cpp/` - C++ wrapper layer and tests
  - `basis_parquet.hpp` - Type-safe C++ wrapper with codec-based ParquetFile API
  - `CMakeLists.txt` - CMake build configuration
  - `tests/parquet_codec_test.cpp` - gtest unit tests for CXX bridge

## Consumer Usage

### Nix flake integration

```nix
# In consuming project's flake.nix
inputs.basis-rs.url = "github:user/basis-rs";
# Then in buildInputs:
buildInputs = [ basis-rs.packages.${system}.default ];
```

### CMake

```cmake
find_package(basis-rs REQUIRED)
target_link_libraries(my_target basis_rs::parquet)
```

### C++ code

```cpp
#include <basis_rs/parquet.hpp>

struct MyData {
    int64_t id;
    std::string name;
    double score;
};

template <>
inline const basis_rs::ParquetCodec<MyData>& basis_rs::GetParquetCodec() {
    static basis_rs::ParquetCodec<MyData> codec = []() {
        basis_rs::ParquetCodec<MyData> c;
        c.Add("id", &MyData::id);
        c.Add("name", &MyData::name);
        c.Add("score", &MyData::score);
        return c;
    }();
    return codec;
}

// Read
basis_rs::ParquetFile file("data.parquet");
auto records = file.ReadAll<MyData>();

// Write
auto writer = file.SpawnWriter<MyData>();
writer.WriteRecord({1, "alice", 95.0});
```

## Key Dependencies

### Rust
- `polars` (with `parquet`, `lazy` features) - DataFrame operations and Parquet I/O
- `thiserror` - Error type definitions
- `cxx` - Type-safe Rust-C++ FFI bridge
- `cxx-build` (build) - CXX bridge code generation

### C++ (test only)
- GoogleTest (via `find_package(GTest)`) - Unit testing
