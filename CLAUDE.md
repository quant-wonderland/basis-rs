# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

basis-rs is a Rust utility library providing data processing modules with C++ FFI bindings. Uses Nix flakes for development environment management.

## Development Environment

```bash
# Enter dev shell (automatic with direnv, or manually)
nix develop

# Build Rust library (release mode, generates static lib)
cargo build --release

# Run Rust tests
cargo test

# Run a single test
cargo test test_roundtrip

# Check with clippy
cargo clippy

# Format code
cargo fmt

# Build C++ tests (requires Rust library to be built first)
mkdir -p cpp/build && cd cpp/build && cmake .. && make

# Run C++ tests
cd cpp/build && ctest --output-on-failure

# Build everything (Rust + C++)
cargo build --release && mkdir -p cpp/build && cd cpp/build && cmake .. && make

# Run all tests
cargo test && cd cpp/build && ctest --output-on-failure
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

## C++ Usage

```cpp
#include "basis_parquet.hpp"

struct MyData {
    int64_t id;
    std::string name;
    double score;
};

template <>
inline const basis::ParquetCodec<MyData>& basis::GetParquetCodec() {
    static basis::ParquetCodec<MyData> codec = []() {
        basis::ParquetCodec<MyData> c;
        c.Add("id", &MyData::id);
        c.Add("name", &MyData::name);
        c.Add("score", &MyData::score);
        return c;
    }();
    return codec;
}

// Read parquet file
basis::ParquetFile file("data.parquet");
auto records = file.ReadAll<MyData>();

// Write parquet file
basis::ParquetWriter<MyData> writer("output.parquet");
writer.WriteRecord(entry);
```

## Key Dependencies

### Rust
- `polars` (with `parquet`, `lazy` features) - DataFrame operations and Parquet I/O
- `thiserror` - Error type definitions
- `cxx` - Type-safe Rust-C++ FFI bridge
- `cxx-build` (build) - CXX bridge code generation

### C++
- GoogleTest (fetched by CMake) - Unit testing
