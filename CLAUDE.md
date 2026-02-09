# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

basis-rs is a Rust utility library providing data processing modules with C++ FFI bindings. Uses Nix flakes for development environment management.

## Development Environment

```bash
# Enter dev shell (automatic with direnv, or manually)
nix develop

# Build Rust library (release mode, generates static lib and C header)
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

- `src/lib.rs` - Crate entry point, re-exports from basis modules
- `src/ffi.rs` - C FFI exports for cross-language usage
- `basis/` - Core modules (uses `#[path]` attribute for non-standard location)
  - `parquet/` - Parquet file I/O using Polars with builder pattern API
- `include/basis_rs.h` - Generated C header (by cbindgen)
- `cpp/` - C++ bindings and tests
  - `basis_rs.hpp` - C++ wrapper with RAII and exception handling
  - `CMakeLists.txt` - CMake build configuration
  - `tests/parquet_test.cpp` - gtest unit tests

## C++ Usage

```cpp
#include "basis_rs.hpp"

// Read parquet file
auto df = basis::DataFrame::read_parquet("data.parquet");
auto ids = df.get_int64_column("id");
auto names = df.get_string_column("name");

// Create and write parquet
basis::DataFrame output;
output.add_column("id", std::vector<int64_t>{1, 2, 3});
output.add_column("name", std::vector<std::string>{"a", "b", "c"});
output.write_parquet("output.parquet");
```

## Key Dependencies

### Rust
- `polars` (with `parquet`, `lazy` features) - DataFrame operations and Parquet I/O
- `thiserror` - Error type definitions
- `libc` - C types for FFI
- `cbindgen` (build) - C header generation

### C++
- GoogleTest (fetched by CMake) - Unit testing
