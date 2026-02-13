# CLAUDE.md

## Project Overview

basis-rs is a Rust utility library with C++ bindings. Three-layer architecture: Rust core (Polars) → CXX bridge (FFI) → header-only C++ API. Consumed via Nix flake + CMake `find_package(basis-rs)`.

## Build & Test

```bash
# Enter dev shell (automatic with direnv, or manually)
nix develop

# Build Rust library (release mode, generates static lib + CXX bridge)
cargo build --release

# Run Rust tests
cargo test

# Check with clippy
cargo clippy

# Format code
cargo fmt

# Build C++ library and tests (requires Rust build first)
cmake -S . -B build -DBASIS_RS_BUILD_TESTS=ON && cmake --build build

# Run C++ tests
cd build && ctest --output-on-failure

# Build + test everything
cargo build --release && cmake -S . -B build -DBASIS_RS_BUILD_TESTS=ON && cmake --build build
cargo test && cd build && ctest --output-on-failure
```

## Architecture

- `src/` — Rust source
  - `lib.rs` — crate entry, re-exports public API
  - `parquet.rs` — Parquet I/O with Polars (builder pattern)
  - `cxx_bridge.rs` — CXX bridge FFI layer
- `include/basis_rs/` — public C++ headers (organized by module)
  - `parquet/` — Parquet module headers
- `cpp/tests/` — C++ gtest suite
- `cmake/` — CMake package config + helpers (`Config.cmake.in`, `BuildHelpers.cmake`)
- `.claude/rules/` — per-module Claude documentation

## Modules

| Module | Rules file | Description |
|--------|-----------|-------------|
| parquet | `.claude/rules/parquet.md` | Parquet I/O with zero-copy DataFrame API, filtering with predicate pushdown, struct-based ReadAllAs/WriteRecord |

## Adding a New Module

1. Create `.claude/rules/<module>.md` with `paths:` frontmatter listing all related source files
2. Add an entry to the Modules table above
3. Document: key files, architecture, design patterns, type mappings, testing conventions
