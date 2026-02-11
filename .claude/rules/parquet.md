---
paths:
  - "src/lib.rs"
  - "src/parquet.rs"
  - "src/cxx_bridge.rs"
  - "include/basis_rs/parquet.hpp"
  - "cpp/tests/parquet_codec_test.cpp"
  - "examples/bench_read.rs"
---

# Parquet Module

Parquet file I/O using Polars, exposed to C++ via CXX bridge with a type-safe codec-based API. Supports column projection, query builder with Select/Filter, and lazy evaluation with predicate pushdown.

## Key Files

| File | Role |
|------|------|
| `src/lib.rs` | Crate entry point: re-exports `ParquetError`, `ParquetReader`, `ParquetWriter`; declares `parquet` and `cxx_bridge` modules |
| `src/parquet.rs` | Rust public API: `ParquetReader` (builder: `with_columns`, `read`, `scan`), `ParquetWriter` (builder: compression, row group size), `ParquetError` enum |
| `src/cxx_bridge.rs` | CXX FFI layer: `#[cxx::bridge(namespace = "basis_rs::ffi")]`, opaque types (`ParquetReader`, `ParquetWriter`, `ParquetQuery`), shared enums (`FilterOp`, `ColumnType`), ~35 FFI functions |
| `include/basis_rs/parquet.hpp` | C++ header-only API: `ParquetCodec<T>` (type-erased column codec), `ParquetFile` (entry point), `ParquetQuery<T>` (query builder), `ParquetWriter<T>` (buffered writer) |
| `cpp/tests/parquet_codec_test.cpp` | 18 gtest tests: roundtrip, numeric types, bool, strings, unicode, large datasets, projection pushdown, query builder (select/filter/combined) |
| `examples/bench_read.rs` | CLI benchmark tool: `cargo run --release --example bench_read -- <file> [--columns col1,col2] [--filter "col>val"] [--runs N]` — measures full read, projected read, and lazy filtered read performance |

## Architecture / Data Flow

```
C++ user code
  -> ParquetFile::ReadAll<T>() / Read<T>().Select().Filter().Collect()
    -> ParquetCodec<T> builds column name list from registered struct members
      -> FFI: parquet_reader_open_projected() / parquet_query_*()
        -> Rust: Polars ParquetReader::with_columns() / LazyFrame::scan_parquet()
          -> Returns DataFrame -> FFI extracts typed columns -> C++ codec fills struct fields
```

Write path:
```
C++ ParquetWriter<T>::WriteRecord(record)
  -> ParquetCodec<T>::WriteAll() serializes to per-column vectors
    -> FFI: parquet_writer_add_*_column() for each column
      -> Rust: ParquetWriter builds DataFrame from columns -> writes Parquet file
```

## Design Patterns

- **Builder pattern**: Rust `ParquetReader` and C++ `ParquetQuery<T>` both use chainable builders
- **Type-erased codec**: `ParquetCodec<T>` stores `std::function` readers/writers per column, registered via `Add("name", &T::member)`
- **Member pointer offsets**: `FindColumnName()` matches member pointers by byte offset (computed in `Add()` via `reinterpret_cast` on a stack-allocated dummy instance)
- **Query filter type erasure**: `FilterEntry::apply` is `std::function<void(ffi::ParquetQuery&)>` — lambda captures typed value and dispatches to the correct `parquet_query_filter_*` FFI function
- **Automatic column projection**: `ReadAll<T>()` only reads the columns registered in the codec, not the full file

## CXX Bridge Gotchas

- **Shared enums**: `ColumnType` and `FilterOp` are CXX shared enums. In generated Rust code they become structs with associated constants, NOT real enums. Use `==` comparisons (not `match`) in Rust
- **`Vec<String>` across FFI**: Works as a parameter — maps to `rust::Vec<rust::String>` on the C++ side
- **Error handling**: FFI functions return `Result<T, String>`. CXX converts `Err(String)` into C++ exceptions (`rust::Error`)
- **Opaque types**: `Box<ParquetReader>` etc. are opaque on the C++ side — accessed only through FFI functions
- **Null handling**: Null values in Parquet columns are converted to default values (0, 0.0, false, "") when reading. Use `Option<T>` in Rust if null preservation is needed

## Type Mapping

| C++ | CXX bridge | Rust/Polars |
|-----|-----------|-------------|
| `int32_t` | `i32` | `Int32` |
| `int64_t` | `i64` | `Int64` |
| `float` | `f32` | `Float32` |
| `double` | `f64` | `Float64` |
| `std::string` | `String` | `String` |
| `bool` | `bool` | `Boolean` |

## Adding a New Column Type

1. Add `ParquetCellCodec<NewType>` specialization in `parquet.hpp` (reader + writer lambdas)
2. Add `parquet_reader_get_<type>_column()` + `parquet_writer_add_<type>_column()` in `cxx_bridge.rs`
3. Add `parquet_query_filter_<type>()` in `cxx_bridge.rs` (for query builder support)
4. Add roundtrip test in `parquet_codec_test.cpp`

## Testing

- **Fixture**: `ParquetCodecTest` (gtest) — creates temp directory in `SetUp()`, removes in `TearDown()`
- **Pattern**: write records -> read back -> assert field values match
- **Test structs**: `SimpleEntry` (id/name/score), `NumericEntry` (i32/i64/f32/f64), `BoolEntry`, `PartialEntry` (subset for projection tests)
- **Rust tests**: `src/parquet.rs` `#[cfg(test)] mod tests` — uses `tempfile` crate, 5 tests + 2 doc-tests
- **Run**: `cargo test && cd build && ctest --output-on-failure`

## Dependencies

- `polars 0.46` with `parquet` + `lazy` features (Rust)
- `cxx 1.0` / `cxx-build 1.0` for FFI bridge generation
- `thiserror 2.0` for Rust error types
- GoogleTest for C++ tests (via CMake `find_package(GTest)`)
