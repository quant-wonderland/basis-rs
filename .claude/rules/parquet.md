---
paths:
  - "src/lib.rs"
  - "src/parquet.rs"
  - "src/cxx_bridge.rs"
  - "include/basis_rs/parquet/*.hpp"
  - "cpp/tests/parquet_test.cpp"
  - "cpp/tests/parquet_benchmark.cpp"
---

# Parquet Module

Parquet file I/O using Polars, exposed to C++ via CXX bridge. The module provides two APIs:

1. **DataFrame API** (recommended): Zero-copy column access via `DataFrame` class (~20x faster)
2. **Query Builder API**: Struct-based reads with filter/select via `ParquetFile` and `ParquetQuery`

## Performance

On a 637MB parquet file with 20M rows and 49 columns:

| Operation | Time | Notes |
|-----------|------|-------|
| Open (4 columns projected) | 54ms | Projection pushdown |
| Zero-copy column access | 0.003ms | Just pointer retrieval |
| Column iteration (sum) | 47ms | Iterate 20M floats |
| Row iteration (chunk-wise) | 67ms | Process rows via chunks |
| ReadAllAs<T> | 503ms | Convert to struct vector |
| Query builder | ~1100ms | ParquetFile.Read().Collect() |

**Speedup: ~20x** for column-oriented workloads via DataFrame zero-copy API.

## Key Files

| File | Role |
|------|------|
| `src/lib.rs` | Crate entry point: re-exports `ParquetError`, `ParquetReader`, `ParquetWriter` |
| `src/parquet.rs` | Rust public API: builder pattern readers/writers |
| `src/cxx_bridge.rs` | CXX FFI layer: `ParquetDataFrame` (zero-copy), `ParquetReader`, `ParquetQuery` |
| `include/basis_rs/parquet/parquet.hpp` | Main C++ API header |
| `cpp/tests/parquet_test.cpp` | Unit tests (27 test cases) |
| `cpp/tests/parquet_benchmark.cpp` | Performance benchmark |

## DataFrame API (Zero-Copy)

```cpp
#include <basis_rs/parquet/parquet.hpp>

// Open file with column projection
basis_rs::DataFrame df("data.parquet", {"StockId", "Close", "High", "Low"});

// Zero-copy column access
auto close = df.GetColumn<float>("Close");

// Column-wise iteration (fastest)
double sum = 0;
for (const auto& chunk : close) {
    for (size_t i = 0; i < chunk.size(); ++i) {
        sum += chunk[i];
    }
}

// Row-wise iteration via chunks (recommended)
auto high = df.GetColumn<float>("High");
auto low = df.GetColumn<float>("Low");
for (size_t c = 0; c < high.NumChunks(); ++c) {
    const auto& h = high.Chunk(c);
    const auto& l = low.Chunk(c);
    for (size_t i = 0; i < h.size(); ++i) {
        float range = h[i] - l[i];
        // process...
    }
}

// Or convert to structs (slower but convenient)
auto records = df.ReadAllAs<TickData>();
```

### Key Classes

- **`DataFrame`**: Owns the Rust DataFrame, provides `GetColumn<T>()` and `ReadAllAs<T>()`
- **`ColumnAccessor<T>`**: Zero-copy access to column data (may have multiple chunks)
- **`ColumnChunkView<T>`**: View into a contiguous memory chunk

### Important Notes

- Column pointers are only valid while `DataFrame` is alive
- Use chunk iteration for best performance (avoids cross-chunk index lookups)
- String columns still require allocation (use `GetStringColumn()`)
- DateTime columns are stored as int64_t milliseconds (use `GetDateTimeColumn()`)

## Query Builder API (Struct-based)

```cpp
// Define struct and codec
struct TickData {
    int32_t stock_id;
    float close;
};

template <>
inline const basis_rs::ParquetCodec<TickData>& basis_rs::GetParquetCodec() {
    static basis_rs::ParquetCodec<TickData> codec = []() {
        basis_rs::ParquetCodec<TickData> c;
        c.Add("StockId", &TickData::stock_id);
        c.Add("Close", &TickData::close);
        return c;
    }();
    return codec;
}

// Read all records via query builder
basis_rs::ParquetFile file("data.parquet");
auto records = file.Read<TickData>().Collect();

// Query with filter
auto filtered = file.Read<TickData>()
    .Filter(&TickData::close, basis_rs::Gt, 10.0f)
    .Collect();

// Write records
auto writer = file.SpawnWriter<TickData>();
writer.WriteRecord({123, 45.6f});
writer.Finish();
```

## Architecture

### Zero-Copy Data Flow
```
C++ DataFrame
  -> parquet_open_projected() FFI
    -> Rust Polars reads parquet
      -> Returns Box<ParquetDataFrame> (opaque, holds DataFrame)
        -> C++ GetColumn<T>() -> parquet_df_get_*_chunks() FFI
          -> Returns raw pointers to Polars internal buffers
            -> C++ iterates directly on Rust memory
```

### Struct Conversion Flow
```
C++ ReadAllAs<T>()
  -> ParquetCodec<T>::ReadAllFromDf()
    -> For each column: GetColumn<T>() -> iterate chunks -> copy to struct fields
```

## Type Mapping

| C++ | CXX | Rust/Polars | Zero-copy |
|-----|-----|-------------|-----------|
| `int32_t` | `i32` | `Int32` | ✓ |
| `int64_t` | `i64` | `Int64` | ✓ |
| `uint64_t` | `u64` | `UInt64` | ✓ |
| `float` | `f32` | `Float32` | ✓ |
| `double` | `f64` | `Float64` | ✓ |
| `std::string` | `String` | `String` | ✗ (allocation) |
| `bool` | `bool` | `Boolean` | ✗ (bit-packed) |
| DateTime | `i64` (ms) | `Datetime` | ✓ |

## Adding a New Column Type

1. Add `parquet_df_get_<type>_chunks()` in `cxx_bridge.rs`
2. Add `DataFrame::GetColumn<Type>()` specialization in `parquet.hpp`
3. Add `ParquetCellCodec<Type>` for legacy API support
4. Add tests

## Testing

```bash
# Run all tests
cargo test && cd build && ctest --output-on-failure

# Run benchmark
./build/cpp/tests/parquet_benchmark
```

## Dependencies

- `polars 0.46` with `parquet` + `lazy` features
- `cxx 1.0` / `cxx-build 1.0`
- `thiserror 2.0`
- GoogleTest (C++ tests)
