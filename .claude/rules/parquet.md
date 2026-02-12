---
paths:
  - "src/lib.rs"
  - "src/parquet.rs"
  - "src/cxx_bridge.rs"
  - "include/basis_rs/parquet/parquet.hpp"
  - "include/basis_rs/parquet/detail/*.hpp"
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
| `include/basis_rs/parquet/parquet.hpp` | Main C++ API header (user-facing: DataFrame, ParquetFile, ParquetWriter) |
| `include/basis_rs/parquet/detail/column_accessor.hpp` | ColumnAccessor, ColumnIterator, ColumnChunkView |
| `include/basis_rs/parquet/detail/codec.hpp` | ParquetCodec for struct-column mapping |
| `include/basis_rs/parquet/detail/query.hpp` | ParquetQuery builder |
| `include/basis_rs/parquet/detail/cell_codec.hpp` | ParquetCellCodec FFI type specializations |
| `include/basis_rs/parquet/detail/type_traits.hpp` | ParquetTypeOf type traits |
| `cpp/tests/parquet_test.cpp` | Unit tests (27 test cases) |
| `cpp/tests/parquet_benchmark.cpp` | Performance benchmark |

## DataFrame API (Zero-Copy)

```cpp
#include <basis_rs/parquet/parquet.hpp>

// Open file with column projection
basis_rs::DataFrame df("data.parquet", {"StockId", "Close", "High", "Low"});

// Zero-copy column access
auto close = df.GetColumn<float>("Close");

// Simple range-for loop (recommended)
double sum = 0;
for (float value : close) {
    sum += value;
}

// Index-based access (O(log n) chunk lookup)
for (size_t i = 0; i < close.size(); ++i) {
    process(close[i]);
}

// Chunk-aware iteration (advanced, best cache locality)
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
- **`ColumnAccessor<T>`**: Zero-copy access to column data with seamless cross-chunk iteration
  - `begin()/end()`: Forward iterator for range-for loops
  - `operator[]`: O(log n) random access via binary search
  - `at()`: Bounds-checked access
  - `NumChunks()/Chunk(i)`: Advanced chunk-aware access
- **`ColumnIterator<T>`**: Forward iterator that traverses across multiple chunks seamlessly
- **`ColumnChunkView<T>`**: View into a contiguous memory chunk

### Important Notes

- Column pointers are only valid while `DataFrame` is alive
- Range-for loop is recommended for most use cases (simple and efficient)
- Use chunk-aware iteration (`NumChunks()/Chunk()`) for maximum cache locality with multiple columns
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

### Header Structure

```
include/basis_rs/parquet/
├── parquet.hpp              # Main public header (DataFrame, ParquetFile, ParquetWriter)
└── detail/                  # Implementation details
    ├── column_accessor.hpp  # ColumnAccessor, ColumnIterator, ColumnChunkView
    ├── codec.hpp            # ParquetCodec
    ├── query.hpp            # ParquetQuery
    ├── cell_codec.hpp       # ParquetCellCodec specializations
    └── type_traits.hpp      # ParquetTypeOf
```

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
    -> For each column: GetColumn<T>() -> iterate via range-for -> copy to struct fields
```

## Type Mapping

| C++ | CXX | Rust/Polars | Zero-copy |
|-----|-----|-------------|-----------|
| `int32_t` | `i32` | `Int32` | Yes |
| `int64_t` | `i64` | `Int64` | Yes |
| `uint64_t` | `u64` | `UInt64` | Yes |
| `float` | `f32` | `Float32` | Yes |
| `double` | `f64` | `Float64` | Yes |
| `std::string` | `String` | `String` | No (allocation) |
| `bool` | `bool` | `Boolean` | No (bit-packed) |
| DateTime | `i64` (ms) | `Datetime` | Yes |

## Adding a New Column Type

1. Add `parquet_df_get_<type>_chunks()` in `cxx_bridge.rs`
2. Add `DataFrame::GetColumn<Type>()` specialization in `parquet.hpp`
3. Add `ParquetCellCodec<Type>` in `detail/cell_codec.hpp` for legacy API support
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
