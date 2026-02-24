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

Parquet file I/O using Polars, exposed to C++ via CXX bridge. The module provides:

1. **DataFrame API**: Zero-copy column access with optional filtering and column selection
2. **ParquetWriter API**: Struct-based writes to Parquet files

## Performance

On a 637MB parquet file with 20M rows and 49 columns:

| Operation | Time | Notes |
|-----------|------|-------|
| Open (4 columns projected) | 113ms | Projection pushdown |
| Zero-copy column access | 0.004ms | Just pointer retrieval |
| Column iteration (sum) | 102ms | Iterate 20M floats (range-for, pointer-based iterator) |
| Row iteration (chunk-wise) | 70ms | Process rows via chunks |
| ReadAllAs<T> | 689ms | Convert to struct vector (chunk-wise access) |
| Filter query (Select+Filter+Collect) | 158ms | Predicate pushdown via Polars lazy scan |

**Speedup: ~20x** for column-oriented workloads via DataFrame zero-copy API.

### Filter Query (DataFrameBuilder / query.hpp)

`DataFrameBuilder` is a thin C++ builder that dispatches to Rust FFI → Polars `LazyFrame::scan_parquet` with predicate/projection pushdown. Pure Rust benchmark confirms zero FFI overhead:

| Path | Time | Rows |
|------|------|------|
| Pure Rust: eager read (projected) | 111ms | 20M |
| Pure Rust: lazy scan (select+filter) | 165ms | 12M |
| C++ FFI: DataFrameBuilder (select+filter) | 166ms | 12M |

- FFI overhead: ~1ms (negligible) — C++ query performance equals pure Polars
- The ~54ms filter cost is Polars evaluating `Close > 10.0f` across 20M rows during I/O
- Polars optimizer auto-reorders select/filter, so call order doesn't matter

### Sorted Column vs Unsorted Column Filter

Data is stored sorted by StockId (ascending). Polars leverages row group min/max statistics to skip irrelevant row groups:

| Filter | Time | Rows | Notes |
|--------|------|------|-------|
| No filter (eager projected) | 116ms | 20M | Baseline |
| StockId == 1 (sorted, head) | 67ms | 4.8K | Row group pruning skips most I/O |
| StockId == 600519 (sorted, tail) | 97ms | 5K | Scans more metadata before pruning |
| StockId > 600000 (sorted, ~45%) | 157ms | 9.3M | Partial row group skip |
| Close > 100.0f (unsorted, ~1.5%) | 112ms | 0.3M | Row group stats less effective |
| Close > 10.0f (unsorted, ~58%) | 184ms | 11.9M | Full scan, large result set |

- Sorted column eq filter (67-97ms) is faster than no-filter baseline (116ms) — row group pruning skips I/O entirely
- Head vs tail ~30ms gap: sequential file read + more metadata to scan before reaching tail row groups
- Unsorted column with small selectivity (Close > 100.0f, 112ms) still benefits from row group min/max stats but less effectively than sorted columns
- Performance is dominated by: (1) row groups read from disk, (2) result set memory allocation

### Write Tuning: row_group_size Impact on Filter Read

Synthetic dataset: 2M rows, 4 columns (StockId int32, Close/High/Low float), sorted by StockId ascending (1..=1000, ~2000 rows each). Filter: `StockId == 500` (returns 2000 rows). Note: absolute times are much smaller than the production benchmark above due to the smaller dataset (~1MB vs 637MB).

| row_group_size | File Size | Read Time | Notes |
|----------------|-----------|-----------|-------|
| 1K | 18MB | 28.6ms | Metadata bloat (17x), too many row groups to parse |
| 10K | 6MB | 2.8ms | Good pruning, slight file overhead |
| 100K | 1.3MB | 2.4ms | Good balance |
| 500K | 1.1MB | 1.2ms | Optimal for this dataset |
| 1M (default) | 1.1MB | 3.2ms | Fewer groups to skip |
| 2M (single) | 1.1MB | 6.0ms | No pruning possible, full scan |

- For sorted-column eq filter: row_group_size 10K-500K is the sweet spot
- Too small (<1K): metadata overhead dominates (file bloat + parse cost)
- Too large (single group): loses row group pruning entirely
- Recommendation: `with_row_group_size(100_000)` to `with_row_group_size(500_000)` for sorted filter workloads

## Key Files

| File | Role |
|------|------|
| `src/lib.rs` | Crate entry point: re-exports `ParquetError`, `ParquetReader`, `ParquetWriter` |
| `src/parquet.rs` | Rust public API: builder pattern readers/writers |
| `src/cxx_bridge.rs` | CXX FFI layer: `ParquetDataFrame` (zero-copy), `ParquetQuery`, `ParquetWriter` |
| `include/basis_rs/parquet/parquet.hpp` | Main C++ API header (DataFrame, DataFrameBuilder, ParquetWriter) |
| `include/basis_rs/parquet/detail/column_accessor.hpp` | ColumnAccessor, ColumnIterator, ColumnChunkView |
| `include/basis_rs/parquet/detail/codec.hpp` | ParquetCodec for struct-column mapping |
| `include/basis_rs/parquet/detail/query.hpp` | DataFrameBuilder implementation |
| `include/basis_rs/parquet/detail/cell_codec.hpp` | ParquetCellCodec FFI type specializations |
| `include/basis_rs/parquet/detail/type_traits.hpp` | ParquetTypeOf type traits, AbseilCivilTime concept |
| `cpp/tests/parquet_test.cpp` | Unit tests |
| `cpp/tests/parquet_benchmark.cpp` | Performance benchmark |

## DataFrame API (Zero-Copy)

```cpp
#include <basis_rs/parquet/parquet.hpp>

// Simple open (all columns)
basis_rs::DataFrame df("data.parquet");

// With column projection
basis_rs::DataFrame df("data.parquet", {"StockId", "Close", "High", "Low"});

// Builder pattern with filtering (predicate pushdown)
auto df = basis_rs::DataFrame::Open("data.parquet")
              .Select({"StockId", "Close", "High", "Low"})
              .Filter("Close", basis_rs::Gt, 10.0f)
              .Collect();

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

- **`DataFrame`**: Owns the Rust DataFrame, provides `GetColumn<T>()`, `ReadAllAs<T>()`, and static `Open()` builder
- **`DataFrameBuilder`**: Builder for DataFrame with `Select()`, `Filter()`, and `Collect()` methods
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

## ParquetWriter API

```cpp
// Define struct and codec
struct TickData {
    int32_t stock_id;
    float close;
    absl::CivilDay date;  // AbseilCivilTime types supported
};

template <>
inline const basis_rs::ParquetCodec<TickData>& basis_rs::GetParquetCodec() {
    static basis_rs::ParquetCodec<TickData> codec = []() {
        basis_rs::ParquetCodec<TickData> c;
        c.Add("StockId", &TickData::stock_id);
        c.Add("Close", &TickData::close);
        c.Add("Date", &TickData::date);
        return c;
    }();
    return codec;
}

// Write records
basis_rs::ParquetWriter<TickData> writer("output.parquet");
writer.WriteRecord({123, 45.6f, absl::CivilDay(2024, 1, 15)});
writer.WriteRecords(records_vector);
writer.Finish();  // Or let destructor auto-finish
```

### AbseilCivilTime Support

The codec supports absl civil time types (`absl::CivilDay`, `absl::CivilSecond`, `absl::CivilMinute`, `absl::CivilHour`). These are stored as int64 milliseconds since epoch in Parquet, converted using Asia/Shanghai timezone.

```cpp
struct DateEntry {
    int64_t id;
    absl::CivilDay date;
};

// Reading: DateTime columns auto-convert to CivilDay
auto entries = df.ReadAllAs<DateEntry>();

// Writing: CivilDay auto-converts to DateTime (milliseconds)
writer.WriteRecord({1, absl::CivilDay(2024, 6, 30)});
```

## Architecture

### Header Structure

```
include/basis_rs/parquet/
├── parquet.hpp              # Main public header (DataFrame, DataFrameBuilder, ParquetWriter)
└── detail/                  # Implementation details
    ├── column_accessor.hpp  # ColumnAccessor, ColumnIterator, ColumnChunkView
    ├── codec.hpp            # ParquetCodec
    ├── query.hpp            # DataFrameBuilder
    ├── cell_codec.hpp       # ParquetCellCodec specializations
    └── type_traits.hpp      # ParquetTypeOf
```

### Zero-Copy Data Flow
```
C++ DataFrame
  -> parquet_open() or parquet_open_projected() FFI
    -> Rust Polars reads parquet
      -> Returns Box<ParquetDataFrame> (opaque, holds DataFrame)
        -> C++ GetColumn<T>() -> parquet_df_get_*_chunks() FFI
          -> Returns raw pointers to Polars internal buffers
            -> C++ iterates directly on Rust memory
```

### Filtered Query Flow
```
C++ DataFrame::Open(path).Select(...).Filter(...).Collect()
  -> DataFrameBuilder accumulates Select/Filter calls
    -> Collect() builds ParquetQuery via FFI
      -> parquet_query_new() -> parquet_query_select() -> parquet_query_filter_*()
        -> parquet_query_collect_df() -> Polars lazy scan with pushdown
          -> Returns DataFrame with filters applied
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
| `absl::CivilDay` | `i64` (ms) | `Datetime` | Yes (via int64) |
| `absl::CivilSecond` | `i64` (ms) | `Datetime` | Yes (via int64) |
| `absl::CivilMinute` | `i64` (ms) | `Datetime` | Yes (via int64) |
| `absl::CivilHour` | `i64` (ms) | `Datetime` | Yes (via int64) |

## Adding a New Column Type

1. Add `parquet_df_get_<type>_chunks()` in `cxx_bridge.rs`
2. Add `DataFrame::GetColumn<Type>()` specialization in `parquet.hpp`
3. Add `ParquetCellCodec<Type>::Write()` in `detail/cell_codec.hpp`
4. Add `ParquetTypeOf<Type>` specialization in `detail/type_traits.hpp`
5. Update `ParquetCodec::Add()` in `detail/codec.hpp` for df_readers_ lambda
6. Add tests

For custom types like `AbseilCivilTime`, use C++ concepts:
```cpp
// type_traits.hpp
template <typename T>
concept AbseilCivilTime = std::same_as<T, absl::CivilDay> || ...;

template <AbseilCivilTime T>
struct ParquetTypeOf<T> {
  static constexpr ffi::ColumnType type = ffi::ColumnType::DateTime;
};

// cell_codec.hpp
template <AbseilCivilTime T>
struct ParquetCellCodec<T> { /* Write with conversion */ };
```

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
- `abseil-cpp` (absl::time for CivilDay/CivilSecond support)
- GoogleTest (C++ tests)
