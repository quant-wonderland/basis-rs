# basis-rs

Rust utility library providing high-performance Parquet I/O with C++ bindings. Rust implements the core logic using [Polars](https://pola.rs/); a CXX bridge exposes it to C++; a header-only C++ layer provides a type-safe, struct-based API.

## Features

- **Zero-copy column access**: Direct pointer access to Parquet column data
- **Predicate pushdown**: Filter rows at the I/O layer using Polars lazy evaluation
- **Projection pushdown**: Read only needed columns from disk
- **Two write APIs**: Struct-based `ParquetWriter<T>` and zero-copy `ColumnarParquetWriter`
- **DateTime support**: Native handling of timestamps with Abseil civil time integration
- **Type-safe C++ API**: Header-only library with compile-time type checking

## Building

Requires a Nix flake environment (or Rust toolchain + CMake manually).

```bash
# Enter dev shell
nix develop

# Build everything
cargo build --release
cmake -S . -B build -DBASIS_RS_BUILD_TESTS=ON && cmake --build build

# Run all tests
cargo test && cd build && ctest --output-on-failure
```

## Rust API

```rust
use basis_rs::ParquetReader;

// Read entire file
let df = ParquetReader::new("data.parquet").read()?;

// Read only specific columns (projection pushdown — only reads needed data from disk)
let df = ParquetReader::new("data.parquet")
    .with_columns(["id", "score"])
    .read()?;

// Lazy evaluation with filtering (predicate pushdown)
let df = ParquetReader::new("data.parquet")
    .scan()?
    .filter(col("score").gt(lit(80.0)))
    .select([col("id"), col("score")])
    .collect()?;
```

## C++ API

### Basic Usage — Define a struct and its codec

```cpp
#include <basis_rs/parquet/parquet.hpp>

struct Trade {
    int64_t id;
    std::string symbol;
    double price;
};

template <>
inline const basis_rs::ParquetCodec<Trade>& basis_rs::GetParquetCodec() {
    static basis_rs::ParquetCodec<Trade> codec = []() {
        basis_rs::ParquetCodec<Trade> c;
        c.Add("id", &Trade::id);
        c.Add("symbol", &Trade::symbol);
        c.Add("price", &Trade::price);
        return c;
    }();
    return codec;
}
```

### ReadAllAs — Read into structs

`ReadAllAs` automatically reads only the columns registered in the codec, even if the Parquet file has many more columns. This provides automatic projection pushdown.

```cpp
basis_rs::DataFrame df("trades.parquet");
auto trades = df.ReadAllAs<Trade>();  // Only reads id, symbol, price from disk
```

### Query Builder — Select and Filter

For more control, use the query builder with column projection and row filtering:

```cpp
// Read with column projection and filtering (predicate pushdown via Polars lazy scan)
auto df = basis_rs::DataFrame::Open("trades.parquet")
    .Select({"id", "price"})
    .Filter("price", basis_rs::Gt, 100.0)
    .Filter("price", basis_rs::Lt, 500.0)
    .Collect();
auto trades = df.ReadAllAs<Trade>();
```

Available filter operators: `basis_rs::Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`.

### Zero-Copy Column Access

For maximum performance, use direct column access to iterate over data without copying:

```cpp
basis_rs::DataFrame df("trades.parquet");
auto price_col = df.GetColumn<double>("price");

// Range-for iteration (zero-copy)
for (double price : price_col) {
    process(price);
}

// Index-based access
for (size_t i = 0; i < price_col.size(); ++i) {
    process(price_col[i]);
}
```

Supported types: `int32_t`, `int64_t`, `uint64_t`, `float`, `double`

For DateTime columns, use `GetDateTimeColumn(df, "timestamp")` which returns `int64_t` milliseconds since Unix epoch.

### Writing Parquet Files

#### Struct-based Writer (ParquetWriter)

For row-oriented data (structs), use `ParquetWriter<T>`:

```cpp
basis_rs::ParquetWriter<Trade> writer("output.parquet");
writer.WithCompression("zstd").WithRowGroupSize(100000);
writer.WriteRecord({1, "AAPL", 150.0});
writer.WriteRecord({2, "GOOG", 2800.0});
writer.Finish();  // Explicit finish to handle errors
```

#### Zero-Copy Columnar Writer (ColumnarParquetWriter)

For columnar data (Structure of Arrays), use `ColumnarParquetWriter` for ~42% better performance:

```cpp
std::vector<int64_t> ids = {1, 2, 3};
std::vector<std::string> symbols = {"AAPL", "GOOG", "MSFT"};
std::vector<double> prices = {150.0, 2800.0, 300.0};

basis_rs::ColumnarParquetWriter writer("output.parquet");
writer.WithCompression("zstd").WithRowGroupSize(500000);
writer.AddColumn("id", ids.data(), ids.size());
writer.AddColumn("symbol", symbols);
writer.AddColumn("price", prices.data(), prices.size());
writer.WriteBatch();  // Column data must stay valid until here
writer.Finish();
```

Key differences:
- `ColumnarParquetWriter`: Zero-copy for numeric types, requires columnar input, ~42% faster
- `ParquetWriter<T>`: Convenient for struct records, automatic AoS→SoA conversion

## Performance

Benchmarked on 5 Parquet files (~550-670MB each, ~18-20M rows, 49 columns, sorted by StockId ascending). Each test item reads all 5 files sequentially (one read per file) to avoid OS page cache bias, results averaged.

### Read Performance

| Operation | Time | Notes |
|-----------|------|-------|
| Open (4 columns projected) | 123ms | Projection pushdown |
| Open (all 49 columns) | 1426ms | Full read |
| Zero-copy column access | 0.004ms | Just pointer retrieval |
| Column iteration (sum 20M floats) | 101ms | Range-for loop |
| Row iteration (chunk-wise) | 73ms | Multi-column chunk access |
| ReadAllAs\<T\> (4 columns) | 723ms | Struct vector conversion (chunk-wise access) |

### Write Performance

Tested with 20.7M rows × 4 columns (StockId: i32, Close/High/Low: f32):

| Writer | Compression | Time | Notes |
|--------|-------------|------|-------|
| Rust baseline | zstd | 588ms | Direct Polars write |
| Rust baseline | snappy | 452ms | Direct Polars write |
| ParquetWriter\<T\> | zstd | 1073ms | Struct-based, AoS→SoA conversion |
| ColumnarParquetWriter | zstd | 401ms | Zero-copy, 62% faster than struct writer |
| ColumnarParquetWriter | snappy | 321ms | Zero-copy, 70% faster than struct writer |

Key findings:
- `ColumnarParquetWriter` with zero-copy is faster than even the Rust baseline
- Zero-copy eliminates both AoS→SoA extraction AND Series::new memcpy overhead
- For columnar data, use `ColumnarParquetWriter` for maximum performance

### Filter Performance

C++ FFI overhead is ~1ms — filter performance equals pure Polars.

| Filter | Time | Notes |
|--------|------|-------|
| No filter (projected) | 107ms | Baseline |
| StockId == 1 (sorted, head) | 71ms | Row group pruning skips most I/O |
| StockId == 600519 (sorted, tail) | 102ms | More metadata to scan before pruning |
| StockId > 600000 (sorted, ~45%) | 136ms | Partial row group skip |
| Close > 100.0f (unsorted, ~1.5%) | 97ms | Row group stats less effective |
| Close > 10.0f (unsorted, ~58%) | 162ms | Full scan, large result set |

Key findings:
- Sorted column eq filter is faster than reading without filter — row group min/max pruning skips I/O entirely
- Unsorted columns still benefit from row group statistics, but less effectively
- Performance is dominated by row groups read from disk and result set memory allocation

### Write Tuning: row_group_size Impact

Original files have 5 row groups × ~5M rows. Re-written with all 49 columns, tested with 4-column projection filter. Multi-file avg.

| row_group_size | Avg File Size | Full Read | StockId==1 | StockId==600519 | Notes |
|----------------|---------------|-----------|------------|-----------------|-------|
| 10K | 729MB | 1365ms | 191ms | 197ms | Metadata bloat, worst overall |
| 50K | 699MB | 643ms | 50ms | 45ms | Good pruning |
| 100K | 675MB | 653ms | 26ms | 25ms | Good balance |
| 500K | 656MB | 709ms | 9ms | 12ms | Optimal filter (~5x faster) |
| 1M | 643MB | 786ms | 11ms | 32ms | Head OK, tail degrades |
| original (~5M) | 608MB | 1083ms | 44ms | 52ms | Baseline |

Recommendation:
- For filter-heavy workloads: `WithRowGroupSize(500000)`
- For full-read workloads: `WithRowGroupSize(100000)`

## Benchmarking

### Built-in Benchmark Tool

A built-in benchmark tool lets you measure read performance on real files.

```bash
# Build the benchmark
cargo build --release --example bench_read

# Basic: read entire file
cargo run --release --example bench_read -- /path/to/data.parquet

# Projected read: only specific columns
cargo run --release --example bench_read -- /path/to/data.parquet --columns id,price,volume

# Filtered read: predicate pushdown via lazy scan
cargo run --release --example bench_read -- /path/to/data.parquet --filter "price>100.0"

# Combined: project + filter with more iterations
cargo run --release --example bench_read -- /path/to/data.parquet \
    --columns id,price --filter "price>100.0" --runs 10
```

### Writing Custom Benchmarks

You can use the `basis_rs` crate directly in a Rust binary:

```rust
use basis_rs::ParquetReader;
use std::time::Instant;

fn main() {
    let path = "/path/to/your/file.parquet";

    let start = Instant::now();
    let df = ParquetReader::new(path)
        .with_columns(["id", "price"])
        .read()
        .unwrap();
    println!("Read {} rows in {:?}", df.height(), start.elapsed());
}
```

## Consumer Integration

### Nix Flake

```nix
inputs.basis-rs.url = "github:user/basis-rs";

# In buildInputs:
buildInputs = [ inputs.basis-rs.packages.${system}.default ];
```

### CMake

```cmake
find_package(basis-rs REQUIRED)
target_link_libraries(my_target basis_rs::parquet)
```

## Development

### Building

```bash
# Enter dev shell (automatic with direnv, or manually)
nix develop

# Build Rust library (required first — generates static lib + CXX bridge)
cargo build --release

# Build C++ library and tests
cmake -S . -B build -DBASIS_RS_BUILD_TESTS=ON
cmake --build build
```

### Testing

```bash
# Rust tests
cargo test

# C++ tests (requires cargo build --release first)
cd build && ctest --output-on-failure
```

## License

MIT OR Apache-2.0
