# basis-rs

Rust utility library providing high-performance Parquet I/O with C++ bindings. Rust implements the core logic using [Polars](https://pola.rs/); a CXX bridge exposes it to C++; a header-only C++ layer provides a type-safe, struct-based API.

## Building

Requires a Nix flake environment (or Rust toolchain + CMake manually).

```bash
# Enter dev shell
nix develop

# Build Rust library (required first — generates static lib + CXX bridge)
cargo build --release

# Build C++ library and tests
cmake -S . -B build -DBASIS_RS_BUILD_TESTS=ON
cmake --build build

# Run all tests
cargo test
cd build && ctest --output-on-failure
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

`ReadAllAs` automatically reads only the columns registered in the codec, even if the Parquet file has many more columns. This is a major performance win for wide tables.

```cpp
basis_rs::DataFrame df("trades.parquet");
auto trades = df.ReadAllAs<Trade>();  // Only reads id, symbol, price from disk
```

### Query Builder — Select and Filter

For more control, use the query builder to read specific columns or filter rows:

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

For maximum performance, use direct column access:

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

### Writing

```cpp
basis_rs::ParquetWriter<Trade> writer("output.parquet");
writer.WriteRecord({1, "AAPL", 150.0});
writer.WriteRecord({2, "GOOG", 2800.0});
writer.Finish();  // or let destructor call it
```

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

Recommendation: `with_row_group_size(500_000)` for filter workloads, `with_row_group_size(100_000)` for full-read workloads.

## Benchmarking Parquet Read Performance

A built-in benchmark tool lets you measure read performance on real files.

### Build the benchmark

```bash
cargo build --release --example bench_read
```

### Run benchmarks

```bash
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

### Example output

```
File: /var/lib/wonder/warehouse/database/lyc/parquet/DatayesTickSliceArchiver/2025/01/08.parquet
Schema: 1284532 rows x 24 cols
Columns:
  id: Int64
  price: Float64
  volume: Int64
  ...

Benchmarks (5 runs each):

  Full read:
    Result: 1284532 rows x 24 cols
    min=142.381ms  median=145.208ms  mean=146.120ms  max=151.563ms  (5 runs)

  Projected read:
    Result: 1284532 rows x 3 cols
    min=31.256ms  median=32.104ms  mean=33.012ms  max=36.891ms  (5 runs)

  Lazy filtered read:
    Result: 42318 rows x 3 cols
    min=18.442ms  median=19.105ms  mean=19.320ms  max=20.812ms  (5 runs)
```

### CLI reference

```
bench_read <file> [options]

Options:
  --columns col1,col2,...   Only read these columns (projection pushdown)
  --filter  "col>value"     Filter rows using lazy scan (predicate pushdown)
  --runs N                  Number of iterations (default: 5)

Filter operators: >, >=, <, <=, ==, !=
Filter value is parsed as f64.
```

### Writing your own Rust benchmarks

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

### Nix flake

```nix
inputs.basis-rs.url = "github:user/basis-rs";
# Then in buildInputs:
buildInputs = [ basis-rs.packages.${system}.default ];
```

### CMake

```cmake
find_package(basis-rs REQUIRED)
target_link_libraries(my_target basis_rs::parquet)
```

## Testing

```bash
# Rust tests
cargo test

# C++ tests (requires cargo build --release first)
cmake -S . -B build -DBASIS_RS_BUILD_TESTS=ON && cmake --build build
cd build && ctest --output-on-failure
```

## License

MIT OR Apache-2.0
