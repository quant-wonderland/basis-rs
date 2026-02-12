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

### ReadAll — Automatic column projection

`ReadAll` automatically reads only the columns registered in the codec, even if the Parquet file has many more columns. This is a major performance win for wide tables.

```cpp
basis_rs::ParquetFile file("trades.parquet");
auto trades = file.ReadAll<Trade>();  // Only reads id, symbol, price from disk
```

### Query Builder — Select and Filter

For more control, use the query builder to read specific fields or filter rows:

```cpp
// Read only id and price (symbol gets default value)
auto trades = file.Read<Trade>()
    .Select(&Trade::id, &Trade::price)
    .Collect();

// Filter rows — predicate pushdown via Polars lazy scan
auto expensive = file.Read<Trade>()
    .Filter(&Trade::price, basis_rs::Gt, 100.0)
    .Collect();

// Combine Select + Filter
auto result = file.Read<Trade>()
    .Select(&Trade::id, &Trade::price)
    .Filter(&Trade::price, basis_rs::Gt, 100.0)
    .Filter(&Trade::price, basis_rs::Lt, 500.0)
    .Collect();
```

Available filter operators: `basis_rs::Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`.

Fields not included in `Select()` will have their default-constructed values (0, 0.0, "", false).

### Writing

```cpp
auto writer = file.SpawnWriter<Trade>();
writer.WriteRecord({1, "AAPL", 150.0});
writer.WriteRecord({2, "GOOG", 2800.0});
// writer.Finish() called automatically on destruction
```

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
