use basis_rs::ParquetReader;
use polars::prelude::*;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: bench_parquet <path> [col1,col2,col3]");
        std::process::exit(1);
    }
    let path = &args[1];

    // Auto-detect 3 selected columns if not specified
    let selected: Vec<String> = if args.len() >= 3 {
        args[2].split(',').map(|s| s.to_string()).collect()
    } else {
        // Pick first 3 non-timestamp numeric columns from schema
        let df = ParquetReader::new(path).with_n_rows(0).read().unwrap();
        df.get_columns()
            .iter()
            .filter(|c| {
                matches!(
                    c.dtype(),
                    DataType::Int64 | DataType::Int32 | DataType::Float32 | DataType::Float64
                )
            })
            .take(3)
            .map(|c| c.name().to_string())
            .collect()
    };

    println!("=== Rust Polars Parquet Benchmark ===");
    println!("File: {}", path);
    println!("Selected columns: {:?}", selected);
    println!();

    // 1. Read all columns (eager)
    {
        let start = Instant::now();
        let df = ParquetReader::new(path).read().unwrap();
        let elapsed = start.elapsed();
        println!(
            "[Rust eager] Read ALL columns: {:.3?}  ({} rows x {} cols)",
            elapsed,
            df.height(),
            df.width()
        );
    }

    // 2. Read selected columns (eager, projection pushdown)
    {
        let cols: Vec<&str> = selected.iter().map(|s| s.as_str()).collect();
        let start = Instant::now();
        let df = ParquetReader::new(path).with_columns(cols).read().unwrap();
        let elapsed = start.elapsed();
        println!(
            "[Rust eager] Read SELECTED columns: {:.3?}  ({} rows x {} cols)",
            elapsed,
            df.height(),
            df.width()
        );
    }

    // 3. Scan all columns (lazy -> collect)
    {
        let start = Instant::now();
        let df = ParquetReader::new(path)
            .scan()
            .unwrap()
            .collect()
            .unwrap();
        let elapsed = start.elapsed();
        println!(
            "[Rust lazy ] Scan ALL columns: {:.3?}  ({} rows x {} cols)",
            elapsed,
            df.height(),
            df.width()
        );
    }

    // 4. Scan selected columns (lazy -> collect)
    {
        let cols: Vec<&str> = selected.iter().map(|s| s.as_str()).collect();
        let start = Instant::now();
        let df = ParquetReader::new(path)
            .with_columns(cols)
            .scan()
            .unwrap()
            .collect()
            .unwrap();
        let elapsed = start.elapsed();
        println!(
            "[Rust lazy ] Scan SELECTED columns: {:.3?}  ({} rows x {} cols)",
            elapsed,
            df.height(),
            df.width()
        );
    }
}
