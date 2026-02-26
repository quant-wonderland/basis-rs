//! Rust write benchmark baseline for comparing with C++ FFI write performance.

use polars::prelude::*;
use std::time::Instant;

const TEST_FILE: &str = "/var/lib/wonder/warehouse/database/lyc/parquet/DatayesTickSliceArchiver/2025/01/02.parquet";

fn benchmark(name: &str, iterations: usize, f: impl Fn()) -> f64 {
    f(); // warmup
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let avg_ms = start.elapsed().as_secs_f64() * 1000.0 / iterations as f64;
    println!("{name}: {avg_ms:.1} ms avg");
    avg_ms
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Rust Write Benchmark (Polars baseline) ===");
    println!("Source: {TEST_FILE}\n");

    // Read source data (4 columns to match C++ benchmark)
    let df = LazyFrame::scan_parquet(TEST_FILE, ScanArgsParquet::default())?
        .select([col("StockId"), col("Close"), col("High"), col("Low")])
        .collect()?;
    let rows = df.height();
    println!("Dataset: {rows} rows, {} columns\n", df.width());

    let tmp = std::env::temp_dir().join("basis_rs_rust_bench");
    std::fs::create_dir_all(&tmp)?;
    let out = tmp.join("bench.parquet");

    let t_zstd = benchmark("Write (zstd, default RGS)", 3, || {
        let mut df = df.clone();
        polars::io::parquet::write::ParquetWriter::new(std::fs::File::create(&out).unwrap())
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)
            .unwrap();
    });

    let t_zstd_500k = benchmark("Write (zstd, RGS=500K)", 3, || {
        let mut df = df.clone();
        polars::io::parquet::write::ParquetWriter::new(std::fs::File::create(&out).unwrap())
            .with_compression(ParquetCompression::Zstd(None))
            .with_row_group_size(Some(500_000))
            .finish(&mut df)
            .unwrap();
    });

    let t_snappy = benchmark("Write (snappy, RGS=500K)", 3, || {
        let mut df = df.clone();
        polars::io::parquet::write::ParquetWriter::new(std::fs::File::create(&out).unwrap())
            .with_compression(ParquetCompression::Snappy)
            .with_row_group_size(Some(500_000))
            .finish(&mut df)
            .unwrap();
    });

    let t_uncomp = benchmark("Write (uncompressed, RGS=500K)", 3, || {
        let mut df = df.clone();
        polars::io::parquet::write::ParquetWriter::new(std::fs::File::create(&out).unwrap())
            .with_compression(ParquetCompression::Uncompressed)
            .with_row_group_size(Some(500_000))
            .finish(&mut df)
            .unwrap();
    });

    // Simulate FFI path: build DataFrame from raw Vecs (like Series::new from slice)
    let col0: Vec<i32> = df.column("StockId").unwrap().i32().unwrap().to_vec_null_aware().left().unwrap();
    let col1: Vec<f32> = df.column("Close").unwrap().f32().unwrap().to_vec_null_aware().left().unwrap();
    let col2: Vec<f32> = df.column("High").unwrap().f32().unwrap().to_vec_null_aware().left().unwrap();
    let col3: Vec<f32> = df.column("Low").unwrap().f32().unwrap().to_vec_null_aware().left().unwrap();

    let t_from_vecs = benchmark("Write from Vecs (zstd, RGS=500K) [simulates FFI]", 3, || {
        let mut df = DataFrame::new(vec![
            Series::new("StockId".into(), &col0).into(),
            Series::new("Close".into(), &col1).into(),
            Series::new("High".into(), &col2).into(),
            Series::new("Low".into(), &col3).into(),
        ]).unwrap();
        polars::io::parquet::write::ParquetWriter::new(std::fs::File::create(&out).unwrap())
            .with_compression(ParquetCompression::Zstd(None))
            .with_row_group_size(Some(500_000))
            .finish(&mut df)
            .unwrap();
    });

    let m = rows as f64 / 1e6;
    println!("\n=== Summary ===");
    println!("Zstd (default):    {t_zstd:.1} ms ({:.1} M rows/s)", m / (t_zstd / 1000.0));
    println!("Zstd (500K RGS):   {t_zstd_500k:.1} ms ({:.1} M rows/s)", m / (t_zstd_500k / 1000.0));
    println!("Snappy (500K RGS): {t_snappy:.1} ms ({:.1} M rows/s)", m / (t_snappy / 1000.0));
    println!("Uncomp (500K RGS): {t_uncomp:.1} ms ({:.1} M rows/s)", m / (t_uncomp / 1000.0));
    println!("From Vecs (zstd):  {t_from_vecs:.1} ms ({:.1} M rows/s) [FFI simulation]", m / (t_from_vecs / 1000.0));

    std::fs::remove_dir_all(&tmp)?;
    Ok(())
}
