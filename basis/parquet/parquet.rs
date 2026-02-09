//! High-performance Parquet read/write operations using Polars.
//!
//! Provides a clean API for reading and writing Parquet files with
//! configurable compression and row group settings.

use polars::prelude::*;
use std::path::Path;
use thiserror::Error;

/// Errors that can occur during Parquet operations.
#[derive(Error, Debug)]
pub enum ParquetError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),

    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

pub type Result<T> = std::result::Result<T, ParquetError>;

/// Parquet file reader with configurable options.
///
/// # Example
/// ```no_run
/// use basis_rs::ParquetReader;
///
/// let df = ParquetReader::new("data.parquet")
///     .with_columns(["id", "name"])
///     .read()?;
/// # Ok::<(), basis_rs::ParquetError>(())
/// ```
pub struct ParquetReader<P: AsRef<Path>> {
    path: P,
    columns: Option<Vec<String>>,
    n_rows: Option<usize>,
    parallel: ParallelStrategy,
}

impl<P: AsRef<Path>> ParquetReader<P> {
    /// Create a new reader for the given path.
    pub fn new(path: P) -> Self {
        Self {
            path,
            columns: None,
            n_rows: None,
            parallel: ParallelStrategy::Auto,
        }
    }

    /// Select specific columns to read (projection pushdown).
    /// This significantly improves performance by only reading needed columns.
    pub fn with_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.columns = Some(columns.into_iter().map(|s| s.as_ref().to_string()).collect());
        self
    }

    /// Limit the number of rows to read.
    pub fn with_n_rows(mut self, n: usize) -> Self {
        self.n_rows = Some(n);
        self
    }

    /// Set the parallel reading strategy.
    pub fn with_parallel(mut self, strategy: ParallelStrategy) -> Self {
        self.parallel = strategy;
        self
    }

    /// Read the Parquet file into a DataFrame.
    pub fn read(self) -> Result<DataFrame> {
        let file = std::fs::File::open(&self.path)?;
        let mut reader = polars::io::parquet::read::ParquetReader::new(file);

        // Apply projection pushdown for better performance
        if let Some(cols) = self.columns {
            reader = reader.with_columns(Some(cols));
        }

        reader = reader.read_parallel(self.parallel);

        let mut df = reader.finish()?;

        // Apply row limit after reading if specified
        if let Some(n) = self.n_rows {
            df = df.head(Some(n));
        }

        Ok(df)
    }

    /// Read using lazy evaluation for optimized query execution.
    /// Polars will automatically apply predicate and projection pushdown.
    pub fn scan(self) -> Result<LazyFrame> {
        let mut args = ScanArgsParquet::default();

        args.n_rows = self.n_rows;
        args.parallel = self.parallel;

        let lf = LazyFrame::scan_parquet(&self.path, args)?;

        // Apply column selection lazily if specified
        if let Some(cols) = self.columns {
            let col_exprs: Vec<_> = cols.iter().map(|c| col(c.as_str())).collect();
            Ok(lf.select(col_exprs))
        } else {
            Ok(lf)
        }
    }
}

/// Parquet file writer with configurable compression and row group settings.
///
/// # Example
/// ```no_run
/// use basis_rs::ParquetWriter;
/// use polars::prelude::*;
///
/// let df = df! {
///     "id" => [1, 2, 3],
///     "name" => ["a", "b", "c"],
/// }?;
///
/// ParquetWriter::new("output.parquet")
///     .with_compression(ParquetCompression::Zstd(None))
///     .with_row_group_size(100_000)
///     .write(&mut df.clone())?;
/// # Ok::<(), basis_rs::ParquetError>(())
/// ```
pub struct ParquetWriter<P: AsRef<Path>> {
    path: P,
    compression: ParquetCompression,
    row_group_size: Option<usize>,
    statistics: StatisticsOptions,
    data_page_size: Option<usize>,
}

impl<P: AsRef<Path>> ParquetWriter<P> {
    /// Create a new writer for the given path.
    pub fn new(path: P) -> Self {
        Self {
            path,
            compression: ParquetCompression::Zstd(None), // Zstd offers best compression/speed ratio
            row_group_size: None,
            statistics: StatisticsOptions::default(),
            data_page_size: None,
        }
    }

    /// Set the compression algorithm.
    /// - `Zstd`: Best balance of compression ratio and speed (default)
    /// - `Snappy`: Fastest, moderate compression
    /// - `Lz4Raw`: Fast with good compression
    /// - `Gzip`: High compression, slower
    /// - `Uncompressed`: No compression, fastest I/O
    pub fn with_compression(mut self, compression: ParquetCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Set the row group size. Larger values improve compression but use more memory.
    /// Default is typically 512K-1M rows. Recommended: 100K-1M rows.
    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = Some(size);
        self
    }

    /// Configure statistics collection for predicate pushdown optimization.
    pub fn with_statistics(mut self, options: StatisticsOptions) -> Self {
        self.statistics = options;
        self
    }

    /// Set the data page size in bytes. Smaller pages allow finer-grained reads.
    pub fn with_data_page_size(mut self, size: usize) -> Self {
        self.data_page_size = Some(size);
        self
    }

    /// Write a DataFrame to the Parquet file.
    pub fn write(self, df: &mut DataFrame) -> Result<()> {
        let file = std::fs::File::create(&self.path)?;
        let mut writer = polars::io::parquet::write::ParquetWriter::new(file)
            .with_compression(self.compression)
            .with_statistics(self.statistics);

        if let Some(size) = self.row_group_size {
            writer = writer.with_row_group_size(Some(size));
        }

        if let Some(size) = self.data_page_size {
            writer = writer.with_data_page_size(Some(size));
        }

        writer.finish(df)?;
        Ok(())
    }

    /// Write a LazyFrame to the Parquet file using sink for memory efficiency.
    /// Ideal for large datasets that don't fit in memory.
    pub fn sink(self, lf: LazyFrame) -> Result<()> {
        let mut options = ParquetWriteOptions::default();
        options.compression = self.compression;
        options.statistics = self.statistics;
        options.row_group_size = self.row_group_size;
        options.data_page_size = self.data_page_size;

        lf.sink_parquet(&self.path, options, None)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_df() -> DataFrame {
        df! {
            "id" => [1i64, 2, 3, 4, 5],
            "name" => ["alice", "bob", "charlie", "diana", "eve"],
            "score" => [85.5, 92.0, 78.5, 95.0, 88.5],
        }
        .unwrap()
    }

    #[test]
    fn test_roundtrip() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.parquet");

        let mut df = sample_df();
        ParquetWriter::new(&path).write(&mut df)?;

        let loaded = ParquetReader::new(&path).read()?;
        assert_eq!(df.shape(), loaded.shape());
        Ok(())
    }

    #[test]
    fn test_column_projection() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.parquet");

        let mut df = sample_df();
        ParquetWriter::new(&path).write(&mut df)?;

        let loaded = ParquetReader::new(&path)
            .with_columns(["id", "name"])
            .read()?;

        assert_eq!(loaded.width(), 2);
        assert!(loaded.column("id").is_ok());
        assert!(loaded.column("name").is_ok());
        assert!(loaded.column("score").is_err());
        Ok(())
    }

    #[test]
    fn test_row_limit() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.parquet");

        let mut df = sample_df();
        ParquetWriter::new(&path).write(&mut df)?;

        let loaded = ParquetReader::new(&path).with_n_rows(2).read()?;
        assert_eq!(loaded.height(), 2);
        Ok(())
    }

    #[test]
    fn test_lazy_scan() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.parquet");

        let mut df = sample_df();
        ParquetWriter::new(&path).write(&mut df)?;

        // Lazy query with filter - Polars optimizes this with predicate pushdown
        let result = ParquetReader::new(&path)
            .scan()?
            .filter(col("score").gt(lit(80.0)))
            .select([col("name"), col("score")])
            .collect()?;

        assert_eq!(result.width(), 2);
        assert!(result.height() < df.height());
        Ok(())
    }

    #[test]
    fn test_compression_options() -> Result<()> {
        let dir = tempdir()?;

        let mut df = sample_df();

        // Test different compression algorithms
        for (name, compression) in [
            ("zstd", ParquetCompression::Zstd(None)),
            ("snappy", ParquetCompression::Snappy),
            ("uncompressed", ParquetCompression::Uncompressed),
        ] {
            let path = dir.path().join(format!("{name}.parquet"));
            ParquetWriter::new(&path)
                .with_compression(compression)
                .write(&mut df)?;

            let loaded = ParquetReader::new(&path).read()?;
            assert_eq!(df.shape(), loaded.shape(), "Failed for {name}");
        }
        Ok(())
    }
}
