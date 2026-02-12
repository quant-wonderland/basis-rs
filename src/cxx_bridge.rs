//! CXX bridge for type-safe Rust-C++ interop.
//!
//! This module provides a safe FFI interface using the cxx crate.
//!
//! ## Architecture
//!
//! The new design focuses on zero-copy column access:
//!
//! 1. ParquetDataFrame: Holds the DataFrame and provides column access via slices
//! 2. No data copying in Rust - C++ accesses raw pointers directly
//! 3. Optional rechunk for single contiguous slice per column
//! 4. ReadAllAs<T> done entirely in C++ using column slices

use crate::parquet::{ParquetReader as PolarsReader, ParquetWriter as PolarsWriter};
use polars::prelude::*;
use std::collections::HashMap;

#[cxx::bridge(namespace = "basis_rs::ffi")]
mod ffi {
    /// Column data type enum shared between Rust and C++.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ColumnType {
        Int64,
        Int32,
        UInt64,
        Float64,
        Float32,
        String,
        Bool,
        DateTime, // milliseconds since epoch
        Unknown,
    }

    /// Information about a column in a Parquet file.
    #[derive(Debug, Clone)]
    struct ColumnInfo {
        name: String,
        dtype: ColumnType,
    }

    /// Filter comparison operator shared between Rust and C++.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FilterOp {
        Eq,
        Ne,
        Lt,
        Le,
        Gt,
        Ge,
    }

    /// A contiguous chunk of column data. For primitive types, this provides
    /// direct pointer access to the underlying memory.
    #[derive(Debug, Clone)]
    struct ColumnChunk {
        /// Pointer to the start of the data (cast to appropriate type in C++)
        ptr: usize,
        /// Number of elements in this chunk
        len: usize,
    }

    extern "Rust" {
        // ==================== New Zero-Copy API ====================

        /// Opaque DataFrame wrapper - keeps data alive while C++ accesses it
        type ParquetDataFrame;

        /// Open a parquet file and return a DataFrame wrapper
        fn parquet_open(path: &str) -> Result<Box<ParquetDataFrame>>;

        /// Open with column projection (only read specified columns)
        fn parquet_open_projected(
            path: &str,
            columns: Vec<String>,
        ) -> Result<Box<ParquetDataFrame>>;

        /// Get number of rows
        fn parquet_df_num_rows(df: &ParquetDataFrame) -> usize;

        /// Get number of columns
        fn parquet_df_num_cols(df: &ParquetDataFrame) -> usize;

        /// Get column info (name and type for each column)
        fn parquet_df_columns(df: &ParquetDataFrame) -> Vec<ColumnInfo>;

        /// Rechunk the DataFrame to ensure single contiguous buffer per column.
        /// This makes subsequent slice access faster but has an upfront cost.
        /// Returns true if rechunking was performed (had multiple chunks).
        fn parquet_df_rechunk(df: &mut ParquetDataFrame) -> bool;

        /// Get number of chunks for a column (1 after rechunk)
        fn parquet_df_num_chunks(df: &ParquetDataFrame, column: &str) -> Result<usize>;

        /// Get column chunks as raw pointers. Each chunk is contiguous memory.
        /// The pointers are valid as long as the ParquetDataFrame is alive.
        fn parquet_df_get_i64_chunks(
            df: &ParquetDataFrame,
            column: &str,
        ) -> Result<Vec<ColumnChunk>>;
        fn parquet_df_get_i32_chunks(
            df: &ParquetDataFrame,
            column: &str,
        ) -> Result<Vec<ColumnChunk>>;
        fn parquet_df_get_u64_chunks(
            df: &ParquetDataFrame,
            column: &str,
        ) -> Result<Vec<ColumnChunk>>;
        fn parquet_df_get_f64_chunks(
            df: &ParquetDataFrame,
            column: &str,
        ) -> Result<Vec<ColumnChunk>>;
        fn parquet_df_get_f32_chunks(
            df: &ParquetDataFrame,
            column: &str,
        ) -> Result<Vec<ColumnChunk>>;
        fn parquet_df_get_bool_chunks(
            df: &ParquetDataFrame,
            column: &str,
        ) -> Result<Vec<ColumnChunk>>;
        fn parquet_df_get_datetime_chunks(
            df: &ParquetDataFrame,
            column: &str,
        ) -> Result<Vec<ColumnChunk>>;

        /// Get string column - returns all strings concatenated with offsets.
        /// This is the only type that requires allocation on read.
        fn parquet_df_get_string_column(df: &ParquetDataFrame, column: &str)
            -> Result<Vec<String>>;

        // ==================== Legacy API (for backward compatibility) ====================

        type ParquetReader;
        type ParquetWriter;
        type ParquetQuery;

        // Reader functions (legacy)
        fn parquet_reader_open(path: &str) -> Result<Box<ParquetReader>>;
        fn parquet_reader_open_projected(
            path: &str,
            columns: Vec<String>,
        ) -> Result<Box<ParquetReader>>;
        fn parquet_reader_num_rows(reader: &ParquetReader) -> usize;
        fn parquet_reader_num_cols(reader: &ParquetReader) -> usize;
        fn parquet_reader_columns(reader: &ParquetReader) -> Vec<ColumnInfo>;

        // Column getters (legacy) - return owned vectors
        fn parquet_reader_get_i64_column(reader: &ParquetReader, name: &str) -> Result<Vec<i64>>;
        fn parquet_reader_get_i32_column(reader: &ParquetReader, name: &str) -> Result<Vec<i32>>;
        fn parquet_reader_get_f64_column(reader: &ParquetReader, name: &str) -> Result<Vec<f64>>;
        fn parquet_reader_get_f32_column(reader: &ParquetReader, name: &str) -> Result<Vec<f32>>;
        fn parquet_reader_get_string_column(
            reader: &ParquetReader,
            name: &str,
        ) -> Result<Vec<String>>;
        fn parquet_reader_get_bool_column(reader: &ParquetReader, name: &str) -> Result<Vec<bool>>;

        // Writer functions
        fn parquet_writer_new(path: &str) -> Result<Box<ParquetWriter>>;
        fn parquet_writer_add_i64_column(
            writer: &mut ParquetWriter,
            name: &str,
            data: &[i64],
        ) -> Result<()>;
        fn parquet_writer_add_i32_column(
            writer: &mut ParquetWriter,
            name: &str,
            data: &[i32],
        ) -> Result<()>;
        fn parquet_writer_add_f64_column(
            writer: &mut ParquetWriter,
            name: &str,
            data: &[f64],
        ) -> Result<()>;
        fn parquet_writer_add_f32_column(
            writer: &mut ParquetWriter,
            name: &str,
            data: &[f32],
        ) -> Result<()>;
        fn parquet_writer_add_string_column(
            writer: &mut ParquetWriter,
            name: &str,
            data: Vec<String>,
        ) -> Result<()>;
        fn parquet_writer_add_bool_column(
            writer: &mut ParquetWriter,
            name: &str,
            data: &[bool],
        ) -> Result<()>;
        fn parquet_writer_finish(writer: Box<ParquetWriter>) -> Result<()>;

        // Query builder functions (lazy evaluation with predicate/projection pushdown)
        fn parquet_query_new(path: &str) -> Result<Box<ParquetQuery>>;
        fn parquet_query_select(query: &mut ParquetQuery, columns: Vec<String>);
        fn parquet_query_filter_i64(
            query: &mut ParquetQuery,
            column: &str,
            op: FilterOp,
            value: i64,
        );
        fn parquet_query_filter_i32(
            query: &mut ParquetQuery,
            column: &str,
            op: FilterOp,
            value: i32,
        );
        fn parquet_query_filter_f64(
            query: &mut ParquetQuery,
            column: &str,
            op: FilterOp,
            value: f64,
        );
        fn parquet_query_filter_f32(
            query: &mut ParquetQuery,
            column: &str,
            op: FilterOp,
            value: f32,
        );
        fn parquet_query_filter_str(
            query: &mut ParquetQuery,
            column: &str,
            op: FilterOp,
            value: &str,
        );
        fn parquet_query_filter_bool(
            query: &mut ParquetQuery,
            column: &str,
            op: FilterOp,
            value: bool,
        );
        fn parquet_query_collect(query: Box<ParquetQuery>) -> Result<Box<ParquetReader>>;

        /// New: collect into zero-copy DataFrame
        fn parquet_query_collect_df(query: Box<ParquetQuery>) -> Result<Box<ParquetDataFrame>>;
    }
}

// ==================== New Zero-Copy Implementation ====================

/// Zero-copy DataFrame wrapper. Keeps the Polars DataFrame alive while
/// C++ accesses column data through raw pointers.
pub struct ParquetDataFrame {
    df: DataFrame,
}

fn dtype_to_column_type(dtype: &DataType) -> ffi::ColumnType {
    match dtype {
        DataType::Int64 => ffi::ColumnType::Int64,
        DataType::Int32 => ffi::ColumnType::Int32,
        DataType::UInt64 => ffi::ColumnType::UInt64,
        DataType::Float64 => ffi::ColumnType::Float64,
        DataType::Float32 => ffi::ColumnType::Float32,
        DataType::String => ffi::ColumnType::String,
        DataType::Boolean => ffi::ColumnType::Bool,
        DataType::Datetime(_, _) => ffi::ColumnType::DateTime,
        _ => ffi::ColumnType::Unknown,
    }
}

fn parquet_open(path: &str) -> Result<Box<ParquetDataFrame>, String> {
    let df = PolarsReader::new(path).read().map_err(|e| e.to_string())?;
    Ok(Box::new(ParquetDataFrame { df }))
}

fn parquet_open_projected(
    path: &str,
    columns: Vec<String>,
) -> Result<Box<ParquetDataFrame>, String> {
    let df = if columns.is_empty() {
        PolarsReader::new(path).read()
    } else {
        PolarsReader::new(path).with_columns(columns).read()
    }
    .map_err(|e| e.to_string())?;
    Ok(Box::new(ParquetDataFrame { df }))
}

fn parquet_df_num_rows(df: &ParquetDataFrame) -> usize {
    df.df.height()
}

fn parquet_df_num_cols(df: &ParquetDataFrame) -> usize {
    df.df.width()
}

fn parquet_df_columns(df: &ParquetDataFrame) -> Vec<ffi::ColumnInfo> {
    df.df
        .get_columns()
        .iter()
        .map(|col| ffi::ColumnInfo {
            name: col.name().to_string(),
            dtype: dtype_to_column_type(col.dtype()),
        })
        .collect()
}

fn parquet_df_rechunk(df: &mut ParquetDataFrame) -> bool {
    let had_multiple = df.df.get_columns().iter().any(|c| c.n_chunks() > 1);
    if had_multiple {
        df.df.rechunk_mut();
    }
    had_multiple
}

fn parquet_df_num_chunks(df: &ParquetDataFrame, column: &str) -> Result<usize, String> {
    let col = df.df.column(column).map_err(|e| e.to_string())?;
    Ok(col.n_chunks())
}

// Macro to generate chunk getter functions for primitive types
macro_rules! impl_get_chunks {
    ($fn_name:ident, $polars_method:ident, $rust_type:ty) => {
        fn $fn_name(df: &ParquetDataFrame, column: &str) -> Result<Vec<ffi::ColumnChunk>, String> {
            let col = df
                .df
                .column(column)
                .map_err(|e| format!("Column '{}' not found: {}", column, e))?;

            let ca = col
                .$polars_method()
                .map_err(|e| format!("Column '{}' type mismatch: {}", column, e))?;

            let chunks: Vec<ffi::ColumnChunk> = ca
                .downcast_iter()
                .map(|arr| {
                    let values = arr.values();
                    ffi::ColumnChunk {
                        ptr: values.as_ptr() as usize,
                        len: values.len(),
                    }
                })
                .collect();

            Ok(chunks)
        }
    };
}

impl_get_chunks!(parquet_df_get_i64_chunks, i64, i64);
impl_get_chunks!(parquet_df_get_i32_chunks, i32, i32);
impl_get_chunks!(parquet_df_get_u64_chunks, u64, u64);
impl_get_chunks!(parquet_df_get_f64_chunks, f64, f64);
impl_get_chunks!(parquet_df_get_f32_chunks, f32, f32);

fn parquet_df_get_bool_chunks(
    df: &ParquetDataFrame,
    column: &str,
) -> Result<Vec<ffi::ColumnChunk>, String> {
    let col = df
        .df
        .column(column)
        .map_err(|e| format!("Column '{}' not found: {}", column, e))?;

    let ca = col
        .bool()
        .map_err(|e| format!("Column '{}' is not Boolean: {}", column, e))?;

    // Boolean arrays in Arrow use bit-packed storage, not direct bool*
    // We return the raw bitmap pointer - C++ needs to decode bits
    let chunks: Vec<ffi::ColumnChunk> = ca
        .downcast_iter()
        .map(|arr| {
            let values = arr.values();
            // values.as_slice() returns (&[u8], offset, len)
            let (slice, _offset, _bit_len) = values.as_slice();
            ffi::ColumnChunk {
                ptr: slice.as_ptr() as usize,
                len: arr.len(), // number of logical boolean elements
            }
        })
        .collect();

    Ok(chunks)
}

fn parquet_df_get_datetime_chunks(
    df: &ParquetDataFrame,
    column: &str,
) -> Result<Vec<ffi::ColumnChunk>, String> {
    let col = df
        .df
        .column(column)
        .map_err(|e| format!("Column '{}' not found: {}", column, e))?;

    // Datetime is stored as Int64 milliseconds
    let ca = col
        .datetime()
        .map_err(|e| format!("Column '{}' is not Datetime: {}", column, e))?;

    let chunks: Vec<ffi::ColumnChunk> = ca
        .downcast_iter()
        .map(|arr| {
            let values = arr.values();
            ffi::ColumnChunk {
                ptr: values.as_ptr() as usize,
                len: values.len(),
            }
        })
        .collect();

    Ok(chunks)
}

fn parquet_df_get_string_column(
    df: &ParquetDataFrame,
    column: &str,
) -> Result<Vec<String>, String> {
    let col = df
        .df
        .column(column)
        .map_err(|e| format!("Column '{}' not found: {}", column, e))?;

    let ca = col
        .str()
        .map_err(|e| format!("Column '{}' is not String: {}", column, e))?;

    // Strings cannot be zero-copy due to encoding differences
    Ok(ca.iter().map(|opt| opt.unwrap_or("").to_string()).collect())
}

// ==================== Legacy Implementation ====================

/// Wrapper around a Polars DataFrame loaded from a Parquet file.
pub struct ParquetReader {
    df: DataFrame,
}

/// Wrapper for building and writing a Parquet file.
pub struct ParquetWriter {
    path: String,
    columns: HashMap<String, Series>,
    column_order: Vec<String>,
}

fn parquet_reader_open(path: &str) -> Result<Box<ParquetReader>, String> {
    let df = PolarsReader::new(path).read().map_err(|e| e.to_string())?;
    Ok(Box::new(ParquetReader { df }))
}

fn parquet_reader_open_projected(
    path: &str,
    columns: Vec<String>,
) -> Result<Box<ParquetReader>, String> {
    let df = if columns.is_empty() {
        PolarsReader::new(path).read()
    } else {
        PolarsReader::new(path).with_columns(columns).read()
    }
    .map_err(|e| e.to_string())?;
    Ok(Box::new(ParquetReader { df }))
}

fn parquet_reader_num_rows(reader: &ParquetReader) -> usize {
    reader.df.height()
}

fn parquet_reader_num_cols(reader: &ParquetReader) -> usize {
    reader.df.width()
}

fn parquet_reader_columns(reader: &ParquetReader) -> Vec<ffi::ColumnInfo> {
    reader
        .df
        .get_columns()
        .iter()
        .map(|col| ffi::ColumnInfo {
            name: col.name().to_string(),
            dtype: dtype_to_column_type(col.dtype()),
        })
        .collect()
}

fn parquet_reader_get_i64_column(reader: &ParquetReader, name: &str) -> Result<Vec<i64>, String> {
    let col = reader
        .df
        .column(name)
        .map_err(|e| format!("Column '{}' not found: {}", name, e))?;

    let ca = col
        .i64()
        .map_err(|e| format!("Column '{}' is not Int64: {}", name, e))?;

    Ok(ca.iter().map(|opt| opt.unwrap_or(0)).collect())
}

fn parquet_reader_get_i32_column(reader: &ParquetReader, name: &str) -> Result<Vec<i32>, String> {
    let col = reader
        .df
        .column(name)
        .map_err(|e| format!("Column '{}' not found: {}", name, e))?;

    let ca = col
        .i32()
        .map_err(|e| format!("Column '{}' is not Int32: {}", name, e))?;

    Ok(ca.iter().map(|opt| opt.unwrap_or(0)).collect())
}

fn parquet_reader_get_f64_column(reader: &ParquetReader, name: &str) -> Result<Vec<f64>, String> {
    let col = reader
        .df
        .column(name)
        .map_err(|e| format!("Column '{}' not found: {}", name, e))?;

    let ca = col
        .f64()
        .map_err(|e| format!("Column '{}' is not Float64: {}", name, e))?;

    Ok(ca.iter().map(|opt| opt.unwrap_or(0.0)).collect())
}

fn parquet_reader_get_f32_column(reader: &ParquetReader, name: &str) -> Result<Vec<f32>, String> {
    let col = reader
        .df
        .column(name)
        .map_err(|e| format!("Column '{}' not found: {}", name, e))?;

    let ca = col
        .f32()
        .map_err(|e| format!("Column '{}' is not Float32: {}", name, e))?;

    Ok(ca.iter().map(|opt| opt.unwrap_or(0.0)).collect())
}

fn parquet_reader_get_string_column(
    reader: &ParquetReader,
    name: &str,
) -> Result<Vec<String>, String> {
    let col = reader
        .df
        .column(name)
        .map_err(|e| format!("Column '{}' not found: {}", name, e))?;

    let ca = col
        .str()
        .map_err(|e| format!("Column '{}' is not String: {}", name, e))?;

    Ok(ca.iter().map(|opt| opt.unwrap_or("").to_string()).collect())
}

fn parquet_reader_get_bool_column(reader: &ParquetReader, name: &str) -> Result<Vec<bool>, String> {
    let col = reader
        .df
        .column(name)
        .map_err(|e| format!("Column '{}' not found: {}", name, e))?;

    let ca = col
        .bool()
        .map_err(|e| format!("Column '{}' is not Boolean: {}", name, e))?;

    Ok(ca.iter().map(|opt| opt.unwrap_or(false)).collect())
}

// ==================== Writer Implementation ====================

fn parquet_writer_new(path: &str) -> Result<Box<ParquetWriter>, String> {
    Ok(Box::new(ParquetWriter {
        path: path.to_string(),
        columns: HashMap::new(),
        column_order: Vec::new(),
    }))
}

fn parquet_writer_add_i64_column(
    writer: &mut ParquetWriter,
    name: &str,
    data: &[i64],
) -> Result<(), String> {
    let series = Series::new(name.into(), data);
    writer.columns.insert(name.to_string(), series);
    writer.column_order.push(name.to_string());
    Ok(())
}

fn parquet_writer_add_i32_column(
    writer: &mut ParquetWriter,
    name: &str,
    data: &[i32],
) -> Result<(), String> {
    let series = Series::new(name.into(), data);
    writer.columns.insert(name.to_string(), series);
    writer.column_order.push(name.to_string());
    Ok(())
}

fn parquet_writer_add_f64_column(
    writer: &mut ParquetWriter,
    name: &str,
    data: &[f64],
) -> Result<(), String> {
    let series = Series::new(name.into(), data);
    writer.columns.insert(name.to_string(), series);
    writer.column_order.push(name.to_string());
    Ok(())
}

fn parquet_writer_add_f32_column(
    writer: &mut ParquetWriter,
    name: &str,
    data: &[f32],
) -> Result<(), String> {
    let series = Series::new(name.into(), data);
    writer.columns.insert(name.to_string(), series);
    writer.column_order.push(name.to_string());
    Ok(())
}

fn parquet_writer_add_string_column(
    writer: &mut ParquetWriter,
    name: &str,
    data: Vec<String>,
) -> Result<(), String> {
    let series = Series::new(name.into(), data);
    writer.columns.insert(name.to_string(), series);
    writer.column_order.push(name.to_string());
    Ok(())
}

fn parquet_writer_add_bool_column(
    writer: &mut ParquetWriter,
    name: &str,
    data: &[bool],
) -> Result<(), String> {
    let series = Series::new(name.into(), data);
    writer.columns.insert(name.to_string(), series);
    writer.column_order.push(name.to_string());
    Ok(())
}

// ==================== Query Builder Implementation ====================

/// Lazy query builder. Accumulates select/filter, executes on collect().
pub struct ParquetQuery {
    path: String,
    columns: Vec<String>,
    filters: Vec<Expr>,
}

fn make_filter_expr(column: &str, op: ffi::FilterOp, value: Expr) -> Expr {
    let c = col(column);
    if op == ffi::FilterOp::Eq {
        c.eq(value)
    } else if op == ffi::FilterOp::Ne {
        c.neq(value)
    } else if op == ffi::FilterOp::Lt {
        c.lt(value)
    } else if op == ffi::FilterOp::Le {
        c.lt_eq(value)
    } else if op == ffi::FilterOp::Gt {
        c.gt(value)
    } else if op == ffi::FilterOp::Ge {
        c.gt_eq(value)
    } else {
        unreachable!()
    }
}

fn parquet_query_new(path: &str) -> Result<Box<ParquetQuery>, String> {
    if !std::path::Path::new(path).exists() {
        return Err(format!("File not found: {}", path));
    }
    Ok(Box::new(ParquetQuery {
        path: path.to_string(),
        columns: Vec::new(),
        filters: Vec::new(),
    }))
}

fn parquet_query_select(query: &mut ParquetQuery, columns: Vec<String>) {
    query.columns = columns;
}

fn parquet_query_filter_i64(query: &mut ParquetQuery, column: &str, op: ffi::FilterOp, value: i64) {
    query.filters.push(make_filter_expr(column, op, lit(value)));
}

fn parquet_query_filter_i32(query: &mut ParquetQuery, column: &str, op: ffi::FilterOp, value: i32) {
    query.filters.push(make_filter_expr(column, op, lit(value)));
}

fn parquet_query_filter_f64(query: &mut ParquetQuery, column: &str, op: ffi::FilterOp, value: f64) {
    query.filters.push(make_filter_expr(column, op, lit(value)));
}

fn parquet_query_filter_f32(query: &mut ParquetQuery, column: &str, op: ffi::FilterOp, value: f32) {
    query.filters.push(make_filter_expr(column, op, lit(value)));
}

fn parquet_query_filter_str(
    query: &mut ParquetQuery,
    column: &str,
    op: ffi::FilterOp,
    value: &str,
) {
    query
        .filters
        .push(make_filter_expr(column, op, lit(value.to_string())));
}

fn parquet_query_filter_bool(
    query: &mut ParquetQuery,
    column: &str,
    op: ffi::FilterOp,
    value: bool,
) {
    query.filters.push(make_filter_expr(column, op, lit(value)));
}

fn execute_query(query: &ParquetQuery) -> Result<DataFrame, String> {
    let args = ScanArgsParquet::default();
    let mut lf = LazyFrame::scan_parquet(&query.path, args).map_err(|e| e.to_string())?;

    // Apply projection
    if !query.columns.is_empty() {
        let col_exprs: Vec<_> = query.columns.iter().map(|c| col(c.as_str())).collect();
        lf = lf.select(col_exprs);
    }

    // Apply filters (AND-ed together)
    for filter_expr in &query.filters {
        lf = lf.filter(filter_expr.clone());
    }

    lf.collect().map_err(|e| e.to_string())
}

fn parquet_query_collect(query: Box<ParquetQuery>) -> Result<Box<ParquetReader>, String> {
    let df = execute_query(&query)?;
    Ok(Box::new(ParquetReader { df }))
}

fn parquet_query_collect_df(query: Box<ParquetQuery>) -> Result<Box<ParquetDataFrame>, String> {
    let df = execute_query(&query)?;
    Ok(Box::new(ParquetDataFrame { df }))
}

fn parquet_writer_finish(writer: Box<ParquetWriter>) -> Result<(), String> {
    // Build DataFrame from columns in order
    let columns: Vec<Column> = writer
        .column_order
        .iter()
        .filter_map(|name| writer.columns.get(name).map(|s| s.clone().into()))
        .collect();

    if columns.is_empty() {
        return Err("No columns to write".to_string());
    }

    let mut df = DataFrame::new(columns).map_err(|e| e.to_string())?;

    PolarsWriter::new(&writer.path)
        .write(&mut df)
        .map_err(|e| e.to_string())
}
