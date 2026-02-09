//! CXX bridge for type-safe Rust-C++ interop.
//!
//! This module provides a safe FFI interface using the cxx crate.

use crate::parquet::{ParquetReader as PolarsReader, ParquetWriter as PolarsWriter};
use polars::prelude::*;
use std::collections::HashMap;

#[cxx::bridge(namespace = "basis::ffi")]
mod ffi {
    /// Column data type enum shared between Rust and C++.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ColumnType {
        Int64,
        Int32,
        Float64,
        Float32,
        String,
        Bool,
    }

    /// Information about a column in a Parquet file.
    #[derive(Debug, Clone)]
    struct ColumnInfo {
        name: String,
        dtype: ColumnType,
    }

    extern "Rust" {
        // Opaque Rust types
        type ParquetReader;
        type ParquetWriter;

        // Reader functions
        fn parquet_reader_open(path: &str) -> Result<Box<ParquetReader>>;
        fn parquet_reader_num_rows(reader: &ParquetReader) -> usize;
        fn parquet_reader_num_cols(reader: &ParquetReader) -> usize;
        fn parquet_reader_columns(reader: &ParquetReader) -> Vec<ColumnInfo>;

        // Column getters - return owned vectors
        fn parquet_reader_get_i64_column(
            reader: &ParquetReader,
            name: &str,
        ) -> Result<Vec<i64>>;
        fn parquet_reader_get_i32_column(
            reader: &ParquetReader,
            name: &str,
        ) -> Result<Vec<i32>>;
        fn parquet_reader_get_f64_column(
            reader: &ParquetReader,
            name: &str,
        ) -> Result<Vec<f64>>;
        fn parquet_reader_get_f32_column(
            reader: &ParquetReader,
            name: &str,
        ) -> Result<Vec<f32>>;
        fn parquet_reader_get_string_column(
            reader: &ParquetReader,
            name: &str,
        ) -> Result<Vec<String>>;
        fn parquet_reader_get_bool_column(
            reader: &ParquetReader,
            name: &str,
        ) -> Result<Vec<bool>>;

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
    }
}

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

// ==================== Reader Implementation ====================

fn parquet_reader_open(path: &str) -> Result<Box<ParquetReader>, String> {
    let df = PolarsReader::new(path)
        .read()
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
        .map(|col| {
            let dtype = match col.dtype() {
                DataType::Int64 => ffi::ColumnType::Int64,
                DataType::Int32 => ffi::ColumnType::Int32,
                DataType::Float64 => ffi::ColumnType::Float64,
                DataType::Float32 => ffi::ColumnType::Float32,
                DataType::String => ffi::ColumnType::String,
                DataType::Boolean => ffi::ColumnType::Bool,
                _ => ffi::ColumnType::String, // Fallback
            };
            ffi::ColumnInfo {
                name: col.name().to_string(),
                dtype,
            }
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

    Ok(ca
        .iter()
        .map(|opt| opt.unwrap_or("").to_string())
        .collect())
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
