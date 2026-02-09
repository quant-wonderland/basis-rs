//! basis-rs: A collection of Rust utilities
//!
//! This crate provides various data processing utilities.

#[path = "../basis/mod.rs"]
pub mod basis;

pub mod cxx_bridge;
pub mod ffi;

// Re-export commonly used items
pub use basis::parquet::{ParquetError, ParquetReader, ParquetWriter};
