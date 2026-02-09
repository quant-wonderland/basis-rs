//! C FFI bindings for basis-rs Parquet functionality.
//!
//! This module provides C-compatible functions for reading and writing Parquet files.

use crate::basis::parquet::{ParquetReader, ParquetWriter};
use libc::{c_char, c_int, size_t};
use polars::prelude::*;
use std::ffi::{CStr, CString};
use std::ptr;

/// Error codes returned by FFI functions.
pub const BASIS_OK: c_int = 0;
pub const BASIS_ERR_NULL_PTR: c_int = -1;
pub const BASIS_ERR_INVALID_UTF8: c_int = -2;
pub const BASIS_ERR_IO: c_int = -3;
pub const BASIS_ERR_COLUMN_NOT_FOUND: c_int = -4;
pub const BASIS_ERR_TYPE_MISMATCH: c_int = -5;
pub const BASIS_ERR_POLARS: c_int = -6;

/// Opaque handle to a DataFrame.
pub struct BasisDataFrame {
    inner: DataFrame,
}

/// Result of reading an Int64 column.
#[repr(C)]
pub struct Int64Column {
    pub data: *mut i64,
    pub len: size_t,
}

/// Result of reading a Float64 column.
#[repr(C)]
pub struct Float64Column {
    pub data: *mut f64,
    pub len: size_t,
}

/// Result of reading a Bool column.
#[repr(C)]
pub struct BoolColumn {
    pub data: *mut bool,
    pub len: size_t,
}

/// Result of reading a String column.
#[repr(C)]
pub struct StringColumn {
    pub data: *mut *mut c_char,
    pub len: size_t,
}

use std::cell::RefCell;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_error(msg: &str) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).ok();
    });
}

/// Get the last error message. Returns NULL if no error.
/// The returned string is valid until the next FFI call.
#[no_mangle]
pub extern "C" fn basis_get_last_error() -> *const c_char {
    LAST_ERROR.with(|e| {
        e.borrow().as_ref().map_or(ptr::null(), |s| s.as_ptr())
    })
}

/// Clear the last error.
#[no_mangle]
pub extern "C" fn basis_clear_error() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

/// Read a Parquet file and return a DataFrame handle.
/// Returns NULL on error. Use `basis_get_last_error()` for details.
#[no_mangle]
pub extern "C" fn basis_parquet_read(path: *const c_char) -> *mut BasisDataFrame {
    if path.is_null() {
        set_error("path is null");
        return ptr::null_mut();
    }

    let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("invalid UTF-8 in path");
            return ptr::null_mut();
        }
    };

    match ParquetReader::new(path_str).read() {
        Ok(df) => Box::into_raw(Box::new(BasisDataFrame { inner: df })),
        Err(e) => {
            set_error(&e.to_string());
            ptr::null_mut()
        }
    }
}

/// Write a DataFrame to a Parquet file.
/// Returns BASIS_OK on success, negative error code on failure.
#[no_mangle]
pub extern "C" fn basis_parquet_write(df: *mut BasisDataFrame, path: *const c_char) -> c_int {
    if df.is_null() || path.is_null() {
        set_error("null pointer");
        return BASIS_ERR_NULL_PTR;
    }

    let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("invalid UTF-8 in path");
            return BASIS_ERR_INVALID_UTF8;
        }
    };

    let df_ref = unsafe { &mut (*df).inner };

    match ParquetWriter::new(path_str).write(df_ref) {
        Ok(()) => BASIS_OK,
        Err(e) => {
            set_error(&e.to_string());
            BASIS_ERR_IO
        }
    }
}

/// Create a new empty DataFrame.
#[no_mangle]
pub extern "C" fn basis_df_new() -> *mut BasisDataFrame {
    let df = DataFrame::empty();
    Box::into_raw(Box::new(BasisDataFrame { inner: df }))
}

/// Free a DataFrame handle.
#[no_mangle]
pub extern "C" fn basis_df_free(df: *mut BasisDataFrame) {
    if !df.is_null() {
        unsafe {
            drop(Box::from_raw(df));
        }
    }
}

/// Get the number of rows in the DataFrame.
#[no_mangle]
pub extern "C" fn basis_df_height(df: *const BasisDataFrame) -> size_t {
    if df.is_null() {
        return 0;
    }
    unsafe { (*df).inner.height() }
}

/// Get the number of columns in the DataFrame.
#[no_mangle]
pub extern "C" fn basis_df_width(df: *const BasisDataFrame) -> size_t {
    if df.is_null() {
        return 0;
    }
    unsafe { (*df).inner.width() }
}

/// Get an Int64 column by name.
/// The caller must free the returned data with `basis_int64_column_free`.
#[no_mangle]
pub extern "C" fn basis_df_get_int64_column(
    df: *const BasisDataFrame,
    name: *const c_char,
    out: *mut Int64Column,
) -> c_int {
    if df.is_null() || name.is_null() || out.is_null() {
        set_error("null pointer");
        return BASIS_ERR_NULL_PTR;
    }

    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("invalid UTF-8 in column name");
            return BASIS_ERR_INVALID_UTF8;
        }
    };

    let df_ref = unsafe { &(*df).inner };

    let col = match df_ref.column(name_str) {
        Ok(c) => c,
        Err(_) => {
            set_error(&format!("column '{}' not found", name_str));
            return BASIS_ERR_COLUMN_NOT_FOUND;
        }
    };

    let i64_col = match col.i64() {
        Ok(c) => c,
        Err(_) => {
            set_error(&format!("column '{}' is not Int64", name_str));
            return BASIS_ERR_TYPE_MISMATCH;
        }
    };

    let len = i64_col.len();
    let mut data: Vec<i64> = Vec::with_capacity(len);

    for opt_val in i64_col.iter() {
        data.push(opt_val.unwrap_or(0));
    }

    let mut boxed = data.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    unsafe {
        (*out).data = ptr;
        (*out).len = len;
    }

    BASIS_OK
}

/// Free an Int64Column's data.
#[no_mangle]
pub extern "C" fn basis_int64_column_free(col: *mut Int64Column) {
    if col.is_null() {
        return;
    }
    unsafe {
        let col_ref = &mut *col;
        if !col_ref.data.is_null() && col_ref.len > 0 {
            drop(Vec::from_raw_parts(col_ref.data, col_ref.len, col_ref.len));
            col_ref.data = ptr::null_mut();
            col_ref.len = 0;
        }
    }
}

/// Get a Float64 column by name.
/// The caller must free the returned data with `basis_float64_column_free`.
#[no_mangle]
pub extern "C" fn basis_df_get_float64_column(
    df: *const BasisDataFrame,
    name: *const c_char,
    out: *mut Float64Column,
) -> c_int {
    if df.is_null() || name.is_null() || out.is_null() {
        set_error("null pointer");
        return BASIS_ERR_NULL_PTR;
    }

    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("invalid UTF-8 in column name");
            return BASIS_ERR_INVALID_UTF8;
        }
    };

    let df_ref = unsafe { &(*df).inner };

    let col = match df_ref.column(name_str) {
        Ok(c) => c,
        Err(_) => {
            set_error(&format!("column '{}' not found", name_str));
            return BASIS_ERR_COLUMN_NOT_FOUND;
        }
    };

    let f64_col = match col.f64() {
        Ok(c) => c,
        Err(_) => {
            set_error(&format!("column '{}' is not Float64", name_str));
            return BASIS_ERR_TYPE_MISMATCH;
        }
    };

    let len = f64_col.len();
    let mut data: Vec<f64> = Vec::with_capacity(len);

    for opt_val in f64_col.iter() {
        data.push(opt_val.unwrap_or(0.0));
    }

    let mut boxed = data.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    unsafe {
        (*out).data = ptr;
        (*out).len = len;
    }

    BASIS_OK
}

/// Free a Float64Column's data.
#[no_mangle]
pub extern "C" fn basis_float64_column_free(col: *mut Float64Column) {
    if col.is_null() {
        return;
    }
    unsafe {
        let col_ref = &mut *col;
        if !col_ref.data.is_null() && col_ref.len > 0 {
            drop(Vec::from_raw_parts(col_ref.data, col_ref.len, col_ref.len));
            col_ref.data = ptr::null_mut();
            col_ref.len = 0;
        }
    }
}

/// Get a String column by name.
/// The caller must free the returned data with `basis_string_column_free`.
#[no_mangle]
pub extern "C" fn basis_df_get_string_column(
    df: *const BasisDataFrame,
    name: *const c_char,
    out: *mut StringColumn,
) -> c_int {
    if df.is_null() || name.is_null() || out.is_null() {
        set_error("null pointer");
        return BASIS_ERR_NULL_PTR;
    }

    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("invalid UTF-8 in column name");
            return BASIS_ERR_INVALID_UTF8;
        }
    };

    let df_ref = unsafe { &(*df).inner };

    let col = match df_ref.column(name_str) {
        Ok(c) => c,
        Err(_) => {
            set_error(&format!("column '{}' not found", name_str));
            return BASIS_ERR_COLUMN_NOT_FOUND;
        }
    };

    let str_col = match col.str() {
        Ok(c) => c,
        Err(_) => {
            set_error(&format!("column '{}' is not String", name_str));
            return BASIS_ERR_TYPE_MISMATCH;
        }
    };

    let len = str_col.len();
    let mut data: Vec<*mut c_char> = Vec::with_capacity(len);

    for opt_val in str_col.iter() {
        let c_str = match opt_val {
            Some(s) => CString::new(s).unwrap_or_default().into_raw(),
            None => CString::new("").unwrap().into_raw(),
        };
        data.push(c_str);
    }

    let mut boxed = data.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    unsafe {
        (*out).data = ptr;
        (*out).len = len;
    }

    BASIS_OK
}

/// Free a StringColumn's data.
#[no_mangle]
pub extern "C" fn basis_string_column_free(col: *mut StringColumn) {
    if col.is_null() {
        return;
    }
    unsafe {
        let col_ref = &mut *col;
        if !col_ref.data.is_null() && col_ref.len > 0 {
            let strings = Vec::from_raw_parts(col_ref.data, col_ref.len, col_ref.len);
            for s in strings {
                if !s.is_null() {
                    drop(CString::from_raw(s));
                }
            }
            col_ref.data = ptr::null_mut();
            col_ref.len = 0;
        }
    }
}

/// Add an Int64 column to the DataFrame.
#[no_mangle]
pub extern "C" fn basis_df_add_int64_column(
    df: *mut BasisDataFrame,
    name: *const c_char,
    data: *const i64,
    len: size_t,
) -> c_int {
    if df.is_null() || name.is_null() || (data.is_null() && len > 0) {
        set_error("null pointer");
        return BASIS_ERR_NULL_PTR;
    }

    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("invalid UTF-8 in column name");
            return BASIS_ERR_INVALID_UTF8;
        }
    };

    let slice = if len > 0 {
        unsafe { std::slice::from_raw_parts(data, len) }
    } else {
        &[]
    };

    let series = Series::new(name_str.into(), slice);
    let df_ref = unsafe { &mut (*df).inner };

    match df_ref.with_column(series) {
        Ok(_) => BASIS_OK,
        Err(e) => {
            set_error(&e.to_string());
            BASIS_ERR_POLARS
        }
    }
}

/// Add a Float64 column to the DataFrame.
#[no_mangle]
pub extern "C" fn basis_df_add_float64_column(
    df: *mut BasisDataFrame,
    name: *const c_char,
    data: *const f64,
    len: size_t,
) -> c_int {
    if df.is_null() || name.is_null() || (data.is_null() && len > 0) {
        set_error("null pointer");
        return BASIS_ERR_NULL_PTR;
    }

    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("invalid UTF-8 in column name");
            return BASIS_ERR_INVALID_UTF8;
        }
    };

    let slice = if len > 0 {
        unsafe { std::slice::from_raw_parts(data, len) }
    } else {
        &[]
    };

    let series = Series::new(name_str.into(), slice);
    let df_ref = unsafe { &mut (*df).inner };

    match df_ref.with_column(series) {
        Ok(_) => BASIS_OK,
        Err(e) => {
            set_error(&e.to_string());
            BASIS_ERR_POLARS
        }
    }
}

/// Add a String column to the DataFrame.
#[no_mangle]
pub extern "C" fn basis_df_add_string_column(
    df: *mut BasisDataFrame,
    name: *const c_char,
    data: *const *const c_char,
    len: size_t,
) -> c_int {
    if df.is_null() || name.is_null() || (data.is_null() && len > 0) {
        set_error("null pointer");
        return BASIS_ERR_NULL_PTR;
    }

    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("invalid UTF-8 in column name");
            return BASIS_ERR_INVALID_UTF8;
        }
    };

    let mut strings: Vec<String> = Vec::with_capacity(len);

    for i in 0..len {
        let c_str_ptr = unsafe { *data.add(i) };
        if c_str_ptr.is_null() {
            strings.push(String::new());
        } else {
            match unsafe { CStr::from_ptr(c_str_ptr) }.to_str() {
                Ok(s) => strings.push(s.to_string()),
                Err(_) => {
                    set_error(&format!("invalid UTF-8 in string at index {}", i));
                    return BASIS_ERR_INVALID_UTF8;
                }
            }
        }
    }

    let series = Series::new(name_str.into(), strings);
    let df_ref = unsafe { &mut (*df).inner };

    match df_ref.with_column(series) {
        Ok(_) => BASIS_OK,
        Err(e) => {
            set_error(&e.to_string());
            BASIS_ERR_POLARS
        }
    }
}

/// Add a Bool column to the DataFrame.
#[no_mangle]
pub extern "C" fn basis_df_add_bool_column(
    df: *mut BasisDataFrame,
    name: *const c_char,
    data: *const bool,
    len: size_t,
) -> c_int {
    if df.is_null() || name.is_null() || (data.is_null() && len > 0) {
        set_error("null pointer");
        return BASIS_ERR_NULL_PTR;
    }

    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("invalid UTF-8 in column name");
            return BASIS_ERR_INVALID_UTF8;
        }
    };

    let slice = if len > 0 {
        unsafe { std::slice::from_raw_parts(data, len) }
    } else {
        &[]
    };

    let series = Series::new(name_str.into(), slice);
    let df_ref = unsafe { &mut (*df).inner };

    match df_ref.with_column(series) {
        Ok(_) => BASIS_OK,
        Err(e) => {
            set_error(&e.to_string());
            BASIS_ERR_POLARS
        }
    }
}

/// Get a Bool column by name.
/// The caller must free the returned data with `basis_bool_column_free`.
#[no_mangle]
pub extern "C" fn basis_df_get_bool_column(
    df: *const BasisDataFrame,
    name: *const c_char,
    out: *mut BoolColumn,
) -> c_int {
    if df.is_null() || name.is_null() || out.is_null() {
        set_error("null pointer");
        return BASIS_ERR_NULL_PTR;
    }

    let name_str = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("invalid UTF-8 in column name");
            return BASIS_ERR_INVALID_UTF8;
        }
    };

    let df_ref = unsafe { &(*df).inner };

    let col = match df_ref.column(name_str) {
        Ok(c) => c,
        Err(_) => {
            set_error(&format!("column '{}' not found", name_str));
            return BASIS_ERR_COLUMN_NOT_FOUND;
        }
    };

    let bool_col = match col.bool() {
        Ok(c) => c,
        Err(_) => {
            set_error(&format!("column '{}' is not Bool", name_str));
            return BASIS_ERR_TYPE_MISMATCH;
        }
    };

    let len = bool_col.len();
    let mut data: Vec<bool> = Vec::with_capacity(len);

    for opt_val in bool_col.iter() {
        data.push(opt_val.unwrap_or(false));
    }

    let mut boxed = data.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    unsafe {
        (*out).data = ptr;
        (*out).len = len;
    }

    BASIS_OK
}

/// Free a BoolColumn's data.
#[no_mangle]
pub extern "C" fn basis_bool_column_free(col: *mut BoolColumn) {
    if col.is_null() {
        return;
    }
    unsafe {
        let col_ref = &mut *col;
        if !col_ref.data.is_null() && col_ref.len > 0 {
            drop(Vec::from_raw_parts(col_ref.data, col_ref.len, col_ref.len));
            col_ref.data = ptr::null_mut();
            col_ref.len = 0;
        }
    }
}
