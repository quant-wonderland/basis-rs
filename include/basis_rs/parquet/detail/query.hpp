#pragma once

// This header should be included from parquet.hpp after DataFrame is defined.
// Do not include this header directly.

#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <vector>

#include "cxx_bridge.rs.h"

namespace basis_rs {

// Forward declaration
class DataFrame;

/// Builder for creating DataFrame with optional filtering and column selection.
/// Filters are pushed down to the Parquet reader for efficiency.
class DataFrameBuilder {
 public:
  explicit DataFrameBuilder(std::filesystem::path path)
      : path_(std::move(path)) {}

  /// Select specific columns to read (projection pushdown)
  DataFrameBuilder& Select(std::initializer_list<std::string> names) {
    for (const auto& name : names) {
      select_names_.push_back(name);
    }
    return *this;
  }

  DataFrameBuilder& Select(const std::vector<std::string>& names) {
    select_names_.insert(select_names_.end(), names.begin(), names.end());
    return *this;
  }

  /// Filter by column name and value (predicate pushdown)
  DataFrameBuilder& Filter(const std::string& column, ffi::FilterOp op,
                           int64_t value) {
    filter_entries_.push_back(
        {column, op, [column, op, value](ffi::ParquetQuery& q) {
           ffi::parquet_query_filter_i64(q, column, op, value);
         }});
    return *this;
  }

  DataFrameBuilder& Filter(const std::string& column, ffi::FilterOp op,
                           int32_t value) {
    filter_entries_.push_back(
        {column, op, [column, op, value](ffi::ParquetQuery& q) {
           ffi::parquet_query_filter_i32(q, column, op, value);
         }});
    return *this;
  }

  DataFrameBuilder& Filter(const std::string& column, ffi::FilterOp op,
                           double value) {
    filter_entries_.push_back(
        {column, op, [column, op, value](ffi::ParquetQuery& q) {
           ffi::parquet_query_filter_f64(q, column, op, value);
         }});
    return *this;
  }

  DataFrameBuilder& Filter(const std::string& column, ffi::FilterOp op,
                           float value) {
    filter_entries_.push_back(
        {column, op, [column, op, value](ffi::ParquetQuery& q) {
           ffi::parquet_query_filter_f32(q, column, op, value);
         }});
    return *this;
  }

  DataFrameBuilder& Filter(const std::string& column, ffi::FilterOp op,
                           const std::string& value) {
    filter_entries_.push_back(
        {column, op, [column, op, value](ffi::ParquetQuery& q) {
           ffi::parquet_query_filter_str(q, column, op, value);
         }});
    return *this;
  }

  DataFrameBuilder& Filter(const std::string& column, ffi::FilterOp op,
                           bool value) {
    filter_entries_.push_back(
        {column, op, [column, op, value](ffi::ParquetQuery& q) {
           ffi::parquet_query_filter_bool(q, column, op, value);
         }});
    return *this;
  }

  /// Execute query and return DataFrame
  DataFrame Collect() const;

  /// Check if any filters are set
  bool HasFilters() const { return !filter_entries_.empty(); }

  /// Check if any column selection is set
  bool HasSelection() const { return !select_names_.empty(); }

  /// Get path
  const std::filesystem::path& path() const { return path_; }

  /// Get selected columns
  const std::vector<std::string>& selected_columns() const {
    return select_names_;
  }

 private:
  friend class DataFrame;

  struct FilterEntry {
    std::string column;
    ffi::FilterOp op;
    std::function<void(ffi::ParquetQuery&)> apply;
  };

  std::filesystem::path path_;
  std::vector<std::string> select_names_;
  std::vector<FilterEntry> filter_entries_;
};

}  // namespace basis_rs
