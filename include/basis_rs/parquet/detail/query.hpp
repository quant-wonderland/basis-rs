#pragma once

// This header should be included from parquet.hpp after DataFrame and ParquetCodec are defined.
// Do not include this header directly.

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <type_traits>
#include <vector>

#include "cxx_bridge.rs.h"

namespace basis_rs {

// ParquetCodec and GetParquetCodec are defined in codec.hpp (included before this)

/// Query builder for filtered Parquet reads with predicate pushdown.
template <typename RecordType>
class ParquetQuery {
 public:
  explicit ParquetQuery(std::filesystem::path path) : path_(std::move(path)) {}

  /// Select specific fields by member pointer (variadic).
  template <typename... MemberPtrs>
  ParquetQuery& Select(MemberPtrs... ptrs) {
    const auto& codec = GetParquetCodec<RecordType>();
    (select_names_.push_back(codec.FindColumnName(ptrs)), ...);
    return *this;
  }

  /// Select specific columns by name.
  ParquetQuery& Select(std::initializer_list<std::string> names) {
    for (const auto& name : names) {
      select_names_.push_back(name);
    }
    return *this;
  }

  /// Add a typed filter predicate using member pointer.
  template <typename T, typename SuperType>
  ParquetQuery& Filter(T SuperType::*accessor, ffi::FilterOp op,
                        const T& value) {
    const auto& codec = GetParquetCodec<RecordType>();
    std::string col_name = codec.FindColumnName(accessor);
    AddFilter(col_name, op, value);
    return *this;
  }

  /// Execute query and return records (uses new zero-copy API internally).
  std::vector<RecordType> Collect() const {
    auto query = ffi::parquet_query_new(path_.string());

    const auto& codec = GetParquetCodec<RecordType>();
    std::vector<std::string> effective_columns;
    std::vector<size_t> selected_indices;

    if (select_names_.empty()) {
      effective_columns = codec.column_names();
      for (size_t i = 0; i < effective_columns.size(); ++i) {
        selected_indices.push_back(i);
      }
    } else {
      effective_columns = select_names_;
      const auto& all_names = codec.column_names();
      for (const auto& sel : select_names_) {
        for (size_t i = 0; i < all_names.size(); ++i) {
          if (all_names[i] == sel) {
            selected_indices.push_back(i);
            break;
          }
        }
      }
    }

    // Include filter columns in scan projection
    std::vector<std::string> scan_columns = effective_columns;
    for (const auto& f : filter_entries_) {
      if (std::find(scan_columns.begin(), scan_columns.end(), f.column) ==
          scan_columns.end()) {
        scan_columns.push_back(f.column);
      }
    }

    // Set projection
    {
      rust::Vec<rust::String> cols;
      cols.reserve(scan_columns.size());
      for (const auto& name : scan_columns) {
        cols.push_back(rust::String(name));
      }
      ffi::parquet_query_select(*query, std::move(cols));
    }

    // Apply filters
    for (const auto& f : filter_entries_) {
      f.apply(*query);
    }

    // Collect into DataFrame (zero-copy)
    auto df_box = ffi::parquet_query_collect_df(std::move(query));

    // Wrap in our DataFrame and use codec to read
    // Note: We need to create a temporary DataFrame wrapper
    size_t num_rows = ffi::parquet_df_num_rows(*df_box);
    std::vector<RecordType> records(num_rows);

    // For now, use legacy reader for filtered queries
    // TODO: Implement zero-copy path for queries
    auto reader = ffi::parquet_query_new(path_.string());
    {
      rust::Vec<rust::String> cols;
      cols.reserve(scan_columns.size());
      for (const auto& name : scan_columns) {
        cols.push_back(rust::String(name));
      }
      ffi::parquet_query_select(*reader, std::move(cols));
    }
    for (const auto& f : filter_entries_) {
      f.apply(*reader);
    }
    auto legacy_reader = ffi::parquet_query_collect(std::move(reader));

    if (select_names_.empty()) {
      return codec.ReadAll(*legacy_reader);
    } else {
      return codec.ReadSelected(*legacy_reader, selected_indices);
    }
  }

 private:
  struct FilterEntry {
    std::string column;
    ffi::FilterOp op;
    std::function<void(ffi::ParquetQuery&)> apply;
  };

  template <typename T>
  void AddFilter(const std::string& col_name, ffi::FilterOp op,
                 const T& value) {
    FilterEntry entry;
    entry.column = col_name;
    entry.op = op;
    if constexpr (std::is_same_v<T, int64_t>) {
      entry.apply = [col_name, op, value](ffi::ParquetQuery& q) {
        ffi::parquet_query_filter_i64(q, col_name, op, value);
      };
    } else if constexpr (std::is_same_v<T, int32_t>) {
      entry.apply = [col_name, op, value](ffi::ParquetQuery& q) {
        ffi::parquet_query_filter_i32(q, col_name, op, value);
      };
    } else if constexpr (std::is_same_v<T, double>) {
      entry.apply = [col_name, op, value](ffi::ParquetQuery& q) {
        ffi::parquet_query_filter_f64(q, col_name, op, value);
      };
    } else if constexpr (std::is_same_v<T, float>) {
      entry.apply = [col_name, op, value](ffi::ParquetQuery& q) {
        ffi::parquet_query_filter_f32(q, col_name, op, value);
      };
    } else if constexpr (std::is_same_v<T, std::string>) {
      entry.apply = [col_name, op, value](ffi::ParquetQuery& q) {
        ffi::parquet_query_filter_str(q, col_name, op, value);
      };
    } else if constexpr (std::is_same_v<T, bool>) {
      entry.apply = [col_name, op, value](ffi::ParquetQuery& q) {
        ffi::parquet_query_filter_bool(q, col_name, op, value);
      };
    } else {
      static_assert(!std::is_same_v<T, T>, "Unsupported filter type");
    }
    filter_entries_.push_back(std::move(entry));
  }

  std::filesystem::path path_;
  std::vector<std::string> select_names_;
  std::vector<FilterEntry> filter_entries_;
};

}  // namespace basis_rs
