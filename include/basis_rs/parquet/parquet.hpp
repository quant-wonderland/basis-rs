#pragma once

/**
 * Type-safe C++ wrapper for basis-rs Parquet functionality.
 *
 * ## DataFrame API
 *
 * DataFrame provides zero-copy column access to Parquet data:
 *
 *   // Simple open
 *   basis_rs::DataFrame df("data.parquet");
 *
 *   // With column projection and filtering
 *   auto df = basis_rs::DataFrame::Open("data.parquet")
 *       .Select({"Close", "High", "Low"})
 *       .Filter("Close", basis_rs::Gt, 10.0f)
 *       .Collect();
 *
 *   // Zero-copy column iteration
 *   auto close_col = df.GetColumn<float>("Close");
 *   for (float value : close_col) {
 *       sum += value;
 *   }
 *   // Index-based access
 *   for (size_t i = 0; i < close_col.size(); ++i) {
 *       process(close_col[i]);
 *   }
 *
 *   // Or use ReadAllAs<T> for convenience (copies data to structs)
 *   auto records = df.ReadAllAs<TickData>();
 *
 * ## ParquetWriter
 *
 * For writing struct records to Parquet files:
 *
 *   basis_rs::ParquetWriter<TickData> writer("output.parquet");
 *   writer.WriteRecord({123, 45.6f});
 *   writer.Finish();
 */

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

// Include CXX-generated header
#include "cxx_bridge.rs.h"

// Include internal detail headers
#include "detail/column_accessor.hpp"
#include "detail/type_traits.hpp"

namespace basis_rs {

// Forward declarations
template <typename RecordType>
class ParquetCodec;

template <typename RecordType>
const ParquetCodec<RecordType>& GetParquetCodec();

template <typename T>
struct ParquetCellCodec;

class DataFrameBuilder;

/// Zero-copy DataFrame wrapper. Provides direct access to Parquet column data.
class DataFrame {
 public:
  /// Open a Parquet file (reads all columns)
  explicit DataFrame(const std::filesystem::path& path)
      : df_(ffi::parquet_open(path.string())) {}

  /// Open with column projection (only read specified columns)
  DataFrame(const std::filesystem::path& path,
            const std::vector<std::string>& columns)
      : df_(OpenProjected(path, columns)) {}

  /// Start building a DataFrame with Select/Filter options
  static DataFrameBuilder Open(const std::filesystem::path& path);

  /// Move constructor
  DataFrame(DataFrame&&) = default;
  DataFrame& operator=(DataFrame&&) = default;

  // Non-copyable (owns the Rust DataFrame)
  DataFrame(const DataFrame&) = delete;
  DataFrame& operator=(const DataFrame&) = delete;

  /// Number of rows
  size_t NumRows() const { return ffi::parquet_df_num_rows(*df_); }

  /// Number of columns
  size_t NumCols() const { return ffi::parquet_df_num_cols(*df_); }

  /// Get column info
  std::vector<ffi::ColumnInfo> Columns() const {
    auto rust_vec = ffi::parquet_df_columns(*df_);
    return std::vector<ffi::ColumnInfo>(rust_vec.begin(), rust_vec.end());
  }

  /// Rechunk for single contiguous buffer per column.
  /// This is optional - most operations work with multiple chunks.
  /// Returns true if rechunking was needed.
  bool Rechunk() { return ffi::parquet_df_rechunk(*df_); }

  /// Get column as typed accessor (zero-copy for primitive types)
  template <typename T>
  ColumnAccessor<T> GetColumn(const std::string& name) const;

  /// Get string column (requires allocation)
  std::vector<std::string> GetStringColumn(const std::string& name) const {
    auto rust_vec = ffi::parquet_df_get_string_column(*df_, name);
    std::vector<std::string> result;
    result.reserve(rust_vec.size());
    for (const auto& s : rust_vec) {
      result.emplace_back(std::string(s));
    }
    return result;
  }

  /// Read all rows as struct records using codec.
  /// This copies data into structs - use GetColumn for zero-copy access.
  template <typename RecordType>
  std::vector<RecordType> ReadAllAs() const;

  /// Access underlying FFI handle (for advanced use)
  const ffi::ParquetDataFrame& Handle() const { return *df_; }
  ffi::ParquetDataFrame& Handle() { return *df_; }

 private:
  friend class DataFrameBuilder;

  /// Private constructor from FFI handle (used by DataFrameBuilder)
  explicit DataFrame(rust::Box<ffi::ParquetDataFrame> df)
      : df_(std::move(df)) {}

  static rust::Box<ffi::ParquetDataFrame> OpenProjected(
      const std::filesystem::path& path,
      const std::vector<std::string>& columns) {
    rust::Vec<rust::String> cols;
    cols.reserve(columns.size());
    for (const auto& c : columns) {
      cols.push_back(rust::String(c));
    }
    return ffi::parquet_open_projected(path.string(), std::move(cols));
  }

  rust::Box<ffi::ParquetDataFrame> df_;
};

// Template specializations for GetColumn
template <>
inline ColumnAccessor<int64_t> DataFrame::GetColumn<int64_t>(
    const std::string& name) const {
  auto chunks = ffi::parquet_df_get_i64_chunks(*df_, name);
  ColumnAccessor<int64_t> accessor;
  for (const auto& chunk : chunks) {
    accessor.AddChunk(reinterpret_cast<const int64_t*>(chunk.ptr), chunk.len);
  }
  return accessor;
}

template <>
inline ColumnAccessor<int32_t> DataFrame::GetColumn<int32_t>(
    const std::string& name) const {
  auto chunks = ffi::parquet_df_get_i32_chunks(*df_, name);
  ColumnAccessor<int32_t> accessor;
  for (const auto& chunk : chunks) {
    accessor.AddChunk(reinterpret_cast<const int32_t*>(chunk.ptr), chunk.len);
  }
  return accessor;
}

template <>
inline ColumnAccessor<uint64_t> DataFrame::GetColumn<uint64_t>(
    const std::string& name) const {
  auto chunks = ffi::parquet_df_get_u64_chunks(*df_, name);
  ColumnAccessor<uint64_t> accessor;
  for (const auto& chunk : chunks) {
    accessor.AddChunk(reinterpret_cast<const uint64_t*>(chunk.ptr), chunk.len);
  }
  return accessor;
}

template <>
inline ColumnAccessor<double> DataFrame::GetColumn<double>(
    const std::string& name) const {
  auto chunks = ffi::parquet_df_get_f64_chunks(*df_, name);
  ColumnAccessor<double> accessor;
  for (const auto& chunk : chunks) {
    accessor.AddChunk(reinterpret_cast<const double*>(chunk.ptr), chunk.len);
  }
  return accessor;
}

template <>
inline ColumnAccessor<float> DataFrame::GetColumn<float>(
    const std::string& name) const {
  auto chunks = ffi::parquet_df_get_f32_chunks(*df_, name);
  ColumnAccessor<float> accessor;
  for (const auto& chunk : chunks) {
    accessor.AddChunk(reinterpret_cast<const float*>(chunk.ptr), chunk.len);
  }
  return accessor;
}

// DateTime is stored as int64_t milliseconds
inline ColumnAccessor<int64_t> GetDateTimeColumn(const DataFrame& df,
                                                  const std::string& name) {
  auto chunks = ffi::parquet_df_get_datetime_chunks(df.Handle(), name);
  ColumnAccessor<int64_t> accessor;
  for (const auto& chunk : chunks) {
    accessor.AddChunk(reinterpret_cast<const int64_t*>(chunk.ptr), chunk.len);
  }
  return accessor;
}

}  // namespace basis_rs

// Include remaining detail headers that depend on DataFrame
#include "detail/cell_codec.hpp"
#include "detail/codec.hpp"
#include "detail/query.hpp"

namespace basis_rs {

// ==================== DataFrame method implementations ====================

inline DataFrameBuilder DataFrame::Open(const std::filesystem::path& path) {
  return DataFrameBuilder(path);
}

inline DataFrame DataFrameBuilder::Collect() const {
  if (filter_entries_.empty()) {
    // No filters - use simple open
    if (select_names_.empty()) {
      return DataFrame(ffi::parquet_open(path_.string()));
    } else {
      rust::Vec<rust::String> cols;
      cols.reserve(select_names_.size());
      for (const auto& c : select_names_) {
        cols.push_back(rust::String(c));
      }
      return DataFrame(ffi::parquet_open_projected(path_.string(), std::move(cols)));
    }
  }

  // Has filters - use query API
  auto query = ffi::parquet_query_new(path_.string());

  // Set projection only if user explicitly selected columns
  if (!select_names_.empty()) {
    // Include filter columns in projection to ensure filter works
    std::vector<std::string> scan_columns = select_names_;
    for (const auto& f : filter_entries_) {
      if (std::find(scan_columns.begin(), scan_columns.end(), f.column) ==
          scan_columns.end()) {
        scan_columns.push_back(f.column);
      }
    }

    rust::Vec<rust::String> cols;
    cols.reserve(scan_columns.size());
    for (const auto& name : scan_columns) {
      cols.push_back(rust::String(name));
    }
    ffi::parquet_query_select(*query, std::move(cols));
  }
  // If no Select() was called, read all columns (no projection)

  // Apply filters
  for (const auto& f : filter_entries_) {
    f.apply(*query);
  }

  // Collect into DataFrame
  return DataFrame(ffi::parquet_query_collect_df(std::move(query)));
}

template <typename RecordType>
std::vector<RecordType> DataFrame::ReadAllAs() const {
  const auto& codec = GetParquetCodec<RecordType>();
  return codec.ReadAllFromDf(*this);
}

// ==================== FilterOp Aliases ====================

inline constexpr auto Eq = ffi::FilterOp::Eq;
inline constexpr auto Ne = ffi::FilterOp::Ne;
inline constexpr auto Lt = ffi::FilterOp::Lt;
inline constexpr auto Le = ffi::FilterOp::Le;
inline constexpr auto Gt = ffi::FilterOp::Gt;
inline constexpr auto Ge = ffi::FilterOp::Ge;

// ==================== ParquetWriter ====================

template <typename RecordType>
class ParquetWriter {
 public:
  explicit ParquetWriter(std::filesystem::path path)
      : path_(std::move(path)) {}

  ParquetWriter(ParquetWriter&& other) noexcept
      : path_(std::move(other.path_)),
        buffer_(std::move(other.buffer_)),
        compression_(std::move(other.compression_)),
        row_group_size_(other.row_group_size_),
        writer_(std::move(other.writer_)),
        finalized_(other.finalized_) {
    other.finalized_ = true;
  }

  // Destructor attempts best-effort flush. Exceptions are silently swallowed
  // because destructors must be noexcept. Call Finish() explicitly to handle errors.
  ~ParquetWriter() {
    if (!finalized_ && (!buffer_.empty() || writer_)) {
      try {
        Finish();
      } catch (...) {
      }
    }
  }

  /// Set compression algorithm ("zstd", "snappy", "uncompressed", "lz4", "gzip").
  ParquetWriter& WithCompression(std::string compression) {
    compression_ = std::move(compression);
    return *this;
  }

  /// Set row group size. When > 0, enables streaming: auto-flushes when buffer reaches this size.
  ParquetWriter& WithRowGroupSize(size_t size) {
    row_group_size_ = size;
    return *this;
  }

  void WriteRecord(const RecordType& record) {
    buffer_.push_back(record);
    MaybeFlush();
  }

  void WriteRecords(const std::vector<RecordType>& records) {
    buffer_.insert(buffer_.end(), records.begin(), records.end());
    MaybeFlush();
  }

  void Finish() {
    if (finalized_) return;
    if (!buffer_.empty()) FlushBatch();
    if (writer_) ffi::parquet_writer_finish(std::move(*writer_));
    writer_.reset();
    finalized_ = true;
  }

  void Discard() {
    buffer_.clear();
    writer_.reset();
    finalized_ = true;
  }

  size_t BufferSize() const { return buffer_.size(); }

 private:
  void MaybeFlush() {
    if (row_group_size_ > 0 && buffer_.size() >= row_group_size_) {
      FlushBatch();
    }
  }

  void FlushBatch() {
    if (buffer_.empty()) return;
    EnsureWriter();
    GetParquetCodec<RecordType>().WriteAll(**writer_, buffer_);
    ffi::parquet_writer_write_batch(**writer_);
    buffer_.clear();
  }

  void EnsureWriter() {
    if (!writer_) {
      writer_ = std::make_unique<rust::Box<ffi::ParquetWriter>>(
          ffi::parquet_writer_new(path_.string(), compression_,
                                  row_group_size_));
    }
  }

  std::filesystem::path path_;
  std::vector<RecordType> buffer_;
  std::string compression_ = "zstd";
  size_t row_group_size_ = 0;
  std::unique_ptr<rust::Box<ffi::ParquetWriter>> writer_;
  bool finalized_ = false;
};

// ==================== ColumnarParquetWriter ====================

/// High-performance columnar writer. Accepts SoA column data directly,
/// eliminating AoS→SoA extraction overhead. ~42% faster than ParquetWriter<T>
/// for users who already have columnar data.
///
/// Usage:
///   ColumnarParquetWriter writer("out.parquet");
///   writer.WithCompression("zstd").WithRowGroupSize(500000);
///   writer.AddColumn("StockId", ids.data(), ids.size());
///   writer.AddColumn("Close", prices.data(), prices.size());
///   writer.WriteBatch();
///   writer.Finish();
///
/// Column data pointers must remain valid until WriteBatch() is called.
class ColumnarParquetWriter {
 public:
  explicit ColumnarParquetWriter(std::filesystem::path path)
      : path_(std::move(path)) {}

  ColumnarParquetWriter(ColumnarParquetWriter&& other) noexcept
      : path_(std::move(other.path_)),
        compression_(std::move(other.compression_)),
        row_group_size_(other.row_group_size_),
        pending_(std::move(other.pending_)),
        writer_(std::move(other.writer_)),
        finalized_(other.finalized_) {
    other.finalized_ = true;
  }

  ~ColumnarParquetWriter() {
    if (!finalized_ && (!pending_.empty() || writer_)) {
      try { Finish(); } catch (...) {}
    }
  }

  ColumnarParquetWriter& WithCompression(std::string compression) {
    compression_ = std::move(compression);
    return *this;
  }

  ColumnarParquetWriter& WithRowGroupSize(size_t size) {
    row_group_size_ = size;
    return *this;
  }

  // Primitive column types — zero-copy, data pointer must stay valid until WriteBatch()
  void AddColumn(const std::string& name, const int32_t* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_i32_column_zerocopy(w, name, rust::Slice<const int32_t>(data, len));
    });
  }

  void AddColumn(const std::string& name, const int64_t* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_i64_column_zerocopy(w, name, rust::Slice<const int64_t>(data, len));
    });
  }

  void AddColumn(const std::string& name, const float* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_f32_column_zerocopy(w, name, rust::Slice<const float>(data, len));
    });
  }

  void AddColumn(const std::string& name, const double* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_f64_column_zerocopy(w, name, rust::Slice<const double>(data, len));
    });
  }

  void AddColumn(const std::string& name, const bool* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_bool_column(w, name, rust::Slice<const bool>(data, len));
    });
  }

  void AddColumn(const std::string& name, const std::vector<std::string>& data) {
    const auto* ptr = &data;
    pending_.push_back([name, ptr](ffi::ParquetWriter& w) {
      ParquetCellCodec<std::string>::Write(w, name, *ptr);
    });
  }

  void AddDateTimeColumn(const std::string& name, const int64_t* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_datetime_column_zerocopy(w, name, rust::Slice<const int64_t>(data, len));
    });
  }

  template <AbseilCivilTime T>
  void AddDateTimeColumn(const std::string& name, const std::vector<T>& data) {
    const auto* ptr = &data;
    pending_.push_back([name, ptr](ffi::ParquetWriter& w) {
      ParquetCellCodec<T>::Write(w, name, *ptr);
    });
  }

  void WriteBatch() {
    if (pending_.empty()) return;
    EnsureWriter();
    for (const auto& col : pending_) col(**writer_);
    ffi::parquet_writer_write_batch(**writer_);
    pending_.clear();
  }

  void Finish() {
    if (finalized_) return;
    if (!pending_.empty()) WriteBatch();
    if (writer_) ffi::parquet_writer_finish(std::move(*writer_));
    writer_.reset();
    finalized_ = true;
  }

  void Discard() {
    pending_.clear();
    writer_.reset();
    finalized_ = true;
  }

 private:
  void EnsureWriter() {
    if (!writer_) {
      writer_ = std::make_unique<rust::Box<ffi::ParquetWriter>>(
          ffi::parquet_writer_new(path_.string(), compression_, row_group_size_));
    }
  }

  std::filesystem::path path_;
  std::string compression_ = "zstd";
  size_t row_group_size_ = 0;
  std::vector<std::function<void(ffi::ParquetWriter&)>> pending_;
  std::unique_ptr<rust::Box<ffi::ParquetWriter>> writer_;
  bool finalized_ = false;
};

}  // namespace basis_rs
