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
///
/// DataFrame supports three access patterns:
/// 1. Zero-copy column access via GetColumn<T>() - fastest, no memory allocation
/// 2. Struct-based access via ReadAllAs<T>() - convenient, copies data into structs
/// 3. String column access via GetStringColumn() - requires allocation due to variable length
///
/// The DataFrame is move-only and owns the underlying Rust DataFrame.
class DataFrame {
 public:
  /// Open a Parquet file and read all columns.
  ///
  /// Example:
  ///   DataFrame df("data.parquet");
  ///   std::cout << df.NumRows() << " rows\n";
  explicit DataFrame(const std::filesystem::path& path)
      : df_(ffi::parquet_open(path.string())) {}

  /// Open a Parquet file with column projection (only reads specified columns from disk).
  ///
  /// This is more efficient than reading all columns when you only need a subset.
  ///
  /// Example:
  ///   DataFrame df("data.parquet", {"id", "price", "volume"});
  DataFrame(const std::filesystem::path& path,
            const std::vector<std::string>& columns)
      : df_(OpenProjected(path, columns)) {}

  /// Start building a DataFrame with Select/Filter options.
  ///
  /// Use this for advanced queries with predicate pushdown:
  ///   auto df = DataFrame::Open("data.parquet")
  ///       .Select({"id", "price"})
  ///       .Filter("price", Gt, 100.0)
  ///       .Collect();
  static DataFrameBuilder Open(const std::filesystem::path& path);

  /// Move constructor
  DataFrame(DataFrame&&) = default;
  DataFrame& operator=(DataFrame&&) = default;

  // Non-copyable (owns the Rust DataFrame)
  DataFrame(const DataFrame&) = delete;
  DataFrame& operator=(const DataFrame&) = delete;

  /// Returns the number of rows in the DataFrame.
  size_t NumRows() const { return ffi::parquet_df_num_rows(*df_); }

  /// Returns the number of columns in the DataFrame.
  size_t NumCols() const { return ffi::parquet_df_num_cols(*df_); }

  /// Returns metadata for all columns (name and data type).
  std::vector<ffi::ColumnInfo> Columns() const {
    auto rust_vec = ffi::parquet_df_columns(*df_);
    return std::vector<ffi::ColumnInfo>(rust_vec.begin(), rust_vec.end());
  }

  /// Rechunk all columns to have a single contiguous buffer.
  ///
  /// Parquet files are organized into row groups, and each row group becomes a separate
  /// chunk in memory. Most operations work efficiently with multiple chunks:
  /// - Range-for iteration: O(1) per element, chunk crossing is rare
  /// - Index access: O(log n) binary search to find chunk, then O(1) access
  ///
  /// When to use Rechunk():
  /// - You need a raw pointer to contiguous memory (e.g., passing to C APIs)
  /// - Heavy random index access patterns (eliminates O(log n) chunk lookup)
  /// - Interfacing with libraries that require contiguous arrays
  ///
  /// When NOT to use Rechunk():
  /// - Sequential iteration (range-for loops) - already optimal
  /// - Large files - rechunking allocates and copies all data (expensive)
  /// - Memory-constrained environments - doubles peak memory usage temporarily
  ///
  /// Returns true if rechunking was performed, false if already single-chunked.
  bool Rechunk() { return ffi::parquet_df_rechunk(*df_); }

  /// Get a column as a typed accessor for zero-copy iteration.
  ///
  /// Supported types: int32_t, int64_t, uint64_t, float, double
  /// For DateTime columns, use GetDateTimeColumn() instead.
  /// For string columns, use GetStringColumn().
  ///
  /// Example:
  ///   auto prices = df.GetColumn<float>("price");
  ///   for (float p : prices) { sum += p; }
  ///
  /// The returned accessor is valid as long as the DataFrame exists.
  template <typename T>
  ColumnAccessor<T> GetColumn(const std::string& name) const;

  /// Get a string column (requires allocation due to variable-length strings).
  ///
  /// Example:
  ///   auto symbols = df.GetStringColumn("symbol");
  ///   for (const auto& s : symbols) { std::cout << s << "\n"; }
  std::vector<std::string> GetStringColumn(const std::string& name) const {
    auto rust_vec = ffi::parquet_df_get_string_column(*df_, name);
    std::vector<std::string> result;
    result.reserve(rust_vec.size());
    for (const auto& s : rust_vec) {
      result.emplace_back(std::string(s));
    }
    return result;
  }

  /// Read all rows as struct records using the registered ParquetCodec.
  ///
  /// This copies data from columnar format into row-oriented structs.
  /// Only columns registered in the codec are read from disk (automatic projection).
  ///
  /// Example:
  ///   auto trades = df.ReadAllAs<Trade>();
  ///   for (const auto& t : trades) { process(t); }
  ///
  /// For zero-copy access, use GetColumn<T>() instead.
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

/// Get a DateTime column as int64_t milliseconds since Unix epoch (zero-copy).
///
/// DateTime values are stored as milliseconds since 1970-01-01 00:00:00 UTC.
///
/// Example:
///   auto timestamps = GetDateTimeColumn(df, "timestamp");
///   for (int64_t ms : timestamps) {
///     auto seconds = ms / 1000;
///     // Convert to your preferred time representation
///   }
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

/// Struct-based Parquet writer with automatic batching and compression.
///
/// ParquetWriter<T> accepts struct records and converts them to columnar format
/// for writing. It buffers records in memory and flushes them when:
/// - The buffer reaches row_group_size (if configured)
/// - Finish() is called explicitly
/// - The destructor runs (best-effort, exceptions swallowed)
///
/// For maximum performance with columnar data, use ColumnarParquetWriter instead.
///
/// Example:
///   struct Trade { int64_t id; double price; };
///   // Register codec (see file header for full example)
///
///   ParquetWriter<Trade> writer("output.parquet");
///   writer.WithCompression("zstd").WithRowGroupSize(100000);
///   writer.WriteRecord({1, 150.0});
///   writer.WriteRecords(more_trades);
///   writer.Finish();  // Explicit finish to handle errors
template <typename RecordType>
class ParquetWriter {
 public:
  /// Create a writer for the specified file path.
  ///
  /// The file is not created until the first write operation.
  /// Default compression is "zstd" with no automatic batching (row_group_size=0).
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

  /// Destructor attempts best-effort flush. Exceptions are silently swallowed
  /// because destructors must be noexcept. Call Finish() explicitly to handle errors.
  ~ParquetWriter() {
    if (!finalized_ && (!buffer_.empty() || writer_)) {
      try {
        Finish();
      } catch (...) {
      }
    }
  }

  /// Set compression algorithm.
  ///
  /// Supported values: "zstd" (default), "snappy", "lz4", "gzip", "uncompressed"
  ///
  /// Returns *this for method chaining.
  ParquetWriter& WithCompression(std::string compression) {
    compression_ = std::move(compression);
    return *this;
  }

  /// Set row group size for automatic batching.
  ///
  /// When row_group_size > 0, the writer automatically flushes buffered records
  /// to disk when the buffer reaches this size. This enables streaming writes
  /// for large datasets without loading everything into memory.
  ///
  /// When row_group_size = 0 (default), all records are buffered until Finish().
  ///
  /// Recommendation: Use 100K-500K for balanced performance and file size.
  ///
  /// Returns *this for method chaining.
  ParquetWriter& WithRowGroupSize(size_t size) {
    row_group_size_ = size;
    return *this;
  }

  /// Write a single record to the buffer.
  ///
  /// If row_group_size is configured and the buffer is full, this triggers
  /// an automatic flush to disk.
  void WriteRecord(const RecordType& record) {
    buffer_.push_back(record);
    MaybeFlush();
  }

  /// Write multiple records to the buffer.
  ///
  /// More efficient than calling WriteRecord() in a loop.
  /// May trigger multiple automatic flushes if row_group_size is configured.
  void WriteRecords(const std::vector<RecordType>& records) {
    buffer_.insert(buffer_.end(), records.begin(), records.end());
    MaybeFlush();
  }

  /// Flush any buffered records and finalize the Parquet file.
  ///
  /// This writes the Parquet footer and closes the file. After calling Finish(),
  /// the writer cannot be used again.
  ///
  /// It's safe to call Finish() multiple times - subsequent calls are no-ops.
  ///
  /// Always call Finish() explicitly to handle potential I/O errors. The destructor
  /// calls Finish() as a fallback, but swallows exceptions.
  void Finish() {
    if (finalized_) return;
    if (!buffer_.empty()) FlushBatch();
    if (writer_) ffi::parquet_writer_finish(std::move(*writer_));
    writer_.reset();
    finalized_ = true;
  }

  /// Discard buffered records without writing them.
  ///
  /// This abandons any buffered data and marks the writer as finalized.
  /// Use this to cancel a write operation without creating a file.
  void Discard() {
    buffer_.clear();
    writer_.reset();
    finalized_ = true;
  }

  /// Returns the number of records currently buffered in memory.
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

/// High-performance zero-copy columnar writer for Parquet files.
///
/// ColumnarParquetWriter accepts data in columnar (Structure of Arrays) format
/// and writes it directly to Parquet without intermediate copies. This is ~42%
/// faster than ParquetWriter<T> when you already have columnar data.
///
/// Key differences from ParquetWriter<T>:
/// - Zero-copy for numeric types (int32/int64/float/double/datetime)
/// - Requires SoA (columnar) input instead of AoS (struct) input
/// - User must keep column data alive until WriteBatch() is called
/// - No automatic batching - call WriteBatch() explicitly
///
/// Performance: ~321ms vs ~1073ms for 20.7M rows (ParquetWriter<T>), snappy compression
///
/// Example:
///   std::vector<int64_t> ids = {1, 2, 3};
///   std::vector<float> prices = {100.0f, 200.0f, 300.0f};
///
///   ColumnarParquetWriter writer("output.parquet");
///   writer.WithCompression("zstd").WithRowGroupSize(500000);
///   writer.AddColumn("id", ids.data(), ids.size());
///   writer.AddColumn("price", prices.data(), prices.size());
///   writer.WriteBatch();  // Data pointers must be valid until here
///   writer.Finish();
///
/// IMPORTANT: Column data pointers passed to AddColumn() must remain valid
/// until WriteBatch() is called. The writer stores pointers, not copies.
class ColumnarParquetWriter {
 public:
  /// Create a writer for the specified file path.
  ///
  /// The file is not created until the first WriteBatch() call.
  /// Default compression is "zstd" with no row group size limit.
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

  /// Set compression algorithm.
  ///
  /// Supported values: "zstd" (default), "snappy", "lz4", "gzip", "uncompressed"
  ///
  /// Returns *this for method chaining.
  ColumnarParquetWriter& WithCompression(std::string compression) {
    compression_ = std::move(compression);
    return *this;
  }

  /// Set row group size.
  ///
  /// Unlike ParquetWriter<T>, this does NOT enable automatic batching.
  /// It only controls the row group size in the output Parquet file.
  /// You must call WriteBatch() explicitly.
  ///
  /// Recommendation: Use 100K-500K for balanced performance and file size.
  ///
  /// Returns *this for method chaining.
  ColumnarParquetWriter& WithRowGroupSize(size_t size) {
    row_group_size_ = size;
    return *this;
  }

  /// Add an int32 column (zero-copy).
  ///
  /// The data pointer must remain valid until WriteBatch() is called.
  /// All columns in a batch must have the same length.
  void AddColumn(const std::string& name, const int32_t* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_i32_column_zerocopy(w, name, rust::Slice<const int32_t>(data, len));
    });
  }

  /// Add an int64 column (zero-copy).
  ///
  /// The data pointer must remain valid until WriteBatch() is called.
  /// All columns in a batch must have the same length.
  void AddColumn(const std::string& name, const int64_t* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_i64_column_zerocopy(w, name, rust::Slice<const int64_t>(data, len));
    });
  }

  /// Add a float column (zero-copy).
  ///
  /// The data pointer must remain valid until WriteBatch() is called.
  /// All columns in a batch must have the same length.
  void AddColumn(const std::string& name, const float* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_f32_column_zerocopy(w, name, rust::Slice<const float>(data, len));
    });
  }

  /// Add a double column (zero-copy).
  ///
  /// The data pointer must remain valid until WriteBatch() is called.
  /// All columns in a batch must have the same length.
  void AddColumn(const std::string& name, const double* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_f64_column_zerocopy(w, name, rust::Slice<const double>(data, len));
    });
  }

  /// Add a boolean column (requires copy due to bit-packing).
  ///
  /// The data pointer must remain valid until WriteBatch() is called.
  /// All columns in a batch must have the same length.
  void AddColumn(const std::string& name, const bool* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_bool_column(w, name, rust::Slice<const bool>(data, len));
    });
  }

  /// Add a string column (requires copy due to variable-length encoding).
  ///
  /// The vector must remain valid until WriteBatch() is called.
  /// All columns in a batch must have the same length.
  void AddColumn(const std::string& name, const std::vector<std::string>& data) {
    const auto* ptr = &data;
    pending_.push_back([name, ptr](ffi::ParquetWriter& w) {
      ParquetCellCodec<std::string>::Write(w, name, *ptr);
    });
  }

  /// Add a DateTime column from int64_t milliseconds (zero-copy).
  ///
  /// DateTime values should be milliseconds since Unix epoch (1970-01-01 00:00:00 UTC).
  /// The data pointer must remain valid until WriteBatch() is called.
  /// All columns in a batch must have the same length.
  void AddDateTimeColumn(const std::string& name, const int64_t* data, size_t len) {
    pending_.push_back([=](ffi::ParquetWriter& w) {
      ffi::parquet_writer_add_datetime_column_zerocopy(w, name, rust::Slice<const int64_t>(data, len));
    });
  }

  /// Add a DateTime column from Abseil civil time types (absl::CivilSecond, etc.).
  ///
  /// The vector must remain valid until WriteBatch() is called.
  /// All columns in a batch must have the same length.
  template <AbseilCivilTime T>
  void AddDateTimeColumn(const std::string& name, const std::vector<T>& data) {
    const auto* ptr = &data;
    pending_.push_back([name, ptr](ffi::ParquetWriter& w) {
      ParquetCellCodec<T>::Write(w, name, *ptr);
    });
  }

  /// Write all pending columns as a single batch.
  ///
  /// After this call, all column data pointers are no longer needed and can be
  /// safely destroyed or reused. The pending column list is cleared.
  ///
  /// Call this method after adding all columns for a batch with AddColumn().
  void WriteBatch() {
    if (pending_.empty()) return;
    EnsureWriter();
    for (const auto& col : pending_) col(**writer_);
    ffi::parquet_writer_write_batch(**writer_);
    pending_.clear();
  }

  /// Finalize the Parquet file and write the footer.
  ///
  /// This flushes any pending batch and closes the file. After calling Finish(),
  /// the writer cannot be used again.
  ///
  /// It's safe to call Finish() multiple times - subsequent calls are no-ops.
  ///
  /// Always call Finish() explicitly to handle potential I/O errors. The destructor
  /// calls Finish() as a fallback, but swallows exceptions.
  void Finish() {
    if (finalized_) return;
    if (!pending_.empty()) WriteBatch();
    if (writer_) ffi::parquet_writer_finish(std::move(*writer_));
    writer_.reset();
    finalized_ = true;
  }

  /// Discard pending columns without writing them.
  ///
  /// This abandons any pending column data and marks the writer as finalized.
  /// Use this to cancel a write operation without creating a file.
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
