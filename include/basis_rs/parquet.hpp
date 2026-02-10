#pragma once

/**
 * Type-safe C++ wrapper for basis-rs Parquet functionality.
 *
 * Provides a simple, struct-based API for reading/writing Parquet files:
 *
 *   struct MyData {
 *       int64_t id;
 *       std::string name;
 *       double score;
 *   };
 *
 *   template <>
 *   inline const basis_rs::ParquetCodec<MyData>& basis_rs::GetParquetCodec() {
 *       static basis_rs::ParquetCodec<MyData> codec = []() {
 *           basis_rs::ParquetCodec<MyData> c;
 *           c.Add("id", &MyData::id);
 *           c.Add("name", &MyData::name);
 *           c.Add("score", &MyData::score);
 *           return c;
 *       }();
 *       return codec;
 *   }
 *
 *   // Usage:
 *   auto records = file.ReadAll<MyData>();
 *   writer.WriteRecord(entry);
 */

#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

// Include CXX-generated header
#include "cxx_bridge.rs.h"

namespace basis_rs {

// Forward declaration
template <typename RecordType>
class ParquetCodec;

template <typename RecordType>
const ParquetCodec<RecordType>& GetParquetCodec();

// ==================== Type Traits ====================

template <typename T>
struct ParquetTypeOf {
  static_assert(!std::is_same_v<T, T>, "Unsupported type for ParquetTypeOf");
};

template <>
struct ParquetTypeOf<int64_t> {
  static constexpr ffi::ColumnType type = ffi::ColumnType::Int64;
};

template <>
struct ParquetTypeOf<int32_t> {
  static constexpr ffi::ColumnType type = ffi::ColumnType::Int32;
};

template <>
struct ParquetTypeOf<double> {
  static constexpr ffi::ColumnType type = ffi::ColumnType::Float64;
};

template <>
struct ParquetTypeOf<float> {
  static constexpr ffi::ColumnType type = ffi::ColumnType::Float32;
};

template <>
struct ParquetTypeOf<std::string> {
  static constexpr ffi::ColumnType type = ffi::ColumnType::String;
};

template <>
struct ParquetTypeOf<bool> {
  static constexpr ffi::ColumnType type = ffi::ColumnType::Bool;
};

// ==================== Cell Codec ====================

template <typename T>
struct ParquetCellCodec {
  static std::vector<T> Read(const ffi::ParquetReader& reader,
                             const std::string& name);
  static void Write(ffi::ParquetWriter& writer, const std::string& name,
                    const std::vector<T>& data);
};

// Specialization: int64_t
template <>
struct ParquetCellCodec<int64_t> {
  static std::vector<int64_t> Read(const ffi::ParquetReader& reader,
                                   const std::string& name) {
    auto rust_vec = ffi::parquet_reader_get_i64_column(reader, name);
    return std::vector<int64_t>(rust_vec.begin(), rust_vec.end());
  }

  static void Write(ffi::ParquetWriter& writer, const std::string& name,
                    const std::vector<int64_t>& data) {
    rust::Slice<const int64_t> slice(data.data(), data.size());
    ffi::parquet_writer_add_i64_column(writer, name, slice);
  }
};

// Specialization: int32_t
template <>
struct ParquetCellCodec<int32_t> {
  static std::vector<int32_t> Read(const ffi::ParquetReader& reader,
                                   const std::string& name) {
    auto rust_vec = ffi::parquet_reader_get_i32_column(reader, name);
    return std::vector<int32_t>(rust_vec.begin(), rust_vec.end());
  }

  static void Write(ffi::ParquetWriter& writer, const std::string& name,
                    const std::vector<int32_t>& data) {
    rust::Slice<const int32_t> slice(data.data(), data.size());
    ffi::parquet_writer_add_i32_column(writer, name, slice);
  }
};

// Specialization: double
template <>
struct ParquetCellCodec<double> {
  static std::vector<double> Read(const ffi::ParquetReader& reader,
                                  const std::string& name) {
    auto rust_vec = ffi::parquet_reader_get_f64_column(reader, name);
    return std::vector<double>(rust_vec.begin(), rust_vec.end());
  }

  static void Write(ffi::ParquetWriter& writer, const std::string& name,
                    const std::vector<double>& data) {
    rust::Slice<const double> slice(data.data(), data.size());
    ffi::parquet_writer_add_f64_column(writer, name, slice);
  }
};

// Specialization: float
template <>
struct ParquetCellCodec<float> {
  static std::vector<float> Read(const ffi::ParquetReader& reader,
                                 const std::string& name) {
    auto rust_vec = ffi::parquet_reader_get_f32_column(reader, name);
    return std::vector<float>(rust_vec.begin(), rust_vec.end());
  }

  static void Write(ffi::ParquetWriter& writer, const std::string& name,
                    const std::vector<float>& data) {
    rust::Slice<const float> slice(data.data(), data.size());
    ffi::parquet_writer_add_f32_column(writer, name, slice);
  }
};

// Specialization: std::string
template <>
struct ParquetCellCodec<std::string> {
  static std::vector<std::string> Read(const ffi::ParquetReader& reader,
                                       const std::string& name) {
    auto rust_vec = ffi::parquet_reader_get_string_column(reader, name);
    std::vector<std::string> result;
    result.reserve(rust_vec.size());
    for (const auto& s : rust_vec) {
      result.emplace_back(std::string(s));
    }
    return result;
  }

  static void Write(ffi::ParquetWriter& writer, const std::string& name,
                    const std::vector<std::string>& data) {
    rust::Vec<rust::String> rust_vec;
    rust_vec.reserve(data.size());
    for (const auto& s : data) {
      rust_vec.push_back(rust::String(s));
    }
    ffi::parquet_writer_add_string_column(writer, name, std::move(rust_vec));
  }
};

// Specialization: bool
template <>
struct ParquetCellCodec<bool> {
  static std::vector<bool> Read(const ffi::ParquetReader& reader,
                                const std::string& name) {
    auto rust_vec = ffi::parquet_reader_get_bool_column(reader, name);
    return std::vector<bool>(rust_vec.begin(), rust_vec.end());
  }

  static void Write(ffi::ParquetWriter& writer, const std::string& name,
                    const std::vector<bool>& data) {
    // std::vector<bool> doesn't have contiguous storage, need to copy
    std::vector<uint8_t> temp(data.size());
    for (size_t i = 0; i < data.size(); ++i) {
      temp[i] = data[i] ? 1 : 0;
    }
    rust::Slice<const bool> slice(reinterpret_cast<const bool*>(temp.data()),
                                  temp.size());
    ffi::parquet_writer_add_bool_column(writer, name, slice);
  }
};

// ==================== ParquetCodec ====================

template <typename RecordType>
class ParquetCodec {
 public:
  using ReaderFunc =
      std::function<void(const ffi::ParquetReader&, std::vector<RecordType>&)>;
  using WriterFunc =
      std::function<void(ffi::ParquetWriter&, const std::vector<RecordType>&)>;

  ParquetCodec() = default;

  /// Register a column with a member pointer.
  template <typename T, typename SuperType>
  void Add(const std::string& name, T SuperType::*accessor) {
    static_assert(std::is_base_of_v<SuperType, RecordType>,
                  "SuperType must be a base of RecordType");

    column_names_.push_back(name);

    // Store reader lambda
    column_readers_.push_back(
        [name, accessor](const ffi::ParquetReader& reader,
                         std::vector<RecordType>& records) {
          auto data = ParquetCellCodec<T>::Read(reader, name);
          for (size_t i = 0; i < data.size() && i < records.size(); ++i) {
            records[i].*accessor = std::move(data[i]);
          }
        });

    // Store writer lambda
    column_writers_.push_back(
        [name, accessor](ffi::ParquetWriter& writer,
                         const std::vector<RecordType>& records) {
          std::vector<T> data;
          data.reserve(records.size());
          for (const auto& rec : records) {
            data.push_back(rec.*accessor);
          }
          ParquetCellCodec<T>::Write(writer, name, data);
        });
  }

  /// Read all records from a reader.
  std::vector<RecordType> ReadAll(const ffi::ParquetReader& reader) const {
    size_t num_rows = ffi::parquet_reader_num_rows(reader);
    std::vector<RecordType> records(num_rows);

    for (const auto& read_col : column_readers_) {
      read_col(reader, records);
    }

    return records;
  }

  /// Write all records to a writer.
  void WriteAll(ffi::ParquetWriter& writer,
                const std::vector<RecordType>& records) const {
    for (const auto& write_col : column_writers_) {
      write_col(writer, records);
    }
  }

  /// Get column names.
  const std::vector<std::string>& column_names() const { return column_names_; }

 private:
  std::vector<std::string> column_names_;
  std::vector<ReaderFunc> column_readers_;
  std::vector<WriterFunc> column_writers_;
};

// ==================== ParquetWriter ====================

template <typename RecordType>
class ParquetWriter {
 public:
  explicit ParquetWriter(std::filesystem::path path)
      : path_(std::move(path)), finalized_(false) {}

  ParquetWriter(ParquetWriter&& other) noexcept
      : path_(std::move(other.path_)),
        buffer_(std::move(other.buffer_)),
        finalized_(other.finalized_) {
    other.finalized_ = true;  // Prevent double-finish
  }

  ~ParquetWriter() {
    if (!finalized_ && !buffer_.empty()) {
      try {
        Finish();
      } catch (...) {
        // Ignore exceptions in destructor
      }
    }
  }

  /// Write a single record (buffered).
  void WriteRecord(const RecordType& record) { buffer_.push_back(record); }

  /// Write multiple records at once.
  void WriteRecords(const std::vector<RecordType>& records) {
    buffer_.insert(buffer_.end(), records.begin(), records.end());
  }

  /// Finish writing and flush to file.
  void Finish() {
    if (finalized_) {
      return;
    }

    if (!buffer_.empty()) {
      auto writer = ffi::parquet_writer_new(path_.string());
      GetParquetCodec<RecordType>().WriteAll(*writer, buffer_);
      ffi::parquet_writer_finish(std::move(writer));
    }

    finalized_ = true;
  }

  /// Discard all buffered data without writing.
  void Discard() {
    buffer_.clear();
    finalized_ = true;
  }

 private:
  std::filesystem::path path_;
  std::vector<RecordType> buffer_;
  bool finalized_;
};

// ==================== ParquetFile ====================

class ParquetFile {
 public:
  explicit ParquetFile(std::filesystem::path path) : path_(std::move(path)) {}

  /// Check if the file exists.
  bool Exists() const { return std::filesystem::exists(path_); }

  /// Get the file path.
  const std::filesystem::path& path() const { return path_; }

  /// Read all records of type RecordType.
  template <typename RecordType>
  std::vector<RecordType> ReadAll() const {
    auto reader = ffi::parquet_reader_open(path_.string());
    return GetParquetCodec<RecordType>().ReadAll(*reader);
  }

  /// Spawn a writer for the given record type.
  template <typename RecordType>
  ParquetWriter<RecordType> SpawnWriter() const {
    return ParquetWriter<RecordType>(path_);
  }

 private:
  std::filesystem::path path_;
};

}  // namespace basis_rs
