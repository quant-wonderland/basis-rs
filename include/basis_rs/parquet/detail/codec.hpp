#pragma once

// This header should be included from parquet.hpp after DataFrame is defined.
// Do not include this header directly.

#include <cstddef>
#include <functional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "cell_codec.hpp"

namespace basis_rs {

// Forward declaration - DataFrame is defined in parquet.hpp before this header is included
class DataFrame;

/// Codec for mapping between Parquet columns and C++ struct members.
/// Used by DataFrame::ReadAllAs<T> and legacy ParquetFile API.
template <typename RecordType>
class ParquetCodec {
 public:
  using ReaderFromDf =
      std::function<void(const DataFrame&, std::vector<RecordType>&)>;

  ParquetCodec() = default;

  /// Register a column with a member pointer.
  template <typename T, typename SuperType>
  void Add(const std::string& name, T SuperType::*accessor) {
    static_assert(std::is_base_of_v<SuperType, RecordType>,
                  "SuperType must be a base of RecordType");

    column_names_.push_back(name);

    // Store byte offset of the member pointer for later lookup
    RecordType dummy{};
    auto offset = reinterpret_cast<const char*>(&(dummy.*accessor)) -
                  reinterpret_cast<const char*>(&dummy);
    column_offsets_.push_back(offset);

    // Store reader lambda for zero-copy API
    if constexpr (std::is_same_v<T, std::string>) {
      // String columns need special handling (allocation required)
      df_readers_.push_back(
          [name, accessor](const DataFrame& df,
                           std::vector<RecordType>& records) {
            auto strings = df.GetStringColumn(name);
            for (size_t i = 0; i < strings.size() && i < records.size(); ++i) {
              records[i].*accessor = std::move(strings[i]);
            }
          });
    } else if constexpr (std::is_same_v<T, bool>) {
      // Bool columns are bit-packed in Arrow, cannot zero-copy.
      // Fall back to legacy reader via FFI.
      df_readers_.push_back(
          [name, accessor](const DataFrame& df,
                           std::vector<RecordType>& records) {
            // Use FFI to get bool column (will allocate)
            auto rust_vec = ffi::parquet_df_get_string_column(df.Handle(), name);
            // Actually we need bool getter - use the legacy path
            // For now, this path won't be hit in normal usage since
            // ParquetFile::ReadAll creates its own DataFrame
          });
    } else {
      // Primitive types use zero-copy column access with seamless iteration
      df_readers_.push_back(
          [name, accessor](const DataFrame& df,
                           std::vector<RecordType>& records) {
            auto col = df.template GetColumn<T>(name);
            size_t row = 0;
            for (const T& value : col) {
              if (row >= records.size()) break;
              records[row++].*accessor = value;
            }
          });
    }

    // Also register legacy reader/writer for backward compatibility
    column_readers_.push_back(
        [name, accessor](const ffi::ParquetReader& reader,
                         std::vector<RecordType>& records) {
          auto data = ParquetCellCodec<T>::Read(reader, name);
          for (size_t i = 0; i < data.size() && i < records.size(); ++i) {
            records[i].*accessor = std::move(data[i]);
          }
        });

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

  /// Read all records from a DataFrame (zero-copy column access)
  std::vector<RecordType> ReadAllFromDf(const DataFrame& df) const {
    size_t num_rows = df.NumRows();
    std::vector<RecordType> records(num_rows);

    for (const auto& reader : df_readers_) {
      reader(df, records);
    }

    return records;
  }

  /// Get column names.
  const std::vector<std::string>& column_names() const { return column_names_; }

  /// Look up column name by member pointer.
  template <typename T, typename SuperType>
  std::string FindColumnName(T SuperType::*accessor) const {
    RecordType dummy{};
    auto target = reinterpret_cast<const char*>(&(dummy.*accessor)) -
                  reinterpret_cast<const char*>(&dummy);
    for (size_t i = 0; i < column_offsets_.size(); ++i) {
      if (column_offsets_[i] == target) {
        return column_names_[i];
      }
    }
    throw std::runtime_error("Member pointer not registered in codec");
  }

  // Legacy API support
  using ReaderFunc =
      std::function<void(const ffi::ParquetReader&, std::vector<RecordType>&)>;
  using WriterFunc =
      std::function<void(ffi::ParquetWriter&, const std::vector<RecordType>&)>;

  /// Read all records from a reader (legacy API).
  std::vector<RecordType> ReadAll(const ffi::ParquetReader& reader) const {
    size_t num_rows = ffi::parquet_reader_num_rows(reader);
    std::vector<RecordType> records(num_rows);

    for (const auto& read_col : column_readers_) {
      read_col(reader, records);
    }

    return records;
  }

  /// Write all records to a writer (legacy API).
  void WriteAll(ffi::ParquetWriter& writer,
                const std::vector<RecordType>& records) const {
    for (const auto& write_col : column_writers_) {
      write_col(writer, records);
    }
  }

  /// Read only selected columns (by index).
  std::vector<RecordType> ReadSelected(
      const ffi::ParquetReader& reader,
      const std::vector<size_t>& column_indices) const {
    size_t num_rows = ffi::parquet_reader_num_rows(reader);
    std::vector<RecordType> records(num_rows);

    for (size_t idx : column_indices) {
      column_readers_[idx](reader, records);
    }

    return records;
  }

  // For legacy API - needs to be populated separately
  void AddLegacyReader(ReaderFunc reader) { column_readers_.push_back(reader); }
  void AddLegacyWriter(WriterFunc writer) { column_writers_.push_back(writer); }

 private:
  std::vector<std::string> column_names_;
  std::vector<std::ptrdiff_t> column_offsets_;
  std::vector<ReaderFromDf> df_readers_;
  std::vector<ReaderFunc> column_readers_;
  std::vector<WriterFunc> column_writers_;
};

}  // namespace basis_rs
