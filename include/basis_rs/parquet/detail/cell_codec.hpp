#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "absl/time/time.h"
#include "cxx_bridge.rs.h"
#include "type_traits.hpp"

namespace basis_rs {

absl::TimeZone GetShanghaiTimeZone() {
  static absl::TimeZone tz_gmt8 = []() {
    absl::TimeZone tz_gmt8;
    absl::LoadTimeZone("Asia/Shanghai", &tz_gmt8);
    return tz_gmt8;
  }();
  return tz_gmt8;
}

/// Codec for reading/writing individual column types to Parquet via FFI.
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

// Specialization: AbseilCivilTime types (absl::CivilDay, absl::CivilSecond, etc.)
template <AbseilCivilTime T>
struct ParquetCellCodec<T> {
  static constexpr absl::Time baseline{};

  static std::vector<T> Read(const ffi::ParquetReader& reader,
                             const std::string& name) {
    auto rust_vec = ffi::parquet_reader_get_i64_column(reader, name);
    std::vector<T> result;
    result.reserve(rust_vec.size());
    for (int64_t ms : rust_vec) {
      std::chrono::milliseconds time(ms);
      absl::Time absl_time = baseline + absl::FromChrono(time);
      result.push_back(
          T{absl::ToCivilSecond(absl_time, GetShanghaiTimeZone())});
    }
    return result;
  }

  static void Write(ffi::ParquetWriter& writer, const std::string& name,
                    const std::vector<T>& data) {
    std::vector<int64_t> i64_data;
    i64_data.reserve(data.size());
    for (const auto& civil : data) {
      auto absl_time = absl::FromCivil(civil, GetShanghaiTimeZone());
      i64_data.push_back(
          absl::ToChronoMilliseconds(absl_time - baseline).count());
    }
    rust::Slice<const int64_t> slice(i64_data.data(), i64_data.size());
    ffi::parquet_writer_add_datetime_column(writer, name, slice);
  }
};

}  // namespace basis_rs
