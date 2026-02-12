#pragma once

#include <cstdint>
#include <string>
#include <type_traits>

#include "cxx_bridge.rs.h"

namespace basis_rs {

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
struct ParquetTypeOf<uint64_t> {
  static constexpr ffi::ColumnType type = ffi::ColumnType::UInt64;
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

}  // namespace basis_rs
