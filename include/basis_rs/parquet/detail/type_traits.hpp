#pragma once

#include <concepts>
#include <cstdint>
#include <string>
#include <type_traits>

#include "absl/time/time.h"
#include "cxx_bridge.rs.h"

namespace basis_rs {

// Concept for Abseil civil time types
template <typename T>
concept AbseilCivilTime = std::same_as<T, absl::CivilDay> ||
                          std::same_as<T, absl::CivilSecond> ||
                          std::same_as<T, absl::CivilMinute> ||
                          std::same_as<T, absl::CivilHour>;

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

// ParquetTypeOf specialization for AbseilCivilTime types
template <AbseilCivilTime T>
struct ParquetTypeOf<T> {
  static constexpr ffi::ColumnType type = ffi::ColumnType::DateTime;
};

}  // namespace basis_rs
