#pragma once

/**
 * C++ wrapper for basis-rs Parquet functionality.
 *
 * Provides a modern C++ interface with RAII and exception handling.
 */

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// Include the generated C header
extern "C" {
#include "basis_rs.h"
}

namespace basis {

class BasisError : public std::runtime_error {
public:
  explicit BasisError(const std::string& msg) : std::runtime_error(msg) {}

  static void check(int result) {
    if (result != 0) {
      const char* err = basis_get_last_error();
      std::string msg = err ? err : "Unknown error";
      basis_clear_error();
      throw BasisError(msg);
    }
  }

  static void check_ptr(void* ptr) {
    if (ptr == nullptr) {
      const char* err = basis_get_last_error();
      std::string msg = err ? err : "Null pointer returned";
      basis_clear_error();
      throw BasisError(msg);
    }
  }
};

class DataFrame {
public:
  DataFrame() : handle_(basis_df_new(), &basis_df_free) {
    BasisError::check_ptr(handle_.get());
  }

  static DataFrame read_parquet(const std::string& path) {
    BasisDataFrame* ptr = basis_parquet_read(path.c_str());
    BasisError::check_ptr(ptr);
    return DataFrame(ptr);
  }

  void write_parquet(const std::string& path) const {
    BasisError::check(basis_parquet_write(handle_.get(), path.c_str()));
  }

  size_t height() const { return basis_df_height(handle_.get()); }

  size_t width() const { return basis_df_width(handle_.get()); }

  std::vector<int64_t> get_int64_column(const std::string& name) const {
    BasisInt64Column col{nullptr, 0};
    BasisError::check(
        basis_df_get_int64_column(handle_.get(), name.c_str(), &col));

    std::vector<int64_t> result(col.data, col.data + col.len);
    basis_int64_column_free(&col);
    return result;
  }

  std::vector<double> get_float64_column(const std::string& name) const {
    BasisFloat64Column col{nullptr, 0};
    BasisError::check(
        basis_df_get_float64_column(handle_.get(), name.c_str(), &col));

    std::vector<double> result(col.data, col.data + col.len);
    basis_float64_column_free(&col);
    return result;
  }

  std::vector<std::string> get_string_column(const std::string& name) const {
    BasisStringColumn col{nullptr, 0};
    BasisError::check(
        basis_df_get_string_column(handle_.get(), name.c_str(), &col));

    std::vector<std::string> result;
    result.reserve(col.len);
    for (size_t i = 0; i < col.len; ++i) {
      result.emplace_back(col.data[i] ? col.data[i] : "");
    }
    basis_string_column_free(&col);
    return result;
  }

  std::vector<bool> get_bool_column(const std::string& name) const {
    BasisBoolColumn col{nullptr, 0};
    BasisError::check(
        basis_df_get_bool_column(handle_.get(), name.c_str(), &col));

    std::vector<bool> result(col.data, col.data + col.len);
    basis_bool_column_free(&col);
    return result;
  }

  void add_column(const std::string& name, const std::vector<int64_t>& data) {
    BasisError::check(basis_df_add_int64_column(handle_.get(), name.c_str(),
                                                data.data(), data.size()));
  }

  void add_column(const std::string& name, const std::vector<double>& data) {
    BasisError::check(basis_df_add_float64_column(handle_.get(), name.c_str(),
                                                  data.data(), data.size()));
  }

  void add_column(const std::string& name,
                  const std::vector<std::string>& data) {
    std::vector<const char*> c_strs;
    c_strs.reserve(data.size());
    for (const auto& s : data) {
      c_strs.push_back(s.c_str());
    }
    BasisError::check(basis_df_add_string_column(handle_.get(), name.c_str(),
                                                 c_strs.data(), c_strs.size()));
  }

  void add_column(const std::string& name, const std::vector<bool>& data) {
    // std::vector<bool> is special and doesn't have data(), need to convert
    std::vector<uint8_t> copy(data.size());
    for (size_t i = 0; i < data.size(); ++i) {
      copy[i] = data[i] ? 1 : 0;
    }
    BasisError::check(basis_df_add_bool_column(
        handle_.get(), name.c_str(), reinterpret_cast<const bool*>(copy.data()),
        copy.size()));
  }

private:
  explicit DataFrame(BasisDataFrame* ptr) : handle_(ptr, &basis_df_free) {}

  std::unique_ptr<BasisDataFrame, decltype(&basis_df_free)> handle_;
};

} // namespace basis
