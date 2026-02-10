#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "basis_rs/parquet.hpp"

using Clock = std::chrono::high_resolution_clock;

static double ms(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration<double, std::milli>(b - a).count();
}

/// Extract all columns from reader whose types the bridge supports,
/// return count of extracted columns and list of first 3 numeric column names.
static size_t ExtractAllCompatible(
    const basis_rs::ffi::ParquetReader& reader,
    const rust::Vec<basis_rs::ffi::ColumnInfo>& cols,
    std::vector<std::string>& selected_out) {
  size_t extracted = 0;
  size_t numeric_picked = 0;

  for (const auto& ci : cols) {
    try {
      switch (ci.dtype) {
        case basis_rs::ffi::ColumnType::Int64: {
          auto v =
              basis_rs::ffi::parquet_reader_get_i64_column(reader, ci.name);
          ++extracted;
          if (numeric_picked < 3) {
            selected_out.emplace_back(std::string(ci.name));
            ++numeric_picked;
          }
          break;
        }
        case basis_rs::ffi::ColumnType::Int32: {
          auto v =
              basis_rs::ffi::parquet_reader_get_i32_column(reader, ci.name);
          ++extracted;
          if (numeric_picked < 3) {
            selected_out.emplace_back(std::string(ci.name));
            ++numeric_picked;
          }
          break;
        }
        case basis_rs::ffi::ColumnType::Float64: {
          auto v =
              basis_rs::ffi::parquet_reader_get_f64_column(reader, ci.name);
          ++extracted;
          if (numeric_picked < 3) {
            selected_out.emplace_back(std::string(ci.name));
            ++numeric_picked;
          }
          break;
        }
        case basis_rs::ffi::ColumnType::Float32: {
          auto v =
              basis_rs::ffi::parquet_reader_get_f32_column(reader, ci.name);
          ++extracted;
          if (numeric_picked < 3) {
            selected_out.emplace_back(std::string(ci.name));
            ++numeric_picked;
          }
          break;
        }
        case basis_rs::ffi::ColumnType::String: {
          auto v =
              basis_rs::ffi::parquet_reader_get_string_column(reader, ci.name);
          ++extracted;
          break;
        }
        case basis_rs::ffi::ColumnType::Bool: {
          auto v =
              basis_rs::ffi::parquet_reader_get_bool_column(reader, ci.name);
          ++extracted;
          break;
        }
        default:
          break;
      }
    } catch (const std::exception& e) {
      // Type mismatch (e.g. UInt64 mapped as Int64 fallback) — skip
      std::cerr << "  skip column '" << std::string(ci.name)
                << "': " << e.what() << std::endl;
    }
  }
  return extracted;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: bench_parquet <path>" << std::endl;
    return 1;
  }
  std::string path = argv[1];

  std::cout << std::fixed << std::setprecision(1);
  std::cout << "=== C++ basis-rs Parquet Benchmark ===" << std::endl;
  std::cout << "File: " << path << std::endl;
  std::cout << std::endl;

  // Discover columns and pick 3 for selected-column test
  std::vector<std::string> selected;

  // -------- 1. Open ALL columns + extract all compatible --------
  {
    auto t0 = Clock::now();
    auto reader = basis_rs::ffi::parquet_reader_open(path);
    auto t1 = Clock::now();

    size_t nrows = basis_rs::ffi::parquet_reader_num_rows(*reader);
    size_t ncols = basis_rs::ffi::parquet_reader_num_cols(*reader);
    auto col_info = basis_rs::ffi::parquet_reader_columns(*reader);

    size_t extracted = ExtractAllCompatible(*reader, col_info, selected);
    auto t2 = Clock::now();

    std::cout << "[C++] Read ALL columns:" << std::endl;
    std::cout << "  Open (Polars read all):         " << ms(t0, t1) << " ms"
              << std::endl;
    std::cout << "  Extract " << extracted
              << " cols to C++ vecs:  " << ms(t1, t2) << " ms" << std::endl;
    std::cout << "  Total:                          " << ms(t0, t2) << " ms"
              << std::endl;
    std::cout << "  Shape: " << nrows << " rows x " << ncols << " cols"
              << std::endl;
    std::cout << std::endl;
  }

  // -------- 2. Open SELECTED columns + extract --------
  if (selected.size() >= 3) {
    std::cout << "Selected columns: ";
    for (size_t i = 0; i < selected.size(); ++i) {
      if (i) std::cout << ", ";
      std::cout << selected[i];
    }
    std::cout << std::endl;

    rust::Vec<rust::String> cols;
    for (const auto& s : selected) {
      cols.push_back(rust::String(s));
    }

    auto t0 = Clock::now();
    auto reader = basis_rs::ffi::parquet_reader_open_with_columns(
        path, std::move(cols));
    auto t1 = Clock::now();

    size_t nrows = basis_rs::ffi::parquet_reader_num_rows(*reader);
    size_t ncols = basis_rs::ffi::parquet_reader_num_cols(*reader);

    // Re-discover types for the selected subset
    auto col_info = basis_rs::ffi::parquet_reader_columns(*reader);
    std::vector<std::string> dummy;
    size_t extracted = ExtractAllCompatible(*reader, col_info, dummy);
    auto t2 = Clock::now();

    std::cout << "[C++] Read SELECTED columns:" << std::endl;
    std::cout << "  Open (Polars read projected):   " << ms(t0, t1) << " ms"
              << std::endl;
    std::cout << "  Extract " << extracted
              << " cols to C++ vecs:    " << ms(t1, t2) << " ms" << std::endl;
    std::cout << "  Total:                          " << ms(t0, t2) << " ms"
              << std::endl;
    std::cout << "  Shape: " << nrows << " rows x " << ncols << " cols"
              << std::endl;
    std::cout << std::endl;
  }

  return 0;
}
