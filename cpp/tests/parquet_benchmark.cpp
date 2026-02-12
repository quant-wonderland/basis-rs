#include <basis_rs/parquet/parquet.hpp>
#include <chrono>
#include <iostream>

// Test file path
const char* TEST_FILE =
    "/var/lib/wonder/warehouse/database/lyc/parquet/DatayesTickSliceArchiver/"
    "2025/01/02.parquet";

// Define a struct matching some columns in the test file
struct TickData {
  int32_t stock_id;
  float close;
  float high;
  float low;
};

template <>
inline const basis_rs::ParquetCodec<TickData>& basis_rs::GetParquetCodec() {
  static basis_rs::ParquetCodec<TickData> codec = []() {
    basis_rs::ParquetCodec<TickData> c;
    c.Add("StockId", &TickData::stock_id);
    c.Add("Close", &TickData::close);
    c.Add("High", &TickData::high);
    c.Add("Low", &TickData::low);
    return c;
  }();
  return codec;
}

template <typename F>
double benchmark(const char* name, F&& func, int iterations = 3) {
  // Warm up
  func();

  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; ++i) {
    func();
  }
  auto end = std::chrono::high_resolution_clock::now();

  double avg_ms =
      std::chrono::duration<double, std::milli>(end - start).count() /
      iterations;
  std::cout << name << ": " << avg_ms << " ms avg" << std::endl;
  return avg_ms;
}

int main() {
  std::cout << "=== C++ Parquet Performance Benchmark ===" << std::endl;
  std::cout << "Test file: " << TEST_FILE << std::endl << std::endl;

  // Benchmark 1: New zero-copy DataFrame API
  std::cout << "--- New Zero-Copy API ---" << std::endl;

  double df_open_time = benchmark("DataFrame open", []() {
    basis_rs::DataFrame df(TEST_FILE);
    (void)df.NumRows();
  });

  double df_open_projected_time = benchmark("DataFrame open (projected)", []() {
    basis_rs::DataFrame df(
        TEST_FILE, {"StockId", "Close", "High", "Low"});
    (void)df.NumRows();
  });

  double column_access_time = 0;
  size_t num_rows = 0;
  {
    basis_rs::DataFrame df(TEST_FILE, {"StockId", "Close", "High", "Low"});
    num_rows = df.NumRows();
    std::cout << "Rows: " << num_rows << std::endl;

    column_access_time = benchmark("Get columns (zero-copy)", [&df]() {
      auto stock_id = df.GetColumn<int32_t>("StockId");
      auto close = df.GetColumn<float>("Close");
      auto high = df.GetColumn<float>("High");
      auto low = df.GetColumn<float>("Low");
      (void)stock_id.size();
      (void)close.size();
      (void)high.size();
      (void)low.size();
    });
  }

  // Benchmark 2: Column-wise operation (sum)
  double column_sum_time = 0;
  {
    basis_rs::DataFrame df(TEST_FILE, {"Close"});
    auto close = df.GetColumn<float>("Close");

    column_sum_time = benchmark("Column sum (zero-copy)", [&close]() {
      double sum = 0;
      for (float value : close) {
        sum += value;
      }
      (void)sum;
    });
  }

  // Benchmark 3: Row-wise iteration (chunk-aware for best performance)
  double row_iter_chunk_time = 0;
  {
    basis_rs::DataFrame df(TEST_FILE, {"StockId", "Close", "High", "Low"});
    auto high = df.GetColumn<float>("High");
    auto low = df.GetColumn<float>("Low");

    row_iter_chunk_time = benchmark("Row iteration (chunk-wise)", [&]() {
      float max_range = 0;
      // Use chunk-aware iteration for maximum cache locality
      for (size_t c = 0; c < high.NumChunks(); ++c) {
        const auto& high_chunk = high.Chunk(c);
        const auto& low_chunk = low.Chunk(c);
        for (size_t i = 0; i < high_chunk.size(); ++i) {
          float range = high_chunk[i] - low_chunk[i];
          if (range > max_range) {
            max_range = range;
          }
        }
      }
      (void)max_range;
    });
  }

  // Benchmark 3b: Row-wise iteration (via global index - slower)
  double row_iter_time = 0;
  {
    basis_rs::DataFrame df(TEST_FILE, {"StockId", "Close", "High", "Low"});
    auto stock_id = df.GetColumn<int32_t>("StockId");
    auto close = df.GetColumn<float>("Close");
    auto high = df.GetColumn<float>("High");
    auto low = df.GetColumn<float>("Low");

    row_iter_time = benchmark("Row iteration (via index, slower)", [&]() {
      float max_range = 0;
      size_t max_idx = 0;
      for (size_t i = 0; i < stock_id.size(); ++i) {
        float range = high[i] - low[i];
        if (range > max_range) {
          max_range = range;
          max_idx = i;
        }
      }
      (void)max_idx;
    });
  }

  // Benchmark 4: ReadAllAs (struct conversion)
  double read_all_as_time = benchmark("ReadAllAs<TickData>", []() {
    basis_rs::DataFrame df(TEST_FILE, {"StockId", "Close", "High", "Low"});
    auto records = df.ReadAllAs<TickData>();
    (void)records.size();
  });

  // Benchmark 5: Legacy ParquetFile API
  std::cout << std::endl << "--- Legacy API (for comparison) ---" << std::endl;

  double legacy_time = benchmark("ParquetFile::ReadAll<TickData>", []() {
    basis_rs::ParquetFile file(TEST_FILE);
    auto records = file.ReadAll<TickData>();
    (void)records.size();
  });

  // Summary
  std::cout << std::endl << "=== Summary ===" << std::endl;
  std::cout << "Rows: " << num_rows << std::endl;
  std::cout << "DataFrame open (projected): " << df_open_projected_time << " ms"
            << std::endl;
  std::cout << "Zero-copy column access: " << column_access_time << " ms"
            << std::endl;
  std::cout << "Column sum: " << column_sum_time << " ms" << std::endl;
  std::cout << "Row iteration (chunk-wise): " << row_iter_chunk_time << " ms" << std::endl;
  std::cout << "Row iteration (index): " << row_iter_time << " ms" << std::endl;
  std::cout << "ReadAllAs: " << read_all_as_time << " ms" << std::endl;
  std::cout << "Legacy ReadAll: " << legacy_time << " ms" << std::endl;

  double total_new = df_open_projected_time + column_access_time;
  std::cout << std::endl
            << "New API (open + access): " << total_new << " ms" << std::endl;
  std::cout << "Speedup over legacy: " << (legacy_time / total_new) << "x"
            << std::endl;

  return 0;
}
