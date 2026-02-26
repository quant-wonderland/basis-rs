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

  // Benchmark 1: DataFrame API
  std::cout << "--- DataFrame API ---" << std::endl;

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

  // Benchmark 5: DataFrame with Filter
  std::cout << std::endl << "--- DataFrame with Filter ---" << std::endl;

  double filter_time = benchmark("DataFrame::Open().Filter().Collect()", []() {
    auto df = basis_rs::DataFrame::Open(TEST_FILE)
                  .Select({"StockId", "Close", "High", "Low"})
                  .Filter("Close", basis_rs::Gt, 10.0f)
                  .Collect();
    (void)df.NumRows();
  });

  // Summary
  std::cout << std::endl << "=== Read Summary ===" << std::endl;
  std::cout << "Rows: " << num_rows << std::endl;
  std::cout << "DataFrame open (projected): " << df_open_projected_time << " ms"
            << std::endl;
  std::cout << "Zero-copy column access: " << column_access_time << " ms"
            << std::endl;
  std::cout << "Column sum: " << column_sum_time << " ms" << std::endl;
  std::cout << "Row iteration (chunk-wise): " << row_iter_chunk_time << " ms" << std::endl;
  std::cout << "Row iteration (index): " << row_iter_time << " ms" << std::endl;
  std::cout << "ReadAllAs: " << read_all_as_time << " ms" << std::endl;
  std::cout << "Filter query: " << filter_time << " ms" << std::endl;

  double total_new = df_open_projected_time + column_access_time;
  std::cout << std::endl
            << "DataFrame (open + access): " << total_new << " ms" << std::endl;

  // ==================== Write Benchmarks ====================
  std::cout << std::endl << "=== Write Benchmark ===" << std::endl;

  // Prepare write data: read source file into structs
  std::vector<TickData> write_data;
  {
    basis_rs::DataFrame df(TEST_FILE, {"StockId", "Close", "High", "Low"});
    write_data = df.ReadAllAs<TickData>();
  }
  std::cout << "Write dataset: " << write_data.size() << " rows, 4 columns"
            << std::endl;

  auto tmp_dir = std::filesystem::temp_directory_path() / "basis_rs_bench";
  std::filesystem::create_directories(tmp_dir);

  // Write: no streaming (default, all buffered)
  auto write_path = tmp_dir / "bench_write.parquet";
  double write_default_time =
      benchmark("Write (default, zstd)", [&]() {
        basis_rs::ParquetWriter<TickData> writer(write_path);
        writer.WriteRecords(write_data);
        writer.Finish();
      });

  // Write: streaming with row_group_size
  double write_streaming_time =
      benchmark("Write (streaming 500K, zstd)", [&]() {
        basis_rs::ParquetWriter<TickData> writer(write_path);
        writer.WithRowGroupSize(500000);
        writer.WriteRecords(write_data);
        writer.Finish();
      });

  // Write: snappy compression
  double write_snappy_time =
      benchmark("Write (streaming 500K, snappy)", [&]() {
        basis_rs::ParquetWriter<TickData> writer(write_path);
        writer.WithCompression("snappy").WithRowGroupSize(500000);
        writer.WriteRecords(write_data);
        writer.Finish();
      });

  // Write: uncompressed
  double write_uncomp_time =
      benchmark("Write (streaming 500K, uncompressed)", [&]() {
        basis_rs::ParquetWriter<TickData> writer(write_path);
        writer.WithCompression("uncompressed").WithRowGroupSize(500000);
        writer.WriteRecords(write_data);
        writer.Finish();
      });

  // ==================== Write Overhead Breakdown ====================
  std::cout << std::endl << "--- Write Overhead Breakdown ---" << std::endl;

  // Measure AoS→SoA extraction only (no FFI)
  benchmark("AoS->SoA extraction (4 cols)", [&]() {
    std::vector<int32_t> col0;
    std::vector<float> col1, col2, col3;
    col0.reserve(write_data.size());
    col1.reserve(write_data.size());
    col2.reserve(write_data.size());
    col3.reserve(write_data.size());
    for (const auto& r : write_data) {
      col0.push_back(r.stock_id);
      col1.push_back(r.close);
      col2.push_back(r.high);
      col3.push_back(r.low);
    }
  });

  // Measure FFI column add + write_batch only (pre-extracted data)
  std::vector<int32_t> pre_col0;
  std::vector<float> pre_col1, pre_col2, pre_col3;
  pre_col0.reserve(write_data.size());
  pre_col1.reserve(write_data.size());
  pre_col2.reserve(write_data.size());
  pre_col3.reserve(write_data.size());
  for (const auto& r : write_data) {
    pre_col0.push_back(r.stock_id);
    pre_col1.push_back(r.close);
    pre_col2.push_back(r.high);
    pre_col3.push_back(r.low);
  }

  benchmark("FFI add_columns + write_batch + finish (pre-extracted)", [&]() {
    auto w = basis_rs::ffi::parquet_writer_new(write_path.string(), "zstd", 500000);
    rust::Slice<const int32_t> s0(pre_col0.data(), pre_col0.size());
    rust::Slice<const float> s1(pre_col1.data(), pre_col1.size());
    rust::Slice<const float> s2(pre_col2.data(), pre_col2.size());
    rust::Slice<const float> s3(pre_col3.data(), pre_col3.size());
    basis_rs::ffi::parquet_writer_add_i32_column(*w, "StockId", s0);
    basis_rs::ffi::parquet_writer_add_f32_column(*w, "Close", s1);
    basis_rs::ffi::parquet_writer_add_f32_column(*w, "High", s2);
    basis_rs::ffi::parquet_writer_add_f32_column(*w, "Low", s3);
    basis_rs::ffi::parquet_writer_write_batch(*w);
    basis_rs::ffi::parquet_writer_finish(std::move(w));
  });

  std::cout << std::endl << "=== Write Summary ===" << std::endl;
  double rows_m = write_data.size() / 1e6;
  std::cout << "Default (zstd):       " << write_default_time << " ms ("
            << rows_m / (write_default_time / 1000.0) << " M rows/s)" << std::endl;
  std::cout << "Streaming (zstd):     " << write_streaming_time << " ms ("
            << rows_m / (write_streaming_time / 1000.0) << " M rows/s)" << std::endl;
  std::cout << "Streaming (snappy):   " << write_snappy_time << " ms ("
            << rows_m / (write_snappy_time / 1000.0) << " M rows/s)" << std::endl;
  std::cout << "Streaming (uncomp):   " << write_uncomp_time << " ms ("
            << rows_m / (write_uncomp_time / 1000.0) << " M rows/s)" << std::endl;

  // Cleanup
  std::filesystem::remove_all(tmp_dir);

  return 0;
}
