#include <basis_rs/parquet/parquet.hpp>
#include <cstdio>
#include <filesystem>
#include <gtest/gtest.h>

namespace fs = std::filesystem;

// ==================== Test Data Structures ====================

struct SimpleEntry
{
  int64_t id;
  std::string name;
  double score;
};

struct NumericEntry
{
  int32_t i32_val;
  int64_t i64_val;
  float f32_val;
  double f64_val;
};

struct BoolEntry
{
  int64_t id;
  bool active;
  bool verified;
};

struct PartialEntry
{
  int64_t id;
  double score;
};

// ==================== Codec Registrations ====================

template <>
inline const basis_rs::ParquetCodec<SimpleEntry> &basis_rs::GetParquetCodec()
{
  static basis_rs::ParquetCodec<SimpleEntry> codec = []()
  {
    basis_rs::ParquetCodec<SimpleEntry> c;
    c.Add("id", &SimpleEntry::id);
    c.Add("name", &SimpleEntry::name);
    c.Add("score", &SimpleEntry::score);
    return c;
  }();
  return codec;
}

template <>
inline const basis_rs::ParquetCodec<NumericEntry> &basis_rs::GetParquetCodec()
{
  static basis_rs::ParquetCodec<NumericEntry> codec = []()
  {
    basis_rs::ParquetCodec<NumericEntry> c;
    c.Add("i32_val", &NumericEntry::i32_val);
    c.Add("i64_val", &NumericEntry::i64_val);
    c.Add("f32_val", &NumericEntry::f32_val);
    c.Add("f64_val", &NumericEntry::f64_val);
    return c;
  }();
  return codec;
}

template <>
inline const basis_rs::ParquetCodec<BoolEntry> &basis_rs::GetParquetCodec()
{
  static basis_rs::ParquetCodec<BoolEntry> codec = []()
  {
    basis_rs::ParquetCodec<BoolEntry> c;
    c.Add("id", &BoolEntry::id);
    c.Add("active", &BoolEntry::active);
    c.Add("verified", &BoolEntry::verified);
    return c;
  }();
  return codec;
}

template <>
inline const basis_rs::ParquetCodec<PartialEntry> &basis_rs::GetParquetCodec()
{
  static basis_rs::ParquetCodec<PartialEntry> codec = []()
  {
    basis_rs::ParquetCodec<PartialEntry> c;
    c.Add("id", &PartialEntry::id);
    c.Add("score", &PartialEntry::score);
    return c;
  }();
  return codec;
}

// ==================== Test Fixture ====================

class ParquetTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    temp_dir_ = fs::temp_directory_path() / "basis_rs_parquet_test";
    fs::create_directories(temp_dir_);
  }

  void TearDown() override { fs::remove_all(temp_dir_); }

  fs::path temp_dir_;
};

// ==================== DataFrame Tests ====================

TEST_F(ParquetTest, DataFrameOpen)
{
  auto path = temp_dir_ / "df_open.parquet";

  // Write test data first
  {
    basis_rs::ParquetFile file(path);
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    writer.Finish();
  }

  // Test opening without projection
  basis_rs::DataFrame df(path);
  EXPECT_EQ(df.NumRows(), 2);
  EXPECT_EQ(df.NumCols(), 3);
}

TEST_F(ParquetTest, DataFrameProjected)
{
  auto path = temp_dir_ / "df_projected.parquet";

  // Write test data
  {
    basis_rs::ParquetFile file(path);
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    writer.Finish();
  }

  // Open with projection
  basis_rs::DataFrame df(path, {"id", "score"});
  EXPECT_EQ(df.NumRows(), 2);
  EXPECT_EQ(df.NumCols(), 2);

  auto columns = df.Columns();
  ASSERT_EQ(columns.size(), 2);
}

TEST_F(ParquetTest, DataFrameReadAllAs)
{
  auto path = temp_dir_ / "df_readallas.parquet";

  // Write test data
  {
    basis_rs::ParquetFile file(path);
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    writer.WriteRecord({3, "charlie", 78.5});
    writer.Finish();
  }

  // Read using DataFrame::ReadAllAs
  basis_rs::DataFrame df(path);
  auto records = df.ReadAllAs<SimpleEntry>();

  ASSERT_EQ(records.size(), 3);
  EXPECT_EQ(records[0].id, 1);
  EXPECT_EQ(records[0].name, "alice");
  EXPECT_DOUBLE_EQ(records[0].score, 85.5);
  EXPECT_EQ(records[1].id, 2);
  EXPECT_EQ(records[1].name, "bob");
  EXPECT_DOUBLE_EQ(records[1].score, 92.0);
}

TEST_F(ParquetTest, DataFrameRechunk)
{
  auto path = temp_dir_ / "df_rechunk.parquet";

  // Write test data
  {
    basis_rs::ParquetFile file(path);
    auto writer = file.SpawnWriter<SimpleEntry>();
    for (int i = 0; i < 100; ++i)
    {
      writer.WriteRecord({i, "name", i * 1.0});
    }
    writer.Finish();
  }

  basis_rs::DataFrame df(path);
  // Rechunk should work (may or may not actually rechunk)
  bool rechunked = df.Rechunk();
  (void)rechunked; // Result is implementation-defined

  // Data should still be accessible
  EXPECT_EQ(df.NumRows(), 100);
}

// ==================== ColumnAccessor Tests ====================

TEST_F(ParquetTest, ColumnAccessorBasic)
{
  auto path = temp_dir_ / "col_basic.parquet";

  // Write test data
  {
    basis_rs::ParquetFile file(path);
    auto writer = file.SpawnWriter<NumericEntry>();
    writer.WriteRecord({1, 100, 1.5f, 2.5});
    writer.WriteRecord({2, 200, 2.5f, 3.5});
    writer.WriteRecord({3, 300, 3.5f, 4.5});
    writer.Finish();
  }

  basis_rs::DataFrame df(path);

  auto i32_col = df.GetColumn<int32_t>("i32_val");
  EXPECT_EQ(i32_col.size(), 3);
  EXPECT_GE(i32_col.NumChunks(), 1);

  auto i64_col = df.GetColumn<int64_t>("i64_val");
  EXPECT_EQ(i64_col.size(), 3);

  auto f32_col = df.GetColumn<float>("f32_val");
  EXPECT_EQ(f32_col.size(), 3);

  auto f64_col = df.GetColumn<double>("f64_val");
  EXPECT_EQ(f64_col.size(), 3);
}

TEST_F(ParquetTest, ColumnAccessorRandomAccess)
{
  auto path = temp_dir_ / "col_random.parquet";

  // Write test data
  {
    basis_rs::ParquetFile file(path);
    auto writer = file.SpawnWriter<NumericEntry>();
    for (int i = 0; i < 10; ++i)
    {
      writer.WriteRecord({i, i * 10, i * 1.0f, i * 2.0});
    }
    writer.Finish();
  }

  basis_rs::DataFrame df(path);
  auto col = df.GetColumn<int32_t>("i32_val");

  // Test random access
  EXPECT_EQ(col[0], 0);
  EXPECT_EQ(col[5], 5);
  EXPECT_EQ(col[9], 9);

  // Test out of range with at()
  EXPECT_THROW(col.at(10), std::out_of_range);
}

TEST_F(ParquetTest, ColumnAccessorIteration)
{
  auto path = temp_dir_ / "col_iter.parquet";

  // Write test data
  {
    basis_rs::ParquetFile file(path);
    auto writer = file.SpawnWriter<NumericEntry>();
    for (int i = 0; i < 100; ++i)
    {
      writer.WriteRecord({i, i, static_cast<float>(i), static_cast<double>(i)});
    }
    writer.Finish();
  }

  basis_rs::DataFrame df(path);
  auto col = df.GetColumn<int32_t>("i32_val");

  // Test seamless range-for iteration (new API)
  int64_t sum = 0;
  for (int32_t value : col)
  {
    sum += value;
  }
  EXPECT_EQ(sum, 99 * 100 / 2); // Sum of 0..99

  // Test index-based access
  int64_t sum2 = 0;
  for (size_t i = 0; i < col.size(); ++i)
  {
    sum2 += col[i];
  }
  EXPECT_EQ(sum2, 99 * 100 / 2);
}

// ==================== ColumnChunkView Tests ====================

TEST_F(ParquetTest, ColumnChunkViewBasic)
{
  auto path = temp_dir_ / "chunk_basic.parquet";

  // Write test data
  {
    basis_rs::ParquetFile file(path);
    auto writer = file.SpawnWriter<NumericEntry>();
    writer.WriteRecord({1, 100, 1.5f, 2.5});
    writer.WriteRecord({2, 200, 2.5f, 3.5});
    writer.Finish();
  }

  basis_rs::DataFrame df(path);
  auto col = df.GetColumn<int32_t>("i32_val");

  ASSERT_GE(col.NumChunks(), 1);
  const auto &chunk = col.Chunk(0);

  EXPECT_FALSE(chunk.empty());
  EXPECT_GE(chunk.size(), 1);
  EXPECT_NE(chunk.data(), nullptr);

  // Test iteration
  std::vector<int32_t> values(chunk.begin(), chunk.end());
  EXPECT_EQ(values[0], 1);
}

// ==================== ParquetWriter Tests ====================

TEST_F(ParquetTest, ParquetWriterBasic)
{
  auto path = temp_dir_ / "writer_basic.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    EXPECT_EQ(writer.BufferSize(), 2);
    writer.Finish();
  }

  EXPECT_TRUE(file.Exists());

  basis_rs::DataFrame df(path);
  EXPECT_EQ(df.NumRows(), 2);
}

TEST_F(ParquetTest, ParquetWriterDiscard)
{
  auto path = temp_dir_ / "writer_discard.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    EXPECT_EQ(writer.BufferSize(), 2);
    writer.Discard();
  }

  // File should not exist after discard
  EXPECT_FALSE(file.Exists());
}

TEST_F(ParquetTest, ParquetWriterWriteRecords)
{
  auto path = temp_dir_ / "writer_records.parquet";
  basis_rs::ParquetFile file(path);

  std::vector<SimpleEntry> entries = {
      {1, "a", 1.0},
      {2, "b", 2.0},
      {3, "c", 3.0},
  };

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecords(entries);
    EXPECT_EQ(writer.BufferSize(), 3);
    writer.Finish();
  }

  basis_rs::DataFrame df(path);
  auto records = df.ReadAllAs<SimpleEntry>();

  ASSERT_EQ(records.size(), 3);
  for (size_t i = 0; i < 3; ++i)
  {
    EXPECT_EQ(records[i].id, entries[i].id);
  }
}

TEST_F(ParquetTest, ParquetWriterAutoFinish)
{
  auto path = temp_dir_ / "writer_auto.parquet";

  {
    basis_rs::ParquetFile file(path);
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "test", 1.0});
    // Destructor should call Finish()
  }

  // File should exist after destructor
  EXPECT_TRUE(fs::exists(path));
}

// ==================== Numeric Type Tests ====================

TEST_F(ParquetTest, NumericTypes)
{
  auto path = temp_dir_ / "numeric.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<NumericEntry>();
    writer.WriteRecord({-100, 1000000000000LL, 3.14f, 2.718281828});
    writer.WriteRecord({0, 0, 0.0f, 0.0});
    writer.WriteRecord({2147483647, -9223372036854775807LL, 1.0e38f, 1.0e308});
    writer.Finish();
  }

  basis_rs::DataFrame df(path);
  auto records = df.ReadAllAs<NumericEntry>();

  ASSERT_EQ(records.size(), 3);

  EXPECT_EQ(records[0].i32_val, -100);
  EXPECT_EQ(records[0].i64_val, 1000000000000LL);
  EXPECT_NEAR(records[0].f32_val, 3.14f, 0.001f);
  EXPECT_NEAR(records[0].f64_val, 2.718281828, 0.0000001);

  EXPECT_EQ(records[1].i32_val, 0);
  EXPECT_EQ(records[1].i64_val, 0);
  EXPECT_FLOAT_EQ(records[1].f32_val, 0.0f);
  EXPECT_DOUBLE_EQ(records[1].f64_val, 0.0);
}

// ==================== String Tests ====================

TEST_F(ParquetTest, EmptyStrings)
{
  auto path = temp_dir_ / "empty_strings.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "", 0.0});
    writer.WriteRecord({2, "not empty", 1.0});
    writer.WriteRecord({3, "", 2.0});
    writer.Finish();
  }

  basis_rs::DataFrame df(path);
  auto records = df.ReadAllAs<SimpleEntry>();

  ASSERT_EQ(records.size(), 3);
  EXPECT_EQ(records[0].name, "");
  EXPECT_EQ(records[1].name, "not empty");
  EXPECT_EQ(records[2].name, "");
}

TEST_F(ParquetTest, UnicodeStrings)
{
  auto path = temp_dir_ / "unicode.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "Hello World", 1.0});
    writer.WriteRecord({2, "你好世界", 2.0});
    writer.WriteRecord({3, "🎉🎊🎈", 3.0});
    writer.Finish();
  }

  basis_rs::DataFrame df(path);
  auto records = df.ReadAllAs<SimpleEntry>();

  ASSERT_EQ(records.size(), 3);
  EXPECT_EQ(records[0].name, "Hello World");
  EXPECT_EQ(records[1].name, "你好世界");
  EXPECT_EQ(records[2].name, "🎉🎊🎈");
}

TEST_F(ParquetTest, GetStringColumn)
{
  auto path = temp_dir_ / "string_col.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 1.0});
    writer.WriteRecord({2, "bob", 2.0});
    writer.Finish();
  }

  basis_rs::DataFrame df(path);
  auto names = df.GetStringColumn("name");

  ASSERT_EQ(names.size(), 2);
  EXPECT_EQ(names[0], "alice");
  EXPECT_EQ(names[1], "bob");
}

// ==================== Large Dataset Tests ====================

TEST_F(ParquetTest, LargeDataset)
{
  auto path = temp_dir_ / "large.parquet";
  basis_rs::ParquetFile file(path);

  const size_t n = 100000;

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    for (size_t i = 0; i < n; ++i)
    {
      writer.WriteRecord(
          {static_cast<int64_t>(i), "name_" + std::to_string(i), i * 0.1});
    }
    writer.Finish();
  }

  basis_rs::DataFrame df(path);
  auto records = df.ReadAllAs<SimpleEntry>();

  ASSERT_EQ(records.size(), n);
  EXPECT_EQ(records[0].id, 0);
  EXPECT_EQ(records[0].name, "name_0");
  EXPECT_EQ(records[n - 1].id, static_cast<int64_t>(n - 1));
}

// ==================== ParquetFile Tests ====================

TEST_F(ParquetTest, FileExists)
{
  auto path = temp_dir_ / "exists.parquet";
  basis_rs::ParquetFile file(path);

  EXPECT_FALSE(file.Exists());

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "test", 1.0});
    writer.Finish();
  }

  EXPECT_TRUE(file.Exists());
}

TEST_F(ParquetTest, FilePath)
{
  auto path = temp_dir_ / "path_test.parquet";
  basis_rs::ParquetFile file(path);

  EXPECT_EQ(file.path(), path);
}

// ==================== Error Handling Tests ====================

TEST_F(ParquetTest, OpenNonExistentFile)
{
  EXPECT_THROW(basis_rs::DataFrame("/nonexistent/path/file.parquet"),
               std::exception);
}

TEST_F(ParquetTest, OpenNonExistentFileProjected)
{
  EXPECT_THROW(
      basis_rs::DataFrame("/nonexistent/path/file.parquet", {"col1"}),
      std::exception);
}

// ==================== Query Builder Tests ====================

TEST_F(ParquetTest, QuerySelectByMemberPointer)
{
  auto path = temp_dir_ / "query_select.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    writer.WriteRecord({3, "charlie", 78.5});
    writer.Finish();
  }

  auto records = file.Read<SimpleEntry>()
                     .Select(&SimpleEntry::id, &SimpleEntry::score)
                     .Collect();

  ASSERT_EQ(records.size(), 3);
  EXPECT_EQ(records[0].id, 1);
  EXPECT_DOUBLE_EQ(records[0].score, 85.5);
  EXPECT_EQ(records[0].name, ""); // not selected
}

TEST_F(ParquetTest, QuerySelectByName)
{
  auto path = temp_dir_ / "query_select_name.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    writer.Finish();
  }

  auto records = file.Read<SimpleEntry>().Select({"id", "name"}).Collect();

  ASSERT_EQ(records.size(), 2);
  EXPECT_EQ(records[0].id, 1);
  EXPECT_EQ(records[0].name, "alice");
  EXPECT_DOUBLE_EQ(records[0].score, 0.0); // default
}

TEST_F(ParquetTest, QueryFilter)
{
  auto path = temp_dir_ / "query_filter.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    writer.WriteRecord({3, "charlie", 78.5});
    writer.Finish();
  }

  auto records = file.Read<SimpleEntry>()
                     .Filter(&SimpleEntry::score, basis_rs::Gt, 80.0)
                     .Collect();

  ASSERT_EQ(records.size(), 2);
  EXPECT_EQ(records[0].name, "alice");
  EXPECT_EQ(records[1].name, "bob");
}

TEST_F(ParquetTest, QuerySelectAndFilter)
{
  auto path = temp_dir_ / "query_both.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    writer.WriteRecord({3, "charlie", 78.5});
    writer.Finish();
  }

  auto records = file.Read<SimpleEntry>()
                     .Select(&SimpleEntry::id, &SimpleEntry::score)
                     .Filter(&SimpleEntry::score, basis_rs::Gt, 80.0)
                     .Collect();

  ASSERT_EQ(records.size(), 2);
  EXPECT_EQ(records[0].id, 1);
  EXPECT_DOUBLE_EQ(records[0].score, 85.5);
  EXPECT_EQ(records[0].name, ""); // not selected
}

TEST_F(ParquetTest, QueryMultipleFilters)
{
  auto path = temp_dir_ / "query_multi_filter.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    writer.WriteRecord({3, "charlie", 78.5});
    writer.WriteRecord({4, "diana", 95.0});
    writer.Finish();
  }

  // score > 80 AND score < 93
  auto records = file.Read<SimpleEntry>()
                     .Filter(&SimpleEntry::score, basis_rs::Gt, 80.0)
                     .Filter(&SimpleEntry::score, basis_rs::Lt, 93.0)
                     .Collect();

  ASSERT_EQ(records.size(), 2); // alice (85.5) and bob (92.0)
  EXPECT_EQ(records[0].name, "alice");
  EXPECT_EQ(records[1].name, "bob");
}

TEST_F(ParquetTest, QueryNoFilterNoSelect)
{
  auto path = temp_dir_ / "query_identity.parquet";
  basis_rs::ParquetFile file(path);

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.Finish();
  }

  // Query with no Select/Filter should return all records
  auto records = file.Read<SimpleEntry>().Collect();

  ASSERT_EQ(records.size(), 1);
  EXPECT_EQ(records[0].id, 1);
  EXPECT_EQ(records[0].name, "alice");
  EXPECT_DOUBLE_EQ(records[0].score, 85.5);
}
