#include "basis_rs.hpp"
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>

namespace fs = std::filesystem;

class ParquetTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    // Create a temporary directory for test files
    temp_dir_ = fs::temp_directory_path() / "basis_rs_test";
    fs::create_directories(temp_dir_);
  }

  void TearDown() override
  {
    // Clean up temporary directory
    fs::remove_all(temp_dir_);
  }

  fs::path temp_dir_;
};

TEST_F(ParquetTest, CreateEmptyDataFrame)
{
  basis::DataFrame df;
  EXPECT_EQ(df.height(), 0);
  EXPECT_EQ(df.width(), 0);
}

TEST_F(ParquetTest, AddInt64Column)
{
  basis::DataFrame df;

  std::vector<int64_t> data = {1, 2, 3, 4, 5};
  df.add_column("id", data);

  EXPECT_EQ(df.height(), 5);
  EXPECT_EQ(df.width(), 1);

  auto retrieved = df.get_int64_column("id");
  EXPECT_EQ(retrieved, data);
}

TEST_F(ParquetTest, AddFloat64Column)
{
  basis::DataFrame df;

  std::vector<double> data = {1.1, 2.2, 3.3, 4.4, 5.5};
  df.add_column("score", data);

  EXPECT_EQ(df.height(), 5);
  EXPECT_EQ(df.width(), 1);

  auto retrieved = df.get_float64_column("score");
  ASSERT_EQ(retrieved.size(), data.size());
  for (size_t i = 0; i < data.size(); ++i)
  {
    EXPECT_DOUBLE_EQ(retrieved[i], data[i]);
  }
}

TEST_F(ParquetTest, AddStringColumn)
{
  basis::DataFrame df;

  std::vector<std::string> data = {"alice", "bob", "charlie", "diana", "eve"};
  df.add_column("name", data);

  EXPECT_EQ(df.height(), 5);
  EXPECT_EQ(df.width(), 1);

  auto retrieved = df.get_string_column("name");
  EXPECT_EQ(retrieved, data);
}

TEST_F(ParquetTest, AddBoolColumn)
{
  basis::DataFrame df;

  std::vector<bool> data = {true, false, true, false, true};
  df.add_column("active", data);

  EXPECT_EQ(df.height(), 5);
  EXPECT_EQ(df.width(), 1);

  auto retrieved = df.get_bool_column("active");
  EXPECT_EQ(retrieved, data);
}

TEST_F(ParquetTest, MultipleColumns)
{
  basis::DataFrame df;

  std::vector<int64_t> ids = {1, 2, 3};
  std::vector<std::string> names = {"a", "b", "c"};
  std::vector<double> scores = {85.5, 92.0, 78.5};

  df.add_column("id", ids);
  df.add_column("name", names);
  df.add_column("score", scores);

  EXPECT_EQ(df.height(), 3);
  EXPECT_EQ(df.width(), 3);

  EXPECT_EQ(df.get_int64_column("id"), ids);
  EXPECT_EQ(df.get_string_column("name"), names);
}

TEST_F(ParquetTest, WriteAndReadParquet)
{
  auto path = temp_dir_ / "test.parquet";

  // Create and write
  {
    basis::DataFrame df;

    std::vector<int64_t> ids = {1, 2, 3, 4, 5};
    std::vector<std::string> names = {"alice", "bob", "charlie", "diana",
                                      "eve"};
    std::vector<double> scores = {85.5, 92.0, 78.5, 95.0, 88.5};

    df.add_column("id", ids);
    df.add_column("name", names);
    df.add_column("score", scores);

    df.write_parquet(path.string());
  }

  // Read back
  {
    auto df = basis::DataFrame::read_parquet(path.string());

    EXPECT_EQ(df.height(), 5);
    EXPECT_EQ(df.width(), 3);

    auto ids = df.get_int64_column("id");
    EXPECT_EQ(ids.size(), 5);
    EXPECT_EQ(ids[0], 1);
    EXPECT_EQ(ids[4], 5);

    auto names = df.get_string_column("name");
    EXPECT_EQ(names[0], "alice");
    EXPECT_EQ(names[4], "eve");

    auto scores = df.get_float64_column("score");
    EXPECT_DOUBLE_EQ(scores[0], 85.5);
    EXPECT_DOUBLE_EQ(scores[2], 78.5);
  }
}

TEST_F(ParquetTest, ReadNonExistentFile)
{
  EXPECT_THROW(basis::DataFrame::read_parquet("/nonexistent/path.parquet"),
               basis::BasisError);
}

TEST_F(ParquetTest, GetNonExistentColumn)
{
  basis::DataFrame df;
  std::vector<int64_t> data = {1, 2, 3};
  df.add_column("id", data);

  EXPECT_THROW(df.get_int64_column("nonexistent"), basis::BasisError);
}

TEST_F(ParquetTest, TypeMismatch)
{
  basis::DataFrame df;
  std::vector<int64_t> data = {1, 2, 3};
  df.add_column("id", data);

  // Trying to get int64 column as float64 should fail
  EXPECT_THROW(df.get_float64_column("id"), basis::BasisError);
}

TEST_F(ParquetTest, LargeDataset)
{
  auto path = temp_dir_ / "large.parquet";

  const size_t n = 100000;

  // Create large dataset
  {
    basis::DataFrame df;

    std::vector<int64_t> ids(n);
    std::vector<double> values(n);

    for (size_t i = 0; i < n; ++i)
    {
      ids[i] = static_cast<int64_t>(i);
      values[i] = static_cast<double>(i) * 0.1;
    }

    df.add_column("id", ids);
    df.add_column("value", values);
    df.write_parquet(path.string());
  }

  // Read back and verify
  {
    auto df = basis::DataFrame::read_parquet(path.string());
    EXPECT_EQ(df.height(), n);

    auto ids = df.get_int64_column("id");
    EXPECT_EQ(ids[0], 0);
    EXPECT_EQ(ids[n - 1], static_cast<int64_t>(n - 1));

    auto values = df.get_float64_column("value");
    EXPECT_DOUBLE_EQ(values[0], 0.0);
    EXPECT_NEAR(values[n - 1], (n - 1) * 0.1, 0.001);
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
