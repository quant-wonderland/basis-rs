#include <basis_rs/parquet.hpp>
#include <cstdio>
#include <filesystem>
#include <gtest/gtest.h>

namespace fs = std::filesystem;

// ==================== Test Data Structures ====================

struct SimpleEntry {
  int64_t id;
  std::string name;
  double score;
};

struct NumericEntry {
  int32_t i32_val;
  int64_t i64_val;
  float f32_val;
  double f64_val;
};

struct BoolEntry {
  int64_t id;
  bool active;
  bool verified;
};

// ==================== Codec Registrations ====================

template <>
inline const basis_rs::ParquetCodec<SimpleEntry>& basis_rs::GetParquetCodec() {
  static basis_rs::ParquetCodec<SimpleEntry> codec = []() {
    basis_rs::ParquetCodec<SimpleEntry> c;
    c.Add("id", &SimpleEntry::id);
    c.Add("name", &SimpleEntry::name);
    c.Add("score", &SimpleEntry::score);
    return c;
  }();
  return codec;
}

template <>
inline const basis_rs::ParquetCodec<NumericEntry>& basis_rs::GetParquetCodec() {
  static basis_rs::ParquetCodec<NumericEntry> codec = []() {
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
inline const basis_rs::ParquetCodec<BoolEntry>& basis_rs::GetParquetCodec() {
  static basis_rs::ParquetCodec<BoolEntry> codec = []() {
    basis_rs::ParquetCodec<BoolEntry> c;
    c.Add("id", &BoolEntry::id);
    c.Add("active", &BoolEntry::active);
    c.Add("verified", &BoolEntry::verified);
    return c;
  }();
  return codec;
}

// ==================== Test Fixture ====================

class ParquetCodecTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ = fs::temp_directory_path() / "basis_rs_codec_test";
    fs::create_directories(temp_dir_);
  }

  void TearDown() override { fs::remove_all(temp_dir_); }

  fs::path temp_dir_;
};

// ==================== Basic Tests ====================

TEST_F(ParquetCodecTest, SimpleRoundtrip) {
  auto path = temp_dir_ / "simple.parquet";
  basis_rs::ParquetFile file(path);

  // Write
  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "alice", 85.5});
    writer.WriteRecord({2, "bob", 92.0});
    writer.WriteRecord({3, "charlie", 78.5});
  }

  // Read
  auto records = file.ReadAll<SimpleEntry>();

  ASSERT_EQ(records.size(), 3);

  EXPECT_EQ(records[0].id, 1);
  EXPECT_EQ(records[0].name, "alice");
  EXPECT_DOUBLE_EQ(records[0].score, 85.5);

  EXPECT_EQ(records[1].id, 2);
  EXPECT_EQ(records[1].name, "bob");
  EXPECT_DOUBLE_EQ(records[1].score, 92.0);

  EXPECT_EQ(records[2].id, 3);
  EXPECT_EQ(records[2].name, "charlie");
  EXPECT_DOUBLE_EQ(records[2].score, 78.5);
}

TEST_F(ParquetCodecTest, NumericTypes) {
  auto path = temp_dir_ / "numeric.parquet";
  basis_rs::ParquetFile file(path);

  // Write
  {
    auto writer = file.SpawnWriter<NumericEntry>();
    writer.WriteRecord({-100, 1000000000000LL, 3.14f, 2.718281828});
    writer.WriteRecord({0, 0, 0.0f, 0.0});
    writer.WriteRecord({2147483647, -9223372036854775807LL, 1.0e38f, 1.0e308});
  }

  // Read
  auto records = file.ReadAll<NumericEntry>();

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

TEST_F(ParquetCodecTest, BoolColumns) {
  auto path = temp_dir_ / "bool.parquet";
  basis_rs::ParquetFile file(path);

  // Write
  {
    auto writer = file.SpawnWriter<BoolEntry>();
    writer.WriteRecord({1, true, false});
    writer.WriteRecord({2, false, true});
    writer.WriteRecord({3, true, true});
    writer.WriteRecord({4, false, false});
  }

  // Read
  auto records = file.ReadAll<BoolEntry>();

  ASSERT_EQ(records.size(), 4);

  EXPECT_TRUE(records[0].active);
  EXPECT_FALSE(records[0].verified);

  EXPECT_FALSE(records[1].active);
  EXPECT_TRUE(records[1].verified);

  EXPECT_TRUE(records[2].active);
  EXPECT_TRUE(records[2].verified);

  EXPECT_FALSE(records[3].active);
  EXPECT_FALSE(records[3].verified);
}

TEST_F(ParquetCodecTest, EmptyStrings) {
  auto path = temp_dir_ / "empty_strings.parquet";
  basis_rs::ParquetFile file(path);

  // Write with empty strings
  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "", 0.0});
    writer.WriteRecord({2, "not empty", 1.0});
    writer.WriteRecord({3, "", 2.0});
  }

  // Read
  auto records = file.ReadAll<SimpleEntry>();

  ASSERT_EQ(records.size(), 3);
  EXPECT_EQ(records[0].name, "");
  EXPECT_EQ(records[1].name, "not empty");
  EXPECT_EQ(records[2].name, "");
}

TEST_F(ParquetCodecTest, UnicodeStrings) {
  auto path = temp_dir_ / "unicode.parquet";
  basis_rs::ParquetFile file(path);

  // Write with Unicode strings
  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "Hello World", 1.0});
    writer.WriteRecord({2, "你好世界", 2.0});
    writer.WriteRecord({3, "🎉🎊🎈", 3.0});
  }

  // Read
  auto records = file.ReadAll<SimpleEntry>();

  ASSERT_EQ(records.size(), 3);
  EXPECT_EQ(records[0].name, "Hello World");
  EXPECT_EQ(records[1].name, "你好世界");
  EXPECT_EQ(records[2].name, "🎉🎊🎈");
}

TEST_F(ParquetCodecTest, WriteRecords) {
  auto path = temp_dir_ / "write_records.parquet";
  basis_rs::ParquetFile file(path);

  // Write using WriteRecords
  std::vector<SimpleEntry> entries = {
      {1, "a", 1.0}, {2, "b", 2.0}, {3, "c", 3.0}, {4, "d", 4.0}, {5, "e", 5.0},
  };

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecords(entries);
  }

  // Read
  auto records = file.ReadAll<SimpleEntry>();

  ASSERT_EQ(records.size(), 5);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(records[i].id, entries[i].id);
    EXPECT_EQ(records[i].name, entries[i].name);
    EXPECT_DOUBLE_EQ(records[i].score, entries[i].score);
  }
}

TEST_F(ParquetCodecTest, LargeDataset) {
  auto path = temp_dir_ / "large.parquet";
  basis_rs::ParquetFile file(path);

  const size_t n = 100000;

  // Write large dataset
  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    for (size_t i = 0; i < n; ++i) {
      writer.WriteRecord(
          {static_cast<int64_t>(i), "name_" + std::to_string(i), i * 0.1});
    }
  }

  // Read
  auto records = file.ReadAll<SimpleEntry>();

  ASSERT_EQ(records.size(), n);

  // Spot check
  EXPECT_EQ(records[0].id, 0);
  EXPECT_EQ(records[0].name, "name_0");

  EXPECT_EQ(records[n - 1].id, static_cast<int64_t>(n - 1));
  EXPECT_EQ(records[n - 1].name, "name_" + std::to_string(n - 1));
}

TEST_F(ParquetCodecTest, FileExists) {
  auto path = temp_dir_ / "exists.parquet";
  basis_rs::ParquetFile file(path);

  EXPECT_FALSE(file.Exists());

  {
    auto writer = file.SpawnWriter<SimpleEntry>();
    writer.WriteRecord({1, "test", 1.0});
  }

  EXPECT_TRUE(file.Exists());
}

TEST_F(ParquetCodecTest, ReadNonExistentFile) {
  basis_rs::ParquetFile file("/nonexistent/path/file.parquet");

  EXPECT_THROW(file.ReadAll<SimpleEntry>(), std::exception);
}
