#pragma once

#include <cstddef>
#include <stdexcept>
#include <vector>

namespace basis_rs {

/// A view into a contiguous chunk of column data.
/// Does not own the data - valid only while the DataFrame is alive.
template <typename T>
class ColumnChunkView {
 public:
  ColumnChunkView(const T* data, size_t size) : data_(data), size_(size) {}

  const T* data() const { return data_; }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  const T& operator[](size_t i) const { return data_[i]; }

  const T* begin() const { return data_; }
  const T* end() const { return data_ + size_; }

 private:
  const T* data_;
  size_t size_;
};

/// A column accessor that provides zero-copy access to column data.
/// The column may be stored in multiple chunks (row groups).
template <typename T>
class ColumnAccessor {
 public:
  ColumnAccessor() = default;

  void AddChunk(const T* data, size_t size) {
    chunks_.emplace_back(data, size);
    total_size_ += size;
  }

  /// Number of chunks (usually equals number of row groups in parquet file)
  size_t NumChunks() const { return chunks_.size(); }

  /// Total number of elements across all chunks
  size_t Size() const { return total_size_; }

  /// Access a specific chunk
  const ColumnChunkView<T>& Chunk(size_t i) const { return chunks_[i]; }

  /// Random access by global index (slower than chunk iteration)
  T operator[](size_t global_idx) const {
    size_t offset = 0;
    for (const auto& chunk : chunks_) {
      if (global_idx < offset + chunk.size()) {
        return chunk[global_idx - offset];
      }
      offset += chunk.size();
    }
    throw std::out_of_range("Index out of range");
  }

  /// Iterator over chunks
  auto begin() const { return chunks_.begin(); }
  auto end() const { return chunks_.end(); }

 private:
  std::vector<ColumnChunkView<T>> chunks_;
  size_t total_size_ = 0;
};

}  // namespace basis_rs
