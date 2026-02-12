#pragma once

#include <algorithm>
#include <cstddef>
#include <iterator>
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

/// Forward iterator that seamlessly traverses across multiple chunks.
template <typename T>
class ColumnIterator {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = T;
  using difference_type = std::ptrdiff_t;
  using pointer = const T*;
  using reference = const T&;

  ColumnIterator() = default;

  ColumnIterator(const std::vector<ColumnChunkView<T>>* chunks, size_t chunk_idx,
                 size_t elem_idx)
      : chunks_(chunks), chunk_idx_(chunk_idx), elem_idx_(elem_idx) {
    SkipEmptyChunks();
  }

  reference operator*() const { return (*chunks_)[chunk_idx_][elem_idx_]; }

  pointer operator->() const { return &(*chunks_)[chunk_idx_][elem_idx_]; }

  ColumnIterator& operator++() {
    ++elem_idx_;
    if (elem_idx_ >= (*chunks_)[chunk_idx_].size()) {
      ++chunk_idx_;
      elem_idx_ = 0;
      SkipEmptyChunks();
    }
    return *this;
  }

  ColumnIterator operator++(int) {
    ColumnIterator tmp = *this;
    ++(*this);
    return tmp;
  }

  bool operator==(const ColumnIterator& other) const {
    return chunk_idx_ == other.chunk_idx_ && elem_idx_ == other.elem_idx_;
  }

  bool operator!=(const ColumnIterator& other) const {
    return !(*this == other);
  }

 private:
  void SkipEmptyChunks() {
    while (chunk_idx_ < chunks_->size() &&
           (*chunks_)[chunk_idx_].size() == 0) {
      ++chunk_idx_;
    }
  }

  const std::vector<ColumnChunkView<T>>* chunks_ = nullptr;
  size_t chunk_idx_ = 0;
  size_t elem_idx_ = 0;
};

/// A column accessor that provides zero-copy access to column data.
/// Supports seamless iteration across multiple chunks (row groups).
///
/// Usage:
///   auto col = df.GetColumn<float>("Close");
///
///   // Simple range-for loop (recommended)
///   for (float value : col) {
///       sum += value;
///   }
///
///   // Index access
///   for (size_t i = 0; i < col.size(); ++i) {
///       process(col[i]);
///   }
template <typename T>
class ColumnAccessor {
 public:
  using iterator = ColumnIterator<T>;
  using const_iterator = ColumnIterator<T>;

  ColumnAccessor() = default;

  void AddChunk(const T* data, size_t size) {
    if (size > 0) {
      chunks_.emplace_back(data, size);
      total_size_ += size;
      // Rebuild prefix sums for O(log n) index lookup
      chunk_offsets_.push_back(total_size_);
    }
  }

  /// Total number of elements across all chunks
  size_t size() const { return total_size_; }
  bool empty() const { return total_size_ == 0; }

  /// Random access by index - O(log n) chunk lookup + O(1) element access
  const T& operator[](size_t idx) const {
    // Binary search to find the right chunk
    auto it =
        std::upper_bound(chunk_offsets_.begin(), chunk_offsets_.end(), idx);
    size_t chunk_idx = it - chunk_offsets_.begin();

    size_t offset = (chunk_idx == 0) ? 0 : chunk_offsets_[chunk_idx - 1];
    return chunks_[chunk_idx][idx - offset];
  }

  /// at() with bounds checking
  const T& at(size_t idx) const {
    if (idx >= total_size_) {
      throw std::out_of_range("ColumnAccessor index out of range");
    }
    return (*this)[idx];
  }

  /// Iterator to beginning - enables range-for loops
  iterator begin() const { return iterator(&chunks_, 0, 0); }

  /// Iterator to end
  iterator end() const { return iterator(&chunks_, chunks_.size(), 0); }

  // ==================== Advanced API ====================

  /// Number of chunks (usually equals number of row groups)
  size_t NumChunks() const { return chunks_.size(); }

  /// Access a specific chunk (for advanced users who need chunk-aware access)
  const ColumnChunkView<T>& Chunk(size_t i) const { return chunks_[i]; }

 private:
  std::vector<ColumnChunkView<T>> chunks_;
  std::vector<size_t> chunk_offsets_;  // Prefix sums for O(log n) lookup
  size_t total_size_ = 0;
};

}  // namespace basis_rs
