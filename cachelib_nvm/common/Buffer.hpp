#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <memory>
#include <utility>

namespace facebook {
namespace cachelib {
class Buffer {
 public:
  // Note: trailing 0 is not copied
  static Buffer fromStringZ(const char* strz) {
    return Buffer(std::strlen(strz), reinterpret_cast<const uint8_t*>(strz));
  }

  static Buffer takeOwnership(uint32_t size, uint8_t *data) {
    assert(data != nullptr);
    Buffer buf;
    buf.size_ = size;
    buf.owns_ = true;
    buf.data_ = data;
    return buf;
  }

  static Buffer wrap(uint32_t size, uint8_t *data) {
    assert(data != nullptr);
    Buffer buf;
    buf.size_ = size;
    buf.owns_ = false;
    buf.data_ = data;
    return buf;
  }

  Buffer() = default;
  explicit Buffer(uint32_t size, const uint8_t *data = nullptr)
      : size_{size},
        owns_{true},
        data_{new uint8_t[size]} {
    if (data) {
      std::memcpy(data_, data, size_);
    }
  }

  // Use @copy instead: to control where we copy
  Buffer(const Buffer&) = delete;
  Buffer& operator=(const Buffer&) = delete;

  Buffer(Buffer&& other) noexcept
      : size_{other.size_},
        owns_{other.owns_},
        data_{other.data_} {
    other.data_ = nullptr;
  }

  Buffer& operator=(Buffer&& other) {
    release();
    size_ = other.size_;
    owns_ = other.owns_;
    data_ = other.data_;
    other.data_ = nullptr;
    return *this;
  }

  ~Buffer() { release(); }

  bool isNull() const {
    return data_ == nullptr;
  }

  const uint8_t* data() const {
    return data_;
  }

  uint8_t* data() {
    return data_;
  }

  uint32_t size() const {
    return size_;
  }

  Buffer copy() const {
    return Buffer{size_, data_};
  }

  Buffer copy(uint32_t offset, uint32_t size) const {
    return Buffer{size, data_ + offset};
  }

  Buffer wrap() const {
    return wrap(size_, data_);
  }

  bool operator==(const Buffer& other) const {
    return size_ == other.size_ &&
           std::equal(data_, data_ + size_, other.data_);
  }

  bool operator!=(const Buffer& other) const {
    return !(*this == other);
  }

 private:
  void release() {
    if (owns_) {
      delete[] data_;
      data_ = nullptr;
    }
  }

  uint32_t size_{};
  bool owns_{true};
  uint8_t* data_{};
};

// Check size if small (two registers)
static_assert(sizeof(Buffer) == 16, "invalid buffer size");
}
}
