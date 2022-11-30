// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>  // RocksDB now requires C++17 support

#include "rocksdb/cleanable.h"

namespace ROCKSDB_NAMESPACE {

class Slice {
 public:
  // Create an empty slice.
  Slice() : data_(""), size_(0) {}

  // Create a slice that refers to d[0,n-1].
  Slice(const char* d, size_t n) : data_(d), size_(n) {}

  // Create a slice that refers to the contents of "s"
  /* implicit */
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

  // Create a slice that refers to the same contents as "sv"
  /* implicit */
  Slice(const std::string_view& sv) : data_(sv.data()), size_(sv.size()) {}

  // Create a slice that refers to s[0,strlen(s)-1]
  /* implicit */
  Slice(const char* s) : data_(s) { size_ = (s == nullptr) ? 0 : strlen(s); }

  // Create a single slice from SliceParts using buf as storage.
  // buf must exist as long as the returned Slice exists.
  Slice(const struct SliceParts& parts, std::string* buf);

  virtual ~Slice() = default;

  // Return a pointer to the beginning of the referenced data
  virtual const char* data() const { return data_; }

  // Return the length (in bytes) of the referenced data
  virtual size_t size() const { return size_; }

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() {
    data_ = "";
    size_ = 0;
  }

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  void remove_suffix(size_t n) {
    assert(n <= size());
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
  // when hex is true, returns a string of twice the length hex encoded (0-9A-F)
  std::string ToString(bool hex = false) const;

  // Return a string_view that references the same data as this slice.
  std::string_view ToStringView() const {
    return std::string_view(data_, size_);
  }

  // Decodes the current slice interpreted as an hexadecimal string into result,
  // if successful returns true, if this isn't a valid hex string
  // (e.g not coming from Slice::ToString(true)) DecodeHex returns false.
  // This slice is expected to have an even number of 0-9A-F characters
  // also accepts lowercase (a-f)
  bool DecodeHex(std::string* result) const;

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const Slice& x) const {
    return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
  }

  bool ends_with(const Slice& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_ + size_ - x.size_, x.data_, x.size_) == 0));
  }

  // Compare two slices and returns the first byte where they differ
  size_t difference_offset(const Slice& b) const;

  // private: make these public for rocksdbjni access
  const char* data_;
  size_t size_;

  // Intentionally copyable
};

/**
 * @brief abstraction of target for data copy into an unpinnable slice
 *
 */
class ValueSink {
 public:
  virtual ~ValueSink() = default;

  virtual void Assign(const char* /*data*/, size_t /*size*/) = 0;
  virtual void Move(const std::string&& /* buf */) = 0;
  virtual void RemovePrefix(size_t len) = 0;
  virtual void RemoveSuffix(size_t len) = 0;
  virtual bool IsEmpty() = 0;
  virtual size_t Size() = 0;
  virtual const char* Data() = 0;

 protected:
  friend class PinnableSlice;
};

/**
 * @brief value sink where the target is a std::string
 * std::string used always to be the target
 * we can do better for other cases, e.g. Java API, where we previously needed
 * to go via a std::string to end up copying the data into a Java byte array
 * or ByteBuffer
 *
 */
class StringValueSink : public ValueSink {
 private:
  std::string* s_;

 public:
  StringValueSink(std::string* buf) : s_(buf){};

  inline void Assign(const char* data, size_t size) { s_->assign(data, size); };
  inline void Move(const std::string&& buf) {
    assert(s_ != nullptr);
    *s_ = std::move(buf);
  };
  inline void Erase(size_t pos, size_t len) {
    s_->erase(pos, len);
  }
  inline void RemovePrefix(size_t len) {
    s_->erase(0, len);
  };
  virtual void RemoveSuffix(size_t len) {
    s_->erase(s_->size() - len, len);
  };
  

  inline bool IsEmpty() { return (s_ == nullptr); };

  inline size_t Size() { return s_->size(); };

  inline const char* Data() { return s_->data(); };
};

static StringValueSink empty_value_sink(nullptr);

/**
 * A Slice that can be pinned with some cleanup tasks, which will be run upon
 * ::Reset() or object destruction, whichever is invoked first. This can be
 * used to avoid memcpy by having the PinnableSlice object referring to the
 * data that is locked in the memory and release them after the data is
 * consumed.
 */
class PinnableSlice : public Slice, public Cleanable {
 public:
  PinnableSlice()
      : my_string_(std::make_unique<std::string>("")),
        my_value_sink_(std::make_unique<StringValueSink>(my_string_.get())),
        value_sink_(my_value_sink_.get()){};
  explicit PinnableSlice(ValueSink* value_sink) : value_sink_(value_sink){};

  PinnableSlice(PinnableSlice&& other);
  PinnableSlice& operator=(PinnableSlice&& other);

  // No copy constructor and copy assignment allowed.
  PinnableSlice(PinnableSlice&) = delete;
  PinnableSlice& operator=(PinnableSlice&) = delete;

  inline void PinSlice(const Slice& s, CleanupFunction f, void* arg1,
                       void* arg2) {
    assert(!pinned_);
    pinned_ = true;
    data_ = s.data();
    size_ = s.size();
    RegisterCleanup(f, arg1, arg2);
    assert(pinned_);
  }

  inline void PinSlice(const Slice& s, Cleanable* cleanable) {
    assert(!pinned_);
    pinned_ = true;
    data_ = s.data();
    size_ = s.size();
    if (cleanable != nullptr) {
      cleanable->DelegateCleanupsTo(this);
    }
    assert(pinned_);
  }

  inline void PinSelf(const Slice& slice) {
    assert(!pinned_);
    value_sink_->Assign(slice.data(), slice.size());
    size_ = value_sink_->Size();
    assert(!pinned_);
  }

  inline void PinSelf() {
    assert(!pinned_);
    size_ = value_sink_->Size();
    assert(!pinned_);
  }

  void remove_suffix(size_t n) {
    assert(n <= size());
    if (pinned_) {
      size_ -= n;
    } else {
      value_sink_->RemoveSuffix(n);
      PinSelf();
    }
  }

  void remove_prefix(size_t n) {
    assert(n <= size());
    if (pinned_) {
      data_ += n;
      size_ -= n;
    } else {
      value_sink_->RemovePrefix(n);
      PinSelf();
    }
  }

  void Reset() {
    Cleanable::Reset();
    pinned_ = false;
    size_ = 0;
  }

  // Return a pointer to the beginning of the referenced data
  const char* data() const { if (pinned_) {
    return data_;
  } else {
    return value_sink_->Data(); }
  }

  // Return the length (in bytes) of the referenced data
  size_t size() const { if (pinned_) {
    return size_;
  } else {
    return value_sink_->Size();
  }
  }

  inline ValueSink& GetSelf() { return *value_sink_; }

  inline bool IsPinned() const { return pinned_; }

 private:
  friend class PinnableSlice4Test;
  std::unique_ptr<std::string> my_string_;
  std::unique_ptr<StringValueSink> my_value_sink_;
  ValueSink* value_sink_;

 protected:
  bool pinned_ = false;
};

// A set of Slices that are virtually concatenated together.  'parts' points
// to an array of Slices.  The number of elements in the array is 'num_parts'.
struct SliceParts {
  SliceParts(const Slice* _parts, int _num_parts)
      : parts(_parts), num_parts(_num_parts) {}
  SliceParts() : parts(nullptr), num_parts(0) {}

  const Slice* parts;
  int num_parts;
};

inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }

inline int Slice::compare(const Slice& b) const {
  assert(data_ != nullptr && b.data_ != nullptr);
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_)
      r = -1;
    else if (size_ > b.size_)
      r = +1;
  }
  return r;
}

inline size_t Slice::difference_offset(const Slice& b) const {
  size_t off = 0;
  const size_t len = (size_ < b.size_) ? size_ : b.size_;
  for (; off < len; off++) {
    if (data_[off] != b.data_[off]) break;
  }
  return off;
}

}  // namespace ROCKSDB_NAMESPACE
