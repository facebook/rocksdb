//  Copyright (c) Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <assert.h>

#include <cstddef>
#include <cstdint>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// This is a data structure specifically designed as a "Set" for a
// pretty small scale of Enum structure. For now, it can support up
// to 64 element, and it is expandable in the future.
template <typename ENUM_TYPE, ENUM_TYPE MAX_VALUE>
class SmallEnumSet {
 public:
  SmallEnumSet() : state_(0) {}

  ~SmallEnumSet() {}

  // Return true if the input enum is included in the "Set" (i.e., changes the
  // internal scalar state successfully), otherwise, it will return false.
  bool Add(const ENUM_TYPE value) {
    static_assert(MAX_VALUE <= 63, "Size currently limited to 64");
    assert(value >= 0 && value <= MAX_VALUE);
    uint64_t old_state = state_;
    uint64_t tmp = 1;
    state_ |= (tmp << value);
    return old_state != state_;
  }

  // Return true if the input enum is contained in the "Set".
  bool Contains(const ENUM_TYPE value) {
    static_assert(MAX_VALUE <= 63, "Size currently limited to 64");
    assert(value >= 0 && value <= MAX_VALUE);
    uint64_t tmp = 1;
    return state_ & (tmp << value);
  }

 private:
  uint64_t state_;
};

// An abstract class for a sequence of items that are materialized
// on demand. This allows for a convenient, memory-inefficient public
// interface for each item while the full sequence data can be stored
// in a memory-efficient way with hidden details. For simplicity,
// foreign language bindings can treat this structure as a vector by
// immediately materializing all the elements with ToVector().
template <typename Item>
class OnDemandSequence {
 public:
  OnDemandSequence() {}
  // Disallow copy.
  OnDemandSequence(const OnDemandSequence&) = delete;
  OnDemandSequence& operator=(const OnDemandSequence&) = delete;

  virtual ~OnDemandSequence() {}

  // STL-like API
  virtual size_t size() = 0;
  virtual bool empty() { return size() == 0; }

  virtual Item operator[](size_t n) = 0;

  template <class IterSequence, typename IterItem>
  struct IteratorImpl {
    IterSequence* parent;
    size_t offset;

    IteratorImpl(IterSequence* _parent, size_t _offset)
        : parent(_parent), offset(_offset) {}

    using Self = IteratorImpl<IterSequence, IterItem>;
    Self& operator++() {
      ++offset;
      return *this;
    }
    Self operator++(int) {
      auto old = *this;
      ++offset;
      return old;
    }
    Self& operator--() {
      --offset;
      return *this;
    }
    Self operator--(int) {
      auto old = *this;
      --offset;
      return old;
    }
    Self operator-(size_t len) const { return Self(parent, offset - len); }
    ptrdiff_t operator-(const Self& other) const {
      assert(parent == other.parent);
      return offset - other.offset;
    }
    Self operator+(size_t len) const { return Self(parent, offset + len); }
    Self& operator+=(size_t len) {
      offset += len;
      return *this;
    }
    Self& operator-=(size_t len) {
      offset -= len;
      return *this;
    }

    IterItem operator*() const { return (*parent)[offset]; }
    IterItem operator[](size_t len) const { return *(*this + len); }

    bool operator==(const Self& other) const {
      assert(parent == other.parent);
      return offset == other.offset;
    }
    bool operator!=(const Self& other) const { return !(*this == other); }
    bool operator>(const Self& other) const {
      assert(parent == other.parent);
      return offset > other.offset;
    }
    bool operator<(const Self& other) const {
      assert(parent == other.parent);
      return offset < other.offset;
    }
    bool operator>=(const Self& other) const {
      assert(parent == other.parent);
      return offset >= other.offset;
    }
    bool operator<=(const Self& other) const {
      assert(parent == other.parent);
      return offset <= other.offset;
    }
  };

  using Iterator = IteratorImpl<OnDemandSequence<Item>, Item>;
  using ConstIterator = IteratorImpl<const OnDemandSequence<Item>, const Item>;

  Iterator begin() { return Iterator(this, 0); }
  Iterator end() { return Iterator(this, size()); }
  ConstIterator begin() const { return ConstIterator(this, 0); }
  ConstIterator end() const { return ConstIterator(this, size()); }

  std::vector<Item> ToVector() {
    std::vector<Item> rv;
    size_t count = size();
    rv.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      rv[i] = (*this)[i];
    }
    return rv;
  }
};

}  // namespace ROCKSDB_NAMESPACE
