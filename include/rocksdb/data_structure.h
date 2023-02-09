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

namespace detail {
int CountTrailingZeroBitsForSmallEnumSet(uint64_t);
}  // namespace detail

// Represents a set of values of some enum type with a small number of
// possible enumerators. For now, it supports enums where no enumerator
// exceeds 63 when converted to int.
template <typename ENUM_TYPE, ENUM_TYPE MAX_ENUMERATOR>
class SmallEnumSet {
 private:
  using StateT = uint64_t;
  static constexpr int kStateBits = sizeof(StateT) * 8;
  static constexpr int kMaxMax = kStateBits - 1;
  static constexpr int kMaxValue = static_cast<int>(MAX_ENUMERATOR);
  static_assert(kMaxValue >= 0);
  static_assert(kMaxValue <= kMaxMax);

 public:
  // construct / create
  SmallEnumSet() : state_(0) {}

  template <class... TRest>
  /*implicit*/ constexpr SmallEnumSet(const ENUM_TYPE e, TRest... rest) {
    *this = SmallEnumSet(rest...).With(e);
  }

  // Return the set that includes all valid values, assuming the enum
  // is "dense" (includes all values converting to 0 through kMaxValue)
  static constexpr SmallEnumSet All() {
    StateT tmp = StateT{1} << kMaxValue;
    return SmallEnumSet(RawStateMarker(), tmp | (tmp - 1));
  }

  // equality
  bool operator==(const SmallEnumSet& that) const {
    return this->state_ == that.state_;
  }
  bool operator!=(const SmallEnumSet& that) const { return !(*this == that); }

  // query

  // Return true if the input enum is contained in the "Set".
  bool Contains(const ENUM_TYPE e) const {
    int value = static_cast<int>(e);
    assert(value >= 0 && value <= kMaxValue);
    StateT tmp = 1;
    return state_ & (tmp << value);
  }

  bool empty() const { return state_ == 0; }

  // iterator
  class const_iterator {
   public:
    // copy
    const_iterator(const const_iterator& that) = default;
    const_iterator& operator=(const const_iterator& that) = default;

    // move
    const_iterator(const_iterator&& that) noexcept = default;
    const_iterator& operator=(const_iterator&& that) noexcept = default;

    // equality
    bool operator==(const const_iterator& that) const {
      assert(set_ == that.set_);
      return this->pos_ == that.pos_;
    }

    bool operator!=(const const_iterator& that) const {
      return !(*this == that);
    }

    // ++iterator
    const_iterator& operator++() {
      if (pos_ < kMaxValue) {
        pos_ = set_->SkipUnset(pos_ + 1);
      } else {
        pos_ = kStateBits;
      }
      return *this;
    }

    // iterator++
    const_iterator operator++(int) {
      auto old = *this;
      ++*this;
      return old;
    }

    ENUM_TYPE operator*() const {
      assert(pos_ <= kMaxValue);
      return static_cast<ENUM_TYPE>(pos_);
    }

   private:
    friend class SmallEnumSet;
    const_iterator(const SmallEnumSet* set, int pos) : set_(set), pos_(pos) {}
    const SmallEnumSet* set_;
    int pos_;
  };

  const_iterator begin() const { return const_iterator(this, SkipUnset(0)); }

  const_iterator end() const { return const_iterator(this, kStateBits); }

  // mutable ops

  // Modifies the set (if needed) to include the given value. Returns true
  // iff the set was modified.
  bool Add(const ENUM_TYPE e) {
    int value = static_cast<int>(e);
    assert(value >= 0 && value <= kMaxValue);
    StateT old_state = state_;
    state_ |= (StateT{1} << value);
    return old_state != state_;
  }

  // Modifies the set (if needed) not to include the given value. Returns true
  // iff the set was modified.
  bool Remove(const ENUM_TYPE e) {
    int value = static_cast<int>(e);
    assert(value >= 0 && value <= kMaxValue);
    StateT old_state = state_;
    state_ &= ~(StateT{1} << value);
    return old_state != state_;
  }

  // applicative ops

  // Return a new set based on this one with the additional value(s) inserted
  constexpr SmallEnumSet With(const ENUM_TYPE e) const {
    int value = static_cast<int>(e);
    assert(value >= 0 && value <= kMaxValue);
    return SmallEnumSet(RawStateMarker(), state_ | (StateT{1} << value));
  }
  template <class... TRest>
  constexpr SmallEnumSet With(const ENUM_TYPE e1, const ENUM_TYPE e2,
                              TRest... rest) const {
    return With(e1).With(e2, rest...);
  }

  // Return a new set based on this one excluding the given value(s)
  constexpr SmallEnumSet Without(const ENUM_TYPE e) const {
    int value = static_cast<int>(e);
    assert(value >= 0 && value <= kMaxValue);
    return SmallEnumSet(RawStateMarker(), state_ & ~(StateT{1} << value));
  }
  template <class... TRest>
  constexpr SmallEnumSet Without(const ENUM_TYPE e1, const ENUM_TYPE e2,
                                 TRest... rest) const {
    return Without(e1).Without(e2, rest...);
  }

 private:
  int SkipUnset(int pos) const {
    StateT tmp = state_ >> pos;
    if (tmp == 0) {
      return kStateBits;
    } else {
      return pos + detail::CountTrailingZeroBitsForSmallEnumSet(tmp);
    }
  }
  struct RawStateMarker {};
  explicit SmallEnumSet(RawStateMarker, StateT state) : state_(state) {}

  StateT state_;
};

}  // namespace ROCKSDB_NAMESPACE
