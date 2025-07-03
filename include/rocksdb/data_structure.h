//  Copyright (c) Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <assert.h>

#include <array>
#include <cstddef>
#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

namespace detail {
int CountTrailingZeroBitsForSmallEnumSet(uint64_t);
int BitsSetToOneForSmallEnumSet(uint64_t);
}  // namespace detail

// Represents a set of values of some enum type with a small number of possible
// enumerators. Assumes that any combination of enumerators with values 0
// through MAX_ENUMERATOR (inclusive) might be part of the set. NOTE: would like
// to use std::bitset, but it doesn't support constexpr (in C++17) operations
// and doesn't support efficient iteration over sparse "set to true" entries.
template <typename ENUM_TYPE, ENUM_TYPE MAX_ENUMERATOR>
class SmallEnumSet {
 private:
  static constexpr int kMaxValue = static_cast<int>(MAX_ENUMERATOR);
  static_assert(kMaxValue >= 0);
  static_assert(kMaxValue < 1024, "MAX_ENUMERATOR is suspiciously large");
  using PieceT = uint64_t;
  static constexpr int kPieceBits = 64;
  static constexpr int kPieceMask = 63;
  static constexpr int kPieceShift = 6;
  static constexpr int kPieceCount = kMaxValue / kPieceBits + 1;
  using StateT = std::array<PieceT, kPieceCount>;
  static constexpr int kStateBits = kPieceBits * kPieceCount;
  static_assert(kStateBits == sizeof(StateT) * 8);
  static_assert(kMaxValue <= kStateBits - 1);

 public:
  // construct / create empty set
  SmallEnumSet() : state_{} {}

  template <class... TRest>
  /*implicit*/ constexpr SmallEnumSet(const ENUM_TYPE e, TRest... rest) {
    *this = SmallEnumSet(rest...).With(e);
  }

  // Return the set that includes all valid values, assuming the enum
  // is "dense" (includes all values converting to 0 through kMaxValue)
  static constexpr SmallEnumSet All() {
    StateT tmp;
    for (int i = 0; i < kPieceCount - 1; ++i) {
      tmp[i] = ~PieceT{0};
    }
    if constexpr (((kMaxValue + 1) & kPieceMask) != 0) {
      tmp[kPieceCount - 1] = (PieceT{1} << ((kMaxValue + 1) & kPieceMask)) - 1;
    } else {
      tmp[kPieceCount - 1] = ~PieceT{0};
    }
    return SmallEnumSet(RawStateMarker(), tmp);
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
    return GetPiece(value) & (PieceT{1} << (value & kPieceMask));
  }

  bool empty() const {
    for (int i = 0; i < kPieceCount; ++i) {
      if (state_[i] != 0) {
        return false;
      }
    }
    return true;
  }

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
        pos_ = kMaxValue + 1;
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

  const_iterator end() const { return const_iterator(this, kMaxValue + 1); }

  size_t count() const {
    size_t rv = 0;
    for (int i = 0; i < kPieceCount; ++i) {
      rv += static_cast<size_t>(detail::BitsSetToOneForSmallEnumSet(state_[i]));
    }
    return rv;
  }

  // mutable ops

  // Modifies the set (if needed) to include the given value. Returns true
  // iff the set was modified.
  bool Add(const ENUM_TYPE e) {
    int value = static_cast<int>(e);
    assert(value >= 0 && value <= kMaxValue);
    PieceT& piece_ref = RefPiece(value);
    PieceT old_piece = piece_ref;
    piece_ref |= (PieceT{1} << (value & kPieceMask));
    return old_piece != piece_ref;
  }

  // Modifies the set (if needed) not to include the given value. Returns true
  // iff the set was modified.
  bool Remove(const ENUM_TYPE e) {
    int value = static_cast<int>(e);
    assert(value >= 0 && value <= kMaxValue);
    PieceT& piece_ref = RefPiece(value);
    PieceT old_piece = piece_ref;
    piece_ref &= ~(PieceT{1} << (value & kPieceMask));
    return old_piece != piece_ref;
  }

  // applicative ops

  // Return a new set based on this one with the additional value(s) inserted
  constexpr SmallEnumSet With(const ENUM_TYPE e) const {
    assert(static_cast<int>(e) >= 0 && static_cast<int>(e) <= kMaxValue);
    SmallEnumSet rv(*this);
    rv.Add(e);
    return rv;
  }
  template <class... TRest>
  constexpr SmallEnumSet With(const ENUM_TYPE e1, const ENUM_TYPE e2,
                              TRest... rest) const {
    return With(e1).With(e2, rest...);
  }

  // Return a new set based on this one excluding the given value(s)
  constexpr SmallEnumSet Without(const ENUM_TYPE e) const {
    assert(static_cast<int>(e) >= 0 && static_cast<int>(e) <= kMaxValue);
    SmallEnumSet rv(*this);
    rv.Remove(e);
    return rv;
  }
  template <class... TRest>
  constexpr SmallEnumSet Without(const ENUM_TYPE e1, const ENUM_TYPE e2,
                                 TRest... rest) const {
    return Without(e1).Without(e2, rest...);
  }

 private:
  int SkipUnset(int pos) const {
    while (pos <= kMaxValue) {
      PieceT remainder = GetPiece(pos) >> (pos & kPieceMask);
      if (remainder != 0) {
        return pos + detail::CountTrailingZeroBitsForSmallEnumSet(remainder);
      }
      pos = (pos + kPieceBits) & ~kPieceMask;
    }
    return kMaxValue + 1;
  }
  struct RawStateMarker {};
  explicit SmallEnumSet(RawStateMarker, StateT state) : state_(state) {}
  PieceT GetPiece(int pos) const {
    if constexpr (kPieceCount == 1) {
      return state_[0];
    } else {
      return state_[pos >> kPieceShift];
    }
  }
  PieceT& RefPiece(int pos) {
    if constexpr (kPieceCount == 1) {
      return state_[0];
    } else {
      return state_[pos >> kPieceShift];
    }
  }

  StateT state_;
};

// A smart pointer that tracks an object and an owner, using a statically
// determined function on those to reclaim the object, if both object and owner
// are non-null
template <typename T, class Owner, auto Fn>
class ManagedPtr {
 public:
  ManagedPtr() = default;
  ManagedPtr(T* ptr, Owner* owner) : ptr_(ptr), owner_(owner) {}
  ~ManagedPtr() {
    if (ptr_ && owner_) {
      if constexpr (std::is_member_function_pointer_v<decltype(Fn)>) {
        (owner_->*Fn)(ptr_);
      } else {
        Fn(owner_, ptr_);
      }
    }
  }
  // No copies
  ManagedPtr(const ManagedPtr&) = delete;
  ManagedPtr& operator=(const ManagedPtr&) = delete;
  // Moves
  ManagedPtr(ManagedPtr&& other) noexcept {
    ptr_ = other.ptr_;
    owner_ = other.owner_;
    other.ptr_ = nullptr;
    other.owner_ = nullptr;
  }
  ManagedPtr& operator=(ManagedPtr&& other) noexcept {
    ptr_ = other.ptr_;
    owner_ = other.owner_;
    other.ptr_ = nullptr;
    other.owner_ = nullptr;
    return *this;
  }

  T* get() const { return ptr_; }
  T* operator->() const { return ptr_; }
  T& operator*() const { return *ptr_; }
  operator bool() const { return ptr_ != nullptr; }

  Owner* owner() const { return owner_; }

 private:
  T* ptr_ = nullptr;
  Owner* owner_ = nullptr;
};

}  // namespace ROCKSDB_NAMESPACE
