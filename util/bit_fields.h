//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>

#include "rocksdb/rocksdb_namespace.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

// Declares a wrapper type around UnderlyingT that allows it to be divided up
// into and accessed as bit fields. This is mostly intended to aid in packing
// fields into atomic variables to reduce the need for locking in concurrent
// code and/or to simplify reasoning on and accommodation of different
// interesting, bug-prone interleavings. Convenient atomic wrappers
// (RelaxedAtomic, AcqRelAtomic) are provided below to aid usage with atomics,
// especially for CAS updates, but it is even possible to combine operations on
// multiple bit fields into a single non-CAS atomic operation using Transforms
// below.
//
// Unlike C/C++ bit fields, this implementation guarantees tight bit packing
// so that all available lock-free atomic bits can be utilized.
//
// The specific bit fields are declared outside the declaration using
// BoolBitField and UnsignedBitField below. Example usage:
//
// struct MyState : public BitFields<uint32_t, MyState> {
//   // Extra helper declarations and/or field type declarations
// };
//
// // Starts with a 16-bit field returned as uint16_t
// using Field1 = UnsignedBitField<MyState, 16, NoPrevBitField>;
// using Field2 = BoolBitField<MyState, Field1>;
// using Field3 = BoolBitField<MyState, Field2>;
// using Field4 = UnsignedBitField<MyState, 5, Field3>;  // 5 bits in a uint8_t
//
// // MyState{} is zero-initialized
// auto state = MyState{}.With<Field1>(42U).With<Field2>(true);
// state.Set<Field4>(3U);
// state.Ref<Field1>() += state.Get<Field4>();
//
// Note that there's nothing preventing you from declaring overlapping fields
// in the same 'MyState' family. This could be useful for variant types where
// an earlier field determines which layout later fields are using. For example,
// an alternate field after Field2:
//
// using Field3a = UnsignedBitField<State, 6, Field2>;  // 6 bits in a uint8_t
//
template <typename UnderlyingT, typename DerivedT>
struct BitFields {
  using U = UnderlyingT;
  U underlying = 0;
  static constexpr int kBitCount = sizeof(U) * 8;

  using Derived = DerivedT;

  // Modify a given field in place
  template <typename BitFieldT>
  void Set(typename BitFieldT::V value) {
    static_assert(std::is_same_v<typename BitFieldT::Parent, Derived>);
    Derived& derived = static_cast<Derived&>(*this);
    BitFieldT::SetIn(derived, value);
  }

  // Return a copy with the given field modified
  template <typename BitFieldT>
  Derived With(typename BitFieldT::V value) const {
    static_assert(std::is_same_v<typename BitFieldT::Parent, Derived>);
    Derived rv = static_cast<const Derived&>(*this);
    BitFieldT::SetIn(rv, value);
    return rv;
  }

  // Get the value of a field
  template <typename BitFieldT>
  typename BitFieldT::V Get() const {
    static_assert(std::is_same_v<typename BitFieldT::Parent, Derived>);
    return BitFieldT::GetFrom(static_cast<const Derived&>(*this));
  }

  // Reference and Ref() are not intended to behave as full references but to
  // provide a convenient way to do operations like +=, |=, etc. Get and Set
  // are preferred for simple operations.
  template <typename BitFieldT>
  struct Reference {
    explicit Reference(BitFields& bf) : bf_(bf) {}
    Reference(const Reference&) = default;
    Reference& operator=(const Reference&) = default;
    Reference(Reference&&) = default;
    Reference& operator=(Reference&&) = default;

    void operator=(typename BitFieldT::V value) { bf_.Set<BitFieldT>(value); }
    void operator+=(typename BitFieldT::V value) {
      bf_.Set<BitFieldT>(bf_.Get<BitFieldT>() + value);
    }
    void operator-=(typename BitFieldT::V value) {
      bf_.Set<BitFieldT>(bf_.Get<BitFieldT>() - value);
    }
    void operator|=(typename BitFieldT::V value) {
      bf_.Set<BitFieldT>(bf_.Get<BitFieldT>() | value);
    }
    void operator&=(typename BitFieldT::V value) {
      bf_.Set<BitFieldT>(bf_.Get<BitFieldT>() & value);
    }

   private:
    BitFields& bf_;
  };

  template <typename BitFieldT>
  Reference<BitFieldT> Ref() {
    return Reference<BitFieldT>(*this);
  }

  bool operator==(const BitFields& other) const = default;
  bool operator!=(const BitFields& other) const = default;
};

// For building atomic updates affecting one or more fields, assuming all the
// updates are bitwise-or.
template <typename BitFieldsT>
struct OrTransform {
  using U = typename BitFieldsT::U;
  U to_or = 0;
  // + for general combine
  OrTransform<BitFieldsT> operator+(OrTransform<BitFieldsT> other) const {
    return OrTransform<BitFieldsT>{to_or | other.to_or};
  }
};

// For building atomic updates affecting one or more fields, assuming all the
// updates are bitwise-and.
template <typename BitFieldsT>
struct AndTransform {
  using U = typename BitFieldsT::U;
  U to_and = 0;
  // + for general combine
  AndTransform<BitFieldsT> operator+(AndTransform<BitFieldsT> other) const {
    return AndTransform<BitFieldsT>{to_and & other.to_and};
  }
};

// TODO: AddTransfrom, which is more complicated due to possible overflow into
// other fields etc.

// Placeholder for PrevField for the first field
struct NoPrevBitField {
  // no instances
  NoPrevBitField() = delete;
  static constexpr int kEndBit = 0;
};

// For declaring a single-bit field accessed as a boolean. See example above on
// BitFields
template <typename BitFieldsT, typename PrevField>
struct BoolBitField {
  using Parent = BitFieldsT;
  using ParentBase =
      BitFields<typename BitFieldsT::U, typename BitFieldsT::Derived>;
  using U = typename BitFieldsT::U;
  using V = bool;
  static constexpr int kBitOffset = PrevField::kEndBit;
  static constexpr int kEndBit = kBitOffset + 1;
  static_assert(kBitOffset >= 0 && kEndBit <= BitFieldsT::kBitCount);

  // no instances
  BoolBitField() = delete;

  // NOTE: allow BitFieldsT to be derived from BitFields<> which can be
  // passed in here
  static bool GetFrom(const ParentBase& bf) {
    return (bf.underlying & (U{1} << kBitOffset)) != 0;
  }
  static void SetIn(ParentBase& bf, bool value) {
    bf.underlying =
        (bf.underlying & ~(U{1} << kBitOffset)) | (U{value} << kBitOffset);
  }
  static OrTransform<BitFieldsT> SetTransform() {
    return OrTransform<BitFieldsT>{U{1} << kBitOffset};
  }
  static AndTransform<BitFieldsT> ClearTransform() {
    return AndTransform<BitFieldsT>{~(U{1} << kBitOffset)};
  }
};

// For declaring a multi-bit field accessed as an unsigned int. See example
// above on BitFields
template <typename BitFieldsT, int kBitCount, typename PrevField>
struct UnsignedBitField {
  using Parent = BitFieldsT;
  using U = typename BitFieldsT::U;
  // Smallest uint type that can fit kBitCount bits
  using V = std::conditional_t<
      kBitCount <= 8, uint8_t,
      std::conditional_t<
          kBitCount <= 16, uint16_t,
          std::conditional_t<kBitCount <= 32, uint32_t, uint64_t>>>;
  static constexpr int kBitOffset = PrevField::kEndBit;
  static constexpr int kEndBit = kBitOffset + kBitCount;
  static_assert(kBitCount >= 1);
  static_assert(kBitCount <= 64);
  static_assert(kBitOffset >= 0 && kEndBit <= BitFieldsT::kBitCount);

  static constexpr V kMask = (V{1} << (kBitCount - 1) << 1) - 1;

  // no instances
  UnsignedBitField() = delete;

  static V GetFrom(const BitFieldsT& bf) {
    return BitwiseAnd(bf.underlying >> kBitOffset, kMask);
  }

  static void SetIn(BitFieldsT& bf, V value) {
    bf.underlying &= ~(static_cast<U>(kMask) << kBitOffset);
    bf.underlying |= static_cast<U>(value & kMask) << kBitOffset;
  }

  static AndTransform<BitFieldsT> ClearTransform() {
    return AndTransform<BitFieldsT>{~(static_cast<U>(kMask) << kBitOffset)};
  }
};

// A handy wrapper for a relaxed atomic on some BitFields type (unlike
// RelaxedAtomic for arithmetic types). For encapsulation, usual arithmetic
// atomic operations are only available by calling Apply[Relaxed]() on
// Transforms returned from field classes. Extending an example from BitFields:
//
// auto transform = Field2::ClearTransform() + Field4::ClearTransform();
// MyState old_state;
// my_atomic.ApplyRelaxed(transform, &old_state);
// auto field2_before_clearing = old_state.Get<Field2>();
//
template <typename BitFieldsT>
class RelaxedBitFieldsAtomic {
 public:
  using U = typename BitFieldsT::U;
  explicit RelaxedBitFieldsAtomic(BitFieldsT initial = {})
      : v_(initial.underlying) {}
  void StoreRelaxed(BitFieldsT desired) {
    v_.store(desired.underlying, std::memory_order_relaxed);
  }
  BitFieldsT LoadRelaxed() const {
    return BitFieldsT{v_.load(std::memory_order_relaxed)};
  }
  bool CasWeakRelaxed(BitFieldsT& expected, BitFieldsT desired) {
    return v_.compare_exchange_weak(expected.underlying, desired.underlying,
                                    std::memory_order_relaxed);
  }
  bool CasStrongRelaxed(BitFieldsT& expected, BitFieldsT desired) {
    return v_.compare_exchange_strong(expected.underlying, desired.underlying,
                                      std::memory_order_relaxed);
  }
  BitFieldsT ExchangeRelaxed(BitFieldsT desired) {
    return BitFieldsT{
        v_.exchange(desired.underlying, std::memory_order_relaxed)};
  }
  void ApplyRelaxed(OrTransform<BitFieldsT> transform,
                    BitFieldsT* before = nullptr, BitFieldsT* after = nullptr) {
    U before_val = v_.fetch_or(transform.to_or, std::memory_order_relaxed);
    if (before) {
      before->underlying = before_val;
    }
    if (after) {
      after->underlying = before_val | transform.to_or;
    }
  }
  void ApplyRelaxed(AndTransform<BitFieldsT> transform,
                    BitFieldsT* before = nullptr, BitFieldsT* after = nullptr) {
    U before_val = v_.fetch_and(transform.to_and, std::memory_order_relaxed);
    if (before) {
      before->underlying = before_val;
    }
    if (after) {
      after->underlying = before_val & transform.to_and;
    }
  }

 protected:
  std::atomic<U> v_;
};

// A handy wrapper for an aquire-release atomic (also relaxed semantics
// available) on some BitFields type. See RelaxedBitFieldsAtomic for more info.
template <typename BitFieldsT>
class AcqRelBitFieldsAtomic : public RelaxedBitFieldsAtomic<BitFieldsT> {
 public:
  using Base = RelaxedBitFieldsAtomic<BitFieldsT>;
  using U = typename BitFieldsT::U;

  explicit AcqRelBitFieldsAtomic(BitFieldsT initial = {}) : Base(initial) {}

  void Store(BitFieldsT desired) {
    Base::v_.store(desired.underlying, std::memory_order_release);
  }
  BitFieldsT Load() const {
    return BitFieldsT{Base::v_.load(std::memory_order_acquire)};
  }
  bool CasWeak(BitFieldsT& expected, BitFieldsT desired) {
    return Base::v_.compare_exchange_weak(
        expected.underlying, desired.underlying, std::memory_order_acq_rel);
  }
  bool CasStrong(BitFieldsT& expected, BitFieldsT desired) {
    return Base::v_.compare_exchange_strong(
        expected.underlying, desired.underlying, std::memory_order_acq_rel);
  }
  BitFieldsT Exchange(BitFieldsT desired) {
    return BitFieldsT{
        Base::v_.exchange(desired.underlying, std::memory_order_acq_rel)};
  }
  void Apply(OrTransform<BitFieldsT> transform, BitFieldsT* before = nullptr,
             BitFieldsT* after = nullptr) {
    U before_val =
        Base::v_.fetch_or(transform.to_or, std::memory_order_acq_rel);
    if (before) {
      before->underlying = before_val;
    }
    if (after) {
      after->underlying = before_val | transform.to_or;
    }
  }
  void Apply(AndTransform<BitFieldsT> transform, BitFieldsT* before = nullptr,
             BitFieldsT* after = nullptr) {
    U before_val =
        Base::v_.fetch_and(transform.to_and, std::memory_order_acq_rel);
    if (before) {
      before->underlying = before_val;
    }
    if (after) {
      after->underlying = before_val & transform.to_and;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
