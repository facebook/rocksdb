//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>

#include "port/lang.h"  // for FALLTHROUGH_INTENDED
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

namespace ribbon {

// RIBBON PHSF & RIBBON Filter (Rapid Incremental Boolean Banding ON-the-fly)
//
// ribbon_config.h: APIs for relating numbers of slots with numbers of
// additions for tolerable construction failure probabilities. This is
// separate from ribbon_impl.h because it might not be needed for
// some applications.
//
// This API assumes uint32_t for number of slots, as a single Ribbon
// linear system should not normally overflow that without big penalties.
//
// Template parameter kCoeffBits uses uint64_t for convenience in case it
// comes from size_t.
//
// Most of the complexity here is trying to optimize speed and
// compiled code size, using templates to minimize table look-ups and
// the compiled size of all linked look-up tables. Look-up tables are
// required because we don't have good formulas, and the data comes
// from running FindOccupancy in ribbon_test.

// Represents a chosen chance of successful Ribbon construction for a single
// seed. Allowing higher chance of failed construction can reduce space
// overhead but takes extra time in construction.
enum ConstructionFailureChance {
  kOneIn2,
  kOneIn20,
  // When using kHomogeneous==true, construction failure chance should
  // not generally exceed target FP rate, so it unlikely useful to
  // allow a higher "failure" chance. In some cases, even more overhead
  // is appropriate. (TODO)
  kOneIn1000,
};

namespace detail {

// It is useful to compile ribbon_test linking to BandingConfigHelper with
// settings for which we do not have configuration data, as long as we don't
// run the code. This template hack supports that.
template <ConstructionFailureChance kCfc, uint64_t kCoeffBits, bool kUseSmash,
          bool kHomogeneous, bool kIsSupported>
struct BandingConfigHelper1MaybeSupported {
 public:
  static uint32_t GetNumToAdd(uint32_t num_slots) {
    // Unsupported
    assert(num_slots == 0);
    (void)num_slots;
    return 0;
  }

  static uint32_t GetNumSlots(uint32_t num_to_add) {
    // Unsupported
    assert(num_to_add == 0);
    (void)num_to_add;
    return 0;
  }
};

// Base class for BandingConfigHelper1 and helper for BandingConfigHelper
// with core implementations built on above data
template <ConstructionFailureChance kCfc, uint64_t kCoeffBits, bool kUseSmash,
          bool kHomogeneous>
struct BandingConfigHelper1MaybeSupported<
    kCfc, kCoeffBits, kUseSmash, kHomogeneous, true /* kIsSupported */> {
 public:
  // See BandingConfigHelper1. Implementation in ribbon_config.cc
  static uint32_t GetNumToAdd(uint32_t num_slots);

  // See BandingConfigHelper1. Implementation in ribbon_config.cc
  static uint32_t GetNumSlots(uint32_t num_to_add);
};

}  // namespace detail

template <ConstructionFailureChance kCfc, uint64_t kCoeffBits, bool kUseSmash,
          bool kHomogeneous>
struct BandingConfigHelper1
    : public detail::BandingConfigHelper1MaybeSupported<
          kCfc, kCoeffBits, kUseSmash, kHomogeneous,
          /* kIsSupported */ kCoeffBits == 64 || kCoeffBits == 128> {
 public:
  // Returns a number of entries that can be added to a given number of
  // slots, with roughly kCfc chance of construction failure per seed,
  // or better. Does NOT do rounding for InterleavedSoln; call
  // RoundUpNumSlots for that.
  //
  // inherited:
  // static uint32_t GetNumToAdd(uint32_t num_slots);

  // Returns a number of slots for a given number of entries to add
  // that should have roughly kCfc chance of construction failure per
  // seed, or better. Does NOT do rounding for InterleavedSoln; call
  // RoundUpNumSlots for that.
  //
  // num_to_add should not exceed roughly 2/3rds of the maximum value
  // of the uint32_t type to avoid overflow.
  //
  // inherited:
  // static uint32_t GetNumSlots(uint32_t num_to_add);
};

// Configured using TypesAndSettings as in ribbon_impl.h
template <ConstructionFailureChance kCfc, class TypesAndSettings>
struct BandingConfigHelper1TS
    : public BandingConfigHelper1<
          kCfc,
          /* kCoeffBits */ sizeof(typename TypesAndSettings::CoeffRow) * 8U,
          TypesAndSettings::kUseSmash, TypesAndSettings::kHomogeneous> {};

// Like BandingConfigHelper1TS except failure chance can be a runtime rather
// than compile time value.
template <class TypesAndSettings>
struct BandingConfigHelper {
 public:
  static constexpr ConstructionFailureChance kDefaultFailureChance =
      TypesAndSettings::kHomogeneous ? kOneIn1000 : kOneIn20;

  static uint32_t GetNumToAdd(
      uint32_t num_slots,
      ConstructionFailureChance max_failure = kDefaultFailureChance) {
    switch (max_failure) {
      default:
        assert(false);
        FALLTHROUGH_INTENDED;
      case kOneIn20: {
        using H1 = BandingConfigHelper1TS<kOneIn20, TypesAndSettings>;
        return H1::GetNumToAdd(num_slots);
      }
      case kOneIn2: {
        using H1 = BandingConfigHelper1TS<kOneIn2, TypesAndSettings>;
        return H1::GetNumToAdd(num_slots);
      }
      case kOneIn1000: {
        using H1 = BandingConfigHelper1TS<kOneIn1000, TypesAndSettings>;
        return H1::GetNumToAdd(num_slots);
      }
    }
  }

  static uint32_t GetNumSlots(
      uint32_t num_to_add,
      ConstructionFailureChance max_failure = kDefaultFailureChance) {
    switch (max_failure) {
      default:
        assert(false);
        FALLTHROUGH_INTENDED;
      case kOneIn20: {
        using H1 = BandingConfigHelper1TS<kOneIn20, TypesAndSettings>;
        return H1::GetNumSlots(num_to_add);
      }
      case kOneIn2: {
        using H1 = BandingConfigHelper1TS<kOneIn2, TypesAndSettings>;
        return H1::GetNumSlots(num_to_add);
      }
      case kOneIn1000: {
        using H1 = BandingConfigHelper1TS<kOneIn1000, TypesAndSettings>;
        return H1::GetNumSlots(num_to_add);
      }
    }
  }
};

}  // namespace ribbon

}  // namespace ROCKSDB_NAMESPACE
