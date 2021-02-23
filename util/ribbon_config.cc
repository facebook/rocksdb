//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/ribbon_config.h"

namespace ROCKSDB_NAMESPACE {

namespace ribbon {

namespace detail {

// Each instantiation of this struct is sufficiently unique for configuration
// purposes, and is only instantiated for settings where we support the
// configuration API. An application might only reference one instantiation,
// meaning the rest could be pruned at link time.
template <ConstructionFailureChance kCfc, uint64_t kCoeffBits, bool kUseSmash>
struct BandingConfigHelperData {
  static constexpr size_t kKnownSize = 18U;

  // Because of complexity in the data, for smaller numbers of slots
  // (powers of two up to 2^17), we record known numbers that can be added
  // with kCfc chance of construction failure and settings in template
  // parameters. Zero means "unsupported (too small) number of slots".
  // (GetNumToAdd below will use interpolation for numbers of slots
  // between powers of two; double rather than integer values here make
  // that more accurate.)
  static const std::array<double, kKnownSize> kKnownToAddByPow2;

  // For sufficiently large number of slots, doubling the number of
  // slots will increase the expected overhead (slots over number added)
  // by approximately this constant.
  // (This is roughly constant regardless of ConstructionFailureChance and
  // smash setting.)
  // (Would be a constant if we had partial template specialization for
  // static const members.)
  static inline double GetFactorPerPow2() {
    if (kCoeffBits == 128U) {
      return 0.0038;
    } else {
      assert(kCoeffBits == 64U);
      return 0.0083;
    }
  }

  // Overhead factor for 2^(kKnownSize-1) slots
  // (Would be a constant if we had partial template specialization for
  // static const members.)
  static inline double GetFinalKnownFactor() {
    return 1.0 * (uint32_t{1} << (kKnownSize - 1)) /
           kKnownToAddByPow2[kKnownSize - 1];
  }

  // GetFinalKnownFactor() - (kKnownSize-1) * GetFactorPerPow2()
  // (Would be a constant if we had partial template specialization for
  // static const members.)
  static inline double GetBaseFactor() {
    return GetFinalKnownFactor() - (kKnownSize - 1) * GetFactorPerPow2();
  }

  // Get overhead factor (slots over number to add) for sufficiently large
  // number of slots (by log base 2)
  static inline double GetFactorForLarge(double log2_num_slots) {
    return GetBaseFactor() + log2_num_slots * GetFactorPerPow2();
  }

  // For a given power of two number of slots (specified by whole number
  // log base 2), implements GetNumToAdd for such limited case, returning
  // double for better interpolation in GetNumToAdd and GetNumSlots.
  static inline double GetNumToAddForPow2(uint32_t log2_num_slots) {
    assert(log2_num_slots <= 32);  // help clang-analyze
    if (log2_num_slots < kKnownSize) {
      return kKnownToAddByPow2[log2_num_slots];
    } else {
      return 1.0 * (uint64_t{1} << log2_num_slots) /
             GetFactorForLarge(1.0 * log2_num_slots);
    }
  }
};

// Based on data from FindOccupancy in ribbon_test
template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn2, 128U, false>::kKnownToAddByPow2{{
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,  // unsupported
        252.984,
        506.109,
        1013.71,
        2029.47,
        4060.43,
        8115.63,
        16202.2,
        32305.1,
        64383.5,
        128274,
    }};

template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn2, 128U, /*smash*/ true>::kKnownToAddByPow2{{
        0,
        0,
        0,
        0,
        0,
        0,
        0,  // unsupported
        126.274,
        254.279,
        510.27,
        1022.24,
        2046.02,
        4091.99,
        8154.98,
        16244.3,
        32349.7,
        64426.6,
        128307,
    }};

template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn2, 64U, false>::kKnownToAddByPow2{{
        0,
        0,
        0,
        0,
        0,
        0,
        0,  // unsupported
        124.94,
        249.968,
        501.234,
        1004.06,
        2006.15,
        3997.89,
        7946.99,
        15778.4,
        31306.9,
        62115.3,
        123284,
    }};

template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn2, 64U, /*smash*/ true>::kKnownToAddByPow2{{
        0,
        0,
        0,
        0,
        0,
        0,  // unsupported
        62.2683,
        126.259,
        254.268,
        509.975,
        1019.98,
        2026.16,
        4019.75,
        7969.8,
        15798.2,
        31330.3,
        62134.2,
        123255,
    }};

template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn20, 128U, false>::kKnownToAddByPow2{{
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,  // unsupported
        248.851,
        499.532,
        1001.26,
        2003.97,
        4005.59,
        8000.39,
        15966.6,
        31828.1,
        63447.3,
        126506,
    }};

template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn20, 128U, /*smash*/ true>::kKnownToAddByPow2{{
        0,
        0,
        0,
        0,
        0,
        0,
        0,  // unsupported
        122.637,
        250.651,
        506.625,
        1018.54,
        2036.43,
        4041.6,
        8039.25,
        16005,
        31869.6,
        63492.8,
        126537,
    }};

template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn20, 64U, false>::kKnownToAddByPow2{{
        0,
        0,
        0,
        0,
        0,
        0,
        0,  // unsupported
        120.659,
        243.346,
        488.168,
        976.373,
        1948.86,
        3875.85,
        7704.97,
        15312.4,
        30395.1,
        60321.8,
        119813,
    }};

template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn20, 64U, /*smash*/ true>::kKnownToAddByPow2{{
        0,
        0,
        0,
        0,
        0,
        0,  // unsupported
        58.6016,
        122.619,
        250.641,
        503.595,
        994.165,
        1967.36,
        3898.17,
        7727.21,
        15331.5,
        30405.8,
        60376.2,
        119836,
    }};

template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn1000, 128U, false>::kKnownToAddByPow2{{
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,  // unsupported
        242.61,
        491.887,
        983.603,
        1968.21,
        3926.98,
        7833.99,
        15629,
        31199.9,
        62307.8,
        123870,
    }};

template <>
const std::array<double, 18> BandingConfigHelperData<
    kOneIn1000, 128U, /*smash*/ true>::kKnownToAddByPow2{{
    0,
    0,
    0,
    0,
    0,
    0,
    0,  // unsupported
    117.19,
    245.105,
    500.748,
    1010.67,
    1993.4,
    3950.01,
    7863.31,
    15652,
    31262.1,
    62462.8,
    124095,
}};

template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn1000, 64U, false>::kKnownToAddByPow2{{
        0,
        0,
        0,
        0,
        0,
        0,
        0,  // unsupported
        114,
        234.8,
        471.498,
        940.165,
        1874,
        3721.5,
        7387.5,
        14592,
        29160,
        57745,
        115082,
    }};

template <>
const std::array<double, 18>
    BandingConfigHelperData<kOneIn1000, 64U, /*smash*/ true>::kKnownToAddByPow2{
        {
            0,
            0,
            0,
            0,
            0,
            0,  // unsupported
            53.0434,
            117,
            245.312,
            483.571,
            950.251,
            1878,
            3736.34,
            7387.97,
            14618,
            29142.9,
            57838.8,
            114932,
        }};

// We hide these implementation details from the .h file with explicit
// instantiations below these partial specializations.

template <ConstructionFailureChance kCfc, uint64_t kCoeffBits, bool kUseSmash,
          bool kHomogeneous>
uint32_t BandingConfigHelper1MaybeSupported<
    kCfc, kCoeffBits, kUseSmash, kHomogeneous,
    true /* kIsSupported */>::GetNumToAdd(uint32_t num_slots) {
  using Data = detail::BandingConfigHelperData<kCfc, kCoeffBits, kUseSmash>;
  if (num_slots == 0) {
    return 0;
  }
  uint32_t num_to_add;
  double log2_num_slots = std::log(num_slots) * 1.4426950409;
  uint32_t floor_log2 = static_cast<uint32_t>(log2_num_slots);
  if (floor_log2 + 1 < Data::kKnownSize) {
    double ceil_portion = 1.0 * num_slots / (uint32_t{1} << floor_log2) - 1.0;
    // Must be a supported number of slots
    assert(Data::kKnownToAddByPow2[floor_log2] > 0.0);
    // Weighted average of two nearest known data points
    num_to_add = static_cast<uint32_t>(
        ceil_portion * Data::kKnownToAddByPow2[floor_log2 + 1] +
        (1.0 - ceil_portion) * Data::kKnownToAddByPow2[floor_log2]);
  } else {
    // Use formula for large values
    double factor = Data::GetFactorForLarge(log2_num_slots);
    assert(factor >= 1.0);
    num_to_add = static_cast<uint32_t>(num_slots / factor);
  }
  if (kHomogeneous) {
    // Even when standard filter construction would succeed, we might
    // have loaded things up too much for Homogeneous filter. (Complete
    // explanation not known but observed empirically.) This seems to
    // correct for that, mostly affecting small filter configurations.
    if (num_to_add >= 8) {
      num_to_add -= 8;
    } else {
      assert(false);
    }
  }
  return num_to_add;
}

template <ConstructionFailureChance kCfc, uint64_t kCoeffBits, bool kUseSmash,
          bool kHomogeneous>
uint32_t BandingConfigHelper1MaybeSupported<
    kCfc, kCoeffBits, kUseSmash, kHomogeneous,
    true /* kIsSupported */>::GetNumSlots(uint32_t num_to_add) {
  using Data = detail::BandingConfigHelperData<kCfc, kCoeffBits, kUseSmash>;

  if (num_to_add == 0) {
    return 0;
  }
  if (kHomogeneous) {
    // Reverse of above in GetNumToAdd
    num_to_add += 8;
  }
  double log2_num_to_add = std::log(num_to_add) * 1.4426950409;
  uint32_t approx_log2_slots = static_cast<uint32_t>(log2_num_to_add + 0.5);
  assert(approx_log2_slots <= 32);  // help clang-analyze

  double lower_num_to_add = Data::GetNumToAddForPow2(approx_log2_slots);
  double upper_num_to_add;
  if (approx_log2_slots == 0 || lower_num_to_add == /* unsupported */ 0) {
    // Return minimum non-zero slots in standard implementation
    return kUseSmash ? kCoeffBits : 2 * kCoeffBits;
  } else if (num_to_add < lower_num_to_add) {
    upper_num_to_add = lower_num_to_add;
    --approx_log2_slots;
    lower_num_to_add = Data::GetNumToAddForPow2(approx_log2_slots);
  } else {
    upper_num_to_add = Data::GetNumToAddForPow2(approx_log2_slots + 1);
  }

  assert(num_to_add >= lower_num_to_add);
  assert(num_to_add < upper_num_to_add);

  double upper_portion =
      (num_to_add - lower_num_to_add) / (upper_num_to_add - lower_num_to_add);

  double lower_num_slots = 1.0 * (uint64_t{1} << approx_log2_slots);

  // Interpolation, round up
  return static_cast<uint32_t>(upper_portion * lower_num_slots +
                               lower_num_slots + 0.999999999);
}

// These explicit instantiations enable us to hide most of the
// implementation details from the .h file. (The .h file currently
// needs to determine whether settings are "supported" or not.)

template struct BandingConfigHelper1MaybeSupported<kOneIn2, 128U, /*sm*/ false,
                                                   /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn2, 128U, /*sm*/ true,
                                                   /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn2, 128U, /*sm*/ false,
                                                   /*hm*/ true, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn2, 128U, /*sm*/ true,
                                                   /*hm*/ true, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn2, 64U, /*sm*/ false,
                                                   /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn2, 64U, /*sm*/ true,
                                                   /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn2, 64U, /*sm*/ false,
                                                   /*hm*/ true, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn2, 64U, /*sm*/ true,
                                                   /*hm*/ true, /*sup*/ true>;

template struct BandingConfigHelper1MaybeSupported<kOneIn20, 128U, /*sm*/ false,
                                                   /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn20, 128U, /*sm*/ true,
                                                   /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn20, 128U, /*sm*/ false,
                                                   /*hm*/ true, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn20, 128U, /*sm*/ true,
                                                   /*hm*/ true, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn20, 64U, /*sm*/ false,
                                                   /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn20, 64U, /*sm*/ true,
                                                   /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn20, 64U, /*sm*/ false,
                                                   /*hm*/ true, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn20, 64U, /*sm*/ true,
                                                   /*hm*/ true, /*sup*/ true>;

template struct BandingConfigHelper1MaybeSupported<
    kOneIn1000, 128U, /*sm*/ false, /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<
    kOneIn1000, 128U, /*sm*/ true, /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<
    kOneIn1000, 128U, /*sm*/ false, /*hm*/ true, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<
    kOneIn1000, 128U, /*sm*/ true, /*hm*/ true, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<
    kOneIn1000, 64U, /*sm*/ false, /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn1000, 64U, /*sm*/ true,
                                                   /*hm*/ false, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<
    kOneIn1000, 64U, /*sm*/ false, /*hm*/ true, /*sup*/ true>;
template struct BandingConfigHelper1MaybeSupported<kOneIn1000, 64U, /*sm*/ true,
                                                   /*hm*/ true, /*sup*/ true>;

}  // namespace detail

}  // namespace ribbon

}  // namespace ROCKSDB_NAMESPACE
