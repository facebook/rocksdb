//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cmath>

#include "test_util/testharness.h"
#include "util/hash.h"
#include "util/ribbon_impl.h"

template <typename TypesAndSettings>
class RibbonTypeParamTest : public ::testing::Test {};

class RibbonTest : public ::testing::Test {};

struct DefaultTypesAndSettings {
  using CoeffRow = ROCKSDB_NAMESPACE::Unsigned128;
  using ResultRow = uint8_t;
  using Index = uint32_t;
  using Hash = uint64_t;
  using Key = ROCKSDB_NAMESPACE::Slice;
  using Seed = uint32_t;
  static constexpr bool kIsFilter = true;
  static constexpr bool kFirstCoeffAlwaysOne = true;
  static constexpr bool kUseSmash = false;
  static Hash HashFn(const Key& key, Seed seed) {
    return ROCKSDB_NAMESPACE::Hash64(key.data(), key.size(), seed);
  }
};

using TypesAndSettings_Coeff128 = DefaultTypesAndSettings;
struct TypesAndSettings_Coeff128Smash : public DefaultTypesAndSettings {
  static constexpr bool kUseSmash = true;
};
struct TypesAndSettings_Coeff64 : public DefaultTypesAndSettings {
  using CoeffRow = uint64_t;
};
struct TypesAndSettings_Coeff64Smash : public DefaultTypesAndSettings {
  using CoeffRow = uint64_t;
  static constexpr bool kUseSmash = true;
};
struct TypesAndSettings_Result16 : public DefaultTypesAndSettings {
  using ResultRow = uint16_t;
};
struct TypesAndSettings_IndexSizeT : public DefaultTypesAndSettings {
  using Index = size_t;
};
struct TypesAndSettings_Hash32 : public DefaultTypesAndSettings {
  using Hash = uint32_t;
  static Hash HashFn(const Key& key, Seed seed) {
    return ROCKSDB_NAMESPACE::Hash(key.data(), key.size(), seed);
  }
};
struct TypesAndSettings_KeyString : public DefaultTypesAndSettings {
  using Key = std::string;
};
struct TypesAndSettings_Seed8 : public DefaultTypesAndSettings {
  using Seed = uint8_t;
};
struct TypesAndSettings_NoAlwaysOne : public DefaultTypesAndSettings {
  static constexpr bool kFirstCoeffAlwaysOne = false;
};

using TestTypesAndSettings =
    ::testing::Types<TypesAndSettings_Coeff128, TypesAndSettings_Coeff128Smash,
                     TypesAndSettings_Coeff64, TypesAndSettings_Coeff64Smash,
                     TypesAndSettings_Result16, TypesAndSettings_IndexSizeT,
                     TypesAndSettings_Hash32, TypesAndSettings_KeyString,
                     TypesAndSettings_Seed8, TypesAndSettings_NoAlwaysOne>;
TYPED_TEST_CASE(RibbonTypeParamTest, TestTypesAndSettings);

namespace {

struct KeyGen {
  KeyGen(const std::string& prefix, uint64_t id) : id_(id), str_(prefix) {
    ROCKSDB_NAMESPACE::PutFixed64(&str_, id_);
  }

  // Prefix (only one required)
  KeyGen& operator++() {
    ++id_;
    return *this;
  }

  KeyGen& operator+=(uint64_t incr) {
    id_ += incr;
    return *this;
  }

  const std::string& operator*() {
    // Use multiplication to mix things up a little in the key
    ROCKSDB_NAMESPACE::EncodeFixed64(&str_[str_.size() - 8],
                                     id_ * uint64_t{0x1500000001});
    return str_;
  }

  bool operator==(const KeyGen& other) {
    // Same prefix is assumed
    return id_ == other.id_;
  }
  bool operator!=(const KeyGen& other) {
    // Same prefix is assumed
    return id_ != other.id_;
  }

  uint64_t id_;
  std::string str_;
};

}  // namespace

TYPED_TEST(RibbonTypeParamTest, SimpleCompactnessAndBacktrackAndFpRate) {
  IMPORT_RIBBON_TYPES_AND_SETTINGS(TypeParam);
  IMPORT_RIBBON_IMPL_TYPES(TypeParam);

  // With a few attempts, we can store with overhead of just 2.5%,
  // with 10k keys on 64-bit ribbon, or 100k keys on 128-bit ribbon.
  const Seed kMaxSeed = 3;
  const double kFactor = 1.025;
  const Index kNumToAdd = sizeof(CoeffRow) < 16 ? 10000 : 100000;
  const Index kNumSlots = static_cast<Index>(kNumToAdd * kFactor);

  // Batch that must be added
  KeyGen keys_begin("added", 0);
  KeyGen keys_end("added", kNumToAdd);

  // Batch that is adjusted depending on whether it can be added
  const Index kNumExtra = (kNumSlots - kNumToAdd) / 10;
  KeyGen extra_begin("extra", 0);
  KeyGen extra_end("extra", kNumExtra);

  // Batch never (successfully) added, but used for querying FP rate
  const Index kNumToCheck = 100000;
  KeyGen other_keys_begin("not", 0);
  KeyGen other_keys_end("not", kNumToCheck);

  SimpleSoln soln;
  Hasher hasher;
  {
    Banding banding;
    // Traditional solve for a fixed set.
    ASSERT_TRUE(banding.ResetAndFindSeedToSolve(kNumSlots, keys_begin, keys_end,
                                                kMaxSeed));

    // Now to test backtracking, starting with guaranteed fail
    banding.EnsureBacktrackSize(kNumToCheck);
    ASSERT_FALSE(banding.AddRangeOrRollBack(other_keys_begin, other_keys_end));

    // Check that we still have a good chance of adding a couple more
    ASSERT_TRUE(banding.Add("one_more"));
    ASSERT_TRUE(banding.Add("two_more"));

    // And some additional batch of kNumExtra (this would probably infinite
    // loop if backtracking was broken)
    int attempts = 0;
    while (!banding.AddRangeOrRollBack(extra_begin, extra_end)) {
      extra_begin += kNumExtra;
      extra_end += kNumExtra;
      ASSERT_LT(++attempts, 10000);
    }
    fprintf(stderr, "Unsuccessful extra attempts: %d\n",
            static_cast<int>(extra_begin.id_ / kNumExtra));

    // Now back-substitution
    soln.BackSubstFrom(banding);
    Seed seed = banding.GetSeed();
    fprintf(stderr, "Successful seed: %d\n", static_cast<int>(seed));
    hasher.ResetSeed(seed);
  }
  // soln and hasher now independent of Banding object

  // Verify keys added
  KeyGen cur = keys_begin;
  while (cur != keys_end) {
    EXPECT_TRUE(soln.FilterQuery(*cur, hasher));
    ++cur;
  }
  // We snuck these in!
  EXPECT_TRUE(soln.FilterQuery("one_more", hasher));
  EXPECT_TRUE(soln.FilterQuery("two_more", hasher));
  cur = extra_begin;
  while (cur != extra_end) {
    EXPECT_TRUE(soln.FilterQuery(*cur, hasher));
    ++cur;
  }

  // Check FP rate (depends only on number of result bits == solution columns)
  constexpr size_t kNumSolutionColumns = 8U * sizeof(ResultRow);

  Index fp_count = 0;
  cur = other_keys_begin;
  while (cur != keys_end) {
    fp_count += soln.FilterQuery(*cur, hasher) ? 1 : 0;
    ++cur;
  }
  fprintf(stderr, "FP rate: %g\n", fp_count * 1.0 / kNumToCheck);
  Index expected = kNumToCheck >> kNumSolutionColumns;
  // Allow about 3 std deviations above
  Index upper_bound =
      expected + 1 + static_cast<Index>(3.0 * std::sqrt(expected));
  EXPECT_LE(fp_count, upper_bound);
}

TEST(RibbonTest, Another) {
  IMPORT_RIBBON_TYPES_AND_SETTINGS(DefaultTypesAndSettings);
  IMPORT_RIBBON_IMPL_TYPES(DefaultTypesAndSettings);

  // TODO
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
