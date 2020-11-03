//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cmath>

#include "test_util/testharness.h"
#include "util/bloom_impl.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/ribbon_impl.h"
#include "util/stop_watch.h"

#ifndef GFLAGS
uint32_t FLAGS_thoroughness = 5;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
// Using 500 is a good test when you have time to be thorough.
// Default is for general RocksDB regression test runs.
DEFINE_uint32(thoroughness, 5, "iterations per configuration");
#endif  // GFLAGS

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
  static constexpr bool kAllowZeroStarts = false;
  static Hash HashFn(const Key& key, Seed seed) {
    // TODO/FIXME: is there sufficient independence with sequential keys and
    // sequential seeds?
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
struct TypesAndSettings_Coeff64Smash1 : public DefaultTypesAndSettings {
  using CoeffRow = uint64_t;
  static constexpr bool kUseSmash = true;
};
struct TypesAndSettings_Coeff64Smash0 : public TypesAndSettings_Coeff64Smash1 {
  static constexpr bool kFirstCoeffAlwaysOne = false;
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
    // NOTE: Using RocksDB 32-bit Hash() here fails test below because of
    // insufficient mixing of seed (or generally insufficient mixing)
    return ROCKSDB_NAMESPACE::Upper32of64(
        ROCKSDB_NAMESPACE::Hash64(key.data(), key.size(), seed));
  }
};
struct TypesAndSettings_Hash32_Result16 : public TypesAndSettings_Hash32 {
  using ResultRow = uint16_t;
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
struct TypesAndSettings_AllowZeroStarts : public DefaultTypesAndSettings {
  static constexpr bool kAllowZeroStarts = true;
};
struct TypesAndSettings_RehasherWrapped : public DefaultTypesAndSettings {
  // This doesn't directly use StandardRehasher as a whole, but simulates
  // its behavior with unseeded hash of key, then seeded hash-to-hash
  // transform.
  static Hash HashFn(const Key& key, Seed seed) {
    Hash unseeded = DefaultTypesAndSettings::HashFn(key, /*seed*/ 0);
    using Rehasher = ROCKSDB_NAMESPACE::ribbon::StandardRehasherAdapter<
        DefaultTypesAndSettings>;
    return Rehasher::HashFn(unseeded, seed);
  }
};
struct TypesAndSettings_RehasherWrapped_Result16
    : public TypesAndSettings_RehasherWrapped {
  using ResultRow = uint16_t;
};
struct TypesAndSettings_Rehasher32Wrapped : public TypesAndSettings_Hash32 {
  // This doesn't directly use StandardRehasher as a whole, but simulates
  // its behavior with unseeded hash of key, then seeded hash-to-hash
  // transform.
  static Hash HashFn(const Key& key, Seed seed) {
    Hash unseeded = TypesAndSettings_Hash32::HashFn(key, /*seed*/ 0);
    using Rehasher = ROCKSDB_NAMESPACE::ribbon::StandardRehasherAdapter<
        TypesAndSettings_Hash32>;
    return Rehasher::HashFn(unseeded, seed);
  }
};

using TestTypesAndSettings = ::testing::Types<
    TypesAndSettings_Coeff128, TypesAndSettings_Coeff128Smash,
    TypesAndSettings_Coeff64, TypesAndSettings_Coeff64Smash0,
    TypesAndSettings_Coeff64Smash1, TypesAndSettings_Result16,
    TypesAndSettings_IndexSizeT, TypesAndSettings_Hash32,
    TypesAndSettings_Hash32_Result16, TypesAndSettings_KeyString,
    TypesAndSettings_Seed8, TypesAndSettings_NoAlwaysOne,
    TypesAndSettings_AllowZeroStarts, TypesAndSettings_RehasherWrapped,
    TypesAndSettings_RehasherWrapped_Result16,
    TypesAndSettings_Rehasher32Wrapped>;
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

// For testing Poisson-distributed (or similar) statistics, get value for
// `stddevs_allowed` standard deviations above expected mean
// `expected_count`.
// (Poisson approximates Binomial only if probability of a trial being
// in the count is low.)
uint64_t PoissonUpperBound(double expected_count, double stddevs_allowed) {
  return static_cast<uint64_t>(
      expected_count + stddevs_allowed * std::sqrt(expected_count) + 1.0);
}

uint64_t PoissonLowerBound(double expected_count, double stddevs_allowed) {
  return static_cast<uint64_t>(std::max(
      0.0, expected_count - stddevs_allowed * std::sqrt(expected_count)));
}

uint64_t FrequentPoissonUpperBound(double expected_count) {
  // Allow up to 5.0 standard deviations for frequently checked statistics
  return PoissonUpperBound(expected_count, 5.0);
}

uint64_t FrequentPoissonLowerBound(double expected_count) {
  return PoissonLowerBound(expected_count, 5.0);
}

uint64_t InfrequentPoissonUpperBound(double expected_count) {
  // Allow up to 3 standard deviations for infrequently checked statistics
  return PoissonUpperBound(expected_count, 3.0);
}

uint64_t InfrequentPoissonLowerBound(double expected_count) {
  return PoissonLowerBound(expected_count, 3.0);
}

}  // namespace

TYPED_TEST(RibbonTypeParamTest, CompactnessAndBacktrackAndFpRate) {
  IMPORT_RIBBON_TYPES_AND_SETTINGS(TypeParam);
  IMPORT_RIBBON_IMPL_TYPES(TypeParam);

  // For testing FP rate etc.
  constexpr Index kNumToCheck = 100000;

  const auto log2_thoroughness =
      static_cast<Seed>(ROCKSDB_NAMESPACE::FloorLog2(FLAGS_thoroughness));
  // FIXME: This upper bound seems excessive
  const Seed max_seed = 12 + log2_thoroughness;

  // With overhead of just 2%, expect ~50% encoding success per
  // seed with ~5k keys on 64-bit ribbon, or ~150k keys on 128-bit ribbon.
  const double kFactor = 1.02;

  uint64_t total_reseeds = 0;
  uint64_t total_single_failures = 0;
  uint64_t total_batch_successes = 0;
  uint64_t total_fp_count = 0;
  uint64_t total_added = 0;

  uint64_t soln_query_nanos = 0;
  uint64_t soln_query_count = 0;
  uint64_t bloom_query_nanos = 0;
  uint64_t isoln_query_nanos = 0;
  uint64_t isoln_query_count = 0;

  for (uint32_t i = 0; i < FLAGS_thoroughness; ++i) {
    Index num_to_add =
        sizeof(CoeffRow) == 16 ? 130000 : TypeParam::kUseSmash ? 5500 : 2500;

    // Use different values between that number and 50% of that number
    num_to_add -= (i * /* misc prime */ 15485863) % (num_to_add / 2);

    total_added += num_to_add;

    // Most of the time, test the Interleaved solution storage, but when
    // we do we have to make num_slots a multiple of kCoeffBits. So
    // sometimes we want to test without that limitation.
    bool test_interleaved = (i % 7) != 6;

    Index num_slots = static_cast<Index>(num_to_add * kFactor);
    if (test_interleaved) {
      // Round to nearest multiple of kCoeffBits
      num_slots = ((num_slots + kCoeffBits / 2) / kCoeffBits) * kCoeffBits;
      // Re-adjust num_to_add to get as close as possible to kFactor
      num_to_add = static_cast<Index>(num_slots / kFactor);
    }

    std::string prefix;
    // Take different samples if you change thoroughness
    ROCKSDB_NAMESPACE::PutFixed32(&prefix,
                                  i + (FLAGS_thoroughness * 123456789U));

    // Batch that must be added
    std::string added_str = prefix + "added";
    KeyGen keys_begin(added_str, 0);
    KeyGen keys_end(added_str, num_to_add);

    // Batch that may or may not be added
    const Index kBatchSize =
        sizeof(CoeffRow) == 16 ? 300 : TypeParam::kUseSmash ? 20 : 10;
    std::string batch_str = prefix + "batch";
    KeyGen batch_begin(batch_str, 0);
    KeyGen batch_end(batch_str, kBatchSize);

    // Batch never (successfully) added, but used for querying FP rate
    std::string not_str = prefix + "not";
    KeyGen other_keys_begin(not_str, 0);
    KeyGen other_keys_end(not_str, kNumToCheck);

    // Vary bytes uniformly for InterleavedSoln to use number of solution
    // columns varying from 0 to max allowed by ResultRow type (and used by
    // SimpleSoln).
    size_t ibytes =
        (i * /* misc odd */ 67896789) % (sizeof(ResultRow) * num_to_add + 1);
    std::unique_ptr<char[]> idata(new char[ibytes]);
    InterleavedSoln isoln(idata.get(), ibytes);

    SimpleSoln soln;
    Hasher hasher;
    bool first_single;
    bool second_single;
    bool batch_success;
    {
      Banding banding;
      // Traditional solve for a fixed set.
      ASSERT_TRUE(banding.ResetAndFindSeedToSolve(num_slots, keys_begin,
                                                  keys_end, max_seed));

      // Now to test backtracking, starting with guaranteed fail
      Index occupied_count = banding.GetOccupiedCount();
      banding.EnsureBacktrackSize(kNumToCheck);
      ASSERT_FALSE(
          banding.AddRangeOrRollBack(other_keys_begin, other_keys_end));
      ASSERT_EQ(occupied_count, banding.GetOccupiedCount());

      // Check that we still have a good chance of adding a couple more
      // individually
      first_single = banding.Add("one_more");
      second_single = banding.Add("two_more");
      Index more_added = (first_single ? 1 : 0) + (second_single ? 1 : 0);
      total_single_failures += 2U - more_added;

      // Or as a batch
      batch_success = banding.AddRangeOrRollBack(batch_begin, batch_end);
      if (batch_success) {
        more_added += kBatchSize;
        ++total_batch_successes;
      }
      ASSERT_LE(banding.GetOccupiedCount(), occupied_count + more_added);

      // Also verify that redundant adds are OK (no effect)
      ASSERT_TRUE(
          banding.AddRange(keys_begin, KeyGen(added_str, num_to_add / 8)));
      ASSERT_LE(banding.GetOccupiedCount(), occupied_count + more_added);

      // Now back-substitution
      soln.BackSubstFrom(banding);
      if (test_interleaved) {
        isoln.BackSubstFrom(banding);
      }

      Seed seed = banding.GetSeed();
      total_reseeds += seed;
      if (seed > log2_thoroughness + 1) {
        fprintf(stderr, "%s high reseeds at %u, %u/%u: %u\n",
                seed > log2_thoroughness + 8 ? "FIXME Extremely" : "Somewhat",
                static_cast<unsigned>(i), static_cast<unsigned>(num_to_add),
                static_cast<unsigned>(num_slots), static_cast<unsigned>(seed));
      }
      hasher.ResetSeed(seed);
    }
    // soln and hasher now independent of Banding object

    // Verify keys added
    KeyGen cur = keys_begin;
    while (cur != keys_end) {
      EXPECT_TRUE(soln.FilterQuery(*cur, hasher));
      EXPECT_TRUE(!test_interleaved || isoln.FilterQuery(*cur, hasher));
      ++cur;
    }
    // We (maybe) snuck these in!
    if (first_single) {
      EXPECT_TRUE(soln.FilterQuery("one_more", hasher));
      EXPECT_TRUE(!test_interleaved || isoln.FilterQuery("one_more", hasher));
    }
    if (second_single) {
      EXPECT_TRUE(soln.FilterQuery("two_more", hasher));
      EXPECT_TRUE(!test_interleaved || isoln.FilterQuery("two_more", hasher));
    }
    if (batch_success) {
      cur = batch_begin;
      while (cur != batch_end) {
        EXPECT_TRUE(soln.FilterQuery(*cur, hasher));
        EXPECT_TRUE(!test_interleaved || isoln.FilterQuery(*cur, hasher));
        ++cur;
      }
    }

    // Check FP rate (depends only on number of result bits == solution columns)
    Index fp_count = 0;
    cur = other_keys_begin;
    {
      ROCKSDB_NAMESPACE::StopWatchNano timer(ROCKSDB_NAMESPACE::Env::Default(),
                                             true);
      while (cur != other_keys_end) {
        fp_count += soln.FilterQuery(*cur, hasher) ? 1 : 0;
        ++cur;
      }
      soln_query_nanos += timer.ElapsedNanos();
      soln_query_count += kNumToCheck;
    }
    {
      double expected_fp_count = soln.ExpectedFpRate() * kNumToCheck;
      // For expected FP rate, also include false positives due to collisions
      // in Hash value. (Negligible for 64-bit, can matter for 32-bit.)
      double correction =
          kNumToCheck * ROCKSDB_NAMESPACE::ribbon::ExpectedCollisionFpRate(
                            hasher, num_to_add);
      EXPECT_LE(fp_count,
                FrequentPoissonUpperBound(expected_fp_count + correction));
      EXPECT_GE(fp_count,
                FrequentPoissonLowerBound(expected_fp_count + correction));
    }
    total_fp_count += fp_count;

    // And also check FP rate for isoln
    if (test_interleaved) {
      Index ifp_count = 0;
      cur = other_keys_begin;
      ROCKSDB_NAMESPACE::StopWatchNano timer(ROCKSDB_NAMESPACE::Env::Default(),
                                             true);
      while (cur != other_keys_end) {
        ifp_count += isoln.FilterQuery(*cur, hasher) ? 1 : 0;
        ++cur;
      }
      isoln_query_nanos += timer.ElapsedNanos();
      isoln_query_count += kNumToCheck;
      {
        double expected_fp_count = isoln.ExpectedFpRate() * kNumToCheck;
        // For expected FP rate, also include false positives due to collisions
        // in Hash value. (Negligible for 64-bit, can matter for 32-bit.)
        double correction =
            kNumToCheck * ROCKSDB_NAMESPACE::ribbon::ExpectedCollisionFpRate(
                              hasher, num_to_add);
        EXPECT_LE(ifp_count,
                  FrequentPoissonUpperBound(expected_fp_count + correction));
        EXPECT_GE(ifp_count,
                  FrequentPoissonLowerBound(expected_fp_count + correction));
      }
      // Since the bits used in isoln are a subset of the bits used in soln,
      // it cannot have fewer FPs
      EXPECT_GE(ifp_count, fp_count);
    }

    // And compare to Bloom time, for fun
    if (ibytes >= /* minimum Bloom impl bytes*/ 64) {
      Index bfp_count = 0;
      cur = other_keys_begin;
      ROCKSDB_NAMESPACE::StopWatchNano timer(ROCKSDB_NAMESPACE::Env::Default(),
                                             true);
      while (cur != other_keys_end) {
        uint64_t h = hasher.GetHash(*cur);
        uint32_t h1 = ROCKSDB_NAMESPACE::Lower32of64(h);
        uint32_t h2 = sizeof(Hash) >= 8 ? ROCKSDB_NAMESPACE::Upper32of64(h)
                                        : h1 * 0x9e3779b9;
        bfp_count += ROCKSDB_NAMESPACE::FastLocalBloomImpl::HashMayMatch(
                         h1, h2, static_cast<uint32_t>(ibytes), 6, idata.get())
                         ? 1
                         : 0;
        ++cur;
      }
      bloom_query_nanos += timer.ElapsedNanos();
      // ensure bfp_count is used
      ASSERT_LT(bfp_count, kNumToCheck);
    }
  }

  // "outside" == key not in original set so either negative or false positive
  fprintf(stderr, "Simple      outside query, hot, incl hashing, ns/key: %g\n",
          1.0 * soln_query_nanos / soln_query_count);
  fprintf(stderr, "Interleaved outside query, hot, incl hashing, ns/key: %g\n",
          1.0 * isoln_query_nanos / isoln_query_count);
  fprintf(stderr, "Bloom       outside query, hot, incl hashing, ns/key: %g\n",
          1.0 * bloom_query_nanos / soln_query_count);

  {
    double average_reseeds = 1.0 * total_reseeds / FLAGS_thoroughness;
    fprintf(stderr, "Average re-seeds: %g\n", average_reseeds);
    // Values above were chosen to target around 50% chance of encoding success
    // rate (average of 1.0 re-seeds) or slightly better. But 1.1 is also close
    // enough.
    EXPECT_LE(total_reseeds,
              InfrequentPoissonUpperBound(1.1 * FLAGS_thoroughness));
    EXPECT_GE(total_reseeds,
              InfrequentPoissonLowerBound(0.9 * FLAGS_thoroughness));
  }

  {
    uint64_t total_singles = 2 * FLAGS_thoroughness;
    double single_failure_rate = 1.0 * total_single_failures / total_singles;
    fprintf(stderr, "Add'l single, failure rate: %g\n", single_failure_rate);
    // A rough bound (one sided) based on nothing in particular
    double expected_single_failures =
        1.0 * total_singles /
        (sizeof(CoeffRow) == 16 ? 128 : TypeParam::kUseSmash ? 64 : 32);
    EXPECT_LE(total_single_failures,
              InfrequentPoissonUpperBound(expected_single_failures));
  }

  {
    // Counting successes here for Poisson to approximate the Binomial
    // distribution.
    // A rough bound (one sided) based on nothing in particular.
    double expected_batch_successes = 1.0 * FLAGS_thoroughness / 2;
    uint64_t lower_bound =
        InfrequentPoissonLowerBound(expected_batch_successes);
    fprintf(stderr, "Add'l batch, success rate: %g (>= %g)\n",
            1.0 * total_batch_successes / FLAGS_thoroughness,
            1.0 * lower_bound / FLAGS_thoroughness);
    EXPECT_GE(total_batch_successes, lower_bound);
  }

  {
    uint64_t total_checked = uint64_t{kNumToCheck} * FLAGS_thoroughness;
    double expected_total_fp_count =
        total_checked * std::pow(0.5, 8U * sizeof(ResultRow));
    // For expected FP rate, also include false positives due to collisions
    // in Hash value. (Negligible for 64-bit, can matter for 32-bit.)
    double average_added = 1.0 * total_added / FLAGS_thoroughness;
    expected_total_fp_count +=
        total_checked * ROCKSDB_NAMESPACE::ribbon::ExpectedCollisionFpRate(
                            Hasher(), average_added);

    uint64_t upper_bound = InfrequentPoissonUpperBound(expected_total_fp_count);
    uint64_t lower_bound = InfrequentPoissonLowerBound(expected_total_fp_count);
    fprintf(stderr, "Average FP rate: %g (~= %g, <= %g, >= %g)\n",
            1.0 * total_fp_count / total_checked,
            expected_total_fp_count / total_checked,
            1.0 * upper_bound / total_checked,
            1.0 * lower_bound / total_checked);
    // FIXME: this can fail for Result16, e.g. --thoroughness=300
    // Seems due to inexpensive hashing in StandardHasher::GetCoeffRow and
    // GetResultRowFromHash as replacing those with different Hash64 instances
    // fixes it, at least mostly.
    EXPECT_LE(total_fp_count, upper_bound);
    EXPECT_GE(total_fp_count, lower_bound);
  }
}

TYPED_TEST(RibbonTypeParamTest, Extremes) {
  IMPORT_RIBBON_TYPES_AND_SETTINGS(TypeParam);
  IMPORT_RIBBON_IMPL_TYPES(TypeParam);

  size_t bytes = 128 * 1024;
  std::unique_ptr<char[]> buf(new char[bytes]);
  InterleavedSoln isoln(buf.get(), bytes);
  SimpleSoln soln;
  Hasher hasher;
  Banding banding;

  // ########################################
  // Add zero keys to minimal number of slots
  KeyGen begin_and_end("foo", 123);
  ASSERT_TRUE(banding.ResetAndFindSeedToSolve(
      /*slots*/ kCoeffBits, begin_and_end, begin_and_end, /*max_seed*/ 0));

  soln.BackSubstFrom(banding);
  isoln.BackSubstFrom(banding);

  // Because there's plenty of memory, we expect the interleaved solution to
  // use maximum supported columns (same as simple solution)
  ASSERT_EQ(isoln.GetUpperNumColumns(), 8U * sizeof(ResultRow));
  ASSERT_EQ(isoln.GetUpperStartBlock(), 0U);

  // Somewhat oddly, we expect same FP rate as if we had essentially filled
  // up the slots.
  constexpr Index kNumToCheck = 100000;
  KeyGen other_keys_begin("not", 0);
  KeyGen other_keys_end("not", kNumToCheck);

  Index fp_count = 0;
  KeyGen cur = other_keys_begin;
  while (cur != other_keys_end) {
    bool isoln_query_result = isoln.FilterQuery(*cur, hasher);
    bool soln_query_result = soln.FilterQuery(*cur, hasher);
    // Solutions are equivalent
    ASSERT_EQ(isoln_query_result, soln_query_result);
    // And in fact we only expect an FP when ResultRow is 0
    ASSERT_EQ(soln_query_result, hasher.GetResultRowFromHash(
                                     hasher.GetHash(*cur)) == ResultRow{0});

    fp_count += soln_query_result ? 1 : 0;
    ++cur;
  }
  {
    ASSERT_EQ(isoln.ExpectedFpRate(), soln.ExpectedFpRate());
    double expected_fp_count = isoln.ExpectedFpRate() * kNumToCheck;
    EXPECT_LE(fp_count, InfrequentPoissonUpperBound(expected_fp_count));
    EXPECT_GE(fp_count, InfrequentPoissonLowerBound(expected_fp_count));
  }

  // ######################################################
  // Use zero bytes for interleaved solution (key(s) added)

  // Add one key
  KeyGen key_begin("added", 0);
  KeyGen key_end("added", 1);
  ASSERT_TRUE(banding.ResetAndFindSeedToSolve(
      /*slots*/ kCoeffBits, key_begin, key_end, /*max_seed*/ 0));

  InterleavedSoln isoln2(nullptr, /*bytes*/ 0);

  isoln2.BackSubstFrom(banding);

  ASSERT_EQ(isoln2.GetUpperNumColumns(), 0U);
  ASSERT_EQ(isoln2.GetUpperStartBlock(), 0U);

  // All queries return true
  ASSERT_TRUE(isoln2.FilterQuery(*other_keys_begin, hasher));
  ASSERT_EQ(isoln2.ExpectedFpRate(), 1.0);
}

TEST(RibbonTest, AllowZeroStarts) {
  IMPORT_RIBBON_TYPES_AND_SETTINGS(TypesAndSettings_AllowZeroStarts);
  IMPORT_RIBBON_IMPL_TYPES(TypesAndSettings_AllowZeroStarts);

  InterleavedSoln isoln(nullptr, /*bytes*/ 0);
  SimpleSoln soln;
  Hasher hasher;
  Banding banding;

  KeyGen begin("foo", 0);
  KeyGen end("foo", 1);
  // Can't add 1 entry
  ASSERT_FALSE(
      banding.ResetAndFindSeedToSolve(/*slots*/ 0, begin, end, /*max_seed*/ 5));

  KeyGen begin_and_end("foo", 123);
  // Can add 0 entries
  ASSERT_TRUE(banding.ResetAndFindSeedToSolve(/*slots*/ 0, begin_and_end,
                                              begin_and_end, /*max_seed*/ 5));

  Seed seed = banding.GetSeed();
  ASSERT_EQ(seed, 0U);
  hasher.ResetSeed(seed);

  // Can construct 0-slot solutions
  isoln.BackSubstFrom(banding);
  soln.BackSubstFrom(banding);

  // Should always return false
  ASSERT_FALSE(isoln.FilterQuery(*begin, hasher));
  ASSERT_FALSE(soln.FilterQuery(*begin, hasher));

  // And report that in FP rate
  ASSERT_EQ(isoln.ExpectedFpRate(), 0.0);
  ASSERT_EQ(soln.ExpectedFpRate(), 0.0);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
