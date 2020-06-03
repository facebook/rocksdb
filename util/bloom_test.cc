//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run this test... Skipping...\n");
  return 0;
}
#else

#include <array>
#include <cmath>
#include <vector>

#include "logging/logging.h"
#include "memory/arena.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/filter_policy_internal.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/gflags_compat.h"
#include "util/hash.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_int32(bits_per_key, 10, "");

namespace ROCKSDB_NAMESPACE {

static const int kVerbose = 1;

static Slice Key(int i, char* buffer) {
  std::string s;
  PutFixed32(&s, static_cast<uint32_t>(i));
  memcpy(buffer, s.c_str(), sizeof(i));
  return Slice(buffer, sizeof(i));
}

static int NextLength(int length) {
  if (length < 10) {
    length += 1;
  } else if (length < 100) {
    length += 10;
  } else if (length < 1000) {
    length += 100;
  } else {
    length += 1000;
  }
  return length;
}

class BlockBasedBloomTest : public testing::Test {
 private:
  std::unique_ptr<const FilterPolicy> policy_;
  std::string filter_;
  std::vector<std::string> keys_;

 public:
  BlockBasedBloomTest() { ResetPolicy(); }

  void Reset() {
    keys_.clear();
    filter_.clear();
  }

  void ResetPolicy(double bits_per_key) {
    policy_.reset(new BloomFilterPolicy(bits_per_key,
                                        BloomFilterPolicy::kDeprecatedBlock));
    Reset();
  }

  void ResetPolicy() { ResetPolicy(FLAGS_bits_per_key); }

  void Add(const Slice& s) {
    keys_.push_back(s.ToString());
  }

  void Build() {
    std::vector<Slice> key_slices;
    for (size_t i = 0; i < keys_.size(); i++) {
      key_slices.push_back(Slice(keys_[i]));
    }
    filter_.clear();
    policy_->CreateFilter(&key_slices[0], static_cast<int>(key_slices.size()),
                          &filter_);
    keys_.clear();
    if (kVerbose >= 2) DumpFilter();
  }

  size_t FilterSize() const {
    return filter_.size();
  }

  Slice FilterData() const { return Slice(filter_); }

  void DumpFilter() {
    fprintf(stderr, "F(");
    for (size_t i = 0; i+1 < filter_.size(); i++) {
      const unsigned int c = static_cast<unsigned int>(filter_[i]);
      for (int j = 0; j < 8; j++) {
        fprintf(stderr, "%c", (c & (1 <<j)) ? '1' : '.');
      }
    }
    fprintf(stderr, ")\n");
  }

  bool Matches(const Slice& s) {
    if (!keys_.empty()) {
      Build();
    }
    return policy_->KeyMayMatch(s, filter_);
  }

  double FalsePositiveRate() {
    char buffer[sizeof(int)];
    int result = 0;
    for (int i = 0; i < 10000; i++) {
      if (Matches(Key(i + 1000000000, buffer))) {
        result++;
      }
    }
    return result / 10000.0;
  }
};

TEST_F(BlockBasedBloomTest, EmptyFilter) {
  ASSERT_TRUE(! Matches("hello"));
  ASSERT_TRUE(! Matches("world"));
}

TEST_F(BlockBasedBloomTest, Small) {
  Add("hello");
  Add("world");
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
  ASSERT_TRUE(! Matches("x"));
  ASSERT_TRUE(! Matches("foo"));
}

TEST_F(BlockBasedBloomTest, VaryingLengths) {
  char buffer[sizeof(int)];

  // Count number of filters that significantly exceed the false positive rate
  int mediocre_filters = 0;
  int good_filters = 0;

  for (int length = 1; length <= 10000; length = NextLength(length)) {
    Reset();
    for (int i = 0; i < length; i++) {
      Add(Key(i, buffer));
    }
    Build();

    ASSERT_LE(FilterSize(), (size_t)((length * 10 / 8) + 40)) << length;

    // All added keys must match
    for (int i = 0; i < length; i++) {
      ASSERT_TRUE(Matches(Key(i, buffer)))
          << "Length " << length << "; key " << i;
    }

    // Check false positive rate
    double rate = FalsePositiveRate();
    if (kVerbose >= 1) {
      fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
              rate*100.0, length, static_cast<int>(FilterSize()));
    }
    ASSERT_LE(rate, 0.02);   // Must not be over 2%
    if (rate > 0.0125) mediocre_filters++;  // Allowed, but not too often
    else good_filters++;
  }
  if (kVerbose >= 1) {
    fprintf(stderr, "Filters: %d good, %d mediocre\n",
            good_filters, mediocre_filters);
  }
  ASSERT_LE(mediocre_filters, good_filters/5);
}

// Ensure the implementation doesn't accidentally change in an
// incompatible way
TEST_F(BlockBasedBloomTest, Schema) {
  char buffer[sizeof(int)];

  ResetPolicy(8);  // num_probes = 5
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 3589896109U);

  ResetPolicy(9);  // num_probes = 6
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 969445585U);

  ResetPolicy(11);  // num_probes = 7
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 1694458207U);

  ResetPolicy(10);  // num_probes = 6
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 2373646410U);

  ResetPolicy(10);
  for (int key = /*CHANGED*/ 1; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 1908442116U);

  ResetPolicy(10);
  for (int key = 1; key < /*CHANGED*/ 88; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 3057004015U);

  // With new fractional bits_per_key, check that we are rounding to
  // whole bits per key for old Bloom filters.
  ResetPolicy(9.5);  // Treated as 10
  for (int key = 1; key < 88; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), /*SAME*/ 3057004015U);

  ResetPolicy(10.499);  // Treated as 10
  for (int key = 1; key < 88; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), /*SAME*/ 3057004015U);

  ResetPolicy();
}

// Different bits-per-byte

class FullBloomTest : public testing::TestWithParam<BloomFilterPolicy::Mode> {
 private:
  BlockBasedTableOptions table_options_;
  std::shared_ptr<const FilterPolicy>& policy_;
  std::unique_ptr<FilterBitsBuilder> bits_builder_;
  std::unique_ptr<FilterBitsReader> bits_reader_;
  std::unique_ptr<const char[]> buf_;
  size_t filter_size_;

 public:
  FullBloomTest() : policy_(table_options_.filter_policy), filter_size_(0) {
    ResetPolicy();
  }

  BuiltinFilterBitsBuilder* GetBuiltinFilterBitsBuilder() {
    // Throws on bad cast
    return &dynamic_cast<BuiltinFilterBitsBuilder&>(*bits_builder_);
  }

  const BloomFilterPolicy* GetBloomFilterPolicy() {
    // Throws on bad cast
    return &dynamic_cast<const BloomFilterPolicy&>(*policy_);
  }

  void Reset() {
    bits_builder_.reset(BloomFilterPolicy::GetBuilderFromContext(
        FilterBuildingContext(table_options_)));
    bits_reader_.reset(nullptr);
    buf_.reset(nullptr);
    filter_size_ = 0;
  }

  void ResetPolicy(double bits_per_key) {
    policy_.reset(new BloomFilterPolicy(bits_per_key, GetParam()));
    Reset();
  }

  void ResetPolicy() { ResetPolicy(FLAGS_bits_per_key); }

  void Add(const Slice& s) {
    bits_builder_->AddKey(s);
  }

  void OpenRaw(const Slice& s) {
    bits_reader_.reset(policy_->GetFilterBitsReader(s));
  }

  void Build() {
    Slice filter = bits_builder_->Finish(&buf_);
    bits_reader_.reset(policy_->GetFilterBitsReader(filter));
    filter_size_ = filter.size();
  }

  size_t FilterSize() const {
    return filter_size_;
  }

  Slice FilterData() { return Slice(buf_.get(), filter_size_); }

  int GetNumProbesFromFilterData() {
    assert(filter_size_ >= 5);
    int8_t raw_num_probes = static_cast<int8_t>(buf_.get()[filter_size_ - 5]);
    if (raw_num_probes == -1) {  // New bloom filter marker
      return static_cast<uint8_t>(buf_.get()[filter_size_ - 3]);
    } else {
      return raw_num_probes;
    }
  }

  bool Matches(const Slice& s) {
    if (bits_reader_ == nullptr) {
      Build();
    }
    return bits_reader_->MayMatch(s);
  }

  // Provides a kind of fingerprint on the Bloom filter's
  // behavior, for reasonbly high FP rates.
  uint64_t PackedMatches() {
    char buffer[sizeof(int)];
    uint64_t result = 0;
    for (int i = 0; i < 64; i++) {
      if (Matches(Key(i + 12345, buffer))) {
        result |= uint64_t{1} << i;
      }
    }
    return result;
  }

  // Provides a kind of fingerprint on the Bloom filter's
  // behavior, for lower FP rates.
  std::string FirstFPs(int count) {
    char buffer[sizeof(int)];
    std::string rv;
    int fp_count = 0;
    for (int i = 0; i < 1000000; i++) {
      // Pack four match booleans into each hexadecimal digit
      if (Matches(Key(i + 1000000, buffer))) {
        ++fp_count;
        rv += std::to_string(i);
        if (fp_count == count) {
          break;
        }
        rv += ',';
      }
    }
    return rv;
  }

  double FalsePositiveRate() {
    char buffer[sizeof(int)];
    int result = 0;
    for (int i = 0; i < 10000; i++) {
      if (Matches(Key(i + 1000000000, buffer))) {
        result++;
      }
    }
    return result / 10000.0;
  }

  uint32_t SelectByImpl(uint32_t for_legacy_bloom,
                        uint32_t for_fast_local_bloom) {
    switch (GetParam()) {
      case BloomFilterPolicy::kLegacyBloom:
        return for_legacy_bloom;
      case BloomFilterPolicy::kFastLocalBloom:
        return for_fast_local_bloom;
      case BloomFilterPolicy::kDeprecatedBlock:
      case BloomFilterPolicy::kAuto:
          /* N/A */;
    }
    // otherwise
    assert(false);
    return 0;
  }
};

TEST_P(FullBloomTest, FilterSize) {
  // In addition to checking the consistency of space computation, we are
  // checking that denoted and computed doubles are interpreted as expected
  // as bits_per_key values.
  bool some_computed_less_than_denoted = false;
  // Note: enforced minimum is 1 bit per key (1000 millibits), and enforced
  // maximum is 100 bits per key (100000 millibits).
  for (auto bpk :
       std::vector<std::pair<double, int> >{{-HUGE_VAL, 1000},
                                            {-INFINITY, 1000},
                                            {0.0, 1000},
                                            {1.234, 1234},
                                            {3.456, 3456},
                                            {9.5, 9500},
                                            {10.0, 10000},
                                            {10.499, 10499},
                                            {21.345, 21345},
                                            {99.999, 99999},
                                            {1234.0, 100000},
                                            {HUGE_VAL, 100000},
                                            {INFINITY, 100000},
                                            {NAN, 100000}}) {
    ResetPolicy(bpk.first);
    auto bfp = GetBloomFilterPolicy();
    EXPECT_EQ(bpk.second, bfp->GetMillibitsPerKey());
    EXPECT_EQ((bpk.second + 500) / 1000, bfp->GetWholeBitsPerKey());

    double computed = bpk.first;
    // This transforms e.g. 9.5 -> 9.499999999999998, which we still
    // round to 10 for whole bits per key.
    computed += 0.5;
    computed /= 1234567.0;
    computed *= 1234567.0;
    computed -= 0.5;
    some_computed_less_than_denoted |= (computed < bpk.first);
    ResetPolicy(computed);
    bfp = GetBloomFilterPolicy();
    EXPECT_EQ(bpk.second, bfp->GetMillibitsPerKey());
    EXPECT_EQ((bpk.second + 500) / 1000, bfp->GetWholeBitsPerKey());

    auto bits_builder = GetBuiltinFilterBitsBuilder();
    for (int n = 1; n < 100; n++) {
      auto space = bits_builder->CalculateSpace(n);
      auto n2 = bits_builder->CalculateNumEntry(space);
      EXPECT_GE(n2, n);
      auto space2 = bits_builder->CalculateSpace(n2);
      EXPECT_EQ(space, space2);
    }
  }
  // Check that the compiler hasn't optimized our computation into nothing
  EXPECT_TRUE(some_computed_less_than_denoted);
  ResetPolicy();
}

TEST_P(FullBloomTest, FullEmptyFilter) {
  // Empty filter is not match, at this level
  ASSERT_TRUE(!Matches("hello"));
  ASSERT_TRUE(!Matches("world"));
}

TEST_P(FullBloomTest, FullSmall) {
  Add("hello");
  Add("world");
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
  ASSERT_TRUE(!Matches("x"));
  ASSERT_TRUE(!Matches("foo"));
}

TEST_P(FullBloomTest, FullVaryingLengths) {
  char buffer[sizeof(int)];

  // Count number of filters that significantly exceed the false positive rate
  int mediocre_filters = 0;
  int good_filters = 0;

  for (int length = 1; length <= 10000; length = NextLength(length)) {
    Reset();
    for (int i = 0; i < length; i++) {
      Add(Key(i, buffer));
    }
    Build();

    ASSERT_LE(FilterSize(),
              (size_t)((length * 10 / 8) + CACHE_LINE_SIZE * 2 + 5));

    // All added keys must match
    for (int i = 0; i < length; i++) {
      ASSERT_TRUE(Matches(Key(i, buffer)))
          << "Length " << length << "; key " << i;
    }

    // Check false positive rate
    double rate = FalsePositiveRate();
    if (kVerbose >= 1) {
      fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
              rate*100.0, length, static_cast<int>(FilterSize()));
    }
    ASSERT_LE(rate, 0.02);   // Must not be over 2%
    if (rate > 0.0125)
      mediocre_filters++;  // Allowed, but not too often
    else
      good_filters++;
  }
  if (kVerbose >= 1) {
    fprintf(stderr, "Filters: %d good, %d mediocre\n",
            good_filters, mediocre_filters);
  }
  ASSERT_LE(mediocre_filters, good_filters/5);
}

namespace {
inline uint32_t SelectByCacheLineSize(uint32_t for64, uint32_t for128,
                                      uint32_t for256) {
  (void)for64;
  (void)for128;
  (void)for256;
#if CACHE_LINE_SIZE == 64
  return for64;
#elif CACHE_LINE_SIZE == 128
  return for128;
#elif CACHE_LINE_SIZE == 256
  return for256;
#else
#error "CACHE_LINE_SIZE unknown or unrecognized"
#endif
}
}  // namespace

// Ensure the implementation doesn't accidentally change in an
// incompatible way. This test doesn't check the reading side
// (FirstFPs/PackedMatches) for LegacyBloom because it requires the
// ability to read filters generated using other cache line sizes.
// See RawSchema.
TEST_P(FullBloomTest, Schema) {
  char buffer[sizeof(int)];

  // Use enough keys so that changing bits / key by 1 is guaranteed to
  // change number of allocated cache lines. So keys > max cache line bits.

  ResetPolicy(2);  // num_probes = 1
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 1);
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(1567096579, 1964771444, 2659542661U),
                   3817481309U));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("11,13,17,25,29,30,35,37,45,53", FirstFPs(10));
  }

  ResetPolicy(3);  // num_probes = 2
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 2);
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(2707206547U, 2571983456U, 218344685),
                   2807269961U));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("4,15,17,24,27,28,29,53,63,70", FirstFPs(10));
  }

  ResetPolicy(5);  // num_probes = 3
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 3);
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(515748486, 94611728, 2436112214U),
                   204628445));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("15,24,29,39,53,87,89,100,103,104", FirstFPs(10));
  }

  ResetPolicy(8);  // num_probes = 5
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 5);
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(1302145999, 2811644657U, 756553699),
                   355564975));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("16,60,66,126,220,238,244,256,265,287", FirstFPs(10));
  }

  ResetPolicy(9);  // num_probes = 6
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(2092755149, 661139132, 1182970461),
                   2137566013U));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("156,367,791,872,945,1015,1139,1159,1265,1435", FirstFPs(10));
  }

  ResetPolicy(11);  // num_probes = 7
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 7);
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(3755609649U, 1812694762, 1449142939),
                   2561502687U));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("34,74,130,236,643,882,962,1015,1035,1110", FirstFPs(10));
  }

  // This used to be 9 probes, but 8 is a better choice for speed,
  // especially with SIMD groups of 8 probes, with essentially no
  // change in FP rate.
  // FP rate @ 9 probes, old Bloom: 0.4321%
  // FP rate @ 9 probes, new Bloom: 0.1846%
  // FP rate @ 8 probes, new Bloom: 0.1843%
  ResetPolicy(14);  // num_probes = 8 (new), 9 (old)
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(static_cast<uint32_t>(GetNumProbesFromFilterData()),
            SelectByImpl(9, 8));
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(178861123, 379087593, 2574136516U),
                   3709876890U));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("130,240,522,565,989,2002,2526,3147,3543", FirstFPs(9));
  }

  // This used to be 11 probes, but 9 is a better choice for speed
  // AND accuracy.
  // FP rate @ 11 probes, old Bloom: 0.3571%
  // FP rate @ 11 probes, new Bloom: 0.0884%
  // FP rate @  9 probes, new Bloom: 0.0843%
  ResetPolicy(16);  // num_probes = 9 (new), 11 (old)
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(static_cast<uint32_t>(GetNumProbesFromFilterData()),
            SelectByImpl(11, 9));
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(1129406313, 3049154394U, 1727750964),
                   1087138490));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("3299,3611,3916,6620,7822,8079,8482,8942,10167", FirstFPs(9));
  }

  ResetPolicy(10);  // num_probes = 6, but different memory ratio vs. 9
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(1478976371, 2910591341U, 1182970461),
                   2498541272U));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("16,126,133,422,466,472,813,1002,1035,1159", FirstFPs(10));
  }

  ResetPolicy(10);
  for (int key = /*CHANGED*/ 1; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(4205696321U, 1132081253U, 2385981855U),
                   2058382345U));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("16,126,133,422,466,472,813,1002,1035,1159", FirstFPs(10));
  }

  ResetPolicy(10);
  for (int key = 1; key < /*CHANGED*/ 2088; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ(
      BloomHash(FilterData()),
      SelectByImpl(SelectByCacheLineSize(2885052954U, 769447944, 4175124908U),
                   23699164));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ("16,126,133,422,466,472,813,1002,1035,1159", FirstFPs(10));
  }

  // With new fractional bits_per_key, check that we are rounding to
  // whole bits per key for old Bloom filters but fractional for
  // new Bloom filter.
  ResetPolicy(9.5);
  for (int key = 1; key < 2088; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(GetNumProbesFromFilterData(), 6);
  EXPECT_EQ(BloomHash(FilterData()),
            SelectByImpl(/*SAME*/ SelectByCacheLineSize(2885052954U, 769447944,
                                                        4175124908U),
                         /*CHANGED*/ 3166884174U));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ(/*CHANGED*/ "126,156,367,444,458,791,813,976,1015,1035",
              FirstFPs(10));
  }

  ResetPolicy(10.499);
  for (int key = 1; key < 2088; key++) {
    Add(Key(key, buffer));
  }
  Build();
  EXPECT_EQ(static_cast<uint32_t>(GetNumProbesFromFilterData()),
            SelectByImpl(6, 7));
  EXPECT_EQ(BloomHash(FilterData()),
            SelectByImpl(/*SAME*/ SelectByCacheLineSize(2885052954U, 769447944,
                                                        4175124908U),
                         /*CHANGED*/ 4098502778U));
  if (GetParam() == BloomFilterPolicy::kFastLocalBloom) {
    EXPECT_EQ(/*CHANGED*/ "16,236,240,472,1015,1045,1111,1409,1465,1612",
              FirstFPs(10));
  }

  ResetPolicy();
}

// A helper class for testing custom or corrupt filter bits as read by
// built-in FilterBitsReaders.
struct RawFilterTester {
  // Buffer, from which we always return a tail Slice, so the
  // last five bytes are always the metadata bytes.
  std::array<char, 3000> data_;
  // Points five bytes from the end
  char* metadata_ptr_;

  RawFilterTester() : metadata_ptr_(&*(data_.end() - 5)) {}

  Slice ResetNoFill(uint32_t len_without_metadata, uint32_t num_lines,
                     uint32_t num_probes) {
    metadata_ptr_[0] = static_cast<char>(num_probes);
    EncodeFixed32(metadata_ptr_ + 1, num_lines);
    uint32_t len = len_without_metadata + /*metadata*/ 5;
    assert(len <= data_.size());
    return Slice(metadata_ptr_ - len_without_metadata, len);
  }

  Slice Reset(uint32_t len_without_metadata, uint32_t num_lines,
               uint32_t num_probes, bool fill_ones) {
    data_.fill(fill_ones ? 0xff : 0);
    return ResetNoFill(len_without_metadata, num_lines, num_probes);
  }

  Slice ResetWeirdFill(uint32_t len_without_metadata, uint32_t num_lines,
                        uint32_t num_probes) {
    for (uint32_t i = 0; i < data_.size(); ++i) {
      data_[i] = static_cast<char>(0x7b7b >> (i % 7));
    }
    return ResetNoFill(len_without_metadata, num_lines, num_probes);
  }
};

TEST_P(FullBloomTest, RawSchema) {
  RawFilterTester cft;
  // Two probes, about 3/4 bits set: ~50% "FP" rate
  // One 256-byte cache line.
  OpenRaw(cft.ResetWeirdFill(256, 1, 2));
  EXPECT_EQ(uint64_t{11384799501900898790U}, PackedMatches());

  // Two 128-byte cache lines.
  OpenRaw(cft.ResetWeirdFill(256, 2, 2));
  EXPECT_EQ(uint64_t{10157853359773492589U}, PackedMatches());

  // Four 64-byte cache lines.
  OpenRaw(cft.ResetWeirdFill(256, 4, 2));
  EXPECT_EQ(uint64_t{7123594913907464682U}, PackedMatches());
}

TEST_P(FullBloomTest, CorruptFilters) {
  RawFilterTester cft;

  for (bool fill : {false, true}) {
    // Good filter bits - returns same as fill
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 6, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Good filter bits - returns same as fill
    OpenRaw(cft.Reset(CACHE_LINE_SIZE * 3, 3, 6, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Good filter bits - returns same as fill
    // 256 is unusual but legal cache line size
    OpenRaw(cft.Reset(256 * 3, 3, 6, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Good filter bits - returns same as fill
    // 30 should be max num_probes
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 30, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Good filter bits - returns same as fill
    // 1 should be min num_probes
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 1, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Type 1 trivial filter bits - returns true as if FP by zero probes
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 0, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Type 2 trivial filter bits - returns false as if built from zero keys
    OpenRaw(cft.Reset(0, 0, 6, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    // Type 2 trivial filter bits - returns false as if built from zero keys
    OpenRaw(cft.Reset(0, 37, 6, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    // Type 2 trivial filter bits - returns false as 0 size trumps 0 probes
    OpenRaw(cft.Reset(0, 0, 0, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    // Bad filter bits - returns true for safety
    // No solution to 0 * x == CACHE_LINE_SIZE
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 0, 6, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Bad filter bits - returns true for safety
    // Can't have 3 * x == 4 for integer x
    OpenRaw(cft.Reset(4, 3, 6, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Bad filter bits - returns true for safety
    // 97 bytes is not a power of two, so not a legal cache line size
    OpenRaw(cft.Reset(97 * 3, 3, 6, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Bad filter bits - returns true for safety
    // 65 bytes is not a power of two, so not a legal cache line size
    OpenRaw(cft.Reset(65 * 3, 3, 6, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Bad filter bits - returns false as if built from zero keys
    // < 5 bytes overall means missing even metadata
    OpenRaw(cft.Reset(static_cast<uint32_t>(-1), 3, 6, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    OpenRaw(cft.Reset(static_cast<uint32_t>(-5), 3, 6, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    // Dubious filter bits - returns same as fill (for now)
    // 31 is not a useful num_probes, nor generated by RocksDB unless directly
    // using filter bits API without BloomFilterPolicy.
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 31, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Dubious filter bits - returns same as fill (for now)
    // Similar, with 127, largest positive char
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 127, fill));
    ASSERT_EQ(fill, Matches("hello"));
    ASSERT_EQ(fill, Matches("world"));

    // Dubious filter bits - returns true (for now)
    // num_probes set to 128 / -128, lowest negative char
    // NB: Bug in implementation interprets this as negative and has same
    // effect as zero probes, but effectively reserves negative char values
    // for future use.
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 128, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));

    // Dubious filter bits - returns true (for now)
    // Similar, with 255 / -1
    OpenRaw(cft.Reset(CACHE_LINE_SIZE, 1, 255, fill));
    ASSERT_TRUE(Matches("hello"));
    ASSERT_TRUE(Matches("world"));
  }
}

INSTANTIATE_TEST_CASE_P(Full, FullBloomTest,
                        testing::Values(BloomFilterPolicy::kLegacyBloom,
                                        BloomFilterPolicy::kFastLocalBloom));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ParseCommandLineFlags(&argc, &argv, true);

  return RUN_ALL_TESTS();
}

#endif  // GFLAGS
