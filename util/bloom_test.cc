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

#include <vector>

#include "logging/logging.h"
#include "memory/arena.h"
#include "rocksdb/filter_policy.h"
#include "table/full_filter_bits_builder.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/hash.h"
#include "util/gflags_compat.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_int32(bits_per_key, 10, "");

namespace rocksdb {

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

class BloomTest : public testing::Test {
 private:
  std::unique_ptr<const FilterPolicy> policy_;
  std::string filter_;
  std::vector<std::string> keys_;

 public:
  BloomTest() : policy_(
      NewBloomFilterPolicy(FLAGS_bits_per_key)) {}

  void Reset() {
    keys_.clear();
    filter_.clear();
  }

  void ResetPolicy(const FilterPolicy* policy = nullptr) {
    if (policy == nullptr) {
      policy_.reset(NewBloomFilterPolicy(FLAGS_bits_per_key));
    } else {
      policy_.reset(policy);
    }
    Reset();
  }

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

  Slice FilterData() const {
    return Slice(filter_);
  }

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

TEST_F(BloomTest, EmptyFilter) {
  ASSERT_TRUE(! Matches("hello"));
  ASSERT_TRUE(! Matches("world"));
}

TEST_F(BloomTest, Small) {
  Add("hello");
  Add("world");
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
  ASSERT_TRUE(! Matches("x"));
  ASSERT_TRUE(! Matches("foo"));
}

TEST_F(BloomTest, VaryingLengths) {
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
TEST_F(BloomTest, Schema) {
  char buffer[sizeof(int)];

  ResetPolicy(NewBloomFilterPolicy(8)); // num_probes = 5
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 3589896109U);

  ResetPolicy(NewBloomFilterPolicy(9)); // num_probes = 6
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 969445585);

  ResetPolicy(NewBloomFilterPolicy(11)); // num_probes = 7
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 1694458207);

  ResetPolicy(NewBloomFilterPolicy(10)); // num_probes = 6
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 2373646410U);

  ResetPolicy(NewBloomFilterPolicy(10));
  for (int key = 1; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 1908442116);

  ResetPolicy(NewBloomFilterPolicy(10));
  for (int key = 1; key < 88; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 3057004015U);

  ResetPolicy();
}


// Different bits-per-byte

class FullBloomTest : public testing::Test {
 private:
  std::unique_ptr<const FilterPolicy> policy_;
  std::unique_ptr<FilterBitsBuilder> bits_builder_;
  std::unique_ptr<FilterBitsReader> bits_reader_;
  std::unique_ptr<const char[]> buf_;
  size_t filter_size_;

 public:
  FullBloomTest() :
      policy_(NewBloomFilterPolicy(FLAGS_bits_per_key, false)),
      filter_size_(0) {
    Reset();
  }

  FullFilterBitsBuilder* GetFullFilterBitsBuilder() {
    return dynamic_cast<FullFilterBitsBuilder*>(bits_builder_.get());
  }

  void Reset() {
    bits_builder_.reset(policy_->GetFilterBitsBuilder());
    bits_reader_.reset(nullptr);
    buf_.reset(nullptr);
    filter_size_ = 0;
  }

  void ResetPolicy(const FilterPolicy* policy = nullptr) {
    if (policy == nullptr) {
      policy_.reset(NewBloomFilterPolicy(FLAGS_bits_per_key, false));
    } else {
      policy_.reset(policy);
    }
    Reset();
  }

  void Add(const Slice& s) {
    bits_builder_->AddKey(s);
  }

  void Build() {
    Slice filter = bits_builder_->Finish(&buf_);
    bits_reader_.reset(policy_->GetFilterBitsReader(filter));
    filter_size_ = filter.size();
  }

  size_t FilterSize() const {
    return filter_size_;
  }

  Slice FilterData() {
    return Slice(buf_.get(), filter_size_);
  }

  bool Matches(const Slice& s) {
    if (bits_reader_ == nullptr) {
      Build();
    }
    return bits_reader_->MayMatch(s);
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

TEST_F(FullBloomTest, FilterSize) {
  uint32_t dont_care1, dont_care2;
  auto full_bits_builder = GetFullFilterBitsBuilder();
  for (int n = 1; n < 100; n++) {
    auto space = full_bits_builder->CalculateSpace(n, &dont_care1, &dont_care2);
    auto n2 = full_bits_builder->CalculateNumEntry(space);
    ASSERT_GE(n2, n);
    auto space2 =
        full_bits_builder->CalculateSpace(n2, &dont_care1, &dont_care2);
    ASSERT_EQ(space, space2);
  }
}

TEST_F(FullBloomTest, FullEmptyFilter) {
  // Empty filter is not match, at this level
  ASSERT_TRUE(!Matches("hello"));
  ASSERT_TRUE(!Matches("world"));
}

TEST_F(FullBloomTest, FullSmall) {
  Add("hello");
  Add("world");
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
  ASSERT_TRUE(!Matches("x"));
  ASSERT_TRUE(!Matches("foo"));
}

TEST_F(FullBloomTest, FullVaryingLengths) {
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

    ASSERT_LE(FilterSize(), (size_t)((length * 10 / 8) + CACHE_LINE_SIZE * 2 + 5)) << length;

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
inline uint32_t SelectByCacheLineSize(uint32_t for64,
                                  uint32_t for128,
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
} // namespace

// Ensure the implementation doesn't accidentally change in an
// incompatible way
TEST_F(FullBloomTest, Schema) {
  char buffer[sizeof(int)];

  // Use enough keys so that changing bits / key by 1 is guaranteed to
  // change number of allocated cache lines. So keys > max cache line bits.

  ResetPolicy(NewBloomFilterPolicy(8)); // num_probes = 5
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()),
            SelectByCacheLineSize(1302145999, 2811644657U, 756553699));

  ResetPolicy(NewBloomFilterPolicy(9)); // num_probes = 6
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()),
            SelectByCacheLineSize(2092755149, 661139132, 1182970461));

  ResetPolicy(NewBloomFilterPolicy(11)); // num_probes = 7
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()),
            SelectByCacheLineSize(3755609649U, 1812694762, 1449142939));

  ResetPolicy(NewBloomFilterPolicy(10)); // num_probes = 6
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()),
            SelectByCacheLineSize(1478976371, 2910591341U, 1182970461));

  ResetPolicy(NewBloomFilterPolicy(10));
  for (int key = 1; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()),
            SelectByCacheLineSize(4205696321U, 1132081253U, 2385981855U));

  ResetPolicy(NewBloomFilterPolicy(10));
  for (int key = 1; key < 2088; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()),
            SelectByCacheLineSize(2885052954U, 769447944, 4175124908U));

  ResetPolicy();
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ParseCommandLineFlags(&argc, &argv, true);

  return RUN_ALL_TESTS();
}

#endif  // GFLAGS
