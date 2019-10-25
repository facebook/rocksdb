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

class BlockBasedBloomTest : public testing::Test {
 private:
  std::unique_ptr<const FilterPolicy> policy_;
  std::string filter_;
  std::vector<std::string> keys_;

 public:
  BlockBasedBloomTest()
      : policy_(NewBloomFilterPolicy(FLAGS_bits_per_key, true)) {}

  void Reset() {
    keys_.clear();
    filter_.clear();
  }

  void ResetPolicy(const FilterPolicy* policy = nullptr) {
    if (policy == nullptr) {
      policy_.reset(NewBloomFilterPolicy(FLAGS_bits_per_key, true));
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

  ResetPolicy(NewBloomFilterPolicy(8, true));  // num_probes = 5
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 3589896109U);

  ResetPolicy(NewBloomFilterPolicy(9, true));  // num_probes = 6
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 969445585);

  ResetPolicy(NewBloomFilterPolicy(11, true));  // num_probes = 7
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 1694458207);

  ResetPolicy(NewBloomFilterPolicy(10, true));  // num_probes = 6
  for (int key = 0; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 2373646410U);

  ResetPolicy(NewBloomFilterPolicy(10, true));
  for (int key = 1; key < 87; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()), 1908442116);

  ResetPolicy(NewBloomFilterPolicy(10, true));
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

  BuiltinFilterBitsBuilder* GetBuiltinFilterBitsBuilder() {
    // Throws on bad cast
    return &dynamic_cast<BuiltinFilterBitsBuilder&>(*bits_builder_.get());
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

  bool Matches(const Slice& s) {
    if (bits_reader_ == nullptr) {
      Build();
    }
    return bits_reader_->MayMatch(s);
  }

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
  auto bits_builder = GetBuiltinFilterBitsBuilder();
  for (int n = 1; n < 100; n++) {
    auto space = bits_builder->CalculateSpace(n);
    auto n2 = bits_builder->CalculateNumEntry(space);
    ASSERT_GE(n2, n);
    auto space2 = bits_builder->CalculateSpace(n2);
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
// incompatible way
TEST_F(FullBloomTest, Schema) {
  char buffer[sizeof(int)];

  // Use enough keys so that changing bits / key by 1 is guaranteed to
  // change number of allocated cache lines. So keys > max cache line bits.

  ResetPolicy(NewBloomFilterPolicy(8));  // num_probes = 5
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()),
            SelectByCacheLineSize(1302145999, 2811644657U, 756553699));

  ResetPolicy(NewBloomFilterPolicy(9));  // num_probes = 6
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()),
            SelectByCacheLineSize(2092755149, 661139132, 1182970461));

  ResetPolicy(NewBloomFilterPolicy(11));  // num_probes = 7
  for (int key = 0; key < 2087; key++) {
    Add(Key(key, buffer));
  }
  Build();
  ASSERT_EQ(BloomHash(FilterData()),
            SelectByCacheLineSize(3755609649U, 1812694762, 1449142939));

  ResetPolicy(NewBloomFilterPolicy(10));  // num_probes = 6
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

// A helper class for testing custom or corrupt filter bits as read by
// FullFilterBitsReader.
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

TEST_F(FullBloomTest, RawSchema) {
  RawFilterTester cft;
  // Two probes, about 3/4 bits set: ~50% "FP" rate
  // One 256-byte cache line.
  OpenRaw(cft.ResetWeirdFill(256, 1, 2));
  ASSERT_EQ(uint64_t{11384799501900898790U}, PackedMatches());

  // Two 128-byte cache lines.
  OpenRaw(cft.ResetWeirdFill(256, 2, 2));
  ASSERT_EQ(uint64_t{10157853359773492589U}, PackedMatches());

  // Four 64-byte cache lines.
  OpenRaw(cft.ResetWeirdFill(256, 4, 2));
  ASSERT_EQ(uint64_t{7123594913907464682U}, PackedMatches());
}

TEST_F(FullBloomTest, CorruptFilters) {
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
    OpenRaw(cft.Reset(-1, 3, 6, fill));
    ASSERT_FALSE(Matches("hello"));
    ASSERT_FALSE(Matches("world"));

    OpenRaw(cft.Reset(-5, 3, 6, fill));
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

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ParseCommandLineFlags(&argc, &argv, true);

  return RUN_ALL_TESTS();
}

#endif  // GFLAGS
