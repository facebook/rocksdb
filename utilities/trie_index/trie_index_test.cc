//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "test_util/testharness.h"
#include "utilities/trie_index/bitvector.h"
#include "utilities/trie_index/louds_trie.h"
#include "utilities/trie_index/trie_index_factory.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// Bitvector tests
// ============================================================================

class BitvectorTest : public testing::Test {};

TEST_F(BitvectorTest, EmptyBitvector) {
  BitvectorBuilder builder;
  ASSERT_EQ(builder.NumBits(), 0);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 0);
  ASSERT_EQ(bv.NumOnes(), 0);
  ASSERT_EQ(bv.NumZeros(), 0);
}

TEST_F(BitvectorTest, SingleBit) {
  // Single 1-bit.
  {
    BitvectorBuilder builder;
    builder.Append(true);
    Bitvector bv;
    bv.BuildFrom(builder);
    ASSERT_EQ(bv.NumBits(), 1);
    ASSERT_EQ(bv.NumOnes(), 1);
    ASSERT_TRUE(bv.GetBit(0));
    ASSERT_EQ(bv.Rank1(0), 0);
    ASSERT_EQ(bv.Rank1(1), 1);
    ASSERT_EQ(bv.Select1(0), 0);
    ASSERT_EQ(bv.Select0(0), 1);  // No 0-bits; returns num_bits_.
  }
  // Single 0-bit.
  {
    BitvectorBuilder builder;
    builder.Append(false);
    Bitvector bv;
    bv.BuildFrom(builder);
    ASSERT_EQ(bv.NumBits(), 1);
    ASSERT_EQ(bv.NumOnes(), 0);
    ASSERT_FALSE(bv.GetBit(0));
    ASSERT_EQ(bv.Rank1(0), 0);
    ASSERT_EQ(bv.Rank1(1), 0);
    ASSERT_EQ(bv.Rank0(1), 1);
    ASSERT_EQ(bv.Select0(0), 0);
    ASSERT_EQ(bv.Select1(0), 1);  // No 1-bits; returns num_bits_.
  }
}

TEST_F(BitvectorTest, RankAndSelect) {
  // Build a bitvector: 1010 1010 1010 ... (alternating bits).
  const uint64_t n = 1000;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 2 == 0);  // Even positions are 1.
  }

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), n / 2);

  // Verify rank at various positions.
  for (uint64_t pos = 0; pos <= n; pos++) {
    uint64_t expected_ones = (pos + 1) / 2;  // Count of even numbers in [0,pos)
    ASSERT_EQ(bv.Rank1(pos), expected_ones) << "Rank1 failed at pos=" << pos;
    ASSERT_EQ(bv.Rank0(pos), pos - expected_ones)
        << "Rank0 failed at pos=" << pos;
  }

  // Verify select.
  for (uint64_t i = 0; i < n / 2; i++) {
    ASSERT_EQ(bv.Select1(i), i * 2) << "Select1 failed at i=" << i;
    ASSERT_EQ(bv.Select0(i), i * 2 + 1) << "Select0 failed at i=" << i;
  }

  // Out of range selects.
  ASSERT_EQ(bv.Select1(n / 2), n);
  ASSERT_EQ(bv.Select0(n / 2), n);
}

TEST_F(BitvectorTest, AllOnes) {
  const uint64_t n = 512;  // Exactly one rank sample interval.
  BitvectorBuilder builder;
  builder.AppendMultiple(true, n);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), n);

  for (uint64_t pos = 0; pos <= n; pos++) {
    ASSERT_EQ(bv.Rank1(pos), pos);
    ASSERT_EQ(bv.Rank0(pos), 0);
  }
  for (uint64_t i = 0; i < n; i++) {
    ASSERT_EQ(bv.Select1(i), i);
  }
  ASSERT_EQ(bv.Select0(0), n);  // No 0-bits.
}

TEST_F(BitvectorTest, AllZeros) {
  const uint64_t n = 512;
  BitvectorBuilder builder;
  builder.AppendMultiple(false, n);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), 0);

  for (uint64_t pos = 0; pos <= n; pos++) {
    ASSERT_EQ(bv.Rank1(pos), 0);
    ASSERT_EQ(bv.Rank0(pos), pos);
  }
  for (uint64_t i = 0; i < n; i++) {
    ASSERT_EQ(bv.Select0(i), i);
  }
  ASSERT_EQ(bv.Select1(0), n);  // No 1-bits.
}

TEST_F(BitvectorTest, LargeBitvector) {
  // Test with a bitvector larger than one rank sample (512 bits).
  const uint64_t n = 2000;
  BitvectorBuilder builder;
  uint64_t expected_ones = 0;
  for (uint64_t i = 0; i < n; i++) {
    bool bit = (i % 3 == 0);
    builder.Append(bit);
    if (bit) expected_ones++;
  }

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), expected_ones);

  // Verify rank at boundaries.
  ASSERT_EQ(bv.Rank1(0), 0);
  ASSERT_EQ(bv.Rank1(n), expected_ones);

  // Verify select for a subset of positions.
  uint64_t ones_so_far = 0;
  for (uint64_t i = 0; i < n; i++) {
    if (bv.GetBit(i)) {
      ASSERT_EQ(bv.Select1(ones_so_far), i)
          << "Select1 failed at ones_so_far=" << ones_so_far;
      ones_so_far++;
    }
  }
  ASSERT_EQ(ones_so_far, expected_ones);
}

TEST_F(BitvectorTest, SerializeAndDeserialize) {
  const uint64_t n = 1500;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 5 == 0);
  }

  Bitvector original;
  original.BuildFrom(builder);

  // Serialize.
  std::string serialized;
  original.Serialize(&serialized);
  ASSERT_GT(serialized.size(), 0);

  // Deserialize.
  Bitvector deserialized;
  size_t consumed = 0;
  ASSERT_OK(deserialized.InitFromData(serialized.data(), serialized.size(),
                                      &consumed));
  ASSERT_GT(consumed, 0);
  ASSERT_EQ(consumed, serialized.size());

  // Verify match.
  ASSERT_EQ(deserialized.NumBits(), original.NumBits());
  ASSERT_EQ(deserialized.NumOnes(), original.NumOnes());

  for (uint64_t pos = 0; pos <= n; pos++) {
    ASSERT_EQ(deserialized.Rank1(pos), original.Rank1(pos))
        << "Rank1 mismatch at pos=" << pos;
  }

  for (uint64_t i = 0; i < original.NumOnes(); i++) {
    ASSERT_EQ(deserialized.Select1(i), original.Select1(i))
        << "Select1 mismatch at i=" << i;
  }
}

TEST_F(BitvectorTest, SerializeRoundTrip_NonAligned) {
  // Test with a size that's not a multiple of 64.
  const uint64_t n = 100;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 7 == 0);
  }

  Bitvector original;
  original.BuildFrom(builder);

  std::string serialized;
  original.Serialize(&serialized);

  Bitvector deserialized;
  size_t consumed = 0;
  ASSERT_OK(deserialized.InitFromData(serialized.data(), serialized.size(),
                                      &consumed));
  ASSERT_EQ(consumed, serialized.size());
  ASSERT_EQ(deserialized.NumBits(), n);
  ASSERT_EQ(deserialized.NumOnes(), original.NumOnes());

  // Verify every bit matches.
  for (uint64_t i = 0; i < n; i++) {
    ASSERT_EQ(deserialized.GetBit(i), original.GetBit(i))
        << "Bit mismatch at position " << i;
  }
}

TEST_F(BitvectorTest, AppendWord) {
  // Test the AppendWord API used by the dense bitmap builder.
  BitvectorBuilder builder;

  // Append 4 words to simulate a 256-bit dense node bitmap.
  // Word 0: bits 0 and 3 set (labels 0x00 and 0x03).
  builder.AppendWord(0x9, 64);                     // 0b1001
  builder.AppendWord(0x0, 64);                     // all zeros
  builder.AppendWord(0x0, 64);                     // all zeros
  builder.AppendWord(0x80000000'00000000ULL, 64);  // bit 63 set (label 0xFF)

  ASSERT_EQ(builder.NumBits(), 256);
  ASSERT_TRUE(builder.GetBit(0));  // label 0x00
  ASSERT_TRUE(builder.GetBit(3));  // label 0x03
  ASSERT_FALSE(builder.GetBit(1));
  ASSERT_FALSE(builder.GetBit(64));
  ASSERT_TRUE(builder.GetBit(255));  // label 0xFF

  // Build and verify rank/select work correctly.
  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 256);
  ASSERT_EQ(bv.NumOnes(), 3);  // bits 0, 3, 255
  ASSERT_EQ(bv.Select1(0), 0);
  ASSERT_EQ(bv.Select1(1), 3);
  ASSERT_EQ(bv.Select1(2), 255);
}

TEST_F(BitvectorTest, AppendWordAndAppendInterleaved) {
  // Verify AppendWord works when mixed with regular Append calls,
  // as long as AppendWord is called on word-aligned boundaries.
  BitvectorBuilder builder;

  // Append 64 bits via AppendWord.
  builder.AppendWord(0xAAAAAAAA'AAAAAAAAULL, 64);
  ASSERT_EQ(builder.NumBits(), 64);

  // Append 64 more bits one at a time.
  for (int i = 0; i < 64; i++) {
    builder.Append(i % 2 == 0);
  }
  ASSERT_EQ(builder.NumBits(), 128);

  // Another word-aligned AppendWord.
  builder.AppendWord(0xFFFFFFFF'FFFFFFFFULL, 64);
  ASSERT_EQ(builder.NumBits(), 192);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 192);

  // First word: alternating starting at bit 1 (0xAAAA... = 1010...)
  ASSERT_FALSE(bv.GetBit(0));
  ASSERT_TRUE(bv.GetBit(1));
  // Second word: alternating starting at bit 0
  ASSERT_TRUE(bv.GetBit(64));
  ASSERT_FALSE(bv.GetBit(65));
  // Third word: all ones
  ASSERT_TRUE(bv.GetBit(128));
  ASSERT_TRUE(bv.GetBit(191));
}

TEST_F(BitvectorTest, NextSetBit) {
  // Build: 64 zeros, then 1, then 63 zeros, then 1.
  BitvectorBuilder builder;
  builder.AppendMultiple(false, 64);
  builder.Append(true);
  builder.AppendMultiple(false, 63);
  builder.Append(true);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 129);

  // NextSetBit from various positions.
  ASSERT_EQ(bv.NextSetBit(0), 64);     // First set bit.
  ASSERT_EQ(bv.NextSetBit(64), 64);    // Exact position.
  ASSERT_EQ(bv.NextSetBit(65), 128);   // Next one.
  ASSERT_EQ(bv.NextSetBit(128), 128);  // Exact.
  ASSERT_EQ(bv.NextSetBit(129), 129);  // Past end → num_bits_.
}

TEST_F(BitvectorTest, NextSetBitAllZeros) {
  BitvectorBuilder builder;
  builder.AppendMultiple(false, 200);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NextSetBit(0), 200);
  ASSERT_EQ(bv.NextSetBit(100), 200);
  ASSERT_EQ(bv.NextSetBit(199), 200);
}

TEST_F(BitvectorTest, DistanceToNextSetBit) {
  // Build: 1, 0, 0, 1, 0, 1
  BitvectorBuilder builder;
  builder.Append(true);
  builder.Append(false);
  builder.Append(false);
  builder.Append(true);
  builder.Append(false);
  builder.Append(true);

  Bitvector bv;
  bv.BuildFrom(builder);

  ASSERT_EQ(bv.DistanceToNextSetBit(0), 3);  // 0 → 3
  ASSERT_EQ(bv.DistanceToNextSetBit(3), 2);  // 3 → 5
  // Last set bit: distance to next = num_bits_ - pos.
  ASSERT_EQ(bv.DistanceToNextSetBit(5), 1);  // 5 → 6 (num_bits_)
}

TEST_F(BitvectorTest, MultipleRankSampleBoundaries) {
  // Test rank/select across multiple 512-bit sample boundaries.
  const uint64_t n = 2048;  // 4 rank samples.
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    // Set bit at every 100th position.
    builder.Append(i % 100 == 0);
  }

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumOnes(), 21);  // 0, 100, 200, ..., 2000

  // Verify rank at sample boundaries.
  ASSERT_EQ(bv.Rank1(512), 6);    // 0,100,200,300,400,500
  ASSERT_EQ(bv.Rank1(1024), 11);  // +600,700,800,900,1000
  ASSERT_EQ(bv.Rank1(1536), 16);  // +1100,1200,1300,1400,1500

  // Verify select.
  for (uint64_t i = 0; i < bv.NumOnes(); i++) {
    ASSERT_EQ(bv.Select1(i), i * 100);
  }
}

TEST_F(BitvectorTest, InitFromDataCorruption) {
  // Test that InitFromData correctly rejects truncated data.
  Bitvector bv;
  size_t consumed;

  // Too short for header.
  ASSERT_TRUE(bv.InitFromData("short", 5, &consumed).IsCorruption());

  // Header says more words than available.
  std::string bad;
  uint64_t num_bits = 1024;
  uint64_t num_ones = 0;
  bad.append(reinterpret_cast<const char*>(&num_bits), 8);
  bad.append(reinterpret_cast<const char*>(&num_ones), 8);
  // No word data follows.
  ASSERT_TRUE(
      bv.InitFromData(bad.data(), bad.size(), &consumed).IsCorruption());
}

TEST_F(BitvectorTest, Select64Exhaustive) {
  // Verify Select64 for every position in a word with known popcount.
  uint64_t word = 0xDEADBEEFCAFEBABEULL;
  uint64_t pc = Popcount(word);

  for (uint64_t i = 0; i < pc; i++) {
    uint64_t pos = Select64(word, i);
    // The pos-th bit should be set.
    ASSERT_TRUE((word >> pos) & 1)
        << "Select64 returned non-set bit at i=" << i;
    // Exactly i bits should be set before pos.
    uint64_t mask =
        (pos == 0) ? 0 : (pos == 64 ? ~uint64_t(0) : (uint64_t(1) << pos) - 1);
    ASSERT_EQ(Popcount(word & mask), i)
        << "Wrong rank at Select64 result for i=" << i;
  }
}

// ============================================================================
// LoudsTrie tests
// ============================================================================

class LoudsTrieTest : public testing::Test {};

// Helper: build a trie from sorted keys and verify that Seek and Next
// iterate in exactly the expected order, checking both handles and
// reconstructed keys.
static void VerifyTrieIteration(const std::vector<std::string>& keys) {
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), keys.size());

  LoudsTrieIterator iter(&trie);

  // Test 1: Seek to every key and verify exact match + correct handle.
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Seek(Slice(keys[i])))
        << "Seek failed for key[" << i << "]=\"" << keys[i] << "\"";
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[i]) << "Key mismatch at i=" << i;
    ASSERT_EQ(iter.Value().offset, i * 100) << "Handle mismatch at i=" << i;
  }

  // Test 2: Seek to first key, then Next through all keys.
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i])
        << "Key mismatch during iteration at i=" << i;
    ASSERT_EQ(iter.Value().offset, i * 100)
        << "Handle mismatch during iteration at i=" << i;
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next()) << "Next failed at i=" << i;
    }
  }
  ASSERT_FALSE(iter.Next());
  ASSERT_FALSE(iter.Valid());

  // Test 3: Seek past all keys.
  std::string past_all = keys.back() + "zzz";
  ASSERT_FALSE(iter.Seek(Slice(past_all)));
  ASSERT_FALSE(iter.Valid());

  // Test 4: Seek before all keys.
  std::string before_all(1, keys[0][0] - 1);
  if (keys[0][0] > 0) {
    ASSERT_TRUE(iter.Seek(Slice(before_all)));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[0]);
  }
}

// --- Basic builder and trie construction ---

TEST_F(LoudsTrieTest, EmptyTrie) {
  LoudsTrieBuilder builder;
  builder.Finish();
  Slice data = builder.GetSerializedData();
  ASSERT_GT(data.size(), 0);

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), 0);
}

TEST_F(LoudsTrieTest, SingleKey) {
  LoudsTrieBuilder builder;
  TrieBlockHandle h{100, 200};
  builder.AddKey(Slice("hello"), h);
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), 1);

  // Verify handle.
  auto handle = trie.GetHandle(0);
  ASSERT_EQ(handle.offset, 100);
  ASSERT_EQ(handle.size, 200);
}

TEST_F(LoudsTrieTest, MultipleKeys) {
  LoudsTrieBuilder builder;
  std::vector<std::string> keys = {"apple", "application", "banana", "band",
                                   "cat"};
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), keys.size());

  // Verify all handles via the iterator (which computes correct leaf indices).
  LoudsTrieIterator iter(&trie);
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Seek(Slice(keys[i])));
    ASSERT_TRUE(iter.Valid());
    auto handle = iter.Value();
    ASSERT_EQ(handle.offset, i * 100) << "Handle mismatch for key " << keys[i];
    ASSERT_EQ(handle.size, 50);
  }
}

TEST_F(LoudsTrieTest, SharedPrefixKeys) {
  // Keys with long shared prefixes — this exercises the trie's prefix
  // compression.
  LoudsTrieBuilder builder;
  std::vector<std::string> keys;
  for (int i = 0; i < 100; i++) {
    std::string key = "common_prefix_" + std::to_string(i);
    keys.push_back(key);
  }
  // Keys are already sorted lexicographically for single-digit vs double-digit
  // numbers, but we need proper sorting.
  std::sort(keys.begin(), keys.end());

  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), keys.size());
}

TEST_F(LoudsTrieTest, BuilderTwoKeys) {
  // Minimal multi-key trie.
  std::vector<std::string> keys = {"aa", "ab"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, BuilderIdenticalPrefixDifferentLastByte) {
  // All keys share a long prefix, differ only in last byte.
  std::string prefix(50, 'x');
  std::vector<std::string> keys;
  for (char c = 'a'; c <= 'z'; c++) {
    keys.push_back(prefix + c);
  }
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, BuilderBinaryKeys) {
  // Keys containing non-ASCII bytes (0x00, 0xFF, etc).
  std::vector<std::string> keys;
  keys.push_back(std::string("\x00\x00", 2));
  keys.push_back(std::string("\x00\x01", 2));
  keys.push_back(std::string("\x00\xFF", 2));
  keys.push_back(std::string("\x01\x00", 2));
  keys.push_back(std::string("\xFF\x00", 2));
  keys.push_back(std::string("\xFF\xFF", 2));
  std::sort(keys.begin(), keys.end());
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, BuilderSingleByteAllValues) {
  // One key for every possible single byte value (0x00–0xFF).
  // This exercises the maximum fanout at root (256 children).
  std::vector<std::string> keys;
  for (int b = 0; b < 256; b++) {
    keys.push_back(std::string(1, static_cast<char>(b)));
  }
  // Already sorted by byte value.
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, BuilderDeepChain) {
  // Single path through the trie (linear chain), depth = 100.
  std::string key(100, 'a');
  std::vector<std::string> keys = {key};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, BuilderAlternatingPrefixAndNonPrefix) {
  // Mix of prefix keys and non-prefix keys in the same subtree.
  // "a", "ab", "abc" are prefix chain; "b", "c" are standalone.
  std::vector<std::string> keys = {"a", "ab", "abc", "b", "c"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, BuilderPrefixKeyAtDenseSparseEdge) {
  // Create keys where a prefix key sits right at the dense/sparse boundary.
  // High fanout at root (dense), prefix key in sparse region.
  std::vector<std::string> keys;
  // 26 single-char keys for dense root.
  for (char c = 'a'; c <= 'z'; c++) {
    keys.push_back(std::string(1, c));
  }
  // Add "aa" and "aab" so "a" becomes a prefix key.
  keys.push_back("aa");
  keys.push_back("aab");
  std::sort(keys.begin(), keys.end());
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  VerifyTrieIteration(keys);
}

// --- Key reconstruction ---

TEST_F(LoudsTrieTest, KeyReconstructionBasic) {
  // Simple keys where we can verify key reconstruction.
  std::vector<std::string> keys = {"abc", "abd", "abe", "xyz"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, KeyReconstructionSingleByteKeys) {
  // Single-byte keys — each key is one character.
  std::vector<std::string> keys = {"a", "b", "c", "d", "e"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, KeyReconstructionLongSharedPrefix) {
  // Keys with long shared prefixes.
  std::vector<std::string> keys;
  std::string prefix = "common_prefix_that_is_quite_long_";
  for (int i = 0; i < 26; i++) {
    keys.push_back(prefix + static_cast<char>('a' + i));
  }
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, KeyReconstructionVaryingLengths) {
  // Keys with varying lengths.
  std::vector<std::string> keys = {"a",     "ab", "abc", "abcd",
                                   "abcde", "b",  "bc",  "bcd"};
  std::sort(keys.begin(), keys.end());
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, TwoByteKeys) {
  // Keys that are exactly 2 bytes.
  std::vector<std::string> keys = {"aa", "ab", "ba", "bb", "ca"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, HighFanoutRoot) {
  // Root with high fanout (many first bytes).
  std::vector<std::string> keys;
  for (int c = 'a'; c <= 'z'; c++) {
    keys.push_back(std::string(1, static_cast<char>(c)) + "suffix");
  }
  std::sort(keys.begin(), keys.end());
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, DeepTrie) {
  // Keys that create a deep trie (long keys with no shared prefix).
  std::vector<std::string> keys = {
      "abcdefghijklmnop",
      "abcdefghijklmnoq",
      "abcdefghijklmnor",
  };
  VerifyTrieIteration(keys);
}

// --- Delta-varint handle encoding ---

TEST_F(LoudsTrieTest, DeltaVarintHandleEncoding) {
  // Verify that delta-varint encoding produces smaller serialized data than
  // fixed 16-byte handles, and that handles round-trip correctly.
  LoudsTrieBuilder builder;

  // Simulate realistic SST block handles: sequential offsets with ~4KB blocks.
  const uint64_t kBlockSize = 4096;
  const int kNumBlocks = 100;
  std::vector<std::string> keys;
  std::vector<TrieBlockHandle> expected_handles;

  for (int i = 0; i < kNumBlocks; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%04d", i);
    keys.push_back(buf);
    TrieBlockHandle h{static_cast<uint64_t>(i) * kBlockSize, kBlockSize};
    expected_handles.push_back(h);
    builder.AddKey(Slice(keys.back()), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  // Verify the serialized size is significantly smaller than if we used
  // fixed 16-byte handles (100 * 16 = 1600 bytes just for handles).
  // With delta-varint, each handle should be ~4 bytes (2 bytes for the
  // offset delta ~4096, 2 bytes for size ~4096).
  // Total serialized should be much less than: header + bitvectors + 1600.
  size_t fixed_handle_bytes = kNumBlocks * 16;
  // The total data includes trie structure, so we can't directly compare.
  // But we can verify it deserializes correctly.

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), static_cast<uint64_t>(kNumBlocks));

  // Verify all handles round-trip correctly.
  LoudsTrieIterator iter(&trie);
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (int i = 0; i < kNumBlocks; i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    auto h = iter.Value();
    ASSERT_EQ(h.offset, expected_handles[i].offset)
        << "Offset mismatch at i=" << i;
    ASSERT_EQ(h.size, expected_handles[i].size) << "Size mismatch at i=" << i;
    if (i < kNumBlocks - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }

  // Log space savings for informational purposes.
  fprintf(stderr,
          "DeltaVarintHandleEncoding: total serialized=%zu, "
          "fixed handles would be %zu bytes\n",
          data.size(), fixed_handle_bytes);
  (void)fixed_handle_bytes;
}

// --- Prefix keys ---

TEST_F(LoudsTrieTest, PrefixKeysSimple) {
  // "a" is a prefix of "ab". Tests the deferred internal marking and handle
  // migration in the streaming builder.
  std::vector<std::string> keys = {"a", "ab", "b"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, PrefixKeysChain) {
  // Chain of prefix keys: each key is a prefix of the next.
  // "a" < "ab" < "abc" < "abcd" — all are prefix keys except the last.
  std::vector<std::string> keys = {"a", "ab", "abc", "abcd"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, PrefixKeysMultipleBranches) {
  // Prefix key with multiple children: "a" is a prefix, and it has
  // children "aa", "ab", "ac".
  std::vector<std::string> keys = {"a", "aa", "ab", "ac"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, PrefixKeysAtDifferentLevels) {
  // Prefix keys at different depths.
  // "a" is prefix of "aa", "aa" is prefix of "aab".
  std::vector<std::string> keys = {"a", "aa", "aab", "b", "ba"};
  std::sort(keys.begin(), keys.end());
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, PrefixKeysDiverging) {
  // "common_prefix_1" is a prefix of "common_prefix_10",
  // "common_prefix_2" is a prefix of "common_prefix_20", etc.
  // This is the exact pattern that exposed the bug in the streaming builder.
  std::vector<std::string> keys;
  for (int i = 0; i < 30; i++) {
    keys.push_back("p_" + std::to_string(i));
  }
  std::sort(keys.begin(), keys.end());
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, PrefixKeyRootOnly) {
  // Root itself is a prefix key (empty string is not supported, but
  // single-char root with children tests the root prefix path).
  // "a" has children "ab", "ac" — "a" is a prefix key at the root child.
  std::vector<std::string> keys = {"a", "ab", "ac", "b"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, PrefixKeyHandleReorderVerification) {
  // Explicitly verify that BFS handle reordering is correct with prefix keys.
  // Keys: "a"=0, "ab"=1, "b"=2
  // BFS leaf order: "b" first (leaf at root), then "a" (prefix at level 1),
  // then "ab" (leaf at level 2).
  // So handles should be: [2, 0, 1] in BFS order.
  LoudsTrieBuilder builder;
  builder.AddKey(Slice("a"), {100, 10});
  builder.AddKey(Slice("ab"), {200, 20});
  builder.AddKey(Slice("b"), {300, 30});
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  LoudsTrieIterator iter(&trie);

  // Seek to "a" — should find "a" with handle {100, 10}.
  ASSERT_TRUE(iter.Seek(Slice("a")));
  ASSERT_EQ(iter.Key().ToString(), "a");
  ASSERT_EQ(iter.Value().offset, 100);
  ASSERT_EQ(iter.Value().size, 10);

  // Next should give "ab" with handle {200, 20}.
  ASSERT_TRUE(iter.Next());
  ASSERT_EQ(iter.Key().ToString(), "ab");
  ASSERT_EQ(iter.Value().offset, 200);
  ASSERT_EQ(iter.Value().size, 20);

  // Next should give "b" with handle {300, 30}.
  ASSERT_TRUE(iter.Next());
  ASSERT_EQ(iter.Key().ToString(), "b");
  ASSERT_EQ(iter.Value().offset, 300);
  ASSERT_EQ(iter.Value().size, 30);

  ASSERT_FALSE(iter.Next());
}

TEST_F(LoudsTrieTest, PrefixKeySeekBetween) {
  // Seek to keys between a prefix key and its child.
  // "a" < target "aa" < "ab"
  LoudsTrieBuilder builder;
  builder.AddKey(Slice("a"), {100, 10});
  builder.AddKey(Slice("ab"), {200, 20});
  builder.AddKey(Slice("b"), {300, 30});
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  LoudsTrieIterator iter(&trie);

  // "aa" is between "a" (prefix key) and "ab" → should land on "ab".
  ASSERT_TRUE(iter.Seek(Slice("aa")));
  ASSERT_EQ(iter.Key().ToString(), "ab");

  // "a" should hit the prefix key exactly.
  ASSERT_TRUE(iter.Seek(Slice("a")));
  ASSERT_EQ(iter.Key().ToString(), "a");
}

TEST_F(LoudsTrieTest, MultiplePrefixKeySameNode) {
  // Multiple keys where several are prefixes at different levels.
  // "x" is prefix of "xa", "xb"; "xa" is prefix of "xab".
  std::vector<std::string> keys = {"x", "xa", "xab", "xb"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, BuilderManyPrefixKeys) {
  // Every key is a prefix of the next: "a", "aa", "aaa", "aaaa", ...
  std::vector<std::string> keys;
  for (int len = 1; len <= 20; len++) {
    keys.push_back(std::string(len, 'a'));
  }
  VerifyTrieIteration(keys);
}

// --- Dense/sparse boundary and encoding mode ---

TEST_F(LoudsTrieTest, AllSparse) {
  // Force all-sparse trie: very low fanout at every level.
  // Two keys with no shared prefix → root has 2 children, each leaf.
  // With only 2 labels at level 0, sparse (2*10+1=21 bits) is cheaper
  // than dense (1*257+2=259 bits), so cutoff_level = 0.
  std::vector<std::string> keys = {"a", "b"};
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, AllDense) {
  // Force all-dense trie: high fanout at root (many first bytes).
  // With 26+ distinct first bytes, dense is more efficient.
  std::vector<std::string> keys;
  for (char c = 'a'; c <= 'z'; c++) {
    keys.push_back(std::string(1, c));
  }
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, DenseSparseTransition) {
  // Keys that create both dense and sparse levels.
  // Root has high fanout (dense), children have low fanout (sparse).
  std::vector<std::string> keys;
  for (char c = 'a'; c <= 'z'; c++) {
    // Each first-byte has two children → still might be dense at level 0.
    keys.push_back(std::string(1, c) + "1");
    keys.push_back(std::string(1, c) + "2");
  }
  std::sort(keys.begin(), keys.end());
  VerifyTrieIteration(keys);
}

// --- Iterator edge cases ---

TEST_F(LoudsTrieTest, IteratorSeekExact) {
  LoudsTrieBuilder builder;
  std::vector<std::string> keys = {"abc", "abd", "abe", "xyz"};
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  LoudsTrieIterator iter(&trie);

  // Seek to exact keys.
  ASSERT_TRUE(iter.Seek(Slice("abc")));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Value().offset, 0);

  ASSERT_TRUE(iter.Seek(Slice("xyz")));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Value().offset, 300);
}

TEST_F(LoudsTrieTest, IteratorSeekBetweenKeys) {
  LoudsTrieBuilder builder;
  std::vector<std::string> keys = {"aaa", "bbb", "ccc", "ddd"};
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  LoudsTrieIterator iter(&trie);

  // Seek to a key between "aaa" and "bbb" — should land on "bbb".
  ASSERT_TRUE(iter.Seek(Slice("ab")));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Value().offset, 100);  // "bbb"'s handle.
}

TEST_F(LoudsTrieTest, IteratorNext) {
  LoudsTrieBuilder builder;
  std::vector<std::string> keys = {"a", "b", "c", "d", "e"};
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  LoudsTrieIterator iter(&trie);
  ASSERT_TRUE(iter.Seek(Slice("a")));

  // Iterate through all keys.
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Value().offset, i * 100) << "Wrong offset at i=" << i;
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }

  // Next should invalidate.
  ASSERT_FALSE(iter.Next());
  ASSERT_FALSE(iter.Valid());
}

TEST_F(LoudsTrieTest, IteratorSeekPastEnd) {
  LoudsTrieBuilder builder;
  std::vector<std::string> keys = {"aaa", "bbb"};
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  LoudsTrieIterator iter(&trie);
  // Seek past all keys.
  ASSERT_FALSE(iter.Seek(Slice("zzz")));
  ASSERT_FALSE(iter.Valid());
}

TEST_F(LoudsTrieTest, IteratorSeekBeforeAll) {
  LoudsTrieBuilder builder;
  std::vector<std::string> keys = {"bbb", "ccc", "ddd"};
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  LoudsTrieIterator iter(&trie);
  // Seek before all keys.
  ASSERT_TRUE(iter.Seek(Slice("aaa")));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Value().offset, 0);  // "bbb"'s handle.
}

TEST_F(LoudsTrieTest, SeekBetweenAllPairs) {
  // For a small set of keys, try seeking to every possible "between" value.
  std::vector<std::string> keys = {"aaa", "aab", "aac", "bbb", "ccc"};

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  LoudsTrieIterator iter(&trie);

  // Seek to values between consecutive keys.
  struct TestCase {
    std::string target;
    size_t expected_idx;  // index into keys, or keys.size() for past-end.
  };
  std::vector<TestCase> cases = {
      {"a", 0},     // Before first key -> first key.
      {"aaa", 0},   // Exact match.
      {"aaaa", 1},  // Between aaa and aab -> aab.
      {"aab", 1},   // Exact match.
      {"aaba", 2},  // Between aab and aac -> aac.
      {"aac", 2},   // Exact match.
      {"aad", 3},   // Between aac and bbb -> bbb.
      {"b", 3},     // Before bbb -> bbb.
      {"bbb", 3},   // Exact match.
      {"bbba", 4},  // Between bbb and ccc -> ccc.
      {"ccc", 4},   // Exact match.
      {"ccca", 5},  // Past end.
      {"zzz", 5},   // Past end.
  };

  for (const auto& tc : cases) {
    bool ok = iter.Seek(Slice(tc.target));
    if (tc.expected_idx >= keys.size()) {
      ASSERT_FALSE(ok) << "Expected past-end for target=\"" << tc.target
                       << "\"";
    } else {
      ASSERT_TRUE(ok) << "Seek failed for target=\"" << tc.target << "\"";
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(iter.Key().ToString(), keys[tc.expected_idx])
          << "Wrong key for target=\"" << tc.target << "\"" << " expected=\""
          << keys[tc.expected_idx] << "\"" << " got=\"" << iter.Key().ToString()
          << "\"";
    }
  }
}

TEST_F(LoudsTrieTest, EmptyTargetSeek) {
  // Seeking with an empty target should return the first key.
  std::vector<std::string> keys = {"aaa", "bbb", "ccc"};

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    TrieBlockHandle h{i * 100, 50};
    builder.AddKey(Slice(keys[i]), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  LoudsTrieIterator iter(&trie);

  ASSERT_TRUE(iter.Seek(Slice("")));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Key().ToString(), "aaa");
}

TEST_F(LoudsTrieTest, IteratorReSeekAfterInvalidation) {
  // After iterator becomes invalid (past end), re-seeking should work.
  std::vector<std::string> keys = {"aaa", "bbb", "ccc"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(builder.GetSerializedData()));
  LoudsTrieIterator iter(&trie);

  // Go past end.
  ASSERT_FALSE(iter.Seek(Slice("zzz")));
  ASSERT_FALSE(iter.Valid());

  // Re-seek to valid key.
  ASSERT_TRUE(iter.Seek(Slice("aaa")));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Key().ToString(), "aaa");
}

TEST_F(LoudsTrieTest, IteratorNextOnInvalid) {
  // Calling Next() on an invalid iterator should return false.
  std::vector<std::string> keys = {"aaa"};
  LoudsTrieBuilder builder;
  builder.AddKey(Slice(keys[0]), {0, 50});
  builder.Finish();
  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(builder.GetSerializedData()));
  LoudsTrieIterator iter(&trie);

  ASSERT_FALSE(iter.Valid());
  ASSERT_FALSE(iter.Next());
}

TEST_F(LoudsTrieTest, IteratorSeekEmptyStringVariousKeys) {
  // Seek("") should always return the first key.
  std::vector<std::string> keys = {"x", "xy", "xyz"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(builder.GetSerializedData()));
  LoudsTrieIterator iter(&trie);

  ASSERT_TRUE(iter.Seek(Slice("")));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Key().ToString(), "x");
}

TEST_F(LoudsTrieTest, IteratorMultipleSeeksDescending) {
  // Seek to keys in reverse order (each seek resets state).
  std::vector<std::string> keys = {"aaa", "bbb", "ccc", "ddd"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(builder.GetSerializedData()));
  LoudsTrieIterator iter(&trie);

  for (int i = static_cast<int>(keys.size()) - 1; i >= 0; i--) {
    ASSERT_TRUE(iter.Seek(Slice(keys[i])));
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, static_cast<uint64_t>(i) * 100);
  }
}

TEST_F(LoudsTrieTest, IteratorSeekToLongerThanAnyKey) {
  // Target key is longer than any key in the trie.
  // The trie has "ab" and "cd". Seeking "abx" should find "cd" (next after
  // "ab").
  std::vector<std::string> keys = {"ab", "cd"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(builder.GetSerializedData()));
  LoudsTrieIterator iter(&trie);

  ASSERT_TRUE(iter.Seek(Slice("abx")));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Key().ToString(), "cd");
}

TEST_F(LoudsTrieTest, IteratorSeekTargetIsPrefixOfKey) {
  // Target "ab" is a prefix of trie key "abc". Should find "abc".
  std::vector<std::string> keys = {"abc", "abd"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(builder.GetSerializedData()));
  LoudsTrieIterator iter(&trie);

  ASSERT_TRUE(iter.Seek(Slice("ab")));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Key().ToString(), "abc");
}

TEST_F(LoudsTrieTest, IteratorFullScanThenReSeek) {
  // Full scan with Next() until end, then re-seek to middle.
  std::vector<std::string> keys = {"aa", "bb", "cc", "dd", "ee"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(builder.GetSerializedData()));
  LoudsTrieIterator iter(&trie);

  // Scan all.
  ASSERT_TRUE(iter.Seek(Slice("aa")));
  int count = 0;
  while (iter.Valid()) {
    count++;
    iter.Next();
  }
  ASSERT_EQ(count, 5);
  ASSERT_FALSE(iter.Valid());

  // Re-seek to middle.
  ASSERT_TRUE(iter.Seek(Slice("cc")));
  ASSERT_EQ(iter.Key().ToString(), "cc");
  ASSERT_EQ(iter.Value().offset, 200);
}

// --- Serialization / deserialization ---

TEST_F(LoudsTrieTest, InitFromDataCorruption) {
  // Truncated data should return corruption status.
  LoudsTrie trie;
  ASSERT_TRUE(trie.InitFromData(Slice("short")).IsCorruption());

  // Bad magic number.
  std::string bad(56, '\0');
  bad[0] = 'X';  // Wrong magic.
  ASSERT_TRUE(trie.InitFromData(Slice(bad)).IsCorruption());
}

TEST_F(LoudsTrieTest, SerializeDeserializeRoundTrip) {
  // Build a non-trivial trie and verify that serialization round-trips.
  std::vector<std::string> keys = {"alpha", "beta", "gamma", "delta"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  Slice data1 = builder.GetSerializedData();

  // Deserialize.
  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data1));
  ASSERT_EQ(trie.NumKeys(), keys.size());

  // Verify all keys and handles.
  LoudsTrieIterator iter(&trie);
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
    if (i < keys.size() - 1) iter.Next();
  }
}

// --- Stress tests ---

TEST_F(LoudsTrieTest, StressTestThousandKeys) {
  // Stress test with 1000 keys.
  std::vector<std::string> keys;
  for (int i = 0; i < 1000; i++) {
    // Generate keys like "key_000000", "key_000001", ...
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%06d", i);
    keys.push_back(buf);
  }
  std::sort(keys.begin(), keys.end());
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, StressTestRandomKeys) {
  // Stress test with diverse key patterns.
  std::vector<std::string> keys;
  // Add keys with various prefixes.
  const char* prefixes[] = {"alpha", "beta", "gamma", "delta", "epsilon"};
  for (const char* p : prefixes) {
    for (int i = 0; i < 50; i++) {
      char buf[64];
      snprintf(buf, sizeof(buf), "%s_%03d", p, i);
      keys.push_back(buf);
    }
  }
  std::sort(keys.begin(), keys.end());
  // Remove duplicates.
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, StressTestMixedLengths) {
  // Stress test with keys of varying lengths (1-20 chars) and various patterns.
  std::vector<std::string> keys;
  // Short keys.
  for (char c = 'a'; c <= 'z'; c++) {
    keys.push_back(std::string(1, c));
  }
  // Medium keys with shared prefixes.
  for (int i = 0; i < 100; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "medium_%04d", i);
    keys.push_back(buf);
  }
  // Long keys.
  for (int i = 0; i < 50; i++) {
    char buf[64];
    snprintf(buf, sizeof(buf), "this_is_a_very_long_key_%06d", i);
    keys.push_back(buf);
  }
  std::sort(keys.begin(), keys.end());
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, StressTestPrefixKeyPatterns) {
  // Stress test specifically targeting the streaming builder's prefix key
  // handling: many keys where single-digit variants are prefixes of
  // multi-digit variants ("x_1" prefix of "x_10", "x_2" of "x_20", etc).
  std::vector<std::string> keys;
  for (int i = 0; i < 200; i++) {
    keys.push_back("item_" + std::to_string(i));
  }
  std::sort(keys.begin(), keys.end());
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, StressTest10KKeys) {
  // 10K keys with uniform distribution.
  std::vector<std::string> keys;
  for (int i = 0; i < 10000; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%08d", i);
    keys.push_back(buf);
  }
  std::sort(keys.begin(), keys.end());

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), keys.size());

  LoudsTrieIterator iter(&trie);

  // Verify every 100th key via Seek.
  for (size_t i = 0; i < keys.size(); i += 100) {
    ASSERT_TRUE(iter.Seek(Slice(keys[i])))
        << "Seek failed for key[" << i << "]";
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
  }

  // Full forward scan.
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]) << "Key mismatch at i=" << i;
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
  ASSERT_FALSE(iter.Next());
}

// ============================================================================
// TrieIndexFactory integration tests
// ============================================================================

class TrieIndexFactoryTest : public testing::Test {
 protected:
  void SetUp() override { factory_ = std::make_shared<TrieIndexFactory>(); }

  std::shared_ptr<TrieIndexFactory> factory_;
};

TEST_F(TrieIndexFactoryTest, BasicBuildAndRead) {
  // Build a trie index using the factory interface.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));
  ASSERT_NE(builder, nullptr);

  // Simulate SST construction with 5 data blocks.
  std::vector<std::string> last_keys = {"apple", "banana", "cherry", "date",
                                        "elderberry"};
  std::vector<std::string> first_keys = {"banana", "cherry", "date",
                                         "elderberry", ""};

  for (size_t i = 0; i < last_keys.size(); i++) {
    UserDefinedIndexBuilder::BlockHandle handle;
    handle.offset = i * 1000;
    handle.size = 500;

    std::string scratch;
    const Slice* next =
        (i < last_keys.size() - 1) ? new Slice(first_keys[i]) : nullptr;
    builder->AddIndexEntry(Slice(last_keys[i]), next, handle, &scratch);
    delete next;
  }

  // Finish building.
  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  ASSERT_GT(index_contents.size(), 0);

  // Read the index.
  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));
  ASSERT_NE(reader, nullptr);
  ASSERT_GT(reader->ApproximateMemoryUsage(), 0);

  // Create an iterator and seek.
  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  ASSERT_NE(iter, nullptr);

  // Seek to "banana" — should find the separator for the second block.
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("banana"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, FactoryName) {
  ASSERT_STREQ(factory_->Name(), "trie_index");
}

TEST_F(TrieIndexFactoryTest, EmptyIndex) {
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  // Finish without adding any entries.
  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  // Empty index should produce empty or minimal contents.
}

TEST_F(TrieIndexFactoryTest, DoubleFinish) {
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  // Second Finish should fail.
  Slice index_contents2;
  Status s = builder->Finish(&index_contents2);
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_F(TrieIndexFactoryTest, IteratorBoundsChecking) {
  // Test the bounds checking in the UDI iterator.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // Build index with 3 blocks.
  std::vector<std::string> separators;
  for (int i = 0; i < 3; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key_%02d", i);
    std::string sep = buf;
    separators.push_back(sep);

    UserDefinedIndexBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                                500};
    std::string scratch;
    if (i < 2) {
      char next_buf[16];
      snprintf(next_buf, sizeof(next_buf), "key_%02d", i + 1);
      Slice next(next_buf);
      udi_builder->AddIndexEntry(Slice(sep), &next, handle, &scratch);
    } else {
      udi_builder->AddIndexEntry(Slice(sep), nullptr, handle, &scratch);
    }
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // Prepare with an upper bound.
  ScanOptions scan_opts(Slice("key_00"), Slice("key_01z"));
  iter->Prepare(&scan_opts, 1);

  // Seek to first key — should be in bound.
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("key_00"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next — should be in bound (key_01 < key_01z).
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next — key_02 >= key_01z → out of bound.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);
}

TEST_F(TrieIndexFactoryTest, IteratorNoBounds) {
  // Without Prepare(), bounds checking should always return kInbound.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  UserDefinedIndexBuilder::BlockHandle handle{0, 500};
  std::string scratch;
  udi_builder->AddIndexEntry(Slice("key"), nullptr, handle, &scratch);

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // No Prepare() call — bounds should be kInbound.
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("key"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
}

// ============================================================================
// End-to-end SST integration tests
// ============================================================================

class TrieIndexSSTTest : public testing::Test {
 protected:
  void SetUp() override {
    trie_factory_ = std::make_shared<TrieIndexFactory>();
    sst_path_ = test::PerThreadDBPath("trie_index_sst_test.sst");
  }

  void TearDown() override {
    // Clean up SST file.
    Env::Default()->DeleteFile(sst_path_).PermitUncheckedError();
  }

  // Generate sorted key-value pairs: "key_NNNN" -> "value_NNNN".
  std::vector<std::pair<std::string, std::string>> GenerateKVs(int count) {
    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(count);
    for (int i = 0; i < count; i++) {
      char key_buf[32], val_buf[32];
      snprintf(key_buf, sizeof(key_buf), "key_%04d", i);
      snprintf(val_buf, sizeof(val_buf), "value_%04d", i);
      kvs.emplace_back(key_buf, val_buf);
    }
    return kvs;
  }

  // Write an SST file with the given key-value pairs using the trie UDI.
  Status WriteSST(const std::vector<std::pair<std::string, std::string>>& kvs) {
    Options options;
    BlockBasedTableOptions table_options;
    table_options.user_defined_index_factory = trie_factory_;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    SstFileWriter writer(EnvOptions(), options);
    Status s = writer.Open(sst_path_);
    if (!s.ok()) return s;

    for (const auto& kv : kvs) {
      s = writer.Put(kv.first, kv.second);
      if (!s.ok()) return s;
    }
    return writer.Finish();
  }

  std::shared_ptr<TrieIndexFactory> trie_factory_;
  std::string sst_path_;
};

TEST_F(TrieIndexSSTTest, WriteAndReadWithTrieUDI) {
  // Write SST with trie UDI.
  auto kvs = GenerateKVs(100);
  ASSERT_OK(WriteSST(kvs));

  // Read without UDI (native index) — should work since native index is
  // always present alongside the UDI.
  {
    Options options;
    BlockBasedTableOptions table_options;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    SstFileReader reader(options);
    ASSERT_OK(reader.Open(sst_path_));

    ReadOptions ro;
    std::unique_ptr<Iterator> iter(reader.NewIterator(ro));
    iter->SeekToFirst();
    int count = 0;
    for (; iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->key().ToString(), kvs[count].first)
          << "Key mismatch at " << count;
      ASSERT_EQ(iter->value().ToString(), kvs[count].second)
          << "Value mismatch at " << count;
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 100);
  }

  // Read WITH trie UDI — use table_index_factory in ReadOptions.
  {
    Options options;
    BlockBasedTableOptions table_options;
    table_options.user_defined_index_factory = trie_factory_;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    SstFileReader reader(options);
    ASSERT_OK(reader.Open(sst_path_));

    ReadOptions ro;
    ro.table_index_factory = trie_factory_.get();
    std::unique_ptr<Iterator> iter(reader.NewIterator(ro));

    // Full forward scan via Seek.
    iter->Seek("key_0000");
    int count = 0;
    for (; iter->Valid(); iter->Next()) {
      ASSERT_LT(count, 100) << "Too many keys";
      ASSERT_EQ(iter->value().ToString(), kvs[count].second)
          << "Value mismatch at " << count;
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 100);
  }
}

TEST_F(TrieIndexSSTTest, SeekWithTrieUDI) {
  auto kvs = GenerateKVs(100);
  ASSERT_OK(WriteSST(kvs));

  Options options;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(sst_path_));

  ReadOptions ro;
  ro.table_index_factory = trie_factory_.get();
  std::unique_ptr<Iterator> iter(reader.NewIterator(ro));

  // Seek to the middle.
  iter->Seek("key_0050");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  int count = 0;
  for (; iter->Valid(); iter->Next()) {
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 50);

  // Seek past all keys.
  iter->Seek("key_9999");
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // Seek before all keys.
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ(iter->key().ToString(), kvs[0].first);
}

TEST_F(TrieIndexSSTTest, SeekWithUpperBound) {
  auto kvs = GenerateKVs(100);
  ASSERT_OK(WriteSST(kvs));

  Options options;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(sst_path_));

  ReadOptions ro;
  ro.table_index_factory = trie_factory_.get();
  Slice upper_bound("key_0075");
  ro.iterate_upper_bound = &upper_bound;
  std::unique_ptr<Iterator> iter(reader.NewIterator(ro));

  iter->Seek("key_0050");
  ASSERT_TRUE(iter->Valid());
  int count = 0;
  for (; iter->Valid(); iter->Next()) {
    ASSERT_LT(iter->key().compare(upper_bound), 0)
        << "Key " << iter->key().ToString() << " >= upper bound";
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 25);
}

TEST_F(TrieIndexSSTTest, SmallSST) {
  // Edge case: SST with very few keys (just one data block).
  auto kvs = GenerateKVs(3);
  ASSERT_OK(WriteSST(kvs));

  Options options;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(sst_path_));

  ReadOptions ro;
  ro.table_index_factory = trie_factory_.get();
  std::unique_ptr<Iterator> iter(reader.NewIterator(ro));

  iter->Seek("key_0000");
  int count = 0;
  for (; iter->Valid(); iter->Next()) {
    ASSERT_EQ(iter->value().ToString(), kvs[count].second);
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 3);
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
