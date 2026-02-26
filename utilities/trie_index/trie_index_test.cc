//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cmath>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"
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
  ASSERT_EQ(builder.NumBits(), 0u);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 0u);
  ASSERT_EQ(bv.NumOnes(), 0u);
  ASSERT_EQ(bv.NumZeros(), 0u);
}

TEST_F(BitvectorTest, SingleBit) {
  // Single 1-bit.
  {
    BitvectorBuilder builder;
    builder.Append(true);
    Bitvector bv;
    bv.BuildFrom(builder);
    ASSERT_EQ(bv.NumBits(), 1u);
    ASSERT_EQ(bv.NumOnes(), 1u);
    ASSERT_TRUE(bv.GetBit(0));
    ASSERT_EQ(bv.Rank1(0), 0u);
    ASSERT_EQ(bv.Rank1(1), 1u);
    ASSERT_EQ(bv.FindNthOneBit(0), 0u);
    ASSERT_EQ(bv.FindNthZeroBit(0), 1u);  // No 0-bits; returns num_bits_.
  }
  // Single 0-bit.
  {
    BitvectorBuilder builder;
    builder.Append(false);
    Bitvector bv;
    bv.BuildFrom(builder);
    ASSERT_EQ(bv.NumBits(), 1u);
    ASSERT_EQ(bv.NumOnes(), 0u);
    ASSERT_FALSE(bv.GetBit(0));
    ASSERT_EQ(bv.Rank1(0), 0u);
    ASSERT_EQ(bv.Rank1(1), 0u);
    ASSERT_EQ(bv.Rank0(1), 1u);
    ASSERT_EQ(bv.FindNthZeroBit(0), 0u);
    ASSERT_EQ(bv.FindNthOneBit(0), 1u);  // No 1-bits; returns num_bits_.
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
    ASSERT_EQ(bv.FindNthOneBit(i), i * 2) << "FindNthOneBit failed at i=" << i;
    ASSERT_EQ(bv.FindNthZeroBit(i), i * 2 + 1)
        << "FindNthZeroBit failed at i=" << i;
  }

  // Out of range selects.
  ASSERT_EQ(bv.FindNthOneBit(n / 2), n);
  ASSERT_EQ(bv.FindNthZeroBit(n / 2), n);
}

TEST_F(BitvectorTest, AllOnes) {
  const uint64_t n = 512;  // Exactly two rank sample intervals (256 each).
  BitvectorBuilder builder;
  builder.AppendMultiple(true, n);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), n);

  for (uint64_t pos = 0; pos <= n; pos++) {
    ASSERT_EQ(bv.Rank1(pos), pos);
    ASSERT_EQ(bv.Rank0(pos), 0u);
  }
  for (uint64_t i = 0; i < n; i++) {
    ASSERT_EQ(bv.FindNthOneBit(i), i);
  }
  ASSERT_EQ(bv.FindNthZeroBit(0), n);  // No 0-bits.
}

TEST_F(BitvectorTest, AllZeros) {
  const uint64_t n = 512;
  BitvectorBuilder builder;
  builder.AppendMultiple(false, n);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), 0u);

  for (uint64_t pos = 0; pos <= n; pos++) {
    ASSERT_EQ(bv.Rank1(pos), 0u);
    ASSERT_EQ(bv.Rank0(pos), pos);
  }
  for (uint64_t i = 0; i < n; i++) {
    ASSERT_EQ(bv.FindNthZeroBit(i), i);
  }
  ASSERT_EQ(bv.FindNthOneBit(0), n);  // No 1-bits.
}

TEST_F(BitvectorTest, LargeBitvector) {
  // Test with a bitvector larger than one rank sample (kBitsPerRankSample).
  const uint64_t n = 2000;
  BitvectorBuilder builder;
  uint64_t expected_ones = 0;
  for (uint64_t i = 0; i < n; i++) {
    bool bit = (i % 3 == 0);
    builder.Append(bit);
    if (bit) {
      expected_ones++;
    }
  }

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), expected_ones);

  // Verify rank at boundaries.
  ASSERT_EQ(bv.Rank1(0), 0u);
  ASSERT_EQ(bv.Rank1(n), expected_ones);

  // Verify select for a subset of positions.
  uint64_t ones_so_far = 0;
  for (uint64_t i = 0; i < n; i++) {
    if (bv.GetBit(i)) {
      ASSERT_EQ(bv.FindNthOneBit(ones_so_far), i)
          << "FindNthOneBit failed at ones_so_far=" << ones_so_far;
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
  original.EncodeTo(&serialized);
  ASSERT_GT(serialized.size(), 0u);

  // Deserialize.
  Bitvector deserialized;
  size_t consumed = 0;
  ASSERT_OK(deserialized.InitFromData(serialized.data(), serialized.size(),
                                      &consumed));
  ASSERT_GT(consumed, 0u);
  ASSERT_EQ(consumed, serialized.size());

  // Verify match.
  ASSERT_EQ(deserialized.NumBits(), original.NumBits());
  ASSERT_EQ(deserialized.NumOnes(), original.NumOnes());

  for (uint64_t pos = 0; pos <= n; pos++) {
    ASSERT_EQ(deserialized.Rank1(pos), original.Rank1(pos))
        << "Rank1 mismatch at pos=" << pos;
  }

  for (uint64_t i = 0; i < original.NumOnes(); i++) {
    ASSERT_EQ(deserialized.FindNthOneBit(i), original.FindNthOneBit(i))
        << "FindNthOneBit mismatch at i=" << i;
  }
}

TEST_F(BitvectorTest, SerializeRoundTripNonAligned) {
  // Test with a size that's not a multiple of 64.
  const uint64_t n = 100;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 7 == 0);
  }

  Bitvector original;
  original.BuildFrom(builder);

  std::string serialized;
  original.EncodeTo(&serialized);

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

  ASSERT_EQ(builder.NumBits(), 256u);
  ASSERT_TRUE(builder.GetBit(0));  // label 0x00
  ASSERT_TRUE(builder.GetBit(3));  // label 0x03
  ASSERT_FALSE(builder.GetBit(1));
  ASSERT_FALSE(builder.GetBit(64));
  ASSERT_TRUE(builder.GetBit(255));  // label 0xFF

  // Build and verify rank/select work correctly.
  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 256u);
  ASSERT_EQ(bv.NumOnes(), 3u);  // bits 0, 3, 255
  ASSERT_EQ(bv.FindNthOneBit(0), 0u);
  ASSERT_EQ(bv.FindNthOneBit(1), 3u);
  ASSERT_EQ(bv.FindNthOneBit(2), 255u);
}

TEST_F(BitvectorTest, AppendWordAndAppendInterleaved) {
  // Verify AppendWord works when mixed with regular Append calls,
  // as long as AppendWord is called on word-aligned boundaries.
  BitvectorBuilder builder;

  // Append 64 bits via AppendWord.
  builder.AppendWord(0xAAAAAAAA'AAAAAAAAULL, 64);
  ASSERT_EQ(builder.NumBits(), 64u);

  // Append 64 more bits one at a time.
  for (int i = 0; i < 64; i++) {
    builder.Append(i % 2 == 0);
  }
  ASSERT_EQ(builder.NumBits(), 128u);

  // Another word-aligned AppendWord.
  builder.AppendWord(0xFFFFFFFF'FFFFFFFFULL, 64);
  ASSERT_EQ(builder.NumBits(), 192u);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 192u);

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
  ASSERT_EQ(bv.NumBits(), 129u);

  // NextSetBit from various positions.
  ASSERT_EQ(bv.NextSetBit(0), 64u);     // First set bit.
  ASSERT_EQ(bv.NextSetBit(64), 64u);    // Exact position.
  ASSERT_EQ(bv.NextSetBit(65), 128u);   // Next one.
  ASSERT_EQ(bv.NextSetBit(128), 128u);  // Exact.
  ASSERT_EQ(bv.NextSetBit(129), 129u);  // Past end → num_bits_.
}

TEST_F(BitvectorTest, NextSetBitAllZeros) {
  BitvectorBuilder builder;
  builder.AppendMultiple(false, 200);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NextSetBit(0), 200u);
  ASSERT_EQ(bv.NextSetBit(100), 200u);
  ASSERT_EQ(bv.NextSetBit(199), 200u);
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

  ASSERT_EQ(bv.DistanceToNextSetBit(0), 3u);  // 0 → 3
  ASSERT_EQ(bv.DistanceToNextSetBit(3), 2u);  // 3 → 5
  // Last set bit: distance to next = num_bits_ - pos.
  ASSERT_EQ(bv.DistanceToNextSetBit(5), 1u);  // 5 → 6 (num_bits_)
}

TEST_F(BitvectorTest, MultipleRankSampleBoundaries) {
  // Test rank/select across multiple rank sample boundaries.
  const uint64_t n = 2048;  // 8 rank samples (256 bits each).
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    // Set bit at every 100th position.
    builder.Append(i % 100 == 0);
  }

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumOnes(), 21u);  // 0, 100, 200, ..., 2000

  // Verify rank at sample boundaries.
  ASSERT_EQ(bv.Rank1(512), 6u);    // 0,100,200,300,400,500
  ASSERT_EQ(bv.Rank1(1024), 11u);  // +600,700,800,900,1000
  ASSERT_EQ(bv.Rank1(1536), 16u);  // +1100,1200,1300,1400,1500

  // Verify select.
  for (uint64_t i = 0; i < bv.NumOnes(); i++) {
    ASSERT_EQ(bv.FindNthOneBit(i), i * 100);
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

TEST_F(BitvectorTest, FindNthSetBitInWordExhaustive) {
  // Verify FindNthSetBitInWord for every position in a word with known
  // popcount.
  uint64_t word = 0xDEADBEEFCAFEBABEULL;
  uint64_t pc = Popcount(word);

  for (uint64_t i = 0; i < pc; i++) {
    uint64_t pos = FindNthSetBitInWord(word, i);
    // The pos-th bit should be set.
    ASSERT_TRUE((word >> pos) & 1)
        << "FindNthSetBitInWord returned non-set bit at i=" << i;
    // Exactly i bits should be set before pos.
    uint64_t mask =
        (pos == 0) ? 0 : (pos == 64 ? ~uint64_t(0) : (uint64_t(1) << pos) - 1);
    ASSERT_EQ(Popcount(word & mask), i)
        << "Wrong rank at FindNthSetBitInWord result for i=" << i;
  }
}

TEST_F(BitvectorTest, MoveConstructor) {
  // Build a non-trivial bitvector and verify move constructor preserves
  // correctness, especially the RecomputePointers() logic.
  const uint64_t n = 1000;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 3 == 0);
  }

  Bitvector original;
  original.BuildFrom(builder);
  uint64_t orig_ones = original.NumOnes();
  uint64_t orig_rank_500 = original.Rank1(500);
  uint64_t orig_select_10 = original.FindNthOneBit(10);

  // Move-construct a new bitvector.
  Bitvector moved(std::move(original));

  // Moved-to should preserve all data.
  ASSERT_EQ(moved.NumBits(), n);
  ASSERT_EQ(moved.NumOnes(), orig_ones);
  ASSERT_EQ(moved.Rank1(500), orig_rank_500);
  ASSERT_EQ(moved.FindNthOneBit(10), orig_select_10);

  // Verify full rank/select consistency.
  for (uint64_t i = 0; i < moved.NumOnes(); i++) {
    uint64_t pos = moved.FindNthOneBit(i);
    ASSERT_TRUE(moved.GetBit(pos))
        << "Bit not set at FindNthOneBit(" << i << ")";
  }

  // Moved-from should be empty/zeroed.
  // NOLINTBEGIN(bugprone-use-after-move)
  ASSERT_EQ(original.NumBits(), 0u);
  ASSERT_EQ(original.NumOnes(), 0u);
  // NOLINTEND(bugprone-use-after-move)
}

TEST_F(BitvectorTest, MoveAssignment) {
  // Build two bitvectors, move-assign the first to the second.
  const uint64_t n = 500;
  BitvectorBuilder builder1;
  for (uint64_t i = 0; i < n; i++) {
    builder1.Append(i % 5 == 0);
  }
  Bitvector bv1;
  bv1.BuildFrom(builder1);

  BitvectorBuilder builder2;
  for (uint64_t i = 0; i < 200; i++) {
    builder2.Append(true);
  }
  Bitvector bv2;
  bv2.BuildFrom(builder2);

  uint64_t bv1_ones = bv1.NumOnes();
  uint64_t bv1_rank_250 = bv1.Rank1(250);

  // Move-assign bv1 to bv2.
  bv2 = std::move(bv1);

  // bv2 should now have bv1's data.
  ASSERT_EQ(bv2.NumBits(), n);
  ASSERT_EQ(bv2.NumOnes(), bv1_ones);
  ASSERT_EQ(bv2.Rank1(250), bv1_rank_250);

  // Moved-from should be empty.
  ASSERT_EQ(bv1.NumBits(), 0u);  // NOLINT(bugprone-use-after-move)
}

TEST_F(BitvectorTest, SerializedSizeMatchesEncoded) {
  // Verify SerializedSize() matches actual encoded output length.
  const uint64_t n = 1000;
  BitvectorBuilder builder;
  for (uint64_t i = 0; i < n; i++) {
    builder.Append(i % 7 == 0);
  }
  Bitvector bv;
  bv.BuildFrom(builder);

  std::string encoded;
  bv.EncodeTo(&encoded);
  ASSERT_EQ(bv.SerializedSize(), encoded.size());
}

TEST_F(BitvectorTest, DistanceToNextSetBitCrossWordBoundary) {
  // Test DistanceToNextSetBit where the next set bit is in a different
  // 64-bit word. Build: bit 0 set, then 127 zeros, then bit 128 set.
  BitvectorBuilder builder;
  builder.Append(true);  // pos 0
  builder.AppendMultiple(false, 127);
  builder.Append(true);  // pos 128
  builder.Append(false);

  Bitvector bv;
  bv.BuildFrom(builder);

  // Distance from pos 0 to next set bit (pos 128) = 128.
  ASSERT_EQ(bv.DistanceToNextSetBit(0), 128u);
  // Distance from pos 128 is to end (no more set bits after).
  // DistanceToNextSetBit returns distance to next set bit after pos,
  // where pos itself must be a set bit.
}

TEST_F(BitvectorTest, NumZeros) {
  // Verify NumZeros for various patterns.
  const uint64_t n = 300;
  BitvectorBuilder builder;
  uint64_t expected_ones = 0;
  for (uint64_t i = 0; i < n; i++) {
    bool bit = (i % 4 == 0);
    builder.Append(bit);
    if (bit) {
      expected_ones++;
    }
  }
  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), n);
  ASSERT_EQ(bv.NumOnes(), expected_ones);
  ASSERT_EQ(bv.NumZeros(), n - expected_ones);
}

TEST_F(BitvectorTest, InitFromDataNumOnesExceedsNumBits) {
  // Craft a header where num_ones > num_bits — should return Corruption.
  std::string buf(2 * sizeof(uint64_t), '\0');
  uint64_t num_bits = 64;
  uint64_t num_ones = 65;  // Invalid: more ones than bits.
  memcpy(&buf[0], &num_bits, sizeof(uint64_t));
  memcpy(&buf[sizeof(uint64_t)], &num_ones, sizeof(uint64_t));

  Bitvector bv;
  size_t consumed = 0;
  Status s = bv.InitFromData(buf.data(), buf.size(), &consumed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, InitFromDataNumBitsExceedsUint32) {
  // Craft a header where num_bits > UINT32_MAX — should return Corruption.
  std::string buf(2 * sizeof(uint64_t), '\0');
  uint64_t num_bits = static_cast<uint64_t>(UINT32_MAX) + 1;
  uint64_t num_ones = 0;
  memcpy(&buf[0], &num_bits, sizeof(uint64_t));
  memcpy(&buf[sizeof(uint64_t)], &num_ones, sizeof(uint64_t));

  Bitvector bv;
  size_t consumed = 0;
  Status s = bv.InitFromData(buf.data(), buf.size(), &consumed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, InitFromDataWordsNotAligned) {
  // Build a valid bitvector, serialize it, then read from a buffer where the
  // words data starts at an odd offset (not 8-byte aligned).
  BitvectorBuilder builder;
  for (int i = 0; i < 128; i++) {
    builder.Append(i % 3 == 0);
  }
  Bitvector bv_orig;
  bv_orig.BuildFrom(builder);

  std::string serialized;
  bv_orig.EncodeTo(&serialized);

  // Allocate buffer with 1-byte extra and offset by 1 to break alignment.
  std::unique_ptr<char[]> unaligned_buf(new char[serialized.size() + 1]);
  memcpy(unaligned_buf.get() + 1, serialized.data(), serialized.size());

  Bitvector bv;
  size_t consumed = 0;
  Status s =
      bv.InitFromData(unaligned_buf.get() + 1, serialized.size(), &consumed);
  // The header is 16 bytes; words start right after. If the base pointer is
  // offset by 1, then the words pointer is at (base+1+16) which is odd.
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, InitFromDataTruncatedRankLUT) {
  // Build a valid bitvector, serialize, then truncate after the words section
  // so the rank LUT cannot be read.
  BitvectorBuilder builder;
  for (int i = 0; i < 512; i++) {
    builder.Append(i % 5 == 0);
  }
  Bitvector bv_orig;
  bv_orig.BuildFrom(builder);

  std::string serialized;
  bv_orig.EncodeTo(&serialized);

  // Header = 16 bytes, words = ceil(512/64)*8 = 64 bytes. Truncate after that.
  size_t truncated_size = 2 * sizeof(uint64_t) + ((512 + 63) / 64) * 8;
  ASSERT_LT(truncated_size, serialized.size());

  Bitvector bv;
  size_t consumed = 0;
  Status s = bv.InitFromData(serialized.data(), truncated_size, &consumed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, InitFromDataTruncatedSelect1Hints) {
  // Build a bitvector with enough ones to have select1 hints, then truncate
  // after the rank LUT so select1 hints cannot be read.
  BitvectorBuilder builder;
  // 600 bits, all ones → lots of select1 hints.
  for (int i = 0; i < 600; i++) {
    builder.Append(true);
  }
  Bitvector bv_orig;
  bv_orig.BuildFrom(builder);

  std::string serialized;
  bv_orig.EncodeTo(&serialized);

  // We need to find a truncation point after the rank LUT but before select1
  // hints finish. Use a binary approach: try reading at decreasing sizes.
  // A valid read needs the full buffer. Remove the last 8 bytes (at minimum).
  size_t truncated_size = serialized.size() - 8;

  Bitvector bv;
  size_t consumed = 0;
  Status s = bv.InitFromData(serialized.data(), truncated_size, &consumed);
  // Should fail for either select1 or select0 hints truncation.
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, InitFromDataTruncatedSelect0Hints) {
  // Build a bitvector with enough zeros to have select0 hints, then truncate
  // so select0 hints cannot be fully read.
  BitvectorBuilder builder;
  // 600 bits, all zeros → lots of select0 hints, zero select1 hints.
  for (int i = 0; i < 600; i++) {
    builder.Append(false);
  }
  Bitvector bv_orig;
  bv_orig.BuildFrom(builder);

  std::string serialized;
  bv_orig.EncodeTo(&serialized);

  // Remove the last 8 bytes.
  size_t truncated_size = serialized.size() - 8;

  Bitvector bv;
  size_t consumed = 0;
  Status s = bv.InitFromData(serialized.data(), truncated_size, &consumed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(BitvectorTest, BuilderReserve) {
  // Verify Reserve() doesn't change behavior, just avoids reallocation.
  BitvectorBuilder builder;
  builder.Reserve(1000);
  for (int i = 0; i < 1000; i++) {
    builder.Append(i % 2 == 0);
  }
  ASSERT_EQ(builder.NumBits(), 1000u);

  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 1000u);
  ASSERT_EQ(bv.NumOnes(), 500u);
  // Verify correctness of rank/select after Reserve.
  ASSERT_EQ(bv.FindNthOneBit(0), 0u);
  ASSERT_EQ(bv.FindNthOneBit(1), 2u);
  ASSERT_EQ(bv.FindNthZeroBit(0), 1u);
}

TEST_F(BitvectorTest, AppendMultipleZeroCount) {
  // AppendMultiple with count=0 should be a no-op.
  BitvectorBuilder builder;
  builder.Append(true);
  builder.AppendMultiple(true, 0);
  builder.AppendMultiple(false, 0);
  ASSERT_EQ(builder.NumBits(), 1u);
  ASSERT_TRUE(builder.GetBit(0));
}

TEST_F(BitvectorTest, AppendMultipleTruePartialWord) {
  // AppendMultiple(true, count) starting at a non-word-aligned position.
  // This exercises the partial-word fill path at the beginning.
  BitvectorBuilder builder;
  // Start with 7 false bits to offset by 7 within the first word.
  builder.AppendMultiple(false, 7);
  // Now append 200 true bits starting at bit position 7.
  builder.AppendMultiple(true, 200);
  // Then 5 more false bits.
  builder.AppendMultiple(false, 5);

  ASSERT_EQ(builder.NumBits(), 212u);
  // Verify the pattern: 7 zeros, 200 ones, 5 zeros.
  for (uint64_t i = 0; i < 7; i++) {
    ASSERT_FALSE(builder.GetBit(i)) << "Expected 0 at " << i;
  }
  for (uint64_t i = 7; i < 207; i++) {
    ASSERT_TRUE(builder.GetBit(i)) << "Expected 1 at " << i;
  }
  for (uint64_t i = 207; i < 212; i++) {
    ASSERT_FALSE(builder.GetBit(i)) << "Expected 0 at " << i;
  }

  // Build the bitvector and verify rank/select correctness.
  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumOnes(), 200u);
  ASSERT_EQ(bv.FindNthOneBit(0), 7u);
  ASSERT_EQ(bv.FindNthOneBit(199), 206u);
  ASSERT_EQ(bv.FindNthZeroBit(0), 0u);
  ASSERT_EQ(bv.FindNthZeroBit(7), 207u);
}

TEST_F(BitvectorTest, FindNthZeroBitNonAligned) {
  // Build a bitvector whose length is not a multiple of 64, then verify
  // FindNthZeroBit handles the last partial word correctly.
  BitvectorBuilder builder;
  // 100 bits: alternating 1, 0.
  for (int i = 0; i < 100; i++) {
    builder.Append(i % 2 == 0);
  }
  // 100 bits, 50 ones (at even positions), 50 zeros (at odd positions).
  Bitvector bv;
  bv.BuildFrom(builder);
  ASSERT_EQ(bv.NumBits(), 100u);
  ASSERT_EQ(bv.NumOnes(), 50u);

  // The zeros are at positions 1, 3, 5, ..., 99.
  for (uint64_t i = 0; i < 50; i++) {
    ASSERT_EQ(bv.FindNthZeroBit(i), 2 * i + 1) << "Mismatch at i=" << i;
  }
  // Asking for the 50th zero-bit (0-indexed) should return num_bits_ (100).
  ASSERT_EQ(bv.FindNthZeroBit(50), 100u);
}

// ============================================================================
// EliasFano tests
// ============================================================================

class EliasFanoTest : public testing::Test {};

TEST_F(EliasFanoTest, EmptySequence) {
  EliasFano ef;
  std::vector<uint64_t> values;
  ef.BuildFrom(values.data(), 0, 0);
  ASSERT_EQ(ef.Count(), 0u);
  ASSERT_EQ(ef.Universe(), 0u);
}

TEST_F(EliasFanoTest, SingleElement) {
  EliasFano ef;
  uint64_t val = 42;
  ef.BuildFrom(&val, 1, 100);
  ASSERT_EQ(ef.Count(), 1u);
  ASSERT_EQ(ef.Universe(), 100u);
  ASSERT_EQ(ef.Access(0), 42u);
}

TEST_F(EliasFanoTest, MonotonicSequence) {
  // Test a typical monotonically increasing sequence.
  std::vector<uint64_t> values = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), 101);
  ASSERT_EQ(ef.Count(), values.size());
  ASSERT_EQ(ef.Universe(), 101u);
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_EQ(ef.Access(i), values[i]) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, ConstantSequence) {
  // All values the same (universe = count, low_bits = 0).
  std::vector<uint64_t> values(50, 7);
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), 8);
  ASSERT_EQ(ef.Count(), 50u);
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_EQ(ef.Access(i), 7u) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, LargeUniverse) {
  // Simulates block offsets in a large SST file (4GB range).
  const uint64_t universe = uint64_t(4) * 1024 * 1024 * 1024;  // 4GB
  const size_t count = 1000;
  std::vector<uint64_t> values(count);
  for (size_t i = 0; i < count; i++) {
    values[i] = i * (universe / count);
  }

  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), universe);
  ASSERT_EQ(ef.Count(), count);
  ASSERT_EQ(ef.Universe(), universe);
  for (size_t i = 0; i < count; i++) {
    ASSERT_EQ(ef.Access(i), values[i]) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, ConsecutiveValues) {
  // Dense sequence: 0, 1, 2, ..., 99.
  std::vector<uint64_t> values(100);
  for (size_t i = 0; i < 100; i++) {
    values[i] = i;
  }
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), 100);
  for (size_t i = 0; i < 100; i++) {
    ASSERT_EQ(ef.Access(i), i) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, WordBoundaryCrossing) {
  // Craft values where low bits span two 64-bit words during packing.
  // Use low_bits ~= 17 (universe/count ~= 131072), values near boundaries.
  const size_t count = 100;
  const uint64_t universe = count * 131072;
  std::vector<uint64_t> values(count);
  for (size_t i = 0; i < count; i++) {
    values[i] = i * 131072 + (i % 7);  // Small offset to exercise low bits.
  }
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), universe);
  for (size_t i = 0; i < count; i++) {
    ASSERT_EQ(ef.Access(i), values[i]) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, SerializeDeserializeRoundTrip) {
  // Build, serialize, deserialize, verify all values match.
  std::vector<uint64_t> values = {0, 5, 100, 1000, 5000, 10000, 50000};
  EliasFano original;
  original.BuildFrom(values.data(), values.size(), 100000);

  std::string serialized;
  original.EncodeTo(&serialized);
  ASSERT_GT(serialized.size(), 0u);
  ASSERT_EQ(original.SerializedSize(), serialized.size());

  EliasFano deserialized;
  size_t consumed = 0;
  ASSERT_OK(deserialized.InitFromData(serialized.data(), serialized.size(),
                                      &consumed));
  ASSERT_EQ(consumed, serialized.size());
  ASSERT_EQ(deserialized.Count(), original.Count());
  ASSERT_EQ(deserialized.Universe(), original.Universe());

  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_EQ(deserialized.Access(i), values[i]) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, SerializeDeserializeEmpty) {
  EliasFano original;
  std::vector<uint64_t> empty;
  original.BuildFrom(empty.data(), 0, 0);

  std::string serialized;
  original.EncodeTo(&serialized);

  EliasFano deserialized;
  size_t consumed = 0;
  ASSERT_OK(deserialized.InitFromData(serialized.data(), serialized.size(),
                                      &consumed));
  ASSERT_EQ(deserialized.Count(), 0u);
}

TEST_F(EliasFanoTest, InitFromDataCorruptionTruncatedHeader) {
  std::string bad_data(10, '\0');  // Too short for 3 uint64_t header.
  EliasFano ef;
  size_t consumed = 0;
  ASSERT_TRUE(ef.InitFromData(bad_data.data(), bad_data.size(), &consumed)
                  .IsCorruption());
}

TEST_F(EliasFanoTest, InitFromDataCorruptionBadLowBits) {
  // Build a valid EliasFano, then corrupt the low_bits field to > 63.
  std::vector<uint64_t> values = {0, 10, 20};
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), 100);

  std::string serialized;
  ef.EncodeTo(&serialized);

  // low_bits is the third uint64_t (bytes 16..23). Set to 64.
  uint64_t bad_low_bits = 64;
  memcpy(&serialized[16], &bad_low_bits, sizeof(uint64_t));

  EliasFano corrupt;
  size_t consumed = 0;
  ASSERT_TRUE(
      corrupt.InitFromData(serialized.data(), serialized.size(), &consumed)
          .IsCorruption());
}

TEST_F(EliasFanoTest, InitFromDataCorruptionTruncatedLowWords) {
  // Build valid, then truncate so low words are incomplete.
  std::vector<uint64_t> values = {0, 100, 200, 300, 400};
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), 1000);

  std::string serialized;
  ef.EncodeTo(&serialized);

  // Truncate a few bytes off the end.
  std::string truncated = serialized.substr(0, serialized.size() - 4);
  EliasFano corrupt;
  size_t consumed = 0;
  ASSERT_TRUE(
      corrupt.InitFromData(truncated.data(), truncated.size(), &consumed)
          .IsCorruption());
}

TEST_F(EliasFanoTest, LargeCount) {
  // Test with 10K elements to exercise multi-word packing.
  const size_t count = 10000;
  const uint64_t universe = count * 1000;
  std::vector<uint64_t> values(count);
  for (size_t i = 0; i < count; i++) {
    values[i] = i * 1000;
  }
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), universe);
  ASSERT_EQ(ef.Count(), count);

  // Spot-check values.
  ASSERT_EQ(ef.Access(0), 0u);
  ASSERT_EQ(ef.Access(count / 2), (count / 2) * 1000);
  ASSERT_EQ(ef.Access(count - 1), (count - 1) * 1000);

  // Full verification.
  for (size_t i = 0; i < count; i++) {
    ASSERT_EQ(ef.Access(i), values[i]) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, InitFromDataTruncatedHighBitvector) {
  // Build a valid EliasFano, serialize it, then truncate so the high bitvector
  // cannot be fully read.
  std::vector<uint64_t> values = {10, 20, 30, 40, 50};
  EliasFano ef;
  ef.BuildFrom(values.data(), values.size(), 100);

  std::string serialized;
  ef.EncodeTo(&serialized);

  // Header is 3 * 8 = 24 bytes. Truncate just a few bytes past the header
  // so the high bitvector InitFromData fails.
  size_t truncated_size = 3 * sizeof(uint64_t) + 4;
  ASSERT_LT(truncated_size, serialized.size());

  EliasFano ef2;
  size_t consumed = 0;
  Status s = ef2.InitFromData(serialized.data(), truncated_size, &consumed);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(EliasFanoTest, MoveConstructor) {
  // Verify that move-constructing an EliasFano preserves correctness.
  // Use the InitFromData path (production path) since the default move ctor
  // doesn't re-seat low_words_ into owned_low_data_. When initialized from
  // serialized data, low_words_ points into external memory (not
  // owned_low_data_), so the default move is safe.
  std::vector<uint64_t> values = {0, 100, 200, 300, 400};
  EliasFano ef_tmp;
  ef_tmp.BuildFrom(values.data(), values.size(), 500);

  std::string serialized;
  ef_tmp.EncodeTo(&serialized);

  EliasFano ef1;
  size_t consumed = 0;
  ASSERT_OK(ef1.InitFromData(serialized.data(), serialized.size(), &consumed));

  // Move-construct ef2 from ef1.
  EliasFano ef2(std::move(ef1));

  ASSERT_EQ(ef2.Count(), 5u);
  ASSERT_EQ(ef2.Universe(), 500u);
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_EQ(ef2.Access(i), values[i]) << "Mismatch at i=" << i;
  }
}

TEST_F(EliasFanoTest, MoveAssignment) {
  // Verify that move-assigning an EliasFano preserves correctness.
  // Use the InitFromData path (see MoveConstructor comment for rationale).
  std::vector<uint64_t> values = {5, 15, 25, 35, 45};
  EliasFano ef_tmp;
  ef_tmp.BuildFrom(values.data(), values.size(), 50);

  std::string serialized;
  ef_tmp.EncodeTo(&serialized);

  EliasFano ef1;
  size_t consumed = 0;
  ASSERT_OK(ef1.InitFromData(serialized.data(), serialized.size(), &consumed));

  // Move-assign ef1 into a fresh ef2.
  EliasFano ef2;
  ef2 = std::move(ef1);

  ASSERT_EQ(ef2.Count(), 5u);
  ASSERT_EQ(ef2.Universe(), 50u);
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_EQ(ef2.Access(i), values[i]) << "Mismatch at i=" << i;
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
  ASSERT_GT(data.size(), 0u);

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), 0u);
}

TEST_F(LoudsTrieTest, SingleKey) {
  LoudsTrieBuilder builder;
  TrieBlockHandle h{100, 200};
  builder.AddKey(Slice("hello"), h);
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), 1u);

  // Verify handle.
  auto handle = trie.GetHandle(0);
  ASSERT_EQ(handle.offset, 100u);
  ASSERT_EQ(handle.size, 200u);
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
    ASSERT_EQ(handle.size, 50u);
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
  keys.emplace_back("\x00\x00", 2);
  keys.emplace_back("\x00\x01", 2);
  keys.emplace_back("\x00\xFF", 2);
  keys.emplace_back("\x01\x00", 2);
  keys.emplace_back("\xFF\x00", 2);
  keys.emplace_back("\xFF\xFF", 2);
  std::sort(keys.begin(), keys.end());
  VerifyTrieIteration(keys);
}

TEST_F(LoudsTrieTest, BuilderSingleByteAllValues) {
  // One key for every possible single byte value (0x00–0xFF).
  // This exercises the maximum fanout at root (256 children).
  std::vector<std::string> keys;
  keys.reserve(256);
  for (int b = 0; b < 256; b++) {
    keys.emplace_back(1, static_cast<char>(b));
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
    keys.emplace_back(1, c);
  }
  // Add "aa" and "aab" so "a" becomes a prefix key.
  keys.emplace_back("aa");
  keys.emplace_back("aab");
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

// --- Block handle encoding ---

TEST_F(LoudsTrieTest, HandleRoundTrip) {
  // Verify that block handles round-trip correctly through serialization.
  // Handles are stored as fixed-width uint64 arrays for zero-copy loading.
  LoudsTrieBuilder builder;

  // Simulate realistic SST block handles: sequential offsets with ~4KB blocks.
  const uint64_t kBlockSize = 4096;
  const int kNumBlocks = 100;
  std::vector<std::string> keys;
  std::vector<TrieBlockHandle> expected_handles;

  for (int i = 0; i < kNumBlocks; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%04d", i);
    keys.emplace_back(buf);
    TrieBlockHandle h{static_cast<uint64_t>(i) * kBlockSize, kBlockSize};
    expected_handles.emplace_back(h);
    builder.AddKey(Slice(keys.back()), h);
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  // Handles are stored as two fixed-width uint64 arrays (offsets + sizes),
  // totaling 16 bytes per handle (100 * 16 = 1600 bytes for handles).
  // This trades space for O(1) zero-copy loading.
  size_t fixed_handle_bytes = kNumBlocks * 16;
  // Verify that handles deserialize correctly.

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

  // Log for informational purposes.
  fprintf(stderr,
          "HandleRoundTrip: total serialized=%zu, "
          "handle arrays=%zu bytes\n",
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
  ASSERT_EQ(iter.Value().offset, 100u);
  ASSERT_EQ(iter.Value().size, 10u);

  // Next should give "ab" with handle {200, 20}.
  ASSERT_TRUE(iter.Next());
  ASSERT_EQ(iter.Key().ToString(), "ab");
  ASSERT_EQ(iter.Value().offset, 200u);
  ASSERT_EQ(iter.Value().size, 20u);

  // Next should give "b" with handle {300, 30}.
  ASSERT_TRUE(iter.Next());
  ASSERT_EQ(iter.Key().ToString(), "b");
  ASSERT_EQ(iter.Value().offset, 300u);
  ASSERT_EQ(iter.Value().size, 30u);

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
    keys.emplace_back(len, 'a');
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
    keys.emplace_back(1, c);
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
  ASSERT_EQ(iter.Value().offset, 0u);

  ASSERT_TRUE(iter.Seek(Slice("xyz")));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Value().offset, 300u);
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
  ASSERT_EQ(iter.Value().offset, 100u);  // "bbb"'s handle.
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
  ASSERT_EQ(iter.Value().offset, 0u);  // "bbb"'s handle.
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
  ASSERT_EQ(iter.Value().offset, 200u);
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

TEST_F(LoudsTrieTest, InitFromDataMaxDepthCorruption) {
  // A corrupted SST could set max_depth_ = UINT32_MAX, which would cause
  // an integer overflow in LoudsTrieIterator (key_cap_ = MaxDepth() + 1
  // wraps to 0). Verify that InitFromData rejects this.
  //
  // Build the header manually: magic(4) + version(4) + num_keys(8) +
  // cutoff_level(4) + max_depth(4) + dense_leaf_count(8) +
  // sparse_leaf_count(8) + dense_node_count(8) + dense_child_count(8) = 56.
  std::string header;
  uint32_t magic = 0x54524945;  // "TRIE"
  uint32_t version = 1;
  uint64_t num_keys = 0;
  uint32_t cutoff_level = 0;
  uint32_t max_depth = UINT32_MAX;  // Corrupt value
  uint64_t zeros = 0;

  header.append(reinterpret_cast<const char*>(&magic), 4);
  header.append(reinterpret_cast<const char*>(&version), 4);
  header.append(reinterpret_cast<const char*>(&num_keys), 8);
  header.append(reinterpret_cast<const char*>(&cutoff_level), 4);
  header.append(reinterpret_cast<const char*>(&max_depth), 4);
  header.append(reinterpret_cast<const char*>(&zeros), 8);  // dense_leaf_count
  header.append(reinterpret_cast<const char*>(&zeros), 8);  // sparse_leaf_count
  header.append(reinterpret_cast<const char*>(&zeros), 8);  // dense_node_count
  header.append(reinterpret_cast<const char*>(&zeros), 8);  // dense_child_count
  // Pad with enough data to pass the header size check.
  header.append(256, '\0');

  LoudsTrie trie2;
  ASSERT_TRUE(trie2.InitFromData(Slice(header)).IsCorruption());

  // Also test a value just above the limit (65537).
  max_depth = 65537;
  std::string header2;
  header2.append(reinterpret_cast<const char*>(&magic), 4);
  header2.append(reinterpret_cast<const char*>(&version), 4);
  header2.append(reinterpret_cast<const char*>(&num_keys), 8);
  header2.append(reinterpret_cast<const char*>(&cutoff_level), 4);
  header2.append(reinterpret_cast<const char*>(&max_depth), 4);
  header2.append(reinterpret_cast<const char*>(&zeros), 8);
  header2.append(reinterpret_cast<const char*>(&zeros), 8);
  header2.append(reinterpret_cast<const char*>(&zeros), 8);
  header2.append(reinterpret_cast<const char*>(&zeros), 8);
  header2.append(256, '\0');

  LoudsTrie trie3;
  ASSERT_TRUE(trie3.InitFromData(Slice(header2)).IsCorruption());
}

TEST_F(LoudsTrieTest, MoveConstructor) {
  // Verify that move constructor works correctly for LoudsTrie, which
  // contains Bitvector members with RecomputePointers() logic.
  std::vector<std::string> keys = {"apple", "banana", "cherry", "date"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie original;
  ASSERT_OK(original.InitFromData(data));
  ASSERT_EQ(original.NumKeys(), keys.size());

  // Move-construct a new trie.
  LoudsTrie moved(std::move(original));
  ASSERT_EQ(moved.NumKeys(), keys.size());

  // Verify iteration works on the moved trie.
  LoudsTrieIterator iter(&moved);
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
  ASSERT_FALSE(iter.Next());
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
    if (i < keys.size() - 1) {
      iter.Next();
    }
  }
}

TEST_F(LoudsTrieTest, SerializeDeserializeRoundTripMisalignedData) {
  // Verify that LoudsTrie::InitFromData handles non-8-byte-aligned data.
  // This can happen when the SST block is read from mmap at an unaligned
  // file offset.
  std::vector<std::string> keys = {"alpha", "beta", "gamma", "delta"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  // Create a buffer with 1-byte offset to guarantee misalignment.
  std::string padded;
  padded.reserve(1 + data.size());
  padded.push_back('\0');  // 1-byte padding
  padded.append(data.data(), data.size());
  const char* misaligned_ptr = padded.data() + 1;
  ASSERT_NE(reinterpret_cast<uintptr_t>(misaligned_ptr) % 8, 0u);

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(Slice(misaligned_ptr, data.size())));
  ASSERT_EQ(trie.NumKeys(), keys.size());

  // Verify all keys and handles via iteration.
  LoudsTrieIterator iter(&trie);
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
    if (i < keys.size() - 1) {
      iter.Next();
    }
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
    keys.emplace_back(buf);
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
      keys.emplace_back(buf);
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
    keys.emplace_back(1, c);
  }
  // Medium keys with shared prefixes.
  for (int i = 0; i < 100; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "medium_%04d", i);
    keys.emplace_back(buf);
  }
  // Long keys.
  for (int i = 0; i < 50; i++) {
    char buf[64];
    snprintf(buf, sizeof(buf), "this_is_a_very_long_key_%06d", i);
    keys.emplace_back(buf);
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
    keys.emplace_back(buf);
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

TEST_F(LoudsTrieTest, MoveAssignment) {
  // Test move assignment operator (move constructor was already tested).
  std::vector<std::string> keys = {"alpha", "beta", "gamma", "delta"};
  std::sort(keys.begin(), keys.end());

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data_copy(builder.GetSerializedData().data(),
                        builder.GetSerializedData().size());

  LoudsTrie trie1;
  ASSERT_OK(trie1.InitFromData(Slice(data_copy)));
  ASSERT_EQ(trie1.NumKeys(), keys.size());

  // Build a different trie and move-assign trie1 to it.
  LoudsTrie trie2;
  trie2 = std::move(trie1);
  ASSERT_EQ(trie2.NumKeys(), keys.size());

  // Verify the moved-to trie works correctly.
  LoudsTrieIterator iter(&trie2);
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
  ASSERT_FALSE(iter.Next());
}

TEST_F(LoudsTrieTest, CutoffLevelAndMaxDepthAndHasChains) {
  // Verify CutoffLevel(), MaxDepth(), and HasChains() return sensible values.
  std::vector<std::string> keys = {"a", "ab", "abc", "abcd", "b", "bc"};
  std::sort(keys.begin(), keys.end());

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  // MaxDepth should equal the longest key length.
  uint32_t max_key_len = 0;
  for (const auto& k : keys) {
    max_key_len = std::max(max_key_len, static_cast<uint32_t>(k.size()));
  }
  ASSERT_EQ(trie.MaxDepth(), max_key_len);

  // CutoffLevel should be within [0, max_depth].
  ASSERT_LE(trie.CutoffLevel(), trie.MaxDepth());
}

TEST_F(LoudsTrieTest, LeafIndex) {
  // Verify LeafIndex() returns correct indices during sequential iteration.
  std::vector<std::string> keys = {"cat", "cow", "dog", "fox"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  LoudsTrieIterator iter(&trie);
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));

  // LeafIndex should be sequential for sequential keys.
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.LeafIndex(), i) << "LeafIndex mismatch at key=" << keys[i];
    ASSERT_EQ(iter.Value().offset, i * 100);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
}

TEST_F(LoudsTrieTest, EmptyTrieIterator) {
  // Verify iterator behavior on an empty trie.
  LoudsTrieBuilder builder;
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));
  ASSERT_EQ(trie.NumKeys(), 0u);

  LoudsTrieIterator iter(&trie);
  ASSERT_FALSE(iter.Seek(Slice("anything")));
  ASSERT_FALSE(iter.Valid());
  ASSERT_FALSE(iter.Next());
  ASSERT_FALSE(iter.Valid());
}

TEST_F(LoudsTrieTest, ApproximateAuxMemoryUsage) {
  // Verify that ApproximateAuxMemoryUsage() returns a reasonable value
  // for a trie with sparse internal nodes.
  std::vector<std::string> keys;
  for (int i = 0; i < 200; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key_%04d", i);
    keys.emplace_back(buf);
  }

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  Slice data = builder.GetSerializedData();

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(data));

  // Aux memory should be > 0 for a trie with sparse internal nodes
  // (child position lookup tables are allocated).
  size_t aux_mem = trie.ApproximateAuxMemoryUsage();
  ASSERT_GT(aux_mem, 0u);
}

TEST_F(LoudsTrieTest, InitFromDataUnsupportedVersion) {
  // Build a valid trie, then patch the format version to a bad value.
  std::vector<std::string> keys = {"a", "b", "c"};
  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data(builder.GetSerializedData().data(),
                   builder.GetSerializedData().size());

  // Version is at offset 4 (after the 4-byte magic).
  uint32_t bad_version = 99;
  memcpy(&data[4], &bad_version, sizeof(uint32_t));

  LoudsTrie trie;
  Status s = trie.InitFromData(Slice(data));
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("unsupported format version"), std::string::npos);
}

TEST_F(LoudsTrieTest, InitFromDataProgressiveTruncation) {
  // Build a valid trie (no chains), serialize it, then try InitFromData with
  // every length from 0 to data.size()-1 and verify all return Corruption.
  // This covers ALL truncation error paths in InitFromData in a single test.
  std::vector<std::string> keys;
  for (int i = 0; i < 20; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%02d", i);
    keys.emplace_back(buf);
  }

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data(builder.GetSerializedData().data(),
                   builder.GetSerializedData().size());

  // Verify the full data works.
  {
    LoudsTrie trie;
    ASSERT_OK(trie.InitFromData(Slice(data)));
  }

  // Every truncated length should fail with Corruption.
  for (size_t len = 0; len < data.size(); len++) {
    LoudsTrie trie;
    Status s = trie.InitFromData(Slice(data.data(), len));
    ASSERT_TRUE(s.IsCorruption()) << "Expected Corruption at len=" << len << "/"
                                  << data.size() << ", got: " << s.ToString();
  }
}

TEST_F(LoudsTrieTest, InitFromDataProgressiveTruncationWithChains) {
  // Same as above, but with keys that produce path compression chains.
  // Keys share a 10-char prefix "AAAAAAAAAA" + varying suffix, ensuring
  // the chain suffix is >= kMinChainLength (8).
  std::vector<std::string> keys;
  std::string prefix(10, 'A');
  for (int i = 0; i < 20; i++) {
    char suffix[8];
    snprintf(suffix, sizeof(suffix), "%02d", i);
    keys.push_back(prefix + suffix);
  }
  std::sort(keys.begin(), keys.end());

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data(builder.GetSerializedData().data(),
                   builder.GetSerializedData().size());

  // Verify the full data works and has chains.
  {
    LoudsTrie trie;
    ASSERT_OK(trie.InitFromData(Slice(data)));
    ASSERT_TRUE(trie.HasChains());
  }

  // Every truncated length should fail with Corruption.
  for (size_t len = 0; len < data.size(); len++) {
    LoudsTrie trie;
    Status s = trie.InitFromData(Slice(data.data(), len));
    ASSERT_TRUE(s.IsCorruption()) << "Expected Corruption at len=" << len << "/"
                                  << data.size() << ", got: " << s.ToString();
  }
}

TEST_F(LoudsTrieTest, SparseBinarySearchPath) {
  // Create a trie where the root node has >16 children in the sparse level,
  // which forces SparseSeekLabel to use std::lower_bound instead of linear
  // scan (kLinearScanThreshold = 16).
  // Strategy: 20 two-byte keys with distinct first bytes. With cutoff=0
  // (all sparse), the root has 20 labels.
  std::vector<std::string> keys;
  // Use bytes 'A' through 'T' (20 distinct first bytes).
  for (int i = 0; i < 20; i++) {
    std::string key(1, static_cast<char>('A' + i));
    key += 'x';  // Second byte to ensure sparse leaf.
    keys.push_back(key);
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

  LoudsTrieIterator iter(&trie);

  // Seek to each key — exercises binary search in the 20-label root node.
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Seek(Slice(keys[i])))
        << "Seek failed for key[" << i << "]";
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
  }

  // Seek to a key between existing labels (binary search inexact match).
  // 'A' + 0.5 doesn't exist — seek "AAx" which is between "Ax" and "Bx".
  ASSERT_TRUE(iter.Seek(Slice("AAx")));
  // Should land on "Ax" or "Bx" — the first key >= "AAx".
  ASSERT_TRUE(iter.Valid());
  std::string result_key = iter.Key().ToString();
  ASSERT_GE(result_key, "AAx");

  // Seek past all keys.
  ASSERT_FALSE(iter.Seek(Slice("Zx")));
  ASSERT_FALSE(iter.Valid());

  // Full forward scan to verify trie integrity.
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
  ASSERT_FALSE(iter.Next());
}

TEST_F(LoudsTrieTest, ChainSeekVariousTargets) {
  // Build a trie with keys that create path compression chains, then seek to
  // various targets that exercise different chain-matching code paths:
  //   1. Full chain match (chain ends at internal node with fanout > 1)
  //   2. Chain mismatch: target < chain suffix
  //   3. Chain mismatch: target > chain suffix
  //   4. Target runs out before chain (cmp < 0)
  //   5. Target runs out before chain (cmp > 0)
  //   6. Target runs out before chain (cmp == 0, mid-chain consumption)
  //   7. target_remaining == 0 (target consumed at parent)
  //
  // Keys: all share a long prefix that triggers chain compression.
  // The prefix "AAAAAAAAA" (9 chars) + varying 2-char suffixes.
  // The chain suffix must be >= kMinChainLength (8 bytes).
  std::vector<std::string> keys;
  std::string prefix(9, 'A');
  // Add keys with suffixes that force a fanout > 1 after the chain.
  keys.push_back(prefix + "Ma");
  keys.push_back(prefix + "Mb");
  keys.push_back(prefix + "Mc");
  keys.push_back(prefix + "Na");
  keys.push_back(prefix + "Nb");
  std::sort(keys.begin(), keys.end());

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data(builder.GetSerializedData().data(),
                   builder.GetSerializedData().size());

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(Slice(data)));

  LoudsTrieIterator iter(&trie);

  // 1. Exact match on each key (full chain match, internal node after chain).
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Seek(Slice(keys[i])))
        << "Seek failed for key[" << i << "]=\"" << keys[i] << "\"";
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    ASSERT_EQ(iter.Value().offset, i * 100);
  }

  // 2. Chain mismatch: target < chain suffix.
  // Seek to prefix + "L" — 'L' < 'M', so chain mismatch with target < suffix.
  // Should land on the first key (prefix + "Ma").
  {
    std::string target = prefix + "La";
    ASSERT_TRUE(iter.Seek(Slice(target)));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[0]);  // "AAAAAAAAA" + "Ma"
  }

  // 3. Chain mismatch: target > chain suffix.
  // Seek to prefix + "Md" — 'd' > 'c' after chain, should advance past "Mc".
  {
    std::string target = prefix + "Md";
    ASSERT_TRUE(iter.Seek(Slice(target)));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), prefix + "Na");
  }

  // 4. Target runs out before chain (shorter target < chain prefix).
  // Seek to prefix + "L" (shorter than chain). 'L' < 'M' → target < chain.
  {
    std::string target = prefix + "L";
    ASSERT_TRUE(iter.Seek(Slice(target)));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[0]);
  }

  // 5. Target runs out before chain (shorter target > chain prefix character).
  // Seek to prefix + "O" — 'O' > 'N' (the highest chain prefix). Should
  // advance past all keys.
  {
    std::string target = prefix + "O";
    ASSERT_FALSE(iter.Seek(Slice(target)));
    ASSERT_FALSE(iter.Valid());
  }

  // 6. Target consumed exactly at the parent of the chain (target_remaining
  // == 0). Seek to just the prefix "AAAAAAAAA" — shorter than any key.
  // All keys are > prefix, so should land on the first key.
  {
    ASSERT_TRUE(iter.Seek(Slice(prefix)));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.Key().ToString(), keys[0]);
  }

  // 7. Full forward scan to verify trie integrity after all seeks.
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
    if (i < keys.size() - 1) {
      ASSERT_TRUE(iter.Next());
    }
  }
  ASSERT_FALSE(iter.Next());
}

TEST_F(LoudsTrieTest, ChainSeekEndAtLeaf) {
  // Test path compression chains that end at a leaf node (end_child_idx ==
  // UINT32_MAX). This requires a key whose suffix after the chain endpoint
  // has no children — i.e., the chain is the entire remaining path.
  //
  // Strategy: Create keys with a long shared prefix and a single-character
  // difference at the very end, so the chain covers most of the key and
  // terminates at a leaf.
  //
  // "AAAAAAAAA" (9 chars prefix) + "B" = 10-char key. The "AAAAAAAA" portion
  // (8 bytes) forms the chain suffix (>= kMinChainLength). The 'B' is the
  // label at the chain-ending leaf.
  //
  // We also add keys with different first characters to ensure the trie has
  // enough structure for the budget formula.
  std::vector<std::string> keys;
  // Main chain key.
  keys.push_back(std::string(9, 'A') + "B");
  // Additional keys with a different prefix to provide sparse-level context.
  for (int i = 0; i < 5; i++) {
    std::string key(1, static_cast<char>('C' + i));
    key += "x";
    keys.push_back(key);
  }
  std::sort(keys.begin(), keys.end());

  LoudsTrieBuilder builder;
  for (size_t i = 0; i < keys.size(); i++) {
    builder.AddKey(Slice(keys[i]), {i * 100, 50});
  }
  builder.Finish();
  std::string data(builder.GetSerializedData().data(),
                   builder.GetSerializedData().size());

  LoudsTrie trie;
  ASSERT_OK(trie.InitFromData(Slice(data)));

  LoudsTrieIterator iter(&trie);

  // Exact seek to the chain-ending-at-leaf key.
  std::string chain_key = std::string(9, 'A') + "B";
  ASSERT_TRUE(iter.Seek(Slice(chain_key)));
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(iter.Key().ToString(), chain_key);

  // Seek to a target longer than the chain key — target has more bytes but
  // trie key ends at leaf. Should Advance() past it.
  std::string longer_target = chain_key + "Z";
  ASSERT_TRUE(iter.Seek(Slice(longer_target)));
  ASSERT_TRUE(iter.Valid());
  // Should land on the next key after the chain key.
  ASSERT_GT(iter.Key().ToString(), chain_key);

  // Seek to a target that is the chain key prefix (target consumed mid-chain).
  std::string mid_chain = std::string(5, 'A');
  ASSERT_TRUE(iter.Seek(Slice(mid_chain)));
  ASSERT_TRUE(iter.Valid());
  // First key >= "AAAAA" is "AAAAAAAAAB".
  ASSERT_EQ(iter.Key().ToString(), chain_key);

  // Full forward scan to verify integrity.
  ASSERT_TRUE(iter.Seek(Slice(keys[0])));
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_TRUE(iter.Valid()) << "Invalid at i=" << i;
    ASSERT_EQ(iter.Key().ToString(), keys[i]);
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
    UserDefinedIndexBuilder::BlockHandle handle{i * 1000, 500};

    std::string scratch;
    Slice next_slice(first_keys[i]);
    const Slice* next = (i < last_keys.size() - 1) ? &next_slice : nullptr;
    builder->AddIndexEntry(Slice(last_keys[i]), next, handle, &scratch);
  }

  // Finish building.
  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  ASSERT_GT(index_contents.size(), 0u);

  // Read the index.
  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));
  ASSERT_NE(reader, nullptr);
  ASSERT_GT(reader->ApproximateMemoryUsage(), 0u);

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

  // Next — the previous separator is for block 1 (< "key_01z"), so
  // CheckBounds conservatively returns kInbound. The data-level iterator
  // handles per-key filtering. This is correct per the UDI API contract:
  // since we don't store first-in-block keys, we cannot determine that
  // block 2 is fully out of bounds.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
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

TEST_F(TrieIndexFactoryTest, UpperBoundDoesNotDropValidBlocks) {
  // Regression test for Finding 1: the trie stores separator keys (upper
  // bounds on block contents), NOT first-in-block keys. CheckBounds must
  // use the previous separator (or seek target) as the reference key, not
  // the current separator, to avoid prematurely rejecting blocks that
  // contain keys within the limit.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // Block 0: last="az", next_first="c" → separator ≈ "b"
  {
    UserDefinedIndexBuilder::BlockHandle handle{0, 1000};
    std::string scratch;
    Slice next("c");
    udi_builder->AddIndexEntry(Slice("az"), &next, handle, &scratch);
  }
  // Block 1: last="cz", next_first="e" → separator ≈ "d"
  {
    UserDefinedIndexBuilder::BlockHandle handle{1000, 1000};
    std::string scratch;
    Slice next("e");
    udi_builder->AddIndexEntry(Slice("cz"), &next, handle, &scratch);
  }
  // Block 2: last="ez", no next → separator ≈ "f"
  {
    UserDefinedIndexBuilder::BlockHandle handle{2000, 1000};
    std::string scratch;
    udi_builder->AddIndexEntry(Slice("ez"), nullptr, handle, &scratch);
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // Upper bound "d": blocks 0 and 1 may contain keys < "d".
  ScanOptions scan_opts(Slice("a"), Slice("d"));
  iter->Prepare(&scan_opts, 1);

  IterateResult result;
  // Seek("a") → lands on block 0 (separator "b"). target "a" < "d" → kInbound.
  ASSERT_OK(iter->SeekAndGetResult(Slice("a"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 0u);

  // Next() → lands on block 1 (separator "d"). Previous separator "b" < "d"
  // → kInbound (conservative). Block 1 contains keys like "c".."cz", which
  // are < "d", so returning kInbound is correct.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 1000u);

  // Next() → lands on block 2 (separator "f"). Previous separator "d" >= "d"
  // → kOutOfBound. All keys in block 2 are >= "d", so this is correct.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);
}

TEST_F(TrieIndexFactoryTest, MultiScanBoundsAdvanceCorrectly) {
  // Regression test for Finding 2: current_scan_idx_ must advance when
  // the seek target is past the current scan's limit. Otherwise all
  // bounds checks evaluate against scan 0's limit.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // 5 blocks with well-separated keys.
  struct BlockDef {
    const char* last_key;
    const char* next_first;
    uint64_t offset;
  };
  BlockDef blocks[] = {
      {"az", "c", 0},    {"cz", "e", 1000},     {"ez", "g", 2000},
      {"gz", "i", 3000}, {"iz", nullptr, 4000},
  };
  for (const auto& b : blocks) {
    UserDefinedIndexBuilder::BlockHandle handle{b.offset, 500};
    std::string scratch;
    if (b.next_first) {
      Slice next(b.next_first);
      udi_builder->AddIndexEntry(Slice(b.last_key), &next, handle, &scratch);
    } else {
      udi_builder->AddIndexEntry(Slice(b.last_key), nullptr, handle, &scratch);
    }
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // Two non-overlapping scan ranges.
  ScanOptions scans[] = {
      ScanOptions(Slice("a"), Slice("c")),  // scan 0: blocks in [a, c)
      ScanOptions(Slice("e"), Slice("g")),  // scan 1: blocks in [e, g)
  };
  iter->Prepare(scans, 2);

  IterateResult result;

  // --- Scan 0 ---
  // Seek("a") → block 0 (separator "b"). target "a" < limit "c" → kInbound.
  ASSERT_OK(iter->SeekAndGetResult(Slice("a"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 0u);

  // Next() → block 1 (separator "d"). prev "b" < "c" → kInbound (conservative).
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next() → block 2 (separator "f"). prev "d" >= "c" → kOutOfBound.
  // Scan 0 is done.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);

  // --- Scan 1 ---
  // Seek("e") should advance current_scan_idx_ to 1 (target "e" >= scan 0
  // limit "c"), then check against scan 1's limit "g".
  // Lands on block 2 (separator "f"). target "e" < "g" → kInbound.
  ASSERT_OK(iter->SeekAndGetResult(Slice("e"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
  ASSERT_EQ(iter->value().offset, 2000u);

  // Next() → block 3 (separator "h"). prev "f" < "g" → kInbound (conservative).
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Next() → block 4 (separator "j"). prev "h" >= "g" → kOutOfBound.
  // Scan 1 is done.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);
}

TEST_F(TrieIndexFactoryTest, RejectsNonBytewiseComparator) {
  // The trie index requires bytewise ordering because it traverses keys
  // byte-by-byte. Non-bytewise comparators should be rejected.
  UserDefinedIndexOption option;

  // ReverseBytewiseComparator should be rejected.
  option.comparator = ReverseBytewiseComparator();
  std::unique_ptr<UserDefinedIndexBuilder> builder;
  Status s = factory_->NewBuilder(option, builder);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_EQ(builder, nullptr);

  // NewReader should also reject non-bytewise comparators.
  Slice dummy_data;
  std::unique_ptr<UserDefinedIndexReader> reader;
  s = factory_->NewReader(option, dummy_data, reader);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_EQ(reader, nullptr);

  // BytewiseComparator should be accepted.
  option.comparator = BytewiseComparator();
  ASSERT_OK(factory_->NewBuilder(option, builder));
  ASSERT_NE(builder, nullptr);
}

TEST_F(TrieIndexFactoryTest, ApproximateMemoryUsageIncludesAuxData) {
  // Verify that ApproximateMemoryUsage() accounts for auxiliary heap
  // allocations (child position lookup tables), not just serialized data.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // Build a non-trivial index with enough keys to produce sparse internal
  // nodes (which allocate child position lookup tables).
  for (int i = 0; i < 100; i++) {
    char last_buf[32];
    char next_buf[32];
    snprintf(last_buf, sizeof(last_buf), "key_%04d", i);
    snprintf(next_buf, sizeof(next_buf), "key_%04d", i + 1);

    UserDefinedIndexBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                                500};
    std::string scratch;
    if (i < 99) {
      Slice next(next_buf);
      udi_builder->AddIndexEntry(Slice(last_buf), &next, handle, &scratch);
    } else {
      udi_builder->AddIndexEntry(Slice(last_buf), nullptr, handle, &scratch);
    }
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));
  size_t serialized_size = index_contents.size();

  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  size_t mem_usage = reader->ApproximateMemoryUsage();
  // Memory usage should be at least the serialized data size.
  ASSERT_GE(mem_usage, serialized_size);

  fprintf(stderr,
          "ApproximateMemoryUsage: serialized=%zu, reported=%zu, "
          "aux_overhead=%zu bytes\n",
          serialized_size, mem_usage, mem_usage - serialized_size);
}

TEST_F(TrieIndexFactoryTest, EmptyTrieIterator) {
  // Seek on an iterator built from an empty index.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);
  ASSERT_NE(iter, nullptr);

  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("anything"), &result));
  // kUnknown because we cannot be certain all keys in this file exceed the
  // upper bound — the next file may still have in-range keys.
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, PrepareWithZeroScans) {
  // Prepare with 0 scan ranges, then seek — should behave as no bounds.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // Build 3 blocks.
  const char* last_keys[] = {"az", "cz", "ez"};
  const char* next_keys[] = {"c", "e", nullptr};
  for (int i = 0; i < 3; i++) {
    UserDefinedIndexBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                                500};
    std::string scratch;
    if (next_keys[i]) {
      Slice next(next_keys[i]);
      udi_builder->AddIndexEntry(Slice(last_keys[i]), &next, handle, &scratch);
    } else {
      udi_builder->AddIndexEntry(Slice(last_keys[i]), nullptr, handle,
                                 &scratch);
    }
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // Prepare with 0 scans.
  iter->Prepare(nullptr, 0);

  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("a"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, RePrepareResetsScanState) {
  // Call Prepare twice — second Prepare should reset scan state.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  const char* last_keys[] = {"az", "cz", "ez"};
  const char* next_keys[] = {"c", "e", nullptr};
  for (int i = 0; i < 3; i++) {
    UserDefinedIndexBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                                500};
    std::string scratch;
    if (next_keys[i]) {
      Slice next(next_keys[i]);
      udi_builder->AddIndexEntry(Slice(last_keys[i]), &next, handle, &scratch);
    } else {
      udi_builder->AddIndexEntry(Slice(last_keys[i]), nullptr, handle,
                                 &scratch);
    }
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // First Prepare with limit "b".
  ScanOptions scan1(Slice("a"), Slice("b"));
  iter->Prepare(&scan1, 1);

  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("a"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // Re-prepare with a broader limit "f".
  ScanOptions scan2(Slice("a"), Slice("f"));
  iter->Prepare(&scan2, 1);

  // Should be able to seek to "d" and get inbound with the new limit.
  ASSERT_OK(iter->SeekAndGetResult(Slice("d"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);
}

TEST_F(TrieIndexFactoryTest, ScanWithNoLimit) {
  // Prepare with a scan range that has no upper limit.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  const char* last_keys[] = {"az", "cz", "ez"};
  const char* next_keys[] = {"c", "e", nullptr};
  for (int i = 0; i < 3; i++) {
    UserDefinedIndexBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                                500};
    std::string scratch;
    if (next_keys[i]) {
      Slice next(next_keys[i]);
      udi_builder->AddIndexEntry(Slice(last_keys[i]), &next, handle, &scratch);
    } else {
      udi_builder->AddIndexEntry(Slice(last_keys[i]), nullptr, handle,
                                 &scratch);
    }
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // ScanOptions with only a start, no limit.
  ScanOptions scan(Slice("a"));
  iter->Prepare(&scan, 1);

  // All seeks should be inbound with no limit.
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("a"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kInbound);

  // No more blocks — kUnknown because we cannot be certain all keys in this
  // file exceed the upper bound.
  ASSERT_OK(iter->NextAndGetResult(&result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kUnknown);
}

TEST_F(TrieIndexFactoryTest, NewReaderWithCorruptedData) {
  // Attempt to create a reader from corrupted data.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::string bad_data(10, '\xff');
  Slice bad_slice(bad_data);
  std::unique_ptr<UserDefinedIndexReader> reader;
  Status s = factory_->NewReader(option, bad_slice, reader);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(TrieIndexFactoryTest, OnKeyAddedNoOp) {
  // Verify that OnKeyAdded() is a no-op and doesn't crash.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));

  // Call OnKeyAdded with various inputs — it should do nothing.
  builder->OnKeyAdded(Slice("key1"), UserDefinedIndexBuilder::kValue,
                      Slice("value1"));
  builder->OnKeyAdded(Slice("key2"), UserDefinedIndexBuilder::kValue,
                      Slice("value2"));
  builder->OnKeyAdded(Slice(""), UserDefinedIndexBuilder::kValue, Slice(""));

  // Building should still succeed (OnKeyAdded should not affect state).
  UserDefinedIndexBuilder::BlockHandle handle{0, 500};
  std::string scratch;
  builder->AddIndexEntry(Slice("key3"), nullptr, handle, &scratch);

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  ASSERT_GT(index_contents.size(), 0u);
}

TEST_F(TrieIndexFactoryTest, NullComparator) {
  // NewBuilder and NewReader with nullptr comparator should succeed.
  // Note: AddIndexEntry uses comparator_->FindShortSeparator() which requires
  // a non-null comparator, so we only test that the factory accepts nullptr.
  UserDefinedIndexOption option;
  option.comparator = nullptr;

  std::unique_ptr<UserDefinedIndexBuilder> builder;
  ASSERT_OK(factory_->NewBuilder(option, builder));
  ASSERT_NE(builder, nullptr);

  // Finish without adding entries — nullptr comparator is accepted.
  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));

  // NewReader with nullptr comparator should also succeed on an empty index
  // (empty index produces empty Slice, which may not be parseable, so we
  // just verify NewBuilder accepts nullptr).
}

TEST_F(TrieIndexFactoryTest, SeekSucceedsButTargetPastLimit) {
  // Set up bounds with a limit, then seek to a target that is >= the limit.
  // CheckBounds should return kOutOfBound even though the trie Seek succeeds.
  UserDefinedIndexOption option;
  option.comparator = BytewiseComparator();

  std::unique_ptr<UserDefinedIndexBuilder> udi_builder;
  ASSERT_OK(factory_->NewBuilder(option, udi_builder));

  // Build 3 blocks: separators "b", "d", "f".
  const char* last_keys[] = {"az", "cz", "ez"};
  const char* next_keys[] = {"c", "e", nullptr};
  for (int i = 0; i < 3; i++) {
    UserDefinedIndexBuilder::BlockHandle handle{static_cast<uint64_t>(i) * 1000,
                                                500};
    std::string scratch;
    if (next_keys[i]) {
      Slice next(next_keys[i]);
      udi_builder->AddIndexEntry(Slice(last_keys[i]), &next, handle, &scratch);
    } else {
      udi_builder->AddIndexEntry(Slice(last_keys[i]), nullptr, handle,
                                 &scratch);
    }
  }

  Slice index_contents;
  ASSERT_OK(udi_builder->Finish(&index_contents));

  std::unique_ptr<UserDefinedIndexReader> reader;
  ASSERT_OK(factory_->NewReader(option, index_contents, reader));

  ReadOptions ro;
  auto iter = reader->NewIterator(ro);

  // Prepare with limit "c" (exclusive upper bound).
  ScanOptions scan(Slice("a"), Slice("c"));
  iter->Prepare(&scan, 1);

  // Seek to "c" — target == limit, so CheckBounds returns kOutOfBound.
  IterateResult result;
  ASSERT_OK(iter->SeekAndGetResult(Slice("c"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);

  // Seek to "d" — target > limit, also kOutOfBound.
  ASSERT_OK(iter->SeekAndGetResult(Slice("d"), &result));
  ASSERT_EQ(result.bound_check_result, IterBoundCheck::kOutOfBound);
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
      char key_buf[32];
      char val_buf[32];
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
    if (!s.ok()) {
      return s;
    }

    for (const auto& kv : kvs) {
      s = writer.Put(kv.first, kv.second);
      if (!s.ok()) {
        return s;
      }
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

// ============================================================================
// Micro-benchmarks: trie Seek vs RocksDB IndexBlockIter.
//
// Compares the trie data structure against the actual RocksDB binary search
// index (IndexBlockIter) which is used in production. The IndexBlockIter
// operates on a prefix-compressed block with InternalKeys, varint decoding,
// and the InternalKeyComparator abstraction — making it a realistic baseline.
// ============================================================================

class TrieSeekBenchmark : public testing::Test {
 protected:
  // Generate sorted fixed-width keys that mimic realistic RocksDB separator
  // keys. Uses snprintf to create keys like "key00000000NNNNNNNN" where N is
  // the zero-padded index. This produces keys with moderate prefix sharing
  // (the "key" prefix + some leading zeros) while ensuring each key differs
  // at multiple byte positions, matching the distribution that SST separator
  // keys would have in practice.
  static std::vector<std::string> GenerateKeys(size_t count,
                                               size_t key_size = 16) {
    std::vector<std::string> keys;
    keys.reserve(count);
    char buf[64];
    for (size_t i = 0; i < count; i++) {
      snprintf(buf, sizeof(buf), "%0*zu", static_cast<int>(key_size), i);
      keys.emplace_back(buf, key_size);
    }
    return keys;
  }

  // Build a trie from sorted separator keys. Returns {trie, serialized_data}.
  // The serialized data string must outlive the trie since the trie stores
  // zero-copy pointers into it.
  static std::pair<LoudsTrie, std::string> BuildTrie(
      const std::vector<std::string>& keys) {
    LoudsTrieBuilder builder;
    for (size_t i = 0; i < keys.size(); i++) {
      TrieBlockHandle handle{i * 4096, 4096};
      builder.AddKey(Slice(keys[i]), handle);
    }
    builder.Finish();

    std::string data(builder.GetSerializedData().data(),
                     builder.GetSerializedData().size());
    LoudsTrie trie;
    Status s = trie.InitFromData(Slice(data));
    assert(s.ok());
    (void)s;
    return {std::move(trie), std::move(data)};
  }

  // Generate random lookup keys (existing keys from the set).
  static std::vector<size_t> GenerateRandomIndices(size_t count,
                                                   size_t num_keys,
                                                   uint64_t seed = 42) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<size_t> dist(0, num_keys - 1);
    std::vector<size_t> indices(count);
    for (auto& idx : indices) {
      idx = dist(rng);
    }
    return indices;
  }
};

// Benchmark the trie against the actual RocksDB IndexBlockIter, which is
// the real binary search index used in production. Both sides replicate
// their full production code paths:
//
// Trie (via UDI wrapper):
//   ParseInternalKey → Seek(user_key) → Key().ToString() (string copy) →
//   CheckBounds (no-op without Prepare) → Value() → BlockHandle
//
// IndexBlockIter:
//   Seek(internal_key) → value() → IndexValue with BlockHandle
//
// The IndexBlockIter operates on a prefix-compressed block with InternalKeys,
// varint decoding, and the InternalKeyComparator — matching production exactly.
TEST_F(TrieSeekBenchmark, TrieVsRealIndexBlockIter) {
  static constexpr size_t kNumLookups = 200000;
  static constexpr size_t kKeySize = 16;
  static constexpr size_t kKeyCounts[] = {100, 500, 1000, 5000, 10000, 32000};

  fprintf(stderr,
          "\n====== Trie vs Real IndexBlockIter: Scaling (%zu-byte keys, %zu "
          "lookups) ======\n",
          kKeySize, kNumLookups);
  fprintf(stderr, "  %8s  %12s  %12s  %8s  %s\n", "Keys", "Trie ns/op",
          "IBI ns/op", "vs IBI", "Winner");
  fprintf(stderr, "  %8s  %12s  %12s  %8s  %s\n", "--------", "------------",
          "------------", "--------", "----------");

  for (size_t num_keys : kKeyCounts) {
    auto keys = GenerateKeys(num_keys, kKeySize);

    // ---- Build trie ----
    auto [trie, trie_data] = BuildTrie(keys);
    LoudsTrieIterator trie_iter(&trie);

    // ---- Build real RocksDB index block ----
    // Use restart_interval=1 (the default for index blocks).
    const int kRestartInterval = 1;
    BlockBuilder index_builder(kRestartInterval,
                               /*use_delta_encoding=*/true,
                               /*use_value_delta_encoding=*/false);

    // Convert user keys to InternalKeys and add to the index block.
    std::vector<std::string> internal_keys;
    internal_keys.reserve(num_keys);
    for (size_t i = 0; i < num_keys; i++) {
      std::string ikey = keys[i];
      AppendInternalKeyFooter(&ikey, /*s=*/0, kTypeValue);
      internal_keys.push_back(ikey);

      BlockHandle handle(i * 4096, 4096);
      IndexValue entry(handle, Slice());
      std::string encoded_value;
      entry.EncodeTo(&encoded_value, /*have_first_key=*/false,
                     /*previous_handle=*/nullptr);
      index_builder.Add(Slice(internal_keys.back()), Slice(encoded_value));
    }

    // Finish the block and create a Block reader.
    Slice raw_block = index_builder.Finish();
    std::string block_data(raw_block.data(), raw_block.size());
    BlockContents contents;
    contents.data = Slice(block_data);
    Block index_block(std::move(contents));

    // ---- Generate random lookup targets ----
    auto indices = GenerateRandomIndices(kNumLookups, num_keys);

    // Pre-build InternalKey seek targets (user_key + kMaxSequenceNumber).
    // Both sides use the same InternalKey targets — the trie path parses
    // them to extract the user key (matching production), while
    // IndexBlockIter uses them directly.
    std::vector<std::string> seek_ikeys;
    seek_ikeys.reserve(kNumLookups);
    for (auto idx : indices) {
      std::string sk = keys[idx];
      AppendInternalKeyFooter(&sk, kMaxSequenceNumber, kValueTypeForSeek);
      seek_ikeys.push_back(std::move(sk));
    }
    std::vector<Slice> seek_ikey_slices;
    seek_ikey_slices.reserve(kNumLookups);
    for (const auto& sk : seek_ikeys) {
      seek_ikey_slices.emplace_back(sk);
    }

    // ---- Benchmark: Trie Seek (full production path) ----
    // Replicates UserDefinedIndexIteratorWrapper::Seek() →
    //   TrieIndexIterator::SeekAndGetResult():
    //   1. ParseInternalKey to extract user_key
    //   2. trie_iter.Seek(user_key)
    //   3. trie_iter.Key().ToString() — string copy (result must outlive call)
    //   4. trie_iter.Value() — look up BlockHandle from packed arrays
    volatile uint64_t trie_checksum = 0;
    std::string key_scratch;  // Reused scratch, like current_key_scratch_
    auto t0 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < kNumLookups; i++) {
      // Step 1: Parse InternalKey → user key (same as wrapper)
      ParsedInternalKey pkey;
      ASSERT_OK(
          ParseInternalKey(seek_ikey_slices[i], &pkey, /*log_err_key=*/false));

      // Step 2: Seek on the trie with user key
      trie_iter.Seek(pkey.user_key);

      if (trie_iter.Valid()) {
        // Step 3: Materialize the key (string copy, matches production)
        key_scratch = trie_iter.Key().ToString();

        // Step 4: Get BlockHandle (two uint32 array reads)
        TrieBlockHandle handle = trie_iter.Value();
        trie_checksum = trie_checksum + handle.offset;
      }
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    double trie_ns =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0)
                .count()) /
        static_cast<double>(kNumLookups);

    // ---- Benchmark: Real IndexBlockIter Seek ----
    // This is exactly what production does: Seek(InternalKey) → value().
    std::unique_ptr<IndexBlockIter> ibi_iter(index_block.NewIndexIterator(
        BytewiseComparator(), kDisableGlobalSequenceNumber,
        /*iter=*/nullptr, /*stats=*/nullptr,
        /*total_order_seek=*/true, /*have_first_key=*/false,
        /*key_includes_seq=*/true, /*value_is_full=*/true));

    volatile uint64_t ibi_checksum = 0;
    auto t2 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < kNumLookups; i++) {
      ibi_iter->Seek(seek_ikey_slices[i]);
      if (ibi_iter->Valid()) {
        IndexValue val = ibi_iter->value();
        ibi_checksum = ibi_checksum + val.handle.offset();
      }
    }
    auto t3 = std::chrono::high_resolution_clock::now();
    double ibi_ns =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2)
                .count()) /
        static_cast<double>(kNumLookups);

    double vs_ibi = (ibi_ns - trie_ns) / ibi_ns * 100.0;
    const char* winner = trie_ns <= ibi_ns ? "TRIE" : "IBI";
    fprintf(stderr, "  %8zu  %10.1f    %10.1f    %+6.1f%%  %s\n", num_keys,
            trie_ns, ibi_ns, vs_ibi, winner);
  }

  fprintf(stderr, "\n");
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
