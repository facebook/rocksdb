//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction/clipping_iterator.h"

#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class ClippingIteratorTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<std::tuple<size_t, size_t>> {
};

TEST_P(ClippingIteratorTest, Clip) {
  const std::vector<std::string> keys{"key0", "key1", "key2", "key3", "key4",
                                      "key5", "key6", "key7", "key8", "key9"};
  const std::vector<std::string> values{
      "unused0", "value1",  "value2",  "value3",  "unused4",
      "unused5", "unused6", "unused7", "unused8", "unused9"};

  assert(keys.size() == values.size());

  // Note: the input always contains key1, key2, and key3; however, the clipping
  // window is based on the test parameters: its left edge is a value in the
  // range [0, 4], and its size is a value in the range [0, 5]
  using test::VectorIterator;
  VectorIterator input({keys[1], keys[2], keys[3]},
                       {values[1], values[2], values[3]});

  const size_t clip_start_idx = std::get<0>(GetParam());
  const size_t clip_window_size = std::get<1>(GetParam());
  const size_t clip_end_idx = clip_start_idx + clip_window_size;

  const Slice start(keys[clip_start_idx]);
  const Slice end(keys[clip_end_idx]);

  ClippingIterator clip(&input, &start, &end, BytewiseComparator());

  // The range the clipping iterator should return values from. This is
  // essentially the intersection of the input range [1, 4) and the clipping
  // window [clip_start_idx, clip_end_idx)
  const size_t data_start_idx =
      std::max(clip_start_idx, static_cast<size_t>(1));
  const size_t data_end_idx = std::min(clip_end_idx, static_cast<size_t>(4));

  // Range is empty; all Seeks should fail
  if (data_start_idx >= data_end_idx) {
    clip.SeekToFirst();
    ASSERT_FALSE(clip.Valid());

    clip.SeekToLast();
    ASSERT_FALSE(clip.Valid());

    for (size_t i = 0; i < keys.size(); ++i) {
      clip.Seek(keys[i]);
      ASSERT_FALSE(clip.Valid());

      clip.SeekForPrev(keys[i]);
      ASSERT_FALSE(clip.Valid());
    }

    return;
  }

  // Range is non-empty; call SeekToFirst and iterate forward
  clip.SeekToFirst();
  ASSERT_TRUE(clip.Valid());
  ASSERT_EQ(clip.key(), keys[data_start_idx]);
  ASSERT_EQ(clip.value(), values[data_start_idx]);

  for (size_t i = data_start_idx + 1; i < data_end_idx; ++i) {
    clip.Next();
    ASSERT_TRUE(clip.Valid());
    ASSERT_EQ(clip.key(), keys[i]);
    ASSERT_EQ(clip.value(), values[i]);
  }

  clip.Next();
  ASSERT_FALSE(clip.Valid());

  // Call SeekToLast and iterate backward
  clip.SeekToLast();
  ASSERT_TRUE(clip.Valid());
  ASSERT_EQ(clip.key(), keys[data_end_idx - 1]);
  ASSERT_EQ(clip.value(), values[data_end_idx - 1]);

  for (size_t i = data_end_idx - 2; i >= data_start_idx; --i) {
    clip.Prev();
    ASSERT_TRUE(clip.Valid());
    ASSERT_EQ(clip.key(), keys[i]);
    ASSERT_EQ(clip.value(), values[i]);
  }

  clip.Prev();
  ASSERT_FALSE(clip.Valid());

  // Call Seek/SeekForPrev for all keys; Seek should return the smallest key
  // which is >= the target; SeekForPrev should return the largest key which is
  // <= the target
  for (size_t i = 0; i < keys.size(); ++i) {
    clip.Seek(keys[i]);

    if (i < data_start_idx) {
      ASSERT_TRUE(clip.Valid());
      ASSERT_EQ(clip.key(), keys[data_start_idx]);
      ASSERT_EQ(clip.value(), values[data_start_idx]);
    } else if (i < data_end_idx) {
      ASSERT_TRUE(clip.Valid());
      ASSERT_EQ(clip.key(), keys[i]);
      ASSERT_EQ(clip.value(), values[i]);
    } else {
      ASSERT_FALSE(clip.Valid());
    }

    clip.SeekForPrev(keys[i]);

    if (i < data_start_idx) {
      ASSERT_FALSE(clip.Valid());
    } else if (i < data_end_idx) {
      ASSERT_TRUE(clip.Valid());
      ASSERT_EQ(clip.key(), keys[i]);
      ASSERT_EQ(clip.value(), values[i]);
    } else {
      ASSERT_TRUE(clip.Valid());
      ASSERT_EQ(clip.key(), keys[data_end_idx - 1]);
      ASSERT_EQ(clip.value(), values[data_end_idx - 1]);
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    ClippingIteratorTest, ClippingIteratorTest,
    ::testing::Combine(
        ::testing::Range(static_cast<size_t>(0), static_cast<size_t>(5)),
        ::testing::Range(static_cast<size_t>(0), static_cast<size_t>(6))));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
