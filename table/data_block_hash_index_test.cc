// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdlib>
#include <string>
#include <unordered_map>

#include "rocksdb/slice.h"
#include "table/data_block_hash_index.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

bool SearchForOffset(DataBlockHashIndex& index, const char* data,
                     uint16_t map_offset, const Slice& key,
                     uint8_t& restart_point) {
  uint8_t entry = index.Seek(data, map_offset, key);
  if (entry == kCollision) {
    return true;
  }

  if (entry == kNoEntry) {
    return false;
  }

  return entry == restart_point;
}

TEST(DataBlockHashIndex, DataBlockHashTestSmall) {
  DataBlockHashIndexBuilder builder(0.75);
  for (int j = 0; j < 5; j++) {
    for (uint8_t i = 0; i < 2 + j; i++) {
      std::string key("key" + std::to_string(i));
      uint8_t restart_point = i;
      builder.Add(key, restart_point);
    }

    size_t estimated_size = builder.EstimateSize();

    std::string buffer("fake"), buffer2;
    size_t original_size = buffer.size();
    estimated_size += original_size;
    builder.Finish(buffer);

    ASSERT_EQ(buffer.size(), estimated_size);

    buffer2 = buffer; // test for the correctness of relative offset


    Slice s(buffer2);
    DataBlockHashIndex index;
    uint16_t map_offset;
    index.Initialize(s.data(), s.size(), &map_offset);

    // the additional hash map should start at the end of the buffer
    ASSERT_EQ(original_size, map_offset);
    for (uint8_t i = 0; i < 2; i++) {
      std::string key("key" + std::to_string(i));
      uint8_t restart_point = i;
      ASSERT_TRUE(SearchForOffset(index, s.data(), map_offset, key, restart_point));
    }
    builder.Reset(2 + j);
    ASSERT_EQ(builder.num_buckets_,
              static_cast<uint16_t>((static_cast<double>(2 + j) / 0.75)));
  }
}

TEST(DataBlockHashIndex, DataBlockHashTest) {
  // bucket_num = 200, #keys = 100. 50% utilization
  DataBlockHashIndexBuilder builder(0.75);

  for (uint8_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint8_t restart_point = i;
    builder.Add(key, restart_point);
  }

  size_t estimated_size = builder.EstimateSize();

  std::string buffer("fake content"), buffer2;
  size_t original_size = buffer.size();
  estimated_size += original_size;
  builder.Finish(buffer);

  ASSERT_EQ(buffer.size(), estimated_size);

  buffer2 = buffer; // test for the correctness of relative offset

  Slice s(buffer2);
  DataBlockHashIndex index;
  uint16_t map_offset;
  index.Initialize(s.data(), s.size(), &map_offset);

  // the additional hash map should start at the end of the buffer
  ASSERT_EQ(original_size, map_offset);
  for (uint8_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint8_t restart_point = i;
    ASSERT_TRUE(SearchForOffset(index, s.data(), map_offset, key, restart_point));
  }
}

TEST(DataBlockHashIndex, DataBlockHashTestCollision) {
  // bucket_num = 2. There will be intense hash collisions
  DataBlockHashIndexBuilder builder(0.75);

  for (uint8_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint8_t restart_point = i;
    builder.Add(key, restart_point);
  }

  size_t estimated_size = builder.EstimateSize();

  std::string buffer("some other fake content to take up space"), buffer2;
  size_t original_size = buffer.size();
  estimated_size += original_size;
  builder.Finish(buffer);

  ASSERT_EQ(buffer.size(), estimated_size);

  buffer2 = buffer; // test for the correctness of relative offset

  Slice s(buffer2);
  DataBlockHashIndex index;
  uint16_t map_offset;
  index.Initialize(s.data(), s.size(), &map_offset);

  // the additional hash map should start at the end of the buffer
  ASSERT_EQ(original_size, map_offset);
  for (uint8_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint8_t restart_point = i;
    ASSERT_TRUE(SearchForOffset(index, s.data(), map_offset, key, restart_point));
  }
}

TEST(DataBlockHashIndex, DataBlockHashTestLarge) {
  DataBlockHashIndexBuilder builder(0.75);
  std::unordered_map<std::string, uint8_t> m;

  for (uint8_t i = 0; i < 100; i++) {
    if (i % 2) {
      continue;  // leave half of the keys out
    }
    std::string key = "key" + std::to_string(i);
    uint8_t restart_point = i;
    builder.Add(key, restart_point);
    m[key] = restart_point;
  }

  size_t estimated_size = builder.EstimateSize();

  std::string buffer("filling stuff"), buffer2;
  size_t original_size = buffer.size();
  estimated_size += original_size;
  builder.Finish(buffer);

  ASSERT_EQ(buffer.size(), estimated_size);

  buffer2 = buffer; // test for the correctness of relative offset

  Slice s(buffer2);
  DataBlockHashIndex index;
  uint16_t map_offset;
  index.Initialize(s.data(), s.size(), &map_offset);

  // the additional hash map should start at the end of the buffer
  ASSERT_EQ(original_size, map_offset);
  for (uint8_t i = 0; i < 100; i++) {
    std::string key = "key" + std::to_string(i);
    uint8_t restart_point = i;
    if (m.count(key)) {
      ASSERT_TRUE(m[key] == restart_point);
      ASSERT_TRUE(SearchForOffset(index, s.data(), map_offset, key, restart_point));
    } else {
      // we allow false positve, so don't test the nonexisting keys.
      // when false positive happens, the search will continue to the
      // restart intervals to see if the key really exist.
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
