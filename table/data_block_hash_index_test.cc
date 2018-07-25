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

bool SearchForOffset(DataBlockHashIndex& index, const Slice& key,
                     uint16_t& restart_point) {
  std::unique_ptr<DataBlockHashIndexIterator> iter;
  iter.reset(index.NewIterator(key));
  for (; iter->Valid(); iter->Next()) {
    if (iter->Value() == restart_point) {
      return true;
    }
  }
  return false;
}

TEST(DataBlockHashIndex, DataBlockHashTestSmall) {
  // bucket_num = 5, #keys = 2. 40% utilization
  DataBlockHashIndexBuilder builder(5);

  for (uint16_t i = 0; i < 2; i++) {
    std::string key("key" + std::to_string(i));
    uint16_t restart_point = i;
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
  DataBlockHashIndex index(s);

  // the additional hash map should start at the end of the buffer
  ASSERT_EQ(original_size, index.DataBlockHashMapStart());
  for (uint16_t i = 0; i < 2; i++) {
    std::string key("key" + std::to_string(i));
    uint16_t restart_point = i;
    ASSERT_TRUE(SearchForOffset(index, key, restart_point));
  }
}

TEST(DataBlockHashIndex, DataBlockHashTest) {
  // bucket_num = 200, #keys = 100. 50% utilization
  DataBlockHashIndexBuilder builder(200);

  for (uint16_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint16_t restart_point = i;
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
  DataBlockHashIndex index(s);

  // the additional hash map should start at the end of the buffer
  ASSERT_EQ(original_size, index.DataBlockHashMapStart());
  for (uint16_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint16_t restart_point = i;
    ASSERT_TRUE(SearchForOffset(index, key, restart_point));
  }
}

TEST(DataBlockHashIndex, DataBlockHashTestCollision) {
  // bucket_num = 2. There will be intense hash collisions
  DataBlockHashIndexBuilder builder(2);

  for (uint16_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint16_t restart_point = i;
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
  DataBlockHashIndex index(s);

  // the additional hash map should start at the end of the buffer
  ASSERT_EQ(original_size, index.DataBlockHashMapStart());
  for (uint16_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint16_t restart_point = i;
    ASSERT_TRUE(SearchForOffset(index, key, restart_point));
  }
}

TEST(DataBlockHashIndex, DataBlockHashTestLarge) {
  DataBlockHashIndexBuilder builder(1000);
  std::unordered_map<std::string, uint16_t> m;

  for (uint16_t i = 0; i < 10000; i++) {
    if (i % 2) {
      continue;  // leave half of the keys out
    }
    std::string key = "key" + std::to_string(i);
    uint16_t restart_point = i;
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
  DataBlockHashIndex index(s);

  // the additional hash map should start at the end of the buffer
  ASSERT_EQ(original_size, index.DataBlockHashMapStart());
  for (uint16_t i = 0; i < 100; i++) {
    std::string key = "key" + std::to_string(i);
    uint16_t restart_point = i;
    if (m.count(key)) {
      ASSERT_TRUE(m[key] == restart_point);
      ASSERT_TRUE(SearchForOffset(index, key, restart_point));
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
