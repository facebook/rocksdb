// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <random>
#include "util/testharness.h"
#include "utilities/flink/flink_compaction_filter.h"

namespace rocksdb {
namespace flink {

#define VALUE FlinkCompactionFilter::StateType::Value
#define LIST FlinkCompactionFilter::StateType::List
#define MAP FlinkCompactionFilter::StateType::Map
#define DISABLED FlinkCompactionFilter::StateType::Disabled

#define KVALUE CompactionFilter::ValueType::kValue
#define KMERGE CompactionFilter::ValueType::kMergeOperand

#define KKEEP CompactionFilter::Decision::kKeep
#define KREMOVE CompactionFilter::Decision::kRemove

#define EXPIRE (time += ttl + 20)

#define FLINK_MAP_STATE_NULL_BYTE_OFFSET 1

std::random_device rd; // NOLINT
std::mt19937 mt(rd()); // NOLINT
std::uniform_int_distribution<int64_t> rnd(0, JAVA_MAX_LONG); // NOLINT

int64_t time = 0;
int64_t ttl = 100;

Slice key = Slice("key"); // NOLINT
char data[16];
std::string stub = ""; // NOLINT

FlinkCompactionFilter::StateType state_type;
CompactionFilter::ValueType value_type;
FlinkCompactionFilter filter; // NOLINT

void SetTimestamp(int64_t timestamp = time, size_t offset = 0, char* value = data) {
  time = timestamp;
  for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
    value[offset + i] = static_cast<char>(static_cast<uint64_t>(timestamp)
            >> ((sizeof(int64_t) - 1 - i) * BITS_PER_BYTE));
  }
}

void init(FlinkCompactionFilter::StateType stype, CompactionFilter::ValueType vtype, size_t timestamp_offset = 0) {
  SetTimestamp(rnd(mt), timestamp_offset);
  state_type = stype;
  value_type = vtype;
  
  filter.Configure(new FlinkCompactionFilter::Config{state_type, timestamp_offset, ttl, false});
}

CompactionFilter::Decision decide(size_t data_size = sizeof(data)) {
  filter.SetCurrentTimestamp(time);
  return filter.FilterV2(0, key, value_type, Slice(data, data_size), &stub, &stub);
}

TEST(FlinkStateTtlTest, CheckStateTypeEnumOrder) { // NOLINT
  // if the order changes it also needs to be adjusted in Java client:
  // in org.rocksdb.FlinkCompactionFilter
  // and in org.rocksdb.FlinkCompactionFilterTest
  EXPECT_EQ(VALUE, 0);
  EXPECT_EQ(LIST, 1);
  EXPECT_EQ(MAP, 2);
  EXPECT_EQ(DISABLED, 3);
}

TEST(FlinkStateTtlTest, SkipShortDataWithoutTimestamp) { // NOLINT
  init(VALUE, KVALUE);
  EXPIRE;
  EXPECT_EQ(decide(TIMESTAMP_BYTE_SIZE - 1), KKEEP);
}

TEST(FlinkValueStateTtlTest, Unexpired) { // NOLINT
  init(VALUE, KVALUE);
  EXPECT_EQ(decide(), KKEEP);
}

TEST(FlinkValueStateTtlTest, Expired) { // NOLINT
  init(VALUE, KVALUE);
  EXPIRE;
  EXPECT_EQ(decide(), KREMOVE);
}

TEST(FlinkValueStateTtlTest, WrongFilterValueType) { // NOLINT
  init(VALUE, KMERGE);
  EXPIRE;
  EXPECT_EQ(decide(), KKEEP);
}

TEST(FlinkListStateTtlTest, Unexpired) { // NOLINT
  init(LIST, KMERGE);
  EXPECT_EQ(decide(), KKEEP);
}

TEST(FlinkListStateTtlTest, Expired) { // NOLINT
  init(LIST, KMERGE);
  EXPIRE;
  EXPECT_EQ(decide(), KREMOVE);
}

TEST(FlinkListStateTtlTest, WrongFilterValueType) { // NOLINT
  init(LIST, KVALUE);
  EXPIRE;
  EXPECT_EQ(decide(), KKEEP);
}

TEST(FlinkMapStateTtlTest, Unexpired) { // NOLINT
  init(MAP, KVALUE, FLINK_MAP_STATE_NULL_BYTE_OFFSET);
  EXPECT_EQ(decide(), KKEEP);
}

TEST(FlinkMapStateTtlTest, Expired) { // NOLINT
  init(MAP, KVALUE, FLINK_MAP_STATE_NULL_BYTE_OFFSET);
  EXPIRE;
  EXPECT_EQ(decide(), KREMOVE);
}

TEST(FlinkMapStateTtlTest, WrongFilterValueType) { // NOLINT
  init(MAP, KMERGE, FLINK_MAP_STATE_NULL_BYTE_OFFSET);
  EXPIRE;
  EXPECT_EQ(decide(), KKEEP);
}

} // namespace flink
} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
