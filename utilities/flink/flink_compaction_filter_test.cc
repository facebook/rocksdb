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

#define KVALUE CompactionFilter::ValueType::kValue
#define KMERGE CompactionFilter::ValueType::kMergeOperand

#define KKEEP CompactionFilter::Decision::kKeep
#define KREMOVE CompactionFilter::Decision::kRemove

#define EXPIRE (time += ttl + 20)

std::random_device rd; // NOLINT
std::mt19937 mt(rd()); // NOLINT
std::uniform_int_distribution<int64_t> rnd(0, JAVA_MAX_LONG); // NOLINT

int64_t time = 0;
int64_t ttl = 100;

class TestTimeProvider : public TimeProvider {
public:
  int64_t CurrentTimestamp() const override { return time; }
};

Slice key = Slice("key"); // NOLINT
char data[16];
std::string stub = ""; // NOLINT

FlinkCompactionFilter::StateType state_type;
CompactionFilter::ValueType value_type;

void SetTimestamp(int64_t timestamp = time, int offset = 0, char* value = data) {
  time = timestamp;
  for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
    value[offset + i] = static_cast<char>(static_cast<uint64_t>(timestamp)
            >> ((sizeof(int64_t) - 1 - i) * BITS_PER_BYTE));
  }
}

FlinkCompactionFilter init(FlinkCompactionFilter::StateType stype, CompactionFilter::ValueType vtype, int offset = 0) {
  SetTimestamp(rnd(mt), offset);
  state_type = stype;
  value_type = vtype;
  return FlinkCompactionFilter(state_type, ttl, new TestTimeProvider());
}

CompactionFilter::Decision decide(FlinkCompactionFilter& filter, size_t data_size = sizeof(data)) {
  return filter.FilterV2(0, key, value_type, Slice(data, data_size), &stub, &stub);
}

TEST(FlinkStateTtlTest, CheckStateTypeEnumOrder) { // NOLINT
  // if the order changes it also needs to be adjusted in Java client:
  // in org.rocksdb.FlinkCompactionFilter
  // and in org.rocksdb.FlinkCompactionFilterTest
  EXPECT_EQ(VALUE, 0);
  EXPECT_EQ(LIST, 1);
  EXPECT_EQ(MAP, 2);
}

TEST(FlinkStateTtlTest, SkipShortDataWithoutTimestamp) { // NOLINT
  auto filter = init(VALUE, KVALUE);
  EXPIRE;
  EXPECT_EQ(decide(filter, TIMESTAMP_BYTE_SIZE - 1), KKEEP);
}

TEST(FlinkValueStateTtlTest, Unexpired) { // NOLINT
  auto filter = init(VALUE, KVALUE);
  EXPECT_EQ(decide(filter), KKEEP);
}

TEST(FlinkValueStateTtlTest, Expired) { // NOLINT
  auto filter = init(VALUE, KVALUE);
  EXPIRE;
  EXPECT_EQ(decide(filter), KREMOVE);
}

TEST(FlinkValueStateTtlTest, WrongFilterValueType) { // NOLINT
  auto filter = init(VALUE, KMERGE);
  EXPIRE;
  EXPECT_EQ(decide(filter), KKEEP);
}

TEST(FlinkListStateTtlTest, Unexpired) { // NOLINT
  auto filter = init(LIST, KMERGE);
  EXPECT_EQ(decide(filter), KKEEP);
}

TEST(FlinkListStateTtlTest, Expired) { // NOLINT
  auto filter = init(LIST, KMERGE);
  EXPIRE;
  EXPECT_EQ(decide(filter), KREMOVE);
}

TEST(FlinkListStateTtlTest, WrongFilterValueType) { // NOLINT
  auto filter = init(LIST, KVALUE);
  EXPIRE;
  EXPECT_EQ(decide(filter), KKEEP);
}

TEST(FlinkMapStateTtlTest, Unexpired) { // NOLINT
  auto filter = init(MAP, KVALUE, FLINK_MAP_STATE_NULL_BYTE_OFFSET);
  EXPECT_EQ(decide(filter), KKEEP);
}

TEST(FlinkMapStateTtlTest, Expired) { // NOLINT
  auto filter = init(MAP, KVALUE, FLINK_MAP_STATE_NULL_BYTE_OFFSET);
  EXPIRE;
  EXPECT_EQ(decide(filter), KREMOVE);
}

TEST(FlinkMapStateTtlTest, WrongFilterValueType) { // NOLINT
  auto filter = init(MAP, KMERGE, FLINK_MAP_STATE_NULL_BYTE_OFFSET);
  EXPIRE;
  EXPECT_EQ(decide(filter), KKEEP);
}

} // namespace flink
} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
