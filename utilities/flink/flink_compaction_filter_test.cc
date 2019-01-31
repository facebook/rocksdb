// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <random>
#include "util/testharness.h"
#include "utilities/flink/flink_compaction_filter.h"

namespace rocksdb {
namespace flink {

#define DISABLED FlinkCompactionFilter::StateType::Disabled
#define VALUE FlinkCompactionFilter::StateType::Value
#define LIST FlinkCompactionFilter::StateType::List

#define KVALUE CompactionFilter::ValueType::kValue
#define KMERGE CompactionFilter::ValueType::kMergeOperand
#define KBLOB CompactionFilter::ValueType::kBlobIndex

#define KKEEP CompactionFilter::Decision::kKeep
#define KREMOVE CompactionFilter::Decision::kRemove
#define KCHANGE CompactionFilter::Decision::kChangeValue

#define EXPIRE (time += ttl + 20)

#define EXPECT_ARR_EQ(arr1, arr2, num) EXPECT_TRUE( 0 == memcmp( arr1, arr2, num ) );

static const std::size_t TEST_TIMESTAMP_OFFSET = static_cast<std::size_t>(2);

static const std::size_t LIST_ELEM_FIXED_LEN = static_cast<std::size_t>(8 + 4);

static const int64_t QUERY_TIME_AFTER_NUM_ENTRIES = static_cast<int64_t>(10);

class ConsoleLogger : public Logger {
public:
  using Logger::Logv;
  ConsoleLogger() : Logger(InfoLogLevel::DEBUG_LEVEL) {}

  void Logv(const char* format, va_list ap) override {
    vprintf(format, ap);
    printf("\n");
  }
};

int64_t time = 0;

class TestTimeProvider : public FlinkCompactionFilter::TimeProvider {
public:
    int64_t CurrentTimestamp() const override {
      return time;
    }
};

std::random_device rd; // NOLINT
std::mt19937 mt(rd()); // NOLINT
std::uniform_int_distribution<int64_t> rnd(JAVA_MIN_LONG, JAVA_MAX_LONG); // NOLINT

int64_t ttl = 100;

Slice key = Slice("key"); // NOLINT
char data[24];
std::string new_list = ""; // NOLINT
std::string stub = ""; // NOLINT

FlinkCompactionFilter::StateType state_type;
CompactionFilter::ValueType value_type;
FlinkCompactionFilter* filter; // NOLINT

void SetTimestamp(int64_t timestamp, size_t offset = 0, char* value = data) {
  for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
    value[offset + i] = static_cast<char>(static_cast<uint64_t>(timestamp)
            >> ((sizeof(int64_t) - 1 - i) * BITS_PER_BYTE));
  }
}

CompactionFilter::Decision decide(size_t data_size = sizeof(data)) {
  return filter->FilterV2(0, key, value_type, Slice(data, data_size), &new_list, &stub);
}

void Init(FlinkCompactionFilter::StateType stype,
          CompactionFilter::ValueType vtype,
          FlinkCompactionFilter::ListElementFilterFactory* fixed_len_filter_factory,
          size_t timestamp_offset,
          bool expired = false) {
  time = expired ? time + ttl + 20 : time;
  state_type = stype;
  value_type = vtype;

  auto config_holder = std::make_shared<FlinkCompactionFilter::ConfigHolder>();
  auto time_provider = new TestTimeProvider();
  auto logger = std::make_shared<ConsoleLogger>();

  filter = new FlinkCompactionFilter(config_holder, std::unique_ptr<FlinkCompactionFilter::TimeProvider>(time_provider), logger);
  auto config = new FlinkCompactionFilter::Config{state_type, timestamp_offset, ttl, QUERY_TIME_AFTER_NUM_ENTRIES,
                                                  unique_ptr<FlinkCompactionFilter::ListElementFilterFactory>(fixed_len_filter_factory)};
  EXPECT_EQ(decide(), KKEEP); // test disabled config
  EXPECT_TRUE(config_holder->Configure(config));
  EXPECT_FALSE(config_holder->Configure(config));
}

void InitValue(FlinkCompactionFilter::StateType stype,
               CompactionFilter::ValueType vtype,
               bool expired = false,
               size_t timestamp_offset = TEST_TIMESTAMP_OFFSET) {
  time = rnd(mt);
  SetTimestamp(time, timestamp_offset);
  Init(stype, vtype, nullptr, timestamp_offset, expired);
}

void InitList(CompactionFilter::ValueType vtype,
              bool all_expired = false,
              bool first_elem_expired = false,
              size_t timestamp_offset = 0) {
  time = rnd(mt);
  SetTimestamp(first_elem_expired ? time - ttl - 20 : time, timestamp_offset); // elem 1 ts
  SetTimestamp(time, LIST_ELEM_FIXED_LEN + timestamp_offset); // elem 2 ts
  auto fixed_len_filter_factory =
          new FlinkCompactionFilter::FixedListElementFilterFactory(LIST_ELEM_FIXED_LEN, static_cast<std::size_t>(0));
  Init(LIST, vtype, fixed_len_filter_factory, timestamp_offset, all_expired);
}

void Deinit() {
  delete filter;
}

TEST(FlinkStateTtlTest, CheckStateTypeEnumOrder) { // NOLINT
  // if the order changes it also needs to be adjusted in Java client:
  // in org.rocksdb.FlinkCompactionFilter
  // and in org.rocksdb.FlinkCompactionFilterTest
  EXPECT_EQ(DISABLED, 0);
  EXPECT_EQ(VALUE, 1);
  EXPECT_EQ(LIST, 2);
}

TEST(FlinkStateTtlTest, SkipShortDataWithoutTimestamp) { // NOLINT
  InitValue(VALUE, KVALUE, true);
  EXPECT_EQ(decide(TIMESTAMP_BYTE_SIZE - 1), KKEEP);
  Deinit();
}

TEST(FlinkValueStateTtlTest, Unexpired) { // NOLINT
  InitValue(VALUE, KVALUE);
  EXPECT_EQ(decide(), KKEEP);
  Deinit();
}

TEST(FlinkValueStateTtlTest, Expired) { // NOLINT
  InitValue(VALUE, KVALUE, true);
  EXPECT_EQ(decide(), KREMOVE);
  Deinit();
}

TEST(FlinkValueStateTtlTest, CachedTimeUpdate) { // NOLINT
  InitValue(VALUE, KVALUE);
  EXPECT_EQ(decide(), KKEEP); // also implicitly cache current timestamp
  EXPIRE; // advance current timestamp to expire but cached should be used
  // QUERY_TIME_AFTER_NUM_ENTRIES - 2:
  // -1 -> for decide disabled in InitValue
  // and -1 -> for decide right after InitValue
  for (int64_t i = 0; i < QUERY_TIME_AFTER_NUM_ENTRIES - 2; i++) {
    EXPECT_EQ(decide(), KKEEP);
  }
  EXPECT_EQ(decide(), KREMOVE); // advanced current timestamp should be updated in cache and expire state
  Deinit();
}

TEST(FlinkValueStateTtlTest, WrongFilterValueType) { // NOLINT
  InitValue(VALUE, KMERGE, true);
  EXPECT_EQ(decide(), KKEEP);
  Deinit();
}

TEST(FlinkListStateTtlTest, Unexpired) { // NOLINT
  InitList(KMERGE);
  EXPECT_EQ(decide(), KKEEP);
  Deinit();

  InitList(KVALUE);
  EXPECT_EQ(decide(), KKEEP);
  Deinit();
}

TEST(FlinkListStateTtlTest, Expired) { // NOLINT
  InitList(KMERGE, true);
  EXPECT_EQ(decide(), KREMOVE);
  Deinit();

  InitList(KVALUE, true);
  EXPECT_EQ(decide(), KREMOVE);
  Deinit();
}

TEST(FlinkListStateTtlTest, HalfExpired) { // NOLINT
  InitList(KMERGE, false, true);
  EXPECT_EQ(decide(), KCHANGE);
  EXPECT_ARR_EQ(new_list.data(), data + LIST_ELEM_FIXED_LEN, LIST_ELEM_FIXED_LEN);
  Deinit();

  InitList(KVALUE, false, true);
  EXPECT_EQ(decide(), KCHANGE);
  EXPECT_ARR_EQ(new_list.data(), data + LIST_ELEM_FIXED_LEN, LIST_ELEM_FIXED_LEN);
  Deinit();
}

TEST(FlinkListStateTtlTest, WrongFilterValueType) { // NOLINT
  InitList(KBLOB, true);
  EXPECT_EQ(decide(), KKEEP);
  Deinit();
}

} // namespace flink
} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
