//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/table_properties_collectors/compact_for_tiering_collector.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <iostream>
#include <vector>

#include "db/seqno_to_time_mapping.h"
#include "port/stack_trace.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "test_util/testharness.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

// Test param:
// 1) whether to enable write time tracking.
class CompactForTieringCollectorTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<bool> {
 public:
  void CheckFileWriteTimePropertyWithOnlyInfinitelyOldEntries(
      bool track_write_time, const UserCollectedProperties& user_properties,
      size_t infinitely_old_entries) {
    std::unique_ptr<DataCollectionUnixWriteTimeInfo> write_time_info;
    Status status = CompactForTieringCollector::
        GetDataCollectionUnixWriteTimeInfoFromUserProperties(user_properties,
                                                             &write_time_info);
    if (!track_write_time) {
      ASSERT_TRUE(status.IsInvalidArgument());
      return;
    }
    ASSERT_OK(status);

    DataCollectionUnixWriteTimeInfo expected_write_time(
        /* _min_write_time= */ TablePropertiesCollector::kUnknownUnixWriteTime,
        /* _max_write_time= */ TablePropertiesCollector::kUnknownUnixWriteTime,
        /* _average_write_time= */
        TablePropertiesCollector::kUnknownUnixWriteTime,
        static_cast<uint64_t>(infinitely_old_entries),
        /* _num_entries_write_time_aggregated= */ 0,
        /* _num_entries_write_time_untracked= */ 0);
    ASSERT_EQ(expected_write_time, *write_time_info);
  }
};

INSTANTIATE_TEST_CASE_P(CompactForTieringCollectorTest,
                        CompactForTieringCollectorTest, ::testing::Bool());

TEST_P(CompactForTieringCollectorTest, CompactionTriggeringNotEnabled) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  bool track_write_time = GetParam();

  // Set compaction trigger ratio to 0 to disable it. No collector created.
  auto factory = NewCompactForTieringCollectorFactory(0, track_write_time);
  std::unique_ptr<TablePropertiesCollector> collector(
      factory->CreateTablePropertiesCollector(context));
  if (!track_write_time) {
    ASSERT_EQ(nullptr, collector);
  } else {
    ASSERT_NE(nullptr, collector);
  }
}

TEST_P(CompactForTieringCollectorTest, TieringDisabled) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = kMaxSequenceNumber;
  bool track_write_time = GetParam();

  // Tiering is disabled on the column family. No collector created.
  {
    for (double compaction_trigger_ratio : {0.0, 0.1, 1.0, 1.5}) {
      auto factory = NewCompactForTieringCollectorFactory(
          compaction_trigger_ratio, track_write_time);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      if (!track_write_time) {
        ASSERT_EQ(nullptr, collector);
      } else {
        ASSERT_NE(nullptr, collector);
      }
    }
  }
}

TEST_P(CompactForTieringCollectorTest, LastLevelFile) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 5;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  bool track_write_time = GetParam();

  // No collector created for a file that is already on the last level.
  {
    for (double compaction_trigger_ratio : {0.0, 0.1, 1.0, 1.5}) {
      auto factory = NewCompactForTieringCollectorFactory(
          compaction_trigger_ratio, track_write_time);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      if (!track_write_time) {
        ASSERT_EQ(nullptr, collector);
      } else {
        ASSERT_NE(nullptr, collector);
      }
    }
  }
}

TEST_P(CompactForTieringCollectorTest, CollectorEnabled) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  const size_t kTotalEntries = 100;
  bool track_write_time = GetParam();

  {
    for (double compaction_trigger_ratio : {0.1, 0.33333333, 0.5, 1.0, 1.5}) {
      auto factory = NewCompactForTieringCollectorFactory(
          compaction_trigger_ratio, track_write_time);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      for (size_t i = 0; i < kTotalEntries; i++) {
        ASSERT_OK(collector->AddUserKeyWithWriteTime("hello", "rocksdb",
                                                     kEntryPut, i, 0, 0));
        ASSERT_FALSE(collector->NeedCompact());
      }
      UserCollectedProperties user_properties;
      ASSERT_OK(collector->Finish(&user_properties));
      ASSERT_EQ(user_properties[CompactForTieringCollector::
                                    kNumEligibleLastLevelEntriesPropertyName],
                std::to_string(50));
      if (compaction_trigger_ratio > 0.5) {
        ASSERT_FALSE(collector->NeedCompact());
      } else {
        ASSERT_TRUE(collector->NeedCompact());
      }

      CheckFileWriteTimePropertyWithOnlyInfinitelyOldEntries(
          track_write_time, user_properties, kTotalEntries);
    }
  }
}

TEST_P(CompactForTieringCollectorTest, TimedPutEntries) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  const size_t kTotalEntries = 100;
  bool track_write_time = GetParam();

  auto factory = NewCompactForTieringCollectorFactory(0.1, track_write_time);
  std::unique_ptr<TablePropertiesCollector> collector(
      factory->CreateTablePropertiesCollector(context));
  for (size_t i = 0; i < kTotalEntries; i++) {
    std::string value;
    PackValueAndSeqno("rocksdb", i, &value);
    ASSERT_OK(collector->AddUserKeyWithWriteTime("hello", value, kEntryTimedPut,
                                                 0, 0, 0));
    ASSERT_FALSE(collector->NeedCompact());
  }
  UserCollectedProperties user_properties;
  ASSERT_OK(collector->Finish(&user_properties));
  ASSERT_EQ(user_properties[CompactForTieringCollector::
                                kNumEligibleLastLevelEntriesPropertyName],
            std::to_string(50));
  ASSERT_TRUE(collector->NeedCompact());

  CheckFileWriteTimePropertyWithOnlyInfinitelyOldEntries(
      track_write_time, user_properties, kTotalEntries);
}

TEST(CompactForTieringCollector, AggregateTimeTest) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  const size_t kTotalEntries = 100;

  auto factory = NewCompactForTieringCollectorFactory(0, true);
  std::unique_ptr<TablePropertiesCollector> collector(
      factory->CreateTablePropertiesCollector(context));
  for (size_t i = 0; i < kTotalEntries; i++) {
    ASSERT_OK(collector->AddUserKeyWithWriteTime(
        "hello", "value", kEntryPut, 0,
        /*unix_write_time*/ static_cast<uint64_t>(i), 0));
  }
  UserCollectedProperties user_properties;
  ASSERT_OK(collector->Finish(&user_properties));

  std::unique_ptr<DataCollectionUnixWriteTimeInfo> write_time_info;
  ASSERT_OK(CompactForTieringCollector::
                GetDataCollectionUnixWriteTimeInfoFromUserProperties(
                    user_properties, &write_time_info));

  DataCollectionUnixWriteTimeInfo expected_write_time(
      /* _min_write_time= */ 1,
      /* _max_write_time= */ 99,
      /* _average_write_time= */ 50,
      /* _num_entries_infinitely_old= */ 1,
      /* _num_entries_write_time_aggregated= */ 99,
      /* _num_entries_write_time_untracked= */ 0);
  ASSERT_EQ(expected_write_time, *write_time_info);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}