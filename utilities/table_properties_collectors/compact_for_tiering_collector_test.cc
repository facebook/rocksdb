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

class CompactForTieringCollectorTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<bool> {
 public:
  void CheckFileWriteTimePropertyWithOnlyInfinitelyOldEntries(
      bool track_write_time, UserCollectedProperties& user_properties,
      size_t infinitely_old_entries) {
    ASSERT_EQ(
        user_properties
            [CompactForTieringCollector::kNumInfinitelyOldEntriesPropertyName],
        track_write_time ? std::to_string(infinitely_old_entries) : "");
    ASSERT_EQ(user_properties[CompactForTieringCollector::
                                  kNumWriteTimeUntrackedEntriesPropertyName],
              track_write_time ? std::to_string(0) : "");
    ASSERT_EQ(user_properties[CompactForTieringCollector::
                                  kNumWriteTimeAggregatedEntriesPropertyName],
              track_write_time ? std::to_string(0) : "");
    ASSERT_EQ(
        user_properties
            [CompactForTieringCollector::kMinDataUnixWriteTimePropertyName],
        track_write_time
            ? std::to_string(TablePropertiesCollector::kUnknownUnixWriteTime)
            : "");
    ASSERT_EQ(
        user_properties
            [CompactForTieringCollector::kMaxDataUnixWriteTimePropertyName],
        track_write_time
            ? std::to_string(TablePropertiesCollector::kUnknownUnixWriteTime)
            : "");
    ASSERT_EQ(
        user_properties
            [CompactForTieringCollector::kAverageDataUnixWriteTimePropertyName],
        track_write_time
            ? std::to_string(TablePropertiesCollector::kUnknownUnixWriteTime)
            : "");
  }
};

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

INSTANTIATE_TEST_CASE_P(CompactForTieringCollectorTest,
                        CompactForTieringCollectorTest, ::testing::Bool());

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
  ASSERT_EQ(
      user_properties
          [CompactForTieringCollector::kNumInfinitelyOldEntriesPropertyName],
      std::to_string(1));
  ASSERT_EQ(user_properties[CompactForTieringCollector::
                                kNumWriteTimeUntrackedEntriesPropertyName],
            std::to_string(0));
  ASSERT_EQ(user_properties[CompactForTieringCollector::
                                kNumWriteTimeAggregatedEntriesPropertyName],
            std::to_string(99));
  ASSERT_EQ(user_properties
                [CompactForTieringCollector::kMinDataUnixWriteTimePropertyName],
            std::to_string(1));
  ASSERT_EQ(user_properties
                [CompactForTieringCollector::kMaxDataUnixWriteTimePropertyName],
            std::to_string(99));
  ASSERT_EQ(
      user_properties
          [CompactForTieringCollector::kAverageDataUnixWriteTimePropertyName],
      std::to_string(50));
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}