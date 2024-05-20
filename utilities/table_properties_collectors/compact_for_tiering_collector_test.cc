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

TEST(CompactForTieringCollector, NotEnabled) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  const size_t kTotalEntries = 100;

  // When not enabled, collectors are no op.
  {
    for (size_t compaction_trigger : {0, 50, 100}) {
      auto factory = NewCompactForTieringCollectorFactory(
          {{1, compaction_trigger}}, /*enabled=*/false);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      for (size_t i = 0; i < kTotalEntries; i++) {
        ASSERT_OK(collector->AddUserKey("hello", "rocksdb", kEntryPut, i, 0));
        ASSERT_FALSE(collector->NeedCompact());
      }
      UserCollectedProperties user_properties;
      ASSERT_OK(collector->Finish(&user_properties));
      ASSERT_EQ(
          user_properties.find(CompactForTieringCollector::
                                   kNumEligibleLastLevelEntriesPropertyName),
          user_properties.end());
      ASSERT_FALSE(collector->NeedCompact());
    }
  }
}

TEST(CompactForTieringCollector, TieringDisabled) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = kMaxSequenceNumber;
  const size_t kTotalEntries = 100;

  // Tiering is disabled on the column family. No stats related to tiering is
  // collected, nor will the file be marked for need compaction regardless of
  // the configured compaction trigger (likely mistakenly).
  {
    for (size_t compaction_trigger : {0, 50, 100}) {
      auto factory = NewCompactForTieringCollectorFactory(
          {{1, compaction_trigger}}, /*enabled=*/true);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      for (size_t i = 0; i < kTotalEntries; i++) {
        ASSERT_OK(collector->AddUserKey("hello", "rocksdb", kEntryPut, i, 0));
        ASSERT_FALSE(collector->NeedCompact());
      }
      UserCollectedProperties user_properties;
      ASSERT_OK(collector->Finish(&user_properties));
      ASSERT_EQ(
          user_properties.find(CompactForTieringCollector::
                                   kNumEligibleLastLevelEntriesPropertyName),
          user_properties.end());
      ASSERT_FALSE(collector->NeedCompact());
    }
  }
}

TEST(CompactForTieringCollector, LastLevelFile) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 5;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  const size_t kTotalEntries = 100;

  // No stats are collected for a file that is already on the last level, nor
  // will the file be marked as need compaction.
  {
    for (size_t compaction_trigger : {0, 50, 100}) {
      auto factory = NewCompactForTieringCollectorFactory(
          {{1, compaction_trigger}}, /*enabled=*/true);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      for (size_t i = 0; i < kTotalEntries; i++) {
        ASSERT_OK(collector->AddUserKey("hello", "rocksdb", kEntryPut, 0, 0));
        ASSERT_FALSE(collector->NeedCompact());
      }
      UserCollectedProperties user_properties;
      ASSERT_OK(collector->Finish(&user_properties));
      ASSERT_EQ(
          user_properties.find(CompactForTieringCollector::
                                   kNumEligibleLastLevelEntriesPropertyName),
          user_properties.end());
      ASSERT_FALSE(collector->NeedCompact());
    }
  }
}

TEST(CompactForTieringCollector, CompactionTriggerNotSet) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  const size_t kTotalEntries = 100;

  // No compaction trigger explicitly set for this column family.
  auto factory =
      NewCompactForTieringCollectorFactory({{2, 15}}, /*enabled=*/true);
  {
    std::unique_ptr<TablePropertiesCollector> collector(
        factory->CreateTablePropertiesCollector(context));
    for (size_t i = 0; i < kTotalEntries; i++) {
      ASSERT_OK(collector->AddUserKey("hello", "rocksdb", kEntryPut, i, 0));
      ASSERT_FALSE(collector->NeedCompact());
    }
    // User property written to sst file, file not marked for compaction.
    UserCollectedProperties user_properties;
    ASSERT_OK(collector->Finish(&user_properties));
    ASSERT_EQ(std::to_string(50),
              user_properties[CompactForTieringCollector::
                                  kNumEligibleLastLevelEntriesPropertyName]);
    ASSERT_FALSE(collector->NeedCompact());
  }

  // Compaction trigger explicitly set to 0.
  factory->SetCompactionTrigger(1, 0);
  {
    std::unique_ptr<TablePropertiesCollector> collector(
        factory->CreateTablePropertiesCollector(context));
    for (size_t i = 0; i < kTotalEntries; i++) {
      ASSERT_OK(collector->AddUserKey("hello", "rocksdb", kEntryPut, i, 0));
      ASSERT_FALSE(collector->NeedCompact());
    }

    // User property written to sst file, file not marked for compaction.
    UserCollectedProperties user_properties;
    ASSERT_OK(collector->Finish(&user_properties));
    ASSERT_EQ(std::to_string(50),
              user_properties[CompactForTieringCollector::
                                  kNumEligibleLastLevelEntriesPropertyName]);
    ASSERT_FALSE(collector->NeedCompact());
  }
}

TEST(CompactForTieringCollector, MarkForCompaction) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  const size_t kTotalEntries = 100;

  {
    for (size_t compaction_trigger : {0, 1, 50, 100}) {
      auto factory = NewCompactForTieringCollectorFactory(
          {{1, compaction_trigger}}, /*enabled=*/true);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      for (size_t i = 0; i < kTotalEntries; i++) {
        ASSERT_OK(collector->AddUserKey("hello", "rocksdb", kEntryPut, i, 0));
        ASSERT_FALSE(collector->NeedCompact());
      }
      UserCollectedProperties user_properties;
      ASSERT_OK(collector->Finish(&user_properties));
      ASSERT_EQ(user_properties[CompactForTieringCollector::
                                    kNumEligibleLastLevelEntriesPropertyName],
                std::to_string(50));
      if (compaction_trigger == 0 || compaction_trigger > 50) {
        ASSERT_FALSE(collector->NeedCompact());
      } else {
        ASSERT_TRUE(collector->NeedCompact());
      }
    }
  }
}

TEST(CompactForTieringCollector, TimedPutEntries) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  const size_t kTotalEntries = 100;

  auto factory =
      NewCompactForTieringCollectorFactory({{1, 50}}, /*enabled=*/true);
  std::unique_ptr<TablePropertiesCollector> collector(
      factory->CreateTablePropertiesCollector(context));
  for (size_t i = 0; i < kTotalEntries; i++) {
    std::string value;
    PackValueAndSeqno("rocksdb", i, &value);
    ASSERT_OK(collector->AddUserKey("hello", value, kEntryTimedPut, 0, 0));
    ASSERT_FALSE(collector->NeedCompact());
  }
  UserCollectedProperties user_properties;
  ASSERT_OK(collector->Finish(&user_properties));
  ASSERT_EQ(user_properties[CompactForTieringCollector::
                                kNumEligibleLastLevelEntriesPropertyName],
            std::to_string(50));
  ASSERT_TRUE(collector->NeedCompact());
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}