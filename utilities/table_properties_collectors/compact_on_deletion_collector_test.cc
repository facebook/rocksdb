//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>

#ifndef ROCKSDB_LITE
#include <algorithm>
#include <cmath>
#include <vector>

#include "port/stack_trace.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "utilities/table_properties_collectors/compact_on_deletion_collector.h"

namespace ROCKSDB_NAMESPACE {

TEST(CompactOnDeletionCollector, DeletionRatio) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id =
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
  const size_t kTotalEntries = 100;

  {
    // Disable deletion ratio.
    for (double deletion_ratio : {-1.5, -1.0, 0.0, 1.5, 2.0}) {
      auto factory = NewCompactOnDeletionCollectorFactory(0, 0, deletion_ratio);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      for (size_t i = 0; i < kTotalEntries; i++) {
        // All entries are deletion entries.
        ASSERT_OK(
            collector->AddUserKey("hello", "rocksdb", kEntryDelete, 0, 0));
        ASSERT_FALSE(collector->NeedCompact());
      }
      ASSERT_OK(collector->Finish(nullptr));
      ASSERT_FALSE(collector->NeedCompact());
    }
  }

  {
    for (double deletion_ratio : {0.3, 0.5, 0.8, 1.0}) {
      auto factory = NewCompactOnDeletionCollectorFactory(0, 0, deletion_ratio);
      const size_t deletion_entries_trigger =
          static_cast<size_t>(deletion_ratio * kTotalEntries);
      for (int delta : {-1, 0, 1}) {
        // Actual deletion entry ratio <, =, > deletion_ratio
        size_t actual_deletion_entries = deletion_entries_trigger + delta;
        std::unique_ptr<TablePropertiesCollector> collector(
            factory->CreateTablePropertiesCollector(context));
        for (size_t i = 0; i < kTotalEntries; i++) {
          if (i < actual_deletion_entries) {
            ASSERT_OK(
                collector->AddUserKey("hello", "rocksdb", kEntryDelete, 0, 0));
          } else {
            ASSERT_OK(
                collector->AddUserKey("hello", "rocksdb", kEntryPut, 0, 0));
          }
          ASSERT_FALSE(collector->NeedCompact());
        }
        ASSERT_OK(collector->Finish(nullptr));
        if (delta >= 0) {
          // >= deletion_ratio
          ASSERT_TRUE(collector->NeedCompact());
        } else {
          ASSERT_FALSE(collector->NeedCompact());
        }
      }
    }
  }
}

TEST(CompactOnDeletionCollector, SlidingWindow) {
  const int kWindowSizes[] =
      {1000, 10000, 10000, 127, 128, 129, 255, 256, 257, 2, 10000};
  const int kDeletionTriggers[] =
      {500, 9500, 4323, 47, 61, 128, 250, 250, 250, 2, 2};
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id =
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;

  std::vector<int> window_sizes;
  std::vector<int> deletion_triggers;
  // deterministic tests
  for (int test = 0; test < 9; ++test) {
    window_sizes.emplace_back(kWindowSizes[test]);
    deletion_triggers.emplace_back(kDeletionTriggers[test]);
  }

  // randomize tests
  Random rnd(301);
  const int kMaxTestSize = 100000l;
  for (int random_test = 0; random_test < 10; random_test++) {
    int window_size = rnd.Uniform(kMaxTestSize) + 1;
    int deletion_trigger = rnd.Uniform(window_size);
    window_sizes.emplace_back(window_size);
    deletion_triggers.emplace_back(deletion_trigger);
  }

  assert(window_sizes.size() == deletion_triggers.size());

  for (size_t test = 0; test < window_sizes.size(); ++test) {
    const int kBucketSize = 128;
    const int kWindowSize = window_sizes[test];
    const int kPaddedWindowSize =
        kBucketSize * ((window_sizes[test] + kBucketSize - 1) / kBucketSize);
    const int kNumDeletionTrigger = deletion_triggers[test];
    const int kBias = (kNumDeletionTrigger + kBucketSize - 1) / kBucketSize;
    // Simple test
    {
      auto factory = NewCompactOnDeletionCollectorFactory(kWindowSize,
                                                          kNumDeletionTrigger);
      const int kSample = 10;
      for (int delete_rate = 0; delete_rate <= kSample; ++delete_rate) {
        std::unique_ptr<TablePropertiesCollector> collector(
            factory->CreateTablePropertiesCollector(context));
        int deletions = 0;
        for (int i = 0; i < kPaddedWindowSize; ++i) {
          if (i % kSample < delete_rate) {
            ASSERT_OK(
                collector->AddUserKey("hello", "rocksdb", kEntryDelete, 0, 0));
            deletions++;
          } else {
            ASSERT_OK(
                collector->AddUserKey("hello", "rocksdb", kEntryPut, 0, 0));
          }
        }
        if (collector->NeedCompact() !=
            (deletions >= kNumDeletionTrigger) &&
            std::abs(deletions - kNumDeletionTrigger) > kBias) {
          fprintf(stderr, "[Error] collector->NeedCompact() != (%d >= %d)"
                  " with kWindowSize = %d and kNumDeletionTrigger = %d\n",
                  deletions, kNumDeletionTrigger,
                  kWindowSize, kNumDeletionTrigger);
          ASSERT_TRUE(false);
        }
        ASSERT_OK(collector->Finish(nullptr));
      }
    }

    // Only one section of a file satisfies the compaction trigger
    {
      auto factory = NewCompactOnDeletionCollectorFactory(kWindowSize,
                                                          kNumDeletionTrigger);
      const int kSample = 10;
      for (int delete_rate = 0; delete_rate <= kSample; ++delete_rate) {
        std::unique_ptr<TablePropertiesCollector> collector(
            factory->CreateTablePropertiesCollector(context));
        int deletions = 0;
        for (int section = 0; section < 5; ++section) {
          int initial_entries = rnd.Uniform(kWindowSize) + kWindowSize;
          for (int i = 0; i < initial_entries; ++i) {
            ASSERT_OK(
                collector->AddUserKey("hello", "rocksdb", kEntryPut, 0, 0));
          }
        }
        for (int i = 0; i < kPaddedWindowSize; ++i) {
          if (i % kSample < delete_rate) {
            ASSERT_OK(
                collector->AddUserKey("hello", "rocksdb", kEntryDelete, 0, 0));
            deletions++;
          } else {
            ASSERT_OK(
                collector->AddUserKey("hello", "rocksdb", kEntryPut, 0, 0));
          }
        }
        for (int section = 0; section < 5; ++section) {
          int ending_entries = rnd.Uniform(kWindowSize) + kWindowSize;
          for (int i = 0; i < ending_entries; ++i) {
            ASSERT_OK(
                collector->AddUserKey("hello", "rocksdb", kEntryPut, 0, 0));
          }
        }
        if (collector->NeedCompact() != (deletions >= kNumDeletionTrigger) &&
            std::abs(deletions - kNumDeletionTrigger) > kBias) {
          fprintf(stderr, "[Error] collector->NeedCompact() %d != (%d >= %d)"
                  " with kWindowSize = %d, kNumDeletionTrigger = %d\n",
                  collector->NeedCompact(),
                  deletions, kNumDeletionTrigger, kWindowSize,
                  kNumDeletionTrigger);
          ASSERT_TRUE(false);
        }
        ASSERT_OK(collector->Finish(nullptr));
      }
    }

    // TEST 3:  Issues a lots of deletes, but their density is not
    // high enough to trigger compaction.
    {
      std::unique_ptr<TablePropertiesCollector> collector;
      auto factory = NewCompactOnDeletionCollectorFactory(kWindowSize,
                                                          kNumDeletionTrigger);
      collector.reset(factory->CreateTablePropertiesCollector(context));
      assert(collector->NeedCompact() == false);
      // Insert "kNumDeletionTrigger * 0.95" deletions for every
      // "kWindowSize" and verify compaction is not needed.
      const int kDeletionsPerSection = kNumDeletionTrigger * 95 / 100;
      if (kDeletionsPerSection >= 0) {
        for (int section = 0; section < 200; ++section) {
          for (int i = 0; i < kPaddedWindowSize; ++i) {
            if (i < kDeletionsPerSection) {
              ASSERT_OK(collector->AddUserKey("hello", "rocksdb", kEntryDelete,
                                              0, 0));
            } else {
              ASSERT_OK(
                  collector->AddUserKey("hello", "rocksdb", kEntryPut, 0, 0));
            }
          }
        }
        if (collector->NeedCompact() &&
            std::abs(kDeletionsPerSection - kNumDeletionTrigger) > kBias) {
          fprintf(stderr, "[Error] collector->NeedCompact() != false"
                  " with kWindowSize = %d and kNumDeletionTrigger = %d\n",
                  kWindowSize, kNumDeletionTrigger);
          ASSERT_TRUE(false);
        }
        ASSERT_OK(collector->Finish(nullptr));
      }
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#else
int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as RocksDBLite does not include utilities.\n");
  return 0;
}
#endif  // !ROCKSDB_LITE
