//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <unordered_set>
#include <vector>

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

#ifndef ROCKSDB_LITE

namespace ROCKSDB_NAMESPACE {

// A helper function that ensures the table properties returned in
// `GetPropertiesOfAllTablesTest` is correct.
// This test assumes entries size is different for each of the tables.
namespace {

void VerifyTableProperties(DB* db, uint64_t expected_entries_size) {
  TablePropertiesCollection props;
  ASSERT_OK(db->GetPropertiesOfAllTables(&props));

  ASSERT_EQ(4U, props.size());
  std::unordered_set<uint64_t> unique_entries;

  // Indirect test
  uint64_t sum = 0;
  for (const auto& item : props) {
    unique_entries.insert(item.second->num_entries);
    sum += item.second->num_entries;
  }

  ASSERT_EQ(props.size(), unique_entries.size());
  ASSERT_EQ(expected_entries_size, sum);

  VerifySstUniqueIds(props);
}
}  // namespace

class DBTablePropertiesTest : public DBTestBase,
                              public testing::WithParamInterface<std::string> {
 public:
  DBTablePropertiesTest()
      : DBTestBase("db_table_properties_test", /*env_do_fsync=*/false) {}
  TablePropertiesCollection TestGetPropertiesOfTablesInRange(
      std::vector<Range> ranges, std::size_t* num_properties = nullptr,
      std::size_t* num_files = nullptr);
};

TEST_F(DBTablePropertiesTest, GetPropertiesOfAllTablesTest) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 8;
  // Part of strategy to prevent pinning table files
  options.max_open_files = 42;
  Reopen(options);

  // Create 4 tables
  for (int table = 0; table < 4; ++table) {
    // Use old meta name for table properties for one file
    if (table == 3) {
      SyncPoint::GetInstance()->SetCallBack(
          "BlockBasedTableBuilder::WritePropertiesBlock:Meta", [&](void* meta) {
            *reinterpret_cast<const std::string**>(meta) =
                &kPropertiesBlockOldName;
          });
      SyncPoint::GetInstance()->EnableProcessing();
    }
    // Build file
    for (int i = 0; i < 10 + table; ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), ToString(table * 100 + i), "val"));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  std::string original_session_id;
  ASSERT_OK(db_->GetDbSessionId(original_session_id));

  // Part of strategy to prevent pinning table files
  SyncPoint::GetInstance()->SetCallBack(
      "VersionEditHandler::LoadTables:skip_load_table_files",
      [&](void* skip_load) { *reinterpret_cast<bool*>(skip_load) = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // 1. Read table properties directly from file
  Reopen(options);
  // Clear out auto-opened files
  dbfull()->TEST_table_cache()->EraseUnRefEntries();
  ASSERT_EQ(dbfull()->TEST_table_cache()->GetUsage(), 0U);
  VerifyTableProperties(db_, 10 + 11 + 12 + 13);

  // 2. Put two tables to table cache and
  Reopen(options);
  // Clear out auto-opened files
  dbfull()->TEST_table_cache()->EraseUnRefEntries();
  ASSERT_EQ(dbfull()->TEST_table_cache()->GetUsage(), 0U);
  // fetch key from 1st and 2nd table, which will internally place that table to
  // the table cache.
  for (int i = 0; i < 2; ++i) {
    Get(ToString(i * 100 + 0));
  }

  VerifyTableProperties(db_, 10 + 11 + 12 + 13);

  // 3. Put all tables to table cache
  Reopen(options);
  // fetch key from all tables, which will place them in table cache.
  for (int i = 0; i < 4; ++i) {
    Get(ToString(i * 100 + 0));
  }
  VerifyTableProperties(db_, 10 + 11 + 12 + 13);

  // 4. Try to read CORRUPT properties (a) directly from file, and (b)
  // through reader on Get

  // It's not practical to prevent table file read on Open, so we
  // corrupt after open and after purging table cache.
  for (bool direct : {true, false}) {
    Reopen(options);
    // Clear out auto-opened files
    dbfull()->TEST_table_cache()->EraseUnRefEntries();
    ASSERT_EQ(dbfull()->TEST_table_cache()->GetUsage(), 0U);

    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    std::string sst_file = props.begin()->first;

    // Corrupt the file's TableProperties using session id
    std::string contents;
    ASSERT_OK(
        ReadFileToString(env_->GetFileSystem().get(), sst_file, &contents));
    size_t pos = contents.find(original_session_id);
    ASSERT_NE(pos, std::string::npos);
    ASSERT_OK(test::CorruptFile(env_, sst_file, static_cast<int>(pos), 1,
                                /*verify checksum fails*/ false));

    // Try to read CORRUPT properties
    if (direct) {
      ASSERT_TRUE(db_->GetPropertiesOfAllTables(&props).IsCorruption());
    } else {
      bool found_corruption = false;
      for (int i = 0; i < 4; ++i) {
        std::string result = Get(ToString(i * 100 + 0));
        if (result.find_first_of("Corruption: block checksum mismatch") !=
            std::string::npos) {
          found_corruption = true;
        }
      }
      ASSERT_TRUE(found_corruption);
    }

    // UN-corrupt file for next iteration
    ASSERT_OK(test::CorruptFile(env_, sst_file, static_cast<int>(pos), 1,
                                /*verify checksum fails*/ false));
  }

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTablePropertiesTest, InvalidIgnored) {
  // RocksDB versions 2.5 - 2.7 generate some properties that Block considers
  // invalid in some way. This approximates that.

  // Inject properties block data that Block considers invalid
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WritePropertiesBlock:BlockData",
      [&](void* block_data) {
        *reinterpret_cast<Slice*>(block_data) = Slice("X");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Build file
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), ToString(i), "val"));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  SyncPoint::GetInstance()->DisableProcessing();

  // Not crashing is good enough
  TablePropertiesCollection props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
}

TEST_F(DBTablePropertiesTest, CreateOnDeletionCollectorFactory) {
  ConfigOptions options;
  options.ignore_unsupported_options = false;

  std::shared_ptr<TablePropertiesCollectorFactory> factory;
  std::string id = CompactOnDeletionCollectorFactory::kClassName();
  ASSERT_OK(
      TablePropertiesCollectorFactory::CreateFromString(options, id, &factory));
  auto del_factory = factory->CheckedCast<CompactOnDeletionCollectorFactory>();
  ASSERT_NE(del_factory, nullptr);
  ASSERT_EQ(0U, del_factory->GetWindowSize());
  ASSERT_EQ(0U, del_factory->GetDeletionTrigger());
  ASSERT_EQ(0.0, del_factory->GetDeletionRatio());
  ASSERT_OK(TablePropertiesCollectorFactory::CreateFromString(
      options, "window_size=100; deletion_trigger=90; id=" + id, &factory));
  del_factory = factory->CheckedCast<CompactOnDeletionCollectorFactory>();
  ASSERT_NE(del_factory, nullptr);
  ASSERT_EQ(100U, del_factory->GetWindowSize());
  ASSERT_EQ(90U, del_factory->GetDeletionTrigger());
  ASSERT_EQ(0.0, del_factory->GetDeletionRatio());
  ASSERT_OK(TablePropertiesCollectorFactory::CreateFromString(
      options,
      "window_size=100; deletion_trigger=90; deletion_ratio=0.5; id=" + id,
      &factory));
  del_factory = factory->CheckedCast<CompactOnDeletionCollectorFactory>();
  ASSERT_NE(del_factory, nullptr);
  ASSERT_EQ(100U, del_factory->GetWindowSize());
  ASSERT_EQ(90U, del_factory->GetDeletionTrigger());
  ASSERT_EQ(0.5, del_factory->GetDeletionRatio());
}

TablePropertiesCollection
DBTablePropertiesTest::TestGetPropertiesOfTablesInRange(
    std::vector<Range> ranges, std::size_t* num_properties,
    std::size_t* num_files) {

  // Since we deref zero element in the vector it can not be empty
  // otherwise we pass an address to some random memory
  EXPECT_GT(ranges.size(), 0U);
  // run the query
  TablePropertiesCollection props;
  EXPECT_OK(db_->GetPropertiesOfTablesInRange(
      db_->DefaultColumnFamily(), &ranges[0], ranges.size(), &props));

  // Make sure that we've received properties for those and for those files
  // only which fall within requested ranges
  std::vector<LiveFileMetaData> vmd;
  db_->GetLiveFilesMetaData(&vmd);
  for (auto& md : vmd) {
    std::string fn = md.db_path + md.name;
    bool in_range = false;
    for (auto& r : ranges) {
      // smallestkey < limit && largestkey >= start
      if (r.limit.compare(md.smallestkey) >= 0 &&
          r.start.compare(md.largestkey) <= 0) {
        in_range = true;
        EXPECT_GT(props.count(fn), 0);
      }
    }
    if (!in_range) {
      EXPECT_EQ(props.count(fn), 0);
    }
  }

  if (num_properties) {
    *num_properties = props.size();
  }

  if (num_files) {
    *num_files = vmd.size();
  }
  return props;
}

TEST_F(DBTablePropertiesTest, GetPropertiesOfTablesInRange) {
  // Fixed random sead
  Random rnd(301);

  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 4096;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.target_file_size_base = 2048;
  options.max_bytes_for_level_base = 40960;
  options.max_bytes_for_level_multiplier = 4;
  options.hard_pending_compaction_bytes_limit = 16 * 1024;
  options.num_levels = 8;
  options.env = env_;

  DestroyAndReopen(options);

  // build a decent LSM
  for (int i = 0; i < 10000; i++) {
    ASSERT_OK(Put(test::RandomKey(&rnd, 5), rnd.RandomString(102)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  if (NumTableFilesAtLevel(0) == 0) {
    ASSERT_OK(Put(test::RandomKey(&rnd, 5), rnd.RandomString(102)));
    ASSERT_OK(Flush());
  }

  ASSERT_OK(db_->PauseBackgroundWork());

  // Ensure that we have at least L0, L1 and L2
  ASSERT_GT(NumTableFilesAtLevel(0), 0);
  ASSERT_GT(NumTableFilesAtLevel(1), 0);
  ASSERT_GT(NumTableFilesAtLevel(2), 0);

  // Query the largest range
  std::size_t num_properties, num_files;
  TestGetPropertiesOfTablesInRange(
      {Range(test::RandomKey(&rnd, 5, test::RandomKeyType::SMALLEST),
             test::RandomKey(&rnd, 5, test::RandomKeyType::LARGEST))},
      &num_properties, &num_files);
  ASSERT_EQ(num_properties, num_files);

  // Query the empty range
  TestGetPropertiesOfTablesInRange(
      {Range(test::RandomKey(&rnd, 5, test::RandomKeyType::LARGEST),
             test::RandomKey(&rnd, 5, test::RandomKeyType::SMALLEST))},
      &num_properties, &num_files);
  ASSERT_GT(num_files, 0);
  ASSERT_EQ(num_properties, 0);

  // Query the middle rangee
  TestGetPropertiesOfTablesInRange(
      {Range(test::RandomKey(&rnd, 5, test::RandomKeyType::MIDDLE),
             test::RandomKey(&rnd, 5, test::RandomKeyType::LARGEST))},
      &num_properties, &num_files);
  ASSERT_GT(num_files, 0);
  ASSERT_GT(num_files, num_properties);
  ASSERT_GT(num_properties, 0);

  // Query a bunch of random ranges
  for (int j = 0; j < 100; j++) {
    // create a bunch of ranges
    std::vector<std::string> random_keys;
    // Random returns numbers with zero included
    // when we pass empty ranges TestGetPropertiesOfTablesInRange()
    // derefs random memory in the empty ranges[0]
    // so want to be greater than zero and even since
    // the below loop requires that random_keys.size() to be even.
    auto n = 2 * (rnd.Uniform(50) + 1);

    for (uint32_t i = 0; i < n; ++i) {
      random_keys.push_back(test::RandomKey(&rnd, 5));
    }

    ASSERT_GT(random_keys.size(), 0U);
    ASSERT_EQ((random_keys.size() % 2), 0U);

    std::vector<Range> ranges;
    auto it = random_keys.begin();
    while (it != random_keys.end()) {
      ranges.push_back(Range(*it, *(it + 1)));
      it += 2;
    }

    TestGetPropertiesOfTablesInRange(std::move(ranges));
  }
}

TEST_F(DBTablePropertiesTest, GetColumnFamilyNameProperty) {
  std::string kExtraCfName = "pikachu";
  CreateAndReopenWithCF({kExtraCfName}, CurrentOptions());

  // Create one table per CF, then verify it was created with the column family
  // name property.
  for (uint32_t cf = 0; cf < 2; ++cf) {
    ASSERT_OK(Put(cf, "key", "val"));
    ASSERT_OK(Flush(cf));

    TablePropertiesCollection fname_to_props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[cf], &fname_to_props));
    ASSERT_EQ(1U, fname_to_props.size());

    std::string expected_cf_name;
    if (cf > 0) {
      expected_cf_name = kExtraCfName;
    } else {
      expected_cf_name = kDefaultColumnFamilyName;
    }
    ASSERT_EQ(expected_cf_name,
              fname_to_props.begin()->second->column_family_name);
    ASSERT_EQ(cf, static_cast<uint32_t>(
                      fname_to_props.begin()->second->column_family_id));
  }
}

TEST_F(DBTablePropertiesTest, GetDbIdentifiersProperty) {
  CreateAndReopenWithCF({"goku"}, CurrentOptions());

  for (uint32_t cf = 0; cf < 2; ++cf) {
    ASSERT_OK(Put(cf, "key", "val"));
    ASSERT_OK(Put(cf, "foo", "bar"));
    ASSERT_OK(Flush(cf));

    TablePropertiesCollection fname_to_props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[cf], &fname_to_props));
    ASSERT_EQ(1U, fname_to_props.size());

    std::string id, sid;
    ASSERT_OK(db_->GetDbIdentity(id));
    ASSERT_OK(db_->GetDbSessionId(sid));
    ASSERT_EQ(id, fname_to_props.begin()->second->db_id);
    ASSERT_EQ(sid, fname_to_props.begin()->second->db_session_id);
  }
}

class DBTableHostnamePropertyTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<int, std::string>> {
 public:
  DBTableHostnamePropertyTest()
      : DBTestBase("db_table_hostname_property_test",
                   /*env_do_fsync=*/false) {}
};

TEST_P(DBTableHostnamePropertyTest, DbHostLocationProperty) {
  option_config_ = std::get<0>(GetParam());
  Options opts = CurrentOptions();
  std::string expected_host_id = std::get<1>(GetParam());
  ;
  if (expected_host_id == kHostnameForDbHostId) {
    ASSERT_OK(env_->GetHostNameString(&expected_host_id));
  } else {
    opts.db_host_id = expected_host_id;
  }
  CreateAndReopenWithCF({"goku"}, opts);

  for (uint32_t cf = 0; cf < 2; ++cf) {
    ASSERT_OK(Put(cf, "key", "val"));
    ASSERT_OK(Put(cf, "foo", "bar"));
    ASSERT_OK(Flush(cf));

    TablePropertiesCollection fname_to_props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[cf], &fname_to_props));
    ASSERT_EQ(1U, fname_to_props.size());

    ASSERT_EQ(fname_to_props.begin()->second->db_host_id, expected_host_id);
  }
}

INSTANTIATE_TEST_CASE_P(
    DBTableHostnamePropertyTest, DBTableHostnamePropertyTest,
    ::testing::Values(
        // OptionConfig, override db_host_location
        std::make_tuple(DBTestBase::OptionConfig::kDefault,
                        kHostnameForDbHostId),
        std::make_tuple(DBTestBase::OptionConfig::kDefault, "foobar"),
        std::make_tuple(DBTestBase::OptionConfig::kDefault, ""),
        std::make_tuple(DBTestBase::OptionConfig::kPlainTableFirstBytePrefix,
                        kHostnameForDbHostId),
        std::make_tuple(DBTestBase::OptionConfig::kPlainTableFirstBytePrefix,
                        "foobar"),
        std::make_tuple(DBTestBase::OptionConfig::kPlainTableFirstBytePrefix,
                        "")));

class DeletionTriggeredCompactionTestListener : public EventListener {
 public:
  void OnCompactionBegin(DB* , const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.compaction_reason,
              CompactionReason::kFilesMarkedForCompaction);
  }

  void OnCompactionCompleted(DB* , const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.compaction_reason,
              CompactionReason::kFilesMarkedForCompaction);
  }
};

TEST_P(DBTablePropertiesTest, DeletionTriggeredCompactionMarking) {
  int kNumKeys = 1000;
  int kWindowSize = 100;
  int kNumDelsTrigger = 90;
  std::shared_ptr<TablePropertiesCollectorFactory> compact_on_del =
    NewCompactOnDeletionCollectorFactory(kWindowSize, kNumDelsTrigger);

  Options opts = CurrentOptions();
  opts.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  opts.table_properties_collector_factories.emplace_back(compact_on_del);

  if(GetParam() == "kCompactionStyleUniversal") {
    opts.compaction_style = kCompactionStyleUniversal;
  }
  Reopen(opts);

  // add an L1 file to prevent tombstones from dropping due to obsolescence
  // during flush
  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  DeletionTriggeredCompactionTestListener *listener =
    new DeletionTriggeredCompactionTestListener();
  opts.listeners.emplace_back(listener);
  Reopen(opts);

  for (int i = 0; i < kNumKeys; ++i) {
    if (i >= kNumKeys - kWindowSize &&
        i < kNumKeys - kWindowSize + kNumDelsTrigger) {
      ASSERT_OK(Delete(Key(i)));
    } else {
      ASSERT_OK(Put(Key(i), "val"));
    }
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(0, NumTableFilesAtLevel(0));

  // Change the window size and deletion trigger and ensure new values take
  // effect
  kWindowSize = 50;
  kNumDelsTrigger = 40;
  static_cast<CompactOnDeletionCollectorFactory*>
    (compact_on_del.get())->SetWindowSize(kWindowSize);
  static_cast<CompactOnDeletionCollectorFactory*>
    (compact_on_del.get())->SetDeletionTrigger(kNumDelsTrigger);
  for (int i = 0; i < kNumKeys; ++i) {
    if (i >= kNumKeys - kWindowSize &&
        i < kNumKeys - kWindowSize + kNumDelsTrigger) {
      ASSERT_OK(Delete(Key(i)));
    } else {
      ASSERT_OK(Put(Key(i), "val"));
    }
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(0, NumTableFilesAtLevel(0));

  // Change the window size to disable delete triggered compaction
  kWindowSize = 0;
  static_cast<CompactOnDeletionCollectorFactory*>
    (compact_on_del.get())->SetWindowSize(kWindowSize);
  static_cast<CompactOnDeletionCollectorFactory*>
    (compact_on_del.get())->SetDeletionTrigger(kNumDelsTrigger);
  for (int i = 0; i < kNumKeys; ++i) {
    if (i >= kNumKeys - kWindowSize &&
        i < kNumKeys - kWindowSize + kNumDelsTrigger) {
      ASSERT_OK(Delete(Key(i)));
    } else {
      ASSERT_OK(Put(Key(i), "val"));
    }
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_LT(0, opts.statistics->getTickerCount(COMPACT_WRITE_BYTES_MARKED));
  ASSERT_LT(0, opts.statistics->getTickerCount(COMPACT_READ_BYTES_MARKED));
}

TEST_P(DBTablePropertiesTest, RatioBasedDeletionTriggeredCompactionMarking) {
  constexpr int kNumKeys = 1000;
  constexpr int kWindowSize = 0;
  constexpr int kNumDelsTrigger = 0;
  constexpr double kDeletionRatio = 0.1;
  std::shared_ptr<TablePropertiesCollectorFactory> compact_on_del =
      NewCompactOnDeletionCollectorFactory(kWindowSize, kNumDelsTrigger,
                                           kDeletionRatio);

  Options opts = CurrentOptions();
  opts.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  opts.table_properties_collector_factories.emplace_back(compact_on_del);

  Reopen(opts);

  // Add an L2 file to prevent tombstones from dropping due to obsolescence
  // during flush
  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  auto* listener = new DeletionTriggeredCompactionTestListener();
  opts.listeners.emplace_back(listener);
  Reopen(opts);

  // Generate one L0 with kNumKeys Put.
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), "not important"));
  }
  ASSERT_OK(Flush());

  // Generate another L0 with kNumKeys Delete.
  // This file, due to deletion ratio, will trigger compaction: 2@0 files to L1.
  // The resulting L1 file has only one tombstone for user key 'Key(0)'.
  // Again, due to deletion ratio, a compaction will be triggered: 1@1 + 1@2
  // files to L2. However, the resulting file is empty because the tombstone
  // and value are both dropped.
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(0, NumTableFilesAtLevel(i));
  }
}

INSTANTIATE_TEST_CASE_P(
    DBTablePropertiesTest,
    DBTablePropertiesTest,
    ::testing::Values(
      "kCompactionStyleLevel",
      "kCompactionStyleUniversal"
      ));

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
