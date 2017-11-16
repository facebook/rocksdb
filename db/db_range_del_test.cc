//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

class DBRangeDelTest : public DBTestBase {
 public:
  DBRangeDelTest() : DBTestBase("/db_range_del_test") {}

  std::string GetNumericStr(int key) {
    uint64_t uint64_key = static_cast<uint64_t>(key);
    std::string str;
    str.resize(8);
    memcpy(&str[0], static_cast<void*>(&uint64_key), 8);
    return str;
  }
};

// PlainTableFactory and NumTableFilesAtLevel() are not supported in
// ROCKSDB_LITE
#ifndef ROCKSDB_LITE
TEST_F(DBRangeDelTest, NonBlockBasedTableNotSupported) {
  if (!IsMemoryMappedAccessSupported()) {
    return;
  }
  Options opts = CurrentOptions();
  opts.table_factory.reset(new PlainTableFactory());
  opts.prefix_extractor.reset(NewNoopTransform());
  opts.allow_mmap_reads = true;
  opts.max_sequential_skip_in_iterations = 999999;
  Reopen(opts);

  ASSERT_TRUE(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "dr1", "dr1")
          .IsNotSupported());
}

TEST_F(DBRangeDelTest, FlushOutputHasOnlyRangeTombstones) {
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "dr1",
                             "dr2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
}

TEST_F(DBRangeDelTest, CompactionOutputHasOnlyRangeTombstone) {
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.statistics = CreateDBStatistics();
  Reopen(opts);

  // snapshot protects range tombstone from dropping due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();
  db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z");
  db_->Flush(FlushOptions());

  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
  dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                              true /* disallow_trivial_move */);
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(0, TestGetTickerCount(opts, COMPACTION_RANGE_DEL_DROP_OBSOLETE));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, CompactionOutputFilesExactlyFilled) {
  // regression test for exactly filled compaction output files. Previously
  // another file would be generated containing all range deletions, which
  // could invalidate the non-overlapping file boundary invariant.
  const int kNumPerFile = 4, kNumFiles = 2, kFileBytes = 9 << 10;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumFiles;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumPerFile));
  options.num_levels = 2;
  options.target_file_size_base = kFileBytes;
  BlockBasedTableOptions table_options;
  table_options.block_size_deviation = 50;  // each block holds two keys
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  // snapshot protects range tombstone from dropping due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();
  db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0), Key(1));

  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 12K (4 values, each 3K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(RandomString(&rnd, 3 << 10));
      ASSERT_OK(Put(Key(i * kNumPerFile + j), values[j]));
      if (j == 0 && i > 0) {
        dbfull()->TEST_WaitForFlushMemTable();
      }
    }
  }
  // put extra key to trigger final flush
  ASSERT_OK(Put("", ""));
  dbfull()->TEST_WaitForFlushMemTable();
  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));

  dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                              true /* disallow_trivial_move */);
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, MaxCompactionBytesCutsOutputFiles) {
  // Ensures range deletion spanning multiple compaction output files that are
  // cut by max_compaction_bytes will have non-overlapping key-ranges.
  // https://github.com/facebook/rocksdb/issues/1778
  const int kNumFiles = 2, kNumPerFile = 1 << 8, kBytesPerVal = 1 << 12;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.disable_auto_compactions = true;
  opts.level0_file_num_compaction_trigger = kNumFiles;
  opts.max_compaction_bytes = kNumPerFile * kBytesPerVal;
  opts.memtable_factory.reset(new SpecialSkipListFactory(kNumPerFile));
  // Want max_compaction_bytes to trigger the end of compaction output file, not
  // target_file_size_base, so make the latter much bigger
  opts.target_file_size_base = 100 * opts.max_compaction_bytes;
  Reopen(opts);

  // snapshot protects range tombstone from dropping due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();

  // It spans the whole key-range, thus will be included in all output files
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             GetNumericStr(0),
                             GetNumericStr(kNumFiles * kNumPerFile - 1)));
  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 1MB (256 values, each 4K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(RandomString(&rnd, kBytesPerVal));
      ASSERT_OK(Put(GetNumericStr(kNumPerFile * i + j), values[j]));
    }
    // extra entry to trigger SpecialSkipListFactory's flush
    ASSERT_OK(Put(GetNumericStr(kNumPerFile), ""));
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
  }

  dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                              true /* disallow_trivial_move */);
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GE(NumTableFilesAtLevel(1), 2);

  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);

  for (size_t i = 0; i < files[1].size() - 1; ++i) {
    ASSERT_TRUE(InternalKeyComparator(opts.comparator)
                    .Compare(files[1][i].largest, files[1][i + 1].smallest) <
                0);
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, SentinelsOmittedFromOutputFile) {
  // Regression test for bug where sentinel range deletions (i.e., ones with
  // sequence number of zero) were included in output files.
  // snapshot protects range tombstone from dropping due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();

  // gaps between ranges creates sentinels in our internal representation
  std::vector<std::pair<std::string, std::string>> range_dels = {{"a", "b"}, {"c", "d"}, {"e", "f"}};
  for (const auto& range_del : range_dels) {
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               range_del.first, range_del.second));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);
  ASSERT_GT(files[0][0].smallest_seqno, 0);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, FlushRangeDelsSameStartKey) {
  db_->Put(WriteOptions(), "b1", "val");
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "c"));
  db_->Put(WriteOptions(), "b2", "val");
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "b"));
  // first iteration verifies query correctness in memtable, second verifies
  // query correctness for a single SST file
  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
      ASSERT_EQ(1, NumTableFilesAtLevel(0));
    }
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "b1", &value).IsNotFound());
    ASSERT_OK(db_->Get(ReadOptions(), "b2", &value));
  }
}

TEST_F(DBRangeDelTest, CompactRangeDelsSameStartKey) {
  db_->Put(WriteOptions(), "unused", "val");  // prevents empty after compaction
  db_->Put(WriteOptions(), "b1", "val");
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "c"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(3, NumTableFilesAtLevel(0));

  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                  true /* disallow_trivial_move */);
      ASSERT_EQ(0, NumTableFilesAtLevel(0));
      ASSERT_EQ(1, NumTableFilesAtLevel(1));
    }
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "b1", &value).IsNotFound());
  }
}
#endif  // ROCKSDB_LITE

TEST_F(DBRangeDelTest, FlushRemovesCoveredKeys) {
  const int kNum = 300, kRangeBegin = 50, kRangeEnd = 250;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  Reopen(opts);

  // Write a third before snapshot, a third between snapshot and tombstone, and
  // a third after the tombstone. Keys older than snapshot or newer than the
  // tombstone should be preserved.
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNum; ++i) {
    if (i == kNum / 3) {
      snapshot = db_->GetSnapshot();
    } else if (i == 2 * kNum / 3) {
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                       GetNumericStr(kRangeBegin), GetNumericStr(kRangeEnd));
    }
    db_->Put(WriteOptions(), GetNumericStr(i), "val");
  }
  db_->Flush(FlushOptions());

  for (int i = 0; i < kNum; ++i) {
    ReadOptions read_opts;
    read_opts.ignore_range_deletions = true;
    std::string value;
    if (i < kRangeBegin || i > kRangeEnd || i < kNum / 3 || i >= 2 * kNum / 3) {
      ASSERT_OK(db_->Get(read_opts, GetNumericStr(i), &value));
    } else {
      ASSERT_TRUE(db_->Get(read_opts, GetNumericStr(i), &value).IsNotFound());
    }
  }
  db_->ReleaseSnapshot(snapshot);
}

// NumTableFilesAtLevel() is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
TEST_F(DBRangeDelTest, CompactionRemovesCoveredKeys) {
  const int kNumPerFile = 100, kNumFiles = 4;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.disable_auto_compactions = true;
  opts.memtable_factory.reset(new SpecialSkipListFactory(kNumPerFile));
  opts.num_levels = 2;
  opts.statistics = CreateDBStatistics();
  Reopen(opts);

  for (int i = 0; i < kNumFiles; ++i) {
    if (i > 0) {
      // range tombstone covers first half of the previous file
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                       GetNumericStr((i - 1) * kNumPerFile),
                       GetNumericStr((i - 1) * kNumPerFile + kNumPerFile / 2));
    }
    // Make sure a given key appears in each file so compaction won't be able to
    // use trivial move, which would happen if the ranges were non-overlapping.
    // Also, we need an extra element since flush is only triggered when the
    // number of keys is one greater than SpecialSkipListFactory's limit.
    // We choose a key outside the key-range used by the test to avoid conflict.
    db_->Put(WriteOptions(), GetNumericStr(kNumPerFile * kNumFiles), "val");

    for (int j = 0; j < kNumPerFile; ++j) {
      db_->Put(WriteOptions(), GetNumericStr(i * kNumPerFile + j), "val");
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
  }
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GT(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ((kNumFiles - 1) * kNumPerFile / 2,
            TestGetTickerCount(opts, COMPACTION_KEY_DROP_RANGE_DEL));

  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kNumPerFile; ++j) {
      ReadOptions read_opts;
      read_opts.ignore_range_deletions = true;
      std::string value;
      if (i == kNumFiles - 1 || j >= kNumPerFile / 2) {
        ASSERT_OK(
            db_->Get(read_opts, GetNumericStr(i * kNumPerFile + j), &value));
      } else {
        ASSERT_TRUE(
            db_->Get(read_opts, GetNumericStr(i * kNumPerFile + j), &value)
                .IsNotFound());
      }
    }
  }
}

TEST_F(DBRangeDelTest, ValidLevelSubcompactionBoundaries) {
  const int kNumPerFile = 100, kNumFiles = 4, kFileBytes = 100 << 10;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumFiles;
  options.max_bytes_for_level_base = 2 * kFileBytes;
  options.max_subcompactions = 4;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumPerFile));
  options.num_levels = 3;
  options.target_file_size_base = kFileBytes;
  options.target_file_size_multiplier = 1;
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < 2; ++i) {
    for (int j = 0; j < kNumFiles; ++j) {
      if (i > 0) {
        // delete [95,105) in two files, [295,305) in next two
        int mid = (j + (1 - j % 2)) * kNumPerFile;
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                         Key(mid - 5), Key(mid + 5));
      }
      std::vector<std::string> values;
      // Write 100KB (100 values, each 1K)
      for (int k = 0; k < kNumPerFile; k++) {
        values.push_back(RandomString(&rnd, 990));
        ASSERT_OK(Put(Key(j * kNumPerFile + k), values[k]));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put("", ""));
      dbfull()->TEST_WaitForFlushMemTable();
      if (j < kNumFiles - 1) {
        // background compaction may happen early for kNumFiles'th file
        ASSERT_EQ(NumTableFilesAtLevel(0), j + 1);
      }
      if (j == options.level0_file_num_compaction_trigger - 1) {
        // When i == 1, compaction will output some files to L1, at which point
        // L1 is not bottommost so range deletions cannot be compacted away. The
        // new L1 files must be generated with non-overlapping key ranges even
        // though multiple subcompactions see the same ranges deleted, else an
        // assertion will fail.
        //
        // Only enable auto-compactions when we're ready; otherwise, the
        // oversized L0 (relative to base_level) causes the compaction to run
        // earlier.
        ASSERT_OK(db_->EnableAutoCompaction({db_->DefaultColumnFamily()}));
        dbfull()->TEST_WaitForCompact();
        ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                  {{"disable_auto_compactions", "true"}}));
        ASSERT_EQ(NumTableFilesAtLevel(0), 0);
        ASSERT_GT(NumTableFilesAtLevel(1), 0);
        ASSERT_GT(NumTableFilesAtLevel(2), 0);
      }
    }
  }
}

TEST_F(DBRangeDelTest, ValidUniversalSubcompactionBoundaries) {
  const int kNumPerFile = 100, kFilesPerLevel = 4, kNumLevels = 4;
  Options options = CurrentOptions();
  options.compaction_options_universal.min_merge_width = kFilesPerLevel;
  options.compaction_options_universal.max_merge_width = kFilesPerLevel;
  options.compaction_options_universal.size_ratio = 10;
  options.compaction_style = kCompactionStyleUniversal;
  options.level0_file_num_compaction_trigger = kFilesPerLevel;
  options.max_subcompactions = 4;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumPerFile));
  options.num_levels = kNumLevels;
  options.target_file_size_base = kNumPerFile << 10;
  options.target_file_size_multiplier = 1;
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < kNumLevels - 1; ++i) {
    for (int j = 0; j < kFilesPerLevel; ++j) {
      if (i == kNumLevels - 2) {
        // insert range deletions [95,105) in two files, [295,305) in next two
        // to prepare L1 for later manual compaction.
        int mid = (j + (1 - j % 2)) * kNumPerFile;
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                         Key(mid - 5), Key(mid + 5));
      }
      std::vector<std::string> values;
      // Write 100KB (100 values, each 1K)
      for (int k = 0; k < kNumPerFile; k++) {
        values.push_back(RandomString(&rnd, 990));
        ASSERT_OK(Put(Key(j * kNumPerFile + k), values[k]));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put("", ""));
      dbfull()->TEST_WaitForFlushMemTable();
      if (j < kFilesPerLevel - 1) {
        // background compaction may happen early for kFilesPerLevel'th file
        ASSERT_EQ(NumTableFilesAtLevel(0), j + 1);
      }
    }
    dbfull()->TEST_WaitForCompact();
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    ASSERT_GT(NumTableFilesAtLevel(kNumLevels - 1 - i), kFilesPerLevel - 1);
  }
  // Now L1-L3 are full, when we compact L1->L2 we should see (1) subcompactions
  // happen since input level > 0; (2) range deletions are not dropped since
  // output level is not bottommost. If no file boundary assertion fails, that
  // probably means universal compaction + subcompaction + range deletion are
  // compatible.
  ASSERT_OK(dbfull()->RunManualCompaction(
      reinterpret_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
          ->cfd(),
      1 /* input_level */, 2 /* output_level */, 0 /* output_path_id */,
      nullptr /* begin */, nullptr /* end */, true /* exclusive */,
      true /* disallow_trivial_move */));
}
#endif  // ROCKSDB_LITE

TEST_F(DBRangeDelTest, CompactionRemovesCoveredMergeOperands) {
  const int kNumPerFile = 3, kNumFiles = 3;
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.memtable_factory.reset(new SpecialSkipListFactory(2 * kNumPerFile));
  opts.merge_operator = MergeOperators::CreateUInt64AddOperator();
  opts.num_levels = 2;
  Reopen(opts);

  // Iterates kNumFiles * kNumPerFile + 1 times since flushing the last file
  // requires an extra entry.
  for (int i = 0; i <= kNumFiles * kNumPerFile; ++i) {
    if (i % kNumPerFile == 0 && i / kNumPerFile == kNumFiles - 1) {
      // Delete merge operands from all but the last file
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "key",
                       "key_");
    }
    std::string val;
    PutFixed64(&val, i);
    db_->Merge(WriteOptions(), "key", val);
    // we need to prevent trivial move using Puts so compaction will actually
    // process the merge operands.
    db_->Put(WriteOptions(), "prevent_trivial_move", "");
    if (i > 0 && i % kNumPerFile == 0) {
      dbfull()->TEST_WaitForFlushMemTable();
    }
  }

  ReadOptions read_opts;
  read_opts.ignore_range_deletions = true;
  std::string expected, actual;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 45);  // 1+2+...+9
  ASSERT_EQ(expected, actual);

  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  expected.clear();
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  uint64_t tmp;
  Slice tmp2(actual);
  GetFixed64(&tmp2, &tmp);
  PutFixed64(&expected, 30);  // 6+7+8+9 (earlier operands covered by tombstone)
  ASSERT_EQ(expected, actual);
}

// NumTableFilesAtLevel() is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
TEST_F(DBRangeDelTest, ObsoleteTombstoneCleanup) {
  // During compaction to bottommost level, verify range tombstones older than
  // the oldest snapshot are removed, while others are preserved.
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.num_levels = 2;
  opts.statistics = CreateDBStatistics();
  Reopen(opts);

  db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "dr1",
                   "dr1");  // obsolete after compaction
  db_->Put(WriteOptions(), "key", "val");
  db_->Flush(FlushOptions());
  const Snapshot* snapshot = db_->GetSnapshot();
  db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "dr2",
                   "dr2");  // protected by snapshot
  db_->Put(WriteOptions(), "key", "val");
  db_->Flush(FlushOptions());

  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, TestGetTickerCount(opts, COMPACTION_RANGE_DEL_DROP_OBSOLETE));

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, TableEvictedDuringScan) {
  // The RangeDelAggregator holds pointers into range deletion blocks created by
  // table readers. This test ensures the aggregator can still access those
  // blocks even if it outlives the table readers that created them.
  //
  // DBIter always keeps readers open for L0 files. So, in order to test
  // aggregator outliving reader, we need to have deletions in L1 files, which
  // are opened/closed on-demand during the scan. This is accomplished by
  // setting kNumRanges > level0_stop_writes_trigger, which prevents deletions
  // from all lingering in L0 (there is at most one range deletion per L0 file).
  //
  // The first L1 file will contain a range deletion since its begin key is 0.
  // SeekToFirst() references that table's reader and adds its range tombstone
  // to the aggregator. Upon advancing beyond that table's key-range via Next(),
  // the table reader will be unreferenced by the iterator. Since we manually
  // call Evict() on all readers before the full scan, this unreference causes
  // the reader's refcount to drop to zero and thus be destroyed.
  //
  // When it is destroyed, we do not remove its range deletions from the
  // aggregator. So, subsequent calls to Next() must be able to use these
  // deletions to decide whether a key is covered. This will work as long as
  // the aggregator properly references the range deletion block.
  const int kNum = 25, kRangeBegin = 0, kRangeEnd = 7, kNumRanges = 5;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.level0_file_num_compaction_trigger = 4;
  opts.level0_stop_writes_trigger = 4;
  opts.memtable_factory.reset(new SpecialSkipListFactory(1));
  opts.num_levels = 2;
  BlockBasedTableOptions bbto;
  bbto.cache_index_and_filter_blocks = true;
  bbto.block_cache = NewLRUCache(8 << 20);
  opts.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(opts);

  // Hold a snapshot so range deletions can't become obsolete during compaction
  // to bottommost level (i.e., L1).
  const Snapshot* snapshot = db_->GetSnapshot();
  for (int i = 0; i < kNum; ++i) {
    db_->Put(WriteOptions(), GetNumericStr(i), "val");
    if (i > 0) {
      dbfull()->TEST_WaitForFlushMemTable();
    }
    if (i >= kNum / 2 && i < kNum / 2 + kNumRanges) {
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                       GetNumericStr(kRangeBegin), GetNumericStr(kRangeEnd));
    }
  }
  // Must be > 1 so the first L1 file can be closed before scan finishes
  dbfull()->TEST_WaitForCompact();
  ASSERT_GT(NumTableFilesAtLevel(1), 1);
  std::vector<uint64_t> file_numbers = ListTableFiles(env_, dbname_);

  ReadOptions read_opts;
  auto* iter = db_->NewIterator(read_opts);
  int expected = kRangeEnd;
  iter->SeekToFirst();
  for (auto file_number : file_numbers) {
    // This puts table caches in the state of being externally referenced only
    // so they are destroyed immediately upon iterator unreferencing.
    TableCache::Evict(dbfull()->TEST_table_cache(), file_number);
  }
  for (; iter->Valid(); iter->Next()) {
    ASSERT_EQ(GetNumericStr(expected), iter->key());
    ++expected;
    // Keep clearing block cache's LRU so range deletion block can be freed as
    // soon as its refcount drops to zero.
    bbto.block_cache->EraseUnRefEntries();
  }
  ASSERT_EQ(kNum, expected);
  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, GetCoveredKeyFromMutableMemtable) {
  db_->Put(WriteOptions(), "key", "val");
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

  ReadOptions read_opts;
  std::string value;
  ASSERT_TRUE(db_->Get(read_opts, "key", &value).IsNotFound());
}

TEST_F(DBRangeDelTest, GetCoveredKeyFromImmutableMemtable) {
  Options opts = CurrentOptions();
  opts.max_write_buffer_number = 3;
  opts.min_write_buffer_number_to_merge = 2;
  // SpecialSkipListFactory lets us specify maximum number of elements the
  // memtable can hold. It switches the active memtable to immutable (flush is
  // prevented by the above options) upon inserting an element that would
  // overflow the memtable.
  opts.memtable_factory.reset(new SpecialSkipListFactory(1));
  Reopen(opts);

  db_->Put(WriteOptions(), "key", "val");
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  db_->Put(WriteOptions(), "blah", "val");

  ReadOptions read_opts;
  std::string value;
  ASSERT_TRUE(db_->Get(read_opts, "key", &value).IsNotFound());
}

TEST_F(DBRangeDelTest, GetCoveredKeyFromSst) {
  db_->Put(WriteOptions(), "key", "val");
  // snapshot prevents key from being deleted during flush
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions read_opts;
  std::string value;
  ASSERT_TRUE(db_->Get(read_opts, "key", &value).IsNotFound());
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, GetCoveredMergeOperandFromMemtable) {
  const int kNumMergeOps = 10;
  Options opts = CurrentOptions();
  opts.merge_operator = MergeOperators::CreateUInt64AddOperator();
  Reopen(opts);

  for (int i = 0; i < kNumMergeOps; ++i) {
    std::string val;
    PutFixed64(&val, i);
    db_->Merge(WriteOptions(), "key", val);
    if (i == kNumMergeOps / 2) {
      // deletes [0, 5]
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "key",
                       "key_");
    }
  }

  ReadOptions read_opts;
  std::string expected, actual;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 30);  // 6+7+8+9
  ASSERT_EQ(expected, actual);

  expected.clear();
  read_opts.ignore_range_deletions = true;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 45);  // 0+1+2+...+9
  ASSERT_EQ(expected, actual);
}

TEST_F(DBRangeDelTest, GetIgnoresRangeDeletions) {
  Options opts = CurrentOptions();
  opts.max_write_buffer_number = 4;
  opts.min_write_buffer_number_to_merge = 3;
  opts.memtable_factory.reset(new SpecialSkipListFactory(1));
  Reopen(opts);

  db_->Put(WriteOptions(), "sst_key", "val");
  // snapshot prevents key from being deleted during flush
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  db_->Put(WriteOptions(), "imm_key", "val");
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  db_->Put(WriteOptions(), "mem_key", "val");
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

  ReadOptions read_opts;
  read_opts.ignore_range_deletions = true;
  for (std::string key : {"sst_key", "imm_key", "mem_key"}) {
    std::string value;
    ASSERT_OK(db_->Get(read_opts, key, &value));
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, IteratorRemovesCoveredKeys) {
  const int kNum = 200, kRangeBegin = 50, kRangeEnd = 150, kNumPerFile = 25;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.memtable_factory.reset(new SpecialSkipListFactory(kNumPerFile));
  Reopen(opts);

  // Write half of the keys before the tombstone and half after the tombstone.
  // Only covered keys (i.e., within the range and older than the tombstone)
  // should be deleted.
  for (int i = 0; i < kNum; ++i) {
    if (i == kNum / 2) {
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                       GetNumericStr(kRangeBegin), GetNumericStr(kRangeEnd));
    }
    db_->Put(WriteOptions(), GetNumericStr(i), "val");
  }
  ReadOptions read_opts;
  auto* iter = db_->NewIterator(read_opts);

  int expected = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(GetNumericStr(expected), iter->key());
    if (expected == kRangeBegin - 1) {
      expected = kNum / 2;
    } else {
      ++expected;
    }
  }
  ASSERT_EQ(kNum, expected);
  delete iter;
}

TEST_F(DBRangeDelTest, IteratorOverUserSnapshot) {
  const int kNum = 200, kRangeBegin = 50, kRangeEnd = 150, kNumPerFile = 25;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.memtable_factory.reset(new SpecialSkipListFactory(kNumPerFile));
  Reopen(opts);

  const Snapshot* snapshot = nullptr;
  // Put a snapshot before the range tombstone, verify an iterator using that
  // snapshot sees all inserted keys.
  for (int i = 0; i < kNum; ++i) {
    if (i == kNum / 2) {
      snapshot = db_->GetSnapshot();
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                       GetNumericStr(kRangeBegin), GetNumericStr(kRangeEnd));
    }
    db_->Put(WriteOptions(), GetNumericStr(i), "val");
  }
  ReadOptions read_opts;
  read_opts.snapshot = snapshot;
  auto* iter = db_->NewIterator(read_opts);

  int expected = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(GetNumericStr(expected), iter->key());
    ++expected;
  }
  ASSERT_EQ(kNum / 2, expected);
  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, IteratorIgnoresRangeDeletions) {
  Options opts = CurrentOptions();
  opts.max_write_buffer_number = 4;
  opts.min_write_buffer_number_to_merge = 3;
  opts.memtable_factory.reset(new SpecialSkipListFactory(1));
  Reopen(opts);

  db_->Put(WriteOptions(), "sst_key", "val");
  // snapshot prevents key from being deleted during flush
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  db_->Put(WriteOptions(), "imm_key", "val");
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  db_->Put(WriteOptions(), "mem_key", "val");
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

  ReadOptions read_opts;
  read_opts.ignore_range_deletions = true;
  auto* iter = db_->NewIterator(read_opts);
  int i = 0;
  std::string expected[] = {"imm_key", "mem_key", "sst_key"};
  for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++i) {
    std::string key;
    ASSERT_EQ(expected[i], iter->key());
  }
  ASSERT_EQ(3, i);
  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

#ifndef ROCKSDB_UBSAN_RUN
TEST_F(DBRangeDelTest, TailingIteratorRangeTombstoneUnsupported) {
  db_->Put(WriteOptions(), "key", "val");
  // snapshot prevents key from being deleted during flush
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

  // iterations check unsupported in memtable, l0, and then l1
  for (int i = 0; i < 3; ++i) {
    ReadOptions read_opts;
    read_opts.tailing = true;
    auto* iter = db_->NewIterator(read_opts);
    if (i == 2) {
      // For L1+, iterators over files are created on-demand, so need seek
      iter->SeekToFirst();
    }
    ASSERT_TRUE(iter->status().IsNotSupported());
    delete iter;
    if (i == 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    } else if (i == 1) {
      MoveFilesToLevel(1);
    }
  }
  db_->ReleaseSnapshot(snapshot);
}

#endif  // !ROCKSDB_UBSAN_RUN

TEST_F(DBRangeDelTest, SubcompactionHasEmptyDedicatedRangeDelFile) {
  const int kNumFiles = 2, kNumKeysPerFile = 4;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumFiles;
  options.max_subcompactions = 2;
  options.num_levels = 2;
  options.target_file_size_base = 4096;
  Reopen(options);

  // need a L1 file for subcompaction to be triggered
  ASSERT_OK(
      db_->Put(WriteOptions(), db_->DefaultColumnFamily(), Key(0), "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);

  // put enough keys to fill up the first subcompaction, and later range-delete
  // them so that the first subcompaction outputs no key-values. In that case
  // it'll consider making an SST file dedicated to range deletions.
  for (int i = 0; i < kNumKeysPerFile; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), Key(i),
                       std::string(1024, 'a')));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(kNumKeysPerFile)));

  // the above range tombstone can be dropped, so that one alone won't cause a
  // dedicated file to be opened. We can make one protected by snapshot that
  // must be considered. Make its range outside the first subcompaction's range
  // to exercise the tricky part of the code.
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             Key(kNumKeysPerFile + 1),
                             Key(kNumKeysPerFile + 2)));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  db_->EnableAutoCompaction({db_->DefaultColumnFamily()});
  dbfull()->TEST_WaitForCompact();
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, MemtableBloomFilter) {
  // regression test for #2743. the range delete tombstones in memtable should
  // be added even when Get() skips searching due to its prefix bloom filter
  const int kMemtableSize = 1 << 20;              // 1MB
  const int kMemtablePrefixFilterSize = 1 << 13;  // 8KB
  const int kNumKeys = 1000;
  const int kPrefixLen = 8;
  Options options = CurrentOptions();
  options.memtable_prefix_bloom_size_ratio =
      static_cast<double>(kMemtablePrefixFilterSize) / kMemtableSize;
  options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(kPrefixLen));
  options.write_buffer_size = kMemtableSize;
  Reopen(options);

  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), "val"));
  }
  Flush();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(kNumKeys)));
  for (int i = 0; i < kNumKeys; ++i) {
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &value).IsNotFound());
  }
}

TEST_F(DBRangeDelTest, CompactionTreatsSplitInputLevelDeletionAtomically) {
  // make sure compaction treats files containing a split range deletion in the
  // input level as an atomic unit. I.e., compacting any input-level file(s)
  // containing a portion of the range deletion causes all other input-level
  // files containing portions of that same range deletion to be included in the
  // compaction.
  const int kNumFilesPerLevel = 4, kValueBytes = 4 << 10;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = kNumFilesPerLevel;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(2 /* num_entries_flush */));
  options.target_file_size_base = kValueBytes;
  // i == 0: CompactFiles
  // i == 1: CompactRange
  // i == 2: automatic compaction
  for (int i = 0; i < 3; ++i) {
    DestroyAndReopen(options);

    ASSERT_OK(Put(Key(0), ""));
    ASSERT_OK(db_->Flush(FlushOptions()));
    MoveFilesToLevel(2);
    ASSERT_EQ(1, NumTableFilesAtLevel(2));

    // snapshot protects range tombstone from dropping due to becoming obsolete.
    const Snapshot* snapshot = db_->GetSnapshot();
    db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                     Key(2 * kNumFilesPerLevel));

    Random rnd(301);
    std::string value = RandomString(&rnd, kValueBytes);
    for (int j = 0; j < kNumFilesPerLevel; ++j) {
      // give files overlapping key-ranges to prevent trivial move
      ASSERT_OK(Put(Key(j), value));
      ASSERT_OK(Put(Key(2 * kNumFilesPerLevel - 1 - j), value));
      if (j > 0) {
        dbfull()->TEST_WaitForFlushMemTable();
        ASSERT_EQ(j, NumTableFilesAtLevel(0));
      }
    }
    // put extra key to trigger final flush
    ASSERT_OK(Put("", ""));
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_EQ(kNumFilesPerLevel, NumTableFilesAtLevel(1));

    ColumnFamilyMetaData meta;
    db_->GetColumnFamilyMetaData(&meta);
    if (i == 0) {
      ASSERT_OK(db_->CompactFiles(
          CompactionOptions(), {meta.levels[1].files[0].name}, 2 /* level */));
    } else if (i == 1) {
      auto begin_str = Key(0), end_str = Key(1);
      Slice begin = begin_str, end = end_str;
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &begin, &end));
    } else if (i == 2) {
      ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                {{"max_bytes_for_level_base", "10000"}}));
      dbfull()->TEST_WaitForCompact();
    }
    ASSERT_EQ(0, NumTableFilesAtLevel(1));
    ASSERT_GT(NumTableFilesAtLevel(2), 0);

    db_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(DBRangeDelTest, UnorderedTombstones) {
  // Regression test for #2752. Range delete tombstones between
  // different snapshot stripes are not stored in order, so the first
  // tombstone of each snapshot stripe should be checked as a smallest
  // candidate.
  Options options = CurrentOptions();
  DestroyAndReopen(options);

  auto cf = db_->DefaultColumnFamily();

  ASSERT_OK(db_->Put(WriteOptions(), cf, "a", "a"));
  ASSERT_OK(db_->Flush(FlushOptions(), cf));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  ASSERT_OK(db_->DeleteRange(WriteOptions(), cf, "b", "c"));
  // Hold a snapshot to separate these two delete ranges.
  auto snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), cf, "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions(), cf));
  db_->ReleaseSnapshot(snapshot);

  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(cf, &files);
  ASSERT_EQ(1, files[0].size());
  ASSERT_EQ("a", files[0][0].smallest.user_key());
  ASSERT_EQ("c", files[0][0].largest.user_key());

  std::string v;
  auto s = db_->Get(ReadOptions(), "a", &v);
  ASSERT_TRUE(s.IsNotFound());
}

#endif  // ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
