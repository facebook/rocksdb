//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// DB-level tests for the trie-based User Defined Index (UDI). Validates that
// a DB opened with the trie UDI factory correctly handles all operation types
// (Put, Delete, Merge, SingleDelete, PutEntity, TimedPut, DeleteRange) through
// flush and compaction, and that the resulting SST files are readable with
// correct data.
//
// These tests complement the SST-level tests in trie_index_test.cc (which use
// SstFileWriter and are limited to Put/Delete/Merge) by exercising the full
// DB path including CompactionIterator, memtable flush, and the UDI builder
// wrapper's ValueType mapping and kTypeValuePreferredSeqno handling.

#include <memory>
#include <string>
#include <vector>

#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/merge_operators.h"
#include "utilities/trie_index/trie_index_factory.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

class TrieIndexDBTest : public testing::Test {
 protected:
  void SetUp() override {
    trie_factory_ = std::make_shared<TrieIndexFactory>();
    dbname_ = test::PerThreadDBPath("trie_index_db_test");
    ASSERT_OK(DestroyDB(dbname_, Options()));
  }

  void TearDown() override {
    if (db_) {
      EXPECT_OK(db_->Close());
      db_.reset();
    }
    EXPECT_OK(DestroyDB(dbname_, last_options_));
  }

  // Opens a DB with the trie UDI factory configured. Caller should set
  // options_ fields before calling this. An optional block_size overrides
  // the default to force more data blocks in the SST.
  Status OpenDB(int block_size = 0) {
    options_.create_if_missing = true;
    BlockBasedTableOptions table_options;
    table_options.user_defined_index_factory = trie_factory_;
    if (block_size > 0) {
      table_options.block_size = block_size;
    }
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    last_options_ = options_;
    return DB::Open(options_, dbname_, &db_);
  }

  // Returns a ReadOptions that routes reads through the standard binary
  // search index (the default when table_index_factory is null).
  ReadOptions StandardIndexReadOptions() const { return ReadOptions(); }

  // Returns a ReadOptions that routes reads through the trie UDI index.
  ReadOptions TrieIndexReadOptions() const {
    ReadOptions ro;
    ro.table_index_factory = trie_factory_.get();
    return ro;
  }

  // Collects all visible keys via forward scan using the given ReadOptions.
  std::vector<std::string> ScanAllKeys(const ReadOptions& ro) {
    std::vector<std::string> keys;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToFirst();
    for (; iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
    EXPECT_OK(iter->status());
    return keys;
  }

  // Collects all visible keys via forward scan using the standard index.
  std::vector<std::string> ScanAllKeys() {
    return ScanAllKeys(StandardIndexReadOptions());
  }

  // Collects all visible (key, value) pairs via forward scan.
  std::vector<std::pair<std::string, std::string>> ScanAllKeyValues(
      const ReadOptions& ro) {
    std::vector<std::pair<std::string, std::string>> kvs;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToFirst();
    for (; iter->Valid(); iter->Next()) {
      kvs.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    EXPECT_OK(iter->status());
    return kvs;
  }

  // Verifies that forward scan via SeekToFirst+Next produces the same key
  // set through both the standard index and the trie index.
  void VerifyForwardScanBothIndexes(
      const std::vector<std::string>& expected_keys) {
    SCOPED_TRACE("standard index");
    ASSERT_EQ(ScanAllKeys(StandardIndexReadOptions()), expected_keys);
    SCOPED_TRACE("trie index");
    ASSERT_EQ(ScanAllKeys(TrieIndexReadOptions()), expected_keys);
  }

  // Verifies that forward scan via SeekToFirst+Next produces the same
  // (key, value) pairs through both indexes.
  void VerifyForwardScanBothIndexes(
      const std::vector<std::pair<std::string, std::string>>& expected_kvs) {
    SCOPED_TRACE("standard index");
    ASSERT_EQ(ScanAllKeyValues(StandardIndexReadOptions()), expected_kvs);
    SCOPED_TRACE("trie index");
    ASSERT_EQ(ScanAllKeyValues(TrieIndexReadOptions()), expected_kvs);
  }

  // Verifies that a point Get returns the expected value through both indexes.
  void VerifyGetBothIndexes(const std::string& key,
                            const std::string& expected_value) {
    for (const auto& ro :
         {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
      SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
      std::string value;
      ASSERT_OK(db_->Get(ro, key, &value));
      ASSERT_EQ(value, expected_value);
    }
  }

  // Verifies that a point Get returns NotFound through both indexes.
  void VerifyGetNotFoundBothIndexes(const std::string& key) {
    for (const auto& ro :
         {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
      SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
      std::string value;
      ASSERT_TRUE(db_->Get(ro, key, &value).IsNotFound());
    }
  }

  // Verifies Get with a snapshot through both indexes.
  void VerifyGetBothIndexes(const Snapshot* snap, const std::string& key,
                            const std::string& expected_value) {
    for (auto base_ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
      SCOPED_TRACE(base_ro.table_index_factory ? "trie index"
                                               : "standard index");
      base_ro.snapshot = snap;
      std::string value;
      ASSERT_OK(db_->Get(base_ro, key, &value));
      ASSERT_EQ(value, expected_value);
    }
  }

  // Verifies Get returns NotFound with a snapshot through both indexes.
  void VerifyGetNotFoundBothIndexes(const Snapshot* snap,
                                    const std::string& key) {
    for (auto base_ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
      SCOPED_TRACE(base_ro.table_index_factory ? "trie index"
                                               : "standard index");
      base_ro.snapshot = snap;
      std::string value;
      ASSERT_TRUE(db_->Get(base_ro, key, &value).IsNotFound());
    }
  }

  // Verifies that a forward scan with a snapshot produces the expected
  // (key, value) pairs through both indexes.
  void VerifyForwardScanBothIndexes(
      const Snapshot* snap,
      const std::vector<std::pair<std::string, std::string>>& expected_kvs) {
    for (auto base_ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
      SCOPED_TRACE(base_ro.table_index_factory ? "trie index"
                                               : "standard index");
      base_ro.snapshot = snap;
      ASSERT_EQ(ScanAllKeyValues(base_ro), expected_kvs);
    }
  }

  // Verifies that Seek to a specific key through both indexes returns the
  // same result.
  void VerifySeekBothIndexes(const std::string& seek_key,
                             const std::string& expected_key,
                             const std::string& expected_value) {
    for (const auto& ro :
         {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
      SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
      std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
      iter->Seek(seek_key);
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), expected_key);
      ASSERT_EQ(iter->value().ToString(), expected_value);
    }
  }

  // Verifies that Seek with a snapshot through both indexes returns the
  // same result.
  void VerifySeekBothIndexes(const Snapshot* snap, const std::string& seek_key,
                             const std::string& expected_key,
                             const std::string& expected_value) {
    for (auto base_ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
      SCOPED_TRACE(base_ro.table_index_factory ? "trie index"
                                               : "standard index");
      base_ro.snapshot = snap;
      std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
      iter->Seek(seek_key);
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), expected_key);
      ASSERT_EQ(iter->value().ToString(), expected_value);
    }
  }

  std::shared_ptr<TrieIndexFactory> trie_factory_;
  std::string dbname_;
  Options options_;
  Options last_options_;
  std::unique_ptr<DB> db_;
};

// ============================================================================
// Flush tests
// ============================================================================

TEST_F(TrieIndexDBTest, FlushWithAllOperationTypes) {
  // Write every supported operation type via the DB API, flush, and verify
  // reads return correct results through both the standard binary search index
  // and the trie UDI. This exercises the full path from memtable through
  // CompactionIterator, BlockBasedTableBuilder, and the UDI builder wrapper's
  // MapToUDIValueType for each internal ValueType.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // kTypeValue
  ASSERT_OK(db_->Put(WriteOptions(), "key_01_put", "val_put"));
  // kTypeMerge
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02_merge", "val_merge"));
  // kTypeDeletion (bare tombstone — no prior value for this key)
  ASSERT_OK(db_->Delete(WriteOptions(), "key_03_del"));
  // kTypeSingleDeletion (preceded by a Put; both cancel out with no snapshot)
  ASSERT_OK(db_->Put(WriteOptions(), "key_04_sdel", "val_sdel"));
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "key_04_sdel"));
  // kTypeWideColumnEntity (with a default column so Get() returns a value)
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "key_05_entity",
                           WideColumns{{"", "default_val"}, {"col1", "val1"}}));
  // Another kTypeValue to anchor the end of the key range
  ASSERT_OK(db_->Put(WriteOptions(), "key_06_put", "val_put2"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Forward scan via both indexes. Expected visible keys after flush:
  //   key_01_put    — Put (visible)
  //   key_02_merge  — Merge single operand (visible)
  //   key_03_del    — bare Delete tombstone (hidden by DBIter)
  //   key_04_sdel   — Put + SingleDelete cancel out (hidden)
  //   key_05_entity — PutEntity (visible)
  //   key_06_put    — Put (visible)
  {
    std::vector<std::string> expected = {"key_01_put", "key_02_merge",
                                         "key_05_entity", "key_06_put"};
    VerifyForwardScanBothIndexes(expected);
  }

  // Point lookups via both indexes.
  VerifyGetBothIndexes("key_01_put", "val_put");
  VerifyGetBothIndexes("key_02_merge", "val_merge");
  VerifyGetNotFoundBothIndexes("key_03_del");
  VerifyGetNotFoundBothIndexes("key_04_sdel");
  // PutEntity: Get() returns the value of the default column ("").
  VerifyGetBothIndexes("key_05_entity", "default_val");
  VerifyGetBothIndexes("key_06_put", "val_put2");
}

TEST_F(TrieIndexDBTest, TimedPutFlush) {
  // TimedPut produces kTypeValuePreferredSeqno entries during flush when
  // preclude_last_level_data_seconds > 0. The UDI wrapper strips the packed
  // preferred seqno suffix via ParsePackedValueForValue() before forwarding
  // to the plugin builder. This test verifies that path end-to-end through
  // both the standard binary search index and the trie UDI.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.compaction_style = kCompactionStyleUniversal;
  // Required for kTypeValuePreferredSeqno to survive the flush path: the
  // seqno_to_time_mapping must be available so a preferred seqno can be
  // computed. With write_unix_time=0, GetProximalSeqnoBeforeTime(0) returns 0,
  // which is < any real seqno, so the entry stays as kTypeValuePreferredSeqno.
  options_.preclude_last_level_data_seconds = 10000;
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Regular Put alongside the TimedPut to verify they coexist.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01_put", "val_put"));

  // TimedPut via WriteBatch (there is no DB::TimedPut method).
  {
    WriteBatch wb;
    ASSERT_OK(wb.TimedPut(db_->DefaultColumnFamily(), "key_02_timed",
                          "val_timed", /*write_unix_time=*/0));
    ASSERT_OK(db_->Write(WriteOptions(), &wb));
  }

  // Merge to verify mixed types work with TimedPut in the same flush.
  ASSERT_OK(db_->Merge(WriteOptions(), "key_03_merge", "val_merge"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Point lookups via both indexes — the packed seqno must be transparent.
  VerifyGetBothIndexes("key_01_put", "val_put");
  VerifyGetBothIndexes("key_02_timed", "val_timed");
  VerifyGetBothIndexes("key_03_merge", "val_merge");

  // Forward scan via both indexes — all three keys visible in order.
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_put", "val_put"},
        {"key_02_timed", "val_timed"},
        {"key_03_merge", "val_merge"}};
    VerifyForwardScanBothIndexes(expected);
  }
}

// ============================================================================
// Compaction tests
// ============================================================================

TEST_F(TrieIndexDBTest, CompactionWithMixedOpsAndSnapshots) {
  // Multiple flushes followed by compaction with a snapshot held. The snapshot
  // forces compaction to preserve multiple versions of the same user key,
  // exercising the UDI builder's handling of duplicate user keys with different
  // sequence numbers and value types. Verified through both indexes.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Flush 1: initial values.
  ASSERT_OK(db_->Put(WriteOptions(), "key_aa", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_bb", "v1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_cc", "m1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Snapshot pins flush 1 versions so compaction preserves them.
  const Snapshot* snap = db_->GetSnapshot();

  // Flush 2: updates that create new versions.
  ASSERT_OK(db_->Put(WriteOptions(), "key_aa", "v2"));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_bb"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_cc", "m2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Compact all levels. Both versions of each key are preserved because the
  // snapshot prevents garbage collection of the older versions.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Current view (no snapshot): key_aa=v2, key_bb deleted, key_cc="m1,m2".
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_aa", "v2"}, {"key_cc", "m1,m2"}};
    VerifyForwardScanBothIndexes(expected);
  }
  VerifyGetBothIndexes("key_aa", "v2");
  VerifyGetNotFoundBothIndexes("key_bb");
  VerifyGetBothIndexes("key_cc", "m1,m2");

  // Snapshot view: key_aa=v1, key_bb=v1, key_cc="m1".
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_aa", "v1"}, {"key_bb", "v1"}, {"key_cc", "m1"}};
    VerifyForwardScanBothIndexes(snap, expected);
  }
  VerifyGetBothIndexes(snap, "key_aa", "v1");
  VerifyGetBothIndexes(snap, "key_bb", "v1");
  VerifyGetBothIndexes(snap, "key_cc", "m1");

  db_->ReleaseSnapshot(snap);
}

TEST_F(TrieIndexDBTest, CompactionWithAllOperationTypes) {
  // Exercises all operation types (Put, Delete, Merge, SingleDelete, PutEntity)
  // across two flushes with a snapshot, then compacts. Verified through both
  // indexes. This ensures the UDI builder handles the full range of value types
  // in compaction output, and that both the current and snapshot views are
  // correct.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Flush 1: initial values with diverse types.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01_put", "v1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02_merge", "m1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_03_sd_target", "sd_val"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "key_04_entity", WideColumns{{"", "e1"}}));
  ASSERT_OK(db_->Put(WriteOptions(), "key_05_del_target", "del_val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Snapshot pins flush 1 versions.
  const Snapshot* snap = db_->GetSnapshot();

  // Flush 2: updates each key with a different operation type.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01_put", "v2"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02_merge", "m2"));
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "key_03_sd_target"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "key_04_entity", WideColumns{{"", "e2"}}));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_05_del_target"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Current view via both indexes: key_01=v2, key_02="m1,m2", key_03 SD'd,
  // key_04=e2, key_05 deleted.
  VerifyGetBothIndexes("key_01_put", "v2");
  VerifyGetBothIndexes("key_02_merge", "m1,m2");
  VerifyGetNotFoundBothIndexes("key_03_sd_target");
  VerifyGetBothIndexes("key_04_entity", "e2");
  VerifyGetNotFoundBothIndexes("key_05_del_target");

  // Current view scan via both indexes: only key_01, key_02, key_04 visible.
  {
    std::vector<std::string> expected = {"key_01_put", "key_02_merge",
                                         "key_04_entity"};
    VerifyForwardScanBothIndexes(expected);
  }

  // Snapshot view via both indexes: all original flush 1 values visible.
  VerifyGetBothIndexes(snap, "key_01_put", "v1");
  VerifyGetBothIndexes(snap, "key_02_merge", "m1");
  VerifyGetBothIndexes(snap, "key_03_sd_target", "sd_val");
  VerifyGetBothIndexes(snap, "key_04_entity", "e1");
  VerifyGetBothIndexes(snap, "key_05_del_target", "del_val");

  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_put", "v1"},
        {"key_02_merge", "m1"},
        {"key_03_sd_target", "sd_val"},
        {"key_04_entity", "e1"},
        {"key_05_del_target", "del_val"}};
    VerifyForwardScanBothIndexes(snap, expected);
  }

  db_->ReleaseSnapshot(snap);
}

TEST_F(TrieIndexDBTest, TimedPutCompaction) {
  // Verifies that kTypeValuePreferredSeqno entries survive compaction and the
  // UDI builder correctly strips the packed seqno during compaction output.
  // Verified through both indexes.
  options_.compaction_style = kCompactionStyleUniversal;
  options_.preclude_last_level_data_seconds = 10000;
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Flush 1: TimedPut + regular Put.
  {
    WriteBatch wb;
    ASSERT_OK(wb.TimedPut(db_->DefaultColumnFamily(), "key_01_timed",
                          "timed_v1", /*write_unix_time=*/0));
    ASSERT_OK(db_->Write(WriteOptions(), &wb));
  }
  ASSERT_OK(db_->Put(WriteOptions(), "key_02_put", "put_v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Snapshot pins flush 1 versions.
  const Snapshot* snap = db_->GetSnapshot();

  // Flush 2: overwrite both keys with regular Puts.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01_timed", "put_v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_02_put", "put_v2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Compact: the snapshot forces both versions of key_01_timed to be kept.
  // The older version is kTypeValuePreferredSeqno with packed seqno.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Current view via both indexes: both keys have the newer value.
  VerifyGetBothIndexes("key_01_timed", "put_v2");
  VerifyGetBothIndexes("key_02_put", "put_v2");
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_timed", "put_v2"}, {"key_02_put", "put_v2"}};
    VerifyForwardScanBothIndexes(expected);
  }

  // Snapshot view via both indexes: key_01 has the original TimedPut value
  // (packed seqno must be transparent), key_02 has its original value.
  VerifyGetBothIndexes(snap, "key_01_timed", "timed_v1");
  VerifyGetBothIndexes(snap, "key_02_put", "put_v1");
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_timed", "timed_v1"}, {"key_02_put", "put_v1"}};
    VerifyForwardScanBothIndexes(snap, expected);
  }

  db_->ReleaseSnapshot(snap);
}

TEST_F(TrieIndexDBTest, CrossFlushSingleDelete) {
  // Verifies that a SingleDelete in a later SST correctly cancels a Put from
  // an earlier SST after compaction with the trie UDI active. Verified through
  // both indexes.
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Flush 1: Puts.
  ASSERT_OK(db_->Put(WriteOptions(), "key_aa", "val_aa"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_bb", "val_bb"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_cc", "val_cc"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Flush 2: SingleDelete key_bb (targets the Put from flush 1).
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "key_bb"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Before compaction: key_bb is already hidden by the merging iterator.
  VerifyGetNotFoundBothIndexes("key_bb");

  // After compaction: SingleDelete + Put fully cancel out, key_bb is gone.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  VerifyGetNotFoundBothIndexes("key_bb");

  // Remaining keys unaffected via both indexes.
  VerifyGetBothIndexes("key_aa", "val_aa");
  VerifyGetBothIndexes("key_cc", "val_cc");

  {
    std::vector<std::string> expected = {"key_aa", "key_cc"};
    VerifyForwardScanBothIndexes(expected);
  }
}

// ============================================================================
// Iteration tests
// ============================================================================

TEST_F(TrieIndexDBTest, ReverseIteration) {
  // Verifies that reverse iteration (SeekToLast, Prev, SeekForPrev) works
  // correctly with mixed operation types. Forward scan and point lookups are
  // verified through both indexes. Reverse operations use the standard index
  // (the trie UDI iterator does not yet support SeekToLast/Prev/SeekForPrev).
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key_01", "v1"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02", "m1"));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_03"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_04", "v4"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key_05",
                           WideColumns{{"", "e5"}}));
  ASSERT_OK(db_->Put(WriteOptions(), "key_06", "v6"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Visible keys: key_01, key_02, key_04, key_05, key_06 (key_03 deleted).

  // Forward scan via both indexes.
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01", "v1"},
        {"key_02", "m1"},
        {"key_04", "v4"},
        {"key_05", "e5"},
        {"key_06", "v6"}};
    VerifyForwardScanBothIndexes(expected);
  }

  // Point lookups via both indexes.
  VerifyGetBothIndexes("key_01", "v1");
  VerifyGetBothIndexes("key_02", "m1");
  VerifyGetNotFoundBothIndexes("key_03");
  VerifyGetBothIndexes("key_04", "v4");
  VerifyGetBothIndexes("key_05", "e5");
  VerifyGetBothIndexes("key_06", "v6");

  // Seek via both indexes.
  VerifySeekBothIndexes("key_04", "key_04", "v4");
  VerifySeekBothIndexes("key_05", "key_05", "e5");

  // Reverse operations below use the standard index only.

  // SeekToLast + full reverse scan.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToLast();
    std::vector<std::string> reverse_keys;
    for (; iter->Valid(); iter->Prev()) {
      reverse_keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    std::vector<std::string> expected = {"key_06", "key_05", "key_04", "key_02",
                                         "key_01"};
    ASSERT_EQ(reverse_keys, expected);
  }

  // SeekForPrev to an exact visible key.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekForPrev("key_04");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_04");
    ASSERT_EQ(iter->value().ToString(), "v4");
  }

  // SeekForPrev to a deleted key — should land on the largest visible key
  // that is <= "key_03", which is key_02.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekForPrev("key_03");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_02");
  }

  // SeekForPrev to a key between existing keys.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekForPrev("key_04_5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_04");
  }

  // SeekForPrev before all keys — should be invalid.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekForPrev("key_00");
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Prev from a Seek position in the middle of the range.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->Seek("key_05");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_05");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_04");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_02");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_01");

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// ============================================================================
// DeleteRange test
// ============================================================================

TEST_F(TrieIndexDBTest, DeleteRangeWithTrieUDI) {
  // Verifies that DeleteRange (kTypeRangeDeletion) works correctly alongside
  // the trie UDI. Range deletions go to a separate range_del_block (not
  // through OnKeyAdded), but we verify that reads correctly filter out
  // range-deleted keys when the trie UDI is active. Forward scan and point
  // lookups verified through both indexes; reverse scan uses standard index.
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  for (int i = 1; i <= 10; i++) {
    char key_buf[16];
    char val_buf[16];
    snprintf(key_buf, sizeof(key_buf), "key_%02d", i);
    snprintf(val_buf, sizeof(val_buf), "val_%02d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key_buf, val_buf));
  }

  // DeleteRange [key_04, key_08) — deletes key_04 through key_07.
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             "key_04", "key_08"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Forward scan via both indexes: key_01..key_03 and key_08..key_10 visible.
  {
    std::vector<std::string> expected = {"key_01", "key_02", "key_03",
                                         "key_08", "key_09", "key_10"};
    VerifyForwardScanBothIndexes(expected);
  }

  // Point lookups via both indexes for deleted keys.
  VerifyGetNotFoundBothIndexes("key_04");
  VerifyGetNotFoundBothIndexes("key_07");

  // Point lookups via both indexes for surviving keys at boundaries.
  VerifyGetBothIndexes("key_03", "val_03");
  VerifyGetBothIndexes("key_08", "val_08");

  // Reverse scan (standard index only) should also respect the range deletion.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToLast();
    std::vector<std::string> reverse_keys;
    for (; iter->Valid(); iter->Prev()) {
      reverse_keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    std::vector<std::string> expected = {"key_10", "key_09", "key_08",
                                         "key_03", "key_02", "key_01"};
    ASSERT_EQ(reverse_keys, expected);
  }
}

// ============================================================================
// DB reopen test
// ============================================================================

TEST_F(TrieIndexDBTest, ReopenWithMixedOperationTypes) {
  // Writes all operation types, flushes, closes the DB, reopens, and verifies
  // all data reads correctly from cold SST files through both indexes. This
  // exercises the read path on a freshly opened DB where no memtable data
  // exists.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key_01", "val_put"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02", "val_merge"));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_03"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_04", "sd_target"));
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "key_04"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key_05",
                           WideColumns{{"", "entity_val"}}));
  ASSERT_OK(db_->Put(WriteOptions(), "key_06", "val_put2"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Close the DB. All data is now only in SST files.
  ASSERT_OK(db_->Close());
  db_.reset();

  // Reopen with the same trie UDI configuration.
  ASSERT_OK(OpenDB());

  // Point lookups on cold data via both indexes.
  VerifyGetBothIndexes("key_01", "val_put");
  VerifyGetBothIndexes("key_02", "val_merge");
  VerifyGetNotFoundBothIndexes("key_03");
  VerifyGetNotFoundBothIndexes("key_04");
  VerifyGetBothIndexes("key_05", "entity_val");
  VerifyGetBothIndexes("key_06", "val_put2");

  // Forward scan via both indexes.
  {
    std::vector<std::string> expected = {"key_01", "key_02", "key_05",
                                         "key_06"};
    VerifyForwardScanBothIndexes(expected);
  }

  // Reverse scan on cold data (standard index only).
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToLast();
    std::vector<std::string> reverse_keys;
    for (; iter->Valid(); iter->Prev()) {
      reverse_keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    std::vector<std::string> expected = {"key_06", "key_05", "key_02",
                                         "key_01"};
    ASSERT_EQ(reverse_keys, expected);
  }
}

// ============================================================================
// Ingest external file test
// ============================================================================

TEST_F(TrieIndexDBTest, IngestExternalFileWithTrieUDI) {
  // Creates an SST with SstFileWriter using the trie UDI, then ingests it
  // into a live DB that also has trie UDI configured. Verifies that both the
  // existing DB data and the ingested data are correctly readable through both
  // indexes.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Write some data directly to the DB and flush.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01", "db_val1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_05", "db_val5"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Create an SST with SstFileWriter using trie UDI, containing mixed ops.
  std::string sst_path = dbname_ + "/ingest.sst";
  {
    Options sst_options;
    sst_options.merge_operator = MergeOperators::CreateStringAppendOperator();
    BlockBasedTableOptions table_options;
    table_options.user_defined_index_factory = trie_factory_;
    sst_options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    SstFileWriter writer(EnvOptions(), sst_options);
    ASSERT_OK(writer.Open(sst_path));
    ASSERT_OK(writer.Put("key_02", "ingest_val2"));
    ASSERT_OK(writer.Merge("key_03", "ingest_merge3"));
    ASSERT_OK(writer.Delete("key_04"));
    ASSERT_OK(writer.Put("key_06", "ingest_val6"));
    ASSERT_OK(writer.Finish());
  }

  // Ingest into the live DB.
  IngestExternalFileOptions ingest_opts;
  ASSERT_OK(db_->IngestExternalFile({sst_path}, ingest_opts));

  // Point lookups via both indexes — combined DB + ingested data.
  VerifyGetBothIndexes("key_01", "db_val1");
  VerifyGetBothIndexes("key_02", "ingest_val2");
  VerifyGetBothIndexes("key_03", "ingest_merge3");
  // key_04: ingested Delete tombstone, no prior value — NotFound.
  VerifyGetNotFoundBothIndexes("key_04");
  VerifyGetBothIndexes("key_05", "db_val5");
  VerifyGetBothIndexes("key_06", "ingest_val6");

  // Forward scan via both indexes.
  {
    std::vector<std::string> expected = {"key_01", "key_02", "key_03", "key_05",
                                         "key_06"};
    VerifyForwardScanBothIndexes(expected);
  }
}

// ============================================================================
// WriteBatch test
// ============================================================================

TEST_F(TrieIndexDBTest, WriteBatchWithMixedOperations) {
  // Verifies that a single WriteBatch containing multiple operation types
  // (Put, Delete, Merge, SingleDelete, PutEntity) works correctly with the
  // trie UDI. Verified through both indexes. Real-world workloads typically
  // batch multiple operations.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // Pre-populate a key that the batch's Delete will target.
  ASSERT_OK(db_->Put(WriteOptions(), "key_02_del", "pre_val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Build a WriteBatch with all operation types.
  WriteBatch wb;
  ASSERT_OK(wb.Put(db_->DefaultColumnFamily(), "key_01_put", "batch_put"));
  ASSERT_OK(wb.Delete(db_->DefaultColumnFamily(), "key_02_del"));
  ASSERT_OK(wb.Merge(db_->DefaultColumnFamily(), "key_03_merge", "batch_m"));
  // Put + SingleDelete within the same batch — they cancel out.
  ASSERT_OK(wb.Put(db_->DefaultColumnFamily(), "key_04_sd", "sd_target"));
  ASSERT_OK(wb.SingleDelete(db_->DefaultColumnFamily(), "key_04_sd"));
  ASSERT_OK(wb.PutEntity(db_->DefaultColumnFamily(), "key_05_entity",
                         WideColumns{{"", "batch_entity"}}));

  ASSERT_OK(db_->Write(WriteOptions(), &wb));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Point lookups via both indexes. Expected visible keys: key_01 (Put),
  // key_03 (Merge), key_05 (PutEntity). key_02 deleted, key_04
  // Put+SingleDelete cancel.
  VerifyGetBothIndexes("key_01_put", "batch_put");
  VerifyGetNotFoundBothIndexes("key_02_del");
  VerifyGetBothIndexes("key_03_merge", "batch_m");
  VerifyGetNotFoundBothIndexes("key_04_sd");
  VerifyGetBothIndexes("key_05_entity", "batch_entity");

  // Forward scan via both indexes.
  {
    std::vector<std::string> expected = {"key_01_put", "key_03_merge",
                                         "key_05_entity"};
    VerifyForwardScanBothIndexes(expected);
  }
}

// ============================================================================
// Large-scale test
// ============================================================================

TEST_F(TrieIndexDBTest, LargeMixedOperationsAcrossBlocks) {
  // Large-scale test with many keys of different operation types and a small
  // block size. This stresses block boundary handling in the trie UDI across
  // Put, Delete, Merge, SingleDelete, and PutEntity entries. Verified through
  // both indexes.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  // Small block size forces many data blocks, exercising the trie index's
  // AddIndexEntry at frequent block boundaries.
  ASSERT_OK(OpenDB(/*block_size=*/128));

  const int kNumKeys = 500;
  // Track keys expected to be visible after flush (non-deleted, non-SD'd).
  std::vector<std::string> expected_visible;

  for (int i = 0; i < kNumKeys; i++) {
    char key_buf[32];
    snprintf(key_buf, sizeof(key_buf), "key_%06d", i);
    std::string key(key_buf);

    // Distribute operation types:
    //   i%10 in [0,3] -> Put         (40%)
    //   i%10 in [4,5] -> Delete      (20%)
    //   i%10 in [6,7] -> Merge       (20%)
    //   i%10 == 8     -> SingleDelete (10%, preceded by Put -- both cancel)
    //   i%10 == 9     -> PutEntity   (10%)
    int type = i % 10;
    if (type <= 3) {
      char val_buf[32];
      snprintf(val_buf, sizeof(val_buf), "val_%06d", i);
      ASSERT_OK(db_->Put(WriteOptions(), key, val_buf));
      expected_visible.push_back(key);
    } else if (type <= 5) {
      ASSERT_OK(db_->Delete(WriteOptions(), key));
      // Bare tombstone — not visible.
    } else if (type <= 7) {
      char val_buf[32];
      snprintf(val_buf, sizeof(val_buf), "merge_%06d", i);
      ASSERT_OK(db_->Merge(WriteOptions(), key, val_buf));
      expected_visible.push_back(key);
    } else if (type == 8) {
      ASSERT_OK(db_->Put(WriteOptions(), key, "to_be_deleted"));
      ASSERT_OK(db_->SingleDelete(WriteOptions(), key));
      // Put + SingleDelete cancel — not visible.
    } else {
      ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                               WideColumns{{"", "entity_val"}}));
      expected_visible.push_back(key);
    }
  }

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Forward scan via both indexes — verify exactly the expected visible keys.
  VerifyForwardScanBothIndexes(expected_visible);

  // Spot-check: Seek to every 10th visible key via both indexes.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    for (size_t i = 0; i < expected_visible.size(); i += 10) {
      iter->Seek(expected_visible[i]);
      ASSERT_TRUE(iter->Valid()) << "Seek failed for " << expected_visible[i];
      ASSERT_EQ(iter->key().ToString(), expected_visible[i]);
    }
  }
}

// ============================================================================
// Seqno side-table tests (same user key spanning data block boundaries)
// ============================================================================

TEST_F(TrieIndexDBTest, SameUserKeyAcrossBlockBoundaries) {
  // Forces the same user key to appear in multiple data blocks by writing many
  // versions with snapshots held to prevent garbage collection, using a tiny
  // block_size. This exercises the trie's seqno side-table: the trie stores
  // only one separator per user key, and the side-table records the seqno +
  // overflow block count so that Seek() can find the correct data block for
  // each version.
  //
  // Without the seqno side-table fix (PR #14412), reads through the trie index
  // would return incorrect data when multiple versions of the same key span
  // different data blocks.
  options_.disable_auto_compactions = true;
  // Tiny block_size (64 bytes) forces each version of the key into its own
  // data block, creating same-user-key block boundaries that the trie must
  // handle via the seqno side-table.
  ASSERT_OK(OpenDB(/*block_size=*/64));

  // Write multiple versions of the same key, holding snapshots so all versions
  // survive the flush to a single SST file.
  const std::string key = "same_key";
  constexpr int kNumVersions = 10;
  std::vector<const Snapshot*> snaps;
  for (int i = 0; i < kNumVersions; i++) {
    std::string val = "ver_" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, val));
    snaps.push_back(db_->GetSnapshot());
  }

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Current view: latest version visible.
  VerifyGetBothIndexes(key, "ver_" + std::to_string(kNumVersions - 1));

  // Each snapshot should see the version written at or before its creation.
  for (int i = 0; i < kNumVersions; i++) {
    std::string expected_val = "ver_" + std::to_string(i);
    VerifyGetBothIndexes(snaps[i], key, expected_val);
  }

  // Forward scan with each snapshot should return exactly one key with the
  // correct version.
  for (int i = 0; i < kNumVersions; i++) {
    std::string expected_val = "ver_" + std::to_string(i);
    std::vector<std::pair<std::string, std::string>> expected = {
        {key, expected_val}};
    VerifyForwardScanBothIndexes(snaps[i], expected);
  }

  // Seek to the key through the trie index with each snapshot — the trie's
  // post-seek correction must advance through overflow blocks to find the
  // correct version for each seqno.
  for (int i = 0; i < kNumVersions; i++) {
    std::string expected_val = "ver_" + std::to_string(i);
    SCOPED_TRACE("snap=" + std::to_string(i));
    VerifySeekBothIndexes(snaps[i], key, key, expected_val);
  }

  for (auto* snap : snaps) {
    db_->ReleaseSnapshot(snap);
  }
}

TEST_F(TrieIndexDBTest, SameUserKeyPutThenDeleteAcrossBlocks) {
  // Same user key with a Put followed by a Delete, where both entries land in
  // different data blocks. A snapshot pins the Put version. After compaction,
  // the current view shows NotFound while the snapshot view shows the Put.
  // This tests the seqno side-table with mixed value types for the same key.
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/64));

  // Write a Put, take snapshot, then Delete.
  ASSERT_OK(db_->Put(WriteOptions(), "del_key", "put_value"));
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_OK(db_->Delete(WriteOptions(), "del_key"));

  // Add surrounding keys to create more data blocks and exercise trie
  // separators around the duplicated key.
  ASSERT_OK(db_->Put(WriteOptions(), "aaa_before", "before_val"));
  ASSERT_OK(db_->Put(WriteOptions(), "zzz_after", "after_val"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Current view: del_key is deleted.
  VerifyGetNotFoundBothIndexes("del_key");
  VerifyGetBothIndexes("aaa_before", "before_val");
  VerifyGetBothIndexes("zzz_after", "after_val");

  // Snapshot view: del_key is visible with the Put value.
  VerifyGetBothIndexes(snap, "del_key", "put_value");

  // Seek to del_key with snapshot through both indexes.
  VerifySeekBothIndexes(snap, "del_key", "del_key", "put_value");

  // Compact to merge the Put + Delete. Snapshot prevents GC of the Put.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // After compaction, same behavior.
  VerifyGetNotFoundBothIndexes("del_key");
  VerifyGetBothIndexes(snap, "del_key", "put_value");

  db_->ReleaseSnapshot(snap);
}

TEST_F(TrieIndexDBTest, SameUserKeyManyVersionsSeekCorrectness) {
  // Writes many versions of three different keys (with snapshots), using a
  // tiny block_size to force same-user-key block boundaries. Verifies that
  // Seek + Get through the trie index returns the correct version for each
  // snapshot, testing the seqno side-table's overflow handling with multiple
  // keys interleaved in the SST.
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/64));

  const std::vector<std::string> keys = {"key_aaa", "key_mmm", "key_zzz"};
  constexpr int kVersionsPerKey = 8;
  // snaps[v] is taken after writing version v of all keys.
  std::vector<const Snapshot*> snaps;

  for (int v = 0; v < kVersionsPerKey; v++) {
    for (const auto& k : keys) {
      std::string val = k + "_v" + std::to_string(v);
      ASSERT_OK(db_->Put(WriteOptions(), k, val));
    }
    snaps.push_back(db_->GetSnapshot());
  }

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify each snapshot sees the correct version of each key via Get and Seek.
  for (int v = 0; v < kVersionsPerKey; v++) {
    for (const auto& k : keys) {
      std::string expected_val = k + "_v" + std::to_string(v);
      SCOPED_TRACE("key=" + k + " v=" + std::to_string(v));
      VerifyGetBothIndexes(snaps[v], k, expected_val);
      VerifySeekBothIndexes(snaps[v], k, k, expected_val);
    }
  }

  // Compact and re-verify. Compaction must preserve all versions because
  // snapshots are held.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (int v = 0; v < kVersionsPerKey; v++) {
    for (const auto& k : keys) {
      std::string expected_val = k + "_v" + std::to_string(v);
      VerifyGetBothIndexes(snaps[v], k, expected_val);
    }
  }

  for (auto* snap : snaps) {
    db_->ReleaseSnapshot(snap);
  }
}

// ============================================================================
// MultiGet test
// ============================================================================

TEST_F(TrieIndexDBTest, MultiGetWithTrieUDI) {
  // Verifies that the batched MultiGet API works correctly with the trie UDI.
  // MultiGet is a separate code path from single Get and uses batched block
  // lookups, so it needs dedicated testing.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  // Write a mix of operation types.
  ASSERT_OK(db_->Put(WriteOptions(), "key_01", "val_01"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key_02", "merge_02"));
  ASSERT_OK(db_->Delete(WriteOptions(), "key_03"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_04", "val_04"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key_05",
                           WideColumns{{"", "entity_05"}}));
  ASSERT_OK(db_->Put(WriteOptions(), "key_06", "val_06"));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // MultiGet through both indexes.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");

    std::vector<Slice> mg_keys = {"key_01",         "key_02", "key_03",
                                  "key_04",         "key_05", "key_06",
                                  "key_nonexistent"};
    std::vector<std::string> mg_values(mg_keys.size());
    std::vector<Status> mg_statuses = db_->MultiGet(ro, mg_keys, &mg_values);

    ASSERT_EQ(mg_statuses.size(), mg_keys.size());
    ASSERT_OK(mg_statuses[0]);
    ASSERT_EQ(mg_values[0], "val_01");
    ASSERT_OK(mg_statuses[1]);
    ASSERT_EQ(mg_values[1], "merge_02");
    ASSERT_TRUE(mg_statuses[2].IsNotFound());
    ASSERT_OK(mg_statuses[3]);
    ASSERT_EQ(mg_values[3], "val_04");
    ASSERT_OK(mg_statuses[4]);
    ASSERT_EQ(mg_values[4], "entity_05");
    ASSERT_OK(mg_statuses[5]);
    ASSERT_EQ(mg_values[5], "val_06");
    ASSERT_TRUE(mg_statuses[6].IsNotFound());
  }
}

// ============================================================================
// WAL replay / crash recovery test
// ============================================================================

TEST_F(TrieIndexDBTest, WALReplayRecovery) {
  // Writes data without flushing, then closes and reopens the DB. The data
  // must be recovered from the WAL and then flushed. This tests that the trie
  // UDI builder handles entries replayed from the WAL correctly.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  // WAL is enabled by default (WriteOptions::disableWAL = false).
  ASSERT_OK(OpenDB());

  // Write data — do NOT flush. Data lives only in the WAL + memtable.
  ASSERT_OK(db_->Put(WriteOptions(), "wal_key_01", "wal_val_01"));
  ASSERT_OK(db_->Merge(WriteOptions(), "wal_key_02", "wal_merge"));
  ASSERT_OK(db_->Put(WriteOptions(), "wal_key_03", "wal_val_03"));
  ASSERT_OK(db_->Delete(WriteOptions(), "wal_key_03"));
  ASSERT_OK(db_->Put(WriteOptions(), "wal_key_04", "wal_val_04"));

  // Close and reopen — triggers WAL replay.
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(OpenDB());

  // After WAL replay, data should be in a memtable. Flush to create SST with
  // the trie UDI.
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify data through both indexes.
  VerifyGetBothIndexes("wal_key_01", "wal_val_01");
  VerifyGetBothIndexes("wal_key_02", "wal_merge");
  VerifyGetNotFoundBothIndexes("wal_key_03");
  VerifyGetBothIndexes("wal_key_04", "wal_val_04");

  // Forward scan.
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"wal_key_01", "wal_val_01"},
        {"wal_key_02", "wal_merge"},
        {"wal_key_04", "wal_val_04"}};
    VerifyForwardScanBothIndexes(expected);
  }
}

// ============================================================================
// Multiple column families test
// ============================================================================

TEST_F(TrieIndexDBTest, MultipleColumnFamilies) {
  // Opens a DB with multiple column families, each using the trie UDI. Writes
  // different data to each CF, flushes, and verifies reads through both indexes
  // for each CF. This tests that the UDI builder/reader are correctly isolated
  // per-CF.
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.disable_auto_compactions = true;
  options_.create_if_missing = true;

  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;

  // Open with default CF first.
  ASSERT_OK(DB::Open(options_, dbname_, &db_));

  // Create two additional CFs with the same trie UDI options.
  ColumnFamilyHandle* cf1 = nullptr;
  ColumnFamilyHandle* cf2 = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(options_, "cf_one", &cf1));
  ASSERT_OK(db_->CreateColumnFamily(options_, "cf_two", &cf2));

  // Write different data to each CF.
  ASSERT_OK(db_->Put(WriteOptions(), "default_key", "default_val"));
  ASSERT_OK(db_->Put(WriteOptions(), cf1, "cf1_key_a", "cf1_val_a"));
  ASSERT_OK(db_->Merge(WriteOptions(), cf1, "cf1_key_b", "cf1_merge"));
  ASSERT_OK(db_->Put(WriteOptions(), cf2, "cf2_key_x", "cf2_val_x"));
  ASSERT_OK(db_->Delete(WriteOptions(), cf2, "cf2_key_y"));
  ASSERT_OK(db_->Put(WriteOptions(), cf2, "cf2_key_z", "cf2_val_z"));

  // Flush all CFs.
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Flush(FlushOptions(), cf1));
  ASSERT_OK(db_->Flush(FlushOptions(), cf2));

  // Verify default CF.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
    std::string value;
    ASSERT_OK(db_->Get(ro, "default_key", &value));
    ASSERT_EQ(value, "default_val");
  }

  // Verify cf_one through both indexes.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
    std::string value;
    ASSERT_OK(db_->Get(ro, cf1, "cf1_key_a", &value));
    ASSERT_EQ(value, "cf1_val_a");
    ASSERT_OK(db_->Get(ro, cf1, "cf1_key_b", &value));
    ASSERT_EQ(value, "cf1_merge");
  }

  // Verify cf_two through both indexes.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
    std::string value;
    ASSERT_OK(db_->Get(ro, cf2, "cf2_key_x", &value));
    ASSERT_EQ(value, "cf2_val_x");
    ASSERT_TRUE(db_->Get(ro, cf2, "cf2_key_y", &value).IsNotFound());
    ASSERT_OK(db_->Get(ro, cf2, "cf2_key_z", &value));
    ASSERT_EQ(value, "cf2_val_z");
  }

  // Forward scan on each CF via both indexes.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");

    // cf_one scan.
    std::unique_ptr<Iterator> it1(db_->NewIterator(ro, cf1));
    it1->SeekToFirst();
    ASSERT_TRUE(it1->Valid());
    ASSERT_EQ(it1->key().ToString(), "cf1_key_a");
    it1->Next();
    ASSERT_TRUE(it1->Valid());
    ASSERT_EQ(it1->key().ToString(), "cf1_key_b");
    it1->Next();
    ASSERT_FALSE(it1->Valid());
    ASSERT_OK(it1->status());

    // cf_two scan.
    std::unique_ptr<Iterator> it2(db_->NewIterator(ro, cf2));
    it2->SeekToFirst();
    ASSERT_TRUE(it2->Valid());
    ASSERT_EQ(it2->key().ToString(), "cf2_key_x");
    it2->Next();
    ASSERT_TRUE(it2->Valid());
    ASSERT_EQ(it2->key().ToString(), "cf2_key_z");
    it2->Next();
    ASSERT_FALSE(it2->Valid());
    ASSERT_OK(it2->status());
  }

  // Clean up CF handles before closing.
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf1));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf2));

  // Close the DB. Need to clear db_ first since TearDown will also close.
  ASSERT_OK(db_->Close());
  db_.reset();

  // Reopen with all CFs to verify persistence.
  {
    std::vector<ColumnFamilyDescriptor> cf_descs = {
        ColumnFamilyDescriptor(kDefaultColumnFamilyName, options_),
        ColumnFamilyDescriptor("cf_one", options_),
        ColumnFamilyDescriptor("cf_two", options_)};
    std::vector<ColumnFamilyHandle*> cf_handles;
    std::unique_ptr<DB> reopen_db;
    ASSERT_OK(DB::Open(options_, dbname_, cf_descs, &cf_handles, &reopen_db));
    db_ = std::move(reopen_db);

    // Verify data survives reopen.
    auto ro = TrieIndexReadOptions();
    std::string value;
    ASSERT_OK(db_->Get(ro, cf_handles[0], "default_key", &value));
    ASSERT_EQ(value, "default_val");
    ASSERT_OK(db_->Get(ro, cf_handles[1], "cf1_key_a", &value));
    ASSERT_EQ(value, "cf1_val_a");
    ASSERT_OK(db_->Get(ro, cf_handles[2], "cf2_key_z", &value));
    ASSERT_EQ(value, "cf2_val_z");

    for (auto* h : cf_handles) {
      ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
    }
  }
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
