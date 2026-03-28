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
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/wide_columns.h"
#include "rocksdb/write_batch.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/compression.h"
#include "util/random.h"
#include "utilities/merge_operators.h"
#include "utilities/trie_index/trie_index_factory.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// Encodes an integer as an 8-byte big-endian key body, matching the pattern
// used by db_stress's test_batches_snapshots mode.
static std::string MakeKeyBody(int k) {
  std::string key_body(8, '\0');
  uint64_t val = static_cast<uint64_t>(k);
  for (int i = 7; i >= 0; --i) {
    key_body[i] = static_cast<char>(val & 0xff);
    val >>= 8;
  }
  return key_body;
}

static std::string MakeStressKey(int prefix, int middle, int suffix) {
  return MakeKeyBody(prefix) + MakeKeyBody(middle) + MakeKeyBody(suffix);
}

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
    {
      SCOPED_TRACE("standard index");
      ASSERT_EQ(ScanAllKeys(StandardIndexReadOptions()), expected_keys);
    }
    {
      SCOPED_TRACE("trie index");
      ASSERT_EQ(ScanAllKeys(TrieIndexReadOptions()), expected_keys);
    }
  }

  // Verifies that forward scan via SeekToFirst+Next produces the same
  // (key, value) pairs through both indexes.
  void VerifyForwardScanBothIndexes(
      const std::vector<std::pair<std::string, std::string>>& expected_kvs) {
    {
      SCOPED_TRACE("standard index");
      ASSERT_EQ(ScanAllKeyValues(StandardIndexReadOptions()), expected_kvs);
    }
    {
      SCOPED_TRACE("trie index");
      ASSERT_EQ(ScanAllKeyValues(TrieIndexReadOptions()), expected_kvs);
    }
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
      ASSERT_OK(iter->status());
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
      ASSERT_OK(iter->status());
    }
  }

  // Opens without UDI factory (standard index only). Used to test graceful
  // degradation when reopening a DB that has UDI SSTs.
  Status OpenDBWithoutUDI(int block_size = 0) {
    options_.create_if_missing = true;
    BlockBasedTableOptions table_options;
    // Deliberately no user_defined_index_factory.
    if (block_size > 0) {
      table_options.block_size = block_size;
    }
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    last_options_ = options_;
    return DB::Open(options_, dbname_, &db_);
  }

  // Verify prefix-scan lockstep across `num_prefixes` iterators.
  //
  // Creates one iterator per prefix digit (0..num_prefixes-1), seeks each to
  // its prefix, then walks all in lockstep asserting key bodies match. When
  // `use_upper_bounds` is true, even-numbered iterators get an upper bound
  // set to the next prefix. When `verify_values` is true, value bodies are
  // also cross-checked.
  //
  // Returns the number of keys walked (per-prefix).
  uint64_t VerifyPrefixScanLockstep(const ReadOptions& base_ro,
                                    int num_prefixes, bool use_upper_bounds,
                                    bool verify_values,
                                    const std::string& trace_context = "") {
    std::vector<std::unique_ptr<Iterator>> iters(num_prefixes);
    std::vector<std::string> prefixes(num_prefixes);
    std::vector<Slice> prefix_slices(num_prefixes);
    std::vector<ReadOptions> ro_copies(num_prefixes);
    std::vector<std::string> upper_bounds(num_prefixes);
    std::vector<Slice> ub_slices(num_prefixes);

    for (int d = 0; d < num_prefixes; ++d) {
      prefixes[d] = std::to_string(d);
      prefix_slices[d] = Slice(prefixes[d]);
      ro_copies[d] = base_ro;
      if (use_upper_bounds && d % 2 == 0) {
        upper_bounds[d] = prefixes[d];
        upper_bounds[d].back()++;
        ub_slices[d] = upper_bounds[d];
        ro_copies[d].iterate_upper_bound = &ub_slices[d];
      }
      iters[d].reset(db_->NewIterator(ro_copies[d]));
      iters[d]->Seek(prefix_slices[d]);
    }

    uint64_t count = 0;
    while (iters[0]->Valid() && iters[0]->key().starts_with(prefix_slices[0])) {
      count++;
      std::vector<std::string> keys(num_prefixes);
      std::vector<std::string> values(num_prefixes);
      for (int d = 0; d < num_prefixes; ++d) {
        EXPECT_TRUE(iters[d]->Valid())
            << trace_context << " iter " << d << " invalid at step " << count;
        EXPECT_TRUE(iters[d]->key().starts_with(prefix_slices[d]))
            << trace_context << " iter " << d << " out of prefix at step "
            << count;
        if (!iters[d]->Valid()) {
          return count;
        }
        keys[d] = iters[d]->key().ToString();
        values[d] = iters[d]->value().ToString();
      }

      std::string key0_body = keys[0].substr(1);
      for (int d = 1; d < num_prefixes; ++d) {
        EXPECT_EQ(key0_body, keys[d].substr(1))
            << trace_context << " key body mismatch at step " << count
            << " iter " << d;
      }

      if (verify_values) {
        std::string val0 = values[0];
        if (!val0.empty()) {
          val0.pop_back();
        }
        for (int d = 1; d < num_prefixes; ++d) {
          std::string vald = values[d];
          if (!vald.empty()) {
            vald.pop_back();
          }
          EXPECT_EQ(val0, vald) << trace_context << " value mismatch at step "
                                << count << " iter " << d;
        }
      }

      for (int d = 0; d < num_prefixes; ++d) {
        iters[d]->Next();
      }
    }

    EXPECT_OK(iters[0]->status());
    for (int d = 1; d < num_prefixes; ++d) {
      EXPECT_TRUE(!iters[d]->Valid() ||
                  !iters[d]->key().starts_with(prefix_slices[d]))
          << trace_context << " iter " << d
          << " still has keys after iter 0 finished";
      EXPECT_OK(iters[d]->status());
    }

    return count;
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
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
  }

  // Point lookups via both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01_put", "val_put"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02_merge", "val_merge"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_03_del"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_04_sdel"));
  // PutEntity: Get() returns the value of the default column ("").
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_05_entity", "default_val"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_06_put", "val_put2"));
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
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01_put", "val_put"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02_timed", "val_timed"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_03_merge", "val_merge"));

  // Forward scan via both indexes — all three keys visible in order.
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_put", "val_put"},
        {"key_02_timed", "val_timed"},
        {"key_03_merge", "val_merge"}};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
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
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
  }
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_aa", "v2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_bb"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_cc", "m1,m2"));

  // Snapshot view: key_aa=v1, key_bb=v1, key_cc="m1".
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_aa", "v1"}, {"key_bb", "v1"}, {"key_cc", "m1"}};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(snap, expected));
  }
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_aa", "v1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_bb", "v1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_cc", "m1"));

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
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01_put", "v2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02_merge", "m1,m2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_03_sd_target"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_04_entity", "e2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_05_del_target"));

  // Current view scan via both indexes: only key_01, key_02, key_04 visible.
  {
    std::vector<std::string> expected = {"key_01_put", "key_02_merge",
                                         "key_04_entity"};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
  }

  // Snapshot view via both indexes: all original flush 1 values visible.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_01_put", "v1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_02_merge", "m1"));
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(snap, "key_03_sd_target", "sd_val"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_04_entity", "e1"));
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(snap, "key_05_del_target", "del_val"));

  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_put", "v1"},
        {"key_02_merge", "m1"},
        {"key_03_sd_target", "sd_val"},
        {"key_04_entity", "e1"},
        {"key_05_del_target", "del_val"}};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(snap, expected));
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
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01_timed", "put_v2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02_put", "put_v2"));
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_timed", "put_v2"}, {"key_02_put", "put_v2"}};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
  }

  // Snapshot view via both indexes: key_01 has the original TimedPut value
  // (packed seqno must be transparent), key_02 has its original value.
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(snap, "key_01_timed", "timed_v1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "key_02_put", "put_v1"));
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key_01_timed", "timed_v1"}, {"key_02_put", "put_v1"}};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(snap, expected));
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
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_bb"));

  // After compaction: SingleDelete + Put fully cancel out, key_bb is gone.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_bb"));

  // Remaining keys unaffected via both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_aa", "val_aa"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_cc", "val_cc"));

  {
    std::vector<std::string> expected = {"key_aa", "key_cc"};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
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
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
  }

  // Point lookups via both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01", "v1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02", "m1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_03"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_04", "v4"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_05", "e5"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_06", "v6"));

  // Seek via both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifySeekBothIndexes("key_04", "key_04", "v4"));
  ASSERT_NO_FATAL_FAILURE(VerifySeekBothIndexes("key_05", "key_05", "e5"));

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
    ASSERT_OK(iter->status());
  }

  // SeekForPrev to a deleted key — should land on the largest visible key
  // that is <= "key_03", which is key_02.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekForPrev("key_03");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_02");
    ASSERT_OK(iter->status());
  }

  // SeekForPrev to a key between existing keys.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekForPrev("key_04_5");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_04");
    ASSERT_OK(iter->status());
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
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
  }

  // Point lookups via both indexes for deleted keys.
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_04"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_07"));

  // Point lookups via both indexes for surviving keys at boundaries.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_03", "val_03"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_08", "val_08"));

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
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01", "val_put"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02", "val_merge"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_03"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_04"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_05", "entity_val"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_06", "val_put2"));

  // Forward scan via both indexes.
  {
    std::vector<std::string> expected = {"key_01", "key_02", "key_05",
                                         "key_06"};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
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
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01", "db_val1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02", "ingest_val2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_03", "ingest_merge3"));
  // key_04: ingested Delete tombstone, no prior value — NotFound.
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_04"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_05", "db_val5"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_06", "ingest_val6"));

  // Forward scan via both indexes.
  {
    std::vector<std::string> expected = {"key_01", "key_02", "key_03", "key_05",
                                         "key_06"};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
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
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01_put", "batch_put"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_02_del"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_03_merge", "batch_m"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("key_04_sd"));
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes("key_05_entity", "batch_entity"));

  // Forward scan via both indexes.
  {
    std::vector<std::string> expected = {"key_01_put", "key_03_merge",
                                         "key_05_entity"};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
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
  ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected_visible));

  // Spot-check: Seek to every 10th visible key via both indexes.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    for (size_t i = 0; i < expected_visible.size(); i += 10) {
      iter->Seek(expected_visible[i]);
      ASSERT_TRUE(iter->Valid()) << "Seek failed for " << expected_visible[i];
      ASSERT_EQ(iter->key().ToString(), expected_visible[i]);
    }
    ASSERT_OK(iter->status());
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
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(key, "ver_" + std::to_string(kNumVersions - 1)));

  // Each snapshot should see the version written at or before its creation.
  for (int i = 0; i < kNumVersions; i++) {
    std::string expected_val = "ver_" + std::to_string(i);
    ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snaps[i], key, expected_val));
  }

  // Forward scan with each snapshot should return exactly one key with the
  // correct version.
  for (int i = 0; i < kNumVersions; i++) {
    std::string expected_val = "ver_" + std::to_string(i);
    std::vector<std::pair<std::string, std::string>> expected = {
        {key, expected_val}};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(snaps[i], expected));
  }

  // Seek to the key through the trie index with each snapshot — the trie's
  // post-seek correction must advance through overflow blocks to find the
  // correct version for each seqno.
  for (int i = 0; i < kNumVersions; i++) {
    std::string expected_val = "ver_" + std::to_string(i);
    SCOPED_TRACE("snap=" + std::to_string(i));
    ASSERT_NO_FATAL_FAILURE(
        VerifySeekBothIndexes(snaps[i], key, key, expected_val));
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
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("del_key"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("aaa_before", "before_val"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("zzz_after", "after_val"));

  // Snapshot view: del_key is visible with the Put value.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "del_key", "put_value"));

  // Seek to del_key with snapshot through both indexes.
  ASSERT_NO_FATAL_FAILURE(
      VerifySeekBothIndexes(snap, "del_key", "del_key", "put_value"));

  // Compact to merge the Put + Delete. Snapshot prevents GC of the Put.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // After compaction, same behavior.
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("del_key"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snap, "del_key", "put_value"));

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
      ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snaps[v], k, expected_val));
      ASSERT_NO_FATAL_FAILURE(
          VerifySeekBothIndexes(snaps[v], k, k, expected_val));
    }
  }

  // Compact and re-verify. Compaction must preserve all versions because
  // snapshots are held.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (int v = 0; v < kVersionsPerKey; v++) {
    for (const auto& k : keys) {
      std::string expected_val = k + "_v" + std::to_string(v);
      ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(snaps[v], k, expected_val));
    }
  }

  for (auto* snap : snaps) {
    db_->ReleaseSnapshot(snap);
  }
}

TEST_F(TrieIndexDBTest,
       AutoPrefixBoundsSnapshotIteratorMatchesStandardIndexWithHiddenVersions) {
  options_.compression = kNoCompression;
  options_.disable_auto_compactions = true;
  options_.max_sequential_skip_in_iterations = 2;
  options_.prefix_extractor.reset(NewFixedPrefixTransform(8));
  ASSERT_OK(OpenDB(/*block_size=*/64));

  auto large_value = [](char ch) { return std::string(96, ch); };

  const std::string lower_bound = MakeStressKey(0x45, 0x12B, 0x00A3);
  const std::string before = MakeStressKey(0xB3, 0x12B, 0x012E);
  const std::string repeated = MakeStressKey(0xB3, 0x12B, 0x012F);
  const std::string expected_1 = MakeStressKey(0xB3, 0x12B, 0x0131);
  const std::string expected_2 = MakeStressKey(0xB3, 0x12B, 0x0132);
  const std::string upper_bound = MakeStressKey(0x1D5, 0x12B, 0x0200);

  ASSERT_OK(db_->Put(WriteOptions(), before, large_value('a')));
  for (int version = 0; version < 8; ++version) {
    ASSERT_OK(db_->Put(WriteOptions(), repeated,
                       large_value(static_cast<char>('b' + version))));
  }
  ASSERT_OK(db_->Put(WriteOptions(), expected_1, large_value('m')));
  ASSERT_OK(db_->Put(WriteOptions(), expected_2, large_value('n')));
  ASSERT_OK(db_->Flush(FlushOptions()));

  const Snapshot* snapshot = db_->GetSnapshot();
  const Slice lower_bound_slice(lower_bound);
  const Slice upper_bound_slice(upper_bound);

  const auto build_read_options =
      [&](const UserDefinedIndexFactory* table_index_factory) {
        ReadOptions ro;
        ro.snapshot = snapshot;
        ro.auto_prefix_mode = true;
        ro.allow_unprepared_value = true;
        ro.auto_refresh_iterator_with_snapshot = true;
        ro.iterate_lower_bound = &lower_bound_slice;
        ro.iterate_upper_bound = &upper_bound_slice;
        ro.table_index_factory = table_index_factory;
        return ro;
      };

  const std::vector<std::pair<std::string, std::string>> expected = {
      {before, large_value('a')},
      {repeated, large_value('i')},
      {expected_1, large_value('m')},
      {expected_2, large_value('n')},
  };

  const UserDefinedIndexFactory* table_index_factories[] = {
      nullptr, trie_factory_.get()};
  for (const auto* table_index_factory : table_index_factories) {
    SCOPED_TRACE(table_index_factory == nullptr ? "standard index"
                                                : "trie index");
    const ReadOptions ro = build_read_options(table_index_factory);
    ASSERT_EQ(ScanAllKeyValues(ro), expected);

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->Seek(before);
    for (const auto& [expected_key, expected_value] : expected) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().ToString(), expected_key);
      ASSERT_EQ(iter->value().ToString(), expected_value);
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  db_->ReleaseSnapshot(snapshot);
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
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("wal_key_01", "wal_val_01"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("wal_key_02", "wal_merge"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("wal_key_03"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("wal_key_04", "wal_val_04"));

  // Forward scan.
  {
    std::vector<std::pair<std::string, std::string>> expected = {
        {"wal_key_01", "wal_val_01"},
        {"wal_key_02", "wal_merge"},
        {"wal_key_04", "wal_val_04"}};
    ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(expected));
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

// ---------------------------------------------------------------------------
// BatchedPrefixScan: reproduces the test_batches_snapshots pattern.
//
// Writes batches of 10 keys {digit+key_body : value_body+digit} for digit in
// 0..9, exactly as the crash-test stress tool does. Then scans each prefix
// concurrently (same snapshot) and checks that:
//   (a) all 10 iterators yield the same key bodies in lockstep, and
//   (b) the values stripped of the trailing digit are identical across
//   prefixes.
//
// We run with both the standard index and the trie index and compare.
// ---------------------------------------------------------------------------
TEST_F(TrieIndexDBTest, BatchedPrefixScan) {
  // Small block size to force many data blocks (and thus many trie entries).
  ASSERT_OK(OpenDB(/*block_size=*/256));

  const int kNumBatches = 200;
  const int kNumPrefixes = 10;
  Random rnd(42);

  // Phase 1: Write batches.
  for (int b = 0; b < kNumBatches; ++b) {
    WriteBatch batch;
    std::string key_body = MakeKeyBody(b);
    std::string value_body = rnd.RandomString(20);

    for (int d = kNumPrefixes - 1; d >= 0; --d) {
      std::string k = std::to_string(d) + key_body;
      std::string v = value_body + std::to_string(d);
      ASSERT_OK(batch.Put(k, v));
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
  }

  // Flush so data is in SSTs with trie index.
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Phase 2: Prefix scan with both indexes.
  for (int idx_type = 0; idx_type < 2; ++idx_type) {
    ReadOptions base_ro =
        idx_type == 0 ? StandardIndexReadOptions() : TrieIndexReadOptions();
    SCOPED_TRACE(idx_type == 0 ? "standard index" : "trie index");

    const Snapshot* snap = db_->GetSnapshot();
    base_ro.snapshot = snap;

    uint64_t count = VerifyPrefixScanLockstep(base_ro, kNumPrefixes,
                                              /*use_upper_bounds=*/true,
                                              /*verify_values=*/true);
    ASSERT_EQ(count, static_cast<uint64_t>(kNumBatches))
        << "expected " << kNumBatches << " entries per prefix";

    db_->ReleaseSnapshot(snap);
  }
}

// Same as above but with multiple flushes, compaction, and a DB reopen
// in between to simulate the crash-recovery path.
TEST_F(TrieIndexDBTest, BatchedPrefixScanAfterReopen) {
  ASSERT_OK(OpenDB(/*block_size=*/256));

  const int kNumBatches = 100;
  const int kNumPrefixes = 10;
  Random rnd(123);

  for (int b = 0; b < kNumBatches; ++b) {
    WriteBatch batch;
    std::string key_body = MakeKeyBody(b);
    std::string value_body = rnd.RandomString(20);

    for (int d = kNumPrefixes - 1; d >= 0; --d) {
      std::string k = std::to_string(d) + key_body;
      std::string v = value_body + std::to_string(d);
      ASSERT_OK(batch.Put(k, v));
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));

    // Flush every 20 batches to create multiple SSTs.
    if ((b + 1) % 20 == 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Compact to merge SSTs.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Close and reopen (simulating recovery).
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(OpenDB(/*block_size=*/256));

  // Prefix scan with trie index after reopen.
  ReadOptions base_ro = TrieIndexReadOptions();
  const Snapshot* snap = db_->GetSnapshot();
  base_ro.snapshot = snap;

  uint64_t count =
      VerifyPrefixScanLockstep(base_ro, kNumPrefixes, /*use_upper_bounds=*/true,
                               /*verify_values=*/false);
  ASSERT_EQ(count, static_cast<uint64_t>(kNumBatches));
  db_->ReleaseSnapshot(snap);
}

// Test with overwrites: multiple writes to the same key body, ensuring
// the latest value is consistent across all prefixes.
TEST_F(TrieIndexDBTest, BatchedPrefixScanWithOverwrites) {
  ASSERT_OK(OpenDB(/*block_size=*/256));

  const int kNumKeys = 50;
  const int kNumOverwrites = 5;
  const int kNumPrefixes = 10;
  Random rnd(999);

  // Write each key body multiple times.
  for (int round = 0; round < kNumOverwrites; ++round) {
    for (int k = 0; k < kNumKeys; ++k) {
      WriteBatch batch;
      std::string key_body = MakeKeyBody(k);
      std::string value_body = rnd.RandomString(20);

      for (int d = kNumPrefixes - 1; d >= 0; --d) {
        std::string key = std::to_string(d) + key_body;
        std::string v = value_body + std::to_string(d);
        ASSERT_OK(batch.Put(key, v));
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
    }

    // Flush after each round.
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  // Now verify with both indexes.
  for (int idx_type = 0; idx_type < 2; ++idx_type) {
    ReadOptions base_ro =
        idx_type == 0 ? StandardIndexReadOptions() : TrieIndexReadOptions();
    SCOPED_TRACE(idx_type == 0 ? "standard index" : "trie index");

    const Snapshot* snap = db_->GetSnapshot();
    base_ro.snapshot = snap;

    uint64_t count = VerifyPrefixScanLockstep(base_ro, kNumPrefixes,
                                              /*use_upper_bounds=*/false,
                                              /*verify_values=*/true);
    ASSERT_EQ(count, static_cast<uint64_t>(kNumKeys));
    db_->ReleaseSnapshot(snap);
  }
}

// Stress-like test: write + delete + rewrite many keys, flush between rounds,
// then verify prefix scan consistency. Simulates the crash test pattern that
// triggered failures.
TEST_F(TrieIndexDBTest, BatchedPrefixScanStressLike) {
  ASSERT_OK(OpenDB(/*block_size=*/4096));

  const int kMaxKey = 10000;
  const int kNumPrefixes = 10;
  const int kNumRounds = 20;
  Random rnd(7777);

  for (int round = 0; round < kNumRounds; ++round) {
    // Write a batch of random keys
    int num_writes = 100 + rnd.Uniform(200);
    for (int w = 0; w < num_writes; ++w) {
      int k = rnd.Uniform(kMaxKey);
      WriteBatch batch;
      std::string key_body = MakeKeyBody(k);
      std::string value_body = rnd.RandomString(rnd.Uniform(60) + 4);
      for (int d = kNumPrefixes - 1; d >= 0; --d) {
        std::string key = std::to_string(d) + key_body;
        std::string v = value_body + std::to_string(d);
        ASSERT_OK(batch.Put(key, v));
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
    }

    // Delete some random keys
    int num_deletes = 50 + rnd.Uniform(100);
    for (int w = 0; w < num_deletes; ++w) {
      int k = rnd.Uniform(kMaxKey);
      WriteBatch batch;
      std::string key_body = MakeKeyBody(k);
      for (int d = kNumPrefixes - 1; d >= 0; --d) {
        std::string key = std::to_string(d) + key_body;
        ASSERT_OK(batch.Delete(key));
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
    }

    // Flush every few rounds
    if (round % 3 == 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    }

    // Verify prefix scan consistency with trie index.
    {
      ReadOptions base_ro = TrieIndexReadOptions();
      const Snapshot* snap = db_->GetSnapshot();
      base_ro.snapshot = snap;

      VerifyPrefixScanLockstep(base_ro, kNumPrefixes,
                               /*use_upper_bounds=*/false,
                               /*verify_values=*/true,
                               "round=" + std::to_string(round));

      db_->ReleaseSnapshot(snap);
    }
  }
}

// ---------------------------------------------------------------------------
// Regression test for the FindShortSuccessor last-block bug.
//
// Before the fix, TrieIndexBuilder::AddIndexEntry called
// FindShortSuccessor() on the last block's separator key, producing a
// shorter key that covered a wider range than the actual data. For example,
// if the last key's user key was "9\xff\xff", FindShortSuccessor would
// produce ":" (0x3A), making the trie claim it covers keys up to ":". A
// seek for "9\xff\xff\x01" (between the real last key and ":") would find a
// block via the trie but not via the standard index, causing prefix scan
// iterators to desynchronize.
//
// The standard ShortenedIndexBuilder (with default kShortenSeparators mode)
// does NOT call FindShortSuccessor on the last block — it uses the last key
// as-is. The fix makes the trie builder match this behavior.
// ---------------------------------------------------------------------------
TEST_F(TrieIndexDBTest, LastBlockSeparatorNotShortened) {
  // Use a small block size so each key lands in its own block.
  ASSERT_OK(OpenDB(/*block_size=*/32));

  // Write keys where the last key has trailing 0xFF bytes, which
  // FindShortSuccessor would shorten by incrementing the byte before the
  // 0xFF suffix ("9\xff\xff" -> ":").
  ASSERT_OK(db_->Put(WriteOptions(), "1aaa", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "5bbb", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), std::string("9\xff\xff", 3), "v3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // The key "9\xff\xff\x01" is lexicographically after "9\xff\xff" but
  // before ":" (0x3A). With the old bug, the trie would return a valid
  // block for this key. With the fix, both indexes correctly say "not
  // found".
  std::string seek_target = std::string("9\xff\xff\x01", 4);

  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek(seek_target);
    ASSERT_TRUE(!iter->Valid()) << "Expected no key at or after seek_target, "
                                << "but got: " << iter->key().ToString(true);
    ASSERT_OK(iter->status());
  }

  // Also verify the actual last key is still findable.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek(std::string("9\xff\xff", 3));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), std::string("9\xff\xff", 3));
    ASSERT_EQ(iter->value().ToString(), "v3");

    // After this key, there should be nothing more.
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// Variant: tests that when deletes remove the last key, seeking past the last
// remaining key correctly returns "not found" with both indexes.
TEST_F(TrieIndexDBTest, LastBlockSeparatorWithDeletes) {
  ASSERT_OK(OpenDB(/*block_size=*/32));

  // Write and flush initial data.
  ASSERT_OK(db_->Put(WriteOptions(), "1aaa", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "5bbb", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), std::string("9\xff\xff", 3), "v3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Delete the last key and flush (creates a tombstone in a new SST).
  ASSERT_OK(db_->Delete(WriteOptions(), std::string("9\xff\xff", 3)));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Now seeking for the deleted key should yield "5bbb" or nothing,
  // depending on the seek target. Both indexes must agree.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    // Seek to the deleted key — should skip it and land on nothing (it was
    // the last key).
    iter->Seek(std::string("9\xff\xff", 3));
    ASSERT_TRUE(!iter->Valid())
        << "Deleted key should not be visible, but got: "
        << iter->key().ToString(true);
    ASSERT_OK(iter->status());

    // Seek to a key between "5bbb" and the deleted key — should find "5bbb"
    // or nothing depending on order. Actually, "6" > "5bbb" and "6" <
    // "9\xff\xff", so seeking "6" should find nothing since there's no key
    // >= "6" that's still alive.
    iter->Seek("6");
    ASSERT_TRUE(!iter->Valid()) << "No live key >= '6' should exist, but got: "
                                << iter->key().ToString(true);
    ASSERT_OK(iter->status());
  }

  // Compact to merge the tombstone, then verify again.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "1aaa");
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "5bbb");
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// Single-entry SST: the trie has exactly one leaf. Validates that Seek,
// SeekToFirst, Next, and Get all work with a one-block, one-key SST.
TEST_F(TrieIndexDBTest, SingleEntrySST) {
  ASSERT_OK(OpenDB());
  ASSERT_OK(db_->Put(WriteOptions(), "only_key", "only_val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Point lookup.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("only_key", "only_val"));

  // Forward scan: exactly one result.
  ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(
      std::vector<std::pair<std::string, std::string>>{
          {"only_key", "only_val"}}));

  // Seek to the exact key.
  ASSERT_NO_FATAL_FAILURE(
      VerifySeekBothIndexes("only_key", "only_key", "only_val"));

  // Seek before the key — should land on it.
  ASSERT_NO_FATAL_FAILURE(VerifySeekBothIndexes("a", "only_key", "only_val"));

  // Seek past the key — should be invalid.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie index" : "standard index");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->Seek("z");
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// Deletion-only SST: flush a Put, then flush a Delete for that key so the
// second SST contains only a tombstone. After compaction, the key is gone.
TEST_F(TrieIndexDBTest, DeletionOnlySST) {
  ASSERT_OK(OpenDB());

  // Flush 1: a real Put.
  ASSERT_OK(db_->Put(WriteOptions(), "del_target", "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Flush 2: only a Delete — this creates an SST whose only entry is a
  // tombstone (the trie still builds an index for the block containing it).
  ASSERT_OK(db_->Delete(WriteOptions(), "del_target"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // The tombstone hides the Put.
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("del_target"));

  // Forward scan: nothing visible.
  ASSERT_NO_FATAL_FAILURE(
      VerifyForwardScanBothIndexes(std::vector<std::string>{}));

  // Compact to merge: key is fully removed.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("del_target"));
  ASSERT_NO_FATAL_FAILURE(
      VerifyForwardScanBothIndexes(std::vector<std::string>{}));
}

// All-same-key SST: multiple versions of the same user key (via snapshots)
// land in the same SST, possibly spanning multiple blocks. Validates that
// the trie's same-key-run handling (seqno-based separators) works at the
// DB level through both indexes.
TEST_F(TrieIndexDBTest, AllSameKeySST) {
  options_.disable_auto_compactions = true;
  // Small block size to force multiple blocks for the same user key.
  ASSERT_OK(OpenDB(/*block_size=*/32));

  // Write several versions of the same key with snapshots to prevent GC.
  std::vector<const Snapshot*> snaps;
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(db_->Put(WriteOptions(), "same_key", "val_" + std::to_string(i)));
    snaps.push_back(db_->GetSnapshot());
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Latest value is visible.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("same_key", "val_9"));

  // Forward scan: only the latest version is visible (without snapshot).
  ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(
      std::vector<std::pair<std::string, std::string>>{{"same_key", "val_9"}}));

  // Each snapshot should see the correct version.
  for (int i = 0; i < 10; i++) {
    SCOPED_TRACE("snapshot " + std::to_string(i));
    std::string expected = "val_" + std::to_string(i);
    ASSERT_NO_FATAL_FAILURE(
        VerifyGetBothIndexes(snaps[i], "same_key", expected));

    // Forward scan with snapshot.
    ASSERT_NO_FATAL_FAILURE(
        VerifyForwardScanBothIndexes(snaps[i], {{"same_key", expected}}));
  }

  // Seek with earliest snapshot — should find the earliest version.
  ASSERT_NO_FATAL_FAILURE(
      VerifySeekBothIndexes(snaps[0], "same_key", "same_key", "val_0"));

  for (auto* snap : snaps) {
    db_->ReleaseSnapshot(snap);
  }
}

// Operations on a completely empty DB: nothing should crash, and after
// creating + deleting all data, the DB should correctly return nothing.
TEST_F(TrieIndexDBTest, EmptyDBOperations) {
  ASSERT_OK(OpenDB());

  // Get / Seek / SeekToFirst on empty memtable (no SSTs yet).
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("anything"));
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    iter->Seek("anything");
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Create an SST, delete its only key, compact → DB has no live data but
  // the trie code path was exercised during flush.
  ASSERT_OK(db_->Put(WriteOptions(), "temp", "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Delete(WriteOptions(), "temp"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("temp"));
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// Focused seek-pattern tests: before all data, between blocks, exact match,
// after all data, and empty-key seek.
TEST_F(TrieIndexDBTest, SeekEdgeCases) {
  ASSERT_OK(OpenDB(/*block_size=*/64));

  // Write keys with deliberate gaps.
  for (const auto& k : {"bbb", "ddd", "fff", "hhh"}) {
    ASSERT_OK(db_->Put(WriteOptions(), k, std::string("v_") + k));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    // Before first key.
    iter->Seek("aaa");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "bbb");

    // Exact first key.
    iter->Seek("bbb");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "bbb");

    // Between keys.
    iter->Seek("ccc");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "ddd");

    // Between keys (eee → fff).
    iter->Seek("eee");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "fff");

    // Exact last key.
    iter->Seek("hhh");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "hhh");

    // After last key.
    iter->Seek("zzz");
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    // Empty key (smallest possible key for BytewiseComparator).
    iter->Seek("");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "bbb");
  }
}

// PutEntity + GetEntity through the trie index read path.
TEST_F(TrieIndexDBTest, GetEntityWithTrieUDI) {
  ASSERT_OK(OpenDB());

  // PutEntity with wide columns.
  WideColumns columns{
      {kDefaultWideColumnName, "default_val"},
      {"col_a", "val_a"},
      {"col_b", "val_b"},
  };
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "entity_key", columns));
  // Also a regular Put to verify GetEntity reads it as a single default column.
  ASSERT_OK(db_->Put(WriteOptions(), "regular_key", "regular_val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");

    // GetEntity on a PutEntity key.
    PinnableWideColumns result;
    ASSERT_OK(
        db_->GetEntity(ro, db_->DefaultColumnFamily(), "entity_key", &result));
    ASSERT_EQ(result.columns().size(), 3u);
    ASSERT_EQ(result.columns()[0].name(), kDefaultWideColumnName);
    ASSERT_EQ(result.columns()[0].value(), "default_val");
    ASSERT_EQ(result.columns()[1].name(), "col_a");
    ASSERT_EQ(result.columns()[1].value(), "val_a");
    ASSERT_EQ(result.columns()[2].name(), "col_b");
    ASSERT_EQ(result.columns()[2].value(), "val_b");

    // GetEntity on a regular Put key returns single default column.
    PinnableWideColumns result2;
    ASSERT_OK(db_->GetEntity(ro, db_->DefaultColumnFamily(), "regular_key",
                             &result2));
    ASSERT_EQ(result2.columns().size(), 1u);
    ASSERT_EQ(result2.columns()[0].name(), kDefaultWideColumnName);
    ASSERT_EQ(result2.columns()[0].value(), "regular_val");

    // GetEntity on nonexistent key.
    PinnableWideColumns result3;
    ASSERT_TRUE(
        db_->GetEntity(ro, db_->DefaultColumnFamily(), "no_such_key", &result3)
            .IsNotFound());
  }
}

// Multiple overlapping L0 SSTs: the level iterator must coordinate trie
// iterators across multiple SST files with overlapping key ranges.
TEST_F(TrieIndexDBTest, OverlappingL0SSTs) {
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  // SST1: keys 00..49.
  for (int i = 0; i < 50; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%03d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "sst1_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // SST2: keys 25..74 (overlapping with SST1).
  for (int i = 25; i < 75; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%03d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "sst2_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // SST3: keys 50..99 (overlapping with SST2).
  for (int i = 50; i < 100; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%03d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "sst3_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Verify: latest writer wins for overlapping keys.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    auto kvs = ScanAllKeyValues(ro);
    ASSERT_EQ(kvs.size(), 100u);
    for (int i = 0; i < 100; i++) {
      char key[16];
      snprintf(key, sizeof(key), "key_%03d", i);
      ASSERT_EQ(kvs[i].first, key);
      if (i < 25) {
        ASSERT_EQ(kvs[i].second, "sst1_" + std::to_string(i));
      } else if (i < 50) {
        ASSERT_EQ(kvs[i].second, "sst2_" + std::to_string(i));
      } else {
        ASSERT_EQ(kvs[i].second, "sst3_" + std::to_string(i));
      }
    }
  }

  // Compact all L0 → L1, re-verify.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    ASSERT_EQ(ScanAllKeyValues(ro).size(), 100u);
  }
}

// CompactRange with a sub-range: only part of the key space is compacted.
TEST_F(TrieIndexDBTest, CompactRangeSubset) {
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 26; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%c", 'a' + i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "val_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Compact only the middle range [key_f, key_p).
  std::string begin = "key_f";
  std::string end = "key_p";
  Slice begin_s(begin);
  Slice end_s(end);
  CompactRangeOptions cro;
  ASSERT_OK(db_->CompactRange(cro, &begin_s, &end_s));

  // All 26 keys should still be readable.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    ASSERT_EQ(ScanAllKeys(ro).size(), 26u);
  }
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_a", "val_0"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_z", "val_25"));
}

// Write keys, delete all of them, compact. The DB should be empty.
TEST_F(TrieIndexDBTest, AllKeysDeletedCompaction) {
  ASSERT_OK(OpenDB());

  for (int i = 0; i < 20; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%02d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "val"));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Delete all keys.
  for (int i = 0; i < 20; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%02d", i);
    ASSERT_OK(db_->Delete(WriteOptions(), key));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Before compaction: tombstones hide all keys.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    ASSERT_EQ(ScanAllKeys(ro).size(), 0u);
  }

  // After compaction: all tombstones and data are gone.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    ASSERT_EQ(ScanAllKeys(ro).size(), 0u);
  }
}

// Keys with special byte values: 0x00, 0xFF, embedded nulls, very short keys.
// These exercise trie byte-traversal edge cases.
TEST_F(TrieIndexDBTest, BinaryKeyEdgeCases) {
  ASSERT_OK(OpenDB(/*block_size=*/64));

  // All keys in sorted order (BytewiseComparator).
  std::vector<std::pair<std::string, std::string>> kvs = {
      {std::string("\x00", 1), "val_null"},
      {std::string("\x00\x00\x00", 3), "val_triple_null"},
      {std::string("\x01", 1), "val_0x01"},
      {"a", "val_a"},
      {std::string("a\x00"
                   "b",
                   3),
       "val_a_null_b"},
      {"mid", "val_mid"},
      {std::string("\xfe", 1), "val_0xfe"},
      {std::string("\xff", 1), "val_0xff"},
      {std::string("\xff\xff\xff", 3), "val_triple_ff"},
  };

  for (const auto& kv : kvs) {
    ASSERT_OK(db_->Put(WriteOptions(), kv.first, kv.second));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Forward scan: all keys in order through both indexes.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    auto actual = ScanAllKeyValues(ro);
    ASSERT_EQ(actual.size(), kvs.size());
    for (size_t i = 0; i < kvs.size(); i++) {
      SCOPED_TRACE("key index " + std::to_string(i));
      ASSERT_EQ(actual[i].first, kvs[i].first);
      ASSERT_EQ(actual[i].second, kvs[i].second);
    }
  }

  // Point lookups for boundary keys.
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(std::string("\x00", 1), "val_null"));
  ASSERT_NO_FATAL_FAILURE(
      VerifyGetBothIndexes(std::string("\xff\xff\xff", 3), "val_triple_ff"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes(std::string("a\x00"
                                                           "b",
                                                           3),
                                               "val_a_null_b"));

  // Seek to embedded-null key.
  ASSERT_NO_FATAL_FAILURE(VerifySeekBothIndexes(
      std::string("\x00", 1), std::string("\x00", 1), "val_null"));
}

// Puts with empty string values.
TEST_F(TrieIndexDBTest, EmptyValuePuts) {
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key1", ""));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "non_empty"));
  ASSERT_OK(db_->Put(WriteOptions(), "key3", ""));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key1", ""));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key2", "non_empty"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key3", ""));

  ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(
      std::vector<std::pair<std::string, std::string>>{
          {"key1", ""}, {"key2", "non_empty"}, {"key3", ""}}));
}

// Zlib compression: data blocks are compressed, UDI block is not.
// Verifies that reads through the trie index work with compressed data.
TEST_F(TrieIndexDBTest, CompressionZlib) {
  if (!Zlib_Supported()) {
    ROCKSDB_GTEST_SKIP("Zlib not linked");
    return;
  }
  options_.compression = kZlibCompression;
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 100; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%04d", i);
    // Compressible value (repeated pattern).
    ASSERT_OK(db_->Put(WriteOptions(), key, std::string(200, 'A' + (i % 26))));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Forward scan.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    ASSERT_EQ(ScanAllKeys(ro).size(), 100u);
  }

  // Spot-check a few keys.
  for (int i : {0, 49, 99}) {
    char key[16];
    snprintf(key, sizeof(key), "key_%04d", i);
    ASSERT_NO_FATAL_FAILURE(
        VerifyGetBothIndexes(key, std::string(200, 'A' + (i % 26))));
  }
}

// Iterator stability: an iterator pinned to a snapshot should not see data
// written after the iterator was created, even after flush.
TEST_F(TrieIndexDBTest, IteratorStabilityDuringFlush) {
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "v2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Open iterator (implicitly pins a snapshot).
  auto ro = TrieIndexReadOptions();
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "key1");

  // Write + flush new data while iterator is open.
  ASSERT_OK(db_->Put(WriteOptions(), "key3", "v3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Existing iterator should NOT see key3.
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "key2");
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // New iterator should see all three keys.
  std::unique_ptr<Iterator> iter2(db_->NewIterator(ro));
  iter2->SeekToFirst();
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ(iter2->key().ToString(), "key1");
  iter2->Next();
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ(iter2->key().ToString(), "key2");
  iter2->Next();
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ(iter2->key().ToString(), "key3");
  ASSERT_OK(iter2->status());
}

// iterate_upper_bound without prefix scan: the iterator should stop at the
// upper bound.
TEST_F(TrieIndexDBTest, IteratorUpperBound) {
  ASSERT_OK(OpenDB(/*block_size=*/64));

  for (const auto& k : {"aa", "bb", "cc", "dd", "ee", "ff"}) {
    ASSERT_OK(db_->Put(WriteOptions(), k, std::string("v_") + k));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& base_ro :
       {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(base_ro.table_index_factory ? "trie" : "standard");

    // Upper bound = "dd" → should see aa, bb, cc only.
    std::string ub_str = "dd";
    Slice ub(ub_str);
    ReadOptions ro = base_ro;
    ro.iterate_upper_bound = &ub;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    std::vector<std::string> keys;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(keys, (std::vector<std::string>{"aa", "bb", "cc"}));

    // Upper bound = "aa" → should see nothing.
    std::string ub2_str = "aa";
    Slice ub2(ub2_str);
    ReadOptions ro2 = base_ro;
    ro2.iterate_upper_bound = &ub2;
    std::unique_ptr<Iterator> iter2(db_->NewIterator(ro2));
    iter2->SeekToFirst();
    ASSERT_FALSE(iter2->Valid());
    ASSERT_OK(iter2->status());

    // Upper bound after all data → should see everything.
    std::string ub3_str = "zz";
    Slice ub3(ub3_str);
    ReadOptions ro3 = base_ro;
    ro3.iterate_upper_bound = &ub3;
    std::unique_ptr<Iterator> iter3(db_->NewIterator(ro3));
    std::vector<std::string> all_keys;
    for (iter3->SeekToFirst(); iter3->Valid(); iter3->Next()) {
      all_keys.push_back(iter3->key().ToString());
    }
    ASSERT_OK(iter3->status());
    ASSERT_EQ(all_keys.size(), 6u);
  }
}

// Combined snapshot + upper_bound: iterator sees the snapshot's view of data,
// bounded by iterate_upper_bound.
TEST_F(TrieIndexDBTest, IteratorSnapshotAndUpperBound) {
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key_a", "old_a"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_b", "old_b"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_c", "old_c"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_d", "old_d"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  const Snapshot* snap = db_->GetSnapshot();

  // Overwrite some keys after the snapshot.
  ASSERT_OK(db_->Put(WriteOptions(), "key_a", "new_a"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_c", "new_c"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_e", "new_e"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& base_ro :
       {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(base_ro.table_index_factory ? "trie" : "standard");

    std::string ub_str = "key_d";
    Slice ub(ub_str);
    ReadOptions ro = base_ro;
    ro.snapshot = snap;
    ro.iterate_upper_bound = &ub;

    auto kvs = ScanAllKeyValues(ro);
    // Snapshot view: old values. Upper bound excludes key_d and key_e.
    ASSERT_EQ(kvs.size(), 3u);
    ASSERT_EQ(kvs[0],
              std::make_pair(std::string("key_a"), std::string("old_a")));
    ASSERT_EQ(kvs[1],
              std::make_pair(std::string("key_b"), std::string("old_b")));
    ASSERT_EQ(kvs[2],
              std::make_pair(std::string("key_c"), std::string("old_c")));
  }
  db_->ReleaseSnapshot(snap);
}

// VerifyChecksum goes through SeekToFirst+Next on the index iterator.
TEST_F(TrieIndexDBTest, VerifyChecksumWithTrieUDI) {
  ASSERT_OK(OpenDB(/*block_size=*/128));

  for (int i = 0; i < 50; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%03d", i);
    ASSERT_OK(db_->Put(WriteOptions(), key, "value_" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // VerifyChecksum with default ReadOptions (standard index).
  ASSERT_OK(db_->VerifyChecksum());

  // VerifyChecksum with trie ReadOptions.
  ASSERT_OK(db_->VerifyChecksum(TrieIndexReadOptions()));
}

// Many small SSTs from frequent flushes: exercises trie iteration across
// many L0 files without compaction.
TEST_F(TrieIndexDBTest, ManySmallSSTs) {
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // 50 flushes, 2 keys each → 50 SSTs.
  for (int f = 0; f < 50; f++) {
    char k1[16];
    char k2[16];
    snprintf(k1, sizeof(k1), "key_%04d", f * 2);
    snprintf(k2, sizeof(k2), "key_%04d", f * 2 + 1);
    ASSERT_OK(db_->Put(WriteOptions(), k1, "v" + std::to_string(f * 2)));
    ASSERT_OK(db_->Put(WriteOptions(), k2, "v" + std::to_string(f * 2 + 1)));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  // Verify all 100 keys are readable.
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    auto keys = ScanAllKeys(ro);
    ASSERT_EQ(keys.size(), 100u);
  }

  // Spot-check first and last.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_0000", "v0"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_0099", "v99"));

  // Compact everything into one SST, re-verify.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (const auto& ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(ro.table_index_factory ? "trie" : "standard");
    ASSERT_EQ(ScanAllKeys(ro).size(), 100u);
  }
}

// Merge values accumulate across multiple compaction rounds.
TEST_F(TrieIndexDBTest, MergeAcrossMultipleCompactions) {
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  ASSERT_OK(OpenDB());

  // Round 1: Put base value.
  ASSERT_OK(db_->Put(WriteOptions(), "key", "base"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key", "base"));

  // Round 2: Merge "m1".
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "m1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key", "base,m1"));

  // Round 3: Merge "m2".
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "m2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key", "base,m1,m2"));

  // Forward scan also returns the accumulated value.
  ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(
      std::vector<std::pair<std::string, std::string>>{{"key", "base,m1,m2"}}));
}

// Graceful degradation: reopen a DB that was written with UDI, but without
// the UDI factory configured. Reads should fall back to the standard index.
TEST_F(TrieIndexDBTest, ReopenWithoutTrieUDI) {
  ASSERT_OK(OpenDB());

  ASSERT_OK(db_->Put(WriteOptions(), "key_a", "val_a"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_b", "val_b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();

  // Reopen WITHOUT UDI. The SST has a UDI meta block, but it's ignored.
  ASSERT_OK(OpenDBWithoutUDI());

  // Reads via standard index should work (UDI meta block is just ignored).
  std::string val;
  ASSERT_OK(db_->Get(ReadOptions(), "key_a", &val));
  ASSERT_EQ(val, "val_a");
  ASSERT_OK(db_->Get(ReadOptions(), "key_b", &val));
  ASSERT_EQ(val, "val_b");

  // Forward scan.
  auto keys = ScanAllKeys(ReadOptions());
  ASSERT_EQ(keys.size(), 2u);
  ASSERT_EQ(keys[0], "key_a");
  ASSERT_EQ(keys[1], "key_b");
}

// Mixed SSTs: some written with UDI, some without. Both should be readable
// through both index paths.
TEST_F(TrieIndexDBTest, MixedSSTsWithAndWithoutUDI) {
  options_.disable_auto_compactions = true;

  // Phase 1: Write with UDI → SST1 has UDI + standard index.
  ASSERT_OK(OpenDB());
  ASSERT_OK(db_->Put(WriteOptions(), "key_01", "udi_val1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_02", "udi_val2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();

  // Phase 2: Reopen WITHOUT UDI, write more → SST2 has only standard index.
  ASSERT_OK(OpenDBWithoutUDI());
  ASSERT_OK(db_->Put(WriteOptions(), "key_03", "noudi_val3"));
  ASSERT_OK(db_->Put(WriteOptions(), "key_04", "noudi_val4"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();

  // Phase 3: Reopen WITH UDI again. SST1 uses trie, SST2 falls back to
  // standard index (UDI block missing → logged warning, graceful fallback).
  options_.disable_auto_compactions = true;
  ASSERT_OK(OpenDB());

  // All 4 keys should be readable through both index paths.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_01", "udi_val1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_02", "udi_val2"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_03", "noudi_val3"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("key_04", "noudi_val4"));

  ASSERT_NO_FATAL_FAILURE(
      VerifyForwardScanBothIndexes({"key_01", "key_02", "key_03", "key_04"}));

  // Compact: merges UDI + non-UDI SSTs → new SST has UDI.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(
      VerifyForwardScanBothIndexes({"key_01", "key_02", "key_03", "key_04"}));
}

// TransactionDB commit: Put + Delete inside a transaction, then commit.
TEST_F(TrieIndexDBTest, TransactionCommit) {
  options_.create_if_missing = true;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;

  TransactionDB* txn_db = nullptr;
  ASSERT_OK(
      TransactionDB::Open(options_, TransactionDBOptions(), dbname_, &txn_db));
  db_.reset(txn_db);

  // Pre-populate a key.
  ASSERT_OK(txn_db->Put(WriteOptions(), "pre_key", "pre_val"));
  ASSERT_OK(txn_db->Flush(FlushOptions()));

  // Begin transaction: Put + Delete + Commit.
  std::unique_ptr<Transaction> txn(
      txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
  ASSERT_OK(txn->Put("txn_key1", "txn_val1"));
  ASSERT_OK(txn->Delete("pre_key"));
  ASSERT_OK(txn->Commit());

  ASSERT_OK(txn_db->Flush(FlushOptions()));

  // Verify through both indexes.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("txn_key1", "txn_val1"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("pre_key"));
}

// TransactionDB rollback: writes should be discarded. Rollback writes DELETE
// entries to WAL, which was previously restricted for UDI.
TEST_F(TrieIndexDBTest, TransactionRollback) {
  options_.create_if_missing = true;
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory = trie_factory_;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  last_options_ = options_;

  TransactionDB* txn_db = nullptr;
  ASSERT_OK(
      TransactionDB::Open(options_, TransactionDBOptions(), dbname_, &txn_db));
  db_.reset(txn_db);

  // Pre-populate data and flush.
  ASSERT_OK(txn_db->Put(WriteOptions(), "keep_key", "keep_val"));
  ASSERT_OK(txn_db->Flush(FlushOptions()));

  // Begin transaction, write, then ROLLBACK.
  std::unique_ptr<Transaction> txn(
      txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
  ASSERT_OK(txn->Put("rollback_key", "rollback_val"));
  ASSERT_OK(txn->Delete("keep_key"));
  ASSERT_OK(txn->Rollback());

  ASSERT_OK(txn_db->Flush(FlushOptions()));

  // Original data should be unchanged. Rolled-back writes should not appear.
  ASSERT_NO_FATAL_FAILURE(VerifyGetBothIndexes("keep_key", "keep_val"));
  ASSERT_NO_FATAL_FAILURE(VerifyGetNotFoundBothIndexes("rollback_key"));

  // Forward scan: only the original key.
  ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(
      std::vector<std::pair<std::string, std::string>>{
          {"keep_key", "keep_val"}}));
}

// total_order_seek with prefix_extractor: a common stress-test configuration.
// With total_order_seek=true, SeekToFirst and full forward scan should work
// correctly even when a prefix extractor is configured.
TEST_F(TrieIndexDBTest, TotalOrderSeekWithPrefixExtractor) {
  options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
  ASSERT_OK(OpenDB(/*block_size=*/128));

  // Keys with different prefixes.
  ASSERT_OK(db_->Put(WriteOptions(), "aaa_1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "aaa_2", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "bbb_1", "v3"));
  ASSERT_OK(db_->Put(WriteOptions(), "ccc_1", "v4"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // With total_order_seek=true, scan all keys across prefixes.
  for (auto base_ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(base_ro.table_index_factory ? "trie" : "standard");
    base_ro.total_order_seek = true;
    auto keys = ScanAllKeys(base_ro);
    ASSERT_EQ(keys.size(), 4u);
    ASSERT_EQ(keys[0], "aaa_1");
    ASSERT_EQ(keys[1], "aaa_2");
    ASSERT_EQ(keys[2], "bbb_1");
    ASSERT_EQ(keys[3], "ccc_1");

    // Seek across prefix boundary.
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    iter->Seek("aab");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "bbb_1");
    ASSERT_OK(iter->status());
  }

  // auto_prefix_mode: let RocksDB decide per-seek.
  for (auto base_ro : {StandardIndexReadOptions(), TrieIndexReadOptions()}) {
    SCOPED_TRACE(base_ro.table_index_factory ? "trie" : "standard");
    base_ro.auto_prefix_mode = true;
    std::unique_ptr<Iterator> iter(db_->NewIterator(base_ro));
    iter->Seek("bbb_1");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "bbb_1");
    ASSERT_OK(iter->status());
  }
}

// ============================================================================
// Multi-level SST + DeleteRange randomized test
//
// Historically bug-prone area: range tombstones interact with data across
// LSM levels (L0, L1, L2+), and the trie index must correctly handle
// seek/scan when blocks are partially or entirely covered by range deletions
// at different levels.
//
// Strategy:
// 1. Populate bottommost level with baseline data (flush + compact)
// 2. Write overlapping data and DeleteRanges to L0 (multiple rounds)
// 3. Partial compactions to create data at intermediate levels
// 4. Verify reads match between standard and trie index after each mutation
// 5. Snapshot before large DeleteRange, verify snapshot preserves state
// 6. Re-insert into deleted ranges, compact, and re-verify
// ============================================================================
TEST_F(TrieIndexDBTest, MultiLevelDeleteRangeRandomized) {
  uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  options_.disable_auto_compactions = true;
  // Small block size forces many data blocks (and thus many trie entries).
  ASSERT_OK(OpenDB(/*block_size=*/256));

  const int kMaxKey = 500;

  auto format_key = [](int k) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key_%05d", k);
    return std::string(buf);
  };

  // Core correctness check: forward scan via both indexes must match.
  auto verify_scan_consistency = [&]() {
    auto standard_kvs = ScanAllKeyValues(StandardIndexReadOptions());
    auto trie_kvs = ScanAllKeyValues(TrieIndexReadOptions());
    ASSERT_EQ(standard_kvs, trie_kvs)
        << "Scan mismatch: standard=" << standard_kvs.size()
        << " trie=" << trie_kvs.size();
  };

  // Phase 1: Populate bottommost level with baseline data.
  for (int i = 0; i < 200; i++) {
    int k = rnd.Uniform(kMaxKey);
    ASSERT_OK(db_->Put(WriteOptions(), format_key(k),
                       "base_" + rnd.RandomString(20)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());

  // Phase 2: Write overlapping data + DeleteRanges across multiple rounds.
  // Each round creates L0 SSTs with a mix of Puts and DeleteRanges,
  // with occasional partial compactions to push data to intermediate levels.
  for (int round = 0; round < 5; round++) {
    SCOPED_TRACE("round=" + std::to_string(round));

    // Write some new/updated keys.
    int num_writes = 30 + rnd.Uniform(70);
    for (int i = 0; i < num_writes; i++) {
      int k = rnd.Uniform(kMaxKey);
      ASSERT_OK(
          db_->Put(WriteOptions(), format_key(k),
                   "r" + std::to_string(round) + "_" + rnd.RandomString(15)));
    }

    // Issue 1-3 random DeleteRanges per round.
    int num_ranges = 1 + rnd.Uniform(3);
    for (int r = 0; r < num_ranges; r++) {
      int range_start = rnd.Uniform(kMaxKey - 10);
      int range_end = range_start + 5 + rnd.Uniform(50);
      if (range_end > kMaxKey) {
        range_end = kMaxKey;
      }
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 format_key(range_start),
                                 format_key(range_end)));
    }

    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());

    // On odd rounds, do a partial compaction to push some data down,
    // creating a multi-level structure where range tombstones at L0
    // must shadow data at L1/L2.
    if (round % 2 == 1) {
      int compact_start = rnd.Uniform(kMaxKey / 2);
      int compact_end = compact_start + kMaxKey / 4;
      std::string start_key = format_key(compact_start);
      std::string end_key = format_key(compact_end);
      Slice s(start_key);
      Slice e(end_key);
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &s, &e));
      ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());
    }
  }

  // Phase 3: Snapshot, then delete a large range. The snapshot must
  // preserve the pre-deletion state while current reads see the deletion.
  const Snapshot* snap = db_->GetSnapshot();
  auto snap_kvs = ScanAllKeyValues(StandardIndexReadOptions());

  int big_start = rnd.Uniform(kMaxKey / 4);
  int big_end = big_start + kMaxKey / 3;
  if (big_end > kMaxKey) {
    big_end = kMaxKey;
  }
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             format_key(big_start), format_key(big_end)));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Current state should reflect the deletion.
  ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());

  // Snapshot state should be unchanged.
  ASSERT_NO_FATAL_FAILURE(VerifyForwardScanBothIndexes(snap, snap_kvs));

  db_->ReleaseSnapshot(snap);

  // Phase 4: Re-insert keys into the deleted range, creating a pattern
  // where range tombstones and live data coexist at different levels.
  for (int i = big_start; i < big_end && i < kMaxKey; i += 3) {
    ASSERT_OK(db_->Put(WriteOptions(), format_key(i),
                       "reinserted_" + rnd.RandomString(10)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());

  // Phase 5: Full compaction — all range tombstones should be resolved.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(verify_scan_consistency());

  // Phase 6: Point lookups for a sample of keys — both indexes must agree.
  for (int i = 0; i < kMaxKey; i += 7) {
    std::string key = format_key(i);
    std::string std_val;
    std::string trie_val;
    Status s1 = db_->Get(StandardIndexReadOptions(), key, &std_val);
    Status s2 = db_->Get(TrieIndexReadOptions(), key, &trie_val);
    ASSERT_EQ(s1.code(), s2.code()) << "Status mismatch for " << key;
    if (s1.ok()) {
      ASSERT_EQ(std_val, trie_val) << "Value mismatch for " << key;
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
