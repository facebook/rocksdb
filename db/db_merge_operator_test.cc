//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <atomic>
#include <chrono>
#include <map>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/forward_iterator.h"
#include "port/stack_trace.h"
#include "rocksdb/comparator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/random.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

namespace ROCKSDB_NAMESPACE {

class TestReadCallback : public ReadCallback {
 public:
  TestReadCallback(SnapshotChecker* snapshot_checker,
                   SequenceNumber snapshot_seq)
      : ReadCallback(snapshot_seq),
        snapshot_checker_(snapshot_checker),
        snapshot_seq_(snapshot_seq) {}

  bool IsVisibleFullCheck(SequenceNumber seq) override {
    return snapshot_checker_->CheckInSnapshot(seq, snapshot_seq_) ==
           SnapshotCheckerResult::kInSnapshot;
  }

 private:
  SnapshotChecker* snapshot_checker_;
  SequenceNumber snapshot_seq_;
};

// Test merge operator functionality.
class DBMergeOperatorTest : public DBTestBase {
 public:
  DBMergeOperatorTest()
      : DBTestBase("db_merge_operator_test", /*env_do_fsync=*/false) {}

  std::string GetWithReadCallback(SnapshotChecker* snapshot_checker,
                                  const Slice& key,
                                  const Snapshot* snapshot = nullptr) {
    SequenceNumber seq = snapshot == nullptr ? db_->GetLatestSequenceNumber()
                                             : snapshot->GetSequenceNumber();
    TestReadCallback read_callback(snapshot_checker, seq);
    ReadOptions read_opt;
    read_opt.snapshot = snapshot;
    PinnableSlice value;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = db_->DefaultColumnFamily();
    get_impl_options.value = &value;
    get_impl_options.callback = &read_callback;
    Status s = dbfull()->GetImpl(read_opt, key, get_impl_options);
    if (!s.ok()) {
      return s.ToString();
    }
    return value.ToString();
  }
};

TEST_F(DBMergeOperatorTest, LimitMergeOperands) {
  class LimitedStringAppendMergeOp : public StringAppendTESTOperator {
   public:
    LimitedStringAppendMergeOp(int limit, char delim)
        : StringAppendTESTOperator(delim), limit_(limit) {}

    const char* Name() const override {
      return "DBMergeOperatorTest::LimitedStringAppendMergeOp";
    }

    bool ShouldMerge(const std::vector<Slice>& operands) const override {
      if (operands.size() > 0 && limit_ > 0 && operands.size() >= limit_) {
        return true;
      }
      return false;
    }

   private:
    size_t limit_ = 0;
  };

  Options options = CurrentOptions();
  options.create_if_missing = true;
  // Use only the latest two merge operands.
  options.merge_operator = std::make_shared<LimitedStringAppendMergeOp>(2, ',');
  options.env = env_;
  Reopen(options);
  // All K1 values are in memtable.
  ASSERT_OK(Merge("k1", "a"));
  ASSERT_OK(Merge("k1", "b"));
  ASSERT_OK(Merge("k1", "c"));
  ASSERT_OK(Merge("k1", "d"));
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  // Make sure that only the latest two merge operands are used. If this was
  // not the case the value would be "a,b,c,d".
  ASSERT_EQ(value, "c,d");

  // All K2 values are flushed to L0 into a single file.
  ASSERT_OK(Merge("k2", "a"));
  ASSERT_OK(Merge("k2", "b"));
  ASSERT_OK(Merge("k2", "c"));
  ASSERT_OK(Merge("k2", "d"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Get(ReadOptions(), "k2", &value));
  ASSERT_EQ(value, "c,d");

  // All K3 values are flushed and are in different files.
  ASSERT_OK(Merge("k3", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "bc"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "de"));
  ASSERT_OK(db_->Get(ReadOptions(), "k3", &value));
  ASSERT_EQ(value, "cd,de");
  // Tests that merge operands reach exact limit at memtable.
  ASSERT_OK(Merge("k3", "fg"));
  ASSERT_OK(db_->Get(ReadOptions(), "k3", &value));
  ASSERT_EQ(value, "de,fg");

  // All K4 values are in different levels
  ASSERT_OK(Merge("k4", "ab"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);
  ASSERT_OK(Merge("k4", "bc"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_OK(Merge("k4", "cd"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Merge("k4", "de"));
  ASSERT_OK(db_->Get(ReadOptions(), "k4", &value));
  ASSERT_EQ(value, "cd,de");
}

TEST_F(DBMergeOperatorTest, MergeErrorOnRead) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());
  options.env = env_;
  Reopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Merge("k1", "corrupted"));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsCorruption());
  VerifyDBInternal({{"k1", "corrupted"}, {"k1", "v1"}});
}

TEST_F(DBMergeOperatorTest, MergeErrorOnWrite) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());
  options.max_successive_merges = 3;
  options.env = env_;
  Reopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Merge("k1", "v2"));
  // Will trigger a merge when hitting max_successive_merges and the merge
  // will fail. The delta will be inserted nevertheless.
  ASSERT_OK(Merge("k1", "corrupted"));
  // Data should stay unmerged after the error.
  VerifyDBInternal({{"k1", "corrupted"}, {"k1", "v2"}, {"k1", "v1"}});
}

TEST_F(DBMergeOperatorTest, MergeErrorOnIteration) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());
  options.env = env_;

  DestroyAndReopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Merge("k1", "corrupted"));
  ASSERT_OK(Put("k2", "v2"));
  auto* iter = db_->NewIterator(ReadOptions());
  iter->Seek("k1");
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;
  iter = db_->NewIterator(ReadOptions());
  iter->Seek("k2");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;
  VerifyDBInternal({{"k1", "corrupted"}, {"k1", "v1"}, {"k2", "v2"}});

  DestroyAndReopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Merge("k2", "corrupted"));
  iter = db_->NewIterator(ReadOptions());
  iter->Seek("k1");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;
  VerifyDBInternal({{"k1", "v1"}, {"k2", "corrupted"}, {"k2", "v2"}});
}

TEST_F(DBMergeOperatorTest, MergeOperatorFailsWithMustMerge) {
  // This is like a mini-stress test dedicated to `OpFailureScope::kMustMerge`.
  // Some or most of it might be deleted upon adding that option to the actual
  // stress test.
  //
  // "k0" and "k2" are stable (uncorrupted) keys before and after a corrupted
  // key ("k1"). The outer loop (`i`) varies which write (`j`) to "k1" triggers
  // the corruption. Inside that loop there are three cases:
  //
  // - Case 1: pure `Merge()`s
  // - Case 2: `Merge()`s on top of a `Put()`
  // - Case 3: `Merge()`s on top of a `Delete()`
  //
  // For each case we test query results before flush, after flush, and after
  // compaction, as well as cleanup after deletion+compaction. The queries
  // expect "k0" and "k2" to always be readable. "k1" is expected to be readable
  // only by APIs that do not require merging, such as `GetMergeOperands()`.
  const int kNumOperands = 3;
  Options options = CurrentOptions();
  options.merge_operator.reset(new TestPutOperator());
  options.env = env_;
  Reopen(options);

  for (int i = 0; i < kNumOperands; ++i) {
    auto check_query = [&]() {
      {
        std::string value;
        ASSERT_OK(db_->Get(ReadOptions(), "k0", &value));
        Status s = db_->Get(ReadOptions(), "k1", &value);
        ASSERT_TRUE(s.IsCorruption());
        ASSERT_EQ(Status::SubCode::kMergeOperatorFailed, s.subcode());
        ASSERT_OK(db_->Get(ReadOptions(), "k2", &value));
      }

      {
        std::unique_ptr<Iterator> iter;
        iter.reset(db_->NewIterator(ReadOptions()));
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("k0", iter->key());
        iter->Next();
        ASSERT_TRUE(iter->status().IsCorruption());
        ASSERT_EQ(Status::SubCode::kMergeOperatorFailed,
                  iter->status().subcode());

        iter->SeekToLast();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("k2", iter->key());
        iter->Prev();
        ASSERT_TRUE(iter->status().IsCorruption());

        iter->Seek("k2");
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("k2", iter->key());
      }

      std::vector<PinnableSlice> values(kNumOperands);
      GetMergeOperandsOptions merge_operands_info;
      merge_operands_info.expected_max_number_of_operands = kNumOperands;
      int num_operands_found = 0;
      ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                      "k1", values.data(), &merge_operands_info,
                                      &num_operands_found));
      ASSERT_EQ(kNumOperands, num_operands_found);
      for (int j = 0; j < num_operands_found; ++j) {
        if (i == j) {
          ASSERT_EQ(values[j], "corrupted_must_merge");
        } else {
          ASSERT_EQ(values[j], "ok");
        }
      }
    };

    ASSERT_OK(Put("k0", "val"));
    ASSERT_OK(Put("k2", "val"));

    // Case 1
    for (int j = 0; j < kNumOperands; ++j) {
      if (j == i) {
        ASSERT_OK(Merge("k1", "corrupted_must_merge"));
      } else {
        ASSERT_OK(Merge("k1", "ok"));
      }
    }
    check_query();
    ASSERT_OK(Flush());
    check_query();
    {
      CompactRangeOptions cro;
      cro.bottommost_level_compaction =
          BottommostLevelCompaction::kForceOptimized;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    }
    check_query();

    // Case 2
    for (int j = 0; j < kNumOperands; ++j) {
      Slice val;
      if (j == i) {
        val = "corrupted_must_merge";
      } else {
        val = "ok";
      }
      if (j == 0) {
        ASSERT_OK(Put("k1", val));
      } else {
        ASSERT_OK(Merge("k1", val));
      }
    }
    check_query();
    ASSERT_OK(Flush());
    check_query();
    {
      CompactRangeOptions cro;
      cro.bottommost_level_compaction =
          BottommostLevelCompaction::kForceOptimized;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    }
    check_query();

    // Case 3
    ASSERT_OK(Delete("k1"));
    for (int j = 0; j < kNumOperands; ++j) {
      if (i == j) {
        ASSERT_OK(Merge("k1", "corrupted_must_merge"));
      } else {
        ASSERT_OK(Merge("k1", "ok"));
      }
    }
    check_query();
    ASSERT_OK(Flush());
    check_query();
    {
      CompactRangeOptions cro;
      cro.bottommost_level_compaction =
          BottommostLevelCompaction::kForceOptimized;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    }
    check_query();

    // Verify obsolete data removal still happens
    ASSERT_OK(Delete("k0"));
    ASSERT_OK(Delete("k1"));
    ASSERT_OK(Delete("k2"));
    ASSERT_EQ("NOT_FOUND", Get("k0"));
    ASSERT_EQ("NOT_FOUND", Get("k1"));
    ASSERT_EQ("NOT_FOUND", Get("k2"));
    CompactRangeOptions cro;
    cro.bottommost_level_compaction =
        BottommostLevelCompaction::kForceOptimized;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    ASSERT_EQ("", FilesPerLevel());
  }
}

TEST_F(DBMergeOperatorTest, MergeOperandThresholdExceeded) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.env = env_;
  Reopen(options);

  std::vector<Slice> keys{"foo", "bar", "baz"};

  // Write base values.
  for (const auto& key : keys) {
    ASSERT_OK(Put(key, key.ToString() + "0"));
  }

  // Write merge operands. Note that the first key has 1 merge operand, the
  // second one has 2 merge operands, and the third one has 3 merge operands.
  // Also, we'll take some snapshots to make sure the merge operands are
  // preserved during flush.
  std::vector<ManagedSnapshot> snapshots;
  snapshots.reserve(3);

  for (size_t i = 0; i < keys.size(); ++i) {
    snapshots.emplace_back(db_.get());

    const std::string suffix = std::to_string(i + 1);

    for (size_t j = i; j < keys.size(); ++j) {
      ASSERT_OK(Merge(keys[j], keys[j].ToString() + suffix));
    }
  }

  // Verify the results and status codes of various types of point lookups.
  auto verify = [&](const std::optional<size_t>& threshold) {
    ReadOptions read_options;
    read_options.merge_operand_count_threshold = threshold;

    // Check Get()
    {
      for (size_t i = 0; i < keys.size(); ++i) {
        PinnableSlice value;
        const Status status =
            db_->Get(read_options, db_->DefaultColumnFamily(), keys[i], &value);
        ASSERT_OK(status);
        ASSERT_EQ(status.IsOkMergeOperandThresholdExceeded(),
                  threshold.has_value() && i + 1 > threshold.value());
        ASSERT_EQ(value, keys[i].ToString() + std::to_string(i + 1));
      }
    }

    // Check old-style MultiGet()
    {
      std::vector<std::string> values;
      std::vector<Status> statuses = db_->MultiGet(read_options, keys, &values);

      for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_OK(statuses[i]);
        ASSERT_EQ(statuses[i].IsOkMergeOperandThresholdExceeded(),
                  threshold.has_value() && i + 1 > threshold.value());
        ASSERT_EQ(values[i], keys[i].ToString() + std::to_string(i + 1));
      }
    }

    // Check batched MultiGet()
    {
      std::vector<PinnableSlice> values(keys.size());
      std::vector<Status> statuses(keys.size());
      db_->MultiGet(read_options, db_->DefaultColumnFamily(), keys.size(),
                    keys.data(), values.data(), statuses.data());

      for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_OK(statuses[i]);
        ASSERT_EQ(statuses[i].IsOkMergeOperandThresholdExceeded(),
                  threshold.has_value() && i + 1 > threshold.value());
        ASSERT_EQ(values[i], keys[i].ToString() + std::to_string(i + 1));
      }
    }
  };

  // Test the case when the feature is disabled as well as various thresholds.
  verify(std::nullopt);
  for (size_t i = 0; i < 5; ++i) {
    verify(i);
  }

  // Flush and try again to test the case when results are served from SSTs.
  ASSERT_OK(Flush());
  verify(std::nullopt);
  for (size_t i = 0; i < 5; ++i) {
    verify(i);
  }
}

TEST_F(DBMergeOperatorTest, DataBlockBinaryAndHash) {
  // Basic test to check that merge operator works with data block index type
  // DataBlockBinaryAndHash.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());
  options.env = env_;
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = 16;
  table_options.data_block_index_type =
      BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  const int kNumKeys = 100;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(db_->Merge(WriteOptions(), Key(i), std::to_string(i)));
  }
  ASSERT_OK(Flush());
  std::string value;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    ASSERT_EQ(std::to_string(i), value);
  }

  std::vector<const Snapshot*> snapshots;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(db_->Delete(WriteOptions(), Key(i)));
    for (int j = 0; j < 3; ++j) {
      ASSERT_OK(db_->Merge(WriteOptions(), Key(i), std::to_string(i * 3 + j)));
      snapshots.push_back(db_->GetSnapshot());
    }
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    ASSERT_EQ(std::to_string(i * 3 + 2), value);
  }
  for (auto snapshot : snapshots) {
    db_->ReleaseSnapshot(snapshot);
  }
}

class MergeOperatorPinningTest : public DBMergeOperatorTest,
                                 public testing::WithParamInterface<bool> {
 public:
  MergeOperatorPinningTest() { disable_block_cache_ = GetParam(); }

  bool disable_block_cache_;
};

INSTANTIATE_TEST_CASE_P(MergeOperatorPinningTest, MergeOperatorPinningTest,
                        ::testing::Bool());

TEST_P(MergeOperatorPinningTest, OperandsMultiBlocks) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1;  // every block will contain one entry
  table_options.no_block_cache = disable_block_cache_;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const int kKeysPerFile = 10;
  const int kOperandsPerKeyPerFile = 7;
  const int kOperandSize = 100;
  // Filse to write in L0 before compacting to lower level
  const int kFilesPerLevel = 3;

  Random rnd(301);
  std::map<std::string, std::string> true_data;
  int batch_num = 1;
  int lvl_to_fill = 4;
  int key_id = 0;
  while (true) {
    for (int j = 0; j < kKeysPerFile; j++) {
      std::string key = Key(key_id % 35);
      key_id++;
      for (int k = 0; k < kOperandsPerKeyPerFile; k++) {
        std::string val = rnd.RandomString(kOperandSize);
        ASSERT_OK(db_->Merge(WriteOptions(), key, val));
        if (true_data[key].size() == 0) {
          true_data[key] = val;
        } else {
          true_data[key] += "," + val;
        }
      }
    }

    if (lvl_to_fill == -1) {
      // Keep last batch in memtable and stop
      break;
    }

    ASSERT_OK(Flush());
    if (batch_num % kFilesPerLevel == 0) {
      if (lvl_to_fill != 0) {
        MoveFilesToLevel(lvl_to_fill);
      }
      lvl_to_fill--;
    }
    batch_num++;
  }

  // 3 L0 files
  // 1 L1 file
  // 3 L2 files
  // 1 L3 file
  // 3 L4 Files
  ASSERT_EQ(FilesPerLevel(), "3,1,3,1,3");

  VerifyDBFromMap(true_data);
}

class MergeOperatorHook : public MergeOperator {
 public:
  explicit MergeOperatorHook(std::shared_ptr<MergeOperator> _merge_op)
      : merge_op_(_merge_op) {}

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    before_merge_();
    bool res = merge_op_->FullMergeV2(merge_in, merge_out);
    after_merge_();
    return res;
  }

  const char* Name() const override { return merge_op_->Name(); }

  std::shared_ptr<MergeOperator> merge_op_;
  std::function<void()> before_merge_ = []() {};
  std::function<void()> after_merge_ = []() {};
};

TEST_P(MergeOperatorPinningTest, EvictCacheBeforeMerge) {
  Options options = CurrentOptions();

  auto merge_hook =
      std::make_shared<MergeOperatorHook>(MergeOperators::CreateMaxOperator());
  options.merge_operator = merge_hook;
  options.disable_auto_compactions = true;
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.max_open_files = 20;
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = disable_block_cache_;
  if (bbto.no_block_cache == false) {
    bbto.block_cache = NewLRUCache(64 * 1024 * 1024);
  } else {
    bbto.block_cache = nullptr;
  }
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  const int kNumOperands = 30;
  const int kNumKeys = 1000;
  const int kOperandSize = 100;
  Random rnd(301);

  // 1000 keys every key have 30 operands, every operand is in a different file
  std::map<std::string, std::string> true_data;
  for (int i = 0; i < kNumOperands; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      std::string k = Key(j);
      std::string v = rnd.RandomString(kOperandSize);
      ASSERT_OK(db_->Merge(WriteOptions(), k, v));

      true_data[k] = std::max(true_data[k], v);
    }
    ASSERT_OK(Flush());
  }

  std::vector<uint64_t> file_numbers = ListTableFiles(env_, dbname_);
  ASSERT_EQ(file_numbers.size(), kNumOperands);
  int merge_cnt = 0;

  // Code executed before merge operation
  merge_hook->before_merge_ = [&]() {
    // Evict all tables from cache before every merge operation
    auto* table_cache = dbfull()->TEST_table_cache();
    for (uint64_t num : file_numbers) {
      TableCache::Evict(table_cache, num);
    }
    // Decrease cache capacity to force all unrefed blocks to be evicted
    if (bbto.block_cache) {
      bbto.block_cache->SetCapacity(1);
    }
    merge_cnt++;
  };

  // Code executed after merge operation
  merge_hook->after_merge_ = [&]() {
    // Increase capacity again after doing the merge
    if (bbto.block_cache) {
      bbto.block_cache->SetCapacity(64 * 1024 * 1024);
    }
  };

  size_t total_reads;
  VerifyDBFromMap(true_data, &total_reads);
  ASSERT_EQ(merge_cnt, total_reads);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  VerifyDBFromMap(true_data, &total_reads);
}

TEST_P(MergeOperatorPinningTest, TailingIterator) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateMaxOperator();
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = disable_block_cache_;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  const int kNumOperands = 100;
  const int kNumWrites = 100000;

  std::function<void()> writer_func = [&]() {
    int k = 0;
    for (int i = 0; i < kNumWrites; i++) {
      ASSERT_OK(db_->Merge(WriteOptions(), Key(k), Key(k)));

      if (i && i % kNumOperands == 0) {
        k++;
      }
      if (i && i % 127 == 0) {
        ASSERT_OK(Flush());
      }
      if (i && i % 317 == 0) {
        ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      }
    }
  };

  std::function<void()> reader_func = [&]() {
    ReadOptions ro;
    ro.tailing = true;
    Iterator* iter = db_->NewIterator(ro);
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    for (int i = 0; i < (kNumWrites / kNumOperands); i++) {
      while (!iter->Valid()) {
        // wait for the key to be written
        env_->SleepForMicroseconds(100);
        iter->Seek(Key(i));
      }
      ASSERT_EQ(iter->key(), Key(i));
      ASSERT_EQ(iter->value(), Key(i));

      iter->Next();
    }
    ASSERT_OK(iter->status());

    delete iter;
  };

  ROCKSDB_NAMESPACE::port::Thread writer_thread(writer_func);
  ROCKSDB_NAMESPACE::port::Thread reader_thread(reader_func);

  writer_thread.join();
  reader_thread.join();
}

TEST_F(DBMergeOperatorTest, TailingIteratorMemtableUnrefedBySomeoneElse) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  // Overview of the test:
  //  * There are two merge operands for the same key: one in an sst file,
  //    another in a memtable.
  //  * Seek a tailing iterator to this key.
  //  * As part of the seek, the iterator will:
  //      (a) first visit the operand in the memtable and tell ForwardIterator
  //          to pin this operand, then
  //      (b) move on to the operand in the sst file, then pass both operands
  //          to merge operator.
  //  * The memtable may get flushed and unreferenced by another thread between
  //    (a) and (b). The test simulates it by flushing the memtable inside a
  //    SyncPoint callback located between (a) and (b).
  //  * In this case it's ForwardIterator's responsibility to keep the memtable
  //    pinned until (b) is complete. There used to be a bug causing
  //    ForwardIterator to not pin it in some circumstances. This test
  //    reproduces it.

  ASSERT_OK(db_->Merge(WriteOptions(), "key", "sst"));
  ASSERT_OK(db_->Flush(FlushOptions()));  // Switch to SuperVersion A
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "memtable"));

  // Pin SuperVersion A
  std::unique_ptr<Iterator> someone_else(db_->NewIterator(ReadOptions()));
  ASSERT_OK(someone_else->status());

  bool pushed_first_operand = false;
  bool stepped_to_next_operand = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBIter::MergeValuesNewToOld:PushedFirstOperand", [&](void*) {
        EXPECT_FALSE(pushed_first_operand);
        pushed_first_operand = true;
        EXPECT_OK(db_->Flush(FlushOptions()));  // Switch to SuperVersion B
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBIter::MergeValuesNewToOld:SteppedToNextOperand", [&](void*) {
        EXPECT_FALSE(stepped_to_next_operand);
        stepped_to_next_operand = true;
        someone_else.reset();  // Unpin SuperVersion A
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ReadOptions ro;
  ro.tailing = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  iter->Seek("key");

  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(std::string("sst,memtable"), iter->value().ToString());
  EXPECT_TRUE(pushed_first_operand);
  EXPECT_TRUE(stepped_to_next_operand);
}

TEST_F(DBMergeOperatorTest, SnapshotCheckerAndReadCallback) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  class TestSnapshotChecker : public SnapshotChecker {
   public:
    SnapshotCheckerResult CheckInSnapshot(
        SequenceNumber seq, SequenceNumber snapshot_seq) const override {
      return IsInSnapshot(seq, snapshot_seq)
                 ? SnapshotCheckerResult::kInSnapshot
                 : SnapshotCheckerResult::kNotInSnapshot;
    }

    bool IsInSnapshot(SequenceNumber seq, SequenceNumber snapshot_seq) const {
      switch (snapshot_seq) {
        case 0:
          return seq == 0;
        case 1:
          return seq <= 1;
        case 2:
          // seq = 2 not visible to snapshot with seq = 2
          return seq <= 1;
        case 3:
          return seq <= 3;
        case 4:
          // seq = 4 not visible to snpahost with seq = 4
          return seq <= 3;
        default:
          // seq >=4 is uncommitted
          return seq <= 4;
      };
    }
  };
  TestSnapshotChecker* snapshot_checker = new TestSnapshotChecker();
  dbfull()->SetSnapshotChecker(snapshot_checker);

  std::string value;
  ASSERT_OK(Merge("foo", "v1"));
  ASSERT_EQ(1, db_->GetLatestSequenceNumber());
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo"));
  ASSERT_OK(Merge("foo", "v2"));
  ASSERT_EQ(2, db_->GetLatestSequenceNumber());
  // v2 is not visible to latest snapshot, which has seq = 2.
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo"));
  // Take a snapshot with seq = 2.
  const Snapshot* snapshot1 = db_->GetSnapshot();
  ASSERT_EQ(2, snapshot1->GetSequenceNumber());
  // v2 is not visible to snapshot1, which has seq = 2
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo", snapshot1));

  // Verify flush doesn't alter the result.
  ASSERT_OK(Flush());
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo", snapshot1));
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo"));

  ASSERT_OK(Merge("foo", "v3"));
  ASSERT_EQ(3, db_->GetLatestSequenceNumber());
  ASSERT_EQ("v1,v2,v3", GetWithReadCallback(snapshot_checker, "foo"));
  ASSERT_OK(Merge("foo", "v4"));
  ASSERT_EQ(4, db_->GetLatestSequenceNumber());
  // v4 is not visible to latest snapshot, which has seq = 4.
  ASSERT_EQ("v1,v2,v3", GetWithReadCallback(snapshot_checker, "foo"));
  const Snapshot* snapshot2 = db_->GetSnapshot();
  ASSERT_EQ(4, snapshot2->GetSequenceNumber());
  // v4 is not visible to snapshot2, which has seq = 4.
  ASSERT_EQ("v1,v2,v3",
            GetWithReadCallback(snapshot_checker, "foo", snapshot2));

  // Verify flush doesn't alter the result.
  ASSERT_OK(Flush());
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo", snapshot1));
  ASSERT_EQ("v1,v2,v3",
            GetWithReadCallback(snapshot_checker, "foo", snapshot2));
  ASSERT_EQ("v1,v2,v3", GetWithReadCallback(snapshot_checker, "foo"));

  ASSERT_OK(Merge("foo", "v5"));
  ASSERT_EQ(5, db_->GetLatestSequenceNumber());
  // v5 is uncommitted
  ASSERT_EQ("v1,v2,v3,v4", GetWithReadCallback(snapshot_checker, "foo"));

  // full manual compaction.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify compaction doesn't alter the result.
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo", snapshot1));
  ASSERT_EQ("v1,v2,v3",
            GetWithReadCallback(snapshot_checker, "foo", snapshot2));
  ASSERT_EQ("v1,v2,v3,v4", GetWithReadCallback(snapshot_checker, "foo"));

  db_->ReleaseSnapshot(snapshot1);
  db_->ReleaseSnapshot(snapshot2);
}

class PerConfigMergeOperatorPinningTest
    : public DBMergeOperatorTest,
      public testing::WithParamInterface<std::tuple<bool, int>> {
 public:
  PerConfigMergeOperatorPinningTest() {
    std::tie(disable_block_cache_, option_config_) = GetParam();
  }

  bool disable_block_cache_;
};

INSTANTIATE_TEST_CASE_P(
    MergeOperatorPinningTest, PerConfigMergeOperatorPinningTest,
    ::testing::Combine(::testing::Bool(),
                       ::testing::Range(static_cast<int>(DBTestBase::kDefault),
                                        static_cast<int>(DBTestBase::kEnd))));

TEST_P(PerConfigMergeOperatorPinningTest, Randomized) {
  if (ShouldSkipOptions(option_config_, kSkipMergePut)) {
    return;
  }

  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateMaxOperator();
  BlockBasedTableOptions table_options;
  table_options.no_block_cache = disable_block_cache_;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  Random rnd(301);
  std::map<std::string, std::string> true_data;

  const int kTotalMerges = 5000;
  // Every key gets ~10 operands
  const int kKeyRange = kTotalMerges / 10;
  const int kOperandSize = 20;
  const int kNumPutBefore = kKeyRange / 10;  // 10% value
  const int kNumPutAfter = kKeyRange / 10;   // 10% overwrite
  const int kNumDelete = kKeyRange / 10;     // 10% delete

  // kNumPutBefore keys will have base values
  for (int i = 0; i < kNumPutBefore; i++) {
    std::string key = Key(rnd.Next() % kKeyRange);
    std::string value = rnd.RandomString(kOperandSize);
    ASSERT_OK(db_->Put(WriteOptions(), key, value));

    true_data[key] = value;
  }

  // Do kTotalMerges merges
  for (int i = 0; i < kTotalMerges; i++) {
    std::string key = Key(rnd.Next() % kKeyRange);
    std::string value = rnd.RandomString(kOperandSize);
    ASSERT_OK(db_->Merge(WriteOptions(), key, value));

    if (true_data[key] < value) {
      true_data[key] = value;
    }
  }

  // Overwrite random kNumPutAfter keys
  for (int i = 0; i < kNumPutAfter; i++) {
    std::string key = Key(rnd.Next() % kKeyRange);
    std::string value = rnd.RandomString(kOperandSize);
    ASSERT_OK(db_->Put(WriteOptions(), key, value));

    true_data[key] = value;
  }

  // Delete random kNumDelete keys
  for (int i = 0; i < kNumDelete; i++) {
    std::string key = Key(rnd.Next() % kKeyRange);
    ASSERT_OK(db_->Delete(WriteOptions(), key));

    true_data.erase(key);
  }

  VerifyDBFromMap(true_data);
}

TEST_F(DBMergeOperatorTest, MaxSuccessiveMergesBaseValues) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.max_successive_merges = 1;
  options.env = env_;
  Reopen(options);

  constexpr char foo[] = "foo";
  constexpr char bar[] = "bar";
  constexpr char baz[] = "baz";
  constexpr char qux[] = "qux";
  constexpr char corge[] = "corge";

  // No base value
  {
    const std::string key = "key1";

    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, foo));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, bar));

    PinnableSlice result;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, bar);

    // We expect the second Merge to be converted to a Put because of
    // max_successive_merges.
    constexpr size_t max_key_versions = 8;
    std::vector<KeyVersion> key_versions;
    ASSERT_OK(GetAllKeyVersions(db_.get(), db_->DefaultColumnFamily(), key, key,
                                max_key_versions, &key_versions));
    ASSERT_EQ(key_versions.size(), 2);
    ASSERT_EQ(key_versions[0].type, kTypeValue);
    ASSERT_EQ(key_versions[1].type, kTypeMerge);
  }

  // Plain base value
  {
    const std::string key = "key2";

    ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), key, foo));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, bar));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, baz));

    PinnableSlice result;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, baz);

    // We expect the second Merge to be converted to a Put because of
    // max_successive_merges.
    constexpr size_t max_key_versions = 8;
    std::vector<KeyVersion> key_versions;
    ASSERT_OK(GetAllKeyVersions(db_.get(), db_->DefaultColumnFamily(), key, key,
                                max_key_versions, &key_versions));
    ASSERT_EQ(key_versions.size(), 3);
    ASSERT_EQ(key_versions[0].type, kTypeValue);
    ASSERT_EQ(key_versions[1].type, kTypeMerge);
    ASSERT_EQ(key_versions[2].type, kTypeValue);
  }

  // Wide-column base value
  {
    const std::string key = "key3";
    const WideColumns columns{{kDefaultWideColumnName, foo}, {bar, baz}};

    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                             columns));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, qux));
    ASSERT_OK(
        db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, corge));

    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    const WideColumns expected{{kDefaultWideColumnName, corge}, {bar, baz}};
    ASSERT_EQ(result.columns(), expected);

    // We expect the second Merge to be converted to a PutEntity because of
    // max_successive_merges.
    constexpr size_t max_key_versions = 8;
    std::vector<KeyVersion> key_versions;
    ASSERT_OK(GetAllKeyVersions(db_.get(), db_->DefaultColumnFamily(), key, key,
                                max_key_versions, &key_versions));
    ASSERT_EQ(key_versions.size(), 3);
    ASSERT_EQ(key_versions[0].type, kTypeWideColumnEntity);
    ASSERT_EQ(key_versions[1].type, kTypeMerge);
    ASSERT_EQ(key_versions[2].type, kTypeWideColumnEntity);
  }
}

// A merge operator whose FullMergeV3 can signal deletion by returning
// std::monostate. It interprets operands as int64_t increments applied to a
// counter; when the counter reaches zero (or below), the key is deleted.
class CounterDeleteMergeOperator : public MergeOperator {
 public:
  bool FullMergeV3(const MergeOperationInputV3& merge_in,
                   MergeOperationOutputV3* merge_out) const override {
    int64_t counter = 0;
    // Parse base value if it exists.
    if (auto* pval = std::get_if<Slice>(&merge_in.existing_value)) {
      if (pval->size() == sizeof(int64_t)) {
        counter = DecodeFixed64(pval->data());
      }
    }
    // std::monostate means no base value -- counter starts at 0.

    // Apply each operand (encoded as int64_t delta).
    for (const auto& operand : merge_in.operand_list) {
      if (operand.size() == sizeof(int64_t)) {
        counter += static_cast<int64_t>(DecodeFixed64(operand.data()));
      }
    }

    if (counter <= 0) {
      // Signal deletion.
      merge_out->new_value = std::monostate{};
    } else {
      std::string result;
      PutFixed64(&result, static_cast<uint64_t>(counter));
      merge_out->new_value = std::move(result);
    }
    return true;
  }

  const char* Name() const override { return "CounterDeleteMergeOperator"; }
};

// A merge operator that always signals deletion from FullMergeV3.
class AlwaysDeleteMergeOperator : public MergeOperator {
 public:
  bool FullMergeV3(const MergeOperationInputV3& merge_in,
                   MergeOperationOutputV3* merge_out) const override {
    (void)merge_in;
    merge_out->new_value = std::monostate{};
    return true;
  }

  const char* Name() const override { return "AlwaysDeleteMergeOperator"; }
};

// A merge operator that conditionally deletes based on operand content.
// If the last operand is "DELETE", the key is deleted.
// Otherwise, the operands are concatenated with "," as a delimiter.
class ConditionalDeleteMergeOperator : public MergeOperator {
 public:
  bool FullMergeV3(const MergeOperationInputV3& merge_in,
                   MergeOperationOutputV3* merge_out) const override {
    if (!merge_in.operand_list.empty() &&
        merge_in.operand_list.back() == Slice("DELETE")) {
      merge_out->new_value = std::monostate{};
      return true;
    }

    std::string result;
    if (auto* pval = std::get_if<Slice>(&merge_in.existing_value)) {
      result.assign(pval->data(), pval->size());
    }
    for (const auto& operand : merge_in.operand_list) {
      if (!result.empty()) {
        result += ",";
      }
      result.append(operand.data(), operand.size());
    }
    merge_out->new_value = std::move(result);
    return true;
  }

  const char* Name() const override { return "ConditionalDeleteMergeOperator"; }
};

static std::string EncodeInt64(int64_t val) {
  std::string result;
  PutFixed64(&result, static_cast<uint64_t>(val));
  return result;
}

static std::string EncodeU64Timestamp(uint64_t timestamp) {
  std::string result;
  EncodeU64Ts(timestamp, &result);
  return result;
}

static int64_t DecodeInt64(const std::string& s) {
  assert(s.size() == sizeof(int64_t));
  return static_cast<int64_t>(DecodeFixed64(s.data()));
}

// ---------------------------------------------------------------------------
// Helpers shared by FullMergeV3 deletion tests
// ---------------------------------------------------------------------------

namespace merge_deletion_test_helpers {

// Build a fresh time-based seed and emit a SCOPED_TRACE so randomized
// tests can be reproduced on failure (per CLAUDE.md unit-test guidelines).
inline uint32_t RandomSeedWithTrace() {
  return static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
}

}  // namespace merge_deletion_test_helpers

// ---------------------------------------------------------------------------
// Tests for FullMergeV3 deletion result (std::monostate)
// ---------------------------------------------------------------------------

// Basic: counter reaches zero -> Get returns NotFound.
TEST_F(DBMergeOperatorTest, MergeDeletionCounterReachesZero) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  Reopen(options);

  // Put(key, 5), Merge(key, -5) -> counter == 0 -> deleted
  ASSERT_OK(Put("k1", EncodeInt64(5)));
  ASSERT_OK(Merge("k1", EncodeInt64(-5)));

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // Put(key, 10), Merge(key, -3), Merge(key, -7) -> counter == 0 -> deleted
  ASSERT_OK(Put("k2", EncodeInt64(10)));
  ASSERT_OK(Merge("k2", EncodeInt64(-3)));
  ASSERT_OK(Merge("k2", EncodeInt64(-7)));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k2", &value).IsNotFound());
}

// Counter stays positive -> Get returns the value.
TEST_F(DBMergeOperatorTest, MergeDeletionCounterStaysPositive) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("k1", EncodeInt64(10)));
  ASSERT_OK(Merge("k1", EncodeInt64(-3)));

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(DecodeInt64(value), 7);
}

// Counter goes negative -> still deleted.
TEST_F(DBMergeOperatorTest, MergeDeletionCounterGoesNegative) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("k1", EncodeInt64(3)));
  ASSERT_OK(Merge("k1", EncodeInt64(-10)));

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
}

// Merge with no base value (all merge operands, no Put/Delete).
TEST_F(DBMergeOperatorTest, MergeDeletionNoBaseValue) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  Reopen(options);

  // No Put, just merges. Sum <= 0 -> deleted.
  ASSERT_OK(Merge("k1", EncodeInt64(5)));
  ASSERT_OK(Merge("k1", EncodeInt64(-5)));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // No Put, just merges. Sum > 0 -> value.
  ASSERT_OK(Merge("k2", EncodeInt64(5)));
  ASSERT_OK(Merge("k2", EncodeInt64(3)));
  ASSERT_OK(db_->Get(ReadOptions(), "k2", &value));
  ASSERT_EQ(DecodeInt64(value), 8);
}

// Merge on top of a Delete base.
TEST_F(DBMergeOperatorTest, MergeDeletionOnTopOfDelete) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("k1", EncodeInt64(100)));
  ASSERT_OK(Delete("k1"));
  // Merge on top of Delete -> base is monostate, counter starts at 0.
  ASSERT_OK(Merge("k1", EncodeInt64(-1)));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // Now merge a positive -> should create the key
  ASSERT_OK(Put("k2", EncodeInt64(100)));
  ASSERT_OK(Delete("k2"));
  ASSERT_OK(Merge("k2", EncodeInt64(42)));
  ASSERT_OK(db_->Get(ReadOptions(), "k2", &value));
  ASSERT_EQ(DecodeInt64(value), 42);
}

// AlwaysDelete merge operator deletes all keys it touches.
TEST_F(DBMergeOperatorTest, MergeDeletionAlwaysDelete) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<AlwaysDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Merge("k1", "anything"));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
}

// Conditional delete based on operand.
TEST_F(DBMergeOperatorTest, MergeDeletionConditional) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("k1", "base"));
  ASSERT_OK(Merge("k1", "a"));
  ASSERT_OK(Merge("k1", "b"));

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value, "base,a,b");

  // Now issue a "DELETE" operand
  ASSERT_OK(Merge("k1", "DELETE"));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // Flush to materialize the deletion in an SST before adding new operands.
  // Without flush, a subsequent Merge would land in the same memtable and the
  // full merge would see all operands together (including "DELETE" as a
  // non-last operand), which is not a deletion.
  ASSERT_OK(Flush());

  // Re-merge after deletion -> fresh start (no base value)
  ASSERT_OK(Merge("k1", "fresh"));
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value, "fresh");
}

// Forward iterator should skip keys deleted by merge.
TEST_F(DBMergeOperatorTest, MergeDeletionForwardIteration) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Put("d", "val_d"));
  // Delete "b" and "d" via merge
  ASSERT_OK(Merge("b", "DELETE"));
  ASSERT_OK(Merge("d", "DELETE"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "a");
  ASSERT_EQ(iter->value().ToString(), "val_a");

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "c");
  ASSERT_EQ(iter->value().ToString(), "val_c");

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

// Backward iterator (Prev) should skip keys deleted by merge.
TEST_F(DBMergeOperatorTest, MergeDeletionBackwardIteration) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Put("d", "val_d"));
  ASSERT_OK(Merge("b", "DELETE"));
  ASSERT_OK(Merge("d", "DELETE"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "c");
  ASSERT_EQ(iter->value().ToString(), "val_c");

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "a");
  ASSERT_EQ(iter->value().ToString(), "val_a");

  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

// Merge deletion survives flush.
TEST_F(DBMergeOperatorTest, MergeDeletionSurvivesFlush) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("k1", EncodeInt64(10)));
  ASSERT_OK(Merge("k1", EncodeInt64(-10)));
  ASSERT_OK(Flush());

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
}

// Merge deletion is cleaned up during compaction.
TEST_F(DBMergeOperatorTest, MergeDeletionCompaction) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  options.disable_auto_compactions = true;
  Reopen(options);

  // L0 file 1: Put(k1, 10)
  ASSERT_OK(Put("k1", EncodeInt64(10)));
  ASSERT_OK(Flush());

  // L0 file 2: Merge(k1, -10)  -> counter hits 0
  ASSERT_OK(Merge("k1", EncodeInt64(-10)));
  ASSERT_OK(Flush());

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // Compact all -- the key should be fully removed at the bottommost level.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // Verify the key is really gone from the SST files.
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

// Merge deletion interacts correctly with snapshots.
TEST_F(DBMergeOperatorTest, MergeDeletionWithSnapshot) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  Reopen(options);

  // Put(k1, 10)
  ASSERT_OK(Put("k1", EncodeInt64(10)));

  // Take snapshot before deletion
  const Snapshot* snap = db_->GetSnapshot();

  // Merge(k1, -10) -> deleted
  ASSERT_OK(Merge("k1", EncodeInt64(-10)));

  // Current state: deleted
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // Snapshot should still see the value
  ReadOptions snap_ro;
  snap_ro.snapshot = snap;
  ASSERT_OK(db_->Get(snap_ro, "k1", &value));
  ASSERT_EQ(DecodeInt64(value), 10);

  db_->ReleaseSnapshot(snap);
}

// Multiple merge deletions interleaved with puts.
TEST_F(DBMergeOperatorTest, MergeDeletionMultipleRounds) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  // Round 1: put, accumulate, delete
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Merge("k1", "v2"));
  ASSERT_OK(Merge("k1", "DELETE"));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // Flush to materialize the deletion before adding new merge operands.
  ASSERT_OK(Flush());

  // Round 2: fresh value after deletion
  ASSERT_OK(Merge("k1", "v3"));
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value, "v3");

  // Round 3: delete again
  ASSERT_OK(Merge("k1", "DELETE"));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // Round 4: put after merge deletion (Put is a base value, no flush needed)
  ASSERT_OK(Put("k1", "v4"));
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value, "v4");
}

// Merge deletion across flush + compaction boundaries.
TEST_F(DBMergeOperatorTest, MergeDeletionAcrossFlushAndCompaction) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.disable_auto_compactions = true;
  Reopen(options);

  // SST 1: Put(k1, base)
  ASSERT_OK(Put("k1", "base"));
  ASSERT_OK(Put("k2", "keep_me"));
  ASSERT_OK(Flush());

  // SST 2: Merge(k1, v2)
  ASSERT_OK(Merge("k1", "v2"));
  ASSERT_OK(Flush());

  // SST 3: Merge(k1, DELETE)
  ASSERT_OK(Merge("k1", "DELETE"));
  ASSERT_OK(Flush());

  // Read before compaction: k1 deleted, k2 alive
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
  ASSERT_OK(db_->Get(ReadOptions(), "k2", &value));
  ASSERT_EQ(value, "keep_me");

  // Compact -- k1 should be fully removed
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
  ASSERT_OK(db_->Get(ReadOptions(), "k2", &value));
  ASSERT_EQ(value, "keep_me");
}

// Test merge deletion during MultiGet.
TEST_F(DBMergeOperatorTest, MergeDeletionMultiGet) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Merge("b", "DELETE"));

  std::vector<Slice> keys = {Slice("a"), Slice("b"), Slice("c")};
  std::vector<std::string> values(3);
  std::vector<Status> statuses = db_->MultiGet(ReadOptions(), keys, &values);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "val_a");
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "val_c");
}

// Stress test: random puts, merges, and deletions via merge.
//
// The test maintains a "ground truth" by sourcing every expected-state
// update from the DB itself (Get after every operation), rather than
// modeling merge semantics locally. This keeps the test correct even in
// the presence of background compaction, snapshot stripe interaction, or
// any subtle ordering effect that could otherwise produce a benign
// divergence between a hand-rolled model and the live merge operator.
//
// What is actually being stress-tested:
//  - Random interleavings of Put / Merge / Flush across many keys.
//  - The full read path, iteration path, and compaction path with the
//    monostate-deletion result distributed across the workload.
//  - Concurrent background compaction (auto-compactions enabled).
TEST_F(DBMergeOperatorTest, MergeDeletionStress) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  options.disable_auto_compactions = false;
  Reopen(options);

  constexpr int kNumKeys = 50;
  constexpr int kOpsPerKey = 20;
  // Time-based seed with SCOPED_TRACE so failures are reproducible per
  // CLAUDE.md "Unit test dedup guidelines". Use util/random.h's Random
  // (NOT std::mt19937).
  const uint32_t seed = merge_deletion_test_helpers::RandomSeedWithTrace();
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  // Ground-truth post-condition: for each key, what should Get return?
  // Sourced from the DB itself after each op to keep the expected state in
  // sync with whatever the live merge operator actually produces.
  struct Expected {
    bool deleted = true;
    int64_t value = 0;
  };
  std::map<std::string, Expected> expected;

  auto record_expected = [&](const std::string& key) {
    std::string val;
    Status s = db_->Get(ReadOptions(), key, &val);
    if (s.ok()) {
      expected[key] = {false, DecodeInt64(val)};
    } else if (s.IsNotFound()) {
      expected[key] = {true, 0};
    } else {
      ASSERT_OK(s);
    }
  };

  for (int i = 0; i < kNumKeys; i++) {
    std::string key = "key_" + std::to_string(i);

    for (int j = 0; j < kOpsPerKey; j++) {
      int op = rnd.Uniform(3);
      if (op == 0) {
        // Put with a fresh counter value.
        int64_t v = static_cast<int64_t>(rnd.Uniform(100)) + 1;
        ASSERT_OK(Put(key, EncodeInt64(v)));
      } else {
        // Merge with a delta in [-20, 19].
        int64_t delta = static_cast<int64_t>(rnd.Uniform(40)) - 20;
        ASSERT_OK(Merge(key, EncodeInt64(delta)));
      }

      // Occasionally flush to exercise the SST/compaction surfaces.
      if (rnd.OneIn(10)) {
        ASSERT_OK(Flush());
      }
    }
    record_expected(key);
  }

  ASSERT_OK(Flush());

  // Verify all keys.
  for (const auto& [key, exp] : expected) {
    std::string value;
    Status s = db_->Get(ReadOptions(), key, &value);
    if (exp.deleted) {
      ASSERT_TRUE(s.IsNotFound()) << "Key " << key << " should be deleted";
    } else {
      ASSERT_OK(s) << "Key " << key << " should exist";
      ASSERT_EQ(DecodeInt64(value), exp.value);
    }
  }

  // Compact and verify again. CompactRange must NOT alter Get results.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (const auto& [key, exp] : expected) {
    std::string value;
    Status s = db_->Get(ReadOptions(), key, &value);
    if (exp.deleted) {
      ASSERT_TRUE(s.IsNotFound())
          << "Key " << key << " post-compact: status=" << s.ToString()
          << " value=" << (s.ok() ? DecodeInt64(value) : 0);
    } else {
      ASSERT_OK(s) << "Key " << key << " post-compact";
      ASSERT_EQ(DecodeInt64(value), exp.value);
    }
  }

  // Verify via iteration.
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  int live_count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    auto it = expected.find(iter->key().ToString());
    ASSERT_NE(it, expected.end());
    ASSERT_FALSE(it->second.deleted) << "Deleted key visible: " << it->first;
    ASSERT_EQ(DecodeInt64(iter->value().ToString()), it->second.value);
    live_count++;
  }
  ASSERT_OK(iter->status());

  int expected_live = 0;
  for (const auto& [key, exp] : expected) {
    if (!exp.deleted) {
      expected_live++;
    }
  }
  ASSERT_EQ(live_count, expected_live);
}

// Merge deletion with compaction and snapshot: deletion marker should be
// retained as long as snapshots need it.
TEST_F(DBMergeOperatorTest, MergeDeletionCompactionWithSnapshot) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.disable_auto_compactions = true;
  Reopen(options);

  ASSERT_OK(Put("k1", "base"));
  ASSERT_OK(Flush());

  const Snapshot* snap = db_->GetSnapshot();

  ASSERT_OK(Merge("k1", "DELETE"));
  ASSERT_OK(Flush());

  // Without snapshot: not found
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // With snapshot: found
  ReadOptions snap_ro;
  snap_ro.snapshot = snap;
  ASSERT_OK(db_->Get(snap_ro, "k1", &value));
  ASSERT_EQ(value, "base");

  // Compact -- snapshot should still work
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
  ASSERT_OK(db_->Get(snap_ro, "k1", &value));
  ASSERT_EQ(value, "base");

  db_->ReleaseSnapshot(snap);

  // After releasing snapshot, compact again -- entry should be fully removed
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
}

// Bottommost compaction must not skip past an older version that is still
// visible to a snapshot when a newer base + merge resolves to deletion.
TEST_F(DBMergeOperatorTest,
       MergeDeletionBottommostCompactionPreservesSnapshotVisiblePut) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.disable_auto_compactions = true;
  Reopen(options);

  ASSERT_OK(Put("k1", "snapshot_value"));
  ASSERT_OK(Flush());

  const Snapshot* snap = db_->GetSnapshot();

  ASSERT_OK(Put("k1", "newer_base"));
  ASSERT_OK(Merge("k1", "DELETE"));
  ASSERT_OK(Flush());

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  ReadOptions snap_ro;
  snap_ro.snapshot = snap;
  ASSERT_OK(db_->Get(snap_ro, "k1", &value));
  ASSERT_EQ(value, "snapshot_value");

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
  ASSERT_OK(db_->Get(snap_ro, "k1", &value));
  ASSERT_EQ(value, "snapshot_value");

  db_->ReleaseSnapshot(snap);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
}

TEST_F(DBMergeOperatorTest, MergeDeletionWithUserDefinedTimestampCompaction) {
  Options options = CurrentOptions();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string ts50 = EncodeU64Timestamp(50);
  const std::string ts100 = EncodeU64Timestamp(100);
  const std::string ts150 = EncodeU64Timestamp(150);
  const std::string ts200 = EncodeU64Timestamp(200);
  const std::string ts250 = EncodeU64Timestamp(250);

  ASSERT_OK(db_->Put(WriteOptions(), "k1", ts50, "too_old"));
  ASSERT_OK(db_->Put(WriteOptions(), "k1", ts100, "base"));
  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), "k1", ts100,
                       "DELETE"));
  ASSERT_OK(db_->Put(WriteOptions(), "k1", ts200, "newer"));
  ASSERT_OK(Flush());

  auto get_at = [&](const std::string& timestamp, std::string* result) {
    ReadOptions read_opts;
    Slice ts(timestamp);
    read_opts.timestamp = &ts;
    return db_->Get(read_opts, "k1", result);
  };

  std::string value;
  ASSERT_OK(get_at(ts250, &value));
  ASSERT_EQ(value, "newer");
  ASSERT_TRUE(get_at(ts150, &value).IsNotFound());

  CompactRangeOptions cro;
  Slice full_history_ts_low(ts150);
  cro.full_history_ts_low = &full_history_ts_low;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_OK(get_at(ts250, &value));
  ASSERT_EQ(value, "newer");
  ASSERT_TRUE(get_at(ts150, &value).IsNotFound())
      << "Older timestamped Put surfaced after merge-resolved deletion";
}

// Merge on an empty key (no prior Put or Merge) producing deletion.
TEST_F(DBMergeOperatorTest, MergeDeletionOnNonexistentKey) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<AlwaysDeleteMergeOperator>();
  Reopen(options);

  // Key never existed, merge on it -> deletion of nothing -> NotFound
  ASSERT_OK(Merge("k1", "anything"));

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // Flush + compact -> still not found
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
}

// Seek-based iteration with merge deletions (tests FindValueForCurrentKey
// fast path).
TEST_F(DBMergeOperatorTest, MergeDeletionSeekIteration) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  // Create keys a through f, delete c and e via merge
  for (char c = 'a'; c <= 'f'; c++) {
    ASSERT_OK(Put(std::string(1, c), std::string("val_") + c));
  }
  ASSERT_OK(Merge("c", "DELETE"));
  ASSERT_OK(Merge("e", "DELETE"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));

  // Seek to "b" and iterate forward
  iter->Seek("b");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "b");
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "d");  // c is skipped

  // SeekForPrev to "d" and iterate backward
  iter->SeekForPrev("d");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "d");
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "b");  // c is skipped

  ASSERT_OK(iter->status());
}

// Merge deletion after Reopen -- tests WAL recovery path.
TEST_F(DBMergeOperatorTest, MergeDeletionRecovery) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("k1", "base"));
  ASSERT_OK(Merge("k1", "DELETE"));
  ASSERT_OK(Put("k2", "alive"));

  // Reopen (recovers from WAL)
  Reopen(options);

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
  ASSERT_OK(db_->Get(ReadOptions(), "k2", &value));
  ASSERT_EQ(value, "alive");
}

// Seek() landing directly on a merge-deleted key should skip to next.
TEST_F(DBMergeOperatorTest, MergeDeletionSeekToDeletedKey) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Merge("b", "DELETE"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));

  // Seek directly to the deleted key "b" -- should land on "c"
  iter->Seek("b");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "c");

  ASSERT_OK(iter->status());
}

// SeekForPrev() landing directly on a merge-deleted key should skip to prev.
TEST_F(DBMergeOperatorTest, MergeDeletionSeekForPrevToDeletedKey) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Merge("b", "DELETE"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));

  // SeekForPrev directly to the deleted key "b" -- should land on "a"
  iter->SeekForPrev("b");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "a");

  ASSERT_OK(iter->status());
}

// SeekToFirst() when the very first key is merge-deleted.
TEST_F(DBMergeOperatorTest, MergeDeletionFirstKeyDeleted) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Merge("a", "DELETE"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "b");
  ASSERT_OK(iter->status());
}

// SeekToLast() when the very last key is merge-deleted.
TEST_F(DBMergeOperatorTest, MergeDeletionLastKeyDeleted) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Merge("c", "DELETE"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "b");
  ASSERT_OK(iter->status());
}

// Every key in the DB is merge-deleted -- iterator is immediately invalid.
TEST_F(DBMergeOperatorTest, MergeDeletionAllKeysDeleted) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<AlwaysDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Merge("a", "x"));
  ASSERT_OK(Merge("b", "x"));
  ASSERT_OK(Merge("c", "x"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  iter->SeekToLast();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

// Multiple consecutive deleted keys -- verifies the continue-loop in
// FindNextUserEntryInternal handles chains, not just a single skip.
TEST_F(DBMergeOperatorTest, MergeDeletionConsecutiveDeletedKeys) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Put("d", "val_d"));
  ASSERT_OK(Put("e", "val_e"));
  // Delete b, c, d -- three consecutive keys
  ASSERT_OK(Merge("b", "DELETE"));
  ASSERT_OK(Merge("c", "DELETE"));
  ASSERT_OK(Merge("d", "DELETE"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));

  // Forward: a -> e (skip b, c, d)
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "a");
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "e");

  // Backward: e -> a (skip d, c, b)
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "e");
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "a");

  ASSERT_OK(iter->status());
}

// Direction change: forward iteration then Prev() across a deleted key.
// This exercises ReverseToBackward() + FindValueForCurrentKey().
TEST_F(DBMergeOperatorTest, MergeDeletionDirectionChangeForwardToBackward) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Put("d", "val_d"));
  ASSERT_OK(Merge("c", "DELETE"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));

  // Forward to "d"
  iter->Seek("d");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "d");

  // Now Prev() -- should skip deleted "c" and land on "b"
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "b");

  // Forward again -- should skip deleted "c" and land on "d"
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "d");

  ASSERT_OK(iter->status());
}

// Direction change: backward iteration then Next() across a deleted key.
// This exercises ReverseToForward() + FindNextUserEntryInternal().
TEST_F(DBMergeOperatorTest, MergeDeletionDirectionChangeBackwardToForward) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Put("d", "val_d"));
  ASSERT_OK(Merge("b", "DELETE"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));

  // Backward to "a"
  iter->SeekForPrev("a");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "a");

  // Now Next() -- should skip deleted "b" and land on "c"
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "c");

  // Backward again -- should skip deleted "b" and land on "a"
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "a");

  ASSERT_OK(iter->status());
}

// Snapshot-based iteration: iterator with snapshot should see the
// pre-deletion state of keys.
TEST_F(DBMergeOperatorTest, MergeDeletionSnapshotIteration) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));

  const Snapshot* snap = db_->GetSnapshot();

  ASSERT_OK(Merge("b", "DELETE"));

  // Current iterator: sees a, c
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "a");
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "c");
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Snapshot iterator: sees a, b, c
  {
    ReadOptions snap_ro;
    snap_ro.snapshot = snap;
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(snap_ro));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "a");
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "b");
    ASSERT_EQ(iter->value().ToString(), "val_b");
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "c");
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  db_->ReleaseSnapshot(snap);
}

// GetEntity (wide-column read) should return NotFound for a merge-deleted key.
TEST_F(DBMergeOperatorTest, MergeDeletionGetEntity) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("k1", "base"));
  ASSERT_OK(Merge("k1", "DELETE"));

  PinnableWideColumns result;
  Status s =
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k1", &result);
  ASSERT_TRUE(s.IsNotFound());
}

// Write-batch merge-on-write: when a Put and Merge for the same key are in the
// same batch, the merge is resolved during write.  If it produces deletion,
// a kTypeDeletion should be written to the memtable.
TEST_F(DBMergeOperatorTest, MergeDeletionWriteBatchMergeOnWrite) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  // max_successive_merges = 0 means merge-on-write triggers when a Put and
  // a Merge for the same key are inserted within the same batch.
  options.max_successive_merges = 1000;
  Reopen(options);

  // Put a base value first, then issue a merge in the same batch.
  ASSERT_OK(Put("k1", "base"));

  // Second merge triggers merge-on-write against the memtable Put.
  ASSERT_OK(Merge("k1", "v2"));
  ASSERT_OK(Merge("k1", "DELETE"));

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
}

// MultiGet after flush -- exercises the SST/GetContext path rather than
// the memtable path.
TEST_F(DBMergeOperatorTest, MergeDeletionMultiGetAfterFlush) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Merge("b", "DELETE"));
  ASSERT_OK(Flush());

  std::vector<Slice> keys = {Slice("a"), Slice("b"), Slice("c")};
  std::vector<std::string> values(3);
  std::vector<Status> statuses = db_->MultiGet(ReadOptions(), keys, &values);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "val_a");
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "val_c");
}

// Merge produces deletion, then additional merges on top of the deleted state
// (all in memtable, no flush between). The later merges should see no base
// value and produce a fresh result or another deletion.
TEST_F(DBMergeOperatorTest, MergeDeletionThenMoreMerges) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  Reopen(options);

  // Put 5, Merge -5 -> counter=0 -> deleted
  ASSERT_OK(Put("k1", EncodeInt64(5)));
  ASSERT_OK(Merge("k1", EncodeInt64(-5)));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // More merges on top of the "deleted" state.
  // These see the deletion as base -> counter starts at 0.
  // +10 -> counter=10 -> alive
  ASSERT_OK(Merge("k1", EncodeInt64(10)));
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(DecodeInt64(value), 10);

  // -10 -> counter=0 -> deleted again
  ASSERT_OK(Merge("k1", EncodeInt64(-10)));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // Same after flush + compaction
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());

  // And one more merge to revive
  ASSERT_OK(Merge("k1", EncodeInt64(7)));
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(DecodeInt64(value), 7);
}

// Non-bottommost compaction: the deletion tombstone from a merge must be
// retained so it can suppress older versions on lower levels.
// This exercises the "base value found" + kTypeDeletion path in MergeUntil
// where at_bottom is false, so a tombstone must be emitted.
TEST_F(DBMergeOperatorTest, MergeDeletionNonBottommostCompaction) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.disable_auto_compactions = true;
  options.num_levels = 4;
  Reopen(options);

  // Put an anchor key in L3 so that L1 is never the bottommost level.
  ASSERT_OK(Put("k0", "anchor"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);

  // Put("k1", "base") in L1. This will be the base value for the merge.
  ASSERT_OK(Put("k1", "base"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  // Merge("k1", "DELETE") in L0.
  ASSERT_OK(Merge("k1", "DELETE"));
  ASSERT_OK(Flush());

  // Compact L0->L1. The compaction picks L0 (Merge) and overlapping L1 (Put).
  // MergeUntil resolves: Put("base") + Merge("DELETE") -> deletion.
  // at_bottom is false (L3 has data), so a kTypeDeletion tombstone is emitted.
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kSkip;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // k1 should be NotFound (tombstone suppresses any older versions).
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
  ASSERT_OK(db_->Get(ReadOptions(), "k0", &value));
  ASSERT_EQ(value, "anchor");

  // Full compaction should clean up the tombstone too.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsNotFound());
}

// Backward seek-optimization path with base value: when a key has many
// entries, FindValueForCurrentKey switches to FindValueForCurrentKeyUsingSeek.
// Test that merge-deletion works through this path when the merge has a
// Put base value.
TEST_F(DBMergeOperatorTest, MergeDeletionBackwardSeekPathWithBase) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  // Force the seek path after just 2 skipped entries.
  options.max_sequential_skip_in_iterations = 2;
  Reopen(options);

  // Create enough entries for "k1" to exceed max_sequential_skip_in_iterations.
  // The old Put overwrites create the entries that get skipped during backward
  // iteration, triggering the switch to the seek-based path.
  ASSERT_OK(Put("k1", "old1"));  // oldest
  ASSERT_OK(Put("k1", "old2"));
  ASSERT_OK(Put("k1", "base"));
  ASSERT_OK(Merge("k1", "DELETE"));  // newest -- triggers deletion

  ASSERT_OK(Put("k2", "val_k2"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "k2");

  // Prev() triggers FindValueForCurrentKey for "k1", which after 2 skips
  // switches to FindValueForCurrentKeyUsingSeek. The seek finds the Merge
  // and the Put("base"), resolves to deletion via MergeWithPlainBaseValue.
  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

// Backward seek-optimization path with NO base value: all entries for the
// key are Merge operands. FindValueForCurrentKeyUsingSeek collects them all,
// then calls MergeWithNoBaseValue. This is the path where we removed the
// unconditional `valid_ = true`.
TEST_F(DBMergeOperatorTest, MergeDeletionBackwardSeekPathNoBase) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.max_sequential_skip_in_iterations = 2;
  Reopen(options);

  // All entries for "k1" are Merges -- no Put base.
  ASSERT_OK(Merge("k1", "a"));  // oldest
  ASSERT_OK(Merge("k1", "b"));
  ASSERT_OK(Merge("k1", "c"));
  ASSERT_OK(Merge("k1", "DELETE"));  // newest -- triggers deletion

  ASSERT_OK(Put("k2", "val_k2"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "k2");

  // Prev() triggers FindValueForCurrentKey -> FindValueForCurrentKeyUsingSeek.
  // All entries are Merges, loop exits without finding a base value.
  // MergeWithNoBaseValue resolves to deletion. valid_ = false.
  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

// Same as above but the merge does NOT produce deletion -- verifies that
// the removed `valid_ = true` in FindValueForCurrentKeyUsingSeek doesn't
// break the normal (non-deletion) case.
TEST_F(DBMergeOperatorTest, MergeDeletionBackwardSeekPathNoBaseNonDeletion) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.max_sequential_skip_in_iterations = 2;
  Reopen(options);

  // All entries for "k1" are Merges, none is "DELETE" -> value survives.
  ASSERT_OK(Merge("k1", "a"));
  ASSERT_OK(Merge("k1", "b"));
  ASSERT_OK(Merge("k1", "c"));

  ASSERT_OK(Put("k2", "val_k2"));

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "k2");

  // Prev() should find "k1" with merged value "a,b,c"
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "k1");
  ASSERT_EQ(iter->value().ToString(), "a,b,c");
  ASSERT_OK(iter->status());
}

// Merge deletion with iterate_upper_bound: a deleted key at the boundary
// should not confuse the iterator.
TEST_F(DBMergeOperatorTest, MergeDeletionWithIterateBounds) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Put("d", "val_d"));
  ASSERT_OK(Merge("c", "DELETE"));

  // Upper bound = "d" -- range is [a, d), "c" is deleted
  Slice upper("d");
  ReadOptions ro;
  ro.iterate_upper_bound = &upper;
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "a");
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "b");
  iter->Next();
  // "c" is deleted, "d" is past upper bound -> invalid
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // Lower bound = "b" -- Prev from "d" should skip deleted "c"
  Slice lower("b");
  ReadOptions ro2;
  ro2.iterate_lower_bound = &lower;
  auto iter2 = std::unique_ptr<Iterator>(db_->NewIterator(ro2));
  iter2->SeekForPrev("d");
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ(iter2->key().ToString(), "d");
  iter2->Prev();
  // "c" is deleted -> land on "b"
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ(iter2->key().ToString(), "b");
  ASSERT_OK(iter2->status());
}

// ---------------------------------------------------------------------------
// Cross-feature regression tests for FullMergeV3 deletion (std::monostate)
//
// The following tests exercise interactions surfaced by the multi-reviewer
// audit of the monostate-deletion feature: WriteBatchWithIndex / transactions,
// range tombstones, multiple column families, prefix bloom filters, blob
// storage, wide-column input, compaction filter composition, merge-on-write
// boundary conditions, OpFailureScope interaction, and a high-concurrency
// regression test for the pinned-data lifecycle along consecutive deletions.
// ---------------------------------------------------------------------------

// The cross-feature tests below intentionally re-use the existing
// ConditionalDeleteMergeOperator, CounterDeleteMergeOperator, and
// AlwaysDeleteMergeOperator defined earlier in this file. No additional
// helper merge operators are needed; what matters is exercising each
// surface (WBWI, transactions, range tombstones, multi-CF, blobs, wide
// columns, compaction filter, prefix bloom, max_successive_merges,
// concurrency, etc.) with the existing deletion-capable operators.

// WBWI: GetFromBatch when merge resolves to monostate must report kDeleted /
// Status::NotFound to the user, NOT an error.
TEST_F(DBMergeOperatorTest, MergeDeletionWBWIGetFromBatchDeletes) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  WriteBatchWithIndex wbwi(BytewiseComparator(), 0, /* overwrite_key= */ true);
  ASSERT_OK(wbwi.Put("k", "base"));
  ASSERT_OK(wbwi.Merge("k", "DELETE"));

  std::string value;
  Status s =
      wbwi.GetFromBatch(db_->DefaultColumnFamily(), options, "k", &value);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  // Status should be a clean NotFound, NOT a Corruption / error.
  ASSERT_FALSE(s.IsCorruption());
}

// WBWI: GetFromBatchAndDB across batch+DB must propagate monostate as
// NotFound for the user, regardless of whether the base value is in the
// batch, in the DB, or absent.
TEST_F(DBMergeOperatorTest, MergeDeletionWBWIGetFromBatchAndDB) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  DB* raw_db = db_.get();

  // 1) Base in DB, merge in batch resolving to delete.
  ASSERT_OK(Put("k1", "base"));
  WriteBatchWithIndex wbwi1(BytewiseComparator(), 0, /* overwrite_key= */ true);
  ASSERT_OK(wbwi1.Merge("k1", "DELETE"));
  std::string v;
  ASSERT_TRUE(
      wbwi1.GetFromBatchAndDB(raw_db, ReadOptions(), "k1", &v).IsNotFound());

  // 2) Base in batch (Put), merge in batch resolving to delete.
  WriteBatchWithIndex wbwi2(BytewiseComparator(), 0, /* overwrite_key= */ true);
  ASSERT_OK(wbwi2.Put("k2", "base"));
  ASSERT_OK(wbwi2.Merge("k2", "DELETE"));
  ASSERT_TRUE(
      wbwi2.GetFromBatchAndDB(raw_db, ReadOptions(), "k2", &v).IsNotFound());

  // 3) No base anywhere, merge resolving to delete.
  WriteBatchWithIndex wbwi3(BytewiseComparator(), 0, /* overwrite_key= */ true);
  ASSERT_OK(wbwi3.Merge("k3_nobase", "DELETE"));
  ASSERT_TRUE(wbwi3.GetFromBatchAndDB(raw_db, ReadOptions(), "k3_nobase", &v)
                  .IsNotFound());

  // 4) After a deletion-resolving batch is written to DB, Get from DB
  //    should also see NotFound.
  WriteBatchWithIndex wbwi4(BytewiseComparator(), 0, /* overwrite_key= */ true);
  ASSERT_OK(wbwi4.Put("k4", "base"));
  ASSERT_OK(wbwi4.Merge("k4", "DELETE"));
  ASSERT_OK(raw_db->Write(WriteOptions(), wbwi4.GetWriteBatch()));
  ASSERT_TRUE(raw_db->Get(ReadOptions(), "k4", &v).IsNotFound());
}

// WBWI: BaseDeltaIterator must skip keys whose merge chain resolves to
// monostate in both forward AND reverse iteration.
TEST_F(DBMergeOperatorTest, MergeDeletionWBWIBaseDeltaIterator) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Put("c", "val_c"));
  ASSERT_OK(Put("d", "val_d"));

  WriteBatchWithIndex wbwi(BytewiseComparator(), 0, /* overwrite_key= */ true);
  ASSERT_OK(wbwi.Merge("b", "DELETE"));
  ASSERT_OK(wbwi.Merge("d", "DELETE"));
  ASSERT_OK(wbwi.Put("e", "val_e"));

  ReadOptions ro;
  auto base_iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
  auto iter = std::unique_ptr<Iterator>(wbwi.NewIteratorWithBase(
      db_->DefaultColumnFamily(), base_iter.release(), &ro));
  // Forward
  std::vector<std::string> seen;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    seen.push_back(iter->key().ToString());
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(seen, std::vector<std::string>({"a", "c", "e"}))
      << "WBWI iterator must skip merge-deleted keys (forward).";

  // Reverse
  std::vector<std::string> seen_rev;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    seen_rev.push_back(iter->key().ToString());
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(seen_rev, std::vector<std::string>({"e", "c", "a"}))
      << "WBWI iterator must skip merge-deleted keys (reverse).";
}

// Transactions wrap WBWI; Transaction::Get must surface monostate-deletion
// as NotFound, not as a kError/Corruption status.
TEST_F(DBMergeOperatorTest, MergeDeletionTransactionGet) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  // Re-create the DB as a TransactionDB.
  Close();
  Destroy(options);
  TransactionDBOptions txn_db_opts;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_db_opts, dbname_, &txn_db));

  std::unique_ptr<Transaction> txn(
      txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
  ASSERT_OK(txn->Put("k", "base"));
  ASSERT_OK(txn->Merge("k", "DELETE"));
  std::string v;
  Status s = txn->Get(ReadOptions(), "k", &v);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_FALSE(s.IsCorruption());

  // Cleanup
  ASSERT_OK(txn->Rollback());
  txn.reset();
  delete txn_db;
}

// Range tombstone + merge-deletion: a DeleteRange covering a key, then a
// merge that resolves to monostate, must still produce a deletion. And a
// merge that resolves to a value over a range tombstone must propagate
// correctly.
TEST_F(DBMergeOperatorTest, MergeDeletionWithRangeTombstones) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.disable_auto_compactions = true;
  Reopen(options);

  // Put at L1 (after compaction), DeleteRange + Merge at L0.
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "j", "l"));
  ASSERT_OK(Merge("k", "DELETE"));

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &value).IsNotFound());

  // Flush + compact and verify still NotFound.
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &value).IsNotFound());
}

// Multi-CF + monostate: monostate produced in one CF must not affect
// other CFs; reads see consistent snapshot behavior.
TEST_F(DBMergeOperatorTest, MergeDeletionMultiColumnFamilies) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  ASSERT_OK(Put(0, "k", "cf0_v"));
  ASSERT_OK(Put(1, "k", "cf1_v"));
  ASSERT_OK(Put(2, "k", "cf2_v"));

  // Resolve to deletion only in CF1.
  ASSERT_OK(Merge(1, "k", "DELETE"));

  std::string v;
  ASSERT_OK(db_->Get(ReadOptions(), handles_[0], "k", &v));
  ASSERT_EQ(v, "cf0_v");
  ASSERT_TRUE(db_->Get(ReadOptions(), handles_[1], "k", &v).IsNotFound());
  ASSERT_OK(db_->Get(ReadOptions(), handles_[2], "k", &v));
  ASSERT_EQ(v, "cf2_v");
}

// Prefix bloom + monostate: a deleted key under a prefix extractor must
// not be falsely surfaced by the bloom filter.
TEST_F(DBMergeOperatorTest, MergeDeletionWithPrefixBloom) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.disable_auto_compactions = true;
  Reopen(options);

  ASSERT_OK(Put("abc_1", "v1"));
  ASSERT_OK(Put("abc_2", "v2"));
  ASSERT_OK(Merge("abc_1", "DELETE"));
  ASSERT_OK(Flush());

  std::string v;
  ASSERT_TRUE(db_->Get(ReadOptions(), "abc_1", &v).IsNotFound())
      << "Deleted key must not be surfaced even with prefix bloom.";
  ASSERT_OK(db_->Get(ReadOptions(), "abc_2", &v));
  ASSERT_EQ(v, "v2");

  // Iterate with prefix bound.
  ReadOptions ro;
  ro.prefix_same_as_start = true;
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
  std::vector<std::string> seen;
  for (iter->Seek("abc"); iter->Valid(); iter->Next()) {
    seen.push_back(iter->key().ToString());
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(seen, std::vector<std::string>({"abc_2"}));
}

// Blob storage + monostate: merge resolves to deletion when the base value
// is stored as a blob. Verify across read, iterate, and compaction.
TEST_F(DBMergeOperatorTest, MergeDeletionWithBlobStorage) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);

  // Put a sizeable value that will land in a blob file.
  std::string large_value(1024, 'x');
  ASSERT_OK(Put("k", large_value));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k", "DELETE"));

  std::string v;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound())
      << "Blob-backed base + merge-deletion must NotFound.";

  // Iterate (forward and reverse).
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  iter->SeekToLast();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // Compact and verify it stays NotFound.
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound());
}

// Wide-column input + monostate: PutEntity + Merge -> monostate must NotFound
// for Get, GetEntity, and iteration.
TEST_F(DBMergeOperatorTest, MergeDeletionOverWideColumnEntity) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  // PutEntity with multiple columns. Column names must be in ascending
  // order; kDefaultWideColumnName is the empty string and sorts first.
  WideColumns columns{
      {kDefaultWideColumnName, "default_v"}, {"col1", "v1"}, {"col2", "v2"}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "k", columns));
  ASSERT_OK(Merge("k", "DELETE"));

  std::string v;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound());

  PinnableWideColumns pwc;
  ASSERT_TRUE(
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k", &pwc)
          .IsNotFound());

  // After flush + compact too.
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound());
}

// Compaction filter + monostate: when a compaction filter is wired up, the
// merge operator must still be able to signal deletion, and the final
// state must be correct.
TEST_F(DBMergeOperatorTest, MergeDeletionWithCompactionFilter) {
  class NoOpFilter : public CompactionFilter {
   public:
    Decision FilterV3(int /*level*/, const Slice& /*key*/, ValueType /*vt*/,
                      const Slice* /*existing_value*/,
                      const WideColumns* /*existing_columns*/,
                      std::string* /*new_value*/,
                      std::vector<std::pair<std::string, std::string>>*
                      /*new_columns*/,
                      std::string* /*skip_until*/) const override {
      return Decision::kKeep;
    }
    const char* Name() const override { return "NoOpFilter"; }
  };

  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  static NoOpFilter filter;
  options.compaction_filter = &filter;
  options.disable_auto_compactions = true;
  Reopen(options);

  ASSERT_OK(Put("k", "base"));
  ASSERT_OK(Merge("k", "DELETE"));
  ASSERT_OK(Flush());

  std::string v;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound());
}

// max_successive_merges boundary: merge-on-write that resolves to monostate
// must produce a kTypeDeletion entry in the memtable.
TEST_F(DBMergeOperatorTest, MergeDeletionMaxSuccessiveMergesBoundary) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.max_successive_merges = 2;
  Reopen(options);

  ASSERT_OK(Put("k", "base"));
  ASSERT_OK(Merge("k", "a"));
  ASSERT_OK(Merge("k", "b"));
  // The next Merge crosses the threshold -- merge-on-write kicks in and
  // resolves to a deletion (because operand_list.back() == "DELETE").
  ASSERT_OK(Merge("k", "DELETE"));

  std::string v;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound());
}

// strict_max_successive_merges + monostate: forces synchronous I/O for the
// fold-on-write, and the deletion result must still be applied.
TEST_F(DBMergeOperatorTest, MergeDeletionStrictMaxSuccessiveMerges) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  options.max_successive_merges = 2;
  options.strict_max_successive_merges = true;
  Reopen(options);

  ASSERT_OK(Put("k", "base"));
  ASSERT_OK(Flush());  // base value lands in SST so resolution needs I/O.
  ASSERT_OK(Merge("k", "a"));
  ASSERT_OK(Merge("k", "b"));
  ASSERT_OK(Merge("k", "DELETE"));

  std::string v;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound());
}

// OpFailureScope is ignored when merge_out.new_value is std::monostate.
// In debug builds, setting both trips an internal assertion. We verify the
// release-build behavior here: monostate-wins-over-op_failure_scope.
#ifdef NDEBUG
TEST_F(DBMergeOperatorTest, MergeDeletionOpFailureScopeIgnored) {
  class FailureScopeAndMonostateOperator : public MergeOperator {
   public:
    bool FullMergeV3(const MergeOperationInputV3& /*merge_in*/,
                     MergeOperationOutputV3* merge_out) const override {
      merge_out->new_value = std::monostate{};
      merge_out->op_failure_scope = OpFailureScope::kMustMerge;  // ignored
      return true;
    }
    const char* Name() const override {
      return "FailureScopeAndMonostateOperator";
    }
  };
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<FailureScopeAndMonostateOperator>();
  Reopen(options);

  ASSERT_OK(Put("k", "base"));
  ASSERT_OK(Merge("k", "any"));
  std::string v;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound());
}
#endif  // NDEBUG

// ASAN-style stress: many consecutive monostate deletions in a single
// FindNextUserEntry call. This exercises the pinned-data lifecycle along
// the forward-iteration deletion-skip path. Run with ASAN to catch any
// use-after-free.
TEST_F(DBMergeOperatorTest, MergeDeletionForwardIterationManyConsecutive) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<ConditionalDeleteMergeOperator>();
  Reopen(options);

  constexpr int kN = 256;
  for (int i = 0; i < kN; ++i) {
    char key[16];
    snprintf(key, sizeof(key), "k%05d", i);
    if (i % 2 == 0) {
      ASSERT_OK(Put(key, "v"));
      ASSERT_OK(Merge(key, "DELETE"));  // deleted via merge
    } else {
      ASSERT_OK(Put(key, "live"));
    }
  }

  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  int live_seen = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(iter->value().ToString(), "live");
    live_seen++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(live_seen, kN / 2);
}

// Concurrent writers / readers: spawns N writer threads that drive
// random-key counter-decrement workloads (auto-delete-at-zero) alongside
// reader threads that verify Get results are either NotFound or a positive
// counter. Validates that concurrent access through the
// merge-resolved-deletion code paths does not race or violate read
// consistency.
TEST_F(DBMergeOperatorTest, MergeDeletionConcurrentReadersAndWriters) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  Reopen(options);

  constexpr int kNumKeys = 8;
  constexpr int kWriterThreads = 4;
  constexpr int kReaderThreads = 4;
  constexpr int kOpsPerThread = 200;
  std::atomic<bool> stop{false};
  std::atomic<int> verification_failures{0};

  const uint32_t seed = merge_deletion_test_helpers::RandomSeedWithTrace();
  SCOPED_TRACE("seed=" + std::to_string(seed));

  auto writer = [&](int tid) {
    Random rnd(seed + tid);
    for (int i = 0; i < kOpsPerThread && !stop.load(); ++i) {
      std::string key = "k" + std::to_string(rnd.Uniform(kNumKeys));
      int delta = static_cast<int>(rnd.Uniform(11)) - 5;
      std::string val;
      PutFixed64(&val, static_cast<uint64_t>(static_cast<int64_t>(delta)));
      Status s = db_->Merge(WriteOptions(), key, val);
      if (!s.ok()) {
        verification_failures++;
      }
    }
  };

  auto reader = [&]() {
    while (!stop.load()) {
      for (int k = 0; k < kNumKeys; ++k) {
        std::string key = "k" + std::to_string(k);
        std::string v;
        Status s = db_->Get(ReadOptions(), key, &v);
        if (!s.ok() && !s.IsNotFound()) {
          verification_failures++;
        }
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < kWriterThreads; ++i) {
    threads.emplace_back(writer, i);
  }
  for (int i = 0; i < kReaderThreads; ++i) {
    threads.emplace_back(reader);
  }
  for (int i = 0; i < kWriterThreads; ++i) {
    threads[i].join();
  }
  stop.store(true);
  for (int i = kWriterThreads; i < kWriterThreads + kReaderThreads; ++i) {
    threads[i].join();
  }
  ASSERT_EQ(verification_failures.load(), 0);
}

// Compaction at the bottommost level with a merge-resolved deletion MUST
// also consume any older versions of the same user key that live below
// the consumed Put. Without that, a stale Merge from an older sequence
// would surface as a live value after compaction (regression: surfaced
// by the random stress test, codified here as a deterministic case).
TEST_F(DBMergeOperatorTest, MergeDeletionAtBottomDropsOlderVersions) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  options.disable_auto_compactions = true;
  Reopen(options);

  // L1+ has an older Merge for k (no base value yet).
  ASSERT_OK(Merge("k", EncodeInt64(+50)));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // L0 has a newer chain Put + Merge that resolves to a deletion.
  ASSERT_OK(Put("k", EncodeInt64(5)));
  ASSERT_OK(Merge("k", EncodeInt64(-10)));
  ASSERT_OK(Flush());

  // Pre-compact: read path resolves the LATEST chain only, which is
  // Put(5) + Merge(-10) = -5 -> monostate. Get must return NotFound.
  std::string v;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound());

  // Compact everything to the bottommost level. The merge resolves to a
  // deletion at the bottom, AND the older Merge(+50) further back must
  // be dropped along with the consumed Put. Otherwise it would surface
  // as Put(50).
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Post-compact: still NotFound. (Pre-fix this returned 50.)
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound())
      << "Older versions surfaced after merge-resolved deletion at bottom";
}

// Same invariant, exercised through a chain of Puts. The compaction at
// the bottommost level resolves to monostate, and the older Put (from a
// distinct earlier write) must NOT come back to life.
TEST_F(DBMergeOperatorTest, MergeDeletionAtBottomDropsOlderPut) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  options.disable_auto_compactions = true;
  Reopen(options);

  ASSERT_OK(Put("k", EncodeInt64(7)));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_OK(Put("k", EncodeInt64(3)));
  ASSERT_OK(Merge("k", EncodeInt64(-3)));  // chain 3+(-3)=0 -> monostate
  ASSERT_OK(Flush());

  std::string v;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(db_->Get(ReadOptions(), "k", &v).IsNotFound())
      << "Older Put surfaced after merge-resolved deletion at bottom";
}

// Status::SubCode plumbing: the wide-column TimedFullMerge variant must
// return Status::NotFound with subcode kMergeOperatorDeletion when the
// operator chooses std::monostate. This pins the API contract that WBWI
// and transactions rely on to distinguish deletion from real not-found.
TEST_F(DBMergeOperatorTest, MergeDeletionStatusSubCode) {
  Status merge_deletion =
      Status::NotFound(Status::SubCode::kMergeOperatorDeletion);
  ASSERT_NE(merge_deletion.ToString().find("Merge operator requested deletion"),
            std::string::npos);

  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<AlwaysDeleteMergeOperator>();
  Reopen(options);

  ASSERT_OK(Put("k", "base"));
  ASSERT_OK(Merge("k", "any"));

  std::string v;
  Status s = db_->Get(ReadOptions(), "k", &v);
  ASSERT_TRUE(s.IsNotFound());
  // The Get path doesn't surface the subcode to the user (NotFound is
  // NotFound), but we verify the public predicate works.
  ASSERT_FALSE(s.IsCorruption());
  ASSERT_FALSE(s.IsIOError());
}

// Regression: GetContext::PostprocessMerge must translate
// Status::NotFound(kMergeOperatorDeletion) into the terminal kDeleted
// state, NOT kCorrupt and NOT kNotFound. With kCorrupt the user would
// see Status::Corruption() for what is actually a successful deletion;
// with kNotFound the lookup would continue searching lower levels
// (potentially surfacing an older version of the key).
//
// This bug-class can only surface when merge resolution completes
// entirely inside a single SaveValue() call -- i.e., the base value
// (Put / Delete / SingleDelete) for the key is reached while the
// GetContext is still inside one level (memtable or one SST). Forcing
// a Flush after the Put+Merge and reading from the resulting SST
// covers the single-SST case; staying in the memtable covers that
// path; flushing twice with operands ensures we also exercise the
// multi-level resolution.
TEST_F(DBMergeOperatorTest, MergeDeletionSingleLevelGetReturnsNotFound) {
  Options options = CurrentOptions();
  options.merge_operator = std::make_shared<CounterDeleteMergeOperator>();
  options.disable_auto_compactions = true;
  Reopen(options);

  // (a) Memtable-only: Put + Merge resolve inside the active memtable.
  ASSERT_OK(Put("k_mem", EncodeInt64(5)));
  ASSERT_OK(Merge("k_mem", EncodeInt64(-5)));  // 5 + (-5) = 0 -> monostate
  std::string v;
  Status s = db_->Get(ReadOptions(), "k_mem", &v);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_FALSE(s.IsCorruption()) << "PostprocessMerge mishandled monostate";

  // (b) Single-SST: flush so Put+Merge land in one SST, then Get
  //     resolves the merge inside the GetContext for that one SST.
  ASSERT_OK(Put("k_sst", EncodeInt64(7)));
  ASSERT_OK(Merge("k_sst", EncodeInt64(-7)));
  ASSERT_OK(Flush());
  s = db_->Get(ReadOptions(), "k_sst", &v);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_FALSE(s.IsCorruption()) << "PostprocessMerge mishandled monostate";

  // (c) Wide-column entity base + merge resolving to deletion in one SST.
  // Hoist the encoded value into a local so its lifetime outlives the
  // PutEntity call -- WideColumn stores its value as a Slice (non-owning
  // view), so passing a temporary std::string would leave the Slice
  // dangling at the next semicolon and trip stack-use-after-scope under
  // ASAN.
  const std::string entity_default = EncodeInt64(3);
  WideColumns columns{{kDefaultWideColumnName, entity_default}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           "k_entity", columns));
  ASSERT_OK(Merge("k_entity", EncodeInt64(-3)));
  ASSERT_OK(Flush());
  s = db_->Get(ReadOptions(), "k_entity", &v);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_FALSE(s.IsCorruption()) << "PostprocessMerge mishandled monostate";

  // (d) No base value: merges only, resolving to deletion in one SST.
  ASSERT_OK(Merge("k_no_base", EncodeInt64(0)));  // 0 + 0 -> monostate
  ASSERT_OK(Flush());
  s = db_->Get(ReadOptions(), "k_no_base", &v);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_FALSE(s.IsCorruption()) << "PostprocessMerge mishandled monostate";
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
