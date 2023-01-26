//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/debug.h"
#include "table/block_based/block_builder.h"
#if !defined(ROCKSDB_LITE)
#include "test_util/sync_point.h"
#endif
#include "rocksdb/merge_operator.h"
#include "utilities/fault_injection_env.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/sortlist.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

namespace ROCKSDB_NAMESPACE {

namespace {
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
}  // anonymous namespace

class DBMergeOperandTest : public DBTestBase {
 public:
  DBMergeOperandTest()
      : DBTestBase("db_merge_operand_test", /*env_do_fsync=*/true) {}
};

TEST_F(DBMergeOperandTest, CacheEvictedMergeOperandReadAfterFreeBug) {
  // There was a bug of reading merge operands after they are mistakely freed
  // in DB::GetMergeOperands, which is surfaced by cache full.
  // See PR#9507 for more.
  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.env = env_;
  BlockBasedTableOptions table_options;

  // Small cache to simulate cache full
  table_options.block_cache = NewLRUCache(1);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Reopen(options);
  int num_records = 4;
  int number_of_operands = 0;
  std::vector<PinnableSlice> values(num_records);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = num_records;

  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k1", "v3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k1", "v4"));

  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(number_of_operands, 4);
  ASSERT_EQ(values[0].ToString(), "v1");
  ASSERT_EQ(values[1].ToString(), "v2");
  ASSERT_EQ(values[2].ToString(), "v3");
  ASSERT_EQ(values[3].ToString(), "v4");
}

TEST_F(DBMergeOperandTest, FlushedMergeOperandReadAfterFreeBug) {
  // Repro for a bug where a memtable containing a merge operand could be
  // deleted before the merge operand was saved to the result.
  auto options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen(options);

  ASSERT_OK(Merge("key", "value"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::GetImpl:PostMemTableGet:0",
        "DBMergeOperandTest::FlushedMergeOperandReadAfterFreeBug:PreFlush"},
       {"DBMergeOperandTest::FlushedMergeOperandReadAfterFreeBug:PostFlush",
        "DBImpl::GetImpl:PostMemTableGet:1"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  auto flush_thread = port::Thread([&]() {
    TEST_SYNC_POINT(
        "DBMergeOperandTest::FlushedMergeOperandReadAfterFreeBug:PreFlush");
    ASSERT_OK(Flush());
    TEST_SYNC_POINT(
        "DBMergeOperandTest::FlushedMergeOperandReadAfterFreeBug:PostFlush");
  });

  PinnableSlice value;
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = 1;
  int number_of_operands;
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "key", &value, &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(1, number_of_operands);

  flush_thread.join();
}

TEST_F(DBMergeOperandTest, GetMergeOperandsBasic) {
  Options options;
  options.create_if_missing = true;
  // Use only the latest two merge operands.
  options.merge_operator = std::make_shared<LimitedStringAppendMergeOp>(2, ',');
  options.env = env_;
  Reopen(options);
  int num_records = 4;
  int number_of_operands = 0;
  std::vector<PinnableSlice> values(num_records);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = num_records;

  // k0 value in memtable
  ASSERT_OK(Put("k0", "PutARock"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k0", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "PutARock");

  // k0.1 value in SST
  ASSERT_OK(Put("k0.1", "RockInSST"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k0.1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "RockInSST");

  // All k1 values are in memtable.
  ASSERT_OK(Merge("k1", "a"));
  ASSERT_OK(Put("k1", "x"));
  ASSERT_OK(Merge("k1", "b"));
  ASSERT_OK(Merge("k1", "c"));
  ASSERT_OK(Merge("k1", "d"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "x");
  ASSERT_EQ(values[1], "b");
  ASSERT_EQ(values[2], "c");
  ASSERT_EQ(values[3], "d");

  // expected_max_number_of_operands is less than number of merge operands so
  // status should be Incomplete.
  merge_operands_info.expected_max_number_of_operands = num_records - 1;
  Status status = db_->GetMergeOperands(
      ReadOptions(), db_->DefaultColumnFamily(), "k1", values.data(),
      &merge_operands_info, &number_of_operands);
  ASSERT_EQ(status.IsIncomplete(), true);
  merge_operands_info.expected_max_number_of_operands = num_records;

  // All k1.1 values are in memtable.
  ASSERT_OK(Merge("k1.1", "r"));
  ASSERT_OK(Delete("k1.1"));
  ASSERT_OK(Merge("k1.1", "c"));
  ASSERT_OK(Merge("k1.1", "k"));
  ASSERT_OK(Merge("k1.1", "s"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k1.1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "c");
  ASSERT_EQ(values[1], "k");
  ASSERT_EQ(values[2], "s");

  // All k2 values are flushed to L0 into a single file.
  ASSERT_OK(Merge("k2", "q"));
  ASSERT_OK(Merge("k2", "w"));
  ASSERT_OK(Merge("k2", "e"));
  ASSERT_OK(Merge("k2", "r"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k2", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "q");
  ASSERT_EQ(values[1], "w");
  ASSERT_EQ(values[2], "e");
  ASSERT_EQ(values[3], "r");

  // All k2.1 values are flushed to L0 into a single file.
  ASSERT_OK(Merge("k2.1", "m"));
  ASSERT_OK(Put("k2.1", "l"));
  ASSERT_OK(Merge("k2.1", "n"));
  ASSERT_OK(Merge("k2.1", "o"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k2.1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "l,n,o");

  // All k2.2 values are flushed to L0 into a single file.
  ASSERT_OK(Merge("k2.2", "g"));
  ASSERT_OK(Delete("k2.2"));
  ASSERT_OK(Merge("k2.2", "o"));
  ASSERT_OK(Merge("k2.2", "t"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k2.2", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "o,t");

  // Do some compaction that will make the following tests more predictable
  //  Slice start("PutARock");
  //  Slice end("t");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // All k3 values are flushed and are in different files.
  ASSERT_OK(Merge("k3", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "bc"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "de"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k3", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "ab");
  ASSERT_EQ(values[1], "bc");
  ASSERT_EQ(values[2], "cd");
  ASSERT_EQ(values[3], "de");

  // All k3.1 values are flushed and are in different files.
  ASSERT_OK(Merge("k3.1", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k3.1", "bc"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3.1", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3.1", "de"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k3.1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "bc");
  ASSERT_EQ(values[1], "cd");
  ASSERT_EQ(values[2], "de");

  // All k3.2 values are flushed and are in different files.
  ASSERT_OK(Merge("k3.2", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("k3.2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3.2", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3.2", "de"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k3.2", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "cd");
  ASSERT_EQ(values[1], "de");

  // All K4 values are in different levels
  ASSERT_OK(Merge("k4", "ba"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);
  ASSERT_OK(Merge("k4", "cb"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_OK(Merge("k4", "dc"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Merge("k4", "ed"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k4", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "ba");
  ASSERT_EQ(values[1], "cb");
  ASSERT_EQ(values[2], "dc");
  ASSERT_EQ(values[3], "ed");

  // First 3 k5 values are in SST and next 4 k5 values are in Immutable
  // Memtable
  ASSERT_OK(Merge("k5", "who"));
  ASSERT_OK(Merge("k5", "am"));
  ASSERT_OK(Merge("k5", "i"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k5", "remember"));
  ASSERT_OK(Merge("k5", "i"));
  ASSERT_OK(Merge("k5", "am"));
  ASSERT_OK(Merge("k5", "rocks"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k5", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "remember");
  ASSERT_EQ(values[1], "i");
  ASSERT_EQ(values[2], "am");
}

TEST_F(DBMergeOperandTest, BlobDBGetMergeOperandsBasic) {
  Options options;
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  // Use only the latest two merge operands.
  options.merge_operator = std::make_shared<LimitedStringAppendMergeOp>(2, ',');
  options.env = env_;
  Reopen(options);
  int num_records = 4;
  int number_of_operands = 0;
  std::vector<PinnableSlice> values(num_records);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = num_records;

  // All k1 values are in memtable.
  ASSERT_OK(Put("k1", "x"));
  ASSERT_OK(Merge("k1", "b"));
  ASSERT_OK(Merge("k1", "c"));
  ASSERT_OK(Merge("k1", "d"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "x");
  ASSERT_EQ(values[1], "b");
  ASSERT_EQ(values[2], "c");
  ASSERT_EQ(values[3], "d");

  // expected_max_number_of_operands is less than number of merge operands so
  // status should be Incomplete.
  merge_operands_info.expected_max_number_of_operands = num_records - 1;
  Status status = db_->GetMergeOperands(
      ReadOptions(), db_->DefaultColumnFamily(), "k1", values.data(),
      &merge_operands_info, &number_of_operands);
  ASSERT_EQ(status.IsIncomplete(), true);
  merge_operands_info.expected_max_number_of_operands = num_records;

  // All k2 values are flushed to L0 into a single file.
  ASSERT_OK(Put("k2", "q"));
  ASSERT_OK(Merge("k2", "w"));
  ASSERT_OK(Merge("k2", "e"));
  ASSERT_OK(Merge("k2", "r"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k2", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "q,w,e,r");

  // Do some compaction that will make the following tests more predictable
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // All k3 values are flushed and are in different files.
  ASSERT_OK(Put("k3", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "bc"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "de"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k3", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "ab");
  ASSERT_EQ(values[1], "bc");
  ASSERT_EQ(values[2], "cd");
  ASSERT_EQ(values[3], "de");

  // All K4 values are in different levels
  ASSERT_OK(Put("k4", "ba"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);
  ASSERT_OK(Merge("k4", "cb"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_OK(Merge("k4", "dc"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Merge("k4", "ed"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k4", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "ba");
  ASSERT_EQ(values[1], "cb");
  ASSERT_EQ(values[2], "dc");
  ASSERT_EQ(values[3], "ed");
}

TEST_F(DBMergeOperandTest, GetMergeOperandsLargeResultOptimization) {
  // These constants are chosen to trigger the large result optimization
  // (pinning a bundle of `DBImpl` resources).
  const int kNumOperands = 1024;
  const int kOperandLen = 1024;

  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  Random rnd(301);
  std::vector<std::string> expected_merge_operands;
  expected_merge_operands.reserve(kNumOperands);
  for (int i = 0; i < kNumOperands; ++i) {
    expected_merge_operands.emplace_back(rnd.RandomString(kOperandLen));
    ASSERT_OK(Merge("key", expected_merge_operands.back()));
  }

  std::vector<PinnableSlice> merge_operands(kNumOperands);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = kNumOperands;
  int num_merge_operands = 0;
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "key", merge_operands.data(),
                                  &merge_operands_info, &num_merge_operands));
  ASSERT_EQ(num_merge_operands, kNumOperands);

  // Ensures the large result optimization was used.
  for (int i = 0; i < kNumOperands; ++i) {
    ASSERT_TRUE(merge_operands[i].IsPinned());
  }

  // Add a Flush() to change the `SuperVersion` to challenge the resource
  // pinning.
  ASSERT_OK(Flush());

  for (int i = 0; i < kNumOperands; ++i) {
    ASSERT_EQ(expected_merge_operands[i], merge_operands[i]);
  }
}

TEST_F(DBMergeOperandTest, GetMergeOperandsBaseDeletionInImmMem) {
  // In this test, "k1" has a MERGE in a mutable memtable on top of a base
  // DELETE in an immutable memtable.
  Options opts = CurrentOptions();
  opts.max_write_buffer_number = 10;
  opts.min_write_buffer_number_to_merge = 10;
  opts.merge_operator = MergeOperators::CreateDeprecatedPutOperator();
  Reopen(opts);

  ASSERT_OK(Put("k1", "val"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("k0", "val"));
  ASSERT_OK(Delete("k1"));
  ASSERT_OK(Put("k2", "val"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Merge("k1", "val"));

  {
    std::vector<PinnableSlice> values(2);

    GetMergeOperandsOptions merge_operands_info;
    merge_operands_info.expected_max_number_of_operands =
        static_cast<int>(values.size());

    std::string key = "k1", from_db;
    int number_of_operands = 0;
    ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                    key, values.data(), &merge_operands_info,
                                    &number_of_operands));
    ASSERT_EQ(1, number_of_operands);
    from_db = values[0].ToString();
    ASSERT_EQ("val", from_db);
  }

  {
    std::string val;
    ASSERT_OK(db_->Get(ReadOptions(), "k1", &val));
    ASSERT_EQ("val", val);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
