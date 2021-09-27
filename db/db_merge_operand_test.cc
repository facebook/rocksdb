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
}  // namespace

class DBMergeOperandTest : public DBTestBase {
 public:
  DBMergeOperandTest()
      : DBTestBase("/db_merge_operand_test", /*env_do_fsync=*/true) {}
};

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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
