//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// #include <iostream>
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/debug.h"
#include "table/block_based/block_builder.h"
#include "test_util/fault_injection_test_env.h"
#if !defined(ROCKSDB_LITE)
#include "test_util/sync_point.h"
#endif
#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend2.h"
#include "utilities/merge_operators/sortlist.h"

namespace rocksdb {

class DBMergeOperandTest : public DBTestBase {
 public:
	DBMergeOperandTest() : DBTestBase("/db_merge_operand_test") {}

	bool binary_search(std::vector<int>& data, int start, int end, int key) {
		if (start > end) return false;
		int mid = start + (end-start)/2;
		if (data[mid] == key) return true;
		else if (data[mid] > key) return binary_search(data, start, mid-1, key);
		else return binary_search(data, mid+1, end, key);
	}

	bool print_info = false;
	bool print_perf = true;
};

TEST_F(DBMergeOperandTest, GetMergeOperandsBasic) {
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

  Options options;
  options.create_if_missing = true;
  // Use only the latest two merge operands.
  options.merge_operator = std::make_shared<LimitedStringAppendMergeOp>(2, ',');
  options.env = env_;
  Reopen(options);
  int num_records = 4;

  // All K1 values are in memtable.
  ASSERT_OK(Merge("k1", "a"));
  Put("k1", "x");
  ASSERT_OK(Merge("k1", "b"));
  ASSERT_OK(Merge("k1", "c"));
  ASSERT_OK(Merge("k1", "d"));
  std::vector<PinnableSlice> values(num_records);
  MergeOperandsInfo merge_operands_info;
  merge_operands_info.expected_number_of_operands = num_records;
  db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(), "k1",
                        values.data(), &merge_operands_info);
  ASSERT_EQ(values[0], "x");
  ASSERT_EQ(values[1], "b");
  ASSERT_EQ(values[2], "c");
  ASSERT_EQ(values[3], "d");

  // num_records is less than number of merge operands so status should be
  // Aborted.
  merge_operands_info.expected_number_of_operands = num_records-1;
  Status status =
      db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(), "k1",
                            values.data(), &merge_operands_info);
  ASSERT_EQ(status.IsAborted(), true);
  merge_operands_info.expected_number_of_operands = num_records;

  // All K2 values are flushed to L0 into a single file.
  ASSERT_OK(Merge("k2", "a"));
  ASSERT_OK(Merge("k2", "b"));
  ASSERT_OK(Merge("k2", "c"));
  ASSERT_OK(Merge("k2", "d"));
  ASSERT_OK(Flush());
  db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(), "k2",
                        values.data(), &merge_operands_info);
  ASSERT_EQ(values[0], "a");
  ASSERT_EQ(values[1], "b");
  ASSERT_EQ(values[2], "c");
  ASSERT_EQ(values[3], "d");

  // All K3 values are flushed and are in different files.
  ASSERT_OK(Merge("k3", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "bc"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "de"));
  db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(), "k3",
                        values.data(), &merge_operands_info);
  ASSERT_EQ(values[0], "ab");
  ASSERT_EQ(values[1], "bc");
  ASSERT_EQ(values[2], "cd");
  ASSERT_EQ(values[3], "de");

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
  db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(), "k4",
                        values.data(), &merge_operands_info);
  ASSERT_EQ(values[0], "ab");
  ASSERT_EQ(values[1], "bc");
  ASSERT_EQ(values[2], "cd");
  ASSERT_EQ(values[3], "de");

  // First 3 k5 values are in SST and next 4 k5 values are in Immutable Memtable
  ASSERT_OK(Merge("k5", "who"));
  ASSERT_OK(Merge("k5", "am"));
  ASSERT_OK(Merge("k5", "i"));
  ASSERT_OK(Flush());
  Put("k5", "remember");
  ASSERT_OK(Merge("k5", "i"));
  ASSERT_OK(Merge("k5", "am"));
  ASSERT_OK(Merge("k5", "rocks"));
  dbfull()->TEST_SwitchMemtable();
  db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(), "k5",
                        values.data(), &merge_operands_info);
  ASSERT_EQ(values[0], "remember");
  ASSERT_EQ(values[1], "i");
  ASSERT_EQ(values[2], "am");

}

TEST_F(DBMergeOperandTest, PerfTest) {
	Options options = CurrentOptions();
	options.create_if_missing = true;
	options.merge_operator = MergeOperators::CreateSortAndSearchOperator();
	DestroyAndReopen(options);

	Random rnd(301);

	const int kTotalMerges = 100000;
	std::string key = "my_key";
	std::string value;

	// Do kTotalMerges merges
	for (int i = 1; i < kTotalMerges; i++) {
	if (i%100 == 0) {
		ASSERT_OK(db_->Merge(WriteOptions(), key, value));
	        if (print_info) std::cout << value << "\n";
		value.clear();
	} else {
		value.append(std::to_string(i)).append(",");
	}
	}
	// Get API call
	PinnableSlice p_slice;
	uint64_t st = env_->NowNanos();
	db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &p_slice);
	SortList s;
	std::vector<int> data;
	s.make_vector(data, p_slice);
	int lookup_key = 1;
	bool found = binary_search(data, 0, data.size()-1, lookup_key);
	if (print_info) std::cout << "Found key? " << std::to_string(found) << "\n";
	uint64_t sp = env_->NowNanos();
	if (print_perf) std::cout << "Get: " << (sp-st)/1000000000.0 << "\n";
	std::string* dat_ = p_slice.GetSelf();
	if (print_info) std::cout << "Sample data from Get API call: " << dat_->substr(0,10);
	data.clear();

	// GetMergeOperands API call
	std::vector<PinnableSlice> a_slice((kTotalMerges/100)+1);
	st = env_->NowNanos();
	MergeOperandsInfo merge_operands_info;
	merge_operands_info.expected_number_of_operands = (kTotalMerges/100)+1;
	db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(), key, a_slice.data(), &merge_operands_info);
	int to_print = 0;
	if (print_info)  {
		std::cout << "Sample data from GetMergeOperands API call: ";
		for (PinnableSlice& psl : a_slice) {
		  std::cout << to_print << " : " <<  *psl.GetSelf() << "\n";
		  if (to_print++ > 2) break;
		}
	}
	for (PinnableSlice& psl : a_slice) {
		s.make_vector(data, psl);
		found = binary_search(data, 0, data.size()-1, lookup_key);
		data.clear();
		if (found) break;
	}
	if (print_info)  std::cout << "Found key? " << std::to_string(found)  << "\n";
	sp = env_->NowNanos();
	if (print_perf)  std::cout << "Get Merge operands: " << (sp-st)/1000000000.0 << "\n";
}

}

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
