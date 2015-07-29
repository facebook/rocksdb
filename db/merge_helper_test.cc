//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <string>
#include <vector>

#include "db/merge_helper.h"
#include "rocksdb/comparator.h"
#include "util/coding.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

class MergeHelperTest : public testing::Test {
 public:
  MergeHelperTest() = default;
  ~MergeHelperTest() = default;

  void RunUInt64MergeHelper(SequenceNumber stop_before, bool at_bottom) {
    InitIterator();
    merge_op_ = MergeOperators::CreateUInt64AddOperator();
    merge_helper_.reset(new MergeHelper(BytewiseComparator(), merge_op_.get(),
          nullptr, 2U, true));
    merge_helper_->MergeUntil(iter_.get(), stop_before, at_bottom, nullptr,
                              Env::Default());
  }

  void RunStringAppendMergeHelper(SequenceNumber stop_before, bool at_bottom) {
    InitIterator();
    merge_op_ = MergeOperators::CreateStringAppendTESTOperator();
    merge_helper_.reset(new MergeHelper(BytewiseComparator(), merge_op_.get(),
          nullptr, 2U, true));
    merge_helper_->MergeUntil(iter_.get(), stop_before, at_bottom, nullptr,
                              Env::Default());
  }

  std::string Key(const std::string& user_key, const SequenceNumber& seq,
      const ValueType& t) {
    std::string ikey;
    AppendInternalKey(&ikey, ParsedInternalKey(Slice(user_key), seq, t));
    return ikey;
  }

  void AddKeyVal(const std::string& user_key, const SequenceNumber& seq,
      const ValueType& t, const std::string& val) {
    ks_.push_back(Key(user_key, seq, t));
    vs_.push_back(val);
  }

  void InitIterator() {
    iter_.reset(new test::VectorIterator(ks_, vs_));
    iter_->SeekToFirst();
  }

  std::string EncodeInt(uint64_t x) {
    std::string result;
    PutFixed64(&result, x);
    return result;
  }

  void CheckState(bool success, int iter_pos) {
    ASSERT_EQ(success, merge_helper_->IsSuccess());
    if (iter_pos == -1) {
      ASSERT_FALSE(iter_->Valid());
    } else {
      ASSERT_EQ(ks_[iter_pos], iter_->key());
    }
  }

  std::unique_ptr<test::VectorIterator> iter_;
  std::shared_ptr<MergeOperator> merge_op_;
  std::unique_ptr<MergeHelper> merge_helper_;
  std::vector<std::string> ks_;
  std::vector<std::string> vs_;
};

// If MergeHelper encounters a new key on the last level, we know that
// the key has no more history and it can merge keys.
TEST_F(MergeHelperTest, MergeAtBottomSuccess) {
  AddKeyVal("a", 20, kTypeMerge, EncodeInt(1U));
  AddKeyVal("a", 10, kTypeMerge, EncodeInt(3U));
  AddKeyVal("b", 10, kTypeMerge, EncodeInt(4U));  // <- Iterator after merge

  RunUInt64MergeHelper(0, true);
  CheckState(true, 2);
  ASSERT_EQ(Key("a", 20, kTypeValue), merge_helper_->key());
  ASSERT_EQ(EncodeInt(4U), merge_helper_->value());
}

// Merging with a value results in a successful merge.
TEST_F(MergeHelperTest, MergeValue) {
  AddKeyVal("a", 40, kTypeMerge, EncodeInt(1U));
  AddKeyVal("a", 30, kTypeMerge, EncodeInt(3U));
  AddKeyVal("a", 20, kTypeValue, EncodeInt(4U));  // <- Iterator after merge
  AddKeyVal("a", 10, kTypeMerge, EncodeInt(1U));

  RunUInt64MergeHelper(0, false);
  CheckState(true, 3);
  ASSERT_EQ(Key("a", 40, kTypeValue), merge_helper_->key());
  ASSERT_EQ(EncodeInt(8U), merge_helper_->value());
}

// Merging stops before a snapshot.
TEST_F(MergeHelperTest, SnapshotBeforeValue) {
  AddKeyVal("a", 50, kTypeMerge, EncodeInt(1U));
  AddKeyVal("a", 40, kTypeMerge, EncodeInt(3U));  // <- Iterator after merge
  AddKeyVal("a", 30, kTypeMerge, EncodeInt(1U));
  AddKeyVal("a", 20, kTypeValue, EncodeInt(4U));
  AddKeyVal("a", 10, kTypeMerge, EncodeInt(1U));

  RunUInt64MergeHelper(31, true);
  CheckState(false, 2);
  ASSERT_EQ(Key("a", 50, kTypeMerge), merge_helper_->keys()[0]);
  ASSERT_EQ(EncodeInt(4U), merge_helper_->values()[0]);
}

// MergeHelper preserves the operand stack for merge operators that
// cannot do a partial merge.
TEST_F(MergeHelperTest, NoPartialMerge) {
  AddKeyVal("a", 50, kTypeMerge, "v2");
  AddKeyVal("a", 40, kTypeMerge, "v");  // <- Iterator after merge
  AddKeyVal("a", 30, kTypeMerge, "v");

  RunStringAppendMergeHelper(31, true);
  CheckState(false, 2);
  ASSERT_EQ(Key("a", 40, kTypeMerge), merge_helper_->keys()[0]);
  ASSERT_EQ("v", merge_helper_->values()[0]);
  ASSERT_EQ(Key("a", 50, kTypeMerge), merge_helper_->keys()[1]);
  ASSERT_EQ("v2", merge_helper_->values()[1]);
}

// Merging with a deletion turns the deletion into a value
TEST_F(MergeHelperTest, MergeDeletion) {
  AddKeyVal("a", 30, kTypeMerge, EncodeInt(3U));
  AddKeyVal("a", 20, kTypeDeletion, "");

  RunUInt64MergeHelper(15, false);
  CheckState(true, -1);
  ASSERT_EQ(Key("a", 30, kTypeValue), merge_helper_->key());
  ASSERT_EQ(EncodeInt(3U), merge_helper_->value());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
