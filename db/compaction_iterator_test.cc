//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/compaction_iterator.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class CompactionIteratorTest : public testing::Test {
 public:
  CompactionIteratorTest() : cmp_(BytewiseComparator()), snapshots_({}) {}

  void InitIterator(const std::vector<std::string>& ks,
                    const std::vector<std::string>& vs,
                    SequenceNumber last_sequence) {
    merge_helper_.reset(new MergeHelper(Env::Default(), cmp_, nullptr, nullptr,
                                        nullptr, 0U, false, 0));
    iter_.reset(new test::VectorIterator(ks, vs));
    iter_->SeekToFirst();
    c_iter_.reset(new CompactionIterator(iter_.get(), cmp_, merge_helper_.get(),
                                         last_sequence, &snapshots_,
                                         Env::Default(), false));
  }

  const Comparator* cmp_;
  std::vector<SequenceNumber> snapshots_;
  std::unique_ptr<MergeHelper> merge_helper_;
  std::unique_ptr<test::VectorIterator> iter_;
  std::unique_ptr<CompactionIterator> c_iter_;
};

// It is possible that the output of the compaction iterator is empty even if
// the input is not.
TEST_F(CompactionIteratorTest, EmptyResult) {
  InitIterator({test::KeyStr("a", 5, kTypeSingleDeletion),
                test::KeyStr("a", 3, kTypeValue)},
               {"", "val"}, 5);
  c_iter_->SeekToFirst();
  ASSERT_FALSE(c_iter_->Valid());
}

// If there is a corruption after a single deletion, the corrupted key should
// be preserved.
TEST_F(CompactionIteratorTest, CorruptionAfterSingleDeletion) {
  InitIterator({test::KeyStr("a", 5, kTypeSingleDeletion),
                test::KeyStr("a", 3, kTypeValue, true),
                test::KeyStr("b", 10, kTypeValue)},
               {"", "val", "val2"}, 10);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 5, kTypeSingleDeletion),
            c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 3, kTypeValue, true), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("b", 10, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_FALSE(c_iter_->Valid());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
