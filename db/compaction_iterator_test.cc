//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/compaction_iterator.h"

#include <string>
#include <vector>

#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class CompactionIteratorTest : public testing::Test {
 public:
  CompactionIteratorTest()
      : cmp_(BytewiseComparator()), icmp_(cmp_), snapshots_({}) {}

  void InitIterators(const std::vector<std::string>& ks,
                     const std::vector<std::string>& vs,
                     const std::vector<std::string>& range_del_ks,
                     const std::vector<std::string>& range_del_vs,
                     SequenceNumber last_sequence) {
    std::unique_ptr<InternalIterator> range_del_iter(
        new test::VectorIterator(range_del_ks, range_del_vs));
    range_del_agg_.reset(new RangeDelAggregator(icmp_, snapshots_));
    range_del_agg_->AddTombstones(std::move(range_del_iter));

    merge_helper_.reset(new MergeHelper(Env::Default(), cmp_, nullptr, nullptr,
                                        nullptr, 0U, false, 0));
    iter_.reset(new test::VectorIterator(ks, vs));
    iter_->SeekToFirst();
    c_iter_.reset(new CompactionIterator(
        iter_.get(), cmp_, merge_helper_.get(), last_sequence, &snapshots_,
        kMaxSequenceNumber, Env::Default(), false, range_del_agg_.get()));
  }

  void AddSnapshot(SequenceNumber snapshot) { snapshots_.push_back(snapshot); }

  const Comparator* cmp_;
  const InternalKeyComparator icmp_;
  std::vector<SequenceNumber> snapshots_;
  std::unique_ptr<MergeHelper> merge_helper_;
  std::unique_ptr<test::VectorIterator> iter_;
  std::unique_ptr<CompactionIterator> c_iter_;
  std::unique_ptr<RangeDelAggregator> range_del_agg_;
};

// It is possible that the output of the compaction iterator is empty even if
// the input is not.
TEST_F(CompactionIteratorTest, EmptyResult) {
  InitIterators({test::KeyStr("a", 5, kTypeSingleDeletion),
                 test::KeyStr("a", 3, kTypeValue)},
                {"", "val"}, {}, {}, 5);
  c_iter_->SeekToFirst();
  ASSERT_FALSE(c_iter_->Valid());
}

// If there is a corruption after a single deletion, the corrupted key should
// be preserved.
TEST_F(CompactionIteratorTest, CorruptionAfterSingleDeletion) {
  InitIterators({test::KeyStr("a", 5, kTypeSingleDeletion),
                 test::KeyStr("a", 3, kTypeValue, true),
                 test::KeyStr("b", 10, kTypeValue)},
                {"", "val", "val2"}, {}, {}, 10);
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

TEST_F(CompactionIteratorTest, SimpleRangeDeletion) {
  InitIterators({test::KeyStr("morning", 5, kTypeValue),
                 test::KeyStr("morning", 2, kTypeValue),
                 test::KeyStr("night", 3, kTypeValue)},
                {"zao", "zao", "wan"},
                {test::KeyStr("ma", 4, kTypeRangeDeletion)}, {"mz"}, 5);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("morning", 5, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("night", 3, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_F(CompactionIteratorTest, RangeDeletionWithSnapshots) {
  AddSnapshot(10);
  std::vector<std::string> ks1;
  ks1.push_back(test::KeyStr("ma", 28, kTypeRangeDeletion));
  std::vector<std::string> vs1{"mz"};
  std::vector<std::string> ks2{test::KeyStr("morning", 15, kTypeValue),
                               test::KeyStr("morning", 5, kTypeValue),
                               test::KeyStr("night", 40, kTypeValue),
                               test::KeyStr("night", 20, kTypeValue)};
  std::vector<std::string> vs2{"zao 15", "zao 5", "wan 40", "wan 20"};
  InitIterators(ks2, vs2, ks1, vs1, 40);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("morning", 5, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("night", 40, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_FALSE(c_iter_->Valid());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
