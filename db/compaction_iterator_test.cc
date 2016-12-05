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

class LoggingForwardVectorIterator : public InternalIterator {
 public:
  struct Action {
    enum class Type {
      SEEK_TO_FIRST,
      SEEK,
      NEXT,
    };

    Type type;
    std::string arg;

    explicit Action(Type _type, std::string _arg = "")
        : type(_type), arg(_arg) {}

    bool operator==(const Action& rhs) const {
      return std::tie(type, arg) == std::tie(rhs.type, rhs.arg);
    }
  };

  LoggingForwardVectorIterator(const std::vector<std::string>& keys,
                               const std::vector<std::string>& values)
      : keys_(keys), values_(values), current_(keys.size()) {
    assert(keys_.size() == values_.size());
  }

  virtual bool Valid() const override { return current_ < keys_.size(); }

  virtual void SeekToFirst() override {
    log.emplace_back(Action::Type::SEEK_TO_FIRST);
    current_ = 0;
  }
  virtual void SeekToLast() override { assert(false); }

  virtual void Seek(const Slice& target) override {
    log.emplace_back(Action::Type::SEEK, target.ToString());
    current_ = std::lower_bound(keys_.begin(), keys_.end(), target.ToString()) -
               keys_.begin();
  }

  virtual void SeekForPrev(const Slice& target) override { assert(false); }

  virtual void Next() override {
    assert(Valid());
    log.emplace_back(Action::Type::NEXT);
    current_++;
  }
  virtual void Prev() override { assert(false); }

  virtual Slice key() const override {
    assert(Valid());
    return Slice(keys_[current_]);
  }
  virtual Slice value() const override {
    assert(Valid());
    return Slice(values_[current_]);
  }

  virtual Status status() const override { return Status::OK(); }

  std::vector<Action> log;

 private:
  std::vector<std::string> keys_;
  std::vector<std::string> values_;
  size_t current_;
};

class FakeCompaction : public CompactionIterator::CompactionProxy {
 public:
  FakeCompaction() = default;

  virtual int level(size_t compaction_input_level) const { return 0; }
  virtual bool KeyNotExistsBeyondOutputLevel(
      const Slice& user_key, std::vector<size_t>* level_ptrs) const {
    return false;
  }
  virtual bool bottommost_level() const { return false; }
  virtual int number_levels() const { return 1; }
  virtual Slice GetLargestUserKey() const {
    return "\xff\xff\xff\xff\xff\xff\xff\xff\xff";
  }
};

class CompactionIteratorTest : public testing::Test {
 public:
  CompactionIteratorTest()
      : cmp_(BytewiseComparator()), icmp_(cmp_), snapshots_({}) {}

  void InitIterators(const std::vector<std::string>& ks,
                     const std::vector<std::string>& vs,
                     const std::vector<std::string>& range_del_ks,
                     const std::vector<std::string>& range_del_vs,
                     SequenceNumber last_sequence,
                     MergeOperator* merge_op = nullptr,
                     CompactionFilter* filter = nullptr) {
    std::unique_ptr<InternalIterator> range_del_iter(
        new test::VectorIterator(range_del_ks, range_del_vs));
    range_del_agg_.reset(new RangeDelAggregator(icmp_, snapshots_));
    ASSERT_OK(range_del_agg_->AddTombstones(std::move(range_del_iter)));

    std::unique_ptr<CompactionIterator::CompactionProxy> compaction;
    if (filter) {
      compaction.reset(new FakeCompaction());
    }

    merge_helper_.reset(new MergeHelper(Env::Default(), cmp_, merge_op, filter,
                                        nullptr, 0U, false, 0));
    iter_.reset(new LoggingForwardVectorIterator(ks, vs));
    iter_->SeekToFirst();
    c_iter_.reset(new CompactionIterator(
        iter_.get(), cmp_, merge_helper_.get(), last_sequence, &snapshots_,
        kMaxSequenceNumber, Env::Default(), false, range_del_agg_.get(),
        std::move(compaction), filter));
  }

  void AddSnapshot(SequenceNumber snapshot) { snapshots_.push_back(snapshot); }

  const Comparator* cmp_;
  const InternalKeyComparator icmp_;
  std::vector<SequenceNumber> snapshots_;
  std::unique_ptr<MergeHelper> merge_helper_;
  std::unique_ptr<LoggingForwardVectorIterator> iter_;
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

TEST_F(CompactionIteratorTest, CompactionFilterSkipUntil) {
  // Expect no merging attempts.
  class MergeOp : public MergeOperator {
   public:
    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override {
      ADD_FAILURE();
      return false;
    }
    bool PartialMergeMulti(const Slice& key,
                           const std::deque<Slice>& operand_list,
                           std::string* new_value,
                           Logger* logger) const override {
      ADD_FAILURE();
      return false;
    }
    const char* Name() const override {
      return "CompactionIteratorTest.CompactionFilterSkipUntil::MergeOp";
    }
  };

  class Filter : public CompactionFilter {
    virtual Decision FilterV2(int level, const Slice& key, ValueType t,
                              const Slice& existing_value,
                              std::string* new_value,
                              std::string* skip_until) const override {
      std::string k = key.ToString();
      std::string v = existing_value.ToString();
      // See InitIterators() call below for the sequence of keys and their
      // filtering decisions. Here we closely assert that compaction filter is
      // called with the expected keys and only them, and with the right values.
      if (k == "a") {
        EXPECT_EQ(ValueType::kValue, t);
        EXPECT_EQ("av50", v);
        return Decision::kKeep;
      }
      if (k == "b") {
        EXPECT_EQ(ValueType::kValue, t);
        EXPECT_EQ("bv60", v);
        *skip_until = "d+";
        return Decision::kRemoveAndSkipUntil;
      }
      if (k == "e") {
        EXPECT_EQ(ValueType::kMergeOperand, t);
        EXPECT_EQ("em71", v);
        return Decision::kKeep;
      }
      if (k == "f") {
        if (v == "fm65") {
          EXPECT_EQ(ValueType::kMergeOperand, t);
          *skip_until = "f";
        } else {
          EXPECT_EQ("fm30", v);
          EXPECT_EQ(ValueType::kMergeOperand, t);
          *skip_until = "g+";
        }
        return Decision::kRemoveAndSkipUntil;
      }
      if (k == "h") {
        EXPECT_EQ(ValueType::kValue, t);
        EXPECT_EQ("hv91", v);
        return Decision::kKeep;
      }
      if (k == "i") {
        EXPECT_EQ(ValueType::kMergeOperand, t);
        EXPECT_EQ("im95", v);
        *skip_until = "z";
        return Decision::kRemoveAndSkipUntil;
      }
      ADD_FAILURE();
      return Decision::kKeep;
    }

    const char* Name() const override {
      return "CompactionIteratorTest.CompactionFilterSkipUntil::Filter";
    }
  };

  MergeOp merge_op;
  Filter filter;
  InitIterators(
      {test::KeyStr("a", 50, kTypeValue),  // keep
       test::KeyStr("a", 45, kTypeMerge),
       test::KeyStr("b", 60, kTypeValue),  // skip to "d+"
       test::KeyStr("b", 40, kTypeValue), test::KeyStr("c", 35, kTypeValue),
       test::KeyStr("d", 70, kTypeMerge),
       test::KeyStr("e", 71, kTypeMerge),  // keep
       test::KeyStr("f", 65, kTypeMerge),  // skip to "f", aka keep
       test::KeyStr("f", 30, kTypeMerge),  // skip to "g+"
       test::KeyStr("f", 25, kTypeValue), test::KeyStr("g", 90, kTypeValue),
       test::KeyStr("h", 91, kTypeValue),  // keep
       test::KeyStr("i", 95, kTypeMerge),  // skip to "z"
       test::KeyStr("j", 99, kTypeValue)},
      {"av50", "am45", "bv60", "bv40", "cv35", "dm70", "em71", "fm65", "fm30",
       "fv25", "gv90", "hv91", "im95", "jv99"},
      {}, {}, kMaxSequenceNumber, &merge_op, &filter);

  // Compaction should output just "a", "e" and "h" keys.
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 50, kTypeValue), c_iter_->key().ToString());
  ASSERT_EQ("av50", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("e", 71, kTypeMerge), c_iter_->key().ToString());
  ASSERT_EQ("em71", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("h", 91, kTypeValue), c_iter_->key().ToString());
  ASSERT_EQ("hv91", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_FALSE(c_iter_->Valid());

  // Check that the compaction iterator did the correct sequence of calls on
  // the underlying iterator.
  using A = LoggingForwardVectorIterator::Action;
  using T = A::Type;
  std::vector<A> expected_actions = {
      A(T::SEEK_TO_FIRST),
      A(T::NEXT),
      A(T::NEXT),
      A(T::SEEK, test::KeyStr("d+", kMaxSequenceNumber, kValueTypeForSeek)),
      A(T::NEXT),
      A(T::NEXT),
      A(T::SEEK, test::KeyStr("g+", kMaxSequenceNumber, kValueTypeForSeek)),
      A(T::NEXT),
      A(T::SEEK, test::KeyStr("z", kMaxSequenceNumber, kValueTypeForSeek))};
  ASSERT_EQ(expected_actions, iter_->log);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
