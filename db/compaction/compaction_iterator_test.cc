//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction/compaction_iterator.h"

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "port/port.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"
#include "util/vector_iterator.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

// Expects no merging attempts.
class NoMergingMergeOp : public MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                   MergeOperationOutput* /*merge_out*/) const override {
    ADD_FAILURE();
    return false;
  }
  bool PartialMergeMulti(const Slice& /*key*/,
                         const std::deque<Slice>& /*operand_list*/,
                         std::string* /*new_value*/,
                         Logger* /*logger*/) const override {
    ADD_FAILURE();
    return false;
  }
  const char* Name() const override {
    return "CompactionIteratorTest NoMergingMergeOp";
  }
};

// Compaction filter that gets stuck when it sees a particular key,
// then gets unstuck when told to.
// Always returns Decision::kRemove.
class StallingFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /*level*/, const Slice& key, ValueType /*type*/,
                    const Slice& /*existing_value*/, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    int k = std::atoi(key.ToString().c_str());
    last_seen.store(k);
    while (k >= stall_at.load()) {
      std::this_thread::yield();
    }
    return Decision::kRemove;
  }

  const char* Name() const override {
    return "CompactionIteratorTest StallingFilter";
  }

  // Wait until the filter sees a key >= k and stalls at that key.
  // If `exact`, asserts that the seen key is equal to k.
  void WaitForStall(int k, bool exact = true) {
    stall_at.store(k);
    while (last_seen.load() < k) {
      std::this_thread::yield();
    }
    if (exact) {
      EXPECT_EQ(k, last_seen.load());
    }
  }

  // Filter will stall on key >= stall_at. Advance stall_at to unstall.
  mutable std::atomic<int> stall_at{0};
  // Last key the filter was called with.
  mutable std::atomic<int> last_seen{0};
};

// Compaction filter that filter out all keys.
class FilterAllKeysCompactionFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /*level*/, const Slice& /*key*/, ValueType /*type*/,
                    const Slice& /*existing_value*/, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    return Decision::kRemove;
  }

  const char* Name() const override { return "AllKeysCompactionFilter"; }
};

class LoggingForwardVectorIterator : public VectorIterator {
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
      : VectorIterator(keys, values) {
    current_ = keys_.size();
  }

  void SeekToFirst() override {
    log.emplace_back(Action::Type::SEEK_TO_FIRST);
    VectorIterator::SeekToFirst();
  }
  void SeekToLast() override { assert(false); }

  void Seek(const Slice& target) override {
    log.emplace_back(Action::Type::SEEK, target.ToString());
    VectorIterator::Seek(target);
  }

  void SeekForPrev(const Slice& /*target*/) override { assert(false); }

  void Next() override {
    assert(Valid());
    log.emplace_back(Action::Type::NEXT);
    VectorIterator::Next();
  }
  void Prev() override { assert(false); }

  Slice key() const override {
    assert(Valid());
    return VectorIterator::key();
  }
  Slice value() const override {
    assert(Valid());
    return VectorIterator::value();
  }

  std::vector<Action> log;
};

class FakeCompaction : public CompactionIterator::CompactionProxy {
 public:
  int level() const override { return 0; }

  bool KeyNotExistsBeyondOutputLevel(
      const Slice& /*user_key*/,
      std::vector<size_t>* /*level_ptrs*/) const override {
    return is_bottommost_level || key_not_exists_beyond_output_level;
  }

  bool bottommost_level() const override { return is_bottommost_level; }

  int number_levels() const override { return 1; }

  Slice GetLargestUserKey() const override {
    return "\xff\xff\xff\xff\xff\xff\xff\xff\xff";
  }

  bool allow_ingest_behind() const override { return is_allow_ingest_behind; }

  bool allow_mmap_reads() const override { return false; }

  bool enable_blob_garbage_collection() const override { return false; }

  double blob_garbage_collection_age_cutoff() const override { return 0.0; }

  uint64_t blob_compaction_readahead_size() const override { return 0; }

  const Version* input_version() const override { return nullptr; }

  bool DoesInputReferenceBlobFiles() const override { return false; }

  const Compaction* real_compaction() const override { return nullptr; }

  bool SupportsPerKeyPlacement() const override {
    return supports_per_key_placement;
  }

  bool WithinPenultimateLevelOutputRange(const Slice& key) const override {
    return (!key.starts_with("unsafe_pb"));
  }

  bool key_not_exists_beyond_output_level = false;

  bool is_bottommost_level = false;

  bool is_allow_ingest_behind = false;

  bool supports_per_key_placement = false;
};

// A simplified snapshot checker which assumes each snapshot has a global
// last visible sequence.
class TestSnapshotChecker : public SnapshotChecker {
 public:
  explicit TestSnapshotChecker(
      SequenceNumber last_committed_sequence,
      const std::unordered_map<SequenceNumber, SequenceNumber>& snapshots =
          {{}})
      : last_committed_sequence_(last_committed_sequence),
        snapshots_(snapshots) {}

  SnapshotCheckerResult CheckInSnapshot(
      SequenceNumber seq, SequenceNumber snapshot_seq) const override {
    if (snapshot_seq == kMaxSequenceNumber) {
      return seq <= last_committed_sequence_
                 ? SnapshotCheckerResult::kInSnapshot
                 : SnapshotCheckerResult::kNotInSnapshot;
    }
    assert(snapshots_.count(snapshot_seq) > 0);
    return seq <= snapshots_.at(snapshot_seq)
               ? SnapshotCheckerResult::kInSnapshot
               : SnapshotCheckerResult::kNotInSnapshot;
  }

 private:
  SequenceNumber last_committed_sequence_;
  // A map of valid snapshot to last visible sequence to the snapshot.
  std::unordered_map<SequenceNumber, SequenceNumber> snapshots_;
};

// Test param:
//   bool: whether to pass snapshot_checker to compaction iterator.
class CompactionIteratorTest : public testing::TestWithParam<bool> {
 public:
  CompactionIteratorTest()
      : cmp_(BytewiseComparator()), icmp_(cmp_), snapshots_({}) {}

  explicit CompactionIteratorTest(const Comparator* ucmp)
      : cmp_(ucmp), icmp_(cmp_), snapshots_({}) {}

  void InitIterators(
      const std::vector<std::string>& ks, const std::vector<std::string>& vs,
      const std::vector<std::string>& range_del_ks,
      const std::vector<std::string>& range_del_vs,
      SequenceNumber last_sequence,
      SequenceNumber last_committed_sequence = kMaxSequenceNumber,
      MergeOperator* merge_op = nullptr, CompactionFilter* filter = nullptr,
      bool bottommost_level = false,
      SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber,
      bool key_not_exists_beyond_output_level = false,
      const std::string* full_history_ts_low = nullptr) {
    std::unique_ptr<InternalIterator> unfragmented_range_del_iter(
        new VectorIterator(range_del_ks, range_del_vs, &icmp_));
    auto tombstone_list = std::make_shared<FragmentedRangeTombstoneList>(
        std::move(unfragmented_range_del_iter), icmp_);
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        new FragmentedRangeTombstoneIterator(tombstone_list, icmp_,
                                             kMaxSequenceNumber));
    range_del_agg_.reset(new CompactionRangeDelAggregator(&icmp_, snapshots_));
    range_del_agg_->AddTombstones(std::move(range_del_iter));

    std::unique_ptr<CompactionIterator::CompactionProxy> compaction;
    if (filter || bottommost_level || key_not_exists_beyond_output_level) {
      compaction_proxy_ = new FakeCompaction();
      compaction_proxy_->is_bottommost_level = bottommost_level;
      compaction_proxy_->is_allow_ingest_behind = AllowIngestBehind();
      compaction_proxy_->key_not_exists_beyond_output_level =
          key_not_exists_beyond_output_level;
      compaction_proxy_->supports_per_key_placement = SupportsPerKeyPlacement();
      compaction.reset(compaction_proxy_);
    }
    bool use_snapshot_checker = UseSnapshotChecker() || GetParam();
    if (use_snapshot_checker || last_committed_sequence < kMaxSequenceNumber) {
      snapshot_checker_.reset(
          new TestSnapshotChecker(last_committed_sequence, snapshot_map_));
    }
    merge_helper_.reset(
        new MergeHelper(Env::Default(), cmp_, merge_op, filter, nullptr, false,
                        0 /*latest_snapshot*/, snapshot_checker_.get(),
                        0 /*level*/, nullptr /*statistics*/, &shutting_down_));

    if (c_iter_) {
      // Since iter_ is still used in ~CompactionIterator(), we call
      // ~CompactionIterator() first.
      c_iter_.reset();
    }
    iter_.reset(new LoggingForwardVectorIterator(ks, vs));
    iter_->SeekToFirst();
    c_iter_.reset(new CompactionIterator(
        iter_.get(), cmp_, merge_helper_.get(), last_sequence, &snapshots_,
        earliest_write_conflict_snapshot, kMaxSequenceNumber,
        snapshot_checker_.get(), Env::Default(),
        false /* report_detailed_time */, false, range_del_agg_.get(),
        nullptr /* blob_file_builder */, true /*allow_data_in_errors*/,
        true /*enforce_single_del_contracts*/,
        /*manual_compaction_canceled=*/kManualCompactionCanceledFalse_,
        std::move(compaction), filter, &shutting_down_, /*info_log=*/nullptr,
        full_history_ts_low));
  }

  void AddSnapshot(SequenceNumber snapshot,
                   SequenceNumber last_visible_seq = kMaxSequenceNumber) {
    snapshots_.push_back(snapshot);
    snapshot_map_[snapshot] = last_visible_seq;
  }

  virtual bool UseSnapshotChecker() const { return false; }

  virtual bool AllowIngestBehind() const { return false; }

  virtual bool SupportsPerKeyPlacement() const { return false; }

  void RunTest(
      const std::vector<std::string>& input_keys,
      const std::vector<std::string>& input_values,
      const std::vector<std::string>& expected_keys,
      const std::vector<std::string>& expected_values,
      SequenceNumber last_committed_seq = kMaxSequenceNumber,
      MergeOperator* merge_operator = nullptr,
      CompactionFilter* compaction_filter = nullptr,
      bool bottommost_level = false,
      SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber,
      bool key_not_exists_beyond_output_level = false,
      const std::string* full_history_ts_low = nullptr) {
    InitIterators(input_keys, input_values, {}, {}, kMaxSequenceNumber,
                  last_committed_seq, merge_operator, compaction_filter,
                  bottommost_level, earliest_write_conflict_snapshot,
                  key_not_exists_beyond_output_level, full_history_ts_low);
    c_iter_->SeekToFirst();
    for (size_t i = 0; i < expected_keys.size(); i++) {
      std::string info = "i = " + std::to_string(i);
      ASSERT_TRUE(c_iter_->Valid()) << info;
      ASSERT_OK(c_iter_->status()) << info;
      ASSERT_EQ(expected_keys[i], c_iter_->key().ToString()) << info;
      ASSERT_EQ(expected_values[i], c_iter_->value().ToString()) << info;
      c_iter_->Next();
    }
    ASSERT_OK(c_iter_->status());
    ASSERT_FALSE(c_iter_->Valid());
  }

  void ClearSnapshots() {
    snapshots_.clear();
    snapshot_map_.clear();
  }

  const Comparator* cmp_;
  const InternalKeyComparator icmp_;
  std::vector<SequenceNumber> snapshots_;
  // A map of valid snapshot to last visible sequence to the snapshot.
  std::unordered_map<SequenceNumber, SequenceNumber> snapshot_map_;
  std::unique_ptr<MergeHelper> merge_helper_;
  std::unique_ptr<LoggingForwardVectorIterator> iter_;
  std::unique_ptr<CompactionIterator> c_iter_;
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg_;
  std::unique_ptr<SnapshotChecker> snapshot_checker_;
  std::atomic<bool> shutting_down_{false};
  const std::atomic<bool> kManualCompactionCanceledFalse_{false};
  FakeCompaction* compaction_proxy_;
};

// It is possible that the output of the compaction iterator is empty even if
// the input is not.
TEST_P(CompactionIteratorTest, EmptyResult) {
  InitIterators({test::KeyStr("a", 5, kTypeSingleDeletion),
                 test::KeyStr("a", 3, kTypeValue)},
                {"", "val"}, {}, {}, 5);
  c_iter_->SeekToFirst();
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
}

// If there is a corruption after a single deletion, the corrupted key should
// be preserved.
TEST_P(CompactionIteratorTest, CorruptionAfterSingleDeletion) {
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
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_P(CompactionIteratorTest, SimpleRangeDeletion) {
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
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_P(CompactionIteratorTest, RangeDeletionWithSnapshots) {
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
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_P(CompactionIteratorTest, CompactionFilterSkipUntil) {
  class Filter : public CompactionFilter {
    Decision FilterV2(int /*level*/, const Slice& key, ValueType t,
                      const Slice& existing_value, std::string* /*new_value*/,
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

  NoMergingMergeOp merge_op;
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
      {}, {}, kMaxSequenceNumber, kMaxSequenceNumber, &merge_op, &filter);

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
  ASSERT_OK(c_iter_->status());
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

TEST_P(CompactionIteratorTest, ShuttingDownInFilter) {
  NoMergingMergeOp merge_op;
  StallingFilter filter;
  InitIterators(
      {test::KeyStr("1", 1, kTypeValue), test::KeyStr("2", 2, kTypeValue),
       test::KeyStr("3", 3, kTypeValue), test::KeyStr("4", 4, kTypeValue)},
      {"v1", "v2", "v3", "v4"}, {}, {}, kMaxSequenceNumber, kMaxSequenceNumber,
      &merge_op, &filter);
  // Don't leave tombstones (kTypeDeletion) for filtered keys.
  compaction_proxy_->key_not_exists_beyond_output_level = true;

  std::atomic<bool> seek_done{false};
  ROCKSDB_NAMESPACE::port::Thread compaction_thread([&] {
    c_iter_->SeekToFirst();
    EXPECT_FALSE(c_iter_->Valid());
    EXPECT_TRUE(c_iter_->status().IsShutdownInProgress());
    seek_done.store(true);
  });

  // Let key 1 through.
  filter.WaitForStall(1);

  // Shutdown during compaction filter call for key 2.
  filter.WaitForStall(2);
  shutting_down_.store(true);
  EXPECT_FALSE(seek_done.load());

  // Unstall filter and wait for SeekToFirst() to return.
  filter.stall_at.store(3);
  compaction_thread.join();
  assert(seek_done.load());

  // Check that filter was never called again.
  EXPECT_EQ(2, filter.last_seen.load());
}

// Same as ShuttingDownInFilter, but shutdown happens during filter call for
// a merge operand, not for a value.
TEST_P(CompactionIteratorTest, ShuttingDownInMerge) {
  NoMergingMergeOp merge_op;
  StallingFilter filter;
  InitIterators(
      {test::KeyStr("1", 1, kTypeValue), test::KeyStr("2", 2, kTypeMerge),
       test::KeyStr("3", 3, kTypeMerge), test::KeyStr("4", 4, kTypeValue)},
      {"v1", "v2", "v3", "v4"}, {}, {}, kMaxSequenceNumber, kMaxSequenceNumber,
      &merge_op, &filter);
  compaction_proxy_->key_not_exists_beyond_output_level = true;

  std::atomic<bool> seek_done{false};
  ROCKSDB_NAMESPACE::port::Thread compaction_thread([&] {
    c_iter_->SeekToFirst();
    ASSERT_FALSE(c_iter_->Valid());
    ASSERT_TRUE(c_iter_->status().IsShutdownInProgress());
    seek_done.store(true);
  });

  // Let key 1 through.
  filter.WaitForStall(1);

  // Shutdown during compaction filter call for key 2.
  filter.WaitForStall(2);
  shutting_down_.store(true);
  EXPECT_FALSE(seek_done.load());

  // Unstall filter and wait for SeekToFirst() to return.
  filter.stall_at.store(3);
  compaction_thread.join();
  assert(seek_done.load());

  // Check that filter was never called again.
  EXPECT_EQ(2, filter.last_seen.load());
}

TEST_P(CompactionIteratorTest, SingleMergeOperand) {
  class Filter : public CompactionFilter {
    Decision FilterV2(int /*level*/, const Slice& key, ValueType t,
                      const Slice& existing_value, std::string* /*new_value*/,
                      std::string* /*skip_until*/) const override {
      std::string k = key.ToString();
      std::string v = existing_value.ToString();

      // See InitIterators() call below for the sequence of keys and their
      // filtering decisions. Here we closely assert that compaction filter is
      // called with the expected keys and only them, and with the right values.
      if (k == "a") {
        EXPECT_EQ(ValueType::kMergeOperand, t);
        EXPECT_EQ("av1", v);
        return Decision::kKeep;
      } else if (k == "b") {
        EXPECT_EQ(ValueType::kMergeOperand, t);
        return Decision::kKeep;
      } else if (k == "c") {
        return Decision::kKeep;
      }

      ADD_FAILURE();
      return Decision::kKeep;
    }

    const char* Name() const override {
      return "CompactionIteratorTest.SingleMergeOperand::Filter";
    }
  };

  class SingleMergeOp : public MergeOperator {
   public:
    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override {
      // See InitIterators() call below for why "c" is the only key for which
      // FullMergeV2 should be called.
      EXPECT_EQ("c", merge_in.key.ToString());

      std::string temp_value;
      if (merge_in.existing_value != nullptr) {
        temp_value = merge_in.existing_value->ToString();
      }

      for (auto& operand : merge_in.operand_list) {
        temp_value.append(operand.ToString());
      }
      merge_out->new_value = temp_value;

      return true;
    }

    bool PartialMergeMulti(const Slice& key,
                           const std::deque<Slice>& operand_list,
                           std::string* new_value,
                           Logger* /*logger*/) const override {
      std::string string_key = key.ToString();
      EXPECT_TRUE(string_key == "a" || string_key == "b");

      if (string_key == "a") {
        EXPECT_EQ(1, operand_list.size());
      } else if (string_key == "b") {
        EXPECT_EQ(2, operand_list.size());
      }

      std::string temp_value;
      for (auto& operand : operand_list) {
        temp_value.append(operand.ToString());
      }
      swap(temp_value, *new_value);

      return true;
    }

    const char* Name() const override {
      return "CompactionIteratorTest SingleMergeOp";
    }

    bool AllowSingleOperand() const override { return true; }
  };

  SingleMergeOp merge_op;
  Filter filter;
  InitIterators(
      // a should invoke PartialMergeMulti with a single merge operand.
      {test::KeyStr("a", 50, kTypeMerge),
       // b should invoke PartialMergeMulti with two operands.
       test::KeyStr("b", 70, kTypeMerge), test::KeyStr("b", 60, kTypeMerge),
       // c should invoke FullMerge due to kTypeValue at the beginning.
       test::KeyStr("c", 90, kTypeMerge), test::KeyStr("c", 80, kTypeValue)},
      {"av1", "bv2", "bv1", "cv2", "cv1"}, {}, {}, kMaxSequenceNumber,
      kMaxSequenceNumber, &merge_op, &filter);

  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 50, kTypeMerge), c_iter_->key().ToString());
  ASSERT_EQ("av1", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ("bv1bv2", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_OK(c_iter_->status());
  ASSERT_EQ("cv1cv2", c_iter_->value().ToString());
}

// In bottommost level, values earlier than earliest snapshot can be output
// with sequence = 0.
TEST_P(CompactionIteratorTest, ZeroOutSequenceAtBottomLevel) {
  AddSnapshot(1);
  RunTest({test::KeyStr("a", 1, kTypeValue), test::KeyStr("b", 2, kTypeValue)},
          {"v1", "v2"},
          {test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 2, kTypeValue)},
          {"v1", "v2"}, kMaxSequenceNumber /*last_committed_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

// In bottommost level, deletions earlier than earliest snapshot can be removed
// permanently.
TEST_P(CompactionIteratorTest, RemoveDeletionAtBottomLevel) {
  AddSnapshot(1);
  RunTest(
      {test::KeyStr("a", 1, kTypeDeletion), test::KeyStr("b", 3, kTypeDeletion),
       test::KeyStr("b", 1, kTypeValue)},
      {"", "", ""},
      {test::KeyStr("b", 3, kTypeDeletion), test::KeyStr("b", 0, kTypeValue)},
      {"", ""}, kMaxSequenceNumber /*last_committed_seq*/,
      nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
      true /*bottommost_level*/);
}

// In bottommost level, single deletions earlier than earliest snapshot can be
// removed permanently.
TEST_P(CompactionIteratorTest, RemoveSingleDeletionAtBottomLevel) {
  AddSnapshot(1);
  RunTest({test::KeyStr("a", 1, kTypeSingleDeletion),
           test::KeyStr("b", 2, kTypeSingleDeletion)},
          {"", ""}, {test::KeyStr("b", 2, kTypeSingleDeletion)}, {""},
          kMaxSequenceNumber /*last_committed_seq*/, nullptr /*merge_operator*/,
          nullptr /*compaction_filter*/, true /*bottommost_level*/);
}

TEST_P(CompactionIteratorTest, ConvertToPutAtBottom) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  RunTest({test::KeyStr("a", 4, kTypeMerge), test::KeyStr("a", 3, kTypeMerge),
           test::KeyStr("a", 2, kTypeMerge), test::KeyStr("b", 1, kTypeValue)},
          {"a4", "a3", "a2", "b1"},
          {test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 0, kTypeValue)},
          {"a2,a3,a4", "b1"}, kMaxSequenceNumber /*last_committed_seq*/,
          merge_op.get(), nullptr /*compaction_filter*/,
          true /*bottomost_level*/);
}

INSTANTIATE_TEST_CASE_P(CompactionIteratorTestInstance, CompactionIteratorTest,
                        testing::Values(true, false));

class PerKeyPlacementCompIteratorTest : public CompactionIteratorTest {
 public:
  bool SupportsPerKeyPlacement() const override { return true; }
};

TEST_P(PerKeyPlacementCompIteratorTest, SplitLastLevelData) {
  std::atomic_uint64_t latest_cold_seq = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        context->output_to_penultimate_level =
            context->seq_num > latest_cold_seq;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  latest_cold_seq = 5;

  InitIterators(
      {test::KeyStr("a", 7, kTypeValue), test::KeyStr("b", 6, kTypeValue),
       test::KeyStr("c", 5, kTypeValue)},
      {"vala", "valb", "valc"}, {}, {}, kMaxSequenceNumber, kMaxSequenceNumber,
      nullptr, nullptr, true);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());

  // the first 2 keys are hot, which should has
  // `output_to_penultimate_level()==true` and seq num not zeroed out
  ASSERT_EQ(test::KeyStr("a", 7, kTypeValue), c_iter_->key().ToString());
  ASSERT_TRUE(c_iter_->output_to_penultimate_level());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("b", 6, kTypeValue), c_iter_->key().ToString());
  ASSERT_TRUE(c_iter_->output_to_penultimate_level());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  // `a` is cold data, which should be output to bottommost
  ASSERT_EQ(test::KeyStr("c", 0, kTypeValue), c_iter_->key().ToString());
  ASSERT_FALSE(c_iter_->output_to_penultimate_level());
  c_iter_->Next();
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(PerKeyPlacementCompIteratorTest, SnapshotData) {
  AddSnapshot(5);

  InitIterators(
      {test::KeyStr("a", 7, kTypeValue), test::KeyStr("b", 6, kTypeDeletion),
       test::KeyStr("b", 5, kTypeValue)},
      {"vala", "", "valb"}, {}, {}, kMaxSequenceNumber, kMaxSequenceNumber,
      nullptr, nullptr, true);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());

  // The first key and the tombstone are within snapshot, which should output
  // to the penultimate level (and seq num cannot be zeroed out).
  ASSERT_EQ(test::KeyStr("a", 7, kTypeValue), c_iter_->key().ToString());
  ASSERT_TRUE(c_iter_->output_to_penultimate_level());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("b", 6, kTypeDeletion), c_iter_->key().ToString());
  ASSERT_TRUE(c_iter_->output_to_penultimate_level());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  // `a` is not protected by the snapshot, the sequence number is zero out and
  // should output bottommost
  ASSERT_EQ(test::KeyStr("b", 0, kTypeValue), c_iter_->key().ToString());
  ASSERT_FALSE(c_iter_->output_to_penultimate_level());
  c_iter_->Next();
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_P(PerKeyPlacementCompIteratorTest, ConflictWithSnapshot) {
  std::atomic_uint64_t latest_cold_seq = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        context->output_to_penultimate_level =
            context->seq_num > latest_cold_seq;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  latest_cold_seq = 6;

  AddSnapshot(5);

  InitIterators({test::KeyStr("a", 7, kTypeValue),
                 test::KeyStr("unsafe_pb", 6, kTypeValue),
                 test::KeyStr("c", 5, kTypeValue)},
                {"vala", "valb", "valc"}, {}, {}, kMaxSequenceNumber,
                kMaxSequenceNumber, nullptr, nullptr, true);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());

  ASSERT_EQ(test::KeyStr("a", 7, kTypeValue), c_iter_->key().ToString());
  ASSERT_TRUE(c_iter_->output_to_penultimate_level());
  // the 2nd key is unsafe to output_to_penultimate_level, but it's within
  // snapshot so for per_key_placement feature it has to be outputted to the
  // penultimate level. which is a corruption. We should never see
  // such case as the data with seq num (within snapshot) should always come
  // from higher compaction input level, which makes it safe to
  // output_to_penultimate_level.
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->status().IsCorruption());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

INSTANTIATE_TEST_CASE_P(PerKeyPlacementCompIteratorTest,
                        PerKeyPlacementCompIteratorTest,
                        testing::Values(true, false));

// Tests how CompactionIterator work together with SnapshotChecker.
class CompactionIteratorWithSnapshotCheckerTest
    : public CompactionIteratorTest {
 public:
  bool UseSnapshotChecker() const override { return true; }
};

// Uncommitted keys (keys with seq > last_committed_seq) should be output as-is
// while committed version of these keys should get compacted as usual.

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_Value) {
  RunTest(
      {test::KeyStr("foo", 3, kTypeValue), test::KeyStr("foo", 2, kTypeValue),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v3", "v2", "v1"},
      {test::KeyStr("foo", 3, kTypeValue), test::KeyStr("foo", 2, kTypeValue)},
      {"v3", "v2"}, 2 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_Deletion) {
  RunTest({test::KeyStr("foo", 2, kTypeDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("foo", 2, kTypeDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"}, 1 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_Merge) {
  auto merge_op = MergeOperators::CreateStringAppendOperator();
  RunTest(
      {test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 2, kTypeMerge),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v3", "v2", "v1"},
      {test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 2, kTypeValue)},
      {"v3", "v1,v2"}, 2 /*last_committed_seq*/, merge_op.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_SingleDelete) {
  RunTest({test::KeyStr("foo", 2, kTypeSingleDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("foo", 2, kTypeSingleDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"}, 1 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_BlobIndex) {
  RunTest({test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 2, kTypeBlobIndex),
           test::KeyStr("foo", 1, kTypeBlobIndex)},
          {"v3", "v2", "v1"},
          {test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 2, kTypeBlobIndex)},
          {"v3", "v2"}, 2 /*last_committed_seq*/);
}

// Test compaction iterator dedup keys visible to the same snapshot.

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_Value) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("foo", 4, kTypeValue), test::KeyStr("foo", 3, kTypeValue),
       test::KeyStr("foo", 2, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "v3", "v2", "v1"},
      {test::KeyStr("foo", 4, kTypeValue), test::KeyStr("foo", 3, kTypeValue),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "v3", "v1"}, 3 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_Deletion) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("foo", 4, kTypeValue),
       test::KeyStr("foo", 3, kTypeDeletion),
       test::KeyStr("foo", 2, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "", "v2", "v1"},
      {test::KeyStr("foo", 4, kTypeValue),
       test::KeyStr("foo", 3, kTypeDeletion),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "", "v1"}, 3 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_Merge) {
  AddSnapshot(2, 1);
  AddSnapshot(4, 3);
  auto merge_op = MergeOperators::CreateStringAppendOperator();
  RunTest(
      {test::KeyStr("foo", 5, kTypeMerge), test::KeyStr("foo", 4, kTypeMerge),
       test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 2, kTypeMerge),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v5", "v4", "v3", "v2", "v1"},
      {test::KeyStr("foo", 5, kTypeMerge), test::KeyStr("foo", 4, kTypeMerge),
       test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 1, kTypeValue)},
      {"v5", "v4", "v2,v3", "v1"}, 4 /*last_committed_seq*/, merge_op.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       DedupSameSnapshot_SingleDeletion) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("foo", 4, kTypeValue),
       test::KeyStr("foo", 3, kTypeSingleDeletion),
       test::KeyStr("foo", 2, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "", "v2", "v1"},
      {test::KeyStr("foo", 4, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "v1"}, 3 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_BlobIndex) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("foo", 4, kTypeBlobIndex),
           test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 2, kTypeBlobIndex),
           test::KeyStr("foo", 1, kTypeBlobIndex)},
          {"v4", "v3", "v2", "v1"},
          {test::KeyStr("foo", 4, kTypeBlobIndex),
           test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 1, kTypeBlobIndex)},
          {"v4", "v3", "v1"}, 3 /*last_committed_seq*/);
}

// At bottom level, sequence numbers can be zero out, and deletions can be
// removed, but only when they are visible to earliest snapshot.

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotZeroOutSequenceIfNotVisibleToEarliestSnapshot) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 1, kTypeValue), test::KeyStr("b", 2, kTypeValue),
           test::KeyStr("c", 3, kTypeValue)},
          {"v1", "v2", "v3"},
          {test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 2, kTypeValue),
           test::KeyStr("c", 3, kTypeValue)},
          {"v1", "v2", "v3"}, kMaxSequenceNumber /*last_committed_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotRemoveDeletionIfNotVisibleToEarliestSnapshot) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("a", 1, kTypeDeletion), test::KeyStr("b", 2, kTypeDeletion),
       test::KeyStr("c", 3, kTypeDeletion)},
      {"", "", ""}, {}, {"", ""}, kMaxSequenceNumber /*last_committed_seq*/,
      nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
      true /*bottommost_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotRemoveDeletionIfValuePresentToEarlierSnapshot) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 4, kTypeDeletion),
           test::KeyStr("a", 1, kTypeValue), test::KeyStr("b", 3, kTypeValue)},
          {"", "", ""},
          {test::KeyStr("a", 4, kTypeDeletion),
           test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 3, kTypeValue)},
          {"", "", ""}, kMaxSequenceNumber /*last_committed_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotRemoveSingleDeletionIfNotVisibleToEarliestSnapshot) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 1, kTypeSingleDeletion),
           test::KeyStr("b", 2, kTypeSingleDeletion),
           test::KeyStr("c", 3, kTypeSingleDeletion)},
          {"", "", ""},
          {test::KeyStr("b", 2, kTypeSingleDeletion),
           test::KeyStr("c", 3, kTypeSingleDeletion)},
          {"", ""}, kMaxSequenceNumber /*last_committed_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

// Single delete should not cancel out values that not visible to the
// same set of snapshots
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       SingleDeleteAcrossSnapshotBoundary) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", "v1"}, 2 /*last_committed_seq*/);
}

// Single delete should be kept in case it is not visible to the
// earliest write conflict snapshot. If a single delete is kept for this reason,
// corresponding value can be trimmed to save space.
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       KeepSingleDeletionForWriteConflictChecking) {
  AddSnapshot(2, 0);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", ""}, 2 /*last_committed_seq*/, nullptr /*merge_operator*/,
          nullptr /*compaction_filter*/, false /*bottommost_level*/,
          2 /*earliest_write_conflict_snapshot*/);
}

// Same as above but with a blob index. In addition to the value getting
// trimmed, the type of the KV is changed to kTypeValue.
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       KeepSingleDeletionForWriteConflictChecking_BlobIndex) {
  AddSnapshot(2, 0);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeBlobIndex)},
          {"", "fake_blob_index"},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", ""}, 2 /*last_committed_seq*/, nullptr /*merge_operator*/,
          nullptr /*compaction_filter*/, false /*bottommost_level*/,
          2 /*earliest_write_conflict_snapshot*/);
}

// Same as above but with a wide-column entity. In addition to the value getting
// trimmed, the type of the KV is changed to kTypeValue.
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       KeepSingleDeletionForWriteConflictChecking_WideColumnEntity) {
  AddSnapshot(2, 0);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeWideColumnEntity)},
          {"", "fake_entity"},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", ""}, 2 /* last_committed_seq */, nullptr /* merge_operator */,
          nullptr /* compaction_filter */, false /* bottommost_level */,
          2 /* earliest_write_conflict_snapshot */);
}

// Compaction filter should keep uncommitted key as-is, and
//   * Convert the latest value to deletion, and/or
//   * if latest value is a merge, apply filter to all subsequent merges.

TEST_F(CompactionIteratorWithSnapshotCheckerTest, CompactionFilter_Value) {
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest(
      {test::KeyStr("a", 2, kTypeValue), test::KeyStr("a", 1, kTypeValue),
       test::KeyStr("b", 3, kTypeValue), test::KeyStr("c", 1, kTypeValue)},
      {"v2", "v1", "v3", "v4"},
      {test::KeyStr("a", 2, kTypeValue), test::KeyStr("a", 1, kTypeDeletion),
       test::KeyStr("b", 3, kTypeValue), test::KeyStr("c", 1, kTypeDeletion)},
      {"v2", "", "v3", ""}, 1 /*last_committed_seq*/,
      nullptr /*merge_operator*/, compaction_filter.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, CompactionFilter_Deletion) {
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest(
      {test::KeyStr("a", 2, kTypeDeletion), test::KeyStr("a", 1, kTypeValue)},
      {"", "v1"},
      {test::KeyStr("a", 2, kTypeDeletion),
       test::KeyStr("a", 1, kTypeDeletion)},
      {"", ""}, 1 /*last_committed_seq*/, nullptr /*merge_operator*/,
      compaction_filter.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       CompactionFilter_PartialMerge) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest({test::KeyStr("a", 3, kTypeMerge), test::KeyStr("a", 2, kTypeMerge),
           test::KeyStr("a", 1, kTypeMerge)},
          {"v3", "v2", "v1"}, {test::KeyStr("a", 3, kTypeMerge)}, {"v3"},
          2 /*last_committed_seq*/, merge_op.get(), compaction_filter.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, CompactionFilter_FullMerge) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest(
      {test::KeyStr("a", 3, kTypeMerge), test::KeyStr("a", 2, kTypeMerge),
       test::KeyStr("a", 1, kTypeValue)},
      {"v3", "v2", "v1"},
      {test::KeyStr("a", 3, kTypeMerge), test::KeyStr("a", 1, kTypeDeletion)},
      {"v3", ""}, 2 /*last_committed_seq*/, merge_op.get(),
      compaction_filter.get());
}

// Tests how CompactionIterator work together with AllowIngestBehind.
class CompactionIteratorWithAllowIngestBehindTest
    : public CompactionIteratorTest {
 public:
  bool AllowIngestBehind() const override { return true; }
};

// When allow_ingest_behind is set, compaction iterator is not targeting
// the bottommost level since there is no guarantee there won't be further
// data ingested under the compaction output in future.
TEST_P(CompactionIteratorWithAllowIngestBehindTest, NoConvertToPutAtBottom) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  RunTest({test::KeyStr("a", 4, kTypeMerge), test::KeyStr("a", 3, kTypeMerge),
           test::KeyStr("a", 2, kTypeMerge), test::KeyStr("b", 1, kTypeValue)},
          {"a4", "a3", "a2", "b1"},
          {test::KeyStr("a", 4, kTypeMerge), test::KeyStr("b", 1, kTypeValue)},
          {"a2,a3,a4", "b1"}, kMaxSequenceNumber /*last_committed_seq*/,
          merge_op.get(), nullptr /*compaction_filter*/,
          true /*bottomost_level*/);
}

TEST_P(CompactionIteratorWithAllowIngestBehindTest,
       MergeToPutIfEncounteredPutAtBottom) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  RunTest({test::KeyStr("a", 4, kTypeMerge), test::KeyStr("a", 3, kTypeMerge),
           test::KeyStr("a", 2, kTypeValue), test::KeyStr("b", 1, kTypeValue)},
          {"a4", "a3", "a2", "b1"},
          {test::KeyStr("a", 4, kTypeValue), test::KeyStr("b", 1, kTypeValue)},
          {"a2,a3,a4", "b1"}, kMaxSequenceNumber /*last_committed_seq*/,
          merge_op.get(), nullptr /*compaction_filter*/,
          true /*bottomost_level*/);
}

INSTANTIATE_TEST_CASE_P(CompactionIteratorWithAllowIngestBehindTestInstance,
                        CompactionIteratorWithAllowIngestBehindTest,
                        testing::Values(true, false));

class CompactionIteratorTsGcTest : public CompactionIteratorTest {
 public:
  CompactionIteratorTsGcTest()
      : CompactionIteratorTest(test::BytewiseComparatorWithU64TsWrapper()) {}
};

TEST_P(CompactionIteratorTsGcTest, NoKeyEligibleForGC) {
  constexpr char user_key[][2] = {{'a', '\0'}, {'b', '\0'}};
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key[0], /*seq=*/4, kTypeValue),
      test::KeyStr(/*ts=*/102, user_key[0], /*seq=*/3,
                   kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/104, user_key[1], /*seq=*/5, kTypeValue)};
  const std::vector<std::string> input_values = {"a3", "", "b2"};
  std::string full_history_ts_low;
  // All keys' timestamps are newer than or equal to 102, thus none of them
  // will be eligible for GC.
  PutFixed64(&full_history_ts_low, 102);
  const std::vector<std::string>& expected_keys = input_keys;
  const std::vector<std::string>& expected_values = input_values;
  const std::vector<std::pair<bool, bool>> params = {
      {false, false}, {false, true}, {true, true}};
  for (const std::pair<bool, bool>& param : params) {
    const bool bottommost_level = param.first;
    const bool key_not_exists_beyond_output_level = param.second;
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            bottommost_level,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            key_not_exists_beyond_output_level, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, NoMergeEligibleForGc) {
  constexpr char user_key[] = "a";
  const std::vector<std::string> input_keys = {
      test::KeyStr(10002, user_key, 102, kTypeMerge),
      test::KeyStr(10001, user_key, 101, kTypeMerge),
      test::KeyStr(10000, user_key, 100, kTypeValue)};
  const std::vector<std::string> input_values = {"2", "1", "a0"};
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendTESTOperator();
  const std::vector<std::string>& expected_keys = input_keys;
  const std::vector<std::string>& expected_values = input_values;
  const std::vector<std::pair<bool, bool>> params = {
      {false, false}, {false, true}, {true, true}};
  for (const auto& param : params) {
    const bool bottommost_level = param.first;
    const bool key_not_exists_beyond_output_level = param.second;
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber, merge_op.get(),
            /*compaction_filter=*/nullptr, bottommost_level,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            key_not_exists_beyond_output_level,
            /*full_history_ts_low=*/nullptr);
  }
}

TEST_P(CompactionIteratorTsGcTest, AllKeysOlderThanThreshold) {
  constexpr char user_key[][2] = {{'a', '\0'}, {'b', '\0'}};
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key[0], /*seq=*/4,
                   kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/102, user_key[0], /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/101, user_key[0], /*seq=*/2, kTypeValue),
      test::KeyStr(/*ts=*/104, user_key[1], /*seq=*/5, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "a1", "b5"};
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, std::numeric_limits<uint64_t>::max());
  {
    // With a snapshot at seq 3, both the deletion marker and the key at 3 must
    // be preserved.
    AddSnapshot(3);
    const std::vector<std::string> expected_keys = {
        input_keys[0], input_keys[1], input_keys[3]};
    const std::vector<std::string> expected_values = {"", "a2", "b5"};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
    ClearSnapshots();
  }
  {
    // No snapshot, the deletion marker should be preserved because the user
    // key may appear beyond output level.
    const std::vector<std::string> expected_keys = {input_keys[0],
                                                    input_keys[3]};
    const std::vector<std::string> expected_values = {"", "b5"};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
  }
  {
    // No snapshot, the deletion marker can be dropped because the user key
    // does not appear in higher levels.
    const std::vector<std::string> expected_keys = {input_keys[3]};
    const std::vector<std::string> expected_values = {"b5"};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/true, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, SomeMergesOlderThanThreshold) {
  constexpr char user_key[][2] = {"a", "f"};
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/25000, user_key[0], /*seq=*/2500, kTypeMerge),
      test::KeyStr(/*ts=*/19000, user_key[0], /*seq=*/2300, kTypeMerge),
      test::KeyStr(/*ts=*/18000, user_key[0], /*seq=*/1800, kTypeMerge),
      test::KeyStr(/*ts=*/16000, user_key[0], /*seq=*/1600, kTypeValue),
      test::KeyStr(/*ts=*/19000, user_key[1], /*seq=*/2000, kTypeMerge),
      test::KeyStr(/*ts=*/17000, user_key[1], /*seq=*/1700, kTypeMerge),
      test::KeyStr(/*ts=*/15000, user_key[1], /*seq=*/1600,
                   kTypeDeletionWithTimestamp)};
  const std::vector<std::string> input_values = {"25", "19", "18", "16",
                                                 "19", "17", ""};
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendTESTOperator();
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, 20000);

  const std::vector<std::pair<bool, bool>> params = {
      {false, false}, {false, true}, {true, true}};

  {
    AddSnapshot(1600);
    AddSnapshot(1900);
    const std::vector<std::string> expected_keys = {
        test::KeyStr(/*ts=*/25000, user_key[0], /*seq=*/2500, kTypeMerge),
        test::KeyStr(/*ts=*/19000, user_key[0], /*seq=*/2300, kTypeMerge),
        test::KeyStr(/*ts=*/18000, user_key[0], /*seq=*/1800, kTypeMerge),
        test::KeyStr(/*ts=*/16000, user_key[0], /*seq=*/1600, kTypeValue),
        test::KeyStr(/*ts=*/19000, user_key[1], /*seq=*/2000, kTypeMerge),
        test::KeyStr(/*ts=*/17000, user_key[1], /*seq=*/1700, kTypeMerge),
        test::KeyStr(/*ts=*/15000, user_key[1], /*seq=*/1600,
                     kTypeDeletionWithTimestamp)};
    const std::vector<std::string> expected_values = {"25", "19", "18", "16",
                                                      "19", "17", ""};
    for (const auto& param : params) {
      const bool bottommost_level = param.first;
      const bool key_not_exists_beyond_output_level = param.second;
      auto expected_keys_copy = expected_keys;
      auto expected_values_copy = expected_values;
      if (bottommost_level || key_not_exists_beyond_output_level) {
        // the kTypeDeletionWithTimestamp will be dropped
        expected_keys_copy.pop_back();
        expected_values_copy.pop_back();
        if (bottommost_level) {
          // seq zero
          expected_keys_copy[3] =
              test::KeyStr(/*ts=*/0, user_key[0], /*seq=*/0, kTypeValue);
        }
      }
      RunTest(input_keys, input_values, expected_keys_copy,
              expected_values_copy,
              /*last_committed_seq=*/kMaxSequenceNumber, merge_op.get(),
              /*compaction_filter=*/nullptr, bottommost_level,
              /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
              key_not_exists_beyond_output_level, &full_history_ts_low);
    }
    ClearSnapshots();
  }

  // No snapshots
  {
    const std::vector<std::string> expected_keys = {
        test::KeyStr(/*ts=*/25000, user_key[0], /*seq=*/2500, kTypeValue),
        test::KeyStr(/*ts=*/19000, user_key[1], /*seq=*/2000, kTypeValue)};
    const std::vector<std::string> expected_values = {"16,18,19,25", "17,19"};
    for (const auto& param : params) {
      const bool bottommost_level = param.first;
      const bool key_not_exists_beyond_output_level = param.second;
      auto expected_keys_copy = expected_keys;
      auto expected_values_copy = expected_values;
      if (bottommost_level) {
        expected_keys_copy[1] =
            test::KeyStr(/*ts=*/0, user_key[1], /*seq=*/0, kTypeValue);
      }
      RunTest(input_keys, input_values, expected_keys_copy,
              expected_values_copy,
              /*last_committed_seq=*/kMaxSequenceNumber, merge_op.get(),
              /*compaction_filter=*/nullptr, bottommost_level,
              /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
              key_not_exists_beyond_output_level, &full_history_ts_low);
    }
  }
}

TEST_P(CompactionIteratorTsGcTest, NewHidesOldSameSnapshot) {
  constexpr char user_key[] = "a";
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key, /*seq=*/4, kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/102, user_key, /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/101, user_key, /*seq=*/2, kTypeValue),
      test::KeyStr(/*ts=*/100, user_key, /*seq=*/1, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "a1", "a0"};
  {
    std::string full_history_ts_low;
    // Keys whose timestamps larger than or equal to 102 will be preserved.
    PutFixed64(&full_history_ts_low, 102);
    const std::vector<std::string> expected_keys = {
        input_keys[0], input_keys[1], input_keys[2]};
    const std::vector<std::string> expected_values = {"", input_values[1],
                                                      input_values[2]};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, DropTombstones) {
  constexpr char user_key[] = "a";
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key, /*seq=*/4, kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/102, user_key, /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/101, user_key, /*seq=*/2, kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/100, user_key, /*seq=*/1, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "", "a0"};
  const std::vector<std::string> expected_keys = {input_keys[0], input_keys[1]};
  const std::vector<std::string> expected_values = {"", "a2"};

  // Take a snapshot at seq 2.
  AddSnapshot(2);

  {
    // Non-bottommost level, but key does not exist beyond output level.
    std::string full_history_ts_low;
    PutFixed64(&full_history_ts_low, 102);
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_sequence=*/kMaxSequenceNumber,
            /*merge_op=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/true, &full_history_ts_low);
  }
  {
    // Bottommost level
    std::string full_history_ts_low;
    PutFixed64(&full_history_ts_low, 102);
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/true,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, RewriteTs) {
  constexpr char user_key[] = "a";
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key, /*seq=*/4, kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/102, user_key, /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/101, user_key, /*seq=*/2, kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/100, user_key, /*seq=*/1, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "", "a0"};
  const std::vector<std::string> expected_keys = {
      input_keys[0], input_keys[1], input_keys[2],
      test::KeyStr(/*ts=*/0, user_key, /*seq=*/0, kTypeValue)};
  const std::vector<std::string> expected_values = {"", "a2", "", "a0"};

  AddSnapshot(1);
  AddSnapshot(2);

  {
    // Bottommost level and need to rewrite both ts and seq.
    std::string full_history_ts_low;
    PutFixed64(&full_history_ts_low, 102);
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/true,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/true, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, SingleDeleteNoKeyEligibleForGC) {
  constexpr char user_key[][2] = {{'a', '\0'}, {'b', '\0'}};
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/104, user_key[0], /*seq=*/4, kTypeSingleDeletion),
      test::KeyStr(/*ts=*/103, user_key[0], /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/102, user_key[1], /*seq=*/2, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a3", "b2"};
  std::string full_history_ts_low;
  // All keys' timestamps are newer than or equal to 102, thus none of them
  // will be eligible for GC.
  PutFixed64(&full_history_ts_low, 102);
  const std::vector<std::string>& expected_keys = input_keys;
  const std::vector<std::string>& expected_values = input_values;
  const std::vector<std::pair<bool, bool>> params = {
      {false, false}, {false, true}, {true, true}};
  for (const std::pair<bool, bool>& param : params) {
    const bool bottommost_level = param.first;
    const bool key_not_exists_beyond_output_level = param.second;
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            bottommost_level,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            key_not_exists_beyond_output_level, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, SingleDeleteDropTombstones) {
  constexpr char user_key[] = "a";
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key, /*seq=*/4, kTypeSingleDeletion),
      test::KeyStr(/*ts=*/102, user_key, /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/101, user_key, /*seq=*/2, kTypeSingleDeletion),
      test::KeyStr(/*ts=*/100, user_key, /*seq=*/1, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "", "a0"};
  const std::vector<std::string> expected_keys = {input_keys[0], input_keys[1]};
  const std::vector<std::string> expected_values = {"", "a2"};

  // Take a snapshot at seq 2.
  AddSnapshot(2);
  {
    const std::vector<std::pair<bool, bool>> params = {
        {false, false}, {false, true}, {true, true}};
    for (const std::pair<bool, bool>& param : params) {
      const bool bottommost_level = param.first;
      const bool key_not_exists_beyond_output_level = param.second;
      std::string full_history_ts_low;
      PutFixed64(&full_history_ts_low, 102);
      RunTest(input_keys, input_values, expected_keys, expected_values,
              /*last_committed_seq=*/kMaxSequenceNumber,
              /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
              bottommost_level,
              /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
              key_not_exists_beyond_output_level, &full_history_ts_low);
    }
  }
}

TEST_P(CompactionIteratorTsGcTest, SingleDeleteAllKeysOlderThanThreshold) {
  constexpr char user_key[][2] = {{'a', '\0'}, {'b', '\0'}};
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key[0], /*seq=*/4, kTypeSingleDeletion),
      test::KeyStr(/*ts=*/102, user_key[0], /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/104, user_key[1], /*seq=*/5, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "b5"};
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, std::numeric_limits<uint64_t>::max());
  {
    // With a snapshot at seq 3, both the deletion marker and the key at 3 must
    // be preserved.
    AddSnapshot(3);
    const std::vector<std::string> expected_keys = {
        input_keys[0], input_keys[1], input_keys[2]};
    const std::vector<std::string> expected_values = {"", "a2", "b5"};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
    ClearSnapshots();
  }
  {
    // No snapshot.
    const std::vector<std::string> expected_keys = {input_keys[2]};
    const std::vector<std::string> expected_values = {"b5"};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
  }
}

INSTANTIATE_TEST_CASE_P(CompactionIteratorTsGcTestInstance,
                        CompactionIteratorTsGcTest,
                        testing::Values(true, false));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
