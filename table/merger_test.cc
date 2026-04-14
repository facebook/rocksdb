//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <string>
#include <utility>
#include <vector>

#include "db/range_tombstone_fragmenter.h"
#include "memory/arena.h"
#include "table/merging_iterator.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Test-only input for one merge level. This regression only needs range
// tombstones, but MergeIteratorBuilder still expects one child per level.
struct LevelData {
  std::vector<RangeTombstone> tombstones;
};

std::unique_ptr<InternalIterator> MakeEmptyPointIter(
    const InternalKeyComparator& icmp) {
  return std::unique_ptr<InternalIterator>(new VectorIterator({}, {}, &icmp));
}

std::unique_ptr<InternalIterator> MakeRangeDelBlockIter(
    const std::vector<RangeTombstone>& tombstones) {
  static const InternalKeyComparator bytewise_icmp(BytewiseComparator());
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (const auto& tombstone : tombstones) {
    auto key_and_value = tombstone.Serialize();
    keys.push_back(key_and_value.first.Encode().ToString());
    values.push_back(key_and_value.second.ToString());
  }
  return std::unique_ptr<InternalIterator>(
      new VectorIterator(std::move(keys), std::move(values), &bytewise_icmp));
}

std::unique_ptr<TruncatedRangeDelIterator> MakeRangeDelIter(
    const InternalKeyComparator& icmp,
    const std::vector<RangeTombstone>& tombstones) {
  if (tombstones.empty()) {
    return nullptr;
  }
  // MergeIteratorBuilder consumes the same fragmented/truncated tombstone
  // iterator stack used by table readers, so build that shape explicitly.
  auto range_del_block_iter = MakeRangeDelBlockIter(tombstones);
  auto fragment_list = std::make_shared<FragmentedRangeTombstoneList>(
      std::move(range_del_block_iter), icmp);
  auto fragment_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
      fragment_list, icmp, kMaxSequenceNumber);
  return std::make_unique<TruncatedRangeDelIterator>(std::move(fragment_iter),
                                                     &icmp, nullptr, nullptr);
}

std::unique_ptr<InternalIterator> BuildMergeIter(
    const InternalKeyComparator& icmp, const std::vector<LevelData>& levels) {
  auto arena = std::make_unique<Arena>();
  auto* arena_ptr = arena.get();
  MergeIteratorBuilder builder(&icmp, arena_ptr, false /* prefix_seek_mode */);
  for (const auto& level : levels) {
    auto point_iter = MakeEmptyPointIter(icmp);
    auto tombstone_iter = MakeRangeDelIter(icmp, level.tombstones);
    builder.AddPointAndTombstoneIterator(point_iter.release(),
                                         std::move(tombstone_iter));
  }
  InternalIterator* iter = builder.Finish();
  // MergeIteratorBuilder placement-news the iterator into `arena`, so keep the
  // arena alive for the lifetime of the returned iterator in this test.
  struct ArenaOwnedIter : public InternalIterator {
    ArenaOwnedIter(std::unique_ptr<Arena>&& a, InternalIterator* i)
        : arena(std::move(a)), iter(i) {}
    ~ArenaOwnedIter() override { iter->~InternalIterator(); }
    bool Valid() const override { return iter->Valid(); }
    void SeekToFirst() override { iter->SeekToFirst(); }
    void SeekToLast() override { iter->SeekToLast(); }
    void Seek(const Slice& target) override { iter->Seek(target); }
    void SeekForPrev(const Slice& target) override {
      iter->SeekForPrev(target);
    }
    void Next() override { iter->Next(); }
    void Prev() override { iter->Prev(); }
    Slice key() const override { return iter->key(); }
    Slice value() const override { return iter->value(); }
    Status status() const override { return iter->status(); }
    bool IsKeyPinned() const override { return iter->IsKeyPinned(); }
    bool IsValuePinned() const override { return iter->IsValuePinned(); }
    void SetPinnedItersMgr(PinnedIteratorsManager* m) override {
      iter->SetPinnedItersMgr(m);
    }
    std::unique_ptr<Arena> arena;
    InternalIterator* iter;
  };
  return std::unique_ptr<InternalIterator>(
      new ArenaOwnedIter(std::move(arena), iter));
}

std::string SeekForPrevTarget(const std::string& user_key) {
  return InternalKey(user_key, kMaxSequenceNumber, kValueTypeForSeekForPrev)
      .Encode()
      .ToString();
}

std::string DecodeHexString(const std::string& hex) {
  std::string decoded;
  assert(Slice(hex).DecodeHex(&decoded));
  return decoded;
}

}  // namespace

class MergerTest : public testing::Test {
 public:
  MergerTest()
      : icomp_(BytewiseComparator()),
        rnd_(3),
        merging_iterator_(nullptr),
        single_iterator_(nullptr) {}
  ~MergerTest() override = default;
  std::vector<std::string> GenerateStrings(size_t len, int string_len) {
    std::vector<std::string> ret;

    for (size_t i = 0; i < len; ++i) {
      InternalKey ik(rnd_.HumanReadableString(string_len), 0,
                     ValueType::kTypeValue);
      ret.push_back(ik.Encode().ToString(false));
    }
    return ret;
  }

  void AssertEquivalence() {
    auto a = merging_iterator_.get();
    auto b = single_iterator_.get();
    if (!a->Valid()) {
      ASSERT_TRUE(!b->Valid());
    } else {
      ASSERT_TRUE(b->Valid());
      ASSERT_EQ(b->key().ToString(), a->key().ToString());
      ASSERT_EQ(b->value().ToString(), a->value().ToString());
    }
  }

  void SeekToRandom() {
    InternalKey ik(rnd_.HumanReadableString(5), 0, ValueType::kTypeValue);
    Seek(ik.Encode().ToString(false));
  }

  void Seek(std::string target) {
    merging_iterator_->Seek(target);
    single_iterator_->Seek(target);
  }

  void SeekToFirst() {
    merging_iterator_->SeekToFirst();
    single_iterator_->SeekToFirst();
  }

  void SeekToLast() {
    merging_iterator_->SeekToLast();
    single_iterator_->SeekToLast();
  }

  void Next(int times) {
    for (int i = 0; i < times && merging_iterator_->Valid(); ++i) {
      AssertEquivalence();
      merging_iterator_->Next();
      single_iterator_->Next();
    }
    AssertEquivalence();
  }

  void Prev(int times) {
    for (int i = 0; i < times && merging_iterator_->Valid(); ++i) {
      AssertEquivalence();
      merging_iterator_->Prev();
      single_iterator_->Prev();
    }
    AssertEquivalence();
  }

  void NextAndPrev(int times) {
    for (int i = 0; i < times && merging_iterator_->Valid(); ++i) {
      AssertEquivalence();
      if (rnd_.OneIn(2)) {
        merging_iterator_->Prev();
        single_iterator_->Prev();
      } else {
        merging_iterator_->Next();
        single_iterator_->Next();
      }
    }
    AssertEquivalence();
  }

  void Generate(size_t num_iterators, size_t strings_per_iterator,
                int letters_per_string) {
    std::vector<InternalIterator*> small_iterators;
    for (size_t i = 0; i < num_iterators; ++i) {
      auto strings = GenerateStrings(strings_per_iterator, letters_per_string);
      small_iterators.push_back(new VectorIterator(strings, strings, &icomp_));
      all_keys_.insert(all_keys_.end(), strings.begin(), strings.end());
    }

    merging_iterator_.reset(
        NewMergingIterator(&icomp_, small_iterators.data(),
                           static_cast<int>(small_iterators.size())));
    single_iterator_.reset(new VectorIterator(all_keys_, all_keys_, &icomp_));
  }

  InternalKeyComparator icomp_;
  Random rnd_;
  std::unique_ptr<InternalIterator> merging_iterator_;
  std::unique_ptr<InternalIterator> single_iterator_;
  std::vector<std::string> all_keys_;
};

TEST_F(MergerTest, SeekToRandomNextTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToRandom();
    AssertEquivalence();
    Next(50000);
  }
}

TEST_F(MergerTest, SeekToRandomNextSmallStringsTest) {
  Generate(1000, 50, 2);
  for (int i = 0; i < 10; ++i) {
    SeekToRandom();
    AssertEquivalence();
    Next(50000);
  }
}

TEST_F(MergerTest, SeekToRandomPrevTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToRandom();
    AssertEquivalence();
    Prev(50000);
  }
}

TEST_F(MergerTest, SeekToRandomRandomTest) {
  Generate(200, 50, 50);
  for (int i = 0; i < 3; ++i) {
    SeekToRandom();
    AssertEquivalence();
    NextAndPrev(5000);
  }
}

TEST_F(MergerTest, SeekToFirstTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToFirst();
    AssertEquivalence();
    Next(50000);
  }
}

TEST_F(MergerTest, SeekToLastTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToLast();
    AssertEquivalence();
    Prev(50000);
  }
}

TEST_F(MergerTest, SeekForPrevUsesMovedTargetForRangeTombstoneChoice) {
  // Mirror the proven stale-target pattern from the crash artifact:
  // base target ...010C is first pulled left to ...0103 by a newer tombstone.
  // The older tombstone [...0102, ...010C) must then be classified against the
  // moved target (...0103), not the stale original base target (...010C).
  const std::string older_start =
      DecodeHexString("00000000000001A3000000000000012B0000000000000102");
  const std::string reseek_start =
      DecodeHexString("00000000000001A3000000000000012B0000000000000103");
  const std::string base_target =
      DecodeHexString("00000000000001A3000000000000012B000000000000010C");
  const std::string newer_end =
      DecodeHexString("00000000000001A3000000000000012B000000000000010D");

  std::vector<LevelData> levels(2);
  levels[0].tombstones.emplace_back(reseek_start, newer_end, 200);
  levels[1].tombstones.emplace_back(older_start, base_target, 100);

  bool saw_older_level_choice = false;
  bool older_level_choose_end = true;
  SyncPoint::GetInstance()->SetCallBack(
      "MergingIterator::SeekForPrevImpl:RangeTombstoneChoice", [&](void* arg) {
        const auto* info = static_cast<const std::pair<size_t, bool>*>(arg);
        if (info->first == 1) {
          saw_older_level_choice = true;
          older_level_choose_end = info->second;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto merge_iter = BuildMergeIter(icomp_, levels);
  merge_iter->SeekForPrev(SeekForPrevTarget(base_target));

  ASSERT_FALSE(merge_iter->Valid());
  ASSERT_OK(merge_iter->status());
  ASSERT_TRUE(saw_older_level_choice);
  ASSERT_FALSE(older_level_choose_end);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
