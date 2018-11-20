//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_del_aggregator_v2.h"

#include <memory>
#include <string>
#include <vector>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "util/testutil.h"

namespace rocksdb {

class RangeDelAggregatorV2Test : public testing::Test {};

namespace {

static auto bytewise_icmp = InternalKeyComparator(BytewiseComparator());

std::unique_ptr<InternalIterator> MakeRangeDelIter(
    const std::vector<RangeTombstone>& range_dels) {
  std::vector<std::string> keys, values;
  for (const auto& range_del : range_dels) {
    auto key_and_value = range_del.Serialize();
    keys.push_back(key_and_value.first.Encode().ToString());
    values.push_back(key_and_value.second.ToString());
  }
  return std::unique_ptr<test::VectorIterator>(
      new test::VectorIterator(keys, values));
}

std::vector<std::unique_ptr<FragmentedRangeTombstoneList>>
MakeFragmentedTombstoneLists(
    const std::vector<std::vector<RangeTombstone>>& range_dels_list) {
  std::vector<std::unique_ptr<FragmentedRangeTombstoneList>> fragment_lists;
  for (const auto& range_dels : range_dels_list) {
    auto range_del_iter = MakeRangeDelIter(range_dels);
    fragment_lists.emplace_back(new FragmentedRangeTombstoneList(
        std::move(range_del_iter), bytewise_icmp, false /* one_time_use */));
  }
  return fragment_lists;
}

struct TruncatedIterScanTestCase {
  ParsedInternalKey start;
  ParsedInternalKey end;
  SequenceNumber seq;
};

struct TruncatedIterSeekTestCase {
  Slice target;
  ParsedInternalKey start;
  ParsedInternalKey end;
  SequenceNumber seq;
  bool invalid;
};

struct ShouldDeleteTestCase {
  ParsedInternalKey lookup_key;
  bool result;
};

struct IsRangeOverlappedTestCase {
  Slice start;
  Slice end;
  bool result;
};

ParsedInternalKey UncutEndpoint(const Slice& s) {
  return ParsedInternalKey(s, kMaxSequenceNumber, kTypeRangeDeletion);
}

ParsedInternalKey InternalValue(const Slice& key, SequenceNumber seq) {
  return ParsedInternalKey(key, seq, kTypeValue);
}

void VerifyIterator(
    TruncatedRangeDelIterator* iter, const InternalKeyComparator& icmp,
    const std::vector<TruncatedIterScanTestCase>& expected_range_dels) {
  // Test forward iteration.
  iter->SeekToFirst();
  for (size_t i = 0; i < expected_range_dels.size(); i++, iter->Next()) {
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(0, icmp.Compare(iter->start_key(), expected_range_dels[i].start));
    EXPECT_EQ(0, icmp.Compare(iter->end_key(), expected_range_dels[i].end));
    EXPECT_EQ(expected_range_dels[i].seq, iter->seq());
  }
  EXPECT_FALSE(iter->Valid());

  // Test reverse iteration.
  iter->SeekToLast();
  std::vector<TruncatedIterScanTestCase> reverse_expected_range_dels(
      expected_range_dels.rbegin(), expected_range_dels.rend());
  for (size_t i = 0; i < reverse_expected_range_dels.size();
       i++, iter->Prev()) {
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(0, icmp.Compare(iter->start_key(),
                              reverse_expected_range_dels[i].start));
    EXPECT_EQ(
        0, icmp.Compare(iter->end_key(), reverse_expected_range_dels[i].end));
    EXPECT_EQ(reverse_expected_range_dels[i].seq, iter->seq());
  }
  EXPECT_FALSE(iter->Valid());
}

void VerifySeek(TruncatedRangeDelIterator* iter,
                const InternalKeyComparator& icmp,
                const std::vector<TruncatedIterSeekTestCase>& test_cases) {
  for (const auto& test_case : test_cases) {
    iter->Seek(test_case.target);
    if (test_case.invalid) {
      ASSERT_FALSE(iter->Valid());
    } else {
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(0, icmp.Compare(iter->start_key(), test_case.start));
      EXPECT_EQ(0, icmp.Compare(iter->end_key(), test_case.end));
      EXPECT_EQ(test_case.seq, iter->seq());
    }
  }
}

void VerifySeekForPrev(
    TruncatedRangeDelIterator* iter, const InternalKeyComparator& icmp,
    const std::vector<TruncatedIterSeekTestCase>& test_cases) {
  for (const auto& test_case : test_cases) {
    iter->SeekForPrev(test_case.target);
    if (test_case.invalid) {
      ASSERT_FALSE(iter->Valid());
    } else {
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(0, icmp.Compare(iter->start_key(), test_case.start));
      EXPECT_EQ(0, icmp.Compare(iter->end_key(), test_case.end));
      EXPECT_EQ(test_case.seq, iter->seq());
    }
  }
}

void VerifyShouldDelete(RangeDelAggregatorV2* range_del_agg,
                        const std::vector<ShouldDeleteTestCase>& test_cases) {
  for (const auto& test_case : test_cases) {
    EXPECT_EQ(
        test_case.result,
        range_del_agg->ShouldDelete(
            test_case.lookup_key, RangeDelPositioningMode::kForwardTraversal));
  }
  for (auto it = test_cases.rbegin(); it != test_cases.rend(); ++it) {
    const auto& test_case = *it;
    EXPECT_EQ(
        test_case.result,
        range_del_agg->ShouldDelete(
            test_case.lookup_key, RangeDelPositioningMode::kBackwardTraversal));
  }
}

void VerifyIsRangeOverlapped(
    RangeDelAggregatorV2* range_del_agg,
    const std::vector<IsRangeOverlappedTestCase>& test_cases) {
  for (const auto& test_case : test_cases) {
    EXPECT_EQ(test_case.result,
              range_del_agg->IsRangeOverlapped(test_case.start, test_case.end));
  }
}

}  // namespace

TEST_F(RangeDelAggregatorV2Test, EmptyTruncatedIter) {
  auto range_del_iter = MakeRangeDelIter({});
  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, kMaxSequenceNumber,
                                           bytewise_icmp));

  TruncatedRangeDelIterator iter(std::move(input_iter), &bytewise_icmp, nullptr,
                                 nullptr);

  iter.SeekToFirst();
  ASSERT_FALSE(iter.Valid());

  iter.SeekToLast();
  ASSERT_FALSE(iter.Valid());
}

TEST_F(RangeDelAggregatorV2Test, UntruncatedIter) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "e", 10}, {"e", "g", 8}, {"j", "n", 4}});
  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, false /* one_time_use */);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, kMaxSequenceNumber,
                                           bytewise_icmp));

  TruncatedRangeDelIterator iter(std::move(input_iter), &bytewise_icmp, nullptr,
                                 nullptr);

  VerifyIterator(&iter, bytewise_icmp,
                 {{UncutEndpoint("a"), UncutEndpoint("e"), 10},
                  {UncutEndpoint("e"), UncutEndpoint("g"), 8},
                  {UncutEndpoint("j"), UncutEndpoint("n"), 4}});

  VerifySeek(
      &iter, bytewise_icmp,
      {{"d", UncutEndpoint("a"), UncutEndpoint("e"), 10},
       {"e", UncutEndpoint("e"), UncutEndpoint("g"), 8},
       {"ia", UncutEndpoint("j"), UncutEndpoint("n"), 4},
       {"n", UncutEndpoint(""), UncutEndpoint(""), 0, true /* invalid */},
       {"", UncutEndpoint("a"), UncutEndpoint("e"), 10}});

  VerifySeekForPrev(
      &iter, bytewise_icmp,
      {{"d", UncutEndpoint("a"), UncutEndpoint("e"), 10},
       {"e", UncutEndpoint("e"), UncutEndpoint("g"), 8},
       {"ia", UncutEndpoint("e"), UncutEndpoint("g"), 8},
       {"n", UncutEndpoint("j"), UncutEndpoint("n"), 4},
       {"", UncutEndpoint(""), UncutEndpoint(""), 0, true /* invalid */}});
}

TEST_F(RangeDelAggregatorV2Test, UntruncatedIterWithSnapshot) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "e", 10}, {"e", "g", 8}, {"j", "n", 4}});
  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, false /* one_time_use */);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, 9 /* snapshot */,
                                           bytewise_icmp));

  TruncatedRangeDelIterator iter(std::move(input_iter), &bytewise_icmp, nullptr,
                                 nullptr);

  VerifyIterator(&iter, bytewise_icmp,
                 {{UncutEndpoint("e"), UncutEndpoint("g"), 8},
                  {UncutEndpoint("j"), UncutEndpoint("n"), 4}});

  VerifySeek(
      &iter, bytewise_icmp,
      {{"d", UncutEndpoint("e"), UncutEndpoint("g"), 8},
       {"e", UncutEndpoint("e"), UncutEndpoint("g"), 8},
       {"ia", UncutEndpoint("j"), UncutEndpoint("n"), 4},
       {"n", UncutEndpoint(""), UncutEndpoint(""), 0, true /* invalid */},
       {"", UncutEndpoint("e"), UncutEndpoint("g"), 8}});

  VerifySeekForPrev(
      &iter, bytewise_icmp,
      {{"d", UncutEndpoint(""), UncutEndpoint(""), 0, true /* invalid */},
       {"e", UncutEndpoint("e"), UncutEndpoint("g"), 8},
       {"ia", UncutEndpoint("e"), UncutEndpoint("g"), 8},
       {"n", UncutEndpoint("j"), UncutEndpoint("n"), 4},
       {"", UncutEndpoint(""), UncutEndpoint(""), 0, true /* invalid */}});
}

TEST_F(RangeDelAggregatorV2Test, TruncatedIter) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "e", 10}, {"e", "g", 8}, {"j", "n", 4}});
  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, false /* one_time_use */);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, kMaxSequenceNumber,
                                           bytewise_icmp));

  InternalKey smallest("d", 7, kTypeValue);
  InternalKey largest("m", 9, kTypeValue);
  TruncatedRangeDelIterator iter(std::move(input_iter), &bytewise_icmp,
                                 &smallest, &largest);

  VerifyIterator(&iter, bytewise_icmp,
                 {{InternalValue("d", 7), UncutEndpoint("e"), 10},
                  {UncutEndpoint("e"), UncutEndpoint("g"), 8},
                  {UncutEndpoint("j"), InternalValue("m", 8), 4}});

  VerifySeek(
      &iter, bytewise_icmp,
      {{"d", InternalValue("d", 7), UncutEndpoint("e"), 10},
       {"e", UncutEndpoint("e"), UncutEndpoint("g"), 8},
       {"ia", UncutEndpoint("j"), InternalValue("m", 8), 4},
       {"n", UncutEndpoint(""), UncutEndpoint(""), 0, true /* invalid */},
       {"", InternalValue("d", 7), UncutEndpoint("e"), 10}});

  VerifySeekForPrev(
      &iter, bytewise_icmp,
      {{"d", InternalValue("d", 7), UncutEndpoint("e"), 10},
       {"e", UncutEndpoint("e"), UncutEndpoint("g"), 8},
       {"ia", UncutEndpoint("e"), UncutEndpoint("g"), 8},
       {"n", UncutEndpoint("j"), InternalValue("m", 8), 4},
       {"", UncutEndpoint(""), UncutEndpoint(""), 0, true /* invalid */}});
}

TEST_F(RangeDelAggregatorV2Test, SingleIterInAggregator) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10}, {"c", "g", 8}});
  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, false /* one_time_use */);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, kMaxSequenceNumber,
                                           bytewise_icmp));

  RangeDelAggregatorV2 range_del_agg(&bytewise_icmp, kMaxSequenceNumber);
  range_del_agg.AddTombstones(std::move(input_iter));

  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 19), false},
                                      {InternalValue("b", 9), true},
                                      {InternalValue("d", 9), true},
                                      {InternalValue("e", 7), true},
                                      {InternalValue("g", 7), false}});

  VerifyIsRangeOverlapped(&range_del_agg, {{"", "_", false},
                                           {"_", "a", true},
                                           {"a", "c", true},
                                           {"d", "f", true},
                                           {"g", "l", false}});
}

TEST_F(RangeDelAggregatorV2Test, MultipleItersInAggregator) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "e", 10}, {"c", "g", 8}},
       {{"a", "b", 20}, {"h", "i", 25}, {"ii", "j", 15}}});

  RangeDelAggregatorV2 range_del_agg(&bytewise_icmp, kMaxSequenceNumber);
  for (const auto& fragment_list : fragment_lists) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(
            fragment_list.get(), kMaxSequenceNumber, bytewise_icmp));
    range_del_agg.AddTombstones(std::move(input_iter));
  }

  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 19), true},
                                      {InternalValue("b", 19), false},
                                      {InternalValue("b", 9), true},
                                      {InternalValue("d", 9), true},
                                      {InternalValue("e", 7), true},
                                      {InternalValue("g", 7), false},
                                      {InternalValue("h", 24), true},
                                      {InternalValue("i", 24), false},
                                      {InternalValue("ii", 14), true},
                                      {InternalValue("j", 14), false}});

  VerifyIsRangeOverlapped(&range_del_agg, {{"", "_", false},
                                           {"_", "a", true},
                                           {"a", "c", true},
                                           {"d", "f", true},
                                           {"g", "l", true},
                                           {"x", "y", false}});
}

TEST_F(RangeDelAggregatorV2Test, MultipleItersInAggregatorWithUpperBound) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "e", 10}, {"c", "g", 8}},
       {{"a", "b", 20}, {"h", "i", 25}, {"ii", "j", 15}}});

  RangeDelAggregatorV2 range_del_agg(&bytewise_icmp, 19);
  for (const auto& fragment_list : fragment_lists) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(),
                                             19 /* snapshot */, bytewise_icmp));
    range_del_agg.AddTombstones(std::move(input_iter));
  }

  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 19), false},
                                      {InternalValue("a", 9), true},
                                      {InternalValue("b", 9), true},
                                      {InternalValue("d", 9), true},
                                      {InternalValue("e", 7), true},
                                      {InternalValue("g", 7), false},
                                      {InternalValue("h", 24), false},
                                      {InternalValue("i", 24), false},
                                      {InternalValue("ii", 14), true},
                                      {InternalValue("j", 14), false}});

  VerifyIsRangeOverlapped(&range_del_agg, {{"", "_", false},
                                           {"_", "a", true},
                                           {"a", "c", true},
                                           {"d", "f", true},
                                           {"g", "l", true},
                                           {"x", "y", false}});
}

TEST_F(RangeDelAggregatorV2Test, MultipleTruncatedItersInAggregator) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "z", 10}}, {{"a", "z", 10}}, {{"a", "z", 10}}});
  std::vector<std::pair<InternalKey, InternalKey>> iter_bounds = {
      {InternalKey("a", 4, kTypeValue),
       InternalKey("m", kMaxSequenceNumber, kTypeRangeDeletion)},
      {InternalKey("m", 20, kTypeValue),
       InternalKey("x", kMaxSequenceNumber, kTypeRangeDeletion)},
      {InternalKey("x", 5, kTypeValue), InternalKey("zz", 30, kTypeValue)}};

  RangeDelAggregatorV2 range_del_agg(&bytewise_icmp, 19);
  for (size_t i = 0; i < fragment_lists.size(); i++) {
    const auto& fragment_list = fragment_lists[i];
    const auto& bounds = iter_bounds[i];
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(),
                                             19 /* snapshot */, bytewise_icmp));
    range_del_agg.AddTombstones(std::move(input_iter), &bounds.first,
                                &bounds.second);
  }

  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 10), false},
                                      {InternalValue("a", 9), false},
                                      {InternalValue("a", 4), true},
                                      {InternalValue("m", 10), false},
                                      {InternalValue("m", 9), true},
                                      {InternalValue("x", 10), false},
                                      {InternalValue("x", 9), false},
                                      {InternalValue("x", 5), true},
                                      {InternalValue("z", 9), false}});

  VerifyIsRangeOverlapped(&range_del_agg, {{"", "_", false},
                                           {"_", "a", true},
                                           {"a", "n", true},
                                           {"l", "x", true},
                                           {"w", "z", true},
                                           {"zzz", "zz", false},
                                           {"zz", "zzz", false}});
}

TEST_F(RangeDelAggregatorV2Test, MultipleTruncatedItersInAggregatorSameLevel) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "z", 10}}, {{"a", "z", 10}}, {{"a", "z", 10}}});
  std::vector<std::pair<InternalKey, InternalKey>> iter_bounds = {
      {InternalKey("a", 4, kTypeValue),
       InternalKey("m", kMaxSequenceNumber, kTypeRangeDeletion)},
      {InternalKey("m", 20, kTypeValue),
       InternalKey("x", kMaxSequenceNumber, kTypeRangeDeletion)},
      {InternalKey("x", 5, kTypeValue), InternalKey("zz", 30, kTypeValue)}};

  RangeDelAggregatorV2 range_del_agg(&bytewise_icmp, 19);

  auto add_iter_to_agg = [&](size_t i) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_lists[i].get(),
                                             19 /* snapshot */, bytewise_icmp));
    range_del_agg.AddTombstones(std::move(input_iter), &iter_bounds[i].first,
                                &iter_bounds[i].second);
  };

  add_iter_to_agg(0);
  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 10), false},
                                      {InternalValue("a", 9), false},
                                      {InternalValue("a", 4), true}});

  add_iter_to_agg(1);
  VerifyShouldDelete(&range_del_agg, {{InternalValue("m", 10), false},
                                      {InternalValue("m", 9), true}});

  add_iter_to_agg(2);
  VerifyShouldDelete(&range_del_agg, {{InternalValue("x", 10), false},
                                      {InternalValue("x", 9), false},
                                      {InternalValue("x", 5), true},
                                      {InternalValue("z", 9), false}});

  VerifyIsRangeOverlapped(&range_del_agg, {{"", "_", false},
                                           {"_", "a", true},
                                           {"a", "n", true},
                                           {"l", "x", true},
                                           {"w", "z", true},
                                           {"zzz", "zz", false},
                                           {"zz", "zzz", false}});
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
