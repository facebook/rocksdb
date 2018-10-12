//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_tombstone_fragmenter.h"

#include "db/db_test_util.h"
#include "rocksdb/comparator.h"
#include "util/testutil.h"

namespace rocksdb {

class RangeTombstoneFragmenterTest : public testing::Test {};

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

void VerifyFragmentedRangeDels(
    FragmentedRangeTombstoneIterator* iter,
    const std::vector<RangeTombstone>& expected_tombstones) {
  iter->SeekToFirst();
  for (size_t i = 0; i < expected_tombstones.size() && iter->Valid();
       i++, iter->Next()) {
    EXPECT_EQ(ExtractUserKey(iter->key()), expected_tombstones[i].start_key_);
    EXPECT_EQ(iter->value(), expected_tombstones[i].end_key_);
    EXPECT_EQ(GetInternalKeySeqno(iter->key()), expected_tombstones[i].seq_);
  }
  EXPECT_FALSE(iter->Valid());
}

struct SeekForPrevTestCase {
  Slice seek_target;
  RangeTombstone expected_position;
  bool out_of_range;
};

void VerifySeekForPrev(FragmentedRangeTombstoneIterator* iter,
                       const std::vector<SeekForPrevTestCase>& cases) {
  for (const auto& testcase : cases) {
    InternalKey ikey_seek_target(testcase.seek_target, 0, kTypeRangeDeletion);
    iter->SeekForPrev(ikey_seek_target.Encode());
    if (testcase.out_of_range) {
      ASSERT_FALSE(iter->Valid());
    } else {
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(ExtractUserKey(iter->key()),
                testcase.expected_position.start_key_);
      EXPECT_EQ(iter->value(), testcase.expected_position.end_key_);
      EXPECT_EQ(GetInternalKeySeqno(iter->key()),
                testcase.expected_position.seq_);
    }
  }
}

struct MaxCoveringTombstoneSeqnumTestCase {
  Slice user_key;
  int result;
};

void VerifyMaxCoveringTombstoneSeqnum(
    FragmentedRangeTombstoneIterator* iter, const Comparator* ucmp,
    const std::vector<MaxCoveringTombstoneSeqnumTestCase>& cases) {
  for (const auto& testcase : cases) {
    InternalKey key_and_snapshot(testcase.user_key, kMaxSequenceNumber,
                                 kTypeValue);
    EXPECT_EQ(testcase.result, MaxCoveringTombstoneSeqnum(
                                   iter, key_and_snapshot.Encode(), ucmp));
  }
}

}  // anonymous namespace

TEST_F(RangeTombstoneFragmenterTest, NonOverlappingTombstones) {
  auto range_del_iter = MakeRangeDelIter({{"a", "b", 10}, {"c", "d", 5}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(&iter, {{"a", "b", 10}, {"c", "d", 5}});
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"", 0}, {"a", 10}, {"b", 0}, {"c", 5}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlappingTombstones) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10}, {"c", "g", 15}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(&iter,
                            {{"a", "c", 10}, {"c", "e", 15}, {"e", "g", 15}});
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", 10}, {"c", 15}, {"e", 15}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, ContiguousTombstones) {
  auto range_del_iter = MakeRangeDelIter(
      {{"a", "c", 10}, {"c", "e", 20}, {"c", "e", 5}, {"e", "g", 15}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(&iter,
                            {{"a", "c", 10}, {"c", "e", 20}, {"e", "g", 15}});
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", 10}, {"c", 20}, {"e", 15}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, RepeatedStartAndEndKey) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "c", 10}, {"a", "c", 7}, {"a", "c", 3}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 10}});
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", 10}, {"b", 10}, {"c", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, RepeatedStartKeyDifferentEndKeys) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "e", 10}, {"a", "g", 7}, {"a", "c", 3}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(&iter,
                            {{"a", "c", 10}, {"c", "e", 10}, {"e", "g", 7}});
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", 10}, {"c", 10}, {"e", 7}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, RepeatedStartKeyMixedEndKeys) {
  auto range_del_iter = MakeRangeDelIter({{"a", "c", 30},
                                          {"a", "g", 20},
                                          {"a", "e", 10},
                                          {"a", "g", 7},
                                          {"a", "c", 3}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(&iter,
                            {{"a", "c", 30}, {"c", "e", 20}, {"e", "g", 20}});
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", 30}, {"c", 20}, {"e", 20}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlapAndRepeatedStartKey) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 10},
                                    {"c", "e", 10},
                                    {"e", "g", 8},
                                    {"g", "i", 6},
                                    {"j", "l", 4},
                                    {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter, bytewise_icmp.user_comparator(),
      {{"a", 10}, {"c", 10}, {"e", 8}, {"i", 0}, {"j", 4}, {"m", 4}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlapAndRepeatedStartKeyWithSnapshot) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */, 9);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(
      &iter, {{"c", "g", 8}, {"g", "i", 6}, {"j", "l", 4}, {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter, bytewise_icmp.user_comparator(),
      {{"a", 0}, {"c", 8}, {"e", 8}, {"i", 0}, {"j", 4}, {"m", 4}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlapAndRepeatedStartKeyUnordered) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"j", "n", 4},
                                          {"c", "i", 6},
                                          {"c", "g", 8},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */, 9);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(
      &iter, {{"c", "g", 8}, {"g", "i", 6}, {"j", "l", 4}, {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter, bytewise_icmp.user_comparator(),
      {{"a", 0}, {"c", 8}, {"e", 8}, {"i", 0}, {"j", 4}, {"m", 4}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlapAndRepeatedStartKeyMultiUse) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, false /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 10},
                                    {"c", "e", 10},
                                    {"c", "e", 8},
                                    {"c", "e", 6},
                                    {"e", "g", 8},
                                    {"e", "g", 6},
                                    {"g", "i", 6},
                                    {"j", "l", 4},
                                    {"j", "l", 2},
                                    {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter, bytewise_icmp.user_comparator(),
      {{"a", 10}, {"c", 10}, {"e", 8}, {"i", 0}, {"j", 4}, {"m", 4}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekForPrevStartKey) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifySeekForPrev(
      &iter,
      {{"a", {"a", "c", 10}}, {"e", {"e", "g", 8}}, {"l", {"l", "n", 4}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekForPrevCovered) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifySeekForPrev(
      &iter,
      {{"b", {"a", "c", 10}}, {"f", {"e", "g", 8}}, {"m", {"l", "n", 4}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekForPrevEndKey) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifySeekForPrev(&iter, {{"c", {"c", "e", 10}},
                            {"g", {"g", "i", 6}},
                            {"i", {"g", "i", 6}},
                            {"n", {"l", "n", 4}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekForPrevOutOfBounds) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifySeekForPrev(&iter,
                    {{"", {}, true /* out of range */}, {"z", {"l", "n", 4}}});
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
