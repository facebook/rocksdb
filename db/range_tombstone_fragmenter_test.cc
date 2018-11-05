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
    EXPECT_EQ(iter->start_key(), expected_tombstones[i].start_key_);
    EXPECT_EQ(iter->value(), expected_tombstones[i].end_key_);
    EXPECT_EQ(iter->seq(), expected_tombstones[i].seq_);
  }
  EXPECT_FALSE(iter->Valid());
}

struct SeekTestCase {
  Slice seek_target;
  SequenceNumber target_seq;
  RangeTombstone expected_position;
  bool out_of_range;
};

void VerifySeek(FragmentedRangeTombstoneIterator* iter,
                const std::vector<SeekTestCase>& cases) {
  for (const auto& testcase : cases) {
    InternalKey ikey_seek_target(testcase.seek_target, testcase.target_seq,
                                 kTypeRangeDeletion);
    iter->Seek(ikey_seek_target.Encode());
    if (testcase.out_of_range) {
      ASSERT_FALSE(iter->Valid());
    } else {
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(testcase.expected_position.start_key_, iter->start_key());
      EXPECT_EQ(testcase.expected_position.end_key_, iter->value());
      EXPECT_EQ(testcase.expected_position.seq_, iter->seq());
    }
  }
}

void VerifySeekForPrev(FragmentedRangeTombstoneIterator* iter,
                       const std::vector<SeekTestCase>& cases) {
  for (const auto& testcase : cases) {
    InternalKey ikey_seek_target(testcase.seek_target, testcase.target_seq,
                                 kTypeRangeDeletion);
    iter->SeekForPrev(ikey_seek_target.Encode());
    if (testcase.out_of_range) {
      ASSERT_FALSE(iter->Valid());
    } else {
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(testcase.expected_position.start_key_, iter->start_key());
      EXPECT_EQ(testcase.expected_position.end_key_, iter->value());
      EXPECT_EQ(testcase.expected_position.seq_, iter->seq());
    }
  }
}

struct MaxCoveringTombstoneSeqnumTestCase {
  Slice user_key;
  SequenceNumber snapshot;
  SequenceNumber result;
};

void VerifyMaxCoveringTombstoneSeqnum(
    FragmentedRangeTombstoneIterator* iter, const Comparator* ucmp,
    const std::vector<MaxCoveringTombstoneSeqnumTestCase>& cases) {
  for (const auto& testcase : cases) {
    InternalKey key_and_snapshot(testcase.user_key, testcase.snapshot,
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
                                   {{"", kMaxSequenceNumber, 0},
                                    {"a", kMaxSequenceNumber, 10},
                                    {"b", kMaxSequenceNumber, 0},
                                    {"c", kMaxSequenceNumber, 5}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlappingTombstones) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10}, {"c", "g", 15}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(&iter,
                            {{"a", "c", 10}, {"c", "e", 15}, {"e", "g", 15}});
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", kMaxSequenceNumber, 10},
                                    {"c", kMaxSequenceNumber, 15},
                                    {"e", kMaxSequenceNumber, 15},
                                    {"g", kMaxSequenceNumber, 0}});
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
                                   {{"a", kMaxSequenceNumber, 10},
                                    {"c", kMaxSequenceNumber, 20},
                                    {"e", kMaxSequenceNumber, 15},
                                    {"g", kMaxSequenceNumber, 0}});
}

TEST_F(RangeTombstoneFragmenterTest, RepeatedStartAndEndKey) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "c", 10}, {"a", "c", 7}, {"a", "c", 3}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 10}});
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", kMaxSequenceNumber, 10},
                                    {"b", kMaxSequenceNumber, 10},
                                    {"c", kMaxSequenceNumber, 0}});
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
                                   {{"a", kMaxSequenceNumber, 10},
                                    {"c", kMaxSequenceNumber, 10},
                                    {"e", kMaxSequenceNumber, 7},
                                    {"g", kMaxSequenceNumber, 0}});
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
                                   {{"a", kMaxSequenceNumber, 30},
                                    {"c", kMaxSequenceNumber, 20},
                                    {"e", kMaxSequenceNumber, 20},
                                    {"g", kMaxSequenceNumber, 0}});
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
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", kMaxSequenceNumber, 10},
                                    {"c", kMaxSequenceNumber, 10},
                                    {"e", kMaxSequenceNumber, 8},
                                    {"i", kMaxSequenceNumber, 0},
                                    {"j", kMaxSequenceNumber, 4},
                                    {"m", kMaxSequenceNumber, 4}});
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
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", 9, 0},
                                    {"c", 9, 8},
                                    {"e", 9, 8},
                                    {"i", 9, 0},
                                    {"j", 9, 4},
                                    {"m", 9, 4}});
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
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", 9, 0},
                                    {"c", 9, 8},
                                    {"e", 9, 8},
                                    {"i", 9, 0},
                                    {"j", 9, 4},
                                    {"m", 9, 4}});
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
  VerifyMaxCoveringTombstoneSeqnum(&iter, bytewise_icmp.user_comparator(),
                                   {{"a", kMaxSequenceNumber, 10},
                                    {"a", 9, 0},
                                    {"c", kMaxSequenceNumber, 10},
                                    {"c", 9, 8},
                                    {"c", 7, 6},
                                    {"c", 5, 0},
                                    {"e", kMaxSequenceNumber, 8},
                                    {"e", 7, 6},
                                    {"e", 5, 0},
                                    {"i", kMaxSequenceNumber, 0},
                                    {"j", kMaxSequenceNumber, 4},
                                    {"j", 3, 2},
                                    {"m", kMaxSequenceNumber, 4}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekStartKey) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, false /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifySeek(&iter, {{"a", kMaxSequenceNumber, {"a", "c", 10}},
                     {"a", 8, {"c", "e", 10}},
                     {"e", kMaxSequenceNumber, {"e", "g", 8}},
                     {"e", 6, {"e", "g", 6}},
                     {"l", kMaxSequenceNumber, {"l", "n", 4}},
                     {"l", 3, {}, true /* out of range */}});
  VerifySeekForPrev(&iter, {{"a", kMaxSequenceNumber, {"a", "c", 10}},
                            {"a", 8, {}, true /* out of range */},
                            {"e", kMaxSequenceNumber, {"e", "g", 8}},
                            {"e", 6, {"e", "g", 6}},
                            {"l", kMaxSequenceNumber, {"l", "n", 4}},
                            {"l", 3, {"j", "l", 4}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekCovered) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, false /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifySeek(&iter, {{"b", kMaxSequenceNumber, {"a", "c", 10}},
                     {"b", 8, {"c", "e", 10}},
                     {"f", kMaxSequenceNumber, {"e", "g", 8}},
                     {"f", 6, {"e", "g", 6}},
                     {"m", kMaxSequenceNumber, {"l", "n", 4}},
                     {"m", 3, {}, true /* out of range */}});
  VerifySeekForPrev(&iter, {{"b", kMaxSequenceNumber, {"a", "c", 10}},
                            {"b", 8, {}, true /* out of range */},
                            {"f", kMaxSequenceNumber, {"e", "g", 8}},
                            {"f", 6, {"e", "g", 6}},
                            {"m", kMaxSequenceNumber, {"l", "n", 4}},
                            {"m", 3, {"j", "l", 4}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekEndKey) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, false /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifySeek(&iter, {{"c", kMaxSequenceNumber, {"c", "e", 10}},
                     {"c", 9, {"c", "e", 8}},
                     {"g", kMaxSequenceNumber, {"g", "i", 6}},
                     {"g", 4, {"j", "l", 4}},
                     {"i", kMaxSequenceNumber, {"j", "l", 4}},
                     {"n", kMaxSequenceNumber, {}, true /* out of range */}});
  VerifySeekForPrev(&iter, {{"c", kMaxSequenceNumber, {"c", "e", 10}},
                            {"c", 9, {"c", "e", 8}},
                            {"g", kMaxSequenceNumber, {"g", "i", 6}},
                            {"g", 4, {"e", "g", 8}},
                            {"i", kMaxSequenceNumber, {"g", "i", 6}},
                            {"n", kMaxSequenceNumber, {"l", "n", 4}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekOutOfBounds) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, false /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifySeek(&iter, {{"", kMaxSequenceNumber, {"a", "c", 10}},
                     {"z", kMaxSequenceNumber, {}, true /* out of range */}});
  VerifySeekForPrev(&iter,
                    {{"", kMaxSequenceNumber, {}, true /* out of range */},
                     {"z", kMaxSequenceNumber, {"l", "n", 4}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekOneTimeUse) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(
      std::move(range_del_iter), bytewise_icmp, true /* one_time_use */);
  FragmentedRangeTombstoneIterator iter(&fragment_list, bytewise_icmp);
  VerifySeek(&iter, {{"a", kMaxSequenceNumber, {"a", "c", 10}},
                     {"e", kMaxSequenceNumber, {"e", "g", 8}},
                     {"e", 6, {"g", "i", 6}},
                     {"l", kMaxSequenceNumber, {"l", "n", 4}},
                     {"l", 3, {}, true /* out of range */}});
  VerifySeekForPrev(&iter, {{"a", kMaxSequenceNumber, {"a", "c", 10}},
                            {"a", 8, {}, true /* out of range */},
                            {"e", kMaxSequenceNumber, {"e", "g", 8}},
                            {"e", 6, {"c", "e", 10}},
                            {"l", kMaxSequenceNumber, {"l", "n", 4}},
                            {"l", 3, {"j", "l", 4}}});
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
