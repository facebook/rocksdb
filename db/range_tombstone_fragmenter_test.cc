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

void VerifyVisibleTombstones(
    FragmentedRangeTombstoneIterator* iter,
    const std::vector<RangeTombstone>& expected_tombstones) {
  iter->SeekToTopFirst();
  for (size_t i = 0; i < expected_tombstones.size() && iter->Valid();
       i++, iter->TopNext()) {
    EXPECT_EQ(iter->start_key(), expected_tombstones[i].start_key_);
    EXPECT_EQ(iter->value(), expected_tombstones[i].end_key_);
    EXPECT_EQ(iter->seq(), expected_tombstones[i].seq_);
  }
  EXPECT_FALSE(iter->Valid());
}

struct SeekTestCase {
  Slice seek_target;
  RangeTombstone expected_position;
  bool out_of_range;
};

void VerifySeek(FragmentedRangeTombstoneIterator* iter,
                const std::vector<SeekTestCase>& cases) {
  for (const auto& testcase : cases) {
    iter->Seek(testcase.seek_target);
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
    iter->SeekForPrev(testcase.seek_target);
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
  SequenceNumber result;
};

void VerifyMaxCoveringTombstoneSeqnum(
    FragmentedRangeTombstoneIterator* iter,
    const std::vector<MaxCoveringTombstoneSeqnumTestCase>& cases) {
  for (const auto& testcase : cases) {
    EXPECT_EQ(testcase.result,
              iter->MaxCoveringTombstoneSeqnum(testcase.user_key));
  }
}

}  // anonymous namespace

TEST_F(RangeTombstoneFragmenterTest, NonOverlappingTombstones) {
  auto range_del_iter = MakeRangeDelIter({{"a", "b", 10}, {"c", "d", 5}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, kMaxSequenceNumber,
                                        bytewise_icmp);
  VerifyFragmentedRangeDels(&iter, {{"a", "b", 10}, {"c", "d", 5}});
  VerifyMaxCoveringTombstoneSeqnum(&iter,
                                   {{"", 0}, {"a", 10}, {"b", 0}, {"c", 5}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlappingTombstones) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10}, {"c", "g", 15}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, kMaxSequenceNumber,
                                        bytewise_icmp);
  VerifyFragmentedRangeDels(
      &iter, {{"a", "c", 10}, {"c", "e", 15}, {"c", "e", 10}, {"e", "g", 15}});
  VerifyMaxCoveringTombstoneSeqnum(&iter,
                                   {{"a", 10}, {"c", 15}, {"e", 15}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, ContiguousTombstones) {
  auto range_del_iter = MakeRangeDelIter(
      {{"a", "c", 10}, {"c", "e", 20}, {"c", "e", 5}, {"e", "g", 15}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, kMaxSequenceNumber,
                                        bytewise_icmp);
  VerifyFragmentedRangeDels(
      &iter, {{"a", "c", 10}, {"c", "e", 20}, {"c", "e", 5}, {"e", "g", 15}});
  VerifyMaxCoveringTombstoneSeqnum(&iter,
                                   {{"a", 10}, {"c", 20}, {"e", 15}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, RepeatedStartAndEndKey) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "c", 10}, {"a", "c", 7}, {"a", "c", 3}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, kMaxSequenceNumber,
                                        bytewise_icmp);
  VerifyFragmentedRangeDels(&iter,
                            {{"a", "c", 10}, {"a", "c", 7}, {"a", "c", 3}});
  VerifyMaxCoveringTombstoneSeqnum(&iter, {{"a", 10}, {"b", 10}, {"c", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, RepeatedStartKeyDifferentEndKeys) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "e", 10}, {"a", "g", 7}, {"a", "c", 3}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, kMaxSequenceNumber,
                                        bytewise_icmp);
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 10},
                                    {"a", "c", 7},
                                    {"a", "c", 3},
                                    {"c", "e", 10},
                                    {"c", "e", 7},
                                    {"e", "g", 7}});
  VerifyMaxCoveringTombstoneSeqnum(&iter,
                                   {{"a", 10}, {"c", 10}, {"e", 7}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, RepeatedStartKeyMixedEndKeys) {
  auto range_del_iter = MakeRangeDelIter({{"a", "c", 30},
                                          {"a", "g", 20},
                                          {"a", "e", 10},
                                          {"a", "g", 7},
                                          {"a", "c", 3}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, kMaxSequenceNumber,
                                        bytewise_icmp);
  VerifyFragmentedRangeDels(&iter, {{"a", "c", 30},
                                    {"a", "c", 20},
                                    {"a", "c", 10},
                                    {"a", "c", 7},
                                    {"a", "c", 3},
                                    {"c", "e", 20},
                                    {"c", "e", 10},
                                    {"c", "e", 7},
                                    {"e", "g", 20},
                                    {"e", "g", 7}});
  VerifyMaxCoveringTombstoneSeqnum(&iter,
                                   {{"a", 30}, {"c", 20}, {"e", 20}, {"g", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlapAndRepeatedStartKey) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter1(&fragment_list, kMaxSequenceNumber,
                                         bytewise_icmp);
  FragmentedRangeTombstoneIterator iter2(&fragment_list, 9 /* snapshot */,
                                         bytewise_icmp);
  FragmentedRangeTombstoneIterator iter3(&fragment_list, 7 /* snapshot */,
                                         bytewise_icmp);
  FragmentedRangeTombstoneIterator iter4(&fragment_list, 5 /* snapshot */,
                                         bytewise_icmp);
  FragmentedRangeTombstoneIterator iter5(&fragment_list, 3 /* snapshot */,
                                         bytewise_icmp);
  for (auto* iter : {&iter1, &iter2, &iter3, &iter4, &iter5}) {
    VerifyFragmentedRangeDels(iter, {{"a", "c", 10},
                                     {"c", "e", 10},
                                     {"c", "e", 8},
                                     {"c", "e", 6},
                                     {"e", "g", 8},
                                     {"e", "g", 6},
                                     {"g", "i", 6},
                                     {"j", "l", 4},
                                     {"j", "l", 2},
                                     {"l", "n", 4}});
  }

  VerifyVisibleTombstones(&iter1, {{"a", "c", 10},
                                   {"c", "e", 10},
                                   {"e", "g", 8},
                                   {"g", "i", 6},
                                   {"j", "l", 4},
                                   {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter1, {{"a", 10}, {"c", 10}, {"e", 8}, {"i", 0}, {"j", 4}, {"m", 4}});

  VerifyVisibleTombstones(&iter2, {{"c", "e", 8},
                                   {"e", "g", 8},
                                   {"g", "i", 6},
                                   {"j", "l", 4},
                                   {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter2, {{"a", 0}, {"c", 8}, {"e", 8}, {"i", 0}, {"j", 4}, {"m", 4}});

  VerifyVisibleTombstones(&iter3, {{"c", "e", 6},
                                   {"e", "g", 6},
                                   {"g", "i", 6},
                                   {"j", "l", 4},
                                   {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter3, {{"a", 0}, {"c", 6}, {"e", 6}, {"i", 0}, {"j", 4}, {"m", 4}});

  VerifyVisibleTombstones(&iter4, {{"j", "l", 4}, {"l", "n", 4}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter4, {{"a", 0}, {"c", 0}, {"e", 0}, {"i", 0}, {"j", 4}, {"m", 4}});

  VerifyVisibleTombstones(&iter5, {{"j", "l", 2}});
  VerifyMaxCoveringTombstoneSeqnum(
      &iter5, {{"a", 0}, {"c", 0}, {"e", 0}, {"i", 0}, {"j", 2}, {"m", 0}});
}

TEST_F(RangeTombstoneFragmenterTest, OverlapAndRepeatedStartKeyUnordered) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"j", "n", 4},
                                          {"c", "i", 6},
                                          {"c", "g", 8},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  FragmentedRangeTombstoneIterator iter(&fragment_list, 9 /* snapshot */,
                                        bytewise_icmp);
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
      &iter, {{"a", 0}, {"c", 8}, {"e", 8}, {"i", 0}, {"j", 4}, {"m", 4}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekStartKey) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);

  FragmentedRangeTombstoneIterator iter1(&fragment_list, kMaxSequenceNumber,
                                         bytewise_icmp);
  VerifySeek(
      &iter1,
      {{"a", {"a", "c", 10}}, {"e", {"e", "g", 8}}, {"l", {"l", "n", 4}}});
  VerifySeekForPrev(
      &iter1,
      {{"a", {"a", "c", 10}}, {"e", {"e", "g", 8}}, {"l", {"l", "n", 4}}});

  FragmentedRangeTombstoneIterator iter2(&fragment_list, 3 /* snapshot */,
                                         bytewise_icmp);
  VerifySeek(&iter2, {{"a", {"j", "l", 2}},
                      {"e", {"j", "l", 2}},
                      {"l", {}, true /* out of range */}});
  VerifySeekForPrev(&iter2, {{"a", {}, true /* out of range */},
                             {"e", {}, true /* out of range */},
                             {"l", {"j", "l", 2}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekCovered) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);

  FragmentedRangeTombstoneIterator iter1(&fragment_list, kMaxSequenceNumber,
                                         bytewise_icmp);
  VerifySeek(
      &iter1,
      {{"b", {"a", "c", 10}}, {"f", {"e", "g", 8}}, {"m", {"l", "n", 4}}});
  VerifySeekForPrev(
      &iter1,
      {{"b", {"a", "c", 10}}, {"f", {"e", "g", 8}}, {"m", {"l", "n", 4}}});

  FragmentedRangeTombstoneIterator iter2(&fragment_list, 3 /* snapshot */,
                                         bytewise_icmp);
  VerifySeek(&iter2, {{"b", {"j", "l", 2}},
                      {"f", {"j", "l", 2}},
                      {"m", {}, true /* out of range */}});
  VerifySeekForPrev(&iter2, {{"b", {}, true /* out of range */},
                             {"f", {}, true /* out of range */},
                             {"m", {"j", "l", 2}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekEndKey) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);

  FragmentedRangeTombstoneIterator iter1(&fragment_list, kMaxSequenceNumber,
                                         bytewise_icmp);
  VerifySeek(&iter1, {{"c", {"c", "e", 10}},
                      {"g", {"g", "i", 6}},
                      {"i", {"j", "l", 4}},
                      {"n", {}, true /* out of range */}});
  VerifySeekForPrev(&iter1, {{"c", {"c", "e", 10}},
                             {"g", {"g", "i", 6}},
                             {"i", {"g", "i", 6}},
                             {"n", {"l", "n", 4}}});

  FragmentedRangeTombstoneIterator iter2(&fragment_list, 3 /* snapshot */,
                                         bytewise_icmp);
  VerifySeek(&iter2, {{"c", {"j", "l", 2}},
                      {"g", {"j", "l", 2}},
                      {"i", {"j", "l", 2}},
                      {"n", {}, true /* out of range */}});
  VerifySeekForPrev(&iter2, {{"c", {}, true /* out of range */},
                             {"g", {}, true /* out of range */},
                             {"i", {}, true /* out of range */},
                             {"n", {"j", "l", 2}}});
}

TEST_F(RangeTombstoneFragmenterTest, SeekOutOfBounds) {
  // Same tombstones as OverlapAndRepeatedStartKey.
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10},
                                          {"c", "g", 8},
                                          {"c", "i", 6},
                                          {"j", "n", 4},
                                          {"j", "l", 2}});

  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);

  FragmentedRangeTombstoneIterator iter(&fragment_list, kMaxSequenceNumber,
                                        bytewise_icmp);
  VerifySeek(&iter, {{"", {"a", "c", 10}}, {"z", {}, true /* out of range */}});
  VerifySeekForPrev(&iter,
                    {{"", {}, true /* out of range */}, {"z", {"l", "n", 4}}});
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
