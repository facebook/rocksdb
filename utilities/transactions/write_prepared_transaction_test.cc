//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_test.h"

#include <inttypes.h>
#include <algorithm>
#include <functional>
#include <string>
#include <thread>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table/mock_table.h"
#include "util/fault_injection_test_env.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/transaction_test_util.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

#include "port/port.h"

using std::string;

namespace rocksdb {

using CommitEntry = WritePreparedTxnDB::CommitEntry;
using CommitEntry64b = WritePreparedTxnDB::CommitEntry64b;
using CommitEntry64bFormat = WritePreparedTxnDB::CommitEntry64bFormat;

TEST(PreparedHeap, BasicsTest) {
  WritePreparedTxnDB::PreparedHeap heap;
  heap.push(14l);
  // Test with one element
  ASSERT_EQ(14l, heap.top());
  heap.push(24l);
  heap.push(34l);
  // Test that old min is still on top
  ASSERT_EQ(14l, heap.top());
  heap.push(13l);
  // Test that the new min will be on top
  ASSERT_EQ(13l, heap.top());
  // Test that it is persistent
  ASSERT_EQ(13l, heap.top());
  heap.push(44l);
  heap.push(54l);
  heap.push(64l);
  heap.push(74l);
  heap.push(84l);
  // Test that old min is still on top
  ASSERT_EQ(13l, heap.top());
  heap.erase(24l);
  // Test that old min is still on top
  ASSERT_EQ(13l, heap.top());
  heap.erase(14l);
  // Test that old min is still on top
  ASSERT_EQ(13l, heap.top());
  heap.erase(13l);
  // Test that the new comes to the top after multiple erase
  ASSERT_EQ(34l, heap.top());
  heap.erase(34l);
  // Test that the new comes to the top after single erase
  ASSERT_EQ(44l, heap.top());
  heap.erase(54l);
  ASSERT_EQ(44l, heap.top());
  heap.pop();  // pop 44l
  // Test that the erased items are ignored after pop
  ASSERT_EQ(64l, heap.top());
  heap.erase(44l);
  // Test that erasing an already popped item would work
  ASSERT_EQ(64l, heap.top());
  heap.erase(84l);
  ASSERT_EQ(64l, heap.top());
  heap.push(85l);
  heap.push(86l);
  heap.push(87l);
  heap.push(88l);
  heap.push(89l);
  heap.erase(87l);
  heap.erase(85l);
  heap.erase(89l);
  heap.erase(86l);
  heap.erase(88l);
  // Test top remians the same after a ranodm order of many erases
  ASSERT_EQ(64l, heap.top());
  heap.pop();
  // Test that pop works with a series of random pending erases
  ASSERT_EQ(74l, heap.top());
  ASSERT_FALSE(heap.empty());
  heap.pop();
  // Test that empty works
  ASSERT_TRUE(heap.empty());
}

TEST(CommitEntry64b, BasicTest) {
  const size_t INDEX_BITS = static_cast<size_t>(21);
  const size_t INDEX_SIZE = static_cast<size_t>(1ull << INDEX_BITS);
  const CommitEntry64bFormat FORMAT(static_cast<size_t>(INDEX_BITS));

  // zero-initialized CommitEntry64b should inidcate an empty entry
  CommitEntry64b empty_entry64b;
  uint64_t empty_index = 11ul;
  CommitEntry empty_entry;
  bool ok = empty_entry64b.Parse(empty_index, &empty_entry, FORMAT);
  ASSERT_FALSE(ok);

  // the zero entry is reserved for un-initialized entries
  const size_t MAX_COMMIT = (1 << FORMAT.COMMIT_BITS) - 1 - 1;
  // Samples over the numbers that are covered by that many index bits
  std::array<uint64_t, 4> is = {{0, 1, INDEX_SIZE / 2 + 1, INDEX_SIZE - 1}};
  // Samples over the numbers that are covered by that many commit bits
  std::array<uint64_t, 4> ds = {{0, 1, MAX_COMMIT / 2 + 1, MAX_COMMIT}};
  // Iterate over prepare numbers that have i) cover all bits of a sequence
  // number, and ii) include some bits that fall into the range of index or
  // commit bits
  for (uint64_t base = 1; base < kMaxSequenceNumber; base *= 2) {
    for (uint64_t i : is) {
      for (uint64_t d : ds) {
        uint64_t p = base + i + d;
        for (uint64_t c : {p, p + d / 2, p + d}) {
          uint64_t index = p % INDEX_SIZE;
          CommitEntry before(p, c), after;
          CommitEntry64b entry64b(before, FORMAT);
          ok = entry64b.Parse(index, &after, FORMAT);
          ASSERT_TRUE(ok);
          if (!(before == after)) {
            printf("base %" PRIu64 " i %" PRIu64 " d %" PRIu64 " p %" PRIu64
                   " c %" PRIu64 " index %" PRIu64 "\n",
                   base, i, d, p, c, index);
          }
          ASSERT_EQ(before, after);
        }
      }
    }
  }
}

class WritePreparedTxnDBMock : public WritePreparedTxnDB {
 public:
  WritePreparedTxnDBMock(DBImpl* db_impl, TransactionDBOptions& opt)
      : WritePreparedTxnDB(db_impl, opt) {}
  WritePreparedTxnDBMock(DBImpl* db_impl, TransactionDBOptions& opt,
                         size_t snapshot_cache_size)
      : WritePreparedTxnDB(db_impl, opt, snapshot_cache_size) {}
  WritePreparedTxnDBMock(DBImpl* db_impl, TransactionDBOptions& opt,
                         size_t snapshot_cache_size, size_t commit_cache_size)
      : WritePreparedTxnDB(db_impl, opt, snapshot_cache_size,
                           commit_cache_size) {}
  void SetDBSnapshots(const std::vector<SequenceNumber>& snapshots) {
    snapshots_ = snapshots;
  }
  void TakeSnapshot(SequenceNumber seq) { snapshots_.push_back(seq); }

 protected:
  virtual const std::vector<SequenceNumber> GetSnapshotListFromDB(
      SequenceNumber /* unused */) override {
    return snapshots_;
  }

 private:
  std::vector<SequenceNumber> snapshots_;
};

class WritePreparedTransactionTest : public TransactionTest {
 protected:
  // If expect_update is set, check if it actually updated old_commit_map_. If
  // it did not and yet suggested not to check the next snapshot, do the
  // opposite to check if it was not a bad suggstion.
  void MaybeUpdateOldCommitMapTestWithNext(uint64_t prepare, uint64_t commit,
                                           uint64_t snapshot,
                                           uint64_t next_snapshot,
                                           bool expect_update) {
    WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
    // reset old_commit_map_empty_ so that its value indicate whether
    // old_commit_map_ was updated
    wp_db->old_commit_map_empty_ = true;
    bool check_next = wp_db->MaybeUpdateOldCommitMap(prepare, commit, snapshot,
                                                     snapshot < next_snapshot);
    if (expect_update == wp_db->old_commit_map_empty_) {
      printf("prepare: %" PRIu64 " commit: %" PRIu64 " snapshot: %" PRIu64
             " next: %" PRIu64 "\n",
             prepare, commit, snapshot, next_snapshot);
    }
    EXPECT_EQ(!expect_update, wp_db->old_commit_map_empty_);
    if (!check_next && wp_db->old_commit_map_empty_) {
      // do the oppotisite to make sure it was not a bad suggestion
      const bool dont_care_bool = true;
      wp_db->MaybeUpdateOldCommitMap(prepare, commit, next_snapshot,
                                     dont_care_bool);
      if (!wp_db->old_commit_map_empty_) {
        printf("prepare: %" PRIu64 " commit: %" PRIu64 " snapshot: %" PRIu64
               " next: %" PRIu64 "\n",
               prepare, commit, snapshot, next_snapshot);
      }
      EXPECT_TRUE(wp_db->old_commit_map_empty_);
    }
  }

  // Test that a CheckAgainstSnapshots thread reading old_snapshots will not
  // miss a snapshot because of a concurrent update by UpdateSnapshots that is
  // writing new_snapshots. Both threads are broken at two points. The sync
  // points to enforce them are specified by a1, a2, b1, and b2. CommitEntry
  // entry is expected to be vital for one of the snapshots that is common
  // between the old and new list of snapshots.
  void SnapshotConcurrentAccessTestInternal(
      WritePreparedTxnDB* wp_db,
      const std::vector<SequenceNumber>& old_snapshots,
      const std::vector<SequenceNumber>& new_snapshots, CommitEntry& entry,
      SequenceNumber& version, size_t a1, size_t a2, size_t b1, size_t b2) {
    // First reset the snapshot list
    const std::vector<SequenceNumber> empty_snapshots;
    wp_db->old_commit_map_empty_ = true;
    wp_db->UpdateSnapshots(empty_snapshots, ++version);
    // Then initialize it with the old_snapshots
    wp_db->UpdateSnapshots(old_snapshots, ++version);

    // Starting from the first thread, cut each thread at two points
    rocksdb::SyncPoint::GetInstance()->LoadDependency({
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(a1),
         "WritePreparedTxnDB::UpdateSnapshots:s:start"},
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(b1),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(a1)},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(a2),
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(b1)},
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(b2),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(a2)},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:end",
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(b2)},
    });
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    {
      ASSERT_TRUE(wp_db->old_commit_map_empty_);
      rocksdb::port::Thread t1(
          [&]() { wp_db->UpdateSnapshots(new_snapshots, version); });
      rocksdb::port::Thread t2([&]() { wp_db->CheckAgainstSnapshots(entry); });
      t1.join();
      t2.join();
      ASSERT_FALSE(wp_db->old_commit_map_empty_);
    }
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();

    wp_db->old_commit_map_empty_ = true;
    wp_db->UpdateSnapshots(empty_snapshots, ++version);
    wp_db->UpdateSnapshots(old_snapshots, ++version);
    // Starting from the second thread, cut each thread at two points
    rocksdb::SyncPoint::GetInstance()->LoadDependency({
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(a1),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:start"},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(b1),
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(a1)},
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(a2),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(b1)},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(b2),
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(a2)},
        {"WritePreparedTxnDB::UpdateSnapshots:p:end",
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(b2)},
    });
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    {
      ASSERT_TRUE(wp_db->old_commit_map_empty_);
      rocksdb::port::Thread t1(
          [&]() { wp_db->UpdateSnapshots(new_snapshots, version); });
      rocksdb::port::Thread t2([&]() { wp_db->CheckAgainstSnapshots(entry); });
      t1.join();
      t2.join();
      ASSERT_FALSE(wp_db->old_commit_map_empty_);
    }
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }
};

INSTANTIATE_TEST_CASE_P(WritePreparedTransactionTest,
                        WritePreparedTransactionTest,
                        ::testing::Values(std::make_tuple(false, true,
                                                          WRITE_PREPARED)));

TEST_P(WritePreparedTransactionTest, CommitMapTest) {
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  assert(wp_db);
  assert(wp_db->db_impl_);
  size_t size = wp_db->COMMIT_CACHE_SIZE;
  CommitEntry c = {5, 12}, e;
  bool evicted = wp_db->AddCommitEntry(c.prep_seq % size, c, &e);
  ASSERT_FALSE(evicted);

  // Should be able to read the same value
  CommitEntry64b dont_care;
  bool found = wp_db->GetCommitEntry(c.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(c, e);
  // Should be able to distinguish between overlapping entries
  found = wp_db->GetCommitEntry((c.prep_seq + size) % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_NE(c.prep_seq + size, e.prep_seq);
  // Should be able to detect non-existent entry
  found = wp_db->GetCommitEntry((c.prep_seq + 1) % size, &dont_care, &e);
  ASSERT_FALSE(found);

  // Reject an invalid exchange
  CommitEntry e2 = {c.prep_seq + size, c.commit_seq + size};
  CommitEntry64b e2_64b(e2, wp_db->FORMAT);
  bool exchanged = wp_db->ExchangeCommitEntry(e2.prep_seq % size, e2_64b, e);
  ASSERT_FALSE(exchanged);
  // check whether it did actually reject that
  found = wp_db->GetCommitEntry(e2.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(c, e);

  // Accept a valid exchange
  CommitEntry64b c_64b(c, wp_db->FORMAT);
  CommitEntry e3 = {c.prep_seq + size, c.commit_seq + size + 1};
  exchanged = wp_db->ExchangeCommitEntry(c.prep_seq % size, c_64b, e3);
  ASSERT_TRUE(exchanged);
  // check whether it did actually accepted that
  found = wp_db->GetCommitEntry(c.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(e3, e);

  // Rewrite an entry
  CommitEntry e4 = {e3.prep_seq + size, e3.commit_seq + size + 1};
  evicted = wp_db->AddCommitEntry(e4.prep_seq % size, e4, &e);
  ASSERT_TRUE(evicted);
  ASSERT_EQ(e3, e);
  found = wp_db->GetCommitEntry(e4.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(e4, e);
}

TEST_P(WritePreparedTransactionTest, MaybeUpdateOldCommitMap) {
  // If prepare <= snapshot < commit we should keep the entry around since its
  // nonexistence could be interpreted as committed in the snapshot while it is
  // not true. We keep such entries around by adding them to the
  // old_commit_map_.
  uint64_t p /*prepare*/, c /*commit*/, s /*snapshot*/, ns /*next_snapshot*/;
  p = 10l, c = 15l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  // If we do not expect the old commit map to be updated, try also with a next
  // snapshot that is expected to update the old commit map. This would test
  // that MaybeUpdateOldCommitMap would not prevent us from checking the next
  // snapshot that must be checked.
  p = 10l, c = 15l, s = 20l, ns = 11l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);

  p = 10l, c = 20l, s = 20l, ns = 19l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  p = 10l, c = 20l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);

  p = 20l, c = 20l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  p = 20l, c = 20l, s = 20l, ns = 19l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);

  p = 10l, c = 25l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, true);

  p = 20l, c = 25l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, true);

  p = 21l, c = 25l, s = 20l, ns = 22l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  p = 21l, c = 25l, s = 20l, ns = 19l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
}

TEST_P(WritePreparedTransactionTest, CheckAgainstSnapshotsTest) {
  std::vector<SequenceNumber> snapshots = {100l, 200l, 300l, 400l, 500l,
                                           600l, 700l, 800l, 900l};
  const size_t snapshot_cache_bits = 2;
  // Safety check to express the intended size in the test. Can be adjusted if
  // the snapshots lists changed.
  assert((1ul << snapshot_cache_bits) * 2 + 1 == snapshots.size());
  DBImpl* mock_db = new DBImpl(options, dbname);
  std::unique_ptr<WritePreparedTxnDBMock> wp_db(
      new WritePreparedTxnDBMock(mock_db, txn_db_options, snapshot_cache_bits));
  SequenceNumber version = 1000l;
  ASSERT_EQ(0, wp_db->snapshots_total_);
  wp_db->UpdateSnapshots(snapshots, version);
  ASSERT_EQ(snapshots.size(), wp_db->snapshots_total_);
  // seq numbers are chosen so that we have two of them between each two
  // snapshots. If the diff of two consecuitive seq is more than 5, there is a
  // snapshot between them.
  std::vector<SequenceNumber> seqs = {50l,  55l,  150l, 155l, 250l, 255l, 350l,
                                      355l, 450l, 455l, 550l, 555l, 650l, 655l,
                                      750l, 755l, 850l, 855l, 950l, 955l};
  assert(seqs.size() > 1);
  for (size_t i = 0; i < seqs.size() - 1; i++) {
    wp_db->old_commit_map_empty_ = true;  // reset
    CommitEntry commit_entry = {seqs[i], seqs[i + 1]};
    wp_db->CheckAgainstSnapshots(commit_entry);
    // Expect update if there is snapshot in between the prepare and commit
    bool expect_update = commit_entry.commit_seq - commit_entry.prep_seq > 5 &&
                         commit_entry.commit_seq >= snapshots.front() &&
                         commit_entry.prep_seq <= snapshots.back();
    ASSERT_EQ(expect_update, !wp_db->old_commit_map_empty_);
  }
}

// Return true if the ith bit is set in combination represented by comb
bool IsInCombination(size_t i, size_t comb) { return comb & (size_t(1) << i); }

// This test is too slow for travis
#ifndef TRAVIS
// Test that CheckAgainstSnapshots will not miss a live snapshot if it is run in
// parallel with UpdateSnapshots.
TEST_P(WritePreparedTransactionTest, SnapshotConcurrentAccessTest) {
  // We have a sync point in the method under test after checking each snapshot.
  // If you increase the max number of snapshots in this test, more sync points
  // in the methods must also be added.
  const std::vector<SequenceNumber> snapshots = {10l, 20l, 30l, 40l, 50l,
                                                 60l, 70l, 80l, 90l, 100l};
  const size_t snapshot_cache_bits = 2;
  // Safety check to express the intended size in the test. Can be adjusted if
  // the snapshots lists changed.
  assert((1ul << snapshot_cache_bits) * 2 + 2 == snapshots.size());
  SequenceNumber version = 1000l;
  // Choose the cache size so that the new snapshot list could replace all the
  // existing items in the cache and also have some overflow.
  DBImpl* mock_db = new DBImpl(options, dbname);
  std::unique_ptr<WritePreparedTxnDBMock> wp_db(
      new WritePreparedTxnDBMock(mock_db, txn_db_options, snapshot_cache_bits));
  // Add up to 2 items that do not fit into the cache
  for (size_t old_size = 1; old_size <= wp_db->SNAPSHOT_CACHE_SIZE + 2;
       old_size++) {
    const std::vector<SequenceNumber> old_snapshots(
        snapshots.begin(), snapshots.begin() + old_size);

    // Each member of old snapshot might or might not appear in the new list. We
    // create a common_snapshots for each combination.
    size_t new_comb_cnt = size_t(1) << old_size;
    for (size_t new_comb = 0; new_comb < new_comb_cnt; new_comb++) {
      printf(".");  // To signal progress
      fflush(stdout);
      std::vector<SequenceNumber> common_snapshots;
      for (size_t i = 0; i < old_snapshots.size(); i++) {
        if (IsInCombination(i, new_comb)) {
          common_snapshots.push_back(old_snapshots[i]);
        }
      }
      // And add some new snapshots to the common list
      for (size_t added_snapshots = 0;
           added_snapshots <= snapshots.size() - old_snapshots.size();
           added_snapshots++) {
        std::vector<SequenceNumber> new_snapshots = common_snapshots;
        for (size_t i = 0; i < added_snapshots; i++) {
          new_snapshots.push_back(snapshots[old_snapshots.size() + i]);
        }
        for (auto it = common_snapshots.begin(); it != common_snapshots.end();
             it++) {
          auto snapshot = *it;
          // Create a commit entry that is around the snapshot and thus should
          // be not be discarded
          CommitEntry entry = {static_cast<uint64_t>(snapshot - 1),
                               snapshot + 1};
          // The critical part is when iterating the snapshot cache. Afterwards,
          // we are operating under the lock
          size_t a_range =
              std::min(old_snapshots.size(), wp_db->SNAPSHOT_CACHE_SIZE) + 1;
          size_t b_range =
              std::min(new_snapshots.size(), wp_db->SNAPSHOT_CACHE_SIZE) + 1;
          // Break each thread at two points
          for (size_t a1 = 1; a1 <= a_range; a1++) {
            for (size_t a2 = a1 + 1; a2 <= a_range; a2++) {
              for (size_t b1 = 1; b1 <= b_range; b1++) {
                for (size_t b2 = b1 + 1; b2 <= b_range; b2++) {
                  SnapshotConcurrentAccessTestInternal(
                      wp_db.get(), old_snapshots, new_snapshots, entry, version,
                      a1, a2, b1, b2);
                }
              }
            }
          }
        }
      }
    }
  }
  printf("\n");
}
#endif

// This test clarifies the contract of AdvanceMaxEvictedSeq method
TEST_P(WritePreparedTransactionTest, AdvanceMaxEvictedSeqBasicTest) {
  DBImpl* mock_db = new DBImpl(options, dbname);
  std::unique_ptr<WritePreparedTxnDBMock> wp_db(
      new WritePreparedTxnDBMock(mock_db, txn_db_options));

  // 1. Set the initial values for max, prepared, and snapshots
  SequenceNumber zero_max = 0l;
  // Set the initial list of prepared txns
  const std::vector<SequenceNumber> initial_prepared = {10,  30,  50, 100,
                                                        150, 200, 250};
  for (auto p : initial_prepared) {
    wp_db->AddPrepared(p);
  }
  // This updates the max value and also set old prepared
  SequenceNumber init_max = 100;
  wp_db->AdvanceMaxEvictedSeq(zero_max, init_max);
  const std::vector<SequenceNumber> initial_snapshots = {20, 40};
  wp_db->SetDBSnapshots(initial_snapshots);
  // This will update the internal cache of snapshots from the DB
  wp_db->UpdateSnapshots(initial_snapshots, init_max);

  // 2. Invoke AdvanceMaxEvictedSeq
  const std::vector<SequenceNumber> latest_snapshots = {20, 110, 220, 300};
  wp_db->SetDBSnapshots(latest_snapshots);
  SequenceNumber new_max = 200;
  wp_db->AdvanceMaxEvictedSeq(init_max, new_max);

  // 3. Verify that the state matches with AdvanceMaxEvictedSeq contract
  // a. max should be updated to new_max
  ASSERT_EQ(wp_db->max_evicted_seq_, new_max);
  // b. delayed prepared should contain every txn <= max and prepared should
  // only contian txns > max
  auto it = initial_prepared.begin();
  for (; it != initial_prepared.end() && *it <= new_max; it++) {
    ASSERT_EQ(1, wp_db->delayed_prepared_.erase(*it));
  }
  ASSERT_TRUE(wp_db->delayed_prepared_.empty());
  for (; it != initial_prepared.end() && !wp_db->prepared_txns_.empty();
       it++, wp_db->prepared_txns_.pop()) {
    ASSERT_EQ(*it, wp_db->prepared_txns_.top());
  }
  ASSERT_TRUE(it == initial_prepared.end());
  ASSERT_TRUE(wp_db->prepared_txns_.empty());
  // c. snapshots should contain everything below new_max
  auto sit = latest_snapshots.begin();
  for (size_t i = 0; sit != latest_snapshots.end() && *sit <= new_max &&
                     i < wp_db->snapshots_total_;
       sit++, i++) {
    ASSERT_TRUE(i < wp_db->snapshots_total_);
    // This test is in small scale and the list of snapshots are assumed to be
    // within the cache size limit. This is just a safety check to double check
    // that assumption.
    ASSERT_TRUE(i < wp_db->SNAPSHOT_CACHE_SIZE);
    ASSERT_EQ(*sit, wp_db->snapshot_cache_[i]);
  }
}

// This test clarfies the existing expectation from the sequence number
// algorithm. It could detect mistakes in updating the code but it is not
// necessarily the one acceptable way. If the algorithm is legitimately changed,
// this unit test should be updated as well.
TEST_P(WritePreparedTransactionTest, SeqAdvanceTest) {
  auto pdb = reinterpret_cast<PessimisticTransactionDB*>(db);
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  auto seq = db_impl->GetLatestSequenceNumber();
  auto exp_seq = seq;

  // Test DB's internal txn. It involves no prepare phase nor a commit marker.
  WriteOptions wopts;
  auto s = db->Put(wopts, "key", "value");
  // Consume one seq per batch
  exp_seq++;
  ASSERT_OK(s);
  seq = db_impl->GetLatestSequenceNumber();
  ASSERT_EQ(exp_seq, seq);

  // Doing it twice might detect some bugs
  s = db->Put(wopts, "key", "value");
  exp_seq++;
  ASSERT_OK(s);
  seq = db_impl->GetLatestSequenceNumber();
  ASSERT_EQ(exp_seq, seq);

  // Testing directly writing a write batch. Functionality-wise it is equivalent
  // to commit without prepare.
  WriteBatch wb;
  wb.Put("k1", "v1");
  wb.Put("k2", "v2");
  wb.Put("k3", "v3");
  s = pdb->Write(wopts, &wb);
  // Consume one seq per batch
  exp_seq++;
  ASSERT_OK(s);
  seq = db_impl->GetLatestSequenceNumber();
  ASSERT_EQ(exp_seq, seq);

  // A full 2pc txn that also involves a commit marker.
  TransactionOptions txn_options;
  WriteOptions write_options;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid");
  ASSERT_OK(s);
  s = txn->Put(Slice("foo"), Slice("bar"));
  s = txn->Put(Slice("foo2"), Slice("bar2"));
  s = txn->Put(Slice("foo3"), Slice("bar3"));
  s = txn->Put(Slice("foo4"), Slice("bar4"));
  s = txn->Put(Slice("foo5"), Slice("bar5"));
  ASSERT_OK(s);
  s = txn->Prepare();
  ASSERT_OK(s);
  // Consume one seq per batch
  exp_seq++;
  s = txn->Commit();
  ASSERT_OK(s);
  // Consume one seq per commit marker
  exp_seq++;
  // Since commit marker does not write to memtable, the last seq number is not
  // updated immedaitely. But the advance should be visible after the next
  // write.

  s = db->Put(wopts, "key", "value");
  // Consume one seq per batch
  exp_seq++;
  ASSERT_OK(s);
  seq = db_impl->GetLatestSequenceNumber();
  ASSERT_EQ(exp_seq, seq);
  delete txn;

  // Commit without prepare. It shoudl write to DB without a commit marker.
  txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid2");
  ASSERT_OK(s);
  s = txn->Put(Slice("foo"), Slice("bar"));
  s = txn->Put(Slice("foo2"), Slice("bar2"));
  s = txn->Put(Slice("foo3"), Slice("bar3"));
  s = txn->Put(Slice("foo4"), Slice("bar4"));
  s = txn->Put(Slice("foo5"), Slice("bar5"));
  ASSERT_OK(s);
  s = txn->Commit();
  ASSERT_OK(s);
  // Consume one seq per batch
  exp_seq++;
  seq = db_impl->GetLatestSequenceNumber();
  ASSERT_EQ(exp_seq, seq);
  pdb->UnregisterTransaction(txn);
  delete txn;
}

// Test WritePreparedTxnDB's IsInSnapshot against different ordering of
// snapshot, max_committed_seq_, prepared, and commit entries.
TEST_P(WritePreparedTransactionTest, IsInSnapshotTest) {
  WriteOptions wo;
  // Use small commit cache to trigger lots of eviction and fast advance of
  // max_evicted_seq_
  const size_t commit_cache_bits = 3;
  // Same for snapshot cache size
  const size_t snapshot_cache_bits = 2;

  // Take some preliminary snapshots first. This is to stress the data structure
  // that holds the old snapshots as it will be designed to be efficient when
  // only a few snapshots are below the max_evicted_seq_.
  for (int max_snapshots = 1; max_snapshots < 20; max_snapshots++) {
    // Leave some gap between the preliminary snapshots and the final snapshot
    // that we check. This should test for also different overlapping scnearios
    // between the last snapshot and the commits.
    for (int max_gap = 1; max_gap < 10; max_gap++) {
      // Since we do not actually write to db, we mock the seq as it would be
      // increaased by the db. The only exception is that we need db seq to
      // advance for our snapshots. for which we apply a dummy put each time we
      // increase our mock of seq.
      uint64_t seq = 0;
      // At each step we prepare a txn and then we commit it in the next txn.
      // This emulates the consecuitive transactions that write to the same key
      uint64_t cur_txn = 0;
      // Number of snapshots taken so far
      int num_snapshots = 0;
      // Number of gaps applied so far
      int gap_cnt = 0;
      // The final snapshot that we will inspect
      uint64_t snapshot = 0;
      bool found_committed = false;
      // To stress the data structure that maintain prepared txns, at each cycle
      // we add a new prepare txn. These do not mean to be committed for
      // snapshot inspection.
      std::set<uint64_t> prepared;
      // We keep the list of txns comitted before we take the last snaphot.
      // These should be the only seq numbers that will be found in the snapshot
      std::set<uint64_t> committed_before;
      DBImpl* mock_db = new DBImpl(options, dbname);
      std::unique_ptr<WritePreparedTxnDBMock> wp_db(new WritePreparedTxnDBMock(
          mock_db, txn_db_options, snapshot_cache_bits, commit_cache_bits));
      // We continue until max advances a bit beyond the snapshot.
      while (!snapshot || wp_db->max_evicted_seq_ < snapshot + 100) {
        // do prepare for a transaction
        seq++;
        wp_db->AddPrepared(seq);
        prepared.insert(seq);

        // If cur_txn is not started, do prepare for it.
        if (!cur_txn) {
          seq++;
          cur_txn = seq;
          wp_db->AddPrepared(cur_txn);
        } else {                                     // else commit it
          seq++;
          wp_db->AddCommitted(cur_txn, seq);
          if (!snapshot) {
            committed_before.insert(cur_txn);
          }
          cur_txn = 0;
        }

        if (num_snapshots < max_snapshots - 1) {
          // Take preliminary snapshots
          wp_db->TakeSnapshot(seq);
          num_snapshots++;
        } else if (gap_cnt < max_gap) {
          // Wait for some gap before taking the final snapshot
          gap_cnt++;
        } else if (!snapshot) {
          // Take the final snapshot if it is not already taken
          snapshot = seq;
          wp_db->TakeSnapshot(snapshot);
          num_snapshots++;
        }

        // If the snapshot is taken, verify seq numbers visible to it. We redo
        // it at each cycle to test that the system is still sound when
        // max_evicted_seq_ advances.
        if (snapshot) {
          for (uint64_t s = 0; s <= seq; s++) {
            bool was_committed =
                (committed_before.find(s) != committed_before.end());
            bool is_in_snapshot = wp_db->IsInSnapshot(s, snapshot);
            if (was_committed != is_in_snapshot) {
              printf("max_snapshots %d max_gap %d seq %" PRIu64 " max %" PRIu64
                     " snapshot %" PRIu64
                     " gap_cnt %d num_snapshots %d s %" PRIu64 "\n",
                     max_snapshots, max_gap, seq,
                     wp_db->max_evicted_seq_.load(), snapshot, gap_cnt,
                     num_snapshots, s);
            }
            ASSERT_EQ(was_committed, is_in_snapshot);
            found_committed = found_committed || is_in_snapshot;
          }
        }
      }
      // Safety check to make sure the test actually ran
      ASSERT_TRUE(found_committed);
      // As an extra check, check if prepared set will be properly empty after
      // they are committed.
      if (cur_txn) {
        wp_db->AddCommitted(cur_txn, seq);
      }
      for (auto p : prepared) {
        wp_db->AddCommitted(p, seq);
      }
      ASSERT_TRUE(wp_db->delayed_prepared_.empty());
      ASSERT_TRUE(wp_db->prepared_txns_.empty());
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr,
          "SKIPPED as Transactions are not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
