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

typedef PessimisticTransactionDB::CommitEntry CommitEntry;

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
};

static const TxnDBWritePolicy wp = WRITE_PREPARED;
INSTANTIATE_TEST_CASE_P(WritePreparedTransactionTest,
                        WritePreparedTransactionTest,
                        ::testing::Values(std::make_tuple(false, true, wp)));

TEST_P(WritePreparedTransactionTest, CommitMapTest) {
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  assert(wp_db);
  assert(wp_db->db_impl_);
  size_t size = wp_db->COMMIT_CACHE_SIZE;
  CommitEntry c = {5, 12}, e;
  bool evicted = wp_db->AddCommitEntry(c.prep_seq % size, c, &e);
  ASSERT_FALSE(evicted);

  // Should be able to read the same value
  bool found = wp_db->GetCommitEntry(c.prep_seq % size, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(c, e);
  // Should be able to distinguish between overlapping entries
  found = wp_db->GetCommitEntry((c.prep_seq + size) % size, &e);
  ASSERT_TRUE(found);
  ASSERT_NE(c.prep_seq + size, e.prep_seq);
  // Should be able to detect non-existent entry
  found = wp_db->GetCommitEntry((c.prep_seq + 1) % size, &e);
  ASSERT_EQ(e.commit_seq, 0);
  ASSERT_FALSE(found);

  // Reject an invalid exchange
  CommitEntry e2 = {c.prep_seq + size, c.commit_seq};
  bool exchanged = wp_db->ExchangeCommitEntry(e2.prep_seq % size, e2, e);
  ASSERT_FALSE(exchanged);
  // check whether it did actually reject that
  found = wp_db->GetCommitEntry(e2.prep_seq % size, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(c, e);

  // Accept a valid exchange
  CommitEntry e3 = {c.prep_seq + size, c.commit_seq + size + 1};
  exchanged = wp_db->ExchangeCommitEntry(c.prep_seq % size, c, e3);
  ASSERT_TRUE(exchanged);
  // check whether it did actually accepted that
  found = wp_db->GetCommitEntry(c.prep_seq % size, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(e3, e);

  // Rewrite an entry
  CommitEntry e4 = {e3.prep_seq + size, e4.commit_seq + size + 1};
  evicted = wp_db->AddCommitEntry(e4.prep_seq % size, e4, &e);
  ASSERT_TRUE(evicted);
  ASSERT_EQ(e3, e);
  found = wp_db->GetCommitEntry(e4.prep_seq % size, &e);
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

// Test WritePreparedTxnDB's IsInSnapshot against different ordering of
// snapshot, max_committed_seq_, prepared, and commit entries.
TEST_P(WritePreparedTransactionTest, IsInSnapshotTest) {
  WriteOptions wo;
  // Use small commit cache to trigger lots of eviction and fast advance of
  // max_evicted_seq_
  // will take effect after ReOpen
  WritePreparedTxnDB::DEF_COMMIT_CACHE_SIZE = 8;
  // Same for snapshot cache size
  WritePreparedTxnDB::DEF_SNAPSHOT_CACHE_SIZE = 5;

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
      std::vector<const Snapshot*> to_be_released;
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
      ReOpen();  // to restart the db
      WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
      assert(wp_db);
      assert(wp_db->db_impl_);
      // We continue until max advances a bit beyond the snapshot.
      while (!snapshot || wp_db->max_evicted_seq_ < snapshot + 100) {
        // do prepare for a transaction
        wp_db->db_impl_->Put(wo, "key", "value");  // dummy put to inc db seq
        seq++;
        ASSERT_EQ(wp_db->db_impl_->GetLatestSequenceNumber(), seq);
        wp_db->AddPrepared(seq);
        prepared.insert(seq);

        // If cur_txn is not started, do prepare for it.
        if (!cur_txn) {
          wp_db->db_impl_->Put(wo, "key", "value");  // dummy put to inc db seq
          seq++;
          ASSERT_EQ(wp_db->db_impl_->GetLatestSequenceNumber(), seq);
          cur_txn = seq;
          wp_db->AddPrepared(cur_txn);
        } else {                                     // else commit it
          wp_db->db_impl_->Put(wo, "key", "value");  // dummy put to inc db seq
          seq++;
          ASSERT_EQ(wp_db->db_impl_->GetLatestSequenceNumber(), seq);
          wp_db->AddCommitted(cur_txn, seq);
          if (!snapshot) {
            committed_before.insert(cur_txn);
          }
          cur_txn = 0;
        }

        if (num_snapshots < max_snapshots - 1) {
          // Take preliminary snapshots
          auto tmp_snapshot = db->GetSnapshot();
          to_be_released.push_back(tmp_snapshot);
          num_snapshots++;
        } else if (gap_cnt < max_gap) {
          // Wait for some gap before taking the final snapshot
          gap_cnt++;
        } else if (!snapshot) {
          // Take the final snapshot if it is not already taken
          auto tmp_snapshot = db->GetSnapshot();
          to_be_released.push_back(tmp_snapshot);
          snapshot = tmp_snapshot->GetSequenceNumber();
          // We increase the db seq artificailly by a dummy Put. Check that this
          // technique is effective and db seq is that same as ours.
          ASSERT_EQ(snapshot, seq);
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
      for (auto s : to_be_released) {
        db->ReleaseSnapshot(s);
      }
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
