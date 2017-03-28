//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <assert.h>
#include <stdint.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <type_traits>
#include <vector>

#include "db/write_callback.h"
#include "db/writer.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/autovector.h"
#include "util/instrumented_mutex.h"

namespace rocksdb {

class WriteThread {
 public:
  WriteThread(uint64_t max_yield_usec, uint64_t slow_yield_usec);

  // IMPORTANT: None of the methods in this class rely on the db mutex
  // for correctness. All of the methods except JoinBatchGroup and
  // EnterUnbatched may be called either with or without the db mutex held.
  // Correctness is maintained by ensuring that only a single thread is
  // a leader at a time.

  // Registers w as ready to become part of a batch group, waits until the
  // caller should perform some work, and returns the current state of the
  // writer.  If w has become the leader of a write batch group, returns
  // STATE_GROUP_LEADER.  If w has been made part of a sequential batch
  // group and the leader has performed the write, returns STATE_DONE.
  // If w has been made part of a parallel batch group and is responsible
  // for updating the memtable, returns STATE_PARALLEL_FOLLOWER.
  //
  // The db mutex SHOULD NOT be held when calling this function, because
  // it will block.
  //
  // Writer* w:        Writer to be executed as part of a batch group
  void JoinBatchGroup(Writer* w);

  // Constructs a write batch group led by leader, which should be a
  // Writer passed to JoinBatchGroup on the current thread.
  //
  // Writer* leader:         Writer that is STATE_GROUP_LEADER
  // Writer** last_writer:   Out-param that identifies the last follower
  // autovector<WriteBatch*>* write_batch_group: Out-param of group members
  // returns:                Total batch group byte size
  size_t EnterAsBatchGroupLeader(Writer* leader, Writer** last_writer,
                                 autovector<Writer*>* write_batch_group);

  // Causes JoinBatchGroup to return STATE_PARALLEL_FOLLOWER for all of the
  // non-leader members of this write batch group.  Sets Writer::sequence
  // before waking them up.
  //
  // ParallalGroup* pg:       Extra state used to coordinate the parallel add
  // SequenceNumber sequence: Starting sequence number to assign to Writer-s
  void LaunchParallelFollowers(ParallelGroup* pg, SequenceNumber sequence);

  // Reports the completion of w's batch to the parallel group leader, and
  // waits for the rest of the parallel batch to complete.  Returns true
  // if this thread is the last to complete, and hence should advance
  // the sequence number and then call EarlyExitParallelGroup, false if
  // someone else has already taken responsibility for that.
  bool CompleteParallelWorker(Writer* w);

  // This method performs an early completion of a parallel write group,
  // where the cleanup work of the leader is performed by a follower who
  // happens to be the last parallel worker to complete.
  void EarlyExitParallelGroup(Writer* w);

  // Unlinks the Writer-s in a batch group, wakes up the non-leaders,
  // and wakes up the next leader (if any).
  //
  // Writer* leader:         From EnterAsBatchGroupLeader
  // Writer* last_writer:    Value of out-param of EnterAsBatchGroupLeader
  // Status status:          Status of write operation
  void ExitAsBatchGroupLeader(Writer* leader, Writer* last_writer,
                              Status status);

  // Waits for all preceding writers (unlocking mu while waiting), then
  // registers w as the currently proceeding writer.
  //
  // Writer* w:              A Writer not eligible for batching
  // InstrumentedMutex* mu:  The db mutex, to unlock while waiting
  // REQUIRES: db mutex held
  void EnterUnbatched(Writer* w, InstrumentedMutex* mu);

  // Completes a Writer begun with EnterUnbatched, unblocking subsequent
  // writers.
  void ExitUnbatched(Writer* w);

 private:
  uint64_t max_yield_usec_;
  uint64_t slow_yield_usec_;

  // Points to the newest pending Writer.  Only leader can remove
  // elements, adding can be done lock-free by anybody
  std::atomic<Writer*> newest_writer_;

  uint8_t AwaitState(Writer* w, uint8_t goal_mask, AdaptationContext* ctx);

  // Links w into the newest_writer_ list. Sets *linked_as_leader to
  // true if w was linked directly into the leader position.  Safe to
  // call from multiple threads without external locking.
  void LinkOne(Writer* w, bool* linked_as_leader);

  // Computes any missing link_newer links.  Should not be called
  // concurrently with itself.
  void CreateMissingNewerLinks(Writer* head);
};

}  // namespace rocksdb
