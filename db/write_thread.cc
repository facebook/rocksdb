//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/write_thread.h"
#include <chrono>
#include <limits>
#include <thread>
#include "db/column_family.h"
#include "port/port.h"
#include "util/sync_point.h"

namespace rocksdb {

WriteThread::WriteThread(uint64_t max_yield_usec, uint64_t slow_yield_usec)
    : max_yield_usec_(max_yield_usec),
      slow_yield_usec_(slow_yield_usec),
      newest_writer_(nullptr) {}

void WriteThread::LinkOne(Writer* w, bool* linked_as_leader) {
  assert(w->state == Writer::STATE_INIT);

  while (true) {
    Writer* writers = newest_writer_.load(std::memory_order_relaxed);
    w->link_older = writers;
    if (newest_writer_.compare_exchange_strong(writers, w)) {
      if (writers == nullptr) {
        // this isn't part of the WriteThread machinery, but helps with
        // debugging and is checked by an assert in WriteImpl
        w->state.store(Writer::STATE_GROUP_LEADER, std::memory_order_relaxed);
      }
      *linked_as_leader = (writers == nullptr);
      return;
    }
  }
}

void WriteThread::CreateMissingNewerLinks(Writer* head) {
  while (true) {
    Writer* next = head->link_older;
    if (next == nullptr || next->link_newer != nullptr) {
      assert(next == nullptr || next->link_newer == head);
      break;
    }
    next->link_newer = head;
    head = next;
  }
}

void WriteThread::JoinBatchGroup(Writer* w) {
  static AdaptationContext ctx("JoinBatchGroup");

  assert(w->batch != nullptr);
  bool linked_as_leader;
  LinkOne(w, &linked_as_leader);

  TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:Wait", w);

  if (!linked_as_leader) {
    AwaitState(w, Writer::STATE_GROUP_LEADER | Writer::STATE_PARALLEL_FOLLOWER |
                      Writer::STATE_COMPLETED,
               &ctx);
    TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:DoneWaiting", w);
  }
}

size_t WriteThread::EnterAsBatchGroupLeader(
    Writer* leader, Writer** last_writer,
    autovector<Writer*>* write_batch_group) {
  assert(leader->link_older == nullptr);
  assert(leader->batch != nullptr);

  size_t size = WriteBatchInternal::ByteSize(leader->batch);
  write_batch_group->push_back(leader);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = leader;

  Writer* newest_writer = newest_writer_.load(std::memory_order_acquire);

  // This is safe regardless of any db mutex status of the caller. Previous
  // calls to ExitAsGroupLeader either didn't call CreateMissingNewerLinks
  // (they emptied the list and then we added ourself as leader) or had to
  // explicitly wake us up (the list was non-empty when we added ourself,
  // so we have already received our MarkJoined).
  CreateMissingNewerLinks(newest_writer);

  // Tricky. Iteration start (leader) is exclusive and finish
  // (newest_writer) is inclusive. Iteration goes from old to new.
  Writer* w = leader;
  while (w != newest_writer) {
    w = w->link_newer;

    if (w->sync && !leader->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->no_slowdown != leader->no_slowdown) {
      // Do not mix writes that are ok with delays with the ones that
      // request fail on delays.
      break;
    }

    if (!w->disable_wal && leader->disable_wal) {
      // Do not include a write that needs WAL into a batch that has
      // WAL disabled.
      break;
    }

    if (w->batch == nullptr) {
      // Do not include those writes with nullptr batch. Those are not writes,
      // those are something else. They want to be alone
      break;
    }

    if (w->callback != nullptr && !w->callback->AllowWriteBatching()) {
      // dont batch writes that don't want to be batched
      break;
    }

    auto batch_size = WriteBatchInternal::ByteSize(w->batch);
    if (size + batch_size > max_size) {
      // Do not make batch too big
      break;
    }

    size += batch_size;
    write_batch_group->push_back(w);
    w->in_batch_group = true;
    *last_writer = w;
  }
  return size;
}

void WriteThread::LaunchParallelFollowers(ParallelGroup* pg,
                                          SequenceNumber sequence) {
  // EnterAsBatchGroupLeader already created the links from leader to
  // newer writers in the group

  pg->leader->parallel_group = pg;

  Writer* w = pg->leader;
  w->sequence = sequence;

  while (w != pg->last_writer) {
    // Writers that won't write don't get sequence allotment
    if (!w->CallbackFailed() && w->ShouldWriteToMemtable()) {
      sequence += WriteBatchInternal::Count(w->batch);
    }
    w = w->link_newer;

    w->sequence = sequence;
    w->parallel_group = pg;
    w->SetState(Writer::STATE_PARALLEL_FOLLOWER);
  }
}

bool WriteThread::CompleteParallelWorker(Writer* w) {
  static AdaptationContext ctx("CompleteParallelWorker");

  auto* pg = w->parallel_group;
  if (!w->status.ok()) {
    std::lock_guard<std::mutex> guard(pg->leader->StateMutex());
    pg->status = w->status;
  }

  auto leader = pg->leader;
  auto early_exit_allowed = pg->early_exit_allowed;

  if (pg->running.load(std::memory_order_acquire) > 1 && pg->running-- > 1) {
    // we're not the last one
    AwaitState(w, Writer::STATE_COMPLETED, &ctx);

    // Caller only needs to perform exit duties if early exit doesn't
    // apply and this is the leader.  Can't touch pg here.  Whoever set
    // our state to STATE_COMPLETED copied pg->status to w.status for us.
    return w == leader && !(early_exit_allowed && w->status.ok());
  }
  // else we're the last parallel worker

  if (w == leader || (early_exit_allowed && pg->status.ok())) {
    // this thread should perform exit duties
    w->status = pg->status;
    return true;
  } else {
    // We're the last parallel follower but early commit is not
    // applicable.  Wake up the leader and then wait for it to exit.
    assert(w->state == Writer::STATE_PARALLEL_FOLLOWER);
    leader->SetState(Writer::STATE_COMPLETED);
    AwaitState(w, Writer::STATE_COMPLETED, &ctx);
    return false;
  }
}

void WriteThread::EarlyExitParallelGroup(Writer* w) {
  auto* pg = w->parallel_group;

  assert(w->state == Writer::STATE_PARALLEL_FOLLOWER);
  assert(pg->status.ok());
  ExitAsBatchGroupLeader(pg->leader, pg->last_writer, pg->status);
  assert(w->status.ok());
  assert(w->state == Writer::STATE_COMPLETED);
  pg->leader->SetState(Writer::STATE_COMPLETED);
}

void WriteThread::ExitAsBatchGroupLeader(Writer* leader, Writer* last_writer,
                                         Status status) {
  assert(leader->link_older == nullptr);

  Writer* head = newest_writer_.load(std::memory_order_acquire);
  if (head != last_writer ||
      !newest_writer_.compare_exchange_strong(head, nullptr)) {
    // Either w wasn't the head during the load(), or it was the head
    // during the load() but somebody else pushed onto the list before
    // we did the compare_exchange_strong (causing it to fail).  In the
    // latter case compare_exchange_strong has the effect of re-reading
    // its first param (head).  No need to retry a failing CAS, because
    // only a departing leader (which we are at the moment) can remove
    // nodes from the list.
    assert(head != last_writer);

    // After walking link_older starting from head (if not already done)
    // we will be able to traverse w->link_newer below. This function
    // can only be called from an active leader, only a leader can
    // clear newest_writer_, we didn't, and only a clear newest_writer_
    // could cause the next leader to start their work without a call
    // to MarkJoined, so we can definitely conclude that no other leader
    // work is going on here (with or without db mutex).
    CreateMissingNewerLinks(head);
    assert(last_writer->link_newer->link_older == last_writer);
    last_writer->link_newer->link_older = nullptr;

    // Next leader didn't self-identify, because newest_writer_ wasn't
    // nullptr when they enqueued (we were definitely enqueued before them
    // and are still in the list).  That means leader handoff occurs when
    // we call MarkJoined
    last_writer->link_newer->SetState(Writer::STATE_GROUP_LEADER);
  }
  // else nobody else was waiting, although there might already be a new
  // leader now

  while (last_writer != leader) {
    last_writer->status = status;
    // we need to read link_older before calling SetState, because as soon
    // as it is marked committed the other thread's Await may return and
    // deallocate the Writer.
    auto next = last_writer->link_older;
    last_writer->SetState(Writer::STATE_COMPLETED);

    last_writer = next;
  }
}

uint8_t WriteThread::AwaitState(Writer* w, uint8_t goal_mask,
                                AdaptationContext* ctx) {
  return w->AwaitState(goal_mask, ctx, max_yield_usec_, slow_yield_usec_);
}

void WriteThread::EnterUnbatched(Writer* w, InstrumentedMutex* mu) {
  static AdaptationContext ctx("EnterUnbatched");

  assert(w->batch == nullptr);
  bool linked_as_leader;
  LinkOne(w, &linked_as_leader);
  if (!linked_as_leader) {
    mu->Unlock();
    TEST_SYNC_POINT("WriteThread::EnterUnbatched:Wait");
    AwaitState(w, Writer::STATE_GROUP_LEADER, &ctx);
    mu->Lock();
  }
}

void WriteThread::ExitUnbatched(Writer* w) {
  Status dummy_status;
  ExitAsBatchGroupLeader(w, w, dummy_status);
}

}  // namespace rocksdb
