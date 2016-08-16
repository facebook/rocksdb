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

uint8_t WriteThread::BlockingAwaitState(Writer* w, uint8_t goal_mask) {
  // We're going to block.  Lazily create the mutex.  We guarantee
  // propagation of this construction to the waker via the
  // STATE_LOCKED_WAITING state.  The waker won't try to touch the mutex
  // or the condvar unless they CAS away the STATE_LOCKED_WAITING that
  // we install below.
  w->CreateMutex();

  auto state = w->state.load(std::memory_order_acquire);
  assert(state != STATE_LOCKED_WAITING);
  if ((state & goal_mask) == 0 &&
      w->state.compare_exchange_strong(state, STATE_LOCKED_WAITING)) {
    // we have permission (and an obligation) to use StateMutex
    std::unique_lock<std::mutex> guard(w->StateMutex());
    w->StateCV().wait(guard, [w] {
      return w->state.load(std::memory_order_relaxed) != STATE_LOCKED_WAITING;
    });
    state = w->state.load(std::memory_order_relaxed);
  }
  // else tricky.  Goal is met or CAS failed.  In the latter case the waker
  // must have changed the state, and compare_exchange_strong has updated
  // our local variable with the new one.  At the moment WriteThread never
  // waits for a transition across intermediate states, so we know that
  // since a state change has occurred the goal must have been met.
  assert((state & goal_mask) != 0);
  return state;
}

uint8_t WriteThread::AwaitState(Writer* w, uint8_t goal_mask,
                                AdaptationContext* ctx) {
  uint8_t state;

  // On a modern Xeon each loop takes about 7 nanoseconds (most of which
  // is the effect of the pause instruction), so 200 iterations is a bit
  // more than a microsecond.  This is long enough that waits longer than
  // this can amortize the cost of accessing the clock and yielding.
  for (uint32_t tries = 0; tries < 200; ++tries) {
    state = w->state.load(std::memory_order_acquire);
    if ((state & goal_mask) != 0) {
      return state;
    }
    port::AsmVolatilePause();
  }

  // If we're only going to end up waiting a short period of time,
  // it can be a lot more efficient to call std::this_thread::yield()
  // in a loop than to block in StateMutex().  For reference, on my 4.0
  // SELinux test server with support for syscall auditing enabled, the
  // minimum latency between FUTEX_WAKE to returning from FUTEX_WAIT is
  // 2.7 usec, and the average is more like 10 usec.  That can be a big
  // drag on RockDB's single-writer design.  Of course, spinning is a
  // bad idea if other threads are waiting to run or if we're going to
  // wait for a long time.  How do we decide?
  //
  // We break waiting into 3 categories: short-uncontended,
  // short-contended, and long.  If we had an oracle, then we would always
  // spin for short-uncontended, always block for long, and our choice for
  // short-contended might depend on whether we were trying to optimize
  // RocksDB throughput or avoid being greedy with system resources.
  //
  // Bucketing into short or long is easy by measuring elapsed time.
  // Differentiating short-uncontended from short-contended is a bit
  // trickier, but not too bad.  We could look for involuntary context
  // switches using getrusage(RUSAGE_THREAD, ..), but it's less work
  // (portability code and CPU) to just look for yield calls that take
  // longer than we expect.  sched_yield() doesn't actually result in any
  // context switch overhead if there are no other runnable processes
  // on the current core, in which case it usually takes less than
  // a microsecond.
  //
  // There are two primary tunables here: the threshold between "short"
  // and "long" waits, and the threshold at which we suspect that a yield
  // is slow enough to indicate we should probably block.  If these
  // thresholds are chosen well then CPU-bound workloads that don't
  // have more threads than cores will experience few context switches
  // (voluntary or involuntary), and the total number of context switches
  // (voluntary and involuntary) will not be dramatically larger (maybe
  // 2x) than the number of voluntary context switches that occur when
  // --max_yield_wait_micros=0.
  //
  // There's another constant, which is the number of slow yields we will
  // tolerate before reversing our previous decision.  Solitary slow
  // yields are pretty common (low-priority small jobs ready to run),
  // so this should be at least 2.  We set this conservatively to 3 so
  // that we can also immediately schedule a ctx adaptation, rather than
  // waiting for the next update_ctx.

  const size_t kMaxSlowYieldsWhileSpinning = 3;

  bool update_ctx = false;
  bool would_spin_again = false;

  if (max_yield_usec_ > 0) {
    update_ctx = Random::GetTLSInstance()->OneIn(256);

    if (update_ctx || ctx->value.load(std::memory_order_relaxed) >= 0) {
      // we're updating the adaptation statistics, or spinning has >
      // 50% chance of being shorter than max_yield_usec_ and causing no
      // involuntary context switches
      auto spin_begin = std::chrono::steady_clock::now();

      // this variable doesn't include the final yield (if any) that
      // causes the goal to be met
      size_t slow_yield_count = 0;

      auto iter_begin = spin_begin;
      while ((iter_begin - spin_begin) <=
             std::chrono::microseconds(max_yield_usec_)) {
        std::this_thread::yield();

        state = w->state.load(std::memory_order_acquire);
        if ((state & goal_mask) != 0) {
          // success
          would_spin_again = true;
          break;
        }

        auto now = std::chrono::steady_clock::now();
        if (now == iter_begin ||
            now - iter_begin >= std::chrono::microseconds(slow_yield_usec_)) {
          // conservatively count it as a slow yield if our clock isn't
          // accurate enough to measure the yield duration
          ++slow_yield_count;
          if (slow_yield_count >= kMaxSlowYieldsWhileSpinning) {
            // Not just one ivcsw, but several.  Immediately update ctx
            // and fall back to blocking
            update_ctx = true;
            break;
          }
        }
        iter_begin = now;
      }
    }
  }

  if ((state & goal_mask) == 0) {
    state = BlockingAwaitState(w, goal_mask);
  }

  if (update_ctx) {
    auto v = ctx->value.load(std::memory_order_relaxed);
    // fixed point exponential decay with decay constant 1/1024, with +1
    // and -1 scaled to avoid overflow for int32_t
    v = v + (v / 1024) + (would_spin_again ? 1 : -1) * 16384;
    ctx->value.store(v, std::memory_order_relaxed);
  }

  assert((state & goal_mask) != 0);
  return state;
}

void WriteThread::SetState(Writer* w, uint8_t new_state) {
  auto state = w->state.load(std::memory_order_acquire);
  if (state == STATE_LOCKED_WAITING ||
      !w->state.compare_exchange_strong(state, new_state)) {
    assert(state == STATE_LOCKED_WAITING);

    std::lock_guard<std::mutex> guard(w->StateMutex());
    assert(w->state.load(std::memory_order_relaxed) != new_state);
    w->state.store(new_state, std::memory_order_relaxed);
    w->StateCV().notify_one();
  }
}

void WriteThread::LinkOne(Writer* w, bool* linked_as_leader) {
  assert(w->state == STATE_INIT);

  while (true) {
    Writer* writers = newest_writer_.load(std::memory_order_relaxed);
    w->link_older = writers;
    if (newest_writer_.compare_exchange_strong(writers, w)) {
      if (writers == nullptr) {
        // this isn't part of the WriteThread machinery, but helps with
        // debugging and is checked by an assert in WriteImpl
        w->state.store(STATE_GROUP_LEADER, std::memory_order_relaxed);
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
    AwaitState(w,
               STATE_GROUP_LEADER | STATE_PARALLEL_FOLLOWER | STATE_COMPLETED,
               &ctx);
    TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:DoneWaiting", w);
  }
}

size_t WriteThread::EnterAsBatchGroupLeader(
    Writer* leader, WriteThread::Writer** last_writer,
    autovector<WriteThread::Writer*>* write_batch_group) {
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

    if (!w->disableWAL && leader->disableWAL) {
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
    if (!w->CallbackFailed()) {
      sequence += WriteBatchInternal::Count(w->batch);
    }
    w = w->link_newer;

    w->sequence = sequence;
    w->parallel_group = pg;
    SetState(w, STATE_PARALLEL_FOLLOWER);
  }
}

bool WriteThread::CompleteParallelWorker(Writer* w) {
  static AdaptationContext ctx("CompleteParallelWorker");

  auto* pg = w->parallel_group;
  if (!w->status.ok()) {
    std::lock_guard<std::mutex> guard(w->StateMutex());
    pg->status = w->status;
  }

  auto leader = pg->leader;
  auto early_exit_allowed = pg->early_exit_allowed;

  if (pg->running.load(std::memory_order_acquire) > 1 && pg->running-- > 1) {
    // we're not the last one
    AwaitState(w, STATE_COMPLETED, &ctx);

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
    assert(w->state == STATE_PARALLEL_FOLLOWER);
    SetState(leader, STATE_COMPLETED);
    AwaitState(w, STATE_COMPLETED, &ctx);
    return false;
  }
}

void WriteThread::EarlyExitParallelGroup(Writer* w) {
  auto* pg = w->parallel_group;

  assert(w->state == STATE_PARALLEL_FOLLOWER);
  assert(pg->status.ok());
  ExitAsBatchGroupLeader(pg->leader, pg->last_writer, pg->status);
  assert(w->status.ok());
  assert(w->state == STATE_COMPLETED);
  SetState(pg->leader, STATE_COMPLETED);
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
    SetState(last_writer->link_newer, STATE_GROUP_LEADER);
  }
  // else nobody else was waiting, although there might already be a new
  // leader now

  while (last_writer != leader) {
    last_writer->status = status;
    // we need to read link_older before calling SetState, because as soon
    // as it is marked committed the other thread's Await may return and
    // deallocate the Writer.
    auto next = last_writer->link_older;
    SetState(last_writer, STATE_COMPLETED);

    last_writer = next;
  }
}

void WriteThread::EnterUnbatched(Writer* w, InstrumentedMutex* mu) {
  static AdaptationContext ctx("EnterUnbatched");

  assert(w->batch == nullptr);
  bool linked_as_leader;
  LinkOne(w, &linked_as_leader);
  if (!linked_as_leader) {
    mu->Unlock();
    TEST_SYNC_POINT("WriteThread::EnterUnbatched:Wait");
    AwaitState(w, STATE_GROUP_LEADER, &ctx);
    mu->Lock();
  }
}

void WriteThread::ExitUnbatched(Writer* w) {
  Status dummy_status;
  ExitAsBatchGroupLeader(w, w, dummy_status);
}

}  // namespace rocksdb
