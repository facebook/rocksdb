//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/write_pipeline.h"

#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "util/sync_point.h"

namespace rocksdb {

WritePipeline::WritePipeline(uint64_t max_yield_usec, uint64_t slow_yield_usec,
                             bool allow_concurrent_memtable_write)
    : WriteThread(max_yield_usec, slow_yield_usec),
      allow_concurrent_memtable_write_(allow_concurrent_memtable_write),
      newest_wal_writer_(nullptr),
      newest_memtable_writer_(nullptr),
      last_sequence_(0) {}

bool WritePipeline::LinkOne(Writer* w, std::atomic<Writer*>& newest_writer) {
  assert(w->state == STATE_INIT || w->state == STATE_PARALLEL_FOLLOWER);
  Writer* newest = newest_writer.load(std::memory_order_relaxed);
  while (true) {
    w->link_older = newest;
    if (newest_writer.compare_exchange_weak(newest, w)) {
      return (newest == nullptr);
    }
  }
}

bool WritePipeline::LinkGroup(WriteGroup& write_group,
                              std::atomic<Writer*>& newest_writer) {
  for (auto w : write_group) {
    w->write_group = nullptr;
  }
  Writer* leader = write_group.leader;
  Writer* newest = newest_writer.load(std::memory_order_relaxed);
  assert(newest != leader);
  assert(leader->link_older == nullptr);
  while (true) {
    leader->link_older = newest;
    if (newest_writer.compare_exchange_weak(newest, write_group.last_writer)) {
      return (newest == nullptr);
    }
  }
}

WritePipeline::Writer* WritePipeline::FindNextLeader(Writer* newest_writer,
                                                     Writer* last_writer) {
  assert(newest_writer != last_writer);
  Writer* w = newest_writer;
  while (w->link_older != last_writer) {
    w = w->link_older;
  }
  assert(w != last_writer);
  return w;
}

void WritePipeline::CreateMissingNewerLinks(Writer* oldest, Writer* newest) {
  Writer* w = newest;
  while (w != oldest) {
    Writer* next = w->link_older;
    next->link_newer = w;
    w = next;
  }
}

void WritePipeline::CompleteLeader(WriteGroup& write_group) {
  assert(write_group.size > 0);
  Writer* leader = write_group.leader;
  if (write_group.size == 1) {
    write_group.leader = nullptr;
    write_group.last_writer = nullptr;
  } else {
    assert(leader->link_newer != nullptr);
    leader->link_newer->link_older = nullptr;
    write_group.leader = leader->link_newer;
  }
  write_group.size -= 1;
  SetState(leader, STATE_COMPLETED);
}

void WritePipeline::CompleteFollower(Writer* w, WriteGroup& write_group) {
  assert(write_group.size > 1);
  assert(w != write_group.leader);
  if (w == write_group.last_writer) {
    w->link_older->link_newer = nullptr;
    write_group.last_writer = w->link_older;
  } else {
    w->link_older->link_newer = w->link_newer;
    w->link_newer->link_older = w->link_older;
  }
  write_group.size -= 1;
  SetState(w, STATE_COMPLETED);
}

void WritePipeline::WaitForMemTableWriters() {
  static AdaptationContext ctx("WaitForMemTableWriters");

  if (newest_memtable_writer_.load() == nullptr) {
    return;
  }
  Writer w;
  if (!LinkOne(&w, newest_memtable_writer_)) {
    AwaitState(&w, STATE_MEMTABLE_WRITER, &ctx);
  }
  newest_memtable_writer_.store(nullptr);
}

void WritePipeline::JoinBatchGroup(Writer* w) {
  static AdaptationContext ctx("JoinBatchGroup");
  assert(w->batch != nullptr);
  if (LinkOne(w, newest_wal_writer_)) {
    SetState(w, STATE_WAL_WRITER);
  } else {
    AwaitState(w, STATE_WAL_WRITER | STATE_MEMTABLE_WRITER |
                      STATE_PARALLEL_FOLLOWER | STATE_COMPLETED,
               &ctx);
  }
}

size_t WritePipeline::EnterAsWALWriter(Writer* leader, WriteGroup* write_group,
                                       DB* db) {
  assert(leader != nullptr);
  assert(leader->link_older == nullptr);
  assert(leader->batch != nullptr);
  assert(write_group != nullptr);
  assert(leader->callback_status.ok());

  size_t size = WriteBatchInternal::ByteSize(leader->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  write_group->leader = leader;
  write_group->last_writer = leader;
  write_group->size = 1;

  if (leader->callback != nullptr && !leader->callback->AllowWriteBatching()) {
    return size;
  }

  Writer* newest_writer = newest_wal_writer_.load();

  // This is safe regardless of any db mutex status of the caller. Previous
  // calls to ExitAsGroupLeader either didn't call CreateMissingNewerLinks
  // (they emptied the list and then we added ourself as leader) or had to
  // explicitly wake us up (the list was non-empty when we added ourself,
  // so we have already received our MarkJoined).
  CreateMissingNewerLinks(leader, newest_writer);

  // Tricky. Iteration start (leader) is exclusive and finish
  // (newest_writer) is inclusive. Iteration goes from old to new.
  Writer* w = leader;
  while (w != newest_writer) {
    w = w->link_newer;

    if (w->CheckCallback(db)) {
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
    }
    write_group->last_writer = w;
    write_group->size++;
  }
  return size;
}

void WritePipeline::ExitAsWALWriter(WriteGroup& write_group) {
  static AdaptationContext ctx("ExitAsWALWriter");

  Writer* leader = write_group.leader;
  Writer* last_writer = write_group.last_writer;

  Writer dummy;
  Writer* newest_writer = newest_wal_writer_.load();
  Writer* next_leader = nullptr;
  assert(newest_writer != nullptr);
  if (newest_writer == last_writer) {
    newest_wal_writer_.compare_exchange_strong(newest_writer, &dummy);
  }
  if (newest_writer != last_writer) {
    next_leader = FindNextLeader(newest_writer, last_writer);
    next_leader->link_older = nullptr;
  }

  for (Writer* w = last_writer; w != leader;) {
    Writer* next = w->link_older;
    w->status = leader->status;
    if (!w->ShouldWriteToMemtable()) {
      CompleteFollower(w, write_group);
    }
    w = next;
  }
  if (!leader->ShouldWriteToMemtable()) {
    CompleteLeader(write_group);
  }
  if (write_group.size > 0) {
    if (LinkGroup(write_group, newest_memtable_writer_)) {
      // The leader can now be different from current writer.
      SetState(write_group.leader, STATE_MEMTABLE_WRITER);
    }
  }

  if (next_leader == nullptr) {
    newest_writer = &dummy;
    if (!newest_wal_writer_.compare_exchange_strong(newest_writer, nullptr)) {
      next_leader = FindNextLeader(newest_writer, &dummy);
      next_leader->link_older = nullptr;
    }
  }
  if (next_leader != nullptr) {
    SetState(next_leader, STATE_WAL_WRITER);
  }

  AwaitState(leader,
             STATE_MEMTABLE_WRITER | STATE_PARALLEL_FOLLOWER | STATE_COMPLETED,
             &ctx);
}

void WritePipeline::EnterAsMemTableWriter(Writer* leader,
                                          WriteGroup* write_group) {
  assert(leader != nullptr);
  assert(leader->link_older == nullptr);
  assert(leader->batch != nullptr);
  assert(write_group != nullptr);

  size_t size = WriteBatchInternal::ByteSize(leader->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  write_group->leader = leader;
  write_group->last_writer = leader;
  write_group->size = 1;

  if (allow_concurrent_memtable_write_ && leader->batch->HasMerge()) {
    return;
  }

  Writer* newest_writer = newest_memtable_writer_.load();
  CreateMissingNewerLinks(leader, newest_writer);

  Writer* w = leader;
  while (w != newest_writer) {
    w = w->link_newer;

    if (w->batch == nullptr) {
      break;
    }

    if (w->batch->HasMerge()) {
      break;
    }

    if (!allow_concurrent_memtable_write_) {
      auto batch_size = WriteBatchInternal::ByteSize(w->batch);
      if (size + batch_size > max_size) {
        // Do not make batch too big
        break;
      }
      size += batch_size;
    }

    write_group->last_writer = w;
    write_group->size++;
  }
}

void WritePipeline::ExitAsMemTableWriter(Writer* self, WriteGroup& write_group,
                                         VersionSet* versions) {
  Writer* leader = write_group.leader;
  Writer* last_writer = write_group.last_writer;
  versions->SetLastSequence(last_writer->sequence +
                            WriteBatchInternal::Count(last_writer->batch) - 1);

  Writer* newest_writer = newest_memtable_writer_.load();
  if (newest_writer != last_writer ||
      !newest_memtable_writer_.compare_exchange_strong(newest_writer,
                                                       nullptr)) {
    Writer* next_leader = FindNextLeader(newest_writer, last_writer);
    next_leader->link_older = nullptr;
    SetState(next_leader, STATE_MEMTABLE_WRITER);
  }
  auto v = write_group.ToVector();
  for (Writer* w : v) {
    if (!write_group.status.ok()) {
      w->status = write_group.status;
    }
    // Leader has to wait for the operation finish, because it owns the write
    // group.
    if (w != leader) {
      SetState(w, STATE_COMPLETED);
    }
  }
  SetState(leader, STATE_COMPLETED);
}

void WritePipeline::LaunchParallelMemTableWriter(WriteGroup& write_group) {
  // EnterAsBatchGroupLeader already created the links from leader to
  // newer writers in the group
  write_group.running.store(write_group.size);
  for (auto w : write_group) {
    w->write_group = &write_group;
    SetState(w, STATE_PARALLEL_FOLLOWER);
  }
}

bool WritePipeline::CompleteParallelMemTableWriter(Writer* w) {
  static AdaptationContext ctx("CompleteParallelWorker");

  WriteGroup* write_group = w->write_group;
  if (!w->status.ok()) {
    std::lock_guard<std::mutex> guard(write_group->leader->StateMutex());
    write_group->status = w->status;
  }

  if (write_group->running-- > 1) {
    // we're not the last one
    AwaitState(w, STATE_COMPLETED, &ctx);
    return false;
  }

  return true;
}

void WritePipeline::EnterUnbatched(Writer* w, InstrumentedMutex* mu) {
  static AdaptationContext ctx("EnterUnbatched");

  mu->Unlock();
  if (!LinkOne(w, newest_wal_writer_)) {
    TEST_SYNC_POINT("WriteThread::EnterUnbatched:Wait");
    AwaitState(w, STATE_WAL_WRITER, &ctx);
  }
  WaitForMemTableWriters();
  mu->Lock();
}

void WritePipeline::ExitUnbatched(Writer* w) {
  Writer* newest_writer = newest_wal_writer_.load();
  if (newest_writer == w) {
    newest_wal_writer_.compare_exchange_strong(newest_writer, nullptr);
  }
  if (newest_writer != w) {
    Writer* next_leader = FindNextLeader(newest_writer, w);
    next_leader->link_older = nullptr;
    SetState(next_leader, STATE_WAL_WRITER);
  }
}

}  // namespace rocksdb
