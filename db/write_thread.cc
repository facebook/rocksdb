//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/write_thread.h"

namespace rocksdb {

void WriteThread::EnterWriteThread(WriteThread::Writer* w) {
  // the following code block pushes the current writer "w" into the writer
  // queue "writers_" and wait until one of the following conditions met:
  // 1. the job of "w" has been done by some other writers.
  // 2. "w" becomes the first writer in "writers_"
  // 3. "w" timed-out.
  writers_.push_back(w);

  while (!w->done && w != writers_.front()) {
    w->cv.Wait();
  }
}

void WriteThread::ExitWriteThread(WriteThread::Writer* w,
                                  WriteThread::Writer* last_writer,
                                  Status status) {
  // Pop out the current writer and all writers being pushed before the
  // current writer from the writer queue.
  while (!writers_.empty()) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }
}

// This function will be called only when the first writer succeeds.
// All writers in the to-be-built batch group will be processed.
//
// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-nullptr batch
size_t WriteThread::BuildBatchGroup(
    WriteThread::Writer** last_writer,
    autovector<WriteBatch*>* write_batch_group) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  assert(first->batch != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);
  write_batch_group->push_back(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;

  if (first->has_callback) {
    // TODO(agiardullo:) Batching not currently supported as this write may
    // fail if the callback function decides to abort this write.
    return size;
  }

  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (!w->disableWAL && first->disableWAL) {
      // Do not include a write that needs WAL into a batch that has
      // WAL disabled.
      break;
    }

    if (w->has_callback) {
      // Do not include writes which may be aborted if the callback does not
      // succeed.
      break;
    }

    if (w->batch == nullptr) {
      // Do not include those writes with nullptr batch. Those are not writes,
      // those are something else. They want to be alone
      break;
    }

    size += WriteBatchInternal::ByteSize(w->batch);
    if (size > max_size) {
      // Do not make batch too big
      break;
    }

    write_batch_group->push_back(w->batch);
    w->in_batch_group = true;
    *last_writer = w;
  }
  return size;
}

}  // namespace rocksdb
