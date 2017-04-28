//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once

#include "db/write_thread.h"

namespace rocksdb {

class DB;
class VersionSet;

class WritePipeline : public WriteThread {
 public:
  WritePipeline(uint64_t max_yield_usec, uint64_t slow_yield_usec,
                bool allow_concurrent_memtable_write);

  void JoinBatchGroup(Writer* w);

  size_t EnterAsWALWriter(Writer* leader, WriteGroup* write_group, DB* db);

  void ExitAsWALWriter(WriteGroup& write_group);

  void EnterAsMemTableWriter(Writer* leader, WriteGroup* write_group);

  void ExitAsMemTableWriter(Writer* self, WriteGroup& write_group,
                            VersionSet* versions);

  void LaunchParallelMemTableWriter(WriteGroup& write_group);

  bool CompleteParallelMemTableWriter(Writer* w);

  void WaitForMemTableWriters();

  virtual void EnterUnbatched(Writer* w, InstrumentedMutex* mu) override;

  virtual void ExitUnbatched(Writer* w) override;

  SequenceNumber UpdateLastSequence(SequenceNumber sequence) {
    if (sequence > last_sequence_) {
      last_sequence_ = sequence;
    }
    return last_sequence_;
  }

 private:
  const bool allow_concurrent_memtable_write_;

  // Points to the newest pending Writer.  Only leader can remove
  // elements, adding can be done lock-free by anybody
  std::atomic<Writer*> newest_wal_writer_;
  std::atomic<Writer*> newest_memtable_writer_;

  SequenceNumber last_sequence_;

  // return linked as leader
  bool LinkOne(Writer* w, std::atomic<Writer*>& newest_writer);

  bool LinkGroup(WriteGroup& write_group, std::atomic<Writer*>& newest_writer);

  // Computes any missing link_newer links.  Should not be called
  // concurrently with itself.
  void CreateMissingNewerLinks(Writer* oldest, Writer* newest);

  Writer* FindNextLeader(Writer* newest_writer, Writer* last_writer);

  void CompleteLeader(WriteGroup& write_group);

  void CompleteFollower(Writer* w, WriteGroup& write_group);
};

}  // namespace rocksdb
