// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>
#include "rocksdb/status.h"

namespace rocksdb {

class DB;
class Status;

// The event listener of RocksDB which contains a set of call-back
// functions triggerred by specific RocksDB events such as flush.
//
// Note that all call-back functions should not run for an extended
// period of time before the function returns.  Otherwise, RocksDB
// may be blocked.
class EventListener {
 public:
  // A call-back function to RocksDB which will be called whenever a
  // registered RocksDB flushes a file.  The default implementation is
  // no-op.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  //
  // @param db a pointer to the rocksdb instance which just flushed
  //     a memtable to disk.
  // @param column_family_id the id of the flushed column family.
  // @param file_path the path to the newly created file.
  // @param triggered_writes_slowdown true when rocksdb is currently
  //     slowing-down all writes to prevent creating too many Level 0
  //     files as compaction seems not able to catch up the write request
  //     speed.  This indicates that there're too many files in Level 0.
  // @param triggered_writes_stop true when rocksdb is currently blocking
  //     any writes to prevent creating more L0 files.  This indicates that
  //     there're too many files in level 0.  Compactions should try to
  //     compact L0 files down to lower levels as soon as possible.
  virtual void OnFlushCompleted(
      DB* db, const std::string& column_family_name,
      const std::string& file_path,
      bool triggered_writes_slowdown,
      bool triggered_writes_stop) {}

  // A call-back function to RocksDB which will be called whenever a
  // compact-file request submitted via DB::ScheduleCompactFiles() has
  // been completed.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  virtual void OnBackgroundCompactFilesCompleted(
      DB* db, std::string job_id, Status s) {}
};

}  // namespace rocksdb
