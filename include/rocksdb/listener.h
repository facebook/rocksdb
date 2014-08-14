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
  virtual void OnFlushCompleted(
      DB* db, const std::string& column_family_name,
      const std::string& file_path) {}

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
