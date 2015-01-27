// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include <vector>
#include "rocksdb/status.h"

namespace rocksdb {

class DB;
class Status;

struct CompactionJobInfo {
  // the name of the column family where the compaction happened.
  std::string cf_name;
  // the status indicating whether the compaction was successful or not.
  Status status;
  // the output level of the compaction.
  int output_level;
  // the names of the compaction input files.
  std::vector<std::string> input_files;
  // the names of the compaction output files.
  std::vector<std::string> output_files;
};

// EventListener class contains a set of call-back functions that will
// be called when specific RocksDB event happens such as flush.  It can
// be used as a building block for developing custom features such as
// stats-collector or external compaction algorithm.
//
// Note that call-back functions should not run for an extended period of
// time before the function returns, otherwise RocksDB may be blocked.
// For example, it is not suggested to do DB::CompactFiles() (as it may
// run for a long while) or issue many of DB::Put() (as Put may be blocked
// in certain cases) in the same thread in the EventListener callback.
// However, doing DB::CompactFiles() and DB::Put() in another thread is
// considered safe.
//
// [Threading] All EventListener callback will be called using the
// actual thread that involves in that specific event.   For example, it
// is the RocksDB background flush thread that does the actual flush to
// call EventListener::OnFlushCompleted().
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

  // A call-back function for RocksDB which will be called whenever
  // a registered RocksDB compacts a file. The default implementation
  // is a no-op.
  //
  // Note that this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns. Otherwise, RocksDB may be blocked.
  //
  // @param db a pointer to the rocksdb instance which just compacted
  //   a file.
  // @param ci a reference to a CompactionJobInfo struct. 'ci' is released
  //  after this function is returned, and must be copied if it is needed
  //  outside of this function.
  virtual void OnCompactionCompleted(DB *db, const CompactionJobInfo& ci) {}
  virtual ~EventListener() {}
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
