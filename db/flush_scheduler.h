//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>
#include <atomic>
#include <mutex>
#include <set>
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyData;

// FlushScheduler keeps track of all column families whose memtable may
// be full and require flushing. Unless otherwise noted, all methods on
// FlushScheduler should be called only with the DB mutex held or from
// a single-threaded recovery context.
class FlushScheduler {
 public:
  FlushScheduler() : head_(nullptr) {}

  // May be called from multiple threads at once, but not concurrent with
  // any other method calls on this instance
  void ScheduleWork(ColumnFamilyData* cfd);

  // Removes and returns Ref()-ed column family. Client needs to Unref().
  // Filters column families that have been dropped.
  ColumnFamilyData* TakeNextColumnFamily();

  // This can be called concurrently with ScheduleWork but it would miss all
  // the scheduled flushes after the last synchronization. This would result
  // into less precise enforcement of memtable sizes but should not matter much.
  bool Empty();

  void Clear();

 private:
  struct Node {
    ColumnFamilyData* column_family;
    Node* next;
  };

  std::atomic<Node*> head_;
#ifndef NDEBUG
  std::mutex checking_mutex_;
  std::set<ColumnFamilyData*> checking_set_;
#endif  // NDEBUG
};

}  // namespace ROCKSDB_NAMESPACE
