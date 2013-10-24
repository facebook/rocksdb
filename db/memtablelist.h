//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once
#include <string>
#include <list>
#include <deque>
#include "rocksdb/db.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "memtable.h"

namespace rocksdb {

class InternalKeyComparator;
class Mutex;
class MemTableListIterator;

//
// This class stores references to all the immutable memtables.
// The memtables are flushed to L0 as soon as possible and in
// any order. If there are more than one immutable memtable, their
// flushes can occur concurrently.  However, they are 'committed'
// to the manifest in FIFO order to maintain correctness and
// recoverability from a crash.
//
class MemTableList {
 public:
  // A list of memtables.
  MemTableList() : size_(0), num_flush_not_started_(0),
    commit_in_progress_(false),
    flush_requested_(false) {
    imm_flush_needed.Release_Store(nullptr);
  }
  ~MemTableList() {};

  // so that backgrund threads can detect non-nullptr pointer to
  // determine whether this is anything more to start flushing.
  port::AtomicPointer imm_flush_needed;

  // Increase reference count on all underling memtables
  void RefAll();

  // Drop reference count on all underling memtables
  void UnrefAll();

  // Returns the total number of memtables in the list
  int size();

  // Returns true if there is at least one memtable on which flush has
  // not yet started.
  bool IsFlushPending(int min_write_buffer_number_to_merge);

  // Returns the earliest memtables that needs to be flushed. The returned
  // memtables are guaranteed to be in the ascending order of created time.
  void PickMemtablesToFlush(std::vector<MemTable*>* mems);

  // Commit a successful flush in the manifest file
  Status InstallMemtableFlushResults(const std::vector<MemTable*> &m,
                      VersionSet* vset, Status flushStatus,
                      port::Mutex* mu, Logger* info_log,
                      uint64_t file_number,
                      std::set<uint64_t>& pending_outputs);

  // New memtables are inserted at the front of the list.
  // Takes ownership of the referenced held on *m by the caller of Add().
  void Add(MemTable* m);

  // Returns an estimate of the number of bytes of data in use.
  size_t ApproximateMemoryUsage();

  // Search all the memtables starting from the most recent one.
  // Return the most recent value found, if any.
  bool Get(const LookupKey& key, std::string* value, Status* s,
           std::deque<std::string>* operands, const Options& options);

  // Returns the list of underlying memtables.
  void GetMemTables(std::vector<MemTable*>* list);

  // Request a flush of all existing memtables to storage
  void FlushRequested() { flush_requested_ = true; }

  // Copying allowed
  // MemTableList(const MemTableList&);
  // void operator=(const MemTableList&);

 private:
  std::list<MemTable*> memlist_;
  int size_;

  // the number of elements that still need flushing
  int num_flush_not_started_;

  // committing in progress
  bool commit_in_progress_;

  // Requested a flush of all memtables to storage
  bool flush_requested_;

};

}  // namespace rocksdb
