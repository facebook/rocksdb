//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <string>
#include <list>
#include <vector>
#include <set>
#include <deque>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/skiplist.h"
#include "db/memtable.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "util/autovector.h"

namespace rocksdb {

class ColumnFamilyData;
class InternalKeyComparator;
class Mutex;

// keeps a list of immutable memtables in a vector. the list is immutable
// if refcount is bigger than one. It is used as a state for Get() and
// Iterator code paths
class MemTableListVersion {
 public:
  explicit MemTableListVersion(MemTableListVersion* old = nullptr);

  void Ref();
  void Unref(autovector<MemTable*>* to_delete = nullptr);

  int size() const;

  // Search all the memtables starting from the most recent one.
  // Return the most recent value found, if any.
  bool Get(const LookupKey& key, std::string* value, Status* s,
           MergeContext& merge_context, const Options& options);

  void AddIterators(const ReadOptions& options,
                    std::vector<Iterator*>* iterator_list);

 private:
  // REQUIRE: m is mutable memtable
  void Add(MemTable* m);
  // REQUIRE: m is mutable memtable
  void Remove(MemTable* m);

  friend class MemTableList;
  std::list<MemTable*> memlist_;
  int size_ = 0;
  int refs_ = 0;
};

// This class stores references to all the immutable memtables.
// The memtables are flushed to L0 as soon as possible and in
// any order. If there are more than one immutable memtable, their
// flushes can occur concurrently.  However, they are 'committed'
// to the manifest in FIFO order to maintain correctness and
// recoverability from a crash.
class MemTableList {
 public:
  // A list of memtables.
  explicit MemTableList(int min_write_buffer_number_to_merge)
      : min_write_buffer_number_to_merge_(min_write_buffer_number_to_merge),
        current_(new MemTableListVersion()),
        num_flush_not_started_(0),
        commit_in_progress_(false),
        flush_requested_(false) {
    imm_flush_needed.Release_Store(nullptr);
    current_->Ref();
  }
  ~MemTableList() {}

  MemTableListVersion* current() { return current_; }

  // so that background threads can detect non-nullptr pointer to
  // determine whether there is anything more to start flushing.
  port::AtomicPointer imm_flush_needed;

  // Returns the total number of memtables in the list
  int size() const;

  // Returns true if there is at least one memtable on which flush has
  // not yet started.
  bool IsFlushPending();

  // Returns the earliest memtables that needs to be flushed. The returned
  // memtables are guaranteed to be in the ascending order of created time.
  void PickMemtablesToFlush(autovector<MemTable*>* mems);

  // Reset status of the given memtable list back to pending state so that
  // they can get picked up again on the next round of flush.
  void RollbackMemtableFlush(const autovector<MemTable*>& mems,
                             uint64_t file_number,
                             std::set<uint64_t>* pending_outputs);

  // Commit a successful flush in the manifest file
  Status InstallMemtableFlushResults(ColumnFamilyData* cfd,
                                     const autovector<MemTable*>& m,
                                     VersionSet* vset, port::Mutex* mu,
                                     Logger* info_log, uint64_t file_number,
                                     std::set<uint64_t>& pending_outputs,
                                     autovector<MemTable*>* to_delete,
                                     Directory* db_directory);

  // New memtables are inserted at the front of the list.
  // Takes ownership of the referenced held on *m by the caller of Add().
  void Add(MemTable* m);

  // Returns an estimate of the number of bytes of data in use.
  size_t ApproximateMemoryUsage();

  // Request a flush of all existing memtables to storage
  void FlushRequested() { flush_requested_ = true; }

  // Copying allowed
  // MemTableList(const MemTableList&);
  // void operator=(const MemTableList&);

 private:
  // DB mutex held
  void InstallNewVersion();

  int min_write_buffer_number_to_merge_;

  MemTableListVersion* current_;

  // the number of elements that still need flushing
  int num_flush_not_started_;

  // committing in progress
  bool commit_in_progress_;

  // Requested a flush of all memtables to storage
  bool flush_requested_;

};

}  // namespace rocksdb
