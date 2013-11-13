//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "db/memtablelist.h"

#include <string>
#include "rocksdb/db.h"
#include "db/memtable.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "util/coding.h"

namespace rocksdb {

class InternalKeyComparator;
class Mutex;
class MemTableListIterator;
class VersionSet;

using std::list;

// Increase reference count on all underling memtables
void MemTableList::RefAll() {
  for (auto &memtable : memlist_) {
    memtable->Ref();
  }
}

// Drop reference count on all underling memtables
void MemTableList::UnrefAll() {
  for (auto &memtable : memlist_) {
    memtable->Unref();
  }
}

// Returns the total number of memtables in the list
int MemTableList::size() {
  assert(num_flush_not_started_ <= size_);
  return size_;
}

// Returns true if there is at least one memtable on which flush has
// not yet started.
bool MemTableList::IsFlushPending(int min_write_buffer_number_to_merge) {
  if ((flush_requested_ && num_flush_not_started_ >= 1) ||
      (num_flush_not_started_ >= min_write_buffer_number_to_merge)) {
    assert(imm_flush_needed.NoBarrier_Load() != nullptr);
    return true;
  }
  return false;
}

// Returns the memtables that need to be flushed.
void MemTableList::PickMemtablesToFlush(std::vector<MemTable*>* ret) {
  for (auto it = memlist_.rbegin(); it != memlist_.rend(); it++) {
    MemTable* m = *it;
    if (!m->flush_in_progress_) {
      assert(!m->flush_completed_);
      num_flush_not_started_--;
      if (num_flush_not_started_ == 0) {
        imm_flush_needed.Release_Store(nullptr);
      }
      m->flush_in_progress_ = true; // flushing will start very soon
      ret->push_back(m);
    }
  }
  flush_requested_ = false; // start-flush request is complete
}

// Record a successful flush in the manifest file
Status MemTableList::InstallMemtableFlushResults(
                      const std::vector<MemTable*> &mems,
                      VersionSet* vset, Status flushStatus,
                      port::Mutex* mu, Logger* info_log,
                      uint64_t file_number,
                      std::set<uint64_t>& pending_outputs) {
  mu->AssertHeld();

  // If the flush was not successful, then just reset state.
  // Maybe a suceeding attempt to flush will be successful.
  if (!flushStatus.ok()) {
    for (MemTable* m : mems) {
      assert(m->flush_in_progress_);
      assert(m->file_number_ == 0);

      m->flush_in_progress_ = false;
      m->flush_completed_ = false;
      m->edit_.Clear();
      num_flush_not_started_++;
      imm_flush_needed.Release_Store((void *)1);
      pending_outputs.erase(file_number);
    }
    return flushStatus;
  }

  // flush was sucessful
  for (size_t i = 0; i < mems.size(); ++i) {
    // All the edits are associated with the first memtable of this batch.
    assert(i == 0 || mems[i]->GetEdits()->NumEntries() == 0);

    mems[i]->flush_completed_ = true;
    mems[i]->file_number_ = file_number;
  }

  // if some other thread is already commiting, then return
  Status s;
  if (commit_in_progress_) {
    return s;
  }

  // Only a single thread can be executing this piece of code
  commit_in_progress_ = true;

  // scan all memtables from the earliest, and commit those
  // (in that order) that have finished flushing. Memetables
  // are always committed in the order that they were created.
  while (!memlist_.empty() && s.ok()) {
    MemTable* m = memlist_.back(); // get the last element
    if (!m->flush_completed_) {
      break;
    }

    Log(info_log,
        "Level-0 commit table #%lu started",
        (unsigned long)m->file_number_);

    // this can release and reacquire the mutex.
    s = vset->LogAndApply(&m->edit_, mu);

    // All the later memtables that have the same filenum
    // are part of the same batch. They can be committed now.
    uint64_t mem_id = 1;  // how many memtables has been flushed.
    do {
      if (s.ok()) { // commit new state
        Log(info_log,
            "Level-0 commit table #%lu: memtable #%lu done",
            (unsigned long)m->file_number_,
            (unsigned long)mem_id);
        memlist_.remove(m);
        assert(m->file_number_ > 0);

        // pending_outputs can be cleared only after the newly created file
        // has been written to a committed version so that other concurrently
        // executing compaction threads do not mistakenly assume that this
        // file is not live.
        pending_outputs.erase(m->file_number_);
        m->Unref();
        size_--;
      } else {
        //commit failed. setup state so that we can flush again.
        Log(info_log,
            "Level-0 commit table #%lu: memtable #%lu failed",
            (unsigned long)m->file_number_,
            (unsigned long)mem_id);
        m->flush_completed_ = false;
        m->flush_in_progress_ = false;
        m->edit_.Clear();
        num_flush_not_started_++;
        pending_outputs.erase(m->file_number_);
        m->file_number_ = 0;
        imm_flush_needed.Release_Store((void *)1);
        s = Status::IOError("Unable to commit flushed memtable");
      }
      ++mem_id;
    } while (!memlist_.empty() && (m = memlist_.back()) &&
             m->file_number_ == file_number);
  }
  commit_in_progress_ = false;
  return s;
}

// New memtables are inserted at the front of the list.
void MemTableList::Add(MemTable* m) {
  assert(size_ >= num_flush_not_started_);
  size_++;
  memlist_.push_front(m);
  m->MarkImmutable();
  num_flush_not_started_++;
  if (num_flush_not_started_ == 1) {
    imm_flush_needed.Release_Store((void *)1);
  }
}

// Returns an estimate of the number of bytes of data in use.
size_t MemTableList::ApproximateMemoryUsage() {
  size_t size = 0;
  for (auto &memtable : memlist_) {
    size += memtable->ApproximateMemoryUsage();
  }
  return size;
}

// Search all the memtables starting from the most recent one.
// Return the most recent value found, if any.
// Operands stores the list of merge operations to apply, so far.
bool MemTableList::Get(const LookupKey& key, std::string* value, Status* s,
                       std::deque<std::string>* operands,
                       const Options& options) {
  for (auto &memtable : memlist_) {
    if (memtable->Get(key, value, s, operands, options)) {
      return true;
    }
  }
  return false;
}

void MemTableList::GetMemTables(std::vector<MemTable*>* output) {
  for (auto &memtable : memlist_) {
    output->push_back(memtable);
  }
}

}  // namespace rocksdb
