//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "db/memtable_list.h"

#include <string>
#include "rocksdb/db.h"
#include "db/memtable.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "util/coding.h"

namespace rocksdb {

class InternalKeyComparator;
class Mutex;
class VersionSet;

MemTableListVersion::MemTableListVersion(MemTableListVersion* old) {
  if (old != nullptr) {
    memlist_ = old->memlist_;
    size_ = old->size_;
    for (auto& m : memlist_) {
      m->Ref();
    }
  }
}

void MemTableListVersion::Ref() { ++refs_; }

void MemTableListVersion::Unref(autovector<MemTable*>* to_delete) {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    // if to_delete is equal to nullptr it means we're confident
    // that refs_ will not be zero
    assert(to_delete != nullptr);
    for (const auto& m : memlist_) {
      MemTable* x = m->Unref();
      if (x != nullptr) {
        to_delete->push_back(x);
      }
    }
    delete this;
  }
}

int MemTableListVersion::size() const { return size_; }

// Returns the total number of memtables in the list
int MemTableList::size() const {
  assert(num_flush_not_started_ <= current_->size_);
  return current_->size_;
}

// Search all the memtables starting from the most recent one.
// Return the most recent value found, if any.
// Operands stores the list of merge operations to apply, so far.
bool MemTableListVersion::Get(const LookupKey& key, std::string* value,
                              Status* s, MergeContext& merge_context,
                              const Options& options) {
  for (auto& memtable : memlist_) {
    if (memtable->Get(key, value, s, merge_context, options)) {
      return true;
    }
  }
  return false;
}

void MemTableListVersion::AddIterators(const ReadOptions& options,
                                       std::vector<Iterator*>* iterator_list) {
  for (auto& m : memlist_) {
    iterator_list->push_back(m->NewIterator(options));
  }
}

// caller is responsible for referencing m
void MemTableListVersion::Add(MemTable* m) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  memlist_.push_front(m);
  ++size_;
}

// caller is responsible for unreferencing m
void MemTableListVersion::Remove(MemTable* m) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  memlist_.remove(m);
  --size_;
}

// Returns true if there is at least one memtable on which flush has
// not yet started.
bool MemTableList::IsFlushPending() const {
  if ((flush_requested_ && num_flush_not_started_ >= 1) ||
      (num_flush_not_started_ >= min_write_buffer_number_to_merge_)) {
    assert(imm_flush_needed.NoBarrier_Load() != nullptr);
    return true;
  }
  return false;
}

// Returns the memtables that need to be flushed.
void MemTableList::PickMemtablesToFlush(autovector<MemTable*>* ret) {
  const auto& memlist = current_->memlist_;
  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
    MemTable* m = *it;
    if (!m->flush_in_progress_) {
      assert(!m->flush_completed_);
      num_flush_not_started_--;
      if (num_flush_not_started_ == 0) {
        imm_flush_needed.Release_Store(nullptr);
      }
      m->flush_in_progress_ = true;  // flushing will start very soon
      ret->push_back(m);
    }
  }
  flush_requested_ = false;  // start-flush request is complete
}

void MemTableList::RollbackMemtableFlush(const autovector<MemTable*>& mems,
     uint64_t file_number, std::set<uint64_t>* pending_outputs) {
  assert(!mems.empty());

  // If the flush was not successful, then just reset state.
  // Maybe a suceeding attempt to flush will be successful.
  for (MemTable* m : mems) {
    assert(m->flush_in_progress_);
    assert(m->file_number_ == 0);

    m->flush_in_progress_ = false;
    m->flush_completed_ = false;
    m->edit_.Clear();
    num_flush_not_started_++;
  }
  pending_outputs->erase(file_number);
  imm_flush_needed.Release_Store(reinterpret_cast<void *>(1));
}

// Record a successful flush in the manifest file
Status MemTableList::InstallMemtableFlushResults(
    const autovector<MemTable*>& mems, VersionSet* vset,
    port::Mutex* mu, Logger* info_log, uint64_t file_number,
    std::set<uint64_t>& pending_outputs, autovector<MemTable*>* to_delete,
    Directory* db_directory) {
  mu->AssertHeld();

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
  while (!current_->memlist_.empty() && s.ok()) {
    MemTable* m = current_->memlist_.back();  // get the last element
    if (!m->flush_completed_) {
      break;
    }

    Log(info_log,
        "Level-0 commit table #%lu started",
        (unsigned long)m->file_number_);

    // this can release and reacquire the mutex.
    s = vset->LogAndApply(&m->edit_, mu, db_directory);

    // we will be changing the version in the next code path,
    // so we better create a new one, since versions are immutable
    InstallNewVersion();

    // All the later memtables that have the same filenum
    // are part of the same batch. They can be committed now.
    uint64_t mem_id = 1;  // how many memtables has been flushed.
    do {
      if (s.ok()) { // commit new state
        Log(info_log,
            "Level-0 commit table #%lu: memtable #%lu done",
            (unsigned long)m->file_number_,
            (unsigned long)mem_id);
        current_->Remove(m);
        assert(m->file_number_ > 0);

        // pending_outputs can be cleared only after the newly created file
        // has been written to a committed version so that other concurrently
        // executing compaction threads do not mistakenly assume that this
        // file is not live.
        pending_outputs.erase(m->file_number_);
        if (m->Unref() != nullptr) {
          to_delete->push_back(m);
        }
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
      }
      ++mem_id;
    } while (!current_->memlist_.empty() && (m = current_->memlist_.back()) &&
             m->file_number_ == file_number);
  }
  commit_in_progress_ = false;
  return s;
}

// New memtables are inserted at the front of the list.
void MemTableList::Add(MemTable* m) {
  assert(current_->size_ >= num_flush_not_started_);
  InstallNewVersion();
  // this method is used to move mutable memtable into an immutable list.
  // since mutable memtable is already refcounted by the DBImpl,
  // and when moving to the imutable list we don't unref it,
  // we don't have to ref the memtable here. we just take over the
  // reference from the DBImpl.
  current_->Add(m);
  m->MarkImmutable();
  num_flush_not_started_++;
  if (num_flush_not_started_ == 1) {
    imm_flush_needed.Release_Store((void *)1);
  }
}

// Returns an estimate of the number of bytes of data in use.
size_t MemTableList::ApproximateMemoryUsage() {
  size_t size = 0;
  for (auto& memtable : current_->memlist_) {
    size += memtable->ApproximateMemoryUsage();
  }
  return size;
}

void MemTableList::InstallNewVersion() {
  if (current_->refs_ == 1) {
    // we're the only one using the version, just keep using it
  } else {
    // somebody else holds the current version, we need to create new one
    MemTableListVersion* version = current_;
    current_ = new MemTableListVersion(current_);
    current_->Ref();
    version->Unref();
  }
}

}  // namespace rocksdb
